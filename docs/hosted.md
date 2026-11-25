# mudcat as a hosted service

Status: sketch. This document describes how mudcat can run as a cloud service (software as a service). The components are those of [architecture.md](architecture.md). The requirements are OP-7 to OP-11 and RD-5 in [requirements.md](requirements.md).

## The main idea: a control plane and a data plane

mudcat is already outside the read path, so it splits well into two planes:

- **Control plane.** The vendor runs it. It holds metadata, provenance, policy and access decisions. It never reads or stores customer data bytes.
- **Data plane.** It runs in the customer's environment: their cloud accounts, HPC centres and enclaves. It holds the bytes, and everything that must read them.

| Plane | Components |
|---|---|
| Control plane (vendor) | API gateway, catalog API, query compiler, resolver, policy engine, authorization port, ingest writer, catalog database, background workers, gateways, hosted MCP server, web UI |
| Data plane (customer) | Site agents, format plugins, movers, compaction compute, and optionally a customer gateway with a metadata store |

Format plugins run inside site agents. Thus extraction, fingerprinting and checksum verification happen next to the data, and only their results go to the control plane.

```d2
vars: {
  d2-config: {
    layout-engine: elk
  }
}
direction: down

users: "Users and agents\nSDK · CLI · web UI · notebooks · MCP clients"

vendor: Vendor control plane (one region) {
  front: "API gateway\ncustomer IdP through OIDC · SCIM groups"
  services: "Catalog services\ncatalog API · query · resolver · policy · authorization port · ingest writer"
  tenants: "Tenant catalogs\none PostgreSQL database for each instance" {
    shape: cylinder
  }
  workers: "Background workers\nindexes · lineage · maintenance · placement · deletion"
  front -> services
  services -> tenants
  tenants -> workers: change log
}

customer: Customer data plane {
  agents: "Site agents\nformat plugins · verification · inventory · deletion"
  broker: "Customer gateway (optional)\ncredentials · customer-held metadata · keys"
  metastore: "Customer metadata store" {
    shape: cylinder
  }
  broker -> metastore
  storage: "Storage\nbuckets · HPC file systems · enclaves"
  movers: "Movers\nFTS3 · Globus · rclone · cloud replication"
  agents -> storage
  agents -> movers
  movers -> storage
  broker -> storage: scoped credentials
}

users -> vendor.front
vendor.workers -> customer.agents: tasks (agents connect out)
users -> customer.storage: direct reads
vendor.services -> customer.broker: split queries · policy checks (gateway connects out)
```

## Connections

- **Site agents connect out.** An agent opens an outbound HTTPS connection to the control plane and leases its tasks. The customer opens no inbound port. This is the model of self-hosted CI runners and workflow agents.
- **Clients read data directly.** A client gets a resolved manifest and credentials from the control plane, then reads from storage. Data bytes never pass through the vendor.
- **Results come back as metadata.** Agents send structure descriptors, metadata, fingerprints, checksums and receipts through the ingest writer.

## Credentials for storage

The resolver must give clients credentials, but the customer must keep control of their storage. There are three modes, and each instance selects one:

| Mode | How the resolver gets credentials | Vendor access to bytes |
|---|---|---|
| **Delegated role** | The customer gives the control plane a role in their cloud account. The role is limited to named buckets and prefixes. The resolver uses the cloud provider's federation to get short-lived credentials, scoped to one ACL root at a time | Possible, but limited to the role, short-lived and visible in the customer's cloud audit log |
| **Credential broker** | A small broker runs in the customer's account. The resolver asks the broker for scoped credentials or signed URLs, and passes them to the client | None. The vendor never holds credentials for customer storage |
| **Agent-signed URLs** | For HPC and on-premises storage, the site agent signs URLs or issues tokens for the storage it serves | None |

The **credential broker** mode is the "no data access" mode. Regulated customers will usually need it.

## Tenancy

A **tenant** is a customer organisation. A tenant has one or more instances. An instance is a logical catalog, not a deployment: one control plane serves many instances. The instance is the unit of isolation, because the model already uses instances as the unit of authority and federation.

| Tier | Isolation | For |
|---|---|---|
| Shared | One PostgreSQL database for each instance, on shared database clusters; shared stateless services | Small teams, trials |
| Dedicated | A dedicated database cluster and dedicated workers for the tenant, in the vendor's account | Large or regulated tenants |
| Customer cloud (BYOC) | The vendor operates the control plane inside the customer's own cloud account | Customers who must keep metadata in their own account |
| Self-hosted | The customer runs the open-source release | Facilities with their own operations staff |

The same code runs in all four tiers. Only the deployment differs.

A database for each instance has these advantages over rows of several tenants in one database:

- **Recovery:** backup, point-in-time recovery and restore apply to one tenant.
- **Migrations:** schema migrations can be applied to one tenant at a time, with canaries.
- **Deletion:** when a tenant leaves, its database is deleted, and nothing remains in shared tables.
- **Keys:** customer-managed encryption keys apply to one database.
- **Limits:** a tenant's rows cannot appear in another tenant's queries, even if the application has an error.

Very small instances can share a database, with one schema for each instance. Larger tenants move to their own database without a change to the application.

## Metadata that the customer controls

"Control" can mean two different things, and mudcat handles them in different ways:

- **Authority.** A customer system, for example a laboratory information system or an electronic notebook, is the only writer of some metadata. The vendor may store a copy.
- **Custody.** Some metadata must not leave the customer's environment at all, for example consent status, patient attributes, proprietary structures or confidential file names.

### Authority: namespaces that only the customer writes

Attribute definitions belong to namespaces (model, section 3). A customer can own a namespace and name the only principals that can write attributes in it, for example the service account of its laboratory system. mudcat then rejects writes to that namespace from any other principal, its own background workers included. The assertion origin of each value records the customer system as the writer.

### Custody: three levels for each attribute

Each attribute definition has a **custody level**. The custody level sets where mudcat stores the values, and what the control plane can do with them.

| Custody level | Where values are | What the control plane can do |
|---|---|---|
| **Vendor-held** | The instance's database in the control plane | Everything: queries, sorts, facets, policy evaluation |
| **Customer-encrypted** | The control plane stores ciphertext. Only the customer holds the key | Equality queries through a **blind index**: a keyed hash of each value, which the customer gateway computes. No range queries, sorts or full-text search. The control plane cannot read the values |
| **Customer-held** | A metadata store in the customer's data plane, behind the customer gateway | Nothing directly. The control plane stores only the fact that the attribute exists for an entity |

The same levels apply to annotation sets and manifests: they can stay in a bucket in the customer's account.

Blind indexes show which entities have equal values. Customers must accept this for each attribute that they put at the customer-encrypted level. Values that are rare enough to identify a person need the customer-held level.

### The customer gateway

The credential broker grows into a **customer gateway** in the data plane, with two jobs:

1. It issues storage credentials (credential-broker mode).
2. It serves customer-held and customer-encrypted metadata.

The gateway runs next to the customer's metadata store, and holds the customer's keys. Like a site agent, the gateway opens its own outbound connection to the control plane, and the control plane sends its requests over that connection. Thus the customer still opens no inbound port.

### How queries, the UI and policy work with customer-held metadata

- **Queries.** The query compiler splits a query in two:
  - the control plane evaluates the predicates on vendor-held attributes;
  - the customer gateway evaluates the predicates on customer-held attributes and returns the matching identifiers.

  The control plane then joins the two results by identifier. The gateway never returns values, only identifiers. For customer-encrypted attributes, the gateway computes the blind-index value of the query literal, and the control plane matches it.
- **The web UI.** The browser gets customer-held values directly from the customer gateway, with the user's own token, and shows them next to the rest of the page. The values never pass through the vendor.
- **Usage policy.** Some policy decisions depend on customer-held values, for example consent status at resolution. For these, the policy engine sends the identifiers and the condition to the customer gateway. The gateway returns the decision and a signature. mudcat records the decision and the signature in the policy decision of the resolution, but not the values.
- **Search indexes.** The control plane's search and vector indexes contain only vendor-held attributes. A customer can run an index of its own behind the gateway. The query compiler then sends text predicates on customer-held fields to it.
- **Availability.** If the customer gateway is not available, mudcat rejects queries and resolutions that need customer-held metadata. Queries that do not need it continue to work.

### Federation as an alternative

A customer that wants to hold much of its metadata can run a small self-hosted instance for it, and federate it with its hosted instances. This uses the normal federation rules: each instance makes the decisions about the data that it owns. Custody levels are better when only a few attributes are sensitive. A federated instance is better when whole datasets are sensitive.

## Regions and residency

- **One region for each instance.** Each instance is pinned to one region of the control plane. Metadata can be sensitive (file names, sources, consent status), so metadata residency matters as much as data residency.
- **Regional control planes.** The vendor runs one control plane in each region it offers. A tenant with instances in several regions links them through the normal federation feed, as independent instances.
- **Global data.** Only account data (tenants, billing, the list of instances and their regions) is global.

## Derived state

- **Manifests.** By default, the control plane stores manifests in a per-tenant store, encrypted with the tenant's key. A tenant can instead give a bucket in its own account. Manifests list file names and element metadata, so some tenants will want them in their own account.
- **Indexes.** Search, vector and identity indexes are per tenant: separate indexes on shared clusters for the shared tier, dedicated clusters above that.
- **Authorization.** The built-in adapter keeps relationships in the instance's database. A tenant can instead connect its own SpiceDB or OpenFGA service, self-hosted or managed, through the authorization port.

## Fairness and limits

- **Separate queues.** Bulk ingest, resolution plans and maintenance have a queue for each tenant, so one tenant's large load does not delay the others.
- **Limits.** Each tier has limits on registration rate, concurrent resolution plans and API requests. The limits are published.
- **Large plans.** Large resolution plans run as asynchronous jobs on workers that scale with demand.

## Operations

- **Upgrades:** online schema migrations, applied tenant by tenant, with canary tenants first.
- **Backups:** point-in-time recovery for each instance database. Restore tests run on a schedule.
- **Audit:** each tenant can stream its audit log to its own security tools.
- **Exit:** a tenant can export a full instance at any time. The export contains a database dump in the format of the open-source release, the manifests and a JSON-LD export. A tenant can thus move to self-hosting without loss.

## What to charge for

The vendor does not hold the data, so the price should not depend on data volume. Possible units:

- catalog entities (versions and replicas) under management;
- elements indexed through manifests;
- resolutions and API requests;
- connected site agents;
- dedicated capacity in the higher tiers.

## Risks

- **Trust in credential vending.** The delegated-role mode gives the vendor a path to customer bytes. The credential broker removes this path, but adds a component that the customer must run.
- **Latency for HPC.** HPC jobs that resolve names at start-up depend on the control plane's availability. Resolved manifests and credentials that jobs can cache for their lifetime reduce this dependency.
- **Enclaves.** Some enclaves permit no outbound connection. Their agents need an approved relay, or the enclave runs a self-hosted instance that federates with the hosted instances.
- **Many databases.** One database for each instance means many databases to operate. Shared clusters and automation keep this practical, but it needs investment early.
