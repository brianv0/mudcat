# mudcat as a hosted service

Status: sketch. This document describes how mudcat can run as a cloud service (software as a service). The components are those of [architecture.md](architecture.md). The requirements are OP-7 to OP-11 and RD-5 in [requirements.md](requirements.md).

## The main idea: a control plane and a data plane

mudcat is already outside the read path, so it splits well into two planes:

- **Control plane.** The vendor runs it. It holds metadata, provenance, policy and access decisions. It never reads or stores customer data bytes.
- **Data plane.** It runs in the customer's environment: their cloud accounts, HPC centres and enclaves. It holds the bytes, and everything that must read them.

| Plane | Components |
|---|---|
| Control plane (vendor) | API gateway, catalog API, query compiler, resolver, policy engine, pinning service, authorization port, ingest writer, catalog database, background workers, gateways, hosted MCP server, web UI |
| Data plane (customer) | Site agents, format plugins, movers, compaction compute, and optionally a credential broker |

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
  services: "Catalog services\ncatalog API · query · resolver · policy · pinning · authorization port · ingest writer"
  tenants: "Tenant catalogs\none PostgreSQL database for each zone" {
    shape: cylinder
  }
  workers: "Background workers\nindexes · lineage · maintenance · placement · deletion"
  front -> services
  services -> tenants
  tenants -> workers: change log
}

customer: Customer data plane {
  agents: "Site agents\nformat plugins · verification · inventory · deletion"
  broker: "Credential broker (optional)\nissues credentials inside the customer account"
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
```

## Connections

- **Site agents connect out.** An agent opens an outbound HTTPS connection to the control plane and leases its tasks. The customer opens no inbound port. This is the model of self-hosted CI runners and workflow agents.
- **Clients read data directly.** A client gets a resolved manifest and credentials from the control plane, then reads from storage. Data bytes never pass through the vendor.
- **Results come back as metadata.** Agents send structure descriptors, metadata, fingerprints, checksums and receipts through the ingest writer.

## Credentials for storage

The resolver must give clients credentials, but the customer must keep control of their storage. There are three modes, and each zone selects one:

| Mode | How the resolver gets credentials | Vendor access to bytes |
|---|---|---|
| **Delegated role** | The customer gives the control plane a role in their cloud account. The role is limited to named buckets and prefixes. The resolver uses the cloud provider's federation to get short-lived credentials, scoped to one ACL root at a time | Possible, but limited to the role, short-lived and visible in the customer's cloud audit log |
| **Credential broker** | A small broker runs in the customer's account. The resolver asks the broker for scoped credentials or signed URLs, and passes them to the client | None. The vendor never holds credentials for customer storage |
| **Agent-signed URLs** | For HPC and on-premises storage, the site agent signs URLs or issues tokens for the storage it serves | None |

The **credential broker** mode is the "no data access" mode. Regulated customers will usually need it.

## Tenancy

A **tenant** is a customer organisation. A tenant has one or more zones. The zone is the unit of isolation, because the model already uses zones as the unit of authority and federation.

| Tier | Isolation | For |
|---|---|---|
| Shared | One PostgreSQL database for each zone, on shared database clusters; shared stateless services | Small teams, trials |
| Dedicated | A dedicated database cluster and dedicated workers for the tenant, in the vendor's account | Large or regulated tenants |
| Customer cloud (BYOC) | The vendor operates the control plane inside the customer's own cloud account | Customers who must keep metadata in their own account |
| Self-hosted | The customer runs the open-source release | Facilities with their own operations staff |

The same code runs in all four tiers. Only the deployment differs.

A database for each zone has these advantages over rows of several tenants in one database:

- **Recovery:** backup, point-in-time recovery and restore apply to one tenant.
- **Migrations:** schema migrations can be applied to one tenant at a time, with canaries.
- **Deletion:** when a tenant leaves, its database is deleted, and nothing remains in shared tables.
- **Keys:** customer-managed encryption keys apply to one database.
- **Limits:** a tenant's rows cannot appear in another tenant's queries, even if the application has an error.

Very small zones can share a database, with one schema for each zone. Larger tenants move to their own database without a change to the application.

## Regions and residency

- **One region for each zone.** Each zone is pinned to one region of the control plane. Metadata can be sensitive (file names, sources, consent status), so metadata residency matters as much as data residency.
- **Regional control planes.** The vendor runs one control plane in each region it offers. A tenant with zones in several regions links them through the normal federation feed, as independent zones.
- **Global data.** Only account data (tenants, billing, the list of zones and their regions) is global.

## Derived state

- **Manifests.** By default, the control plane stores manifests in a per-tenant store, encrypted with the tenant's key. A tenant can instead give a bucket in its own account. Manifests list file names and element metadata, so some tenants will want them in their own account.
- **Indexes.** Search, vector and identity indexes are per tenant: separate indexes on shared clusters for the shared tier, dedicated clusters above that.
- **Authorization.** The built-in adapter keeps relationships in the zone's database. A tenant can instead connect its own SpiceDB or OpenFGA service, self-hosted or managed, through the authorization port.

## Fairness and limits

- **Separate queues.** Bulk ingest, resolution plans and maintenance have a queue for each tenant, so one tenant's large load does not delay the others.
- **Limits.** Each tier has limits on registration rate, concurrent resolution plans and API requests. The limits are published.
- **Large plans.** Large resolution plans run as asynchronous jobs on workers that scale with demand.

## Operations

- **Upgrades:** online schema migrations, applied tenant by tenant, with canary tenants first.
- **Backups:** point-in-time recovery for each zone database. Restore tests run on a schedule.
- **Audit:** each tenant can stream its audit log to its own security tools.
- **Exit:** a tenant can export a full zone at any time. The export contains a database dump in the format of the open-source release, the manifests and a JSON-LD export. A tenant can thus move to self-hosting without loss.

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
- **Enclaves.** Some enclaves permit no outbound connection. Their agents need an approved relay, or the enclave runs a self-hosted zone that federates with the hosted zones.
- **Many databases.** One database for each zone means many databases to operate. Shared clusters and automation keep this practical, but it needs investment early.
