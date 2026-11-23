# mudcat architecture

Status: sketch. This document describes the components, where state lives and the main flows. [model.md](model.md) defines the entities that this document refers to. Requirements are in [requirements.md](requirements.md), and this document describes how the design meets them. Field-specific semantics, formats, gateways and examples are in [fields.md](fields.md).

## Requirements that shape the architecture

| Requirement | Consequence | Requirement IDs |
|---|---|---|
| 10^4–10^9 catalog entities, up to ~10^11 elements | Elements live in columnar manifests, not in the catalog database | PF-1, PF-2 |
| Data in several clouds, HPC centres and enclaves | Federated zones; storage resources with jurisdiction and egress attributes; site agents near the data | RD-4, PL-2, PL-3, PL-4 |
| Training reads at GPU-cluster rates | The catalog never proxies reads; it issues resolved manifests and credentials once per job | RD-1, RD-2, RD-3 |
| Small teams must be able to run it | PostgreSQL is the only necessary dependency; every other index is optional and rebuildable | OP-1, OP-2, AC-2 |
| Mixed data structures | Format plugins produce structure descriptors; selectors address parts without enumeration | ST-1, ST-4, ST-5 |
| Usage policy, takedowns, legal holds | A fixed set of enforcement points; deletion as a tracked workflow | UP-1 to UP-8 |
| Organisations already run Zanzibar-style authorization services | Access checks go through an authorization port with built-in, SpiceDB and OpenFGA adapters | AC-1, AC-3, AC-4 |
| Bulk registration from many workers | A batched ingest writer; workers never write rows one at a time | IN-3, PF-3, OP-4, OP-5 |

## Planes

| Plane | Holds | Technology | Source of truth? |
|---|---|---|---|
| Catalog database | Zones, collections, data objects, versions, replicas, storage resources, datasets, snapshots, sources, policies, specifications, activities and their qualified relations, ACLs | PostgreSQL | Yes |
| Manifests and run records | Element lists, annotation sets, run-record bundles | Parquet or Lance files in object storage, each registered as a version | Yes, and immutable |
| Derived indexes | Full-text and faceted search, vector search, content-identity index, similarity tables, effective-policy cache | PostgreSQL full-text search and pgvector by default; OpenSearch and columnar index files optionally | No; rebuildable |
| Data | The bytes | Object storage, HPC file systems, tape | Outside mudcat |

No class of state may exist only in a derived index. DataHub keeps time-series aspects only in its search index, and its documentation admits that this makes recovery more difficult.

## Components

```
   Clients: Python SDK · CLI · web UI · data loaders · notebooks · agents (MCP)
                                   │
          ┌────────────────────────┼─────────────────────────────────────┐
          │                  API gateway (OIDC, tokens)                  │
          │                                                              │
          │  Catalog API ─ Query compiler ─ Resolver ─ Policy engine     │
          │       │                              │           │           │
          │  Ingest writer (batched)             │     Pinning service   │
          │       │                              │           │           │
          │  ┌────▼──────────────────────────────▼───────────▼────────┐  │
          │  │                 PostgreSQL catalog                     │  │
          │  │      entities · relations · ACLs · policies · outbox   │  │
          │  └────┬───────────────────────────────────────────────────┘  │
          │       │ change log (CloudEvents)                             │
          │  ┌────▼──────────┬───────────────┬────────────┬───────────┐  │
          │  │ Index builders│ Lineage       │ Placement  │ Deletion  │  │
          │  │ search, vector│ ingest        │ reconciler │ workflow  │  │
          │  │ identity      │ (OpenLineage, │            │           │  │
          │  │               │ MLflow, …)    │            │           │  │
          │  └───────────────┴───────────────┴─────┬──────┴─────┬─────┘  │
          │                                        │            │        │
          │  Gateways: Iceberg REST · DCAT/Croissant/RO-Crate export ·   │
          │            MCP server · fsspec · federation feed ·           │
          │            field gateways (fields.md)                        │
          └────────────────────────────────────────┼────────────┼────────┘
                                                   │ tasks      │
                     ┌─────────────────────────────▼────────────▼─────────┐
                     │ Site agents (one or more per storage resource)     │
                     │ extract · fingerprint · verify · inventory ·       │
                     │ delete with receipts · invoke movers               │
                     └───────────────┬────────────────────────────────────┘
                                     │
                 Buckets · HPC file systems · tape · enclaves
                 Movers: FTS3 · Globus · rclone · cloud replication · Pelican
```

### Catalog API

The catalog API is a stateless service with these interfaces:

- **REST interface:** it has an OpenAPI description and uses JSON by default. Content negotiation gives JSON-LD with the published context. Paging uses cursors with a stable default order, and counts are estimates. To count results, Datacat's REST layer iterated over the full result set. pgstac documents the same failure mode.
- **Bulk interface** for registration, manifest submission and resolution: Apache Arrow over HTTP or Arrow Flight. It handles millions of rows per call.
- **Event stream** of catalog changes, for consumers that need it.

### Query compiler

The query language extends Datacat's, for example `size > 6200 and status =~ "valid*"`, over path globs such as `/raw/**`.

It adds:

- set operations over datasets and snapshots, named queries, and joins through reference data (from MetaCat's MQL);
- lineage operators: `ancestors()`, `descendants()`, `derived_from(<query>)`, `used_in(<model>, role=training)`;
- policy predicates: `permitted(train, purpose=commercial)`.

Queries compile to SQL against the catalog database. Lineage operators use recursive queries over relation tables. By default, they exclude ubiquitous roles. The query compiler pushes element-level predicates down to manifest scans. A query engine such as DuckDB, DataFusion or Trino runs these scans, not PostgreSQL.

### Ingest writer

All writes from bulk clients and site agents go through a queue to a batched writer. This writer commits in large transactions. Before the Butler adopted this pattern, it measured inserts from hundreds of concurrent workers that took up to ten minutes.

- **Idempotency:** the key for registration is (data object alias, content checksum), so retries are harmless.
- **Ordering:** registration commits catalog rows before a producer may delete its staged copy. This avoids the Butler's orphaned-file problem in reverse.

Interactive single writes bypass the queue but use the same transaction code.

### Catalog database

The catalog database is PostgreSQL with one schema per model area: namespace and storage, structure, metadata, datasets, policy, provenance, placement.

- **Identifiers:** UUIDv7 primary keys for every entity, versions and snapshots included. They keep index locality for time-ordered inserts (the Butler also made this change). The embedded timestamp is not a privacy risk, because the access model controls who can see an identifier. A separate table holds aliases, with a unique index per scheme.
- **Paths:** the catalog stores paths as materialised paths in `ltree`, so it evaluates path globs and ACL inheritance in SQL. This removes Datacat's Java tree walk and its temporary tables.
- **Metadata:** the catalog stores metadata as JSONB documents. It promotes declared attributes to typed, indexed columns. GIN indexes serve equality and containment queries. Range queries and sorts need the promoted columns.
- **Partitioning:** from the first schema version, the catalog partitions large tables (versions, replicas, relations) by zone and namespace.
- **Outbox:** every transaction appends its changes to an outbox table. A relay publishes these changes as CloudEvents.

mudcat produces RDF from this schema through a declared mapping and the JSON-LD context. A triple store is optional, and mudcat uses it only for export.

### Format plugins and site agents

**Format plugins** read a version's header or index and produce:

- a structure descriptor;
- extracted metadata;
- element lists, where the structure enumerates elements;
- fingerprints.

Initial plugins: Parquet, Lance, CSV with CSVW, HDF5, Zarr, netCDF, tar and zip, MCAP, common video and audio containers, safetensors, GGUF. Field profiles in [fields.md](fields.md) add plugins for field formats. Each plugin has a version, and mudcat records this version with every descriptor.

**Site agents** run close to a storage resource and lease work from the catalog. They generalise Datacat's crawler and its scan-status contract. Leases expire, so the catalog reassigns the work of a failed agent.

Agent tasks:

- **Registration** from bucket event notifications and storage inventories. Agents do not poll the storage.
- **Extraction and fingerprinting.**
- **Checksum verification,** on a policy-driven schedule. Where possible, agents use checksums that the storage provider already computed.
- **Deletion,** with a receipt per replica.
- **Transfers,** which agents hand to movers.

Agents report results through the ingest writer.

### Resolver

The resolver serves reads, but it is not in the data path:

- **Name resolution.** The resolver resolves names through views to one version and its best replica for the caller's compute location. It chooses among the verified replicas that jurisdiction and egress rules permit. It prefers the nearest replica, then the cheapest.
- **Job resolution.** For a training or analysis job, the resolver resolves a pinned specification and returns three things:
  - the **resolved manifest**, as Arrow files that list elements, selectors and replica URLs;
  - **credentials:** signed URLs, or vended credentials scoped to a prefix. The resolver issues prefix-scoped credentials only if the prefix is fully inside one ACL root that the caller can read. If not, the resolver issues signed URLs per object, in batches, as the job requests them. The resolver checks read permission on a snapshot once, at the snapshot. This check then covers all of the snapshot's members;
  - a **run token** that ties the job's reads to an activity record.
- **Observed capture.** The resolver logs the credentials that it issues under a run token. These logs give the `observed` usage relations.

### Policy engine and pinning service

There are five enforcement points: **registration**, **pinning**, **materialisation**, **export** and **deletion**. Everything else happens asynchronously from the change log. This follows the idea of the iRODS policy enforcement point, but without a general rule language in the request path.

- **Access control.** The policy engine delegates access control to the authorization port (next section).
- **Usage policy.** mudcat always evaluates usage policy itself, in SQL, over the usage grants, constraints and duties attached to sources. The **effective-policy cache** holds the accumulated duties per snapshot, representation and model. It is a derived index, and mudcat recomputes it after each change.
- **Pinning.** The pinning service:
  - resolves a specification to snapshot hashes;
  - applies exclusion lists;
  - checks that every source that contributes grants the requested action and purpose;
  - writes the pinned specification with its policy decision and, where the provider issues one, the authorization consistency token.

  The pinning service fails if the policy does not permit a necessary component.
- **Terms compiler.** The terms compiler compiles licence identifiers and ODRL documents into grants, constraints and duties. It queues anything that it cannot translate for review.

### Authorization port

Access checks go through one internal interface. Thus mudcat itself or an external Zanzibar-style service, self-hosted or managed, can serve them.

| Adapter | Backing service | Deployment |
|---|---|---|
| Built-in | Relationship tuples in the catalog's PostgreSQL, evaluated with recursive SQL | Development and lab profiles; no extra dependency |
| SpiceDB | SpiceDB, self-hosted or as AuthZed's managed service | Lab and multi-site |
| OpenFGA | OpenFGA, self-hosted or as Okta FGA (managed OpenFGA) | Lab and multi-site |
| Others | Any service that supports the same operations, for example Ory Keto (Ory Network) | Through the same adapter contract |
| Cedar (partial) | Cedar or Amazon Verified Permissions | Policy-based, not tuple-based; mudcat supplies entity data with each request. Supports checks, not reverse lookups |

Managed offerings change often. Before you choose one, confirm the current offerings and their API coverage.

**The interface** covers the operations that all Zanzibar-style services share:

| Operation | Purpose | SpiceDB | OpenFGA |
|---|---|---|---|
| Write and delete relationships, idempotently | Grants and structural sync | WriteRelationships | Write |
| Check one permission | Per-request authorization | CheckPermission | Check |
| Check many permissions | Bulk resolution, filters for results | CheckBulkPermissions | BatchCheck |
| List objects a subject can reach | Pre-filter for search | LookupResources | ListObjects |
| List subjects with a permission | Sharing UI, audit | LookupSubjects | ListUsers |
| Read relationships | Reconciliation, export | ReadRelationships | Read |
| Load and validate the schema | Deployment | WriteSchema | WriteAuthorizationModel |

**A neutral schema.** mudcat defines its authorization schema once, in a neutral form. Then it compiles this schema to each provider's schema language (the SpiceDB schema language, the OpenFGA modelling language, a Cedar schema with policies). The core schema uses only features that both main implementations support:

- direct relations;
- subject sets such as `group#member`;
- union, intersection and exclusion;
- the traversal of a relation to another object (SpiceDB arrows, OpenFGA tuple-to-userset);
- public wildcards;
- conditions written in CEL with request context (SpiceDB caveats, OpenFGA conditions), used for ticket expiry, embargo dates and use counts.

Only optional extensions may use provider-specific features, such as SpiceDB's native relationship expiry or its all-parents arrow. The core schema never uses them.

**Identifiers.** mudcat identifies objects as `type:uuid`, with no paths or names. Thus renames never touch the authorization service. Identifiers also stay within the length and character limits of the provider.

**Consistency of the two stores.**

- **Writes.** The catalog writes relationship changes to its outbox in the same transaction as the catalog change. A sync worker applies them to the external service idempotently. Where the provider returns a consistency token, the sync worker stores it.
- **Visibility of new objects.** Until the provider acknowledges the relationships of a new object, only its creator can see the object. Interactive API calls can wait for acknowledgement instead.
- **Revocation.** mudcat checks grant removals, takedowns and compartment changes with the provider's strongest consistency mode. In SpiceDB, this is a fully consistent or at-least-as-fresh request. In OpenFGA, it is higher consistency. This makes sure that a revocation applies to every check made after it.
- **Drift repair.** A reconciler periodically compares catalog-owned relationships with the provider's copy and repairs differences.

**Filters for large result sets.** Search never asks the provider to check every result.

1. The catalog asks for the small set of ACL roots and compartments that the subject can read. This lookup covers a few thousand objects at most. The catalog caches the set briefly and filters in SQL.
2. The catalog uses per-object checks only for objects that users share individually. It runs these checks as bulk checks.

Provider list operations have result limits, especially in OpenFGA. For this reason, the catalog computes the coarse set first.

**Failure behaviour.**

- **Fail closed.** If mudcat cannot reach the provider, it denies requests.
- **Bounded decision cache.** Cached decisions have a short time-to-live. mudcat never uses them for revocation-sensitive checks.
- **Audit.** mudcat writes every decision that it uses at an enforcement point to the audit table. Where a provider token exists, mudcat writes it with the decision.

**Conformance tests.** One suite of authorization scenarios runs against every adapter: inheritance through ACL roots, compartments, tickets, revocation ordering and lookups. mudcat supports an adapter only if it passes.

### Index builders

Consumers of the change log keep derived indexes current:

- **Search documents:** the metadata of each entity, plus a denormalised summary of its lineage and policy, for facets. The lineage summary gives the root sources, the pipeline and code version that produced the entity, and upstream licences.
- **Vector embeddings,** of descriptions and of dataset-level content summaries. Element-level embeddings are annotation sets, and external engines index them.
- **Content-identity index:** a map from fingerprint or domain key to (snapshot, element). Batch jobs build it as partitioned columnar files. It starts with exact checksums and domain keys. Near-duplicate fingerprints come later.
- **Similarity tables** for registered similarity criteria, versioned with the criterion.

Every index has a version, and mudcat can rebuild it online. The steps are: build a new index, write to both, verify, switch an alias, and retire the old one. DataHub's upgrade failures show that reindexing must be a tested subsystem from the start.

### Lineage ingest

Adapters translate external records into activities and qualified relations, and mark them `imported`:

- OpenLineage events from Spark, Airflow, dbt and Flink;
- MLflow and Weights & Biases runs;
- Nextflow and Snakemake provenance;
- Hugging Face revisions.

The adapters match external identifiers through aliases. If an adapter cannot match an event, the event creates a placeholder entity. mudcat reconciles this entity later.

### Placement reconciler

The reconciler evaluates replication rules against completeness records, as Rucio's rule engine does, with snapshots and representations as the unit:

- it requests transfers from pluggable movers: FTS3, Globus, rclone, cloud-provider replication and lifecycle rules, Pelican;
- it records each transfer as an activity;
- it updates completeness after site agents verify the result.

Where a cloud provider can enforce a rule natively, the reconciler configures and observes the provider's mechanism. It does not move the bytes itself.

### Deletion workflow

A takedown, licence expiry or retention deadline starts a workflow:

1. Check legal holds. A hold diverts the data that it matches to its retention compartment. It also stops deletion of that data.
2. Enumerate occurrences through the content-identity index and lineage: derived representations, annotation sets, caches, replicas.
3. Issue deletion tasks to site agents and collect a receipt per replica.
4. Mark affected snapshots non-materialisable and create redacted successors. Keep the original snapshot hashes.
5. Flag the models trained on affected data, with the number of affected elements. A person then makes a decision, and the workflow records it.

### Gateways

- **Iceberg REST catalog** for records-structured datasets. With it, Spark, Trino, DuckDB and PyIceberg can read these datasets by name. The gateway also vends credentials.
- **Field gateways:** optional gateways that a field profile defines, listed in [fields.md](fields.md). They generate service metadata from CSVW descriptions through community schema profiles that are registered as plugins.
- **Metadata export:** DCAT, Croissant 1.1, RO-Crate and DataCite, generated from the model for publication and citation.
- **AI governance export:** SPDX 3 AI and Dataset profiles, and training-content summaries generated from pinned specifications.
- **MCP server** for agents. It runs with the caller's identity and permissions. Its tools:
  - search;
  - describe;
  - resolve;
  - lineage;
  - policy check.
- **fsspec filesystem** (`mudcat://`) for notebooks and Python tools.
- **Federation feed:** signed CloudEvents that zones exchange. These events carry snapshot descriptions, lineage summaries, tombstones and completeness records. A zone that receives a tombstone runs its own deletion workflow.

### Clients

- **Python SDK and CLI** with parity. They cover all entity operations and bulk registration.
- **Run context** for declared provenance:

  ```python
  with mudcat.run(code="git:abc123", image="sha256:...") as run:
      train = run.open("specs/vision-mix@pinned:7f3a", role="training")
      ...
      run.publish("models/vit-l", files=checkpoint_dir)
  ```

- **Data loader integrations** that consume resolved manifests directly: PyTorch, Ray Data, Daft, Hugging Face `datasets`, xarray.
- **Web UI** for discovery and administration. It previews images, video, arrays and tables, and offers faceted filters and similarity search.

### Identity and authentication

- **Users** authenticate with OIDC.
- **Services** use OAuth client credentials.
- **Storage:** clients reach storage that needs certificate-based or token-based grid credentials through credentials that the resolver issues. Some sites need both kinds, so the resolver can issue both.
- **Audit:** mudcat records every catalog write, credential issue and export in an append-only audit table.

## Federate, delegate or replicate

Every piece of state needs one of three answers to the question of where it lives:

- **Replicate:** one authority owns the state and keeps copies of it elsewhere, for locality or availability.
- **Delegate:** another system is the authority for a function, and mudcat accepts its answers.
- **Federate:** several authorities each own their own state, and exchange descriptions under an agreement.

Two tests decide most cases:

1. **Who can say no?** If one authority controls the state, replicate it within that authority. Or delegate it to a specialist system that the authority runs or contracts. If several independent authorities each control part of it, federate.
2. **Is it immutable?** Anyone can copy immutable things (versions, published snapshots, pinned specifications, provenance records) anywhere without coordination. Mutable authoritative state (ACLs, grants, replica state, lifecycle status) has exactly one writer. Never replicate it with multiple writers.

### Replicate

Replication happens within one authority, which usually means one zone:

- **Bytes:** replicas of versions on several storage resources. Replication rules place them to put data near compute.
- **Catalog reads:** PostgreSQL read replicas in other regions. Writes go to the primary.
- **Derived indexes:** search, vector and identity indexes. mudcat rebuilds them from the change log wherever it needs them.
- **Structural relationships:** mudcat copies them into an external authorization service, but the catalog stays their owner.

Rucio's model, one catalog with many sites, is the pattern to follow inside one authority.

### Delegate

mudcat delegates to another system when that system is the authority by nature, or does the job better. A delegation needs three things:

- a defined contract;
- a failure mode, which is fail closed for security functions;
- a way to verify the answer afterwards, such as reconciliation or audit.

| Function | Delegated to | How mudcat verifies |
|---|---|---|
| Identity and group membership | OIDC identity provider; SCIM for groups | Token validation; membership snapshots in the audit log |
| Access checks | The authorization port: built-in, SpiceDB, OpenFGA | Conformance suite; relationship reconciliation |
| Movement of bytes | FTS3, Globus, rclone, Pelican, provider replication | Site agents verify checksums and update completeness |
| Replication inside a cloud | Provider replication and lifecycle rules | Inventory reconciliation |
| Checksums | Storage-provided checksums where they exist | Periodic re-verification on a policy schedule |
| Internal file lists of tables | Iceberg, Delta, Lance and Icechunk metadata | The catalog pins a table snapshot identifier; it does not copy the file list |
| Query execution over manifests | DuckDB, DataFusion, Trino | Results are traceable to pinned manifests |
| Run tracking | MLflow, Weights & Biases, workflow engines | Lineage ingest matches identifiers through aliases |
| Training inside an enclave, or federated learning at a partner | The enclave or partner | mudcat records the activity with its location and inputs; obligations attach to the model that the activity produces |

mudcat never delegates usage-policy decisions: the evaluation at pinning, and the propagation of duties. These are the system's purpose. They depend on lineage that only the catalog holds.

### Federate

mudcat uses federation where several authorities will not, or may not, transfer control:

- separate organisations;
- legal jurisdictions;
- enclaves;
- commercial partners;
- national facilities in a collaboration.

Each authority runs its own zone with its own namespace, identifiers, access control and policy.

Between zones:

- **Zones exchange immutable descriptions,** never mutable state: published snapshot descriptions, lineage summaries, completeness records and tombstones, over the signed federation feed. A zone may harvest other zones' published descriptions into its own search index, much as DCAT harvesters do. This copies descriptions, not authority.
- **References cross zones by IRI.** A pinned specification in zone A can name a snapshot in zone B. To resolve it, the resolver asks zone B. Zone B applies its own access and usage policy.
- **Identity federates, but authorization does not.** Zone B can accept zone A's users through OIDC federation, but the zone that owns the data makes every decision about it.
- **Agreements are explicit.** Data or descriptions flow only under an agreement between zones. The zones record each agreement as an ODRL offer and acceptance.
  - A zone that accepts copies of data under an agreement also accepts the obligations attached to them.
  - A zone honours tombstones automatically for copies that it received under an agreement that makes this necessary.
  - The zone queues other tombstones for review.
- **Data may never move at all.** An enclave zone may publish descriptions and lineage, but forbid any replica outside it. The enclave zone registers the models trained inside it, with its obligations attached.

### Shared custody: one dataset, several sovereign custodians

Some collaborations sit between the two cases. An example is an international collaboration in which several institutions each hold and serve copies of one data release:

- The collaboration produces one logical dataset, such as a data release.
- Several institutions in different countries each hold, process and serve copies. Each institution wants ownership of its copies for reasons of sovereignty. These reasons include national law, the requirements of the agencies that fund it, and control over its own users and storage.
- Neither single-authority replication nor loose federation fits:
  - a single catalog would remove sovereignty from the custodians;
  - independent catalogs lose the fact that it is one dataset. The Butler team has a draft diagnosis of its multi-site problems. It says that collection structure and metadata corrections do not propagate, and that there is no completeness check per site.

mudcat treats this as federation with a shared namespace. It splits authority by kind of state, not by site:

| State | Authority | Other zones |
|---|---|---|
| Definition of the logical dataset: data objects, published snapshots, corrections to metadata and reference data | The **producing zone** for each part. A part processed at a site is owned by that site's zone | Subscribe to it, and hold read-only copies of the descriptions |
| A release that combines parts produced at several sites | A **release snapshot** that lists the member snapshots from each producing zone, with their Merkle hashes. Each producer signs its members, and the release is co-signed | Verify it from the hashes alone |
| Replicas, replica state and completeness on a site's storage | The **custodian zone** that owns that storage | Receive completeness records, which together give the global answer to "where are the copies" |
| Access decisions for a site's users and copies | The **custodian zone** | — |
| Minimum terms of use, such as access rights and embargo periods | Set by the **publisher** in the agreement | Each custodian enforces them, and may add stricter local terms but not looser ones |
| Local additions: annotation sets, local metadata, derived products | The zone that created them | Shared if the agreement says so |

Three mechanisms make this work without shared mutable state:

- **Identifiers of the producer.** A zone that registers a replica of a version that another zone produced uses the UUIDv7 identifier that the producing zone minted. Registration is idempotent on (data object, content checksum), so independent registrations of the same content converge. The Butler reaches the same goal with deterministic UUIDs. mudcat reaches it because it carries the identifier of the producer.
- **Verifiable snapshot hashes.** Each snapshot carries a Merkle hash that anyone can verify. Thus anyone who holds the hashes can verify a release of parts that different zones own. No zone has to trust another zone's database.
- **The federation feed.** Corrections and tombstones flow from producers, and completeness flows from custodians, as signed events. A failure to apply one is visible, because each custodian publishes the snapshot hashes that it holds completely.

### Decision table

| State | Within one authority | Across authorities |
|---|---|---|
| Bytes | Replicate, by replication rules | Copies only under an agreement, which carries obligations; enclave data never leaves |
| Catalog records | Single primary with read replicas | Federate; exchange published, immutable descriptions. Under shared custody, the producing zone owns definitions and each custodian owns its replica records |
| Search and identity indexes | Derived and replicated freely | Each zone builds its own; it may include harvested descriptions |
| Provenance | Single primary; run records as catalogued files | Exchange summaries; detail stays with the zone that owns it |
| Identity | Delegate to the identity provider | Federate through OIDC |
| Access control | Delegate checks to the authorization port; the catalog owns structural relationships | Never shared; the zone that owns the data decides |
| Usage policy and duties | Evaluated by mudcat | Duties travel with copies under the agreement |
| Tombstones and holds | Deletion workflow | Propagated over the federation feed; honoured as the agreement specifies |
| Transfers | Delegate to movers | Each side's movers, under the agreement |

## Main flows

### Register data in place

1. A bucket event or inventory report reaches a site agent.
2. The agent runs the applicable format plugin. The plugin produces a structure descriptor, metadata and fingerprints, and an element list if the structure has one.
3. The agent submits a batch of versions and replicas to the ingest writer. If there is an element list, the batch also has a manifest version.
4. The writer commits the batch, and the outbox emits events.
5. Index builders update search and identity indexes.

### Publish a snapshot

1. A user assembles a working snapshot from versions and annotation sets.
2. On publish, the catalog validates member metadata against the dataset's schemas and computes the Merkle hash. The snapshot becomes immutable.
3. Optionally, the catalog mints a DOI and exports Croissant and DCAT descriptions.

### Train a model

1. A user submits a specification.
2. The pinning service resolves it to snapshot hashes, applies exclusion lists and evaluates usage policy. Then it writes the pinned specification with its policy decision.
3. A suitable representation may not exist on a storage resource near the target cluster. In that case, mudcat creates a materialisation and a replication rule. The reconciler completes them.
4. The job gets a resolved manifest, credentials and a run token from the resolver. Then it reads storage directly.
5. At the end, the job publishes checkpoints as model versions. The catalog records the training activity with `observed` and `declared` relations, and the full run record as a bundle.

### Evaluate a model

1. An evaluation activity uses a model and an evaluation suite.
2. Before the catalog accepts results, it checks the suite's similarity criteria against the model's pinned training specification. It records a leakage assertion either way.

### Take down a source

This is the deletion workflow above. A tombstone on a source or element starts it.

## Deployment profiles

| Profile | Components | Use |
|---|---|---|
| Development | Single process with PostgreSQL in a container; plugins run in-process | Laptops, CI, tests |
| Lab | Catalog API, ingest writer and index builders on one node, PostgreSQL, one site agent per storage resource | AI-for-science startups and single-site groups |
| Multi-site | One zone per administrative domain, PostgreSQL with read replicas, agents at every storage resource, optional OpenSearch, federation feeds between zones | Collaborations, multi-cloud labs, national facilities |

A single PostgreSQL primary is a single-region write dependency. Multi-region deployments either accept that and use read replicas elsewhere, or run one zone per region and federate. This document proposes no multi-primary design.

## Scale envelope

| Quantity | Target | Notes |
|---|---|---|
| Catalog entities | 10^9 (TBR) versions and replicas per zone | PF-1. Postgres-first catalogs (MetaCat, pgstac) report 10^8; 10^9 needs tests |
| Elements | 10^11 (TBR) per zone | PF-2. In manifests; never rows in PostgreSQL |
| Registration rate | TBD versions per minute through bulk ingest; design estimate 10^5–10^6 | PF-3. To be measured |
| Metadata query | First page in less than TBD seconds over 10^8 (TBR) versions | PF-4 |
| Snapshots | 10^6 | |
| Lineage relations | 10^9 at version and snapshot level | Element-level lineage stays in manifests |

## Technology choices (proposal)

- **Catalog server:** Rust or Go, for a single static binary with predictable memory under bulk loads.
- **SDK and format plugins:** Python, because the scientific format libraries (h5py, zarr, pyarrow) are there. Plugins run inside site agents.
- **Catalog database:** PostgreSQL 16 or later, with `ltree` and `pgvector`.
- **Manifests:** Parquet, with Lance where random access is necessary.
- **Query engine for manifest scans:** DuckDB embedded, with DataFusion or Trino for large scans.
- **Authorization:** the built-in adapter by default; SpiceDB or OpenFGA (self-hosted or managed) through the authorization port.
- **Events:** CloudEvents over the outbox relay. A Kafka or NATS bridge only where a site already runs one.

## Build order

1. **Core catalog.** Namespace, data objects, versions, replicas, structure descriptors with the CSVW model, typed metadata and the query language. This step also includes bulk ingest, the Python SDK and CLI, and an importer from current Datacat databases.
2. **Snapshots and provenance.** Datasets, snapshots, annotation sets, PROV relations, the run context, OpenLineage and MLflow ingest, and the change log.
3. **Specifications and policy.** Sources, usage grants and duties, the terms compiler and pinning. This step also includes the authorization port with the built-in adapter, resolved manifests and credentials, and data loader integrations. SpiceDB and OpenFGA adapters follow after the conformance suite exists.
4. **Identity and evaluation.** The content-identity index (exact checksums first), similarity criteria, leakage assertions, and the deletion workflow.
5. **Placement and federation.** Replication rules, the reconciler, movers, completeness, and federation feeds.
6. **Gateways.** Iceberg REST, field gateways, Croissant, DCAT and RO-Crate export, and the MCP server, added as users request them. MCP is cheap and can come earlier.

Each step is useful without the next. The first two together are already a better Datacat.

## Open questions

- **Bulk wire format:** Arrow Flight, or Arrow IPC over plain HTTP. Flight is faster but harder to put behind ordinary gateways.
- **Single-row writes:** if they should also go through the ingest queue. This gives simpler consistency, but at the cost of latency.
- **Element-level policy:** how far it should go beyond exclusion lists.
- **Federation trust:** how zones authenticate one another, and which agreement terms are necessary before a zone honours received tombstones automatically.
- **Authorization source of truth:** if the external service should own grants when a deployment has one, instead of mudcat as owner with a synchronised copy.
- **Embedded mode:** if an embedded SQLite mode is worth its maintenance cost next to PostgreSQL.
