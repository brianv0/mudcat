# mudcat architecture

Status: sketch. This document describes the components, where state lives and the main flows. The entities referred to here are defined in [model.md](model.md).

## Requirements that shape the architecture

| Requirement | Consequence |
|---|---|
| 10^4–10^9 catalog entities, up to ~10^11 elements | Elements live in columnar manifests, not in the catalog database |
| Data in several clouds, HPC centres and enclaves | Federated zones; storage resources with jurisdiction and egress attributes; site agents near the data |
| Training reads at GPU-cluster rates | The catalog never proxies reads; it issues resolved manifests and credentials once per job |
| Small teams must be able to run it | PostgreSQL is the only required dependency; every other index is optional and rebuildable |
| Mixed data structures | Format plugins produce structure descriptors; selectors address parts without enumeration |
| Usage policy, takedowns, legal holds | A fixed set of enforcement points; deletion as a tracked workflow |
| Organisations already run Zanzibar-style authorization services | Access checks go through an authorization port with built-in, SpiceDB and OpenFGA adapters |
| Bulk registration from many workers | A batched ingest writer; workers never write rows one at a time |

## Planes

| Plane | Holds | Technology | Source of truth? |
|---|---|---|---|
| Catalog database | Zones, collections, data objects, versions, replicas, storage resources, datasets, snapshots, sources, policies, specifications, activities and their qualified relations, ACLs | PostgreSQL | Yes |
| Manifests and run records | Element listings, annotation sets, run-record bundles | Parquet or Lance files in object storage, each registered as a version | Yes, and immutable |
| Derived indexes | Full-text and faceted search, vector search, content-identity index, similarity tables, effective-policy cache | PostgreSQL full-text search and pgvector by default; OpenSearch and columnar index files optionally | No; rebuildable |
| Data | The bytes | Object storage, HPC file systems, tape | Outside mudcat |

No class of state may exist only in a derived index. DataHub keeps time-series aspects only in its search index, and its documentation concedes that this complicates recovery.

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
          │  Gateways: Iceberg REST · IVOA TAP · DCAT/Croissant/RO-Crate │
          │            export · MCP server · fsspec · federation feed    │
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

A stateless service with these interfaces:

- **REST interface:** OpenAPI-described, JSON by default, with JSON-LD available by content negotiation using the published context. Paging uses cursors with a stable default order, and counts are estimates. Datacat's REST layer iterated the full result set to count, and pgstac documents the same failure mode.
- **Bulk interface** for registration, manifest submission and resolution: Apache Arrow over HTTP or Arrow Flight. It handles millions of rows per call.
- **Event stream** of catalog changes, for consumers that need it.

### Query compiler

The query language extends Datacat's, for example `run > 6200 and quality =~ "GO*"`, over path globs such as `/raw/**`.

It adds:

- set operations over datasets and snapshots, named queries, and joins through reference data (from MetaCat's MQL);
- lineage operators: `ancestors()`, `descendants()`, `derived_from(<query>)`, `used_in(<model>, role=training)`;
- policy predicates: `permitted(train, purpose=commercial)`.

Queries compile to SQL against the catalog database. Lineage operators use recursive queries over relation tables, with ubiquitous roles excluded by default. Element-level predicates are pushed down to manifest scans and run by a query engine such as DuckDB, DataFusion or Trino, not by PostgreSQL.

### Ingest writer

All writes from bulk clients and site agents go through a queue to a batching writer that commits in large transactions. The Butler measured inserts from hundreds of concurrent workers taking up to ten minutes before it adopted this pattern.

- **Idempotency:** registration is keyed by (data object alias, content checksum), so retries are harmless.
- **Ordering:** registration commits catalog rows before a producer may delete its staging copy. This avoids the Butler's orphaned-file problem in reverse.

Interactive single writes bypass the queue but use the same transaction code.

### Catalog database

PostgreSQL with one schema per model area: namespace and storage, structure, metadata, datasets, policy, provenance, placement.

- **Identifiers:** UUIDv7 primary keys, which keep index locality for time-ordered inserts (also a Butler change). Aliases are kept in a separate table with a unique index per scheme.
- **Paths:** stored as materialised paths in `ltree`, so path globs and ACL inheritance are evaluated in SQL. This removes Datacat's Java tree walk and its temporary tables.
- **Metadata:** stored as JSONB documents. Declared attributes are promoted to typed, indexed columns. GIN indexes serve equality and containment queries; range queries and sorts need the promoted columns.
- **Partitioning:** large tables (versions, replicas, relations) are partitioned by zone and namespace from the first schema version.
- **Outbox:** every transaction appends its changes to an outbox table, which a relay publishes as CloudEvents.

RDF is produced from this schema through a declared mapping and the JSON-LD context. A triple store is optional and is used only for export.

### Format plugins and site agents

**Format plugins** read a version's header or index and produce:

- a structure descriptor;
- extracted metadata;
- element listings, where the structure enumerates elements;
- fingerprints.

Initial plugins: Parquet, Lance, CSV with CSVW, HDF5, Zarr, netCDF, FITS, ROOT, tar and zip, MCAP, common video and audio containers, safetensors, GGUF, mmCIF, AnnData. Plugins are versioned, and their version is recorded with every descriptor.

**Site agents** run close to a storage resource and lease work from the catalog, generalising Datacat's crawler and its scan-status contract. Leases expire, so a failed agent's work is reassigned.

Agent tasks:

- **Registration** from bucket event notifications and storage inventories, instead of polling.
- **Extraction and fingerprinting.**
- **Checksum verification,** on a policy-driven schedule, using checksums the storage provider has already computed where possible.
- **Deletion,** with a receipt per replica.
- **Transfers,** which are handed to movers.

Agents report results through the ingest writer.

### Resolver

The resolver serves reads without being in the data path:

- **Name resolution.** Resolves names through views to one version and its best replica for the caller's compute location: verified, nearest, cheapest, and permitted by jurisdiction and egress rules.
- **Job resolution.** For a training or analysis job, resolves a pinned specification and returns three things:
  - the **resolved manifest**, as Arrow files listing elements, selectors and replica URLs;
  - **credentials:** signed URLs, or vended credentials scoped to a prefix. Prefix-scoped credentials are issued only when the prefix lies entirely inside one ACL root the caller can read; otherwise the resolver issues signed URLs per object, in batches, as the job requests them. Read permission on a snapshot is checked once, at the snapshot, and then covers all of its members;
  - a **run token** that ties the job's reads to an activity record.
- **Observed capture.** Credentials issued under a run token are logged, which yields the `observed` usage relations.

### Policy engine and pinning service

There are five enforcement points: **registration**, **pinning**, **materialisation**, **export** and **deletion**. Everything else happens asynchronously from the change log. This follows the iRODS policy enforcement point idea, without a general rule language in the request path.

- **Access control.** Delegated to the authorization port (next section).
- **Usage policy.** Always evaluated by mudcat, in SQL, over the usage grants, constraints and duties attached to sources. The **effective-policy cache** holds the accumulated duties per snapshot, representation and model. It is a derived index, recomputed on change.
- **Pinning.** The pinning service:
  - resolves a specification to snapshot hashes;
  - applies exclusion lists;
  - checks that every contributing source grants the requested action and purpose;
  - writes the pinned specification with its policy decision, including the authorization consistency token where the provider issues one.

  It fails if a required component is not permitted.
- **Terms compiler.** Compiles licence identifiers and ODRL documents into grants, constraints and duties, and queues anything it cannot translate for review.

### Authorization port

Access checks go through one internal interface, so they can be served by mudcat itself or by an external Zanzibar-style service, self-hosted or managed.

| Adapter | Backing service | Deployment |
|---|---|---|
| Built-in | Relationship tuples in the catalog's PostgreSQL, evaluated with recursive SQL | Development and lab profiles; no extra dependency |
| SpiceDB | SpiceDB, self-hosted or as AuthZed's managed service | Lab and multi-site |
| OpenFGA | OpenFGA, self-hosted or as Okta FGA (managed OpenFGA) | Lab and multi-site |
| Others | Any service that supports the same operations, for example Ory Keto (Ory Network) | Through the same adapter contract |
| Cedar (partial) | Cedar or Amazon Verified Permissions | Policy-based, not tuple-based; mudcat supplies entity data with each request. Supports checks, not reverse lookups |

Managed offerings change often; confirm the current ones and their API coverage before choosing.

**The interface** covers the operations all Zanzibar-style services share:

| Operation | Purpose | SpiceDB | OpenFGA |
|---|---|---|---|
| Write and delete relationships, idempotently | Grants and structural sync | WriteRelationships | Write |
| Check one permission | Per-request authorization | CheckPermission | Check |
| Check many permissions | Bulk resolution, result filtering | CheckBulkPermissions | BatchCheck |
| List objects a subject can reach | Search pre-filtering | LookupResources | ListObjects |
| List subjects with a permission | Sharing UI, audit | LookupSubjects | ListUsers |
| Read relationships | Reconciliation, export | ReadRelationships | Read |
| Load and validate the schema | Deployment | WriteSchema | WriteAuthorizationModel |

**A neutral schema.** mudcat's authorization schema is written once in a neutral form and compiled to each provider's schema language (the SpiceDB schema language, the OpenFGA modelling language, a Cedar schema with policies). The core schema uses only features both main implementations support:

- direct relations;
- subject sets such as `group#member`;
- union, intersection and exclusion;
- following a relation to another object (SpiceDB arrows, OpenFGA tuple-to-userset);
- public wildcards;
- conditions written in CEL with request context (SpiceDB caveats, OpenFGA conditions), used for ticket expiry, embargo dates and use counts.

Provider-specific features, such as SpiceDB's native relationship expiry or its all-parents arrow, may be used only in optional extensions, never in the core schema.

**Identifiers.** Objects are identified as `type:uuid`, with no paths or names, so renames never touch the authorization service and identifiers stay within provider length and character limits.

**Keeping the two stores consistent.**

- **Writes.** Relationship changes are written to the catalog's outbox in the same transaction as the catalog change. A sync worker applies them to the external service idempotently and stores the provider's consistency token where one is returned.
- **Visibility of new objects.** Until a new object's relationships are acknowledged, it is visible only to its creator. Interactive API calls can instead wait for acknowledgement.
- **Revocation.** Grant removals, takedowns and compartment changes are checked with the provider's strongest consistency mode (a fully consistent or at-least-as-fresh request in SpiceDB, higher consistency in OpenFGA), so that a revocation applies to every check made after it.
- **Drift repair.** A reconciler periodically compares catalog-owned relationships with the provider's copy and repairs differences.

**Filtering large result sets.** Search never asks the provider to check every result.

1. The catalog asks for the small set of ACL roots and compartments the subject can read (a lookup over a few thousand objects at most), caches it briefly, and filters in SQL.
2. Per-object checks are used only for objects shared individually, and run as bulk checks.

Provider list operations have result limits, OpenFGA's especially, which is why the coarse set is computed first.

**Failure behaviour.**

- **Fail closed.** If the provider is unreachable, requests are denied.
- **Bounded decision cache.** Cached decisions have a short time-to-live and are never used for revocation-sensitive checks.
- **Audit.** Every decision used at an enforcement point is written to the audit table, with its provider token where one exists.

**Conformance tests.** One suite of authorization scenarios runs against every adapter: inheritance through ACL roots, compartments, tickets, revocation ordering and lookups. An adapter is supported only if it passes.

### Index builders

Consumers of the change log keep derived indexes current:

- **Search documents:** each entity's metadata, plus a denormalised summary of its lineage (root sources, producing pipeline and code version, upstream licences) and policy, used for facets.
- **Vector embeddings,** of descriptions and of dataset-level content summaries. Element-level embeddings are annotation sets and are indexed by external engines.
- **Content-identity index:** fingerprint or domain key mapped to (snapshot, element), held as partitioned columnar files and built by batch jobs. It starts with exact checksums and domain keys; near-duplicate fingerprints come later.
- **Similarity tables** for registered similarity criteria, versioned with the criterion.

Every index is versioned and can be rebuilt online: build a new index, write to both, verify, switch an alias, retire the old one. DataHub's upgrade failures show that reindexing must be a tested subsystem from the start.

### Lineage ingest

Adapters translate external records into activities and qualified relations, marked `imported`:

- OpenLineage events from Spark, Airflow, dbt and Flink;
- MLflow and Weights & Biases runs;
- Nextflow, Snakemake and AiiDA provenance;
- Hugging Face revisions.

External identifiers are matched through aliases. An event that cannot be matched creates a placeholder entity, which is reconciled later.

### Placement reconciler

The reconciler evaluates replication rules against completeness records, as Rucio's rule engine does, with snapshots and representations as the unit:

- it requests transfers from pluggable movers: FTS3, Globus, rclone, cloud-provider replication and lifecycle rules, Pelican;
- it records each transfer as an activity;
- it updates completeness once site agents have verified the result.

Where a cloud provider can enforce a rule natively, the reconciler configures and observes the provider's mechanism instead of moving bytes itself.

### Deletion workflow

A takedown, licence expiry or retention deadline starts a workflow:

1. Check legal holds. A hold diverts matching data to its retention compartment and stops deletion for it.
2. Enumerate occurrences through the content-identity index and lineage: derived representations, annotation sets, caches, replicas.
3. Issue deletion tasks to site agents and collect a receipt per replica.
4. Mark affected snapshots non-materialisable and create redacted successors. The original snapshot hashes are retained.
5. Flag models trained on affected data with the affected element count, for a recorded human decision.

### Gateways

- **Iceberg REST catalog** for records-structured datasets, so Spark, Trino, DuckDB and PyIceberg can read them by name, with credential vending.
- **IVOA TAP,** with TAP_SCHEMA generated from CSVW descriptions using the Felis profile.
- **Metadata export:** DCAT, Croissant 1.1, RO-Crate and DataCite, generated from the model for publication and citation.
- **AI governance export:** SPDX 3 AI and Dataset profiles, and training-content summaries generated from pinned specifications.
- **MCP server** for agents, executing with the caller's identity and permissions. Its tools:
  - search;
  - describe;
  - resolve;
  - lineage;
  - policy check.
- **fsspec filesystem** (`mudcat://`) for notebooks and Python tools.
- **Federation feed:** signed CloudEvents exchanged between zones, carrying snapshot descriptions, lineage summaries, tombstones and completeness records. A zone that receives a tombstone runs its own deletion workflow.

### Clients

- **Python SDK and CLI** with parity, covering all entity operations and bulk registration.
- **Run context** for declared provenance:

  ```python
  with mudcat.run(code="git:abc123", image="sha256:...") as run:
      train = run.open("specs/vision-mix@pinned:7f3a", role="training")
      ...
      run.publish("models/vit-l", files=checkpoint_dir)
  ```

- **Data loader integrations** that consume resolved manifests directly: PyTorch, Ray Data, Daft, Hugging Face `datasets`, xarray.
- **Web UI** for discovery and administration. It previews images, video, spectra, arrays and tables, and offers faceted filters and similarity search.

### Identity and authentication

- **Users** authenticate with OIDC.
- **Services** use OAuth client credentials.
- **Storage** that requires X.509 or WLCG and SciTokens profiles is reached through credentials the resolver issues. The WLCG move from X.509 to tokens has slipped to 2028, so both are needed.
- **Audit:** every catalog write, credential issue and export is written to an append-only audit table.

## Federate, delegate or replicate

Every piece of state needs one of three answers to the question of where it lives:

- **Replicate:** one authority owns the state and keeps copies of it elsewhere, for locality or availability.
- **Delegate:** another system is the authority for a function, and mudcat accepts its answers.
- **Federate:** several authorities each own their own state, and exchange descriptions under an agreement.

Two tests decide most cases:

1. **Who can say no?** If one authority controls the state, replicate it (within that authority) or delegate it (to a specialist system that authority runs or contracts). If several independent authorities each control part of it, federate.
2. **Is it immutable?** Immutable things (versions, published snapshots, pinned specifications, provenance records) can be copied anywhere without coordination. Mutable authoritative state (ACLs, grants, replica state, lifecycle status) has exactly one writer. It is never replicated with multiple writers.

### Replicate

Replication happens within one authority, which normally means one zone:

- **Bytes:** replicas of versions on several storage resources, placed by replication rules, to put data near compute.
- **Catalog reads:** PostgreSQL read replicas in other regions, with writes going to the primary.
- **Derived indexes:** search, vector and identity indexes, rebuilt from the change log wherever they are needed.
- **Structural relationships:** copied into an external authorization service, with the catalog remaining their owner.

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
| Moving bytes | FTS3, Globus, rclone, Pelican, provider replication | Site agents verify checksums and update completeness |
| Replication inside a cloud | Provider replication and lifecycle rules | Inventory reconciliation |
| Checksums | Storage-provided checksums where they exist | Periodic re-verification on a policy schedule |
| Internal file lists of tables | Iceberg, Delta, Lance and Icechunk metadata | The catalog pins a table snapshot identifier; it does not copy the file list |
| Query execution over manifests | DuckDB, DataFusion, Trino | Results are traceable to pinned manifests |
| Run tracking | MLflow, Weights & Biases, workflow engines | Lineage ingest matches identifiers through aliases |
| Training inside an enclave, or federated learning at a partner | The enclave or partner | The activity is recorded with its location and inputs; obligations attach to the resulting model |

mudcat never delegates usage-policy decisions: the evaluation at pinning, and the propagation of duties. These are the system's purpose. They depend on lineage that only the catalog holds.

### Federate

Federation is used where several authorities will not, or may not, cede control:

- separate organisations;
- legal jurisdictions;
- enclaves;
- commercial partners;
- national facilities in a collaboration.

Each authority runs its own zone with its own namespace, identifiers, access control and policy.

Between zones:

- **Immutable descriptions are exchanged,** never mutable state: published snapshot descriptions, lineage summaries, completeness records and tombstones, over the signed federation feed. A zone may harvest other zones' published descriptions into its own search index, much as DCAT harvesting does. This copies descriptions, not authority.
- **References cross zones by IRI.** A pinned specification in zone A can name a snapshot in zone B. Resolving it asks zone B, which applies its own access and usage policy.
- **Identity federates; authorization does not.** Zone B can accept zone A's users through OIDC federation, but the zone that owns the data makes every decision about it.
- **Agreements are explicit.** Data or descriptions flow only under an agreement between zones, recorded as an ODRL offer and acceptance.
  - A zone that accepts copies of data under an agreement takes on the obligations attached to them.
  - Tombstones are honoured automatically for copies received under agreements that require it.
  - Other tombstones are queued for review.
- **Data may never move at all.** An enclave zone may publish descriptions and lineage while forbidding any replica outside it. Models trained inside it are registered in it, with its obligations attached.

### Shared custody: one dataset, several sovereign custodians

Some collaborations sit between the two cases. Rubin is the main example:

- One logical dataset, such as a data release, is produced by the collaboration.
- Several institutions in different countries each hold, process and serve copies, and each wants ownership of its copies for sovereignty reasons: national law, funding-agency requirements, control over its own users and storage.
- Neither single-authority replication nor loose federation fits:
  - a single catalog would take sovereignty away from the custodians;
  - independent catalogs lose the fact that it is one dataset. The Butler team's draft diagnosis of its multi-site problems is that collection structure and metadata corrections do not propagate, and there is no completeness check per site.

mudcat treats this as federation with a shared namespace. Authority is split by kind of state, not by site:

| State | Authority | Other zones |
|---|---|---|
| Definition of the logical dataset: data objects, published snapshots, corrections to metadata and reference data | The **producing zone** for each part. A part processed at a site is owned by that site's zone | Subscribe to it, and hold read-only copies of the descriptions |
| A release that combines parts produced at several sites | A **release snapshot** whose members are the hashes of member snapshots from each producing zone. Each producer signs its members, and the release is co-signed | Verify it from the hashes alone |
| Replicas, replica state and completeness on a site's storage | The **custodian zone** that owns that storage | Receive completeness records, which together give the global answer to "where are the copies" |
| Access decisions for a site's users and copies | The **custodian zone** | — |
| Minimum terms of use, such as data rights and proprietary periods | Set by the **publisher** in the agreement | Each custodian enforces them, and may add stricter local terms but not looser ones |
| Local additions: annotation sets, local metadata, derived products | The zone that created them | Shared if the agreement says so |

Three mechanisms make this work without shared mutable state:

- **Content-derived identifiers.** A version's identifier is derived from its data object's identifier and its content checksum (model, Identity). Every zone that registers a copy therefore arrives at the same identifier, as the Butler does with deterministic UUIDs where sites must agree.
- **Hash-identified snapshots.** Snapshots are Merkle hashes, so a release made of parts owned by different zones can be verified by anyone holding the hashes, and no zone has to trust another zone's database.
- **The federation feed.** Corrections and tombstones flow from producers, and completeness flows from custodians, as signed events. A failure to apply one is visible, because each custodian publishes the snapshot hashes it holds completely.

### Decision table

| State | Within one authority | Across authorities |
|---|---|---|
| Bytes | Replicate, by replication rules | Copies only under an agreement, which carries obligations; enclave data never leaves |
| Catalog records | Single primary with read replicas | Federate; exchange published, immutable descriptions. Under shared custody, the producing zone owns definitions and each custodian owns its replica records |
| Search and identity indexes | Derived and replicated freely | Each zone builds its own; it may include harvested descriptions |
| Provenance | Single primary; run records as catalogued files | Exchange summaries; detail stays with the owning zone |
| Identity | Delegate to the identity provider | Federate through OIDC |
| Access control | Delegate checks to the authorization port; the catalog owns structural relationships | Never shared; the owning zone decides |
| Usage policy and duties | Evaluated by mudcat | Duties travel with copies under the agreement |
| Tombstones and holds | Deletion workflow | Propagated over the federation feed; honoured according to the agreement |
| Transfers | Delegate to movers | Each side's movers, under the agreement |

## Main flows

### Register data in place

1. A bucket event or inventory report reaches a site agent.
2. The agent runs the matching format plugin. The plugin produces a structure descriptor, metadata and fingerprints, and an element listing if the structure has one.
3. The agent submits a batch to the ingest writer: versions and replicas, with a manifest version if there is an element listing.
4. The writer commits the batch, and the outbox emits events.
5. Index builders update search and identity indexes.

### Publish a snapshot

1. A user assembles a working snapshot from versions and annotation sets.
2. On publish, the catalog validates member metadata against the dataset's schemas and computes the Merkle hash. The snapshot becomes immutable.
3. Optionally, a DOI is minted and Croissant and DCAT descriptions are exported.

### Train a model

1. A user submits a specification.
2. The pinning service resolves it to snapshot hashes, applies exclusion lists, evaluates usage policy, and writes the pinned specification together with its policy decision.
3. If no suitable representation exists on a storage resource near the target cluster, a materialisation and a replication rule are created, and the reconciler completes them.
4. The job obtains a resolved manifest, credentials and a run token from the resolver, then reads storage directly.
5. At the end, the job publishes checkpoints as model versions. The catalog records the training activity with `observed` and `declared` relations, and the full run record as a bundle.

### Evaluate a model

1. An evaluation activity uses a model and an evaluation suite.
2. Before results are accepted, the catalog checks the suite's similarity criteria against the model's pinned training specification. It records a leakage assertion either way.

### Take down a source

This is the deletion workflow above, started by a tombstone on a source or element.

## Deployment profiles

| Profile | Components | Use |
|---|---|---|
| Development | Single process with PostgreSQL in a container; plugins run in-process | Laptops, CI, tests |
| Lab | Catalog API, ingest writer and index builders on one node, PostgreSQL, one site agent per storage resource | AI-for-science startups and single-site groups |
| Multi-site | One zone per administrative domain, PostgreSQL with read replicas, agents at every storage resource, optional OpenSearch, federation feeds between zones | Collaborations, multi-cloud labs, national facilities |

A single PostgreSQL primary is a single-region write dependency. Multi-region deployments either accept that, using read replicas elsewhere, or run one zone per region and federate. No multi-primary design is proposed.

## Scale envelope

| Quantity | Target | Notes |
|---|---|---|
| Catalog entities | 10^9 versions and replicas per zone | Postgres-first catalogs (MetaCat, pgstac) report 10^8; 10^9 needs testing |
| Elements | 10^11 per zone | In manifests; never rows in PostgreSQL |
| Registration rate | 10^5–10^6 versions per minute through bulk ingest | To be measured |
| Snapshots | 10^6 | |
| Lineage relations | 10^9 at version and snapshot level | Element-level lineage stays in manifests |

## Technology choices (proposal)

- **Catalog server:** Rust or Go, for a single static binary with predictable memory under bulk loads.
- **SDK and format plugins:** Python, because the scientific format libraries (astropy, h5py, zarr, uproot, pyarrow) are there. Plugins run inside site agents.
- **Catalog database:** PostgreSQL 16 or later, with `ltree` and `pgvector`.
- **Manifests:** Parquet, with Lance where random access is needed.
- **Query engine for manifest scans:** DuckDB embedded, with DataFusion or Trino for large scans.
- **Authorization:** the built-in adapter by default; SpiceDB or OpenFGA (self-hosted or managed) through the authorization port.
- **Events:** CloudEvents over the outbox relay. A Kafka or NATS bridge only where a site already runs one.

## Build order

1. **Core catalog.** Namespace, data objects, versions, replicas, structure descriptors with the CSVW model, typed metadata, the query language, bulk ingest, the Python SDK and CLI, and an importer from existing Datacat databases.
2. **Snapshots and provenance.** Datasets, snapshots, annotation sets, PROV relations, the run context, OpenLineage and MLflow ingest, and the change log.
3. **Specifications and policy.** Sources, usage grants and duties, the terms compiler, pinning, the authorization port with the built-in adapter, resolved manifests and credentials, and data loader integrations. SpiceDB and OpenFGA adapters follow once the conformance suite exists.
4. **Identity and evaluation.** The content-identity index (exact checksums first), similarity criteria, leakage assertions, and the deletion workflow.
5. **Placement and federation.** Replication rules, the reconciler, movers, completeness, and federation feeds.
6. **Gateways.** Iceberg REST, TAP, Croissant, DCAT and RO-Crate export, and the MCP server, added as users ask for them. MCP is cheap and can come earlier.

Each step is useful without the next. The first two together are already a better Datacat.

## Open questions

- **Bulk wire format:** Arrow Flight, or Arrow IPC over plain HTTP. Flight is faster but harder to put behind ordinary gateways.
- **Single-row writes:** whether they should also go through the ingest queue, for simpler consistency, at the cost of latency.
- **Element-level policy:** how far it should go beyond exclusion lists.
- **Federation trust:** how zones authenticate one another, and what agreement terms are needed before received tombstones are honoured automatically.
- **Authorization source of truth:** whether grants should be owned by the external service when one is configured, instead of by mudcat with a synchronised copy.
- **Embedded mode:** whether an embedded SQLite mode is worth maintaining alongside PostgreSQL.
