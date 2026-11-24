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

```d2
vars: {
  d2-config: {
    layout-engine: elk
  }
}
direction: down

clients: "Clients\nPython SDK · CLI · web UI · data loaders · notebooks · agents (MCP)"

zone: mudcat zone {
  gateway: "API gateway (OIDC, tokens)"
  gateways: "Gateways\nIceberg REST · DCAT, Croissant, RO-Crate export · MCP server\nfsspec · federation feed · field gateways (fields.md)"

  services: Services {
    grid-columns: 4
    api: Catalog API
    query: Query compiler
    resolver: Resolver
    policy: Policy engine
    ingest: "Ingest writer\n(batched)"
    pinning: Pinning service
    authz: "Authorization port\n(built-in, or SpiceDB · OpenFGA)"
  }

  db: "PostgreSQL catalog\nentities · relations · ACLs · policies · outbox" {
    shape: cylinder
  }

  consumers: "Background workers (change log and schedules)" {
    grid-columns: 5
    index: "Index builders\nsearch · vector · identity"
    lineage: "Lineage ingest\nOpenLineage · MLflow · …"
    maintenance: "Catalog maintenance\nseal and merge manifests\nexpire snapshots"
    placement: Placement reconciler
    deletion: Deletion workflow
  }

  gateway -> services
  gateways -> services
  services -> db
  db -> consumers: change log (CloudEvents)
}

agents: "Site agents (one or more per storage resource)\nextract · fingerprint · verify · inventory · delete with receipts · invoke movers"

storage: "Storage\nbuckets · HPC file systems · tape · enclaves\nclients read it directly, with issued credentials"
movers: "Movers\nFTS3 · Globus · rclone · cloud replication · Pelican"

clients -> zone.gateway
zone.consumers -> agents: tasks
agents -> movers
agents -> storage
movers -> storage
```

### Catalog API

The catalog API is a stateless service with these interfaces:

- **REST interface:** it has an OpenAPI description and uses JSON by default. Content negotiation gives JSON-LD with the published context. Paging uses cursors with a stable default order, and counts are estimates. To count results, Datacat's REST layer iterated over the full result set. pgstac documents the same failure mode.
- **Bulk interface** for registration, manifest submission and resolution: Apache Arrow over HTTP or Arrow Flight. It handles millions of rows per call.
- **Event stream** of catalog changes, for consumers that need it.
- **Commits with requirements.** A request that changes a dataset has the shape of an Iceberg REST commit. It has a list of **requirements** and a list of **updates**. Iceberg calls a requirement such as `assert-ref-snapshot-id` an assertion about the current table state. In mudcat, a requirement such as "branch `main` points to snapshot S" goes with an update such as "add these members". mudcat applies all the updates in one transaction, but only if all the requirements are true. If not, it returns a conflict, and the client tries again. One request can change several datasets, in one transaction. This is optimistic concurrency.

### Commit cost

Iceberg commits are slow, usually seconds. Each commit writes new manifests, a new manifest list and a new metadata file to object storage before it swaps the pointer in the catalog. When two writers conflict, the loser writes these files again. Many small commits also make many small files and snapshots, which later need compaction and expiry. A REST catalog helps, because the server applies the updates, but the file writes stay.

mudcat keeps object-storage writes out of the commit path:

- **A commit is a database transaction.** It changes rows in the catalog database only. It writes no files.
- **Recent changes stay as rows.** The members that recent commits added or removed stay as rows in the catalog database. A background compactor later writes them into manifest files. A snapshot is thus a list of sealed manifests plus the rows that the compactor did not seal yet.
- **The Merkle hash is computed at publication,** not at each commit. A published snapshot always has its hash (ID-5).
- **Appends do not conflict.** Some updates only add members, and their requirement only names the branch. mudcat applies such an update to the newest snapshot of the branch, and the client does not retry. Iceberg treats appends in a similar way when a client retries. Updates that remove or replace members keep their strict requirements.
- **Batched snapshots.** The ingest writer groups the registrations for one dataset into one commit for each batch interval. Thus a high rate of registration does not make one snapshot for each file.

### Query compiler

The query language extends Datacat's, for example `size > 6200 and status =~ "valid*"`, over path globs such as `/raw/**`.

It adds:

- set operations over datasets and snapshots, named queries, and joins through reference data (from MetaCat's MQL);
- lineage operators: `ancestors()`, `descendants()`, `derived_from(<query>)`, `used_in(<model>, role=training)`;
- policy predicates: `permitted(train, purpose=commercial)`.

Queries compile to SQL against the catalog database. Lineage operators use recursive queries over relation tables. By default, they exclude ubiquitous roles. The query compiler pushes element-level predicates down to manifest scans. A query engine such as DuckDB, DataFusion or Trino runs these scans, not PostgreSQL.

### Ingest writer

All writes from bulk clients and site agents go through a queue to a batched writer. This writer commits in large transactions. Before the Butler adopted this pattern, it measured inserts from hundreds of concurrent workers that took up to ten minutes.

- **Idempotency:** the key for registration is (data object, content checksum), so retries are harmless.
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

**Format plugins** are augmenters (model, section 3) that read the bytes. They read a version's header or index and produce:

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
- **Consistency checks.** Agents compare storage inventories with the replica records, to find dark data (files that no replica describes) and lost replicas.
- **Data compaction and rewrites,** when the maintenance section below assigns them to the site.

Agents report results through the ingest writer.

### Augmenter runtime

Augmenters run where their inputs are:

| Place | Runs | Isolation |
|---|---|---|
| Site agent | Augmenters that read bytes: format plugins, fingerprints, embeddings, classifiers that read content | A sandbox (container or WebAssembly) with read access only to the leased objects, and no network access except to the catalog |
| Control plane worker | Augmenters that read only metadata: rule-based classification, tag propagation, metrics over metadata | First-party augmenters only |
| Customer gateway | Augmenters that read customer-held metadata | Like a site agent |

The runtime:

- **Scheduling.** The runtime gets work from the change log, from schedules and from requests. Each run is a leased task, so a failed run goes back to the queue.
- **Writes.** Augmenters write their outputs through the ingest writer, into their own namespaces.
- **Review.** Outputs that need review go into a review queue in the web UI, with their status `proposed`.
- **Recomputation.** A new augmenter version starts a background recomputation of its outputs. The old outputs stay visible until the new ones are complete.
- **Costs.** Each run records its compute time, so that the vendor of a hosted service can charge for augmentation separately.

### Resolver

The resolver serves reads, but it is not in the data path:

- **Name resolution.** The resolver resolves names through views to one version and its best replica for the caller's compute location. It chooses among the verified replicas that jurisdiction and egress rules permit. It prefers the nearest replica, then the cheapest.
- **Job resolution.** For a training or analysis job, the resolver resolves a pinned specification and returns three things:
  - the **resolved manifest**, as Arrow files that list elements, selectors and replica URLs;
  - **credentials:** signed URLs, or vended credentials scoped to a prefix. The resolver issues prefix-scoped credentials only if the prefix is fully inside one ACL root that the caller can read. If not, the resolver issues signed URLs per object, in batches, as the job requests them. The resolver checks read permission on a snapshot once, at the snapshot. This check then covers all of the snapshot's members;
  - a **run token** that ties the job's reads to an activity record.
- **Resolution in parts.** A large pinned specification can resolve to billions of elements. In this case, the resolver plans the resolution, as the server-side scan planning of the Iceberg REST catalog does. It returns a plan identifier and a list of parts. Workers then get the parts in parallel. Each part is a manifest file with its credentials.
- **Observed capture.** The resolver logs the credentials that it issues under a run token. These logs give the `observed` usage relations.

### Planning with statistics

The resolver uses statistics (model, section 4) to plan work for a compute engine. A plan for a large resolution has these steps:

1. **Prune snapshots and manifests.** The planner compares the predicates of the request with snapshot statistics and manifest summaries, and skips every manifest that cannot match.
2. **Check policy by manifest.** The source bitmap of a manifest shows which sources its entries use. If every source of a manifest permits the requested action and purpose, the pinning service accepts the whole manifest without a check of each element. It checks single elements only in manifests that mix permitted and other sources, or that the invalidation set can touch. The fingerprint filters of a manifest show if a takedown can touch it.
3. **Prune files.** Inside the remaining manifests, the planner compares the predicates with file statistics: time spans, coordinate extents, tag sets, bounds.
4. **Size the tasks.** The planner groups the remaining elements into read tasks of about equal cost. It uses bytes, element counts and measured decode cost, not file counts. This prevents a few large or slow files from holding up the job.
5. **Place the tasks.** Each task gets the storage resource of its best replica, and the hints of its data: compute location, hardware, cache class. Engines turn these into scheduling constraints.
6. **Order the tasks.** For tape and other slow storage, the planner orders tasks by tape and position, and asks site agents to stage files in batches.

The plan returns each task with its expected size, row count and cost. Engines such as Ray use these estimates for their own planning.

Other uses of statistics:

- **Mixtures without scans.** Counts per source and per tag value in snapshot statistics give exact component sizes. Thus the pinning service can turn the weights of a specification into sample rates without a scan.
- **Stratified sampling and curricula.** Quantile sketches and manifest bounds let the planner select shards by score or difficulty, and order them for a curriculum.
- **Incremental work.** The change summary between two snapshots, with manifest-level summaries, lets an augmenter process only the members that a new snapshot added.
- **Contamination and takedown checks.** Fingerprint filters in manifest summaries let mudcat test a fingerprint against a whole zone, and scan only the manifests that can contain it.
- **Vector routing.** Embedding centroids in manifest summaries let a similarity search open only the shards near the query vector.
- **Estimates.** Sketches give the estimated counts of the REST interface and the facet counts of the web UI.

### Compute-engine integration

mudcat gives compute engines a read connector and a write connector. Ray Data is the first target:

- **Read connector.** A Ray `Datasource` turns each task of a resolution plan into a Ray read task. It has readers for each structure kind: byte range, member, row range, hyperslab and time span. It passes the size and row estimates of each task to Ray, and the placement constraints as node labels. It refreshes credentials during long jobs, and it carries the run token, so that mudcat records the observed lineage.
- **Write connector.** A Ray `Datasink` registers outputs through bulk registration. Registration is idempotent (OP-5), because Ray retries failed tasks. Outputs become versions, representations or annotation sets, with the run as their generating activity.
- **Feedback.** The connectors report measured decode times and unreadable files, which become statistics (model, section 4).

The same connectors are the model for Spark, Daft and PyTorch data loaders.

### Policy engine and pinning service

There are five enforcement points: **registration**, **pinning**, **materialisation**, **export** and **deletion**. Everything else happens asynchronously from the change log. This follows the idea of the iRODS policy enforcement point, but without a general rule language in the request path.

- **Access control.** The policy engine delegates access control to the authorization port (next section).
- **Usage policy.** mudcat always evaluates usage policy itself, in SQL, over the usage grants, constraints and duties attached to sources. The **effective-policy cache** holds the accumulated duties per snapshot, representation and model. It is a derived index, and mudcat recomputes it after each change.
- **Pinning.** The pinning service:
  - resolves a specification to snapshots, and records the identifier and the Merkle hash of each;
  - applies the exclusion lists of each dataset and the zone-wide invalidation set;
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

A takedown, licence expiry, retention deadline or erasure request starts a workflow. The operations are those of the model (model, section 5: removal, tombstones and holds):

1. Check legal holds. A hold diverts the data that it matches to its retention compartment. It also stops purge and erasure of that data.
2. Add the tombstone to the zone-wide invalidation set. From this point, the resolver and the pinning service exclude the content from every snapshot, old or new.
3. Enumerate occurrences through the content-identity index and lineage: derived representations, annotation sets, caches, replicas.
4. If the request needs purge, issue deletion tasks to site agents, and collect a receipt per replica. Where the storage has native versioning, the site agent also removes old object versions, so that the bytes are really gone.
5. If the request needs erasure, replace the catalog records with minimal tombstones.
6. Make redacted successors of the affected snapshots where their owners want them. The original snapshots keep their hashes.
7. Flag the models trained on affected data, with the number of affected elements. A person then makes a decision, and the workflow records it.
8. Send the tombstone on the federation feed to the zones that hold copies.

### Maintenance

Iceberg deployments run table maintenance: data-file compaction, manifest rewrites, snapshot expiry and orphan-file removal. mudcat needs the same kinds of work, but splits it by what the work touches.

| Task | Touches | Who does it |
|---|---|---|
| Seal recent member rows into manifest files; merge small manifests | Catalog database and the zone's manifest store | Catalog maintenance, a central background worker |
| Expire unpublished snapshots that nothing refers to; remove the manifests that only they used | Catalog database and manifest store | Catalog maintenance |
| Rebuild and verify derived indexes | Derived indexes | Index builders |
| Verify checksums; find dark data and lost replicas | Bytes on a storage resource | Site agents |
| Compact small data files into larger shards; rewrite files to drop purged content | Bytes on a storage resource | A transform activity that site agents or external compute run |
| Maintain registered external tables (Iceberg, Delta, Lance) | The table's own files | Delegated to the table's engine; mudcat records the new table snapshot |
| Remove bytes for a takedown or expiry | Bytes on a storage resource | Deletion workflow, through site agents |

**Compaction of data files is not invisible in mudcat.** In Iceberg, compaction replaces data files under the same logical table, and readers do not see a change. In mudcat, files are immutable versions with checksums, replicas and lineage. Thus compaction is a transform activity:

- It makes a new **representation** of the same snapshot. The logical members do not change, so pinned specifications stay valid.
- mudcat records the activity, with its inputs and outputs, in provenance.
- Replication rules then move to the new representation, and the replicas of the old representation retire under their normal retention.
- A rewrite that drops purged content makes a redacted successor snapshot instead, because the logical members change.

**Catalog maintenance runs from schedules and from the change log.** It seals manifests when the unsealed rows of a dataset pass a size or age limit. It also runs expiry each day. It never changes a published snapshot or its Merkle hash: sealing changes only where the members are stored, not what they are.

### Gateways

- **Iceberg REST catalog** for records-structured datasets. With it, Spark, Trino, DuckDB and PyIceberg can read these datasets by name. The gateway also vends credentials. Optionally, the gateway can also show the manifest of any snapshot as an Iceberg table, so that these engines can query elements and their metadata. To do this, the gateway generates Iceberg metadata from mudcat's manifests. mudcat does not store its own state in Iceberg metadata files.
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
2. The pinning service resolves it to snapshots, applies exclusion lists and the invalidation set, and evaluates usage policy. Then it writes the pinned specification with its policy decision.
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
| Hosted | A vendor runs the control plane for many tenants; site agents run in each customer's environment. See [hosted.md](hosted.md) | Teams that do not want to operate mudcat |

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
