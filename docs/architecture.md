# mudcat architecture

Status: sketch. This document describes the components, where state lives and the main flows. [model.md](model.md) defines the entities that this document refers to. Requirements are in [requirements.md](requirements.md), and this document describes how the design meets them. Field-specific semantics, formats, gateways and examples are in [fields.md](fields.md).

## Requirements that shape the architecture

| Requirement | Consequence | Requirement IDs |
|---|---|---|
| 10^4–10^9 catalog entities, up to ~10^11 elements | Elements live in columnar manifests, not in the catalog database | PF-1, PF-2 |
| Data in several clouds, HPC centres and enclaves | Federated tenants; storage resources with jurisdiction and egress attributes; site agents near the data | RD-4, PL-2, PL-3, PL-4 |
| Training reads at GPU-cluster rates | The catalog never proxies reads; it issues resolved manifests and credentials once per job | RD-1, RD-2, RD-3 |
| Small teams must be able to run it | PostgreSQL is the only necessary dependency; every other index is optional and rebuildable | OP-1, OP-2, AC-2 |
| Mixed data structures | Format plugins produce structure descriptors; selectors address parts without enumeration | ST-1, ST-4, ST-5 |
| Usage policy, takedowns, legal holds | A fixed set of enforcement points; deletion as a tracked workflow | UP-1 to UP-8 |
| Organisations already run Zanzibar-style authorization services | Access checks go through an authorization port with built-in, SpiceDB and OpenFGA adapters | AC-1, AC-3, AC-4 |
| Many small tenants in one deployment | The tenant ID is first in every key; one layer resolves the tenant; one schema for one tenant and for many | ID-11, ID-12, OP-8, OP-12, OP-13 |
| Bulk registration from many workers | A batched ingest writer; workers never write rows one at a time | IN-3, PF-3, OP-4, OP-5 |

## Planes

| Plane | Holds | Technology | Source of truth? |
|---|---|---|---|
| Catalog database | Tenants, collections, artifacts, versions, replicas, storage resources, datasets, snapshots, sources, policies, resolutions, activities and their qualified relations, ACLs | PostgreSQL | Yes |
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

instance: mudcat instance {
  gateway: "API gateway (OIDC, tokens)"
  gateways: "Gateways\nIceberg REST · public catalog (Schema.org) · DCAT, Croissant, RO-Crate · MCP server\nfsspec · federation feed · field gateways (fields.md)"

  services: Services {
    grid-columns: 3
    api: Catalog API
    query: Query compiler
    resolver: Resolver
    policy: Policy engine
    ingest: "Ingest writer\n(batched)"
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

clients -> instance.gateway
instance.consumers -> agents: tasks
agents -> movers
agents -> storage
movers -> storage
```

### Catalog API

The catalog API is a stateless service with these interfaces:

- **REST interface:** it has an OpenAPI description and uses JSON by default. Content negotiation gives JSON-LD with the published context. Paging uses cursors with a stable default order, and counts are estimates. To count results, Datacat's REST layer iterated over the full result set. pgstac documents the same failure mode.
- **Bulk interface** for registration, manifest submission and resolution: Apache Arrow over HTTP or Arrow Flight. It handles millions of rows per call.
- **Change stream** of catalog changes (see Change stream, below).
- **Commits with requirements.** A request that changes a dataset has the shape of an Iceberg REST commit. It has a list of **requirements** and a list of **updates**. Iceberg calls a requirement such as `assert-ref-snapshot-id` an assertion about the current table state. In mudcat, a requirement such as "branch `main` points to snapshot S" goes with an update such as "add these members". mudcat applies all the updates in one transaction, but only if all the requirements are true for the newest state (see Commits on the newest state, below). If not, it returns a conflict, and the client tries again. One request can change several datasets, in one transaction. This is optimistic concurrency.

### Commit cost

Iceberg commits are slow, usually seconds. Each commit writes new manifests, a new manifest list and a new metadata file to object storage before it swaps the pointer in the catalog. When two writers conflict, the loser writes these files again. Many small commits also make many small files and snapshots, which later need compaction and expiry. A REST catalog helps, because the server applies the updates, but the file writes stay.

mudcat keeps object-storage writes out of the commit path:

- **A commit is a database transaction.** It changes rows in the catalog database only. It writes no files.
- **Recent changes stay as rows.** The members that recent commits added or removed stay as rows in the catalog database. A background compactor later writes them into manifest files. A snapshot is thus a list of sealed manifests plus the rows that the compactor did not seal yet.
- **The Merkle hash is computed at publication,** not at each commit. A published snapshot always has its hash (ID-5).
- **Only a false requirement is a conflict.** mudcat applies each commit to the newest snapshot of the branch. An append has no requirement on other members, so appends never conflict. A commit that removes or replaces members conflicts only when another writer changed the same members (see the next section).
- **Batched snapshots.** The ingest writer groups the registrations for one dataset into one commit for each batch interval. Thus a high rate of registration does not make one snapshot for each file.

### Commits on the newest state

**The rule.** mudcat tests the requirements of a commit against the newest state of the dataset at the time of the commit, and applies the updates to that state. The state that the client read is not a part of the test. Thus a commit fails only when one of its own requirements is false. A change by another writer that no requirement refers to is not a conflict.

Iceberg calls this a server-side retry. In the reference server of Iceberg it is a loop: when the pointer moved, the server loads the table again, tests the requirements again and applies the updates again. mudcat needs no loop. A commit locks the row of the branch ref, reads the newest snapshot, tests the requirements, applies the updates and ends. The rule is the same for each kind of dataset, because a commit changes members and annotation sets, and not files.

**Requirements, from narrow to wide.**

| Requirement | True when | Used for |
|---|---|---|
| None | Always | An append |
| `assert-create` | The dataset, the ref or the path does not exist | A new dataset, a new ref, a new artifact at a path |
| `assert-member` | A given member has a given version, or is absent | The replacement or the removal of that member |
| `assert-annotation-set` | A given annotation set has a given version | An enricher that replaces its own output |
| `assert-structure` | The structure and the last field ID of the dataset are the given ones | A change of the structure |
| `assert-unchanged-in` | No commit after a given snapshot added or removed a member that a given predicate matches | The replacement of one day of data; the removal of duplicates in one partition |
| `assert-ref-snapshot` | The branch ref points to a given snapshot | A change that depends on all members |

**The client library sets the requirements.** A person selects an operation, and the library adds the requirements that the operation needs:

| Operation | Requirements |
|---|---|
| Append members | None |
| Replace or remove named members | `assert-member` for each member |
| Replace the members that a predicate matches | `assert-unchanged-in` for that predicate |
| Add or replace an annotation set | `assert-annotation-set` |
| Change the structure | `assert-structure` |
| Replace all members | `assert-ref-snapshot` |

**The answer to a commit** gives the snapshot that the commit made and the snapshot that mudcat applied the updates to. The second one can be newer than the snapshot that the client read. A conflict names the requirement that is false, the member or the ref that it refers to, and the commit that changed it.

**The same rule applies outside datasets.** A new version of an artifact is a commit with a requirement on `latest`, or with none. A change to collections has `assert-create` for each new path. A write to a metadata payload has a requirement on the revision of that payload. Tables that their own engine maintains, such as Lance and Delta tables, do not get their commits through mudcat. Iceberg tables get them through the gateway, with the requirements of the Iceberg specification.

**Limits.**

- **The requirements are the read set that the client declares.** A client that used more of the dataset than its requirements say can commit on a state that it did not expect. `assert-ref-snapshot` gives full isolation. Each narrower requirement gives more concurrency and less isolation.
- **A wide requirement can fail again and again.** A writer with `assert-ref-snapshot` on a branch with many appends gets a conflict at each attempt. Such a writer must use a narrower requirement, or work on its own branch ref and move the main branch ref in one commit at the end.
- **The number of requirements has a limit.** A commit with more than TBD `assert-member` requirements must use `assert-unchanged-in`.
- **`assert-unchanged-in` needs history.** mudcat tests it with the change summaries and the manifest summaries of the commits after the given snapshot. If that snapshot expired, the requirement is false.

**A repeated commit is safe.** A commit can carry an idempotency key. For a second commit with the same key, mudcat returns the result of the first commit and makes no snapshot. The Iceberg REST specification has the same function for requests that change state. A commit whose updates change nothing makes no snapshot, and returns the present one.

### Query compiler

The query language extends Datacat's, for example `size > 6200 and status =~ "valid*"`, over path globs such as `/raw/**`.

It adds:

- set operations over datasets and snapshots, named queries, and joins through reference data (from MetaCat's MQL);
- lineage operators: `ancestors()`, `descendants()`, `derived_from(<query>)`, `used_in(<model>, role=training)`;
- policy predicates: `permitted(train, purpose=commercial)`.

Queries compile to SQL against the catalog database. Lineage operators and policy predicates need special care, because a simple implementation is slow. The next two sections tell how mudcat keeps them fast. The query compiler pushes element-level predicates down to manifest scans. A query engine such as DuckDB, DataFusion or Trino runs these scans, not PostgreSQL.

### Performance of lineage operators

A recursive query over all provenance relations does not scale. mudcat uses these measures:

- **Traversal at the level of snapshots and activities.** The graph that the operators walk contains datasets, snapshots, artifacts with their own lineage, models and activities. It has millions of relations, not billions. Lineage of single files and elements stays in run records and manifests. mudcat joins it only after the traversal made the set small.
- **Short fixed paths are joins, not traversals.** `used_in(<model>, role=training)` goes from the model to the activity that generated it, to its resolution, to the inputs. This is a join of fixed depth.
- **Limits.** Each traversal has a default depth and a limit on the number of nodes. It excludes ubiquitous roles by default. A query that passes a limit returns an error that states the limit. It does not return a partial result as if it were complete.
- **The smaller side first.** For `derived_from(<query>)`, the planner estimates the size of both sides. It starts from the results of the inner query and goes down, or from the candidates and goes up, and uses the smaller start set.
- **A lineage summary for common questions.** Each entity keeps its root sources and the activity that produced it as indexed columns. A background worker updates them from the change stream. "Everything that comes from source X" is then an index lookup, not a traversal. The summary is a derived index.
- **Ancestor table where it is small enough.** For datasets and snapshots, a background worker also keeps a table of all (ancestor, descendant) pairs. mudcat does not keep this table for artifacts and versions, where it would grow too large.
- **Long traversals run as jobs.** A traversal that cannot finish in the time limit of a request runs as a background job, and the client gets the result later.
- **The run view is a bounded traversal.** The run view of a model version (model, section 6) stops at other training activities. Thus its size depends on one run, and not on the age of the pipeline.

### Performance of policy predicates

`permitted(train, purpose=commercial)` must not evaluate usage policy for each row. mudcat uses these measures:

- **Policy is on sources, and sources are few.** The table of usage grants has one row for each (source, action, purpose). For an artifact or a version, the predicate is a join from its source to this small table.
- **Effective policy for sets.** For a snapshot or a dataset, the answer needs every source of every member. The effective-policy cache keeps, for each snapshot, the set of (action, purpose) pairs that all its sources permit. The predicate is then a lookup. Counts per source in snapshot statistics give the sources of a snapshot without a scan of its members.
- **Updates by source.** When the grants of a source change, a background worker computes the cache again only for the snapshots that use that source.
- **Time limits are evaluated at query time.** The cache keeps the earliest expiry date of the grants that it used. The predicate compares this date with the time of the query. Thus an expired licence takes effect at once, without an update of the cache.
- **The decision at resolution is always exact.** The predicate in a query is a filter for discovery. The policy engine evaluates policy again, completely, when a job resolves data.

### Ingest writer

All writes from bulk clients and site agents go through a queue to a batched writer. This writer commits in large transactions. Before the Butler adopted this pattern, it measured inserts from hundreds of concurrent workers that took up to ten minutes.

- **Idempotency:** the key for registration is (artifact, content checksum), so retries are harmless.
- **Ordering:** registration commits catalog rows before a producer may delete its staged copy. This avoids the Butler's orphaned-file problem in reverse.

Interactive single writes bypass the queue but use the same transaction code.

### Catalog database

The catalog database is PostgreSQL with one schema per model area: namespace and storage, structure, metadata, datasets, policy, provenance, placement.

- **IDs and identifiers:** UUIDv7 primary keys (IDs) for every entity, versions and snapshots included. They keep index locality for time-ordered inserts (the Butler also made this change). The embedded timestamp is not a privacy risk, because the access model controls who can see an ID. A separate table holds identifiers (paths, DOIs, names in other systems), with a unique index per scheme.
- **Paths:** the catalog stores paths as materialised paths in `ltree`, so it evaluates path globs and ACL inheritance in SQL. This removes Datacat's Java tree walk and its temporary tables.
- **Metadata:** the catalog stores each metadata payload as one JSONB document, with a key of (entity, namespace). One kind of index is not enough for all cases (see Metadata indexes, below).
- **Partitioning:** from the first schema version, the catalog partitions large tables (versions, replicas, relations) by tenant and namespace.
- **Tenant key:** the tenant ID is the first column of every primary key and of every index. Thus the rows of one tenant are together, and a move of a tenant copies them without a change to keys.
- **Foreign keys:** inside one tenant, with the tenant ID as part of each foreign key. References between tenants, references to more than one kind of entity, and some references in the largest tables have no foreign key (model, Cheap copies).
- **Commits and positions:** each transaction that changes the catalog writes one commit record: commit ID, tenant, actor and time. History rows carry the commit ID, not a wall-clock time. "As of time T" is the last commit at or before T.
- **Outbox:** every transaction appends its events to an outbox table, with the transaction ID of the database. A relay publishes them on the change stream, and assigns their positions (see Change stream).

mudcat produces RDF from this schema through a declared mapping and the JSON-LD context. A triple store is optional, and mudcat uses it only for export.

### Metadata indexes

GIN indexes on JSONB are not enough alone. They serve equality and containment for any number of keys. But they do not serve range queries or sorts, their updates are expensive, and PostgreSQL has poor estimates of selectivity for JSONB conditions. These limits matter most for the metadata of versions, where there can be 10^9 payloads. mudcat thus uses four kinds of index:

| Metadata | Index | Serves |
|---|---|---|
| Rich entities (collections, artifacts, datasets, snapshots, sources, models) | GIN on each payload | Equality and containment on any key, declared or not |
| Declared attributes that queries use often, on any entity | A **typed attribute index**: a derived table with rows of (attribute, typed value, entity), and one B-tree for each value type | Equality, range and sort, with good estimates. A new attribute needs no new database index |
| A small number of the most used attributes | Columns with their own indexes | The fastest filters and sorts |
| All other metadata of versions | No index in the catalog database. The values are also columns in manifests, with bounds for each manifest | Scans by a columnar engine, with pruning by manifest statistics |

Rules:

- **The payload is the source of truth.** The typed attribute index and the manifest columns are derived, and mudcat can build them again.
- **The attribute registry decides.** A declared attribute has a flag that puts it in the typed attribute index. mudcat records how often queries use each attribute, and how many distinct values it has. The planner uses these numbers, and an administrator can see which attributes need an index.
- **No limit on the number of keys, but a limit on indexed keys.** A deployment can have many thousands of metadata keys. Only the declared ones that queries use get rows in the typed attribute index.
- **Many conditions in one query.** Each condition on the typed attribute index is one more join, which was the weak point of Datacat's metadata tables. For a query with many conditions over very many versions, the planner uses the manifest columns and a columnar engine.
- **Search engines have the same problem.** An external search engine also has a limit on the number of fields. The state document thus gives undeclared attributes as a list of (key, value) pairs, not as one field for each key.

The limits of each kind of index are not measured yet. They are part of the first benchmark (PF-4).

### Tenants

One instance serves one tenant or many tenants. The code knows about tenants in one layer only.

- **Tenant resolver.** The outermost layer of a request resolves the tenant. In a hosted instance, it reads the tenant from the hostname and checks the audience of the token. In a self-hosted instance with one tenant, a resolver with no configuration always returns that tenant.
- **Tenant-scoped transaction.** The same layer opens the database transaction for that tenant. Row-level security then filters every table. The code below this layer has no tenant parameter. A request with an ID from another tenant finds nothing.
- **Same schema.** The schema is the same for one tenant and for many. With one tenant, an operator can leave row-level security off.

Some parts cannot get the tenant from a request, and must carry it:

| Part | Rule |
|---|---|
| Background workers | Each job carries its tenant |
| Caches | Each tenant has its own caches. A cache key without a tenant is an error |
| Change stream | Positions and subscriptions are per tenant |
| Storage | Manifest prefixes, storage credentials and encryption keys are per tenant |
| Search | Each query has a tenant filter, or each tenant has its own index |
| Limits and logs | Rate limits, quotas, metrics and audit are per tenant |
| Work between tenants | A share or a copy between two tenants of the same instance is an explicit path, with an audit record |

An isolation test suite runs each interface as two tenants, and tries the IDs of each tenant against the other.

### Format plugins and site agents

**Format plugins** are enrichers (model, section 3) that read the bytes. They read a version's header or index and produce:

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
- **Discovery.** On an external storage resource with discovery on, an agent makes a discovered entry for each file that no replica describes (model, section 1). It applies the grouping rules of the storage resource. It also reads the access logs of the storage, where they exist, and reports which job read or wrote which object.
- **Data compaction and rewrites,** when the maintenance section below assigns them to the site.

Agents report results through the ingest writer.

### Enricher runtime

Enrichers run where their inputs are:

| Place | Runs | Isolation |
|---|---|---|
| Site agent | Enrichers that read bytes: format plugins, fingerprints, embeddings, classifiers that read content | A sandbox (container or WebAssembly) with read access only to the leased objects, and no network access except to the catalog |
| Control plane worker | Enrichers that read only metadata: rule-based classification, tag propagation, metrics over metadata | First-party enrichers only |
| Customer gateway | Enrichers that read customer-held metadata | Like a site agent |

The runtime:

- **Scheduling.** The runtime gets work from the change log, from schedules and from requests. Each run is a leased task, so a failed run goes back to the queue.
- **Writes.** Enrichers write their outputs through the ingest writer, into their own namespaces.
- **Review.** Outputs that need review go into a review queue in the web UI, with their status `proposed`.
- **Recomputation.** A new enricher version starts a background recomputation of its outputs. The old outputs stay visible until the new ones are complete.
- **Expectations.** A control plane worker tests the expectations of a dataset in the commit of each new snapshot, from the statistics of the snapshot. The results are in the same transaction as the snapshot, so an event for a failure cannot be lost.
- **Status records.** For each entity and each enricher, the runtime keeps the time, the result and the error of the last run. An enricher that needs the output of another enricher reads this record, and waits until that output exists. It runs again when that output changes. Goods coordinates its analysis modules in the same way, with no central schedule of dependencies. The records also answer questions about operation, for example which part of a collection an enricher has processed.
- **Order of work.** The queue for discovered entries has an order of importance: entries with metadata from a person, with many resolutions or with many dependents come first. A second queue goes through all entries more slowly. Goods runs its most expensive analysis in the same two ways.
- **Costs.** Each run records its compute time, so that the vendor of a hosted service can charge for enrichment separately.

### Resolver

The resolver serves reads, but it is not in the data path:

- **Name resolution.** The resolver resolves a name to one version and its best replica for the caller's compute location. A request can state a version, a ref or a time. If it states none, the resolver uses `latest`. It chooses among the verified replicas that jurisdiction and egress rules permit. It prefers the nearest replica, then the cheapest.
- **Job resolution.** A training or analysis job states its inputs with their roles, and the action and purpose. The resolver fixes each input to an exact snapshot or version. The policy engine then evaluates usage policy. The resolver writes the **resolution** with its policy decision (model, section 7), and returns three things:
  - the **resolved manifest**, as Arrow files that list elements, selectors and replica URLs;
  - **credentials:** signed URLs, or vended credentials scoped to a prefix. The resolver issues prefix-scoped credentials only if the prefix is fully inside one ACL root that the caller can read. If not, the resolver issues signed URLs per object, in batches, as the job requests them. The resolver checks read permission on a snapshot once, at the snapshot. This check then covers all of the snapshot's members;
  - a **run token** that ties the job's reads to an activity record.
- **Resolution in parts.** A large request can resolve to billions of elements. In this case, the resolver plans the resolution, as the server-side scan planning of the Iceberg REST catalog does. It returns a plan ID and a list of parts. Workers then get the parts in parallel. Each part is a manifest file with its credentials.
- **Observed capture.** The resolver logs the credentials that it issues under a run token. These logs give the `observed` usage relations.

### Planning with statistics

The resolver uses statistics (model, section 4) to plan work for a compute engine. A plan for a large resolution has these steps:

1. **Prune snapshots and manifests.** The planner compares the predicates of the request with snapshot statistics and manifest summaries, and skips every manifest that cannot match.
2. **Check policy by manifest.** The source bitmap of a manifest shows which sources its entries use. If every source of a manifest permits the requested action and purpose, the policy engine accepts the whole manifest without a check of each element. It checks single elements only in manifests that mix permitted and other sources, or that the invalidation set can touch. The fingerprint filters of a manifest show if a takedown can touch it.
3. **Prune files.** Inside the remaining manifests, the planner compares the predicates with file statistics: time spans, coordinate extents, tag sets, bounds.
4. **Size the tasks.** The planner groups the remaining elements into read tasks of about equal cost. It uses bytes, element counts and measured decode cost, not file counts. This prevents a few large or slow files from holding up the job.
5. **Place the tasks.** Each task gets the storage resource of its best replica, and the hints of its data: compute location, hardware, cache class. Engines turn these into scheduling constraints.
6. **Order the tasks.** For tape and other slow storage, the planner orders tasks by tape and position, and asks site agents to stage files in batches.

The plan returns each task with its expected size, row count and cost. Engines such as Ray use these estimates for their own planning.

Other uses of statistics:

- **Sizes without scans.** Counts per source and per tag value in snapshot statistics give the exact size of each input. Thus a job can compute sample rates without a scan.
- **Stratified sampling and curricula.** Quantile sketches and manifest bounds let the planner select shards by score or difficulty, and order them for a curriculum.
- **Incremental work.** The change summary between two snapshots, with manifest-level summaries, lets an enricher process only the members that a new snapshot added.
- **Contamination and takedown checks.** Fingerprint filters in manifest summaries let mudcat test a fingerprint against a whole tenant, and scan only the manifests that can contain it.
- **Vector routing.** Embedding centroids in manifest summaries let a similarity search open only the shards near the query vector.
- **Estimates.** Sketches give the estimated counts of the REST interface and the facet counts of the web UI.

### Compute-engine integration

mudcat gives compute engines a read connector and a write connector. Ray Data is the first target:

- **Read connector.** A Ray `Datasource` turns each task of a resolution plan into a Ray read task. It has readers for each structure kind: byte range, member, row range, hyperslab and time span. It passes the size and row estimates of each task to Ray, and the placement constraints as node labels. It refreshes credentials during long jobs, and it carries the run token, so that mudcat records the observed lineage.
- **Write connector.** A Ray `Datasink` registers outputs through bulk registration. Registration is idempotent (OP-5), because Ray retries failed tasks. Outputs become versions, representations or annotation sets, with the run as their generating activity.
- **Feedback.** The connectors report measured decode times and unreadable files, which become statistics (model, section 4).

The same connectors are the model for Spark, Daft and PyTorch data loaders.

### Policy engine

There are five enforcement points: **registration**, **resolution**, **materialisation**, **export** and **deletion**. Everything else happens asynchronously from the change log. This follows the idea of the iRODS policy enforcement point, but without a general rule language in the request path.

- **Access control.** The policy engine delegates access control to the authorization port (next section).
- **Usage policy.** mudcat always evaluates usage policy itself, in SQL, over the usage grants, constraints and duties attached to sources. The **effective-policy cache** holds the accumulated duties per snapshot, representation and model. It is a derived index, and mudcat recomputes it after each change.
- **Evaluation at resolution.** For each job resolution, the policy engine:
  - applies the exclusion lists of each dataset and the tenant-wide invalidation set;
  - checks that every source that contributes grants the requested action and purpose;
  - gives the policy decision to the resolver, with the authorization consistency token where the provider issues one.

  The resolution fails if the policy does not permit a necessary input.
- **Terms compiler.** The terms compiler compiles licence identifiers and ODRL documents into grants, constraints and duties. It queues anything that it cannot translate for review.

### Authorization port

Access checks go through one internal interface. Thus mudcat itself or an external Zanzibar-style service, self-hosted or managed, can serve them.

| Adapter | Backing service | Deployment |
|---|---|---|
| Built-in | Relationship tuples in the catalog's PostgreSQL, evaluated with recursive SQL | Development and lab profiles; no extra dependency |
| SpiceDB | SpiceDB, self-hosted or as AuthZed's managed service | Lab and multi-site |
| OpenFGA | OpenFGA, self-hosted or as Okta FGA (managed OpenFGA) | Lab and multi-site |
| Others | Any service that supports the same operations, for example Ory Keto (Ory Network) | Through the same adapter contract |

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

**A neutral schema.** mudcat defines its authorization schema once, in a neutral form. Then it compiles this schema to each provider's schema language (the SpiceDB schema language and the OpenFGA modelling language). The core schema uses only features that both main implementations support:

- direct relations;
- subject sets such as `group#member`;
- union, intersection and exclusion;
- the traversal of a relation to another object (SpiceDB arrows, OpenFGA tuple-to-userset);
- public wildcards;
- conditions written in CEL with request context (SpiceDB caveats, OpenFGA conditions), used for ticket expiry, embargo dates and use counts.

Only optional extensions may use provider-specific features, such as SpiceDB's native relationship expiry or its all-parents arrow. The core schema never uses them.

**Object IDs.** mudcat identifies objects as `type:uuid`, with no paths or names. Thus renames never touch the authorization service. Object IDs also stay within the length and character limits of the provider.

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

### Change stream

mudcat publishes each change to the catalog as an event. This is change data capture, but at the level of the model. An event describes a change to an entity, not a change to a database row. Thus the stream stays stable when the database schema changes.

**Source.** A commit writes its events to the outbox in the same transaction as the change. A relay then publishes them. Thus an event exists for a change if, and only if, the change committed.

**Positions in commit order.** The database gives a number to a row at the time of the insert, not at the time of the commit. A stream that used this number could show event 11 before event 10, and a consumer could then miss event 10. Thus the relay assigns the positions:

1. The relay reads only the outbox rows of transactions that are older than every transaction still in progress. No earlier event can appear later.
2. The relay gives each event its position at the time of publication. The relay is the only writer of positions.

The Salesforce Database has the same rule: readers see only up to a "visible epoch" that is behind the commits in progress.

**Envelope.** Each event is a CloudEvents 1.0 event with these attributes:

| Attribute | Content |
|---|---|
| `id` | The ID of the event |
| `source` | The IRI of the tenant |
| `type` | The event type, with a version, for example `mudcat.snapshot.committed.v1` |
| `subject` | The ID of the entity that changed |
| `time` | The commit time |
| `dataschema` | The JSON Schema of the event data |
| `sequence` | The position of the event in the stream of the tenant. Positions only increase, and they are in commit order |
| `commit` | The ID of the commit. All the events of one commit share it |
| `revision` | The number of changes to this entity so far. A consumer uses it to ignore an event that it applied before |
| `actor` | The principal that made the change |

**Event types.** Each family covers one part of the model:

| Family | Events |
|---|---|
| Collections and artifacts | Collection created, moved, removed. Artifact registered. Version registered. Delete marker written. Identifier added, changed, removed |
| Replicas and storage | Replica registered, verified, suspect, lost, retired. Storage resource changed |
| Metadata | Metadata payload written, removed. Tag assigned, removed. Field declared. Schema changed. Enricher output proposed, accepted, rejected |
| Datasets | Snapshot committed (with its change summary, see below). Ref moved. Snapshot published, deprecated, withdrawn, expired. Manifest sealed |
| Access and policy | Relationship written, removed. Source and terms changed. Usage grant, constraint or duty changed. Legal hold placed, released |
| Removal | Invalidation added. Replica purged (with its deletion receipt). Entity erased |
| Provenance | Activity started, ended. Resolution created. Usage and generation recorded |
| Placement | Replication rule created, removed. Completeness changed. Transfer requested, finished, failed |

**Content of a snapshot-committed event.** The event always carries the change summary of the snapshot: the operation, and the numbers and total sizes of the members added and removed. An Iceberg snapshot summary has the same kind of content: it gives numbers, not lists of files. For the lists:

- If the commit added and removed at most a configured number of members (for example 1,000), the event also carries the IDs of those members.
- For a larger commit, the event carries a link to the **changes between snapshots** interface. This interface returns the members that one snapshot added and removed in comparison with another snapshot, in pages. Iceberg gives the same information through the status of each entry in its manifests.

Thus an event has a size limit, and a consumer can always get the complete list.

**Two streams.** Consumers need two different things, so mudcat publishes two streams from the same outbox:

- **Change events** tell what happened, in order. Each event carries the changed values, before and after. mudcat keeps change events for a retention period.
- **State documents** give the complete current description of an entity after each change (see Search, below). The key of a state document is the entity ID, so a message broker can keep only the newest document for each entity. A consumer that builds an index needs only this stream.

**Start and recovery of a consumer.** A new consumer, or one that lost its position, asks for a **backfill**. mudcat then sends the state document of each entity that the consumer can read. It marks the position where the backfill started, and continues with the live stream from that position. This is how mudcat rebuilds a derived index (OP-2, OP-3).

**Delivery.**

- Delivery is at least once. A consumer uses `revision` to make its writes idempotent.
- Events for one entity arrive in order. The partition key is the entity ID.
- A consumer keeps its position (`sequence`), and can find a gap.

**Removal.** When mudcat invalidates or erases an entity, the state stream carries a removal event for it. A consumer must then delete its copy. The terms of a subscription make this an obligation.

**Permissions.** A subscription has an identity. mudcat sends it only the events for entities that this identity can read. Customer-held metadata values are never in an event. The federation feed is a subscription with a signature on each event.

**Transport.** The built-in transport is an HTTP feed with a position, so mudcat needs no message broker. Connectors publish the same events to Kafka, NATS and the event services of cloud providers. mudcat can also publish lineage as OpenLineage events, for tools that read that format.

### Search

Exact queries go to the catalog database, and their results are authoritative. Search is for the other case: a user does not know the exact name or value. Search results come from a derived index, so they can be a short time behind the catalog.

**Search document.** The state document of an entity is also its search document. It has these parts:

| Part | Content |
|---|---|
| Identity | ID, kind, name, path and other identifiers |
| Description | Description text and documentation |
| Metadata | Each metadata payload, with its namespace and its assertion origin. The field definitions give the type of each attribute: keyword, number, date, text or location |
| Tags | The tags of the entity, with the tags that it inherits |
| Structure | Structure kind, and the names and descriptions of fields. Thus a user can find a table by one of its columns |
| Statistics | Size, counts, time span and spatial extent |
| Lineage | Root sources, the activity that produced the entity, and the number of entities upstream and downstream |
| Policy | Licences and the permitted actions and purposes |
| Use | How often and how recently jobs resolved the entity |
| Access | ACL root and compartments, for the filter on permissions |

**Scope.** Collections, artifacts, datasets, snapshots, sources, models and activities each have a search document. A version has no search document of its own by default: its artifact carries a summary of its versions. Elements are not in this index. Their embeddings are annotation sets, and vector engines index them.

**Kinds of match.** A search combines four kinds:

- **Lexical.** Full-text match with ranking. Names and paths are split at separators and at changes of letter case, so `rawFrames_2026` matches "raw frames".
- **Tolerant.** Match with spelling errors (edit distance or trigrams), and match on the first letters of a word for completion while the user types.
- **Semantic.** Match on meaning, with a vector embedding of a text form of the search document. Each embedding records the version of the model that made it. Synonyms come from the semantic annotations of fields and from the vocabularies of field profiles.
- **Similar entity.** "More like this one", from shared fields, tags, lineage neighbours and embeddings.

**Ranking.** The rank combines the lexical score and the vector score, with reciprocal rank fusion. It then adds signals from the catalog: use, published status, review status, and how recent the entity is. Deprecated entities rank lower. A user can exclude metadata that a model inferred.

**Filters and permissions.** A search can include exact conditions on any indexed attribute, on tags and on lineage. The index applies the filter on permissions. Then, before mudcat returns results, the catalog checks access control and the invalidation set again for each result, because the index can be behind.

**Ranked and complete answers.** A request states if it wants a ranked answer or a complete answer ([search.md](search.md)). A ranked answer comes from the index and gives the position in the change stream that the index has processed. A complete answer comes from the catalog database, through the query compiler. It has no rank, and it is never behind.

**Parts of the search document.** A word in a request can name the part that it must match: name, path, field, owner, tag, a job that reads or writes the entity, or an entity upstream or downstream. The index keeps the position of each part of a path, so that a search for consecutive parts matches them only in that order. Each identifier that an entity had at any time is in its search document.

**The filter on permissions has two plans.** The index holds the ACL root and the compartments of each document. For a caller who can read most of the catalog, the engine ranks first and then removes what the caller cannot read. For a caller who can read a small part, that plan returns short pages and is slow, so the engine filters first and ranks inside the readable part. mudcat selects the plan from the readable share of the caller. Counts and facets always come from the readable part.

**Engines.** In the development and lab profiles, PostgreSQL does all of this: full-text search, trigram match and `pgvector`. In larger deployments, the state stream feeds an external search engine, for example OpenSearch. The same stream feeds knowledge graphs and retrieval indexes for AI agents.

### Index builders

Consumers of the change log keep derived indexes current:

- **Search index:** built from the state stream (see Search, above), with lexical, tolerant and vector match.
- **Content-identity index:** a map from fingerprint or domain key to (snapshot, element). Batch jobs build it as partitioned columnar files. It starts with exact checksums and domain keys. Near-duplicate fingerprints come later.
- **Similarity tables** for registered similarity criteria, versioned with the criterion.

Every index has a version, and mudcat can rebuild it online. The steps are: build a new index, write to both, verify, switch an alias, and retire the old one. DataHub's upgrade failures show that reindexing must be a tested subsystem from the start.

### Lineage ingest

Adapters translate external records into activities and qualified relations, and mark them `imported`:

- OpenLineage events from Spark, Airflow, dbt and Flink;
- MLflow and Weights & Biases runs;
- Nextflow and Snakemake provenance;
- Hugging Face revisions.
- access logs of storage and logs of job schedulers, for discovered data. Relations from these logs have the mark `inferred`, not `imported`.

The adapters match the names that external tools use through identifiers. If an adapter cannot match an event, the event creates a placeholder entity. mudcat reconciles this entity later.

### Placement reconciler

The reconciler evaluates replication rules against completeness records, as Rucio's rule engine does, with snapshots and representations as the unit:

- it requests transfers from pluggable movers: FTS3, Globus, rclone, cloud-provider replication and lifecycle rules, Pelican;
- it records each transfer as an activity;
- it updates completeness after site agents verify the result.

Where a cloud provider can enforce a rule natively, the reconciler configures and observes the provider's mechanism. It does not move the bytes itself.

### Deletion workflow

A takedown, licence expiry, retention deadline or erasure request starts a workflow. The operations are those of the model (model, section 5: removal, invalidation and holds):

1. Check legal holds. A hold diverts the data that it matches to its retention compartment. It also stops purge and erasure of that data.
2. Add the invalidation record to the tenant-wide invalidation set. From this point, the resolver excludes the content from every snapshot, old or new.
3. Enumerate occurrences through the content-identity index and lineage: derived representations, annotation sets, caches, replicas.
4. If the request needs purge, issue deletion tasks to site agents, and collect a receipt per replica. Where the storage has native versioning, the site agent also removes old object versions, so that the bytes are really gone.
5. If the request needs erasure, replace the catalog records with erasure records.
6. Make redacted successors of the affected snapshots where their owners want them. The original snapshots keep their hashes.
7. Flag the models trained on affected data, with the number of affected elements. A person then makes a decision, and the workflow records it.
8. Send the invalidation on the federation feed to the tenants that hold copies.

### Maintenance

Iceberg deployments run table maintenance: data-file compaction, manifest rewrites, snapshot expiry and orphan-file removal. mudcat needs the same kinds of work, but splits it by what the work touches.

| Task | Touches | Who does it |
|---|---|---|
| Seal recent member rows into manifest files; merge small manifests | Catalog database and the tenant's manifest store | Catalog maintenance, a central background worker |
| Expire unpublished snapshots that nothing refers to; remove the manifests that only they used | Catalog database and manifest store | Catalog maintenance |
| Rebuild and verify derived indexes | Derived indexes | Index builders |
| Verify checksums; find dark data and lost replicas | Bytes on a storage resource | Site agents |
| Compact small data files into larger shards; rewrite files to drop purged content | Bytes on a storage resource | A transform activity that site agents or external compute run |
| Maintain registered external tables (Iceberg, Delta, Lance) | The table's own files | Delegated to the table's engine; mudcat records the new table snapshot |
| Remove bytes for a takedown or expiry | Bytes on a storage resource | Deletion workflow, through site agents |
| Purge the bytes of intermediates and caches at the end of their lifetime, and keep the stub | Bytes on a storage resource; catalog database | Deletion workflow, through site agents |
| Compact the stubs of old pipeline runs | Catalog database | Catalog maintenance |
| Compute use records and the list of abandoned data | Catalog database | Catalog maintenance, from the change stream |

**Compaction of data files is not invisible in mudcat.** In Iceberg, compaction replaces data files under the same logical table, and readers do not see a change. In mudcat, files are immutable versions with checksums, replicas and lineage. Thus compaction is a transform activity:

- It makes a new **representation** of the same snapshot. The logical members do not change, so resolutions stay valid.
- mudcat records the activity, with its inputs and outputs, in provenance.
- Replication rules then move to the new representation, and the replicas of the old representation retire under their normal retention.
- A rewrite that drops purged content makes a redacted successor snapshot instead, because the logical members change.

**Catalog maintenance runs from schedules and from the change log.** It seals manifests when the unsealed rows of a dataset pass a size or age limit. It also runs expiry each day. It never changes a published snapshot or its Merkle hash: sealing changes only where the members are stored, not what they are.

### Gateways

- **Iceberg REST catalog.** The gateway has two functions. For Iceberg tables, it is a writable catalog with a basic scope (see Conformance of the Iceberg REST gateway, below). For other records-structured datasets, it is a read interface, as follows.
- **Iceberg REST read interface** for records-structured datasets. With it, Spark, Trino, DuckDB and PyIceberg can read these datasets by name. The gateway also vends credentials. Optionally, the gateway can also show the manifest of any snapshot as an Iceberg table, so that these engines can query elements and their metadata. To do this, the gateway generates Iceberg metadata from mudcat's manifests. mudcat does not store its own state in Iceberg metadata files.
- **Field gateways:** optional gateways that a field profile defines, listed in [fields.md](fields.md). They generate service metadata from CSVW descriptions through community schema profiles that are registered as plugins.
- **Public catalog:** a landing page for each public dataset, with its Schema.org description as JSON-LD in the page, and a sitemap of these pages. Dataset search engines read these two things. The gateway serves only what an anonymous reader can see.
- **Metadata export and import:** DCAT 3, Croissant 1.1, RO-Crate and DataCite, generated from the model for publication and citation (model, section 4, public description). The gateway also imports Schema.org, DCAT and Croissant descriptions from other repositories as datasets on reference storage.
- **AI governance export:** SPDX 3 AI and Dataset profiles, and training-content summaries generated from the resolutions that training runs used.
- **MCP server** for agents. It runs with the caller's identity and permissions. Its tools:
  - search;
  - describe;
  - resolve;
  - lineage;
  - policy check.
- **fsspec filesystem** (`mudcat://`) for notebooks and Python tools.
- **Federation feed:** signed CloudEvents that tenants exchange. These events carry snapshot descriptions, lineage summaries, invalidations and completeness records. A tenant that receives an invalidation runs its own deletion workflow.

### Conformance of the Iceberg REST gateway

**Scope.** The gateway implements the endpoints of the Iceberg REST specification for the configuration, for namespaces and for tables: create, load, list, commit, rename, drop and register. It does not implement views, and it does not implement commits to more than one table, in its first scope. The configuration response lists the endpoints that the gateway implements, as the specification permits. Clients then do not call the other endpoints.

**Mapping.**

| Iceberg | mudcat |
|---|---|
| Catalog prefix or warehouse | A tenant, or a collection of it |
| Namespace, with nested namespaces | A collection |
| Table | An artifact whose structure kind is records and whose format is Iceberg |
| Table snapshot | A version of that artifact |
| Metadata file location | An attribute of the version. mudcat moves this pointer in the commit |
| Requirements and updates of a commit | The same, in one database transaction |
| Vended credentials | Credentials from the resolver, with the access check of the caller |

**A commit writes one file.** The Iceberg specification makes the catalog write the new table metadata file. This is the only place where mudcat writes to object storage in a commit. mudcat writes the file first and moves the pointer in the transaction. A file whose transaction fails is removed by maintenance.

**The conformance suite exists.** The Iceberg project has the REST Compatibility Kit. It runs the tests of the Java reference client against a server that the configuration names. It has about 106 tests for namespaces and tables, and about 52 tests for views. mudcat must pass the tests for namespaces and tables (IN-14). [evals/conformance/iceberg-rest.yaml](../evals/conformance/iceberg-rest.yaml) gives the command, the options of the kit and the value that mudcat sets for each option.

**The kit is not enough alone.** It uses one client. Thus mudcat also runs read and write tests with PyIceberg, Spark, Trino and DuckDB, as other catalog projects do, and it validates each response against the OpenAPI document of the specification.

**What the kit does not test.** It does not test access control, usage policy, tenants or lineage. The task templates in [evals/](../evals/README.md) test these through the same gateway (T5.1).

### Clients

- **Python SDK and CLI** with parity. They cover all entity operations and bulk registration.
- **Run context** for declared provenance:

  ```python
  with mudcat.run(code="git:abc123", image="sha256:...") as run:
      train = run.open("datasets/vision-mix@release-3", role="training", purpose="research")
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
2. **Is it immutable?** Anyone can copy immutable things (versions, published snapshots, resolutions, provenance records) anywhere without coordination. Mutable authoritative state (ACLs, grants, replica state, lifecycle status) has exactly one writer. Never replicate it with multiple writers.

### Replicate

Replication happens within one authority, which usually means one tenant:

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
| Query execution over manifests | DuckDB, DataFusion, Trino | Results are traceable to registered manifests |
| Run tracking | MLflow, Weights & Biases, workflow engines | Lineage ingest matches external names through identifiers |
| Training inside an enclave, or federated learning at a partner | The enclave or partner | mudcat records the activity with its location and inputs; obligations attach to the model that the activity produces |

mudcat never delegates usage-policy decisions: the evaluation at resolution, and the propagation of duties. These are the system's purpose. They depend on lineage that only the catalog holds.

### Federate

mudcat uses federation where several authorities will not, or may not, transfer control:

- separate organisations;
- legal jurisdictions;
- enclaves;
- commercial partners;
- national facilities in a collaboration.

Each authority runs its own tenant with its own namespace, identifiers, access control and policy.

Between tenants:

- **Tenants exchange immutable descriptions,** never mutable state: published snapshot descriptions, lineage summaries, completeness records and invalidations, over the signed federation feed. A tenant may harvest other tenants' published descriptions into its own search index, much as DCAT harvesters do. This copies descriptions, not authority.
- **References cross tenants by IRI.** A job in tenant A can name a snapshot in tenant B. To resolve it, the resolver asks tenant B. Tenant B applies its own access and usage policy.
- **Identity federates, but authorization does not.** Tenant B can accept tenant A's users through OIDC federation, but the tenant that owns the data makes every decision about it.
- **Agreements are explicit.** Data or descriptions flow only under an agreement between tenants. The tenants record each agreement as an ODRL offer and acceptance.
  - A tenant that accepts copies of data under an agreement also accepts the obligations attached to them.
  - A tenant honours invalidations automatically for copies that it received under an agreement that makes this necessary.
  - The tenant queues other invalidations for review.
- **Data may never move at all.** An enclave tenant may publish descriptions and lineage, but forbid any replica outside it. The enclave tenant registers the models trained inside it, with its obligations attached.

### Shared custody: one dataset, several sovereign custodians

Some collaborations sit between the two cases. An example is an international collaboration in which several institutions each hold and serve copies of one data release:

- The collaboration produces one logical dataset, such as a data release.
- Several institutions in different countries each hold, process and serve copies. Each institution wants ownership of its copies for reasons of sovereignty. These reasons include national law, the requirements of the agencies that fund it, and control over its own users and storage.
- Neither single-authority replication nor loose federation fits:
  - a single catalog would remove sovereignty from the custodians;
  - independent catalogs lose the fact that it is one dataset. The Butler team has a draft diagnosis of its multi-site problems. It says that collection structure and metadata corrections do not propagate, and that there is no completeness check per site.

mudcat treats this as federation with a shared namespace. It splits authority by kind of state, not by site:

| State | Authority | Other tenants |
|---|---|---|
| Definition of the logical dataset: artifacts, published snapshots, corrections to metadata and reference data | The **producing tenant** for each part. A part processed at a site is owned by that site's tenant | Subscribe to it, and hold read-only copies of the descriptions |
| A release that combines parts produced at several sites | A **release snapshot** that lists the member snapshots from each producing tenant, with their Merkle hashes. Each producer signs its members, and the release is co-signed | Verify it from the hashes alone |
| Replicas, replica state and completeness on a site's storage | The **custodian tenant** that owns that storage | Receive completeness records, which together give the global answer to "where are the copies" |
| Access decisions for a site's users and copies | The **custodian tenant** | — |
| Minimum terms of use, such as access rights and embargo periods | Set by the **publisher** in the agreement | Each custodian enforces them, and may add stricter local terms but not looser ones |
| Local additions: annotation sets, local metadata, derived products | The tenant that created them | Shared if the agreement says so |

Three mechanisms make this work without shared mutable state:

- **IDs of the producer.** A tenant that registers a replica of a version that another tenant produced uses the ID that the producing tenant minted. Registration is idempotent on (artifact, content checksum), so independent registrations of the same content converge. The Butler reaches the same goal with deterministic UUIDs. mudcat reaches it because it carries the ID of the producer.
- **Verifiable snapshot hashes.** Each snapshot carries a Merkle hash that anyone can verify. Thus anyone who holds the hashes can verify a release of parts that different tenants own. No tenant has to trust another tenant's database.
- **The federation feed.** Corrections and invalidations flow from producers, and completeness flows from custodians, as signed events. A failure to apply one is visible, because each custodian publishes the snapshot hashes that it holds completely.

### Decision table

| State | Within one authority | Across authorities |
|---|---|---|
| Bytes | Replicate, by replication rules | Copies only under an agreement, which carries obligations; enclave data never leaves |
| Catalog records | Single primary with read replicas | Federate; exchange published, immutable descriptions. Under shared custody, the producing tenant owns definitions and each custodian owns its replica records |
| Search and identity indexes | Derived and replicated freely | Each tenant builds its own; it may include harvested descriptions |
| Provenance | Single primary; run records as catalogued files | Exchange summaries; detail stays with the tenant that owns it |
| Identity | Delegate to the identity provider | Federate through OIDC |
| Access control | Delegate checks to the authorization port; the catalog owns structural relationships | Never shared; the tenant that owns the data decides |
| Usage policy and duties | Evaluated by mudcat | Duties travel with copies under the agreement |
| Invalidations and holds | Deletion workflow | Propagated over the federation feed; honoured as the agreement specifies |
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

1. A job names its inputs with their roles, for example a dataset at a tag ref in the training role. It also states the action and the purpose.
2. The resolver fixes each input to an exact snapshot. The policy engine applies exclusion lists and the invalidation set, and evaluates usage policy. The resolver then writes the resolution with its policy decision.
3. A suitable representation may not exist on a storage resource near the target cluster. In that case, mudcat creates a materialisation and a replication rule. The reconciler completes them.
4. The job gets a resolved manifest, credentials and a run token from the resolver. Then it reads storage directly.
5. At the end, the job publishes checkpoints as model versions. The catalog records the training activity with `observed` and `declared` relations, and the full run record as a bundle.

### Evaluate a model

1. An evaluation activity uses a model and an evaluation dataset.
2. Before the catalog accepts results, it checks the similarity criteria of the evaluation dataset against the training inputs of the model. It records a leakage assertion either way.

### Take down a source

This is the deletion workflow above. An invalidation of a source or element starts it.

## Deployment profiles

| Profile | Components | Use |
|---|---|---|
| Development | Single process with PostgreSQL in a container; plugins run in-process | Laptops, CI, tests |
| Lab | Catalog API, ingest writer and index builders on one node, PostgreSQL, one site agent per storage resource | AI-for-science startups and single-site groups |
| Multi-site | One instance per administrative domain, each with its own tenant; PostgreSQL with read replicas, agents at every storage resource, optional OpenSearch, federation feeds between tenants | Collaborations, multi-cloud labs, national facilities |
| Hosted | A vendor runs the control plane for many tenants; site agents run in each customer's environment. See [hosted.md](hosted.md) | Teams that do not want to operate mudcat |

A single PostgreSQL primary is a single-region write dependency. Multi-region deployments either accept that and use read replicas elsewhere, or run one instance per region and federate the tenants. This document proposes no multi-primary design.

## Scale envelope

| Quantity | Target | Notes |
|---|---|---|
| Catalog entities | 10^9 (TBR) versions and replicas per tenant | PF-1. Postgres-first catalogs (MetaCat, pgstac) report 10^8; 10^9 needs tests |
| Elements | 10^11 (TBR) per tenant | PF-2. In manifests; never rows in PostgreSQL |
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

1. **Core catalog.** Namespace, artifacts, versions, replicas, structure descriptors with the CSVW model, typed metadata and the query language. This step also includes bulk ingest, the Python SDK and CLI, and an importer from current Datacat databases.
2. **Snapshots and provenance.** Datasets, snapshots, annotation sets, PROV relations, the run context, OpenLineage and MLflow ingest, and the change log.
3. **Policy and resolution.** Sources, usage grants and duties, the terms compiler, and policy evaluation at resolution. This step also includes the authorization port with the built-in adapter, resolved manifests and credentials, and data loader integrations. SpiceDB and OpenFGA adapters follow after the conformance suite exists.
4. **Identity and evaluation.** The content-identity index (exact checksums first), similarity criteria, leakage assertions, and the deletion workflow.
5. **Placement and federation.** Replication rules, the reconciler, movers, completeness, and federation feeds.
6. **Gateways.** Iceberg REST, field gateways, Croissant, DCAT and RO-Crate export, and the MCP server, added as users request them. MCP is cheap and can come earlier.

Each step is useful without the next. The first two together are already a better Datacat.

## Open questions

- **Bulk wire format:** Arrow Flight, or Arrow IPC over plain HTTP. Flight is faster but harder to put behind ordinary gateways.
- **Single-row writes:** if they should also go through the ingest queue. This gives simpler consistency, but at the cost of latency.
- **Element-level policy:** how far it should go beyond exclusion lists.
- **Federation trust:** how tenants authenticate one another, and which agreement terms are necessary before a tenant honours received invalidations automatically.
- **Authorization source of truth:** if the external service should own grants when a deployment has one, instead of mudcat as owner with a synchronised copy.
- **Embedded mode:** if an embedded SQLite mode is worth its maintenance cost next to PostgreSQL.
