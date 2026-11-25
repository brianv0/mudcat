# mudcat implementation

Status: a working reference implementation in Rust of the design in [model.md](model.md), [architecture.md](architecture.md) and [hosted.md](hosted.md). It passes all 280 variants of the 15 task templates in [evals/](../evals/README.md), with the 16 invariants. Performance measurements are in [performance.md](performance.md).

## Build and run

```sh
scripts/dev-db.sh start                 # PostgreSQL 16 on localhost:54329, the only service that mudcat needs
cargo build --release
target/release/mudcat init              # apply the schema
target/release/mudcat create-tenant group-a --admin user:alice
target/release/mudcat token group-a user:alice
target/release/mudcat serve --addr 127.0.0.1:8080
# each tenant has its own endpoint: group-a.mudcat.localhost
curl -H 'host: group-a.mudcat.localhost' -H "authorization: Bearer $TOKEN" localhost:8080/openapi.json

target/release/mudcat-evals --scale full        # all 280 variants with the invariants
target/release/mudcat-evals --template T3.2 -v  # one template, each check
target/release/mudcat-bench                     # the benchmarks of performance.md
cargo test                                      # the reference scenarios as integration tests
```

`mudcat authz-schema spicedb` and `mudcat authz-schema openfga` print the neutral authorization schema compiled for the two external services.

## Map of the code

| Module | Design | Contents |
|---|---|---|
| `schema.sql` | architecture.md, "Catalog database", "Tenants" | One schema for one tenant and for many (OP-13). The tenant ID is the first column of every key and defaults to the tenant of the session; row-level security is forced, also for the owner of the tables |
| `instance.rs` | "Tenants" | The instance, the tenant-scoped session and transaction, the commit record of each write (EV-14), the outbox writer, the clock |
| `namespace.rs` | model.md section 1 | Storage resources, collections with ACL roots and mounts, artifacts, versions, replicas, identifiers with history. Registration is idempotent on (artifact, checksum); batches use set-based statements |
| `metadata.rs` | model.md section 3 | Payloads per namespace with an owner and writers (ST-9), validation against declared attributes (ST-6), assertion origin (ST-7), the typed attribute index (PF-9), customer-held values (ST-10), tags with inheritance and propagation (ST-17), enrichers and review (ST-13 to ST-15) |
| `datasets.rs` | model.md section 4 | Datasets, snapshots over shared manifest references, branch and tag refs with history, commits with requirements, publication with Merkle hashes, method equivalence, changes between snapshots (EV-12), cheap copies inside a tenant and between tenants (ID-13), selections, annotation sets, representations, sealing (OP-6) and expiry (ID-9) |
| `policy.rs` | model.md section 5 | Sources, usage grants, duties, the terms compiler for SPDX and ODRL with a review queue (UP-5, UP-6), the evaluation over sources, the effective-policy cache (PF-8), the invalidation set (UP-9), legal holds and their retention compartments (UP-8), purge with deletion receipts (UP-7), erasure (UP-11), flags on models |
| `authz.rs` | "Authorization port" | The built-in adapter: relationship tuples, ACL roots (AC-5), compartments, exclusions, per-object conditions such as an embargo, membership sources with an age limit, fail-closed behaviour (AC-6), the audit of each decision (AC-8), the neutral schema |
| `resolver.rs` | "Resolver", "Planning with statistics" | Inputs fixed to exact snapshots and versions (UP-3), selections, invalidations, usage policy over all sources (UP-2), jurisdiction rules for the caller (RD-9), replica choice by place (RD-4), plans by structure kind with selectors (ST-4, ST-5, RD-7), prefix credentials only inside one ACL root (RD-3), resolution again of an old resolution (QY-8) |
| `provenance.rs` | model.md section 6 and 7 | Runs as activities with qualified usage and roles (PR-2), the run record as a catalogued version (PR-5), observed usage from run tokens (PR-4), models and derivation, lineage with limits (PF-7), the reports QY-1, QY-2 (training-content summary), QY-5, QY-6 |
| `leakage.rs` | model.md section 7 | Similarity criteria and leakage assertions (QY-3), computed from the content-identity index and from selectors, with no scan of data |
| `placement.rs` | model.md section 8, "Placement reconciler" | Replication rules, transfers, completeness records (PL-1), the mover, site agents that lease tasks (verification, deletion), inventory reports with dark data and lost replicas (PL-6), the read connector |
| `stream.rs` | "Change stream" | The relay that gives positions in commit order (EV-13), polling with a retention floor, subscriptions filtered by identity (EV-6), backfill (EV-5), state documents (EV-4), removal events (EV-7) |
| `search.rs` | "Search", "Index builders" | The derived index fed from the change stream, online rebuild (OP-3), lexical, trigram and vector match with reciprocal rank fusion (EV-9, EV-10), and the check of each result against the catalog (EV-11) |
| `query.rs` | "Query compiler" | The query language: path globs and predicates over columns, declared and undeclared attributes, `permitted()`, `used_in()`, `derived_from()`, `tag()`, compiled to SQL with parameters, in pages |
| `tenants.rs` | hosted.md, "Tenancy" | Tenant IDs with 122 random bits (ID-11), the move of a tenant to its own database with a write fence and rollback (OP-14) |
| `auth.rs`, `api.rs` | "Catalog API", "Gateways", "Identity and authentication" | Tokens with the tenant as audience, the tenant resolver from hostname and token (OP-12), sign-in through an outside account and through a pipeline identity, the REST interface with OpenAPI (IN-2), JSON-LD (IN-1), the Iceberg REST catalog (IN-5), the MCP server (IN-6) |
| `export.rs` | "Metadata export" | DCAT and Croissant 1.1 descriptions of published snapshots (IN-4), the duties of a model at export |
| `storage.rs`, `plugins.rs` | "Format plugins and site agents" | A simulated object store and POSIX file system with credentials, object versions and an access log; format plugins that read a header |
| `evals/` | evals/README.md | The eval runner: a driver for each task template, the invariants, the mapping of results to the checks of tasks.yaml |

## What is simulated

mudcat does not store data, so the evals need a world around it. These parts are simulations in the same process, with the interfaces that the real ones have:

- **Storage.** Buckets and POSIX file systems on the local disk (`storage.rs`), with signed URLs, prefix credentials, versioned buckets and an access log. Clients read through the read connector with the credentials that the resolver issued; the evals check that mudcat served no bytes (INV-1).
- **Movers and site agents.** In-process, but they act only through the catalog interfaces: leases, tasks, results.
- **Identity providers.** A GitHub-like organisation with teams and tokens, and a CI system that signs pipeline identity tokens.
- **Format plugins.** Simulated files carry a one-line header that stands in for the index of a real format.
- **Embeddings.** A hashed bag of words and trigrams, with a model version, in place of an embedding model; vector similarity is computed in Rust, because pgvector is not installed in this environment.

## Decisions where the documents were silent

The evals contain decisions that the design documents do not state (evals/README.md lists some). The implementation makes these choices:

| Question | Decision |
|---|---|
| Commits that only add members | Applied to the newest snapshot of the branch, with no retry. A commit that removes or replaces members must name the current snapshot |
| Legal hold and removal | A hold blocks purge and erasure, not invalidation. Purge and erasure invalidate first; held content that is invalidated moves to the retention compartment of the hold, which only its members can read |
| The place option of a resolution | `strict` fails with the number of members that have no verified replica at the place; `prefer` takes the far replica for those members |
| Records of training runs | The training-content summary names each run with no resolution and states that it is not complete |
| Methods that differ | A commit gets a warning; publication is refused until an equivalence is declared, and the published snapshot refers to it |
| Terms that are compiled in part | A request is blocked for review if the source has no compiled grant for the action and the purpose, and the source has a clause in the review queue |
| Retrieval | `retrieve` is an action of usage grants, and `retrieval` a role of usage |
| Management modes of storage | `managed` or `external`. A POSIX resource in the external mode enforces access itself: the plan is a manifest file with paths, and mudcat issues no credentials |
| A snapshot as a member of a dataset | Permitted only for a published snapshot, so that the member has a Merkle hash |
| An as-of request for an expired snapshot | An error that states the expiry, never another snapshot |
| An embargo | An attribute definition with the access condition `embargo`; a tuple with the condition `embargo` grants access only after the date. No batch job changes a record when the embargo ends |
| A DOI | Minted only for a published snapshot; the snapshot then has a public read tuple |
| A background job with no tenant | It cannot insert (the tenant ID has no default) and row-level security shows it no rows |
| A tenant move with writes | Writers hold a shared advisory lock of the tenant and check its status and database; the move takes the lock exclusively. Refused writes get a retryable error |
| Follow rules | A dataset can follow a collection; applying the rule commits the latest versions to main |
| Search and access | The coarse filter on ACL roots runs on the candidates of the index, and each denial goes to the audit log; then the catalog checks each result, its newest version and the invalidation set |

## Findings

- **The relay couples the latency of tenants.** The relay publishes only events of transactions older than every transaction in progress, as the design requires. In PostgreSQL this horizon covers all transactions of the cluster, so a long transaction of one tenant (or of another database on the same server) delays the stream of every other tenant. The order is never wrong; only the latency grows. A deployment should keep write transactions short, and use a dedicated database for tenants with long transactions.
- **Element-level invalidation needs the identity index.** A takedown by fingerprint must find the element inside derived files (shards, caches). The content-identity index holds the keys of the element lists of manifests, so the resolver can leave out single elements of a record or container version.
- **Statistics of versions make leakage checks cheap.** Time ranges as statistics of versions, and keys in the content-identity index, let mudcat compute every criterion of the evals without a read of data.

## Not implemented

These parts of the design are not in this implementation: the SpiceDB and OpenFGA adapters (the schema compiles for both, but the port has only the built-in adapter); Apache Arrow as the bulk wire format (the bulk interface uses JSON); Parquet or Lance manifest files (manifests are JSON Lines); OpenLineage and MLflow ingest; the signed federation feed between instances; the Python SDK, the web UI, the fsspec file system and the Datacat importer; JSON Schemas for each event type (events carry a `dataschema` URL only); the customer gateway as a separate service.
