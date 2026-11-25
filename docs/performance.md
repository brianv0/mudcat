# mudcat performance

Status: first measurements of the Rust implementation ([implementation.md](implementation.md)) against the performance requirements PF-3 to PF-8 of [requirements.md](requirements.md), and of the main flows. The raw results are in [evals/results/bench.json](../evals/results/bench.json); `target/release/mudcat-bench` makes them again.

## Setup

| | |
|---|---|
| Machine | One virtual machine: 4 CPUs, 15 GB of memory. The benchmark, PostgreSQL and the simulated storage share the CPUs |
| Database | PostgreSQL 16.15, local, `shared_buffers = 1GB`, `synchronous_commit = off`, `fsync = on` (scripts/dev-db.sh) |
| Build | `cargo build --release` |
| Scale | 1,000,000 versions with one replica each for registration, queries and the tenant move; 10,000 and 100,000 members for resolution; 50,000 documents for search |
| Method | Each benchmark runs in its own tenant of one database, so the tables also hold the rows of the other tenants (about 2.9 million versions in all). Latencies are the median (p50) of 5 to 20 runs, or of all operations of a load test |

These are numbers for one small machine. They show the shape of the costs and where the design is sensitive, not the limits of a production deployment. With `synchronous_commit = on`, each commit waits for a disk flush, which adds the flush latency of the disk to the commit numbers.

## Results against the requirements

| Requirement | Measurement | Result |
|---|---|---|
| PF-3 bulk registration (TBD versions per minute) | Versions with a replica, idempotent on (artifact, checksum), in transactions of 1,000 or 5,000 | 160,000 per minute with 1 worker; **about 500,000 per minute with 4 workers**; 420,000 to 450,000 with 8 and 16 workers (4 CPUs). A repeated batch of 5,000 makes no new version |
| PF-4 first page of a metadata query over 10^8 (TBR) versions | First page of 100, over 1,000,000 versions of one tenant | **5 to 60 ms** for path globs, name globs, typed-index conditions that match many entities, declared and undeclared attributes and `permitted()`; **220 to 290 ms** for a selective typed-index condition and for a range on a column with no matching order |
| PF-5 commit to a dataset, with concurrent writers | Commits of 10 members to one branch of a dataset of 1,000 to 4,200 members | **13 ms** for one writer (74 commits per second). 8 writers: 29 commits per second, p50 230 ms; 32 writers: 70 commits per second, p50 430 ms, no retry. Commits that name the snapshot: 15 and 28 commits per second, with 2,051 and 6,820 conflicts that the clients retried |
| PF-6 ancestry query | A chain of 200 fine-tuned models, each with its training run, back to a dataset of 1,000 versions | **0.57 s** for the whole ancestry (402 nodes); 42 ms to reach a depth limit of 10 |
| PF-7 error at a limit | The same chain with a limit of 100 nodes | The error names the limit, after 0.14 s; no partial result |
| PF-8 policy predicate without a read of members | `permitted(train, research)` on snapshots of 10,000 and 100,000 members | **1.2 to 1.4 ms** from the effective-policy cache, for both sizes; the refresh of the cache takes 17 ms and 153 ms |
| PF-9 a new indexed attribute without a new index | Payloads with three attributes on 1,000,000 versions, two of them in the typed attribute index | 35 s for the payloads and the index rows (set-based); no database index was made |

## Other flows

| Flow | Result |
|---|---|
| Change stream relay (EV-13) | 18,700 events per second from the outbox to positions, in commit order (3,000,137 events) |
| Change stream poll (EV-3) | 13,000 to 45,000 events per second in pages of 10,000 |
| Commit to visible in the stream (EV-1) | p50 **22 to 30 ms**, p95 40 ms, with the relay run by the poll |
| Resolution (RD-2) | 10,000 members: 0.42 s; 100,000 members: 2.8 s, about 28 µs for each member: plan, replica choice, credentials and the record of the resolution |
| Resolution with 1,000 invalidations (UP-9) | 10,000 members: 0.32 s; 100,000 members: 2.7 s: the invalidation set adds no measurable cost |
| Publication (ID-5) | Merkle hash over 4,200 members: 38 ms |
| Models that used a source (QY-1) | 34 ms over 17 models |
| Search index build (OP-2) | 420 documents per second from the change stream, with 4 workers |
| Search (EV-9, EV-10) | p50 **310 to 350 ms** over 50,000 documents, with a spelling error or not, including the check of each result against the catalog (EV-11) |
| Search index rebuild (OP-3) | 50,000 documents in 400 s, in one transaction, while the old version serves |
| Tenant move (OP-14) | 12,000,738 rows (1,000,000 versions with their replicas, events and history) in 370 s: 32,000 rows per second |

## What the measurements found

The first full run had several results that were far from the targets. Each had a cause in the design or in the use of PostgreSQL, and each fix is in the code now.

| Finding | Before | After | Cause and fix |
|---|---|---|---|
| The order number of a commit scanned the outbox | 16 workers: 224,000 versions per minute; commit to visible: 146 ms | 423,000 per minute; 22 ms | A commit sets its commit-order number on its outbox rows. Without an index on the commit ID, this was a scan of the outbox under the commit lock of the tenant. An index fixed it |
| Row-level security and planner statistics | Lookups by key that took minutes | Milliseconds | The planner evaluates `mc_tenant()` when it estimates. A tenant that the statistics do not know yet looks empty, so a scan of the whole tenant looks as cheap as a lookup by key. The tenant column now has no statistics; the derived search tables keep them, because they are read by scans |
| First page of a query | 4.6 s for a path glob over 1,000,000 versions | 5 ms | The planner cannot estimate the join of an artifact to its latest version from the version side (two correlated columns), so it hashed both tables and sorted them. The query now drives from the artifacts in key order, joins the latest version by its key, and compiles each metadata condition for the smaller side: from the typed index when it matches few entities (counted up to 20,000), or as a lookup for each row otherwise |
| Commits under contention | 32 writers: 12 commits per second | 70 | Each commit read all members of the parent snapshot and copied its manifest references one row at a time while it held the branch lock. A commit now reads only the members that it names, takes counts and sizes from the summary of the parent, and copies the references with one statement |
| Search | 0.95 s | 0.31 s | The spelling tolerance computed trigram similarity against every document that contains a common word. It now finds close words in the vocabulary of the index and expands the lexical query. The check of the results is one bulk check, with one audit entry for each search |
| Lineage across models | 2 nodes | 402 nodes | The traversal did not follow `fineTunedFrom` from an entity that also had a generating activity. This was a defect, not a performance problem; the benchmark found it |

## Open points

- **Registration is bound by index maintenance.** Each version writes about 25 index entries: artifact, identifier, version, replica, replica history, entity and outbox rows, and the content-identity index. One worker reaches 160,000 versions per minute; more workers than CPUs do not help. The 10^9 versions of PF-1 need partitioning of the largest tables, which the schema does not have yet.
- **Contended commits are serial.** The writers of one branch hold its row lock while they build the snapshot. A commit that names the snapshot must retry after each other commit, so 32 such writers make thousands of retries. Add-only commits never retry, as the design intends. A higher rate needs the batched snapshots of the ingest writer (architecture.md, "Commit cost").
- **The commit lock of a tenant is serial.** It covers only the end of each writing transaction: the order number and the commit. With `synchronous_commit = on` it also covers the flush to disk, which caps the commits of one tenant at the flush rate of the disk.
- **The first page of a selective query** takes 200 to 300 ms because it reads and sorts every match of the condition. A cursor over the typed index in key order would make it constant.
- **Index rebuilds are single-threaded.** The rebuild writes the new version in one transaction, at 125 documents per second.
- **Scale.** The requirements name 10^8 and 10^9 versions; these measurements stop at about 3 million in one database. The costs above grow with the matches of a query, not with the size of the tenant, except for registration (index depth) and the tenant move (linear).
