# Apache Gravitino: decomposition and lessons for mudcat

Status: research note. This note breaks Apache Gravitino into systems, and it states what the code of each system shows that mudcat must decide, copy or avoid. The detailed evidence is in [detail/](detail/). The requirements that come from it are in [requirements.md](requirements.md). The quirks of the code are in [quirks-index.md](quirks-index.md).

## 1. Scope and method

- **Source.** The `apache/gravitino` repository, commit `9aa78cf` (2026-11-26), shallow clone. There is no git history. The history of a feature comes from SQL upgrade scripts, `TODO` comments, issue numbers in comments, test names and the files in `design-docs/`.
- **Method.** One reader for each of 13 subsystems read the code, the tests that fix edge cases, the SQL schema and the docs. Each reader wrote a decomposition, candidate requirements, a ledger of odd behaviour with `path:line` evidence, and a list of open items. The lead then checked the headline claims against the code.
- **Counts.** There are 13 detail files with 4,818 lines. They hold 741 candidate requirements (409 marked IMPL and 332 marked GAP) and 460 ledger entries. [requirements.md](requirements.md) merges the most useful candidates into 135 for mudcat. Many detail candidates overlap, because several readers found the same lesson from different sides.
- **Kinds.** IMPL means that Gravitino implements the rule. GAP means that Gravitino fails to implement it, or implements it badly, and mudcat must do better. A GAP is a lesson, not a criticism: Gravitino has more than ten years of compatibility to carry that mudcat does not.
- **Checked by the lead.** The lead confirmed these claims in the code:
  - The hidden-tag prefix `gravitino_hidden_` (`policy/HiddenPolicyTag.java`).
  - The `deleted_at` column in the unique and primary keys of the 2.0.0 PostgreSQL schema.
  - The comments that refer to issue #14279 in `TableOperationDispatcher`.
  - The replay window in `EntityChangeLogPoller`. Its own comment says that late transactions "may still be missed".
  - The statement in `IcebergIdempotencyManager` that request bodies "are not compared".
  - The file-system cache key of the GVFS client: scheme, authority and Hadoop user.
  - `EntityFieldLimits.MAX_NAME_LENGTH = 128`.
  - The regex principal mapper. It returns the original principal when the pattern does not match.
  - The call to `System.exit` before the shutdown hook is registered.
  - The default of 1 and the maximum of 10 for the retention of entity versions.
  - The JWT parser. It sets a signing key and a clock skew, and it requires no expiry claim and no issuer.
- **Not checked.** Nothing was built or run. Items marked `(inferred)` in the detail files are conclusions, not quotations. Line numbers in the detail files can be wrong by a few lines.
- **Limit.** Gravitino is a system whose metadata is mutable and whose objects are mostly tables. A GAP in Gravitino is not always a GAP for mudcat. Section 9 lists the differences.

## 2. What Gravitino is

Gravitino is a "federated metadata lake". It registers external systems as catalogs under one hierarchy: metalake, catalog, schema, object. The systems are Hive, JDBC databases, Iceberg, Paimon, Hudi, Glue, Kafka, Lance, filesets on file systems and models. For an external catalog, the external system stays the source of truth for structure. Gravitino keeps a thin registration row for each object, with an ID, an owner, tags, grants and statistics. It adds role-based access control, with optional push-down to Apache Ranger. It also adds tags, policies, statistics, jobs, event listeners, audit, lineage intake and engine connectors for Spark, Trino and Flink. It serves an Iceberg REST catalog and a Lance REST namespace as separate planes. The metadata store is one relational database: H2, MySQL or PostgreSQL.

## 3. Decomposition

### 3.1 Layers and subsystems

```
L4  Governance and operation      K  Tags, policies, statistics, semantic model   (10)
                                  L  Events, audit, lineage, metrics              (09)
                                  M  Jobs, maintenance, optimizer                 (11)
L3  Interface planes              I  Iceberg REST server, Lance REST              (08)
                                  J  Engine connectors, clients, MCP server       (13)
L2  Federation                    F  Catalog framework: dispatcher chain, SPI     (05)
                                  G  Relational and lakehouse providers           (06)
                                  H  Non-tabular providers: fileset, Kafka,
                                     model, Lance                                 (07)
L1  Security                      D  Authorization: RBAC, Ranger, tags            (03)
                                  E  Authentication, credential vending,
                                     secrets                                      (04)
L0  Foundation                    A  Backend store: schema, versions, GC          (01)
                                  B  Cache, locks, consistency, multi-node        (02)
                                  C  Server, REST conventions, config, lifecycle  (12)
```

The number in brackets is the detail file.

| Subsystem | Main Gravitino code | mudcat counterpart | mudcat areas | Candidate requirements | Ledger entries |
|---|---|---|---|---|---|
| A Backend store | `core/.../storage/**`, `scripts/{h2,mysql,postgresql}` | PostgreSQL catalog database | ID, OP, PF, EV | 60 | 35 |
| B Cache, lock, consistency | `core/.../cache`, `core/.../lock` | Commit model, relay, derived indexes | ID, EV, AC | 42 | 29 |
| C Server, API, config | `server/**`, `server-common/**`, `common/**`, `Configs` | REST interface, operation | IN, OP | 57 | 35 |
| D Authorization | `core/.../authorization`, `authorizations/**` | Authorization port, adapters | AC, UP | 55 | 30 |
| E Authentication and credentials | `core/.../auth`, `credential`, `secret`, `bundles/**` | Credential issue, run tokens | RD, AC, OP | 44 | 34 |
| F Catalog framework | `core/.../catalog`, `connector`, `api` | Registration of external formats | PL, ID, IN | 64 | 41 |
| G Relational and lakehouse providers | `catalogs/**`, `catalogs-contrib/**` | Format profiles, Iceberg gateway | ST, PL | 54 | 62 |
| H Non-tabular providers | `catalogs/catalog-fileset`, `-kafka`, `-model`, `lance/**` | Artifacts, versions, replicas | UP, RD, ST, PL | 62 | 30 |
| I Iceberg REST | `iceberg/**` | Iceberg REST gateway | IN, ID, RD | 56 | 46 |
| J Engines, clients, MCP | `spark-connector`, `trino-connector`, `flink-connector`, `clients/**`, `mcp-server` | Interfaces, MCP, Ray connector | IN, QY | 52 | 34 |
| K Tags, policy, statistics | `core/.../tag`, `policy`, `stats`, `semantic`, `domain` | Tags, usage policy, expectations | ST, UP | 62 | 30 |
| L Events, audit, lineage | `core/.../listener`, `audit`, `lineage/**` | Change stream, audit, provenance | EV, PR, AC | 62 | 27 |
| M Jobs, maintenance | `core/.../job`, `maintenance/**`, `plugins/**` | Enrichers, compaction, purge | PR, UP, OP | 71 | 27 |

### 3.2 The path of one request

The reader of subsystem F traced the chain in `GravitinoEnv.java` (around lines 1105 to 1200):

```
REST resource  (authorization filter runs here, before the chain)
 -> EventDispatcher      pre-event, post-event, failure event
 -> NormalizeDispatcher  name rules, case folding, capability checks
 -> HookDispatcher       owner assignment, push-down to the external authorizer, rollback
 -> OperationDispatcher  tree locks, connector call, entity-store write, import or merge
     -> CatalogManager   lease on a cached catalog wrapper, classloader swap
         -> connector (isolated classloader) -> external system
```

A second, internal chain has only the Normalize and Operation layers. Server components use it, so their calls produce no events and no hooks.

Three facts about this chain decide much of what follows:

1. **The authorization filter runs before name normalisation.** Authorization and the operation must use one resolved identifier, or a name that normalises differently can reach a different object. (CF-R26)
2. **Hooks run around the operation, not inside its transaction.** The grant is committed first. The call to the external authorizer comes after it, and its result is discarded. (AZ-R35 to AZ-R41)
3. **The store write is a second step after the external change.** If the external change succeeds and the store write fails, the error text tells the user to load the object, not to repeat the create. (CF-R14)

### 3.3 The store

- One SPI (`EntityStore`) with one real implementation (`relational`). Above it sit an entity cache (per node, Redis designed but not built), a name-to-ID resolver, pending-write fences and a change-log poller.
- 52 tables in schema 2.0.0. Every table has `deleted_at BIGINT NOT NULL DEFAULT 0` (0 = live) in each unique key. Entity IDs are random 63-bit integers minted in Java.
- Three SQL dialects. MyBatis picks a provider for each dialect. The base provider is written in MySQL style, PostgreSQL has about 38 overrides, and H2 runs in MySQL mode.
- Relations (owner, tags, roles, policy bindings) are polymorphic pairs of (ID, type) with no foreign key. Cascades are written by hand, and a garbage collector removes orphans.

## 4. Lessons

Each lesson gives what the code shows, the evidence in the detail files, and what it means for mudcat. A reference such as CF-R4 is a candidate requirement in a detail file. "Status" compares the lesson with [../../requirements.md](../../requirements.md).

### Lesson 1. Two systems of record break identity, atomicity and deletion

- **What the code shows.** The external system owns structure. Gravitino owns the ID. To keep the two together, Gravitino writes its ID into the external object. For Hive it is a table parameter. For JDBC it is a suffix in the comment: `(From Gravitino, DO NOT EDIT: gravitino.v1.uid<n>)`. For a Kafka topic it is a truncated UUID. A MySQL schema has no field for it, so its identity is its name. (CF-R4, RL-R26, RL-R27, FS-R48)
- An object gets its ID only when someone first loads it, and the load writes to the store under a write lock. A tag or a grant can attach only after that load. (CF-R9)
- Create is two steps with no shared transaction. A failure between them leaves an object in the external system that the store does not know. (CF-R14 to CF-R16, IR-R49)
- There is no way to unregister one object. To fix an identity conflict, an operator force-drops the whole catalog and loses its owners, grants and tags. The primary keys had to change to `(id, deleted_at)` so that a deleted ID can register again. (CF-R7, CF-R8)
- A load that races with an alter can drop the IDs of columns, and with them the tags on the columns. The code comments say so and refer to issue #14279, which is open. (CF-R17, CF-R18)
- **For mudcat.** Keep identity in the mudcat database only. Register an external object with no write to it. Assign the ID at registration, not at first read. Offer an unregister for one object. Record the external version that each registration saw, and refuse an update that rests on an older one.
- **Status.** New. mudcat has "register, don't replace", but no requirement states that registration writes nothing to the external system.

### Lesson 2. Optimistic concurrency is local and bespoke

- **What the code shows.** One alter operation (semantic model) takes an `expectedRevision`. Every other alter is last-writer-wins. Creates and drops have no idempotency key. A drop of an object that does not exist returns success with `dropped: false`. (SV-R1 to SV-R6)
- In the store, every write checks `current_version`. Parent rows are locked with `FOR SHARE` or `FOR UPDATE` so that nobody creates a child under a parent that a transaction is dropping. Locks follow one order. Version 2.0 added `occ_version`, because one column had served as both the pointer to the history row and the concurrency token. (BS-R12 to BS-R16)
- Gravitino also has JVM-local tree locks. Its own comments say that they cannot order writes between servers. (CL-R29)
- **For mudcat.** The commit model (ID-7, ID-14 to ID-21) already covers the main idea. The evidence adds three points. A precondition must be possible on every alter, drop and ref move. The concurrency token must be separate from the number of the content version. No write may depend on an in-process lock.
- **Status.** Covered by ID-7 and ID-14 to ID-21. Tightened by candidates G-ID-3, G-ID-4 and G-OP-3.

### Lesson 3. An auto-increment log is not a commit-ordered change stream

- **What the code shows.** `entity_change_log` is written in the same transaction as the change. Its IDs come from a sequence, so rows commit out of ID order. A poller that reads "everything after the last ID" can skip a row that commits late. Gravitino reads a 60-second replay window again and remembers recent IDs to remove duplicates. The code comment and the docs say that the result is "bounded compensation, not lossless". A listener that fails is not retried, because the shared cursor moves on. Retention of the log is by time. On PostgreSQL, `CURRENT_TIMESTAMP` is the start time of the transaction, not the commit time. (BS-R45, CL-R10 to CL-R14, EA-R24 to EA-R27, SV-R53)
- **For mudcat.** This is the "relay that orders commits" that [open-items.md](../../open-items.md) lists as unmeasured. Gravitino shows what happens without it. A change record must be visible to consumers only when no lower-numbered transaction can still commit. Each consumer needs its own cursor. The log must keep a record until all consumers have passed it, or mark a consumer as lost.
- **Status.** Covered in intent by EV-3, EV-13 and ID-21. Tightened by candidates G-EV-1 to G-EV-4.

### Lesson 4. A deletion marker in the unique key costs a lot

- **What the code shows.** All unique keys include `deleted_at`, in milliseconds, with 0 for live rows. Two deletes in the same millisecond collide, and only the domain relation table has a patch for this. A retired row keeps its ID, so a new entity with the same ID could inherit owners, tags and grants. The retention of deleted rows is capped at 30 days, and the retention of versions at 10, so history is not immutable. The single-owner rule was never enforced by a constraint, and the 2.0 upgrade merges duplicate owners. (BS-R1, BS-R2, BS-R5, BS-R8, BS-R11, BS-R28)
- **For mudcat.** Use a PostgreSQL partial unique index on live rows. Do not put a timestamp in a key. Do not cap the history of published versions. Enforce single-valued relations by constraint.
- **Status.** Conflicts with the use of a count limit on versions only if mudcat adds one. UP-8 and ID-8 already forbid it for published snapshots. New candidates G-ID-1, G-ID-2.

### Lesson 5. Pushing grants to an external enforcement point needs an outbox

- **What the code shows.** The grant commits to the database. Then a plugin (Ranger, or JDBC) receives it. The plugin returns a Boolean, which the caller discards. Failures of owner roles in Ranger are swallowed. A chain of plugins stops at the first `false` with no rollback. Nothing compares the two policy states later. (AZ-R35 to AZ-R41)
- A group-membership bug was a real security defect. Shared enforcer state held group membership that came from the IdP claims of one request. Interleaved requests with different claims could add an allow or drop a deny. The fix derives membership for each request. (AZ-R18)
- To check a request, the authorizer holds an in-memory copy of all grants. It costs a 2,363-line class with generation counters, partial-load markers, back-off and fail-closed reloads, and 107 race tests. A better design evaluates against a snapshot in the database, with a revision token. (AZ-R29 to AZ-R31)
- Ownership and deny disagree. An owner of an ancestor bypasses a deny, and an owner of the leaf does not. The docs and the code also disagree on allow plus deny in one role. Inheritance is written in regex macros and a separate registry, so a new privilege touches about eight places. Ranger maps `USE_CATALOG` to SQL `SELECT`. (AZ-R2, AZ-R4 to AZ-R6, AZ-R39)
- Audit records only denials. Nothing answers "who can read X, and by which path". (AZ-R48, AZ-R50, AZ-R51)
- **For mudcat.** The authorization port (AC-1 to AC-4) is the right shape. Add these rules:
  - A consistency token on each check and each write (AZ-R30, CL-R22).
  - An outbox with per-point state for adapters that push to an external service (AZ-R35 to AZ-R37).
  - One table of permissions and scopes (AZ-R2).
  - One stated rule for owner against deny (AZ-R5).
  - Restrictions that no grant can override (AZ-R48).
  - The record of the path that allowed an access (AZ-R50).
- **Status.** AC-6 to AC-8 cover part. The rest is new: G-AC-1 to G-AC-10 and G-AC-16, G-AC-17.

### Lesson 6. Credential vending is the sharpest edge

- **What the code shows.** The scope of a vended credential comes from table metadata that the client supplied (`location`, `write.data.path`). Nothing checks that the path is inside a registered storage root. Paths go unescaped into a GCS CEL condition and into S3, OSS and COS ARNs, where `*` is a wildcard. Nothing checks the size limit of an AWS or GCS policy. (CR-R1, CR-R2, CR-R6)
- The server decides read or write from the privileges of the caller. A read-only job gets a write credential. A caller cannot ask for read-only, a sub-prefix or a shorter lifetime. STS and SAS tokens cannot be revoked, and the cache serves a token for its whole lifetime (default 3,600 seconds). (CR-R3 to CR-R5, CR-R8)
- If `credential-providers` is missing, the server falls back to a long-lived static key. An Iceberg JDBC catalog hands out the password of its own backing database. Lance REST gives the static storage keys of the catalog to any caller who may describe a table. Issuance on the fileset endpoint is not audited. (CR-R15, CR-R17, CR-R19, FS-R32)
- Secrets are in plaintext in the database. The in-memory secret provider is the only one that ships, and the KMS SPI only inspects keys. Redaction is by substring of the key name, with a separate list for audit logs. (CR-R35 to CR-R37)
- Authentication has these gaps (CR-R26 to CR-R33):
  - A JWT needs no `exp` claim, and the static-key path has no issuer check.
  - The regex principal mapper accepts a principal that does not match.
  - The principal has no realm.
  - `simple` and `basic` share the `Basic` header.
  - Local passwords use SHA3-512 with 5,000 iterations, and there is no upgrade path.
- **What to copy.** Separate grants for dynamic (short-lived) and static (long-lived) credentials. A check that a named location is one of the registered locations of the object. A per-caller cloud session name. The choice, per federated catalog, of token forwarding or a service account. (CR-R16, CR-R18, CR-R21, CR-R24)
- **For mudcat.** RD-3 states the right rule: a prefix inside one ACL root. The evidence adds four requirements. The credential is the intersection of the request and the grant. The server refuses a request when the provider cannot express the scope. A stated bound limits the delay between a revocation and the end of all issued credentials. There is no fallback to a long-lived key.
- **Status.** RD-3 covers the root. The rest is new: G-RD-1 to G-RD-12, and for authentication G-AC-11 to G-AC-15.

### Lesson 7. A file-like object forced into a table-shaped model

- **What the code shows.** A fileset is MANAGED or EXTERNAL. This is a mutable flag, `WRITE_FILESET` alone can change it, and it decides if a drop deletes bytes. With `disable-filesystem-ops=true`, a MANAGED drop deletes nothing. The delete of bytes runs inside the database transaction and is not atomic over several locations. (FS-R1 to FS-R12)
- Lance deletion was hardened later. Datasets created or registered through REST are forced external, and purge needs an opt-in by an administrator. Registration does not check who owns the storage location. (FS-R57)
- The rule for the default location exists in four versions: server, Java client, Python client and the legacy case. A legacy single-location fileset has no `default-location-name`, and no migration adds it. (FS-R25)
- Model versions: moving an alias is two writes with no transaction. The numeric-alias ban is checked on link only. A URI can change, and no content digest exists. (FS-R39 to FS-R42)
- Path safety is good, and tests fix it. The server refuses `..`, NUL and sibling-prefix escapes before any storage call. It decides containment by path ancestry, not by string prefix. (FS-R17 to FS-R22)
- The Java GVFS client shares one `FileSystem` for filesets in the same bucket, because the cache key has no fileset. Credentials scoped to one fileset can serve another. This is inferred, not run. (FS-R30)
- **For mudcat.** This is the nearest Gravitino comes to "not everything is a table", and it shows the cost of treating bytes as a property of a record. Ownership of bytes must be a separate fact with its own privilege. Unregister must never touch bytes. The intent to delete must be stored before bytes go, and the record must stay until deletion is confirmed.
- **Status.** UP-7, UP-8 and PL-7 cover purge. New: G-UP-1 to G-UP-6, G-RD-10.

### Lesson 8. The Iceberg REST server is a thin wrapper, and that has a price

- **What the code shows.** The server is a wrapper over the upstream `CatalogHandlers`, so many spec rules come from the library. Gravitino adds `IcebergCommitValidation`, injected as an extra `UpdateRequirement` so that it runs again on each retry. No run of the upstream conformance kit exists in `iceberg/**`. The tests of Gravitino are the only evidence. (IR-R2, IR-R9 to IR-R11)
- Reflection on private fields, planted classes in `org.apache.iceberg.*` and a fork of Iceberg 1.13 for read restrictions are the main arguments for a native implementation. (IR-R53)
- The idempotency design is durable, with generation-fence tokens. It has weaknesses. A replay compares the method, path and query, not the body. The key is the only primary key, with no principal or tenant. The stored response can contain vended credentials (inferred). The record is not in the transaction of the mutation. Expiry uses the clocks of replicas. (IR-R13 to IR-R21)
- Table-location overlap is a security boundary, because credentials are scoped by storage prefix. Create, register and set-location need load access to every table that overlaps. On backends other than JDBC the lookup scans the catalog. (IR-R26, IR-R27)
- A nested namespace is flattened with `:` and `.` is refused, so `["a:b"]` and `["a","b"]` alias to one schema. Gravitino blocks it only when authorization is on. The design promised per-segment encoding, and the code does not do it. (IR-R8)
- Docs and code disagree in four places. The read-restriction capability header is not implemented. Restrictions apply on load-table only, not on scan plan or credentials. The docs say plan tasks are implemented, and the code rejects all of them. The docs say register-view is missing, and it is served and advertised. (IR-R42, IR-R43, IR-R55)
- **For mudcat.** The evidence supports IN-14 to IN-17 and the design choice to implement the contract natively. It also tightens ID-16. A replay must compare a digest of the body. The record must be scoped to the principal, must not store credentials, must share the transaction of the mutation and must use the database clock.
- **Status.** Covered by IN-14 to IN-16 in outline. Tightening: G-IN-1 to G-IN-6 and G-ID-5 to G-ID-8.

### Lesson 9. Events and audit are two systems that should be one

- **What the code shows.** Audit is a listener on an in-memory bus: queue size 3,000, silent drop when full, 3-second drain on shutdown, lost on a crash. The change log is in the transaction. A veto by a synchronous listener produces no failure event, so audit sees a generic 403. Listeners run in the order of a `HashMap`, not in the configured order. The Javadoc says that listener exceptions propagate. The production wrapper swallows them. (EA-R13, EA-R16, EA-R17, EA-R21)
- Credential issuance, secrets, lineage intake, `*Exists` checks and `import*` produce no operation event, only a generic HTTP event with an unknown operation and a null identifier. Events carry names, not entity IDs, and no request ID, event ID, tenant or commit. (EA-R2, EA-R3, EA-R6 to EA-R11)
- Events hand unredacted properties to listeners. Only the two audit formatters redact. The resolution of the client address through a trusted proxy is good. (EA-R10, EA-R35, EA-R36)
- Lineage is receive-and-forward. Nothing is stored or linked to catalog objects, and the processor is a no-op. HTTP 201 means "queued". The convention is namespace = metalake and name = `catalog.schema.table`, with a `datasetType` facet. An output dataset needs only read visibility. The sender is not recorded. (EA-R44 to EA-R51)
- **For mudcat.** The audit record and the change record are one record, written in the transaction of the change. Add the request ID, the entity ID, the tenant, and the commit that the request saw and made. A lineage claim about an output needs write access or a run token. Record the sender separately from the producer that the event claims.
- **Status.** AC-8, EV-1, PR-4 and PR-6 cover the intent. New: G-EV-5 to G-EV-11 and G-PR-1 to G-PR-4.

### Lesson 10. Tags carry policy, so inheritance rules are security rules

- **What the code shows.** Tag inheritance resolves nearest-first by tag name, and a child assignment replaces the parent assignment with its values (`tag/EffectiveTagResolver.java`). The Gravitino read-restriction design admits that a table can set `data_access=public` to escape an inherited restriction. The REST list path repeats the logic. It runs one query for each ancestor and filters by authorization after the merge. An invisible nearer tag therefore hides a visible inherited one. (TP-R4, TP-R5, TP-R17)
- Policies attach only to tags, with a selector for presence or one exact value. Two row-filter policies, or two column-mask policies, on one target fail closed. Policy versions are kept under a global retention of 1 by default, so a past decision cannot be explained (verified: default 1, maximum 10). (TP-R26 to TP-R35)
- Policies are bound to tags through a hidden tag named `gravitino_hidden_<id>`, in the user namespace. A name collision needs a restore from backup. (SV-R29)
- Cascades are written by hand: a per-container SQL `UNION` over every entity type, plus a periodic sweep. Partition statistics are not removed when a table drops. Statistics exist for tables only and must carry a `custom-` prefix. They are written in an optimistic batch. They carry no reference to the data version, so nobody can judge staleness. (TP-R19 to TP-R22, TP-R40 to TP-R47)
- The semantic model (revisions, `expectedRevision`, a latest pointer and an active pointer) fits the mudcat idea of immutable versions and mutable pointers. The code lacks the `PUBLISHED_EXPLICITLY` state that the design promises. Glossary, knowledge-base article and ontology exist only as design documents.
- **For mudcat.** ST-17 (inheritance of tags) needs a sharp rule: a tag definition can be non-overridable, so that embargo and legal-hold tags cannot be replaced below. One shared resolver must serve every consumer. Statistics must name the immutable version that they describe.
- **Status.** ST-17 and ST-22 cover the idea. New: G-ST-1 to G-ST-4, G-UP-7 to G-UP-9.

### Lesson 11. Maintenance leaves no provenance and has weak safety rules

- **What the code shows.** A bound in-flight slot (`finished_at = 0`, unique on table, policy and `finished_at`) is released only by a best-effort `finish` call from the Spark job. If the pod dies, the table is never maintained again for that policy. The design lists a reconcile step that does not exist. (JM-R22)
- The catalog records counters before and after a run, keyed by table ID and Iceberg snapshot ID. It does not record the files replaced or deleted, the resulting snapshot or the policy version. No lineage call exists in the subsystem. Statistics from the optimizer CLI carry no snapshot ID, which caused stale-statistics bugs (#14674, #14677). (JM-R39 to JM-R42)
- Snapshot expiry has age and retain-last fields only, and leaves the protection of refs to Iceberg. Orphan removal guards in-flight writers only by a 3-day clock window. It checks only the lexical prefix of the scan root. Files of another table under that prefix are at risk. (JM-R44 to JM-R48)
- Jobs run under one cluster-static identity. Callbacks are authorised by a metalake-wide `RUN_JOB`. Secrets are in plaintext in stored templates and in `SparkApplication` arguments. This is the confused-deputy problem that run tokens and RD-3 solve. (JM-R32 to JM-R34)
- Async purge is a hard delete with no undrop or hold. Credentials sit in `file_io_props`. A FAILED purge lifts the name tombstone and leaves leaked files. (JM-R54 to JM-R57)
- **What to copy.** Register the job before submit, with a placeholder execution ID. A three-way submit outcome (rejected, unknown, found by job ID). Deterministic runner names. Leaf-first deletion with "not found" as success. A heartbeat on a thread other than the deleting thread. A name tombstone keyed by catalog ID. (JM-R1, JM-R3, JM-R4, JM-R52 to JM-R54)
- **Status.** PR-7, ID-9 and UP-8 cover the intent. New: G-PR-5, G-UP-10 to G-UP-14, G-OP-6, G-OP-13 and G-OP-14.

### Lesson 12. The REST surface has conventions, and also holes

- **What the code shows.** Names have three grammars: `\w+` for a metalake, `\w[\w-]*` for a catalog and a domain, and `\w[\w/=-]*` by default. The 64-code-point limit is in the code, the database column is 128 and table columns are 300. A rule applies on create and rename only, so legacy names stay usable. (SV-R25 to SV-R28)
- A masked value (`******`) is refused on write, so a GET followed by a PUT fails. (SV-R31)
- The error model has about 15 coarse codes and a Java class name in the `type` field. The codes 409 and 405 share code 1006. The client has no case for optimistic-lock code 1012 in the table, schema or catalog handlers. There is no pagination and no body size limit. `tooManyRequests()` exists and no caller uses it (verified). Version negotiation is by media type, and one operation uses v2. (SV-R13 to SV-R24)
- A failed start calls `System.exit` before the shutdown hook is registered (verified), so auxiliary services that started stay up. Three shutdown timeouts disagree: 3 s, 30 s and a SIGKILL after about 10 s. Health is a sticky out-of-memory flag, and status casing differs between Gravitino (`up`) and the Iceberg REST server (`UP`). (SV-R33 to SV-R35, SV-R47 to SV-R49)
- MySQL and PostgreSQL schemas are created by hand. There is no schema version table and no check at start. Upgrades need all servers stopped. A fresh installation and an upgraded one differ: the default of `storage_location_name` is `'default'` in the first and `'unknown'` in the second. (BS-R55 to BS-R58, SV-R45, SV-R46)
- A metalake disable is copied into each catalog with no transaction. (SV-R10, SV-R11, CF-R61)
- **For mudcat.** Use one name grammar, one length limit and one place that states them. Keep the shape of the error response and give each condition one code. Check the schema version at start. Derive "enabled" at read time.
- **Status.** IN-2 and OP-1 are the nearest rules. New: G-ST-14, G-ST-15, G-IN-7 to G-IN-10, G-OP-1 to G-OP-5, G-OP-9, G-OP-11 and G-OP-12.

### Lesson 13. Engines wrap the catalog, and each discovers it differently

- **What the code shows.** Each engine connector gives metadata and DDL. The native engine catalog does the I/O, with secrets copied from catalog properties at session start. Only credentials that do not expire are used. Spark and Trino therefore send Iceberg through the Iceberg REST server by default, to get vended credentials. (EC-R3, EC-R7, EC-R8)
- Discovery differs. Spark lists catalogs once at driver start under the application identity. Trino polls every 10 seconds and issues `CREATE CATALOG` against itself. Flink asks Gravitino live, with no cache. Change detection uses a clock and a name, because a catalog has no immutable ID. Trino compares `lastModifiedTime`. Spark JDBC compares `createTime` to catch a delete and re-create. (EC-R5, EC-R6, EC-R15 to EC-R18)
- Module count follows SPI churn: 6 Trino modules (the 482 SPI break and a JDK 24 to 25 change), 3 Spark modules, 4 Flink modules. The matrix has gaps: Paimon only on Spark 3.5, Hive absent on Flink 2.x. (EC-R1, EC-R40, EC-R41)
- The client accepts any older major version of the server. Java and Python differ on timeout (180 s against 10 s) and on retry. With no idempotency key, the MCP server tells the user to "check its result before retrying". (EC-R24 to EC-R27)
- The MCP server has 81 tools that return JSON strings. 36 mutate, and 7 have a `readOnlyHint`. Tools are filtered by entity tag, so exposing `table` also exposes `drop_table`. An unknown tag silently exposes no tools, and the docs name the tag `statistics` while the code uses `statistic`. There is no search tool, and audit records "deny" for any exception. (EC-R45 to EC-R48)
- **For mudcat.** Serve every Iceberg table through the Iceberg REST gateway, so that an engine needs no mudcat plugin. Publish a support matrix as data. Give each catalog an immutable ID and a change token. Classify each MCP tool as read, write or destructive, and add one search tool that returns identifiers and the commit it read.
- **Status.** IN-5, IN-6 and IN-14 cover the base. New: G-IN-11 to G-IN-14, G-IN-16 and G-IN-17.

### Lesson 14. Mapping many sources to one model is lossy, and "unsupported" is an exception

- **What the code shows.** The escape for a type with no common equivalent is `ExternalType(native string)`. MySQL `BIT`, `BINARY` and `ENUM` needed a second query to `information_schema` to repair the type. ClickHouse silently ignores `nullOrdering`. (RL-R1 to RL-R9)
- "Unsupported" is signalled only by exceptions, of three different types. The `Capability` SPI has six methods, so a client cannot ask before it sends. (RL-R18, RL-R19)
- One partition transform or one index that the common model cannot represent can make a table unloadable (an unknown Iceberg transform, a Doris `NGRAM_BF` index). ClickHouse omits such indexes instead. The table format is inferred from engine parameters on each listing. (RL-R34, RL-R39)
- Registered tables drift from the source. A Delta schema is asserted by the caller and never checked. Lance refreshes columns on load by comparing `lance.version`, with a compare-and-set and bounded retries, so a read changes the catalog. (RL-R28 to RL-R33)
- Deadlock and lock-timeout errors are deliberately not mapped to 503, because the DDL may have partly committed. Glue sends its `versionId`. The Hive alter has an open `TODO` for a table lock. A probe of the backend under the global lock needs hard timeouts. Hive 3 falls back to Hive 2 by matching error text. (RL-R23, RL-R45, RL-R50, RL-R51)
- **For mudcat.** The format profiles (ST-2, ST-3) need three things. The first is an opaque type that round-trips. The second is a published matrix of what is lossless, lossy or opaque. The third is a machine-readable capability per source and per operation. A read must not change catalog state, except an idempotent, conditional repair.
- **Status.** ST-3 and ST-6 are the nearest rules. New: G-ST-5 to G-ST-10, G-IN-15.

## 5. Odd behaviour by cause

The full list is in [quirks-index.md](quirks-index.md). This digest shows the kinds of cause that the readers found, with a few examples. The tags are the readers' own and are approximate. The counts are MODEL 94, OTHER 86, CONCURRENCY 74, SPEC 65, SECURITY 40, STORE 37, ENGINE 33 and COMPAT 30.

| Cause | Examples |
|---|---|
| **SPEC** (an external protocol) | The Iceberg REST namespace separator (0x1F in the spec, `:` in Gravitino). An optional `field-id` on an added partition field. Existence probes (`HEAD`) that need looser authorization than a read. Engines that send reserved properties, which the server drops with no error. An "unsupported" exception that maps to 406 but is also raised for an invalid table definition. Hive 3 to Hive 2 fallback by error text. Kafka identity by a truncated UUID. Lance branches and tags refused, but a `version` parameter ignored. |
| **STORE** (the backend database) | `ON CONFLICT` has one target in PostgreSQL, while MySQL `ON DUPLICATE KEY` matches any unique key. H2 has no `FOR SHARE`. PostgreSQL has no `DELETE ... LIMIT`. The bind-parameter ceiling of PostgreSQL. Lock timeouts are classified by vendor code, not by SQLSTATE, and are detected only if the operator set one. Auto-increment order is not commit order. A 63-bit random ID collides at about 3 x 10^9 rows. A hierarchical schema name is a path string in one `VARCHAR(128)`. A fresh and an upgraded schema differ in defaults. |
| **ENGINE** (Spark, Trino, Flink) | Three discovery mechanisms. Trino polls every 10 s. Clock-based change detection. Six Trino modules for SPI breaks. Engines that lowercase names. |
| **COMPAT** (legacy and migration) | Legacy single-location filesets with no default-location name. Name rules that apply on create and rename only. A client that accepts any older major version. Config defaults in the shipped file that differ from the code default. Docs that describe a user `externalId` and `enabled` that the code does not have. |
| **CONCURRENCY** | JVM-local locks in a multi-server product. Load-versus-alter races that drop column IDs. Idempotency that is not in the transaction. A shared enforcer state that mixed group claims. A cache fill that races with an invalidation, fixed by an epoch and pending-write blocks. |
| **SECURITY** | Scope from client metadata. Wildcards in cloud policies. Static-key fallback. Plaintext secrets. Missing `exp` and issuer checks. Shared Lance keys. A role that wrongly receives a deny or an allow through shared group state. |
| **MODEL** (impedance mismatch) | A table-shaped model for files, topics and models. Types that do not map. Partition transforms and indexes that the model cannot hold. System state in the user namespace (hidden tags). Metalake state copied into catalogs. |

## 6. How the findings compare with mudcat's current requirements

Gravitino confirms these mudcat requirements, which it implements in part or shows to be needed:

| mudcat | Gravitino evidence |
|---|---|
| ID-7, ID-14 to ID-21 (commit requirements, idempotency, commit IDs in answers) | SV-R1 to SV-R6, BS-R12 to BS-R16, IR-R9 to IR-R21, CL-R2, CL-R3 |
| ID-8, ID-9 (keep published snapshots, expire only if unreferenced) | JM-R48, BS-R8 |
| EV-1, EV-3, EV-13 (commit-ordered events) | BS-R45, CL-R10 to CL-R14, EA-R24 to EA-R27 |
| RD-1, RD-3 (no proxy, prefix inside an ACL root) | CR-R1, CR-R3, FS-R27, IR-R26 |
| AC-1, AC-6, AC-7 (port, fail closed, revocation at once) | AZ-R4, AZ-R29, CL-R20 |
| AC-8 (audit of decisions) | AZ-R50, EA-R17 |
| PR-4, PR-6 (usage from run-token reads; OpenLineage import) | EA-R44 to EA-R51, CR-R19 |
| PR-7 (compaction as an activity) | JM-R39 |
| UP-7, UP-8, UP-17, PL-7 (takedown, legal hold, purge) | JM-R44 to JM-R57, FS-R1 to FS-R12, RL-R30 |
| ST-17, ST-22 (tag inheritance, expectations on statistics) | TP-R4, TP-R5, TP-R42 |
| IN-5, IN-14 to IN-17 (Iceberg gateway) | IR-R2, IR-R53, IR-R55 |
| IN-6 (MCP server with caller permissions) | EC-R45 to EC-R48 |
| OP-1 (PostgreSQL only) | BS-R53, BS-R54 |
| OP-15, OP-16 (no removal while referenced; atomic registration) | CF-R14, IR-R49, CL-R40 |

The evidence points to these places where the current mudcat requirements are too weak:

1. **ID-16 (idempotency).** It needs a digest of the body, a scope by principal, no stored credentials, the same transaction as the commit and the database clock.
2. **AC-7 (revocation).** It states "each check that starts after the removal". It does not bound the life of credentials that were already issued. Add a stated delay (CR-R8).
3. **AC-8 (audit).** "Each access decision" does not say if an allow records its grant path (AZ-R50).
4. **ST-17 (tag inheritance).** It does not say if a child can replace an inherited tag. For restriction tags it must not (TP-R4).
5. **EV-13 (positions in commit order).** It does not state how a consumer learns that no lower position can still commit (EA-R25).
6. **RD-3 (credential scope).** It does not let the caller ask for less, and it does not name a policy-size check (CR-R4, CR-R6).
7. **UP-8 and OP-15.** They do not cover a purge that fails half-way (JM-R55, FS-R7).

## 7. What Gravitino does well

The readers marked these as worth copying:

- A fixed lock order: tenant, parent collections from the root, then entities by ascending ID (BS-R16).
- A cache fill that checks an invalidation epoch and pending writes (CL-R34).
- A cache by allowlist. Users, roles, models and functions are never cached (CL-R16).
- A separate grant for long-lived credentials (CR-R16) and a per-caller session name (CR-R18).
- A check that a named location is registered, with a default (CR-R21), and a validated location selector.
- Path containment by ancestry, with `..` and NUL refused before any storage call (FS-R17, FS-R18).
- An opaque type that holds the native string and writes it back unchanged (RL-R1, RL-R2).
- Fail-closed on conflicting policies (TP-R30), and validation of policy content only when the content changes (TP-R32).
- A trusted-proxy rule for the client address (EA-R10).
- Register the job before submit, and never replay a submit with an unknown outcome (JM-R1, JM-R3, JM-R4).
- Leaf-first deletion with "not found" as success, and a heartbeat on its own thread (JM-R52, JM-R53).
- Tests against real MySQL and PostgreSQL, and a test that loads fresh and upgraded schemas and runs the upgrade again (BS-R54, BS-R56).

## 8. Open questions for mudcat

These are decisions that the findings raise and that the owner of the project must make. They are not decided here.

1. **Reads that change state.** Gravitino imports and repairs on a read (lazy import, column refresh by `lance.version`). Should mudcat forbid every catalog change on a read, including repair by an idempotent conditional update? (G-OP-4)
2. **Revocation delay.** What is the longest delay that mudcat accepts between a takedown and the end of every issued credential? This sets the credential lifetime for affected data. (G-RD-6)
3. **Ownership of bytes.** Is "mudcat owns these bytes" a flag on a replica, a property of the storage resource, or a property of the registration? Gravitino chose a mutable flag on the record, and it caused problems. (G-UP-1)
4. **Unregister.** Does mudcat allow a person to unregister one external object and keep its owners, tags and grants for a later registration? (G-PL-3)
5. **Non-overridable tags.** Is "non-overridable" a property of the tag definition, or of a policy? (G-ST-1)
6. **Policy history.** How many versions of a policy does mudcat keep? UP-13 covers grants and ACLs. It does not name policies. (G-UP-8)
7. **Authorization snapshot.** Does the built-in adapter evaluate grants in SQL against one snapshot, or keep a mirror in memory? The Gravitino mirror cost about 2,400 lines and 107 race tests. (G-AC-6)
8. **Consumer cursors.** Does each change-stream consumer have a durable cursor in the database, and what is the retention rule for a lost consumer? (G-EV-3, G-EV-4)
9. **Statistics and versions.** May a statistic exist with no reference to the immutable version that it describes? (G-ST-3)
10. **Support matrix.** Is the engine-by-format support matrix data that the gateway serves, or a document? (G-IN-11)

## 9. Where mudcat differs, so do not copy blindly

- **Mutable against immutable.** Gravitino alters objects in place and keeps a bounded history. mudcat keeps immutable versions and moves pointers. Rules for version retention, `deleted_at` and revision counters do not transfer. The rules for concurrency and for the order of events do.
- **One metalake against many tenants.** A Gravitino metalake is close to a tenant, but IDs are not tenant-qualified and keys do not start with a tenant. The multi-tenant rules of mudcat (OP-8, OP-12 to OP-14) get little evidence here.
- **Three dialects against one.** Most STORE quirks vanish if mudcat supports PostgreSQL only (BS-R53). Do not copy the dialect layer.
- **Credentials.** Gravitino connectors copy long-lived secrets into engines, and the Lance server gives out static keys. mudcat resolves names and issues scoped credentials, and never proxies reads (RD-1).
- **Scale.** Gravitino has no figure for 10^9 versions or 10^11 elements. Its evidence for PF-1 to PF-8 is weak, except the cost of name resolution (BS-R42) and of the location-overlap scan (IR-R27).
- **Provenance.** Gravitino does not store lineage. Its evidence for PR-1 to PR-12 is mostly negative.

## 10. Files

| File | Contents |
|---|---|
| [requirements.md](requirements.md) | Candidate requirements for mudcat in the style of [../../requirements.md](../../requirements.md), grouped by area, with the status against the existing requirements |
| [quirks-index.md](quirks-index.md) | The titles of all ledger entries, by subsystem |
| [detail/01-backend-store.md](detail/01-backend-store.md) | Backend store |
| [detail/02-cache-lock-consistency.md](detail/02-cache-lock-consistency.md) | Cache, locks, consistency |
| [detail/03-authorization.md](detail/03-authorization.md) | Authorization |
| [detail/04-authn-credentials-secrets.md](detail/04-authn-credentials-secrets.md) | Authentication, credentials, secrets |
| [detail/05-catalog-framework-federation.md](detail/05-catalog-framework-federation.md) | Catalog framework and federation |
| [detail/06-relational-lakehouse-catalogs.md](detail/06-relational-lakehouse-catalogs.md) | Relational and lakehouse providers |
| [detail/07-fileset-kafka-model-lance.md](detail/07-fileset-kafka-model-lance.md) | Fileset, Kafka, model, Lance |
| [detail/08-iceberg-rest.md](detail/08-iceberg-rest.md) | Iceberg REST server |
| [detail/09-events-audit-lineage-metrics.md](detail/09-events-audit-lineage-metrics.md) | Events, audit, lineage, metrics |
| [detail/10-tags-policy-stats-semantic.md](detail/10-tags-policy-stats-semantic.md) | Tags, policy, statistics, semantic model |
| [detail/11-jobs-maintenance.md](detail/11-jobs-maintenance.md) | Jobs and maintenance |
| [detail/12-server-api-config.md](detail/12-server-api-config.md) | Server, API, configuration |
| [detail/13-engine-connectors-clients.md](detail/13-engine-connectors-clients.md) | Engine connectors, clients, MCP |
