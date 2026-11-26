# Index of quirks and gotchas

Each entry is the title of one entry in the "Quirks and gotchas ledger" (section 3) of a detail file. The detail file gives the evidence (`path:line`), the cause category and the implication for mudcat. Titles were extracted by script, so a few sub-headings can appear as entries.

## [Backend store (relational entity store, schema scripts, GC, change log)](detail/01-backend-store.md) (35 entries)

1. Tombstone timestamps collide on the unique key
2. Primary key of four root tables includes `deleted_at`, the others do not
3. PostgreSQL `ON CONFLICT` has one target, MySQL `ON DUPLICATE KEY` any unique key
4. PG `ON CONFLICT DO UPDATE` needs qualified columns
5. H2 has no `FOR SHARE`
6. `DELETE ... LIMIT` does not exist in PostgreSQL
7. Bind-parameter ceiling in PostgreSQL
8. Isolation is READ COMMITTED everywhere and MyBatis caches reads inside a session
9. Deadlock policy differs between write paths
10. Lock-wait timeouts are classified by vendor code, not SQLState
11. A lock timeout can only be detected if the operator set one
12. Auto-increment ID order is not commit order
13. The change-log retention must exceed several poll cycles
14. Cache coherence is per node and name-keyed
15. Overwrite does not necessarily keep the entity that the caller built
16. Owner: the schema allowed two live owners
17. Relations are polymorphic with no foreign key
18. Table columns are versioned as deltas, other types as snapshots
19. Version retention is hard-bounded and prunes history
20. `current_version` doubled as OCC token and history pointer
21. Name limit differs by layer
22. Hierarchical schema names use an ASCII-1 separator in one column
23. Model version "number or alias" is decided with `NumberUtils.isCreatable`
24. Large text types are not equivalent
25. MySQL index prefix on `partition_name`
26. Partial indexes on PG, composite indexes on MySQL
27. Upgrade scripts: fresh schema and upgraded schema drift
28. MySQL cannot roll back DDL, so upgrades are guarded by `information_schema` checks
29. Schema is not versioned in the database; the server never checks
30. DB error text can leak
31. Batch operations are mostly unimplemented
32. Post-delete action cannot be atomic with the default backend method
33. 63-bit random IDs are also stored as MySQL `BIGINT UNSIGNED` and PG signed `BIGINT`
34. Same ID can sit in two schemas' history
35. Idempotency record insert and retry run in separate transactions

## [Caching, locking, consistency, multi-node (ref prefix CL)](detail/02-cache-lock-consistency.md) (29 entries)

1. Tree lock is JVM-local, so it fakes cross-node safety
2. Alter takes only a READ lock on the table
3. A rename needs a lock on the parent, because only one WRITE can be taken per call
4. No lock upgrade; code releases the lock before cleanup
5. Lock waits have no timeout; deadlock detection only logs
6. Tree lock bookkeeping is itself bounded memory
7. Lock order is documented only for tree lock, lifecycle lock, cache key
8. Database locks use the same parent-first order, and H2 differs
9. Isolation is READ COMMITTED and reads do not lock
10. Lock-wait timeouts are vendor specific, and deadlocks are excluded
11. `exists` and `get` can say a peer-dropped entity is still there; `list` cannot
12. Cache fills can resurrect stale data unless fenced
13. Only some entity types may be cached, and the metalake on/off flag is the accepted exception
14. For external catalogs the cache holds only IDs and audit fields
15. TTL: expire after write; `maxEntries` silently ignored; weight 0 never evicts
16. Change-log IDs are not commit order
17. A failed listener is not retried; the shared cursor moves on
18. The change log carries the old name on ALTER; the contract is unwritten in code
19. Name paths with dots
20. Poller clocks: DB clock for the change log, JVM clock for the owner stream
21. Authorization uses three consistency levels in one component
22. Authorization has bounded retry loops and fail-closed paths
23. Credential cache serves a fraction of the life, not the whole life
24. Iceberg REST: the entity store is a shadow of the Iceberg catalog
25. Idempotency state is outside the mutation's transaction
26. Optimistic conflicts exist in the public API contract, but only for some calls
27. Batch child deletes are limited by bind-parameter counts
28. Per-version table rows: overwrite must advance, not reset, the version
29. A load can write the store; loads and alters race on column IDs

## [Authorization (AZ)](detail/03-authorization.md) (30 entries)

1. Grants are committed before the external enforcement point is told.
2. Plugin return value is discarded.
3. Ranger owner-role failures are swallowed.
4. Unmapped privileges silently do nothing in Ranger.
5. `USE_CATALOG` and `USE_SCHEMA` become `SELECT` in Ranger.
6. One Ranger policy per resource; read-modify-write without a lock.
7. Ranger rename is rewriting policy names and resources by hand.
8. Shared Ranger owner roles leaked rights across catalogs.
9. Chain of plugins stops at the first `false`, no rollback.
10. Engines see Ranger changes after a poll, not at commit.
11. Group membership in the shared enforcer was a real vulnerability.
12. In-memory policy mirror needs a consistency protocol.
13. Unresolvable DENY object becomes a catalog-wide deny.
14. Name-to-ID caches need an entity change log, and the log must store the pre-mutation name.
15. Authorization depends on connector name rules.
16. Dots in names break authorization.
17. Owner is set after the object exists.
18. Hierarchical schema creation needs a catalog write lock to assign ownership.
19. `setOwners` swallows plugin errors per object; `setOwner` does not.
20. Owner and deny precedence differ by path.
21. ALLOW+DENY in one role: doc and code disagree.
22. Inheritance is hardcoded in regex macros.
23. Each endpoint carries its own copy of the traversal rule.
24. List short-circuit is a second copy of the policy.
25. List filter drops entries on any engine error.
26. Audit exists only for denials.
27. Authorization failure returns 500, list returns empty.
28. Pass-through authorizer auto-creates users.
29. Role recreated under the same name fails closed.
30. Privilege bit mask is dead.

## [Authentication, credential vending, secrets, encryption (Gravitino) — findings for mudcat](detail/04-authn-credentials-secrets.md) (34 entries)

1. The credential scope comes from client-controlled table metadata
2. Provider is chosen from the table location only; other paths are silently dropped
3. Read vs write is decided by the server from authorization, not requested
4. Setting a role ARN without `credential-providers` vends a long-lived key
5. Static JDBC credentials of the backing database are vendable
6. S3 `ListBucket` needs a bare-prefix escape hatch for Hadoop
7. S3 partition is guessed in two different ways
8. Per-cloud policy shapes and quirks that the generators encode
9. Refresh endpoints exist for S3, GCS, ADLS only
10. Azure SAS is one directory-scoped token, signed with a user-delegation key per call
11. AWS limits the generator does not guard
12. Wrapped errors lose their type
13. Credentials and clocks
14. Python GVFS holds a write lock during the REST call
15. Hadoop S3A has two SDK generations
16. Client picks among several credentials with a hard-coded order
17. Unknown credential type breaks the whole response on the Java client
18. Masked placeholder can become a value
19. Raw secret reference as a property value
20. Secret key detection by name hits unrelated keys and misses others
21. Nested JSON masking and duplicate keys
22. Legacy leak switch
23. Static vs dynamic access check has a catalog-level special case
24. Two authenticators can claim the same header
25. Malformed Kerberos header returns 500
26. JWT validation defaults
27. Same-subject collision across realms
28. Basic IdP password hashing
29. Principal keeps the raw `Authorization` header
30. Federated catalogs: three credential sources
31. Tencent session name differs from the shared helper
32. Context sets are mutable and merged in place
33. Catalog credential manager is built once
34. Spark/Flink take only non-expiring credentials at init

## [Catalog framework and federation (ref prefix CF)](detail/05-catalog-framework-federation.md) (41 entries)

1. Identity is born at the first read (lazy import)
2. Identity is stored in the external system, in whatever field the system offers
3. JDBC comment updates must re-attach the ID
4. Kafka identity is a truncated UUID
5. Create is two non-atomic steps; recovery is by loading
6. Table create does not pre-check the store; schema create does
7. Import conflicts cannot be fixed per object
8. Tables do not follow an external rename; schemas do
9. Primary keys had to be changed to allow re-registration
10. Store failures on external paths are logged and hidden
11. Load/alter race loses column IDs, and tree locks are per server
12. Column rename detection is heuristic
13. Connectors that return no column list
14. "Drop returned false" is ambiguous
15. Core hard-codes provider names to decide if a catalog is empty
16. Tree lock granularity trades visibility for throughput
17. Capabilities can depend on a live connection
18. Case rules differ per scope and per backend, and sometimes per table format
19. Name limits disagree across five places
20. The authorisation system leaks into naming
21. Nested namespaces are squeezed into a flat schema name
22. Authorised name and operated name must be the same
23. The class-sharing rule is a hard-coded prefix list
24. Isolation dimensions are listed explicitly and each has a story
25. Pool operations serialise per key
26. The connector's loader must not be the context loader during store access
27. Unloading a classloader is a long list of leak fixes
28. A catalog property can point the server at code
29. Stack inspection decides whether the in-use check runs
30. In-use state is copied downward and updated non-atomically
31. Property checks are lenient for undeclared keys, strict for declared ones
32. Effective config is frozen into the catalog at create
33. Create-time verification is opt-in per connector and uneven
34. Test connection uses a sentinel identity
35. Owner is set after create; the failure policy differs by type
36. Secrets are staged, then committed or rolled back around entity writes
37. Column "initial default" is returned by connectors but cannot be set or stored
38. Dropping an external table leaves data; purge is gated; unprovision is best-effort
39. Stored Lance columns can be stale; refresh is a policy
40. Glue and HMS: all table types visible, views hidden, view formats per engine
41. Events fire at the outermost layer, so internal imports are silent

## [Relational and lakehouse catalog providers (ref prefix RL)](detail/06-relational-lakehouse-catalogs.md) (62 entries)

1. Identity written into the external object's comment
2. Alter comment must preserve the marker
3. Cluster topology stored in the comment
4. Declared schema vs actual schema (drift)
5. Delta registration trusts the caller
6. Lance staged create
7. Purge is two non-atomic steps
8. Iceberg purge stopped at the first unreadable file
9. Rename column is not a universal operation
10. "Modify column" restates the whole column
11. PostgreSQL has no column position and no table-option alteration
12. Type change keeps the collation
13. Standalone unique index is not a constraint
14. Doris and ClickHouse behave differently across versions
15. Dialect-specific default rendering
16. Hologres is nearly append-only for schema
17. OceanBase runs changes one by one
18. ClickHouse alters are engine-specific
19. MySQL `BIT` and boolean are not symmetric
20. Unsigned integers are first-class in MySQL and ClickHouse, absent in Glue/Hive/Iceberg
21. PostgreSQL numeric corner cases
22. PostgreSQL arrays
23. `bpchar` and `varchar` without length
24. ClickHouse type surface
25. Hive type gaps
26. Glue stores some Iceberg types as strings
27. Glue Hive-format columns are lower-cased and may collide
28. Iceberg field ids are assigned by the connector
29. Iceberg defaults and required columns
30. Iceberg distribution mode is not the common distribution
31. Iceberg partition transforms
32. Hive partitions are identity-only and positional
33. Paimon limits
34. Same type, different case rule per backend
35. Normalisation is not reversible
36. Existing names can be invalid for creation
37. Hierarchical namespaces
38. Schema operations are thin
39. PostgreSQL `catalog` is a database, `schema` is a Gravitino schema; one connection, one database
40. Backend probe under the global write lock
41. Connection validation via `isValid()`, not SQL
42. Pool exhaustion deadlock
43. Retry hazards of DDL
44. JDBC URL as an attack surface
45. Driver jars are not bundled
46. Connection timeouts differ by driver
47. Hive client version auto-detect
48. Hive 3 constraints are separate objects
49. Hive listing lies about table type and format
50. Hive views: one dialect each, plus engine-private encodings
51. Hive SerDe-derived tables are read-only for columns
52. Hive alter lacks a lock
53. Glue update replaces the whole table
54. Glue hosts several formats; Gravitino mixes SDKs
55. Glue error messages
56. Iceberg JDBC backend control table race
57. Iceberg JDBC defaults the connector sets
58. Iceberg JDBC namespace property length
59. REST backend `warehouse` means something else
60. GCS tokens are reminted before use
61. Iceberg read-only properties and reserved names
62. Hudi has no write path

## [Non-tabular catalogs: fileset, messaging (Kafka), model registry, Lance (ref prefix FS)](detail/07-fileset-kafka-model-lance.md) (30 entries)

1. Drop means different things depending on a flag and a type.
2. A metadata edit can arm data deletion.
3. Two filesets may overlap and one drop can delete the other's data.
4. Storage delete runs inside the metadata transaction.
5. REST-created Lance tables cannot be dropped by default.
6. Lance REST hands out the catalog's own storage keys.
7. Java GVFS shares one FileSystem across filesets in the same bucket, including its credentials.
8. Default location rule is implemented four times, differently.
9. "unknown" is a reserved location name; the schema default is "default".
10. Sub-path hygiene lives in one function, and the tests show how much it took.
11. Client strips the location prefix by string length.
12. Object stores disagree about trailing slashes on directories.
13. Config for a location is picked by first match in a HashMap.
14. One REST call per filesystem call.
15. Client-reported data operation is audit input and the only server-side input for one rule.
16. Kafka identity is a truncated UUID and "creation time" is now.
17. Dropping a topic always deletes the topic.
18. Kafka names Kafka accepts, Gravitino rejects.
19. Kafka config providers could read the server's environment.
20. Model alias syntax is validated in one of two places.
21. A moving alias is two writes.
22. "Immutable release" in docs, mutable URIs in code.
23. Concurrency in model versions is solved with a parent-row counter.
24. Lance branch/tag refused, version silently ignored.
25. Lance schema is a cache that may be stale and may be written on read.
26. Lance delete is metadata and data in two steps, order depends on type.
27. Namespaces in Lance REST are really the Gravitino hierarchy.
28. Authorization differs between Lance deployment modes.
29. Python GVFS ABS path test is an enum f-string.
30. Credential for the server probe is registered in a static in-memory map.

## [Iceberg REST Catalog Server (Gravitino `iceberg/**`)  (ref prefix IR)](detail/08-iceberg-rest.md) (46 entries)

1. Existence probes are authorised more loosely than reads
2. Engines send reserved properties, so the server must drop them silently
3. Optional `field-id` on added partition fields
4. 404 means "no namespace/table", not "no metadata file"
5. Unsupported-operation exception means 406 in the mapper, but invalid table definitions raise it too
6. Schema evolution has no "empty table" exemption
7. v3 date promotion not in Iceberg's own `isPromotionAllowed`
8. Server validation is injected as a requirement so retries re-run it
9. `RetryableValidationException` becomes 409
10. Cache invalidated before and after a commit; correctness comes from the pointer check
11. Create-table transaction does not retry
12. Idempotent replays drop response headers
13. A lost acknowledgement or a failed response write blocks the key until expiry
14. Fencing token per key from 256 striped counters
15. Expired-row reuse needs a second transaction
16. Purge SQL differs per dialect
17. In-memory store silently loses keys under pressure; JDBC store unavailable standalone
18. Credentials are scoped by prefix, so table locations are a security boundary
19. Overlap test works only for the standard `<table>/metadata/<file>` layout
20. Location normalisation is dialect-specific by scheme
21. Credential refresh endpoints are per-credential-type and sometimes omitted
22. Local and HDFS locations never get credentials; the request still succeeds
23. Credential privilege is computed per request from live authorization
24. Namespace aliasing through the flattening separator
25. Dot and length limits on namespaces
26. JDBC namespace property values cap at 1000 code points
27. REST-backend identifiers are case-sensitive and re-registration conflicts
28. Lazy import means entity IDs appear on first load
29. Nested namespace creation creates and owns missing ancestors
30. Async purge is selected by a non-standard header and falls back silently
31. Failed purge frees the name and leaks files
32. Tombstone check misses two paths
33. Drop namespace does not check emptiness itself
34. Pagination starts only with an empty or present `pageToken`
35. ETag must cover caller-dependent bytes
36. `snapshots=refs` filtering must keep credentials and history
37. Metrics queue drops reports on overload with 503
38. JDBC catalog V1 migration races between catalogs sharing one `iceberg_tables`
39. Views unsupported on JDBC schema V0
40. Hive catalog needs impersonation context for every call
41. Planting classes in Iceberg's packages
42. Two Iceberg runtimes
43. Aux-mode classloader isolation
44. Authorization is path-driven and evaluated before the body is parsed for most endpoints; body-aware handlers fill the gaps
45. Federated REST backend skips local authorization by default
46. Read restrictions are experimental, off by default, and need authorization on

## [Events, auditing, lineage/provenance, metrics (ref prefix EA)](detail/09-events-audit-lineage-metrics.md) (27 entries)

1. Pre-event veto is invisible to audit
2. A veto only works in SYNC mode; an async listener throwing Forbidden is logged and ignored
3. Exception contract in `EventBus` doc disagrees with real behaviour
4. Success event is dispatched inside the `try` that emits a failure event
5. Listener order is a hash order, not configuration order
6. Audit is lossy by construction
7. Entity change log: serial ids commit out of order
8. The poller advances the cursor even when a listener failed
9. Retention can silently outrun a slow node
10. Change-log append is in-transaction, events are not
11. Events reach listeners with unredacted query parameters and properties
12. Generic fallback events exist because coverage is incomplete
13. Dispatchers silently skip some methods
14. Thread-local request context is the glue
15. Success + 5xx and failure + 2xx outcomes are both possible for one request
16. `XForwardedFor` handling is an auditing security issue
17. Lineage 201 means "queued", not "kept"
18. Lineage reuses the event bus through a fake event
19. Output datasets need only read-level visibility
20. Lineage dataset identity is a dot-joined string with a type facet
21. Lineage tenant check only when authorization is on
22. Engines define the identifier mapping, outside this repo
23. One slow lineage sink rejects all lineage
24. Metrics: per-method annotation coverage and timers that read zero
25. Metrics sources are registered by name and replaced racily
26. Metrics endpoints bypass the auth and audit chain
27. Audit operation names collapse in the legacy enum

## [Metadata enrichment and governance objects: tags, policies, statistics, semantic model, domains/glossary (ref prefix TP)](detail/10-tags-policy-stats-semantic.md) (30 entries)

1. Nearest tag wins by name, so a child can weaken a parent
2. ALL_VALUES cannot express exclusion
3. Two inheritance implementations disagree on cost and on hidden tags
4. Authorisation filtering happens after the shadowing merge
5. No value is stored as the empty string, and docs still say NULL
6. Deleted-at is part of every unique key, so soft delete forces unique names to be re-checked in code
7. Soft-deleted relation rows are physically removed only after a delay, by two collectors
8. Cascade SQL is a UNION over every entity type, per container
9. Historical entity rows may be re-registered under a different parent
10. Delete of a tag cleans its relations in the same transaction, but only the tag delete is checked
11. Read-committed insert race on missing rows
12. Store reports missing tag and missing object with one exception type
13. Policy "enabled" and content share one version row; retention default is 1
14. Validation tightened over time must not block disable or rename
15. Legacy direct object-policy bindings became hidden tags
16. Policy supportedObjectTypes is deprecated but still validated
17. Policy to tag relation cannot change its selector in place
18. Statistic names equal under MySQL collation but not in Java
19. Batch statistic writes deadlock by plan order
20. Statistics are table-only although the table is polymorphic
21. Partition statistics bypass the entity store and the cascade
22. Lance backend cannot upsert
23. Revision GC versus publication status drift
24. Semantic-model deletion leaves tag and owner rows to the sweep
25. Definition text and association table can disagree
26. Derived indexes are keyed by the active revision only
27. Metric rename is a delete plus an add with a new ID
28. Tag locking dance protects against parent drop
29. Row-filter and column-mask policies bind by field ID at load time
30. Policy-on-tag selectors do not validate against allowed values

## [Jobs, table maintenance, optimizer, plugins (ref prefix JM)](detail/11-jobs-maintenance.md) (27 entries)

1. Job row exists before the execution does
2. Executor SPI is a contract about failure, not only about success
3. Multi-node status polling: only the owning node can ask
4. Local executor restart orphans jobs for about a week
5. Finished-job retention equals staging-dir retention
6. Slot released only by the job itself
7. Soft-slot uniqueness trick reused from tombstones
8. Claim lease uses the database clock
9. A successful `runJob` cannot be undone by a failed bind
10. Maintenance commit may re-trigger maintenance
11. Design doc and code disagree on names and shape
12. Expiry retention: policy default is invisible
13. Relative cutoffs are resolved late
14. Orphan removal lists twice
15. Orphan scan is bounded only by lexical prefix
16. Retention minimum is enforced by the engine, not the catalog
17. Unresolved placeholder reaches the job as literal text
18. Masking is a display feature
19. Anyone who may run a job may influence its code
20. Callback authority is metalake-wide
21. Status of a vanished execution
22. Statistics without version binding cause wrong decisions
23. Typos in score expressions disable work silently
24. Ambiguous nearest policy throws at run time
25. Snapshot-drift choices have a history
26. Async purge is a second job system
27. Purge row stores credentials and a table of the same name can be created meanwhile

## [Server, REST API surface, configuration, lifecycle, identity/naming rules (ref prefix SV)](detail/12-server-api-config.md) (35 entries)

1. Three name grammars for three entity types, one length limit enforced below the column width
2. Name rules apply on create and rename only, so legacy names live forever
3. `.` is both a legal name character in places and the qualified-name separator
4. Hidden tags named by prefix implement policies
5. Metalake enable/disable is denormalised into every catalog with no transaction
6. Cross-server safety of drop relies on the store, not on the lock
7. Optimistic concurrency exists in three places only, and each is bespoke
8. Client does not map the optimistic-lock code in most resource handlers
9. Error identity is (code, class-name string)
10. 409 and 405 carry the "unsupported operation" code
11. API version is per-operation and there is one v2 operation
12. Version regex is unanchored and the parse is unbounded (inferred bug)
13. Server branches on `X-Client-Version`
14. `DropResponse` has a vestigial `deleted` field
15. Read-modify-write breaks on masked properties
16. Alter validation skips keys the metadata does not declare
17. Shipped template and code defaults disagree
18. CORS default omits PATCH, which the API uses
19. Shutdown has three disagreeing timeouts
20. Failed start leaks started services
21. Startup order is load-bearing
22. `initializeBaseComponents`, `initializeMetadataComponents`, `initializeFullComponents` are three profiles of one singleton
23. Health: sticky OOM state and cross-component sharing
24. Health status casing differs per component
25. Config and filters: which paths are authenticated is a per-path-spec decision
26. Access-control APIs exist but answer 405 when authorization is off
27. Default authentication admits any user name; default admin is `anonymous`
28. Database schema is not created or checked, except on H2
29. Soft deletion is the uniqueness mechanism
30. The change log's ID order is not commit order
31. IDs are 63 random bits
32. Iceberg/Lance REST as auxiliary service: one metalake, isolated classloader, advertised URI guessing
33. Bulk API is best-effort, not atomic
34. A 429 helper that nothing calls
35. Every response metered by name; every handler repeats the same try/catch

## [Multi-engine support: Spark / Trino / Flink connectors, Java and Python clients, MCP server (ref prefix EC)](detail/13-engine-connectors-clients.md) (34 entries)

1. Spark sees only the catalogs that existed at driver start, and as the wrong user
2. Trino registers catalogs by calling `CREATE CATALOG` on itself
3. The Trino refresh loop can die silently
4. Catalog change detection by clock, and by name
5. A reload failure can unregister a working catalog
6. Unqualified catalog names from two metalakes collide
7. Trino lower-cases catalog names; PostgreSQL needs unquoted identifiers
8. Same Gravitino table, three engines, three type fates
9. Spark JDBC pushdown can change query results
10. Spark JDBC writes to PostgreSQL UUID/external columns are rejected up front
11. Time travel combines the current Gravitino schema with a historic snapshot
12. Branch-loaded tables write to the wrong place unless blocked (Spark 4.1)
13. Trino cannot `CREATE OR REPLACE TABLE AS SELECT`
14. A failed CTAS drops the table, even if another query now owns it
15. Views: three engines, three behaviours
16. `tableExists` lies when the caller lacks read privilege
17. Spark SQL-on-file paths collide with table lookup
18. Spark `ALTER` batches with zero changes, and default-value ALTERs
19. Flink silently dropped computed columns and watermarks (now rejected)
20. Flink tables are stored as serialised properties
21. Flink needs one catalog manager per JVM; a second with different config fails
22. ServiceLoader failures are swallowed for optional providers (Flink)
23. REST auth was never propagated when loading a persisted Flink catalog (#11601)
24. Iceberg routing can strand static storage credentials
25. A user token's identity for cache partitioning is read without verification
26. A session that sets the same token as the application is treated as inheriting it
27. Client version check rule accepts every older major
28. The server shapes responses by the client version (fileset subPath)
29. Python and Java clients behave differently on the same server
30. Concurrent-change conflict has a code but no precondition
31. MCP tools: unknown tag hides everything; docs and code tag names differ
32. MCP: 'deny' means any exception
33. MCP write timeouts leave the operation in doubt
34. Server error bodies carry stack traces
