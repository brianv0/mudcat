> **Research note, not a requirement document.** Drafted by a code-reading agent from Apache Gravitino commit `9aa78cf` (2026-11-26), then spot-checked by the lead. Line numbers can drift by a few lines; search by file and symbol. Items marked `(inferred)` were not confirmed in code. The text does not follow Simplified Technical English. See [../README.md](../README.md) for the synthesis.

# Caching, locking, consistency, multi-node (ref prefix CL)

Source: Gravitino checkout at `/home/user/apache/gravitino` (commit 9aa78cf). Paths below are repo-relative. `(inferred)` marks a conclusion I drew and did not read as a statement in the code.

## 1. Decomposition

Gravitino has **four unrelated mechanisms** that people call "locking and caching". They have different guarantees. mudcat must not merge them.

| # | Mechanism | Scope | What it protects |
|---|---|---|---|
| A | **Tree lock** (`lock/`) | One JVM only | Orders local API calls on one name path |
| B | **Row locks + version CAS** in the metadata store | The database, so all nodes | Real cross-node correctness of writes |
| C | **Entity cache** (`cache/`) plus **change-log poller** | Per node, eventually coherent | Read speed for metalake, catalog, schema, table, topic, view, fileset, tag, policy, job |
| D | **Other caches**: authorization (JCasbin), catalog wrapper, credential, Iceberg metadata and scan plan, idempotency | Per node (mostly) | Read speed, each with its own staleness rule |

### 1.1 Tree lock (A)

- `core/src/main/java/org/apache/gravitino/lock/LockManager.java`: factory and janitor. One tree of `TreeLockNode` per JVM (`GravitinoEnv.java:1083`). Config `gravitino.lock.maxNodes` (100000), `minNodes` (1000), `cleanIntervalInSecs` (60) (`Configs.java:380-399`).
- `TreeLock.java`: one lock per name path. It takes **READ on every ancestor from the root and the requested mode on the leaf** (`TreeLock.java:103`). Unlock runs in reverse order.
- `TreeLockNode.java`: a `ReentrantReadWriteLock` per name segment, a child map, a reference count (`TreeLockNode.java:104`).
- `TreeLockUtils.doWithTreeLock(ident, READ|WRITE, executable)` is the only entry point used by callers (`TreeLockUtils.java:44`). `doWithRootTreeLock` locks the pseudo-path `/`.
- Callers (non-test): `catalog/{Table,Schema,Topic,Fileset,View,Function}OperationDispatcher`, `CatalogManager`, `MetalakeManager`, `AccessControlManager`, `PermissionManager`, `JobManager`, `StatisticManager`, `BuiltInJobTemplateEventListener`, `hook/SchemaHookDispatcher`, and the Iceberg REST hook dispatchers (`iceberg/iceberg-rest-server/.../IcebergTableHookDispatcher.java:79`, `IcebergNamespaceHookDispatcher.java:85,183`).
- Lock hierarchy by operation (from the dispatchers):

| Operation | Locked path | Mode |
|---|---|---|
| Load, list | the entity (list: the container) | READ |
| Create table | the table node (so sibling creates run in parallel) | WRITE (`TableOperationDispatcher.java:245-250`) |
| Drop table | the table | WRITE (`:456-460`) |
| Purge table | the **schema** | WRITE (`:515-520`), not the table. The two disagree. |
| Alter table, no rename | the table | READ (!) (`:309`) |
| Rename table within schema | the schema | WRITE |
| Rename table across schemas | the catalog | WRITE (`:288,309`) |
| Rename metalake | the root `/` | WRITE (`MetalakeManager.java:364`) |
| Create/alter catalog | the catalog, or the metalake if renaming | WRITE (`CatalogManager.java:1268`) |
| Add/remove user | virtual path `<metalake>.system.user` | WRITE (`AccessControlManager.java:70`) |
| Grant role to user | each role path, separately | READ (`PermissionManager.java:76`) |

- Documented lock order: **tree lock, then catalog lifecycle lock, then per-key cache lock** (`CatalogManager.java:885`).
- Documented rule: do not nest tree locks (`MetalakeManager.java:375`); release the table lock before the schema cleanup to avoid a lock upgrade (`TableOperationDispatcher.java:494`).

### 1.2 Store-level locks and version checks (B)

- Isolation is `READ_COMMITTED` (`storage/relational/session/SqlSessions.java:67`).
- Each mutable row has `current_version` and `last_version`. Updates are `UPDATE ... WHERE id = ? AND current_version = ? AND deleted_at = 0`; zero rows updated means conflict (`mapper/provider/base/TableMetaBaseSQLProvider.java:285`, `service/OccWriteSupport.java` `updateWithVersion`, `deleteWithVersion`).
- `OccWriteSupport.writeFailure` re-reads the row and classifies a zero-row update as **not found** (row gone or other identity) or **OptimisticLockException** (row changed).
- `OptimisticLockException` maps to HTTP 409 with error code 1012 (`server/.../ExceptionHandlers.java:1336`, `common/.../ErrorConstants.java:61`).
- Parent-before-child **row locks**: `lockParentForChildWrite` takes the parent catalog or schema row `FOR SHARE` or `FOR UPDATE` in the same transaction as the child write (`OccWriteSupport.java:215`, `SchemaMetaService.java:576,596,644`, `MetalakeMetaService.java:496`).
- Lock-wait timeouts are converted into `OptimisticLockException` per backend (`utils/ExceptionUtils.java:38-56`).

### 1.3 Entity cache and change log (C)

- SPI: `cache/EntityCache.java` (extends `SupportsEntityStoreCache`), `cache/Coherence.java` with `NONE | LOCAL_PER_NODE | SHARED`, `cache/CacheFactory.java` (name to class map, only `caffeine`).
- `cache/BaseEntityCache.java`: **allowlist of cacheable types** `CACHEABLE_TYPES` (`:55`).
- `cache/CaffeineEntityCache.java`: Caffeine map keyed `(NameIdentifier, EntityType)`, a radix-tree prefix index for cascading invalidation, striped locks (`SegmentedLock.java`), a weigher (`EntityCacheWeigher.java`).
- `storage/relational/RelationalEntityStore.java`: the cache sits in front of `get`, `exists`, `getTableId`, `batchGet`. It also holds the **epoch and pending-write fences** (`cacheInvalidationEpoch` `:124`, `cacheBlocks` `:129`, `beginPendingWrite` / `endPendingWrite` `:1033-1075`, `cacheIfEpochUnchanged` `:970`).
- `cache/CachedEntityIdResolver.java`: name to ID resolution served from the same cache, "all or nothing" for the full ancestor chain.
- Writer side: `JDBCBackend.java:258-300,471,1066,1335` append a row to `entity_change_log` **in the same transaction** as the change (`EntityChangeLogWriter.java:39`). Allowlisted types only, plus function drops (for authorization).
- Transport: `EntityChangeLogPoller.java` (one per JVM, one shared cursor, listeners called on the poller thread), `EntityChangeLogCleaner.java`, `EntityChangeLogNameIdentifierCodec.java`.
- Listeners today: `EntityCacheChangeLogListener` (entity cache), `CatalogChangeLogListener` (`catalog/CatalogChangeLogListener.java`), `JcasbinChangeListener` (`server-common/.../jcasbin/JcasbinChangeListener.java`).
- Table `entity_change_log(id BIGSERIAL, metalake_name, entity_type, entity_full_name VARCHAR(512), operate_type SMALLINT, created_at BIGINT)` plus index on `created_at` (`scripts/postgresql/schema-2.0.0-postgresql.sql:1176-1191`). Operate codes 1=ALTER, 2=DROP, 3=INSERT "never re-used".
- Config keys:

| Key | Default | Note |
|---|---|---|
| `gravitino.cache.enabled` | true | |
| `gravitino.cache.implementation` | `caffeine` | the design doc calls it `gravitino.cache.impl` |
| `gravitino.cache.maxEntries` | 10000 | **ignored** when weigher is on |
| `gravitino.cache.enableWeigher` | true | |
| `gravitino.cache.expireTimeInMs` | 3600000 | expire after write |
| `gravitino.cache.lockSegments` | 16 | |
| `gravitino.entityChangeLog.pollIntervalSecs` | 3 | |
| `.pollBatchSize` | 2000 | full batch triggers an immediate re-poll |
| `.replayWindowSecs` | 60 | 0 disables replay, max 86400 |
| `.replayMaxEntries` | 100000 | |
| `.retentionSecs` | 30 days | 0 disables cleanup |
| `.cleanupIntervalSecs` | 1 day | |

  (`Configs.java:207-278,546-611`)

### 1.4 Other caches (D)

- **Authorization** (`server-common/.../jcasbin/JcasbinAuthorizer.java`, 2363 lines): three tiers. (1) per-request dedup in `AuthorizationRequestContext`; (2) **version-validated** shared caches (`userRoleCache`, `groupRoleCache`, `loadedRoles`), each entry carries the `*_meta.updated_at` it was loaded at and every read probes the DB value (`:108-114`); (3) **eventually consistent** `metadataIdCache` (name to ID) and `ownerRelCache`, invalidated by the change-log poller and an owner poller (`:115-119`). Knobs: `gravitino.authorization.jcasbin.cacheExpirationSecs` (3600), `roleCacheSize` (10000), `ownerCacheSize` (100000), `metadataIdCacheSize` (100000), `changePollIntervalSecs` (3) (`Configs.java:430-475`).
- **Catalog wrapper cache** (`CatalogManager.java:559`): expire after access, `gravitino.catalog.cache.evictionIntervalMs` = 1 h. A removal retires the wrapper; in-flight operations keep a lease.
- **Credential cache** (`core/.../credential/CredentialCache.java`): per catalog, key = `(credentialType, CredentialContext)`; the context for path credentials includes user name and read and write paths (`common/.../PathBasedCredentialContext.java:79`). Entry lifetime = **15 % of the credential's remaining life** (`CredentialConfig.java:36`).
- **Iceberg REST**: table metadata cache validated against the catalog's current metadata location on every hit (`iceberg-common/.../BaseTableMetadataCache.java:71`); scan-plan cache keyed by snapshot ID, filter, projection, schema ID (`ScanPlanCacheKey.java`); idempotency store `in-memory` (best effort, single node) or `jdbc` (`design-docs/iceberg-idempotency-key.md`).
- Docs and design docs: `design-docs/gravitino-entity-cache-multinode-design.md` (current), `design-docs/cache-improvement-design.md` (marked "earlier proposal" at its top; its one-second poll default is obsolete), `design-docs/entity-store-consistent-reads-audit.md`, `docs/gravitino-server-config.md:342-409`.

### 1.5 Extension points

`EntityCache` SPI (`gravitino.cache.implementation`), `GravitinoCache<K,V>` (authorization caches), `EntityChangeLogListener` (`supportsReplay()` opt-in), `SupportsEntityChangeLog` on the store, `IdempotencyStore`, `TableMetadataCache`, `ScanPlanCache`. A Redis `SHARED` cache is **designed but not in the code** (see section 4).

### 1.6 Relation to mudcat

mudcat has one PostgreSQL database and a commit model (requirements checked against the newest state, every response names its commit). Gravitino's lessons divide as follows:

- Mechanism A (JVM-local locks) is the thing to **avoid**; mechanism B is the thing to **copy**. Gravitino's own comments admit that A cannot protect multi-node writes.
- Mechanism C shows how to do a derived cache with a change feed on one database. It works but gives only eventual coherence with no read token. mudcat's commit IDs can make this exact.
- Mechanism D shows three useful patterns: probe-validated caches (JCasbin versions, Iceberg metadata location), immutable-keyed caches (scan plan by snapshot), and short-lived credentials cached for a fraction of their life.

## 2. Candidate requirements

Kind: IMPL = Gravitino does it. GAP = Gravitino fails or omits it; mudcat should do better.

| Ref | Kind | Requirement (mudcat style) | Rationale / Gravitino evidence | Area | Ver |
|---|---|---|---|---|---|
| CL-R1 | IMPL | mudcat shall treat each cache as derived state that a client write never relies on. | "The cache never holds the truth. Every write goes to the DB first, under the version lock" (`design-docs/gravitino-entity-cache-multinode-design.md:266`). A stale cache cannot cause a lost update. | OP | I |
| CL-R2 | GAP | mudcat shall state in each read response the ID of the newest commit that the answering node had applied. | Gravitino returns no version with reads, so a client cannot tell a stale peer from a fresh one. Docs: "this does not introduce linearizable reads" (`docs/gravitino-server-config.md:408`). Matches mudcat's "every response names the commit it saw". | IN | T |
| CL-R3 | GAP | When a request carries a minimum commit ID, mudcat shall answer from a state that includes that commit or reject the request with a retryable status. | Gravitino offers read-your-writes only on the writing node and, in design, only with a Redis cache: "eventual (next successful poll)" vs "strong (read-your-writes)" (`design-docs/...multinode-design.md:409`). No client-visible token exists. | IN | T |
| CL-R4 | IMPL | mudcat shall check the requirements of a commit against the stored newest state inside the commit transaction, never against a cache. | Version-guarded `UPDATE ... AND current_version = ?` (`TableMetaBaseSQLProvider.java:285`); the REST layer pre-check of a Semantic Model revision is repeated inside the store update (`SemanticModelOperations.java:364`, `ManagedSemanticModelOperations.java:294`). | ID | T |
| CL-R5 | IMPL | mudcat shall report a failed commit requirement as a conflict, distinct from a not-found result, and apply no part of the commit. | `OccWriteSupport.writeFailure` returns `NoSuchEntityException` or `OptimisticLockException`; HTTP 409 code 1012 (`OccWriteSupport.java`, `ErrorConstants.java:61`). Stats docs: "No part of that update is committed" (`api/.../SupportsStatistics.java:49`). | ID | T |
| CL-R6 | GAP | When a commit requirement fails, mudcat shall return the ID of the newest commit and the failed requirement in the error body. | The 409 body is a message string "was modified concurrently; retry the operation" (`ExceptionUtils.java:96-101`). The client cannot tell which fact changed and must re-read. | IN | T |
| CL-R7 | IMPL | mudcat shall keep the version counter of a record increasing when the record is overwritten, deleted and recreated, or restored. | "Keep the OCC token monotonic on overwrite. Resetting it ... would let a writer that read the same old version pass its CAS" (`TableMetaBaseSQLProvider.java:250`). | ID | T |
| CL-R8 | GAP | mudcat shall detect a changed record with a commit sequence number and shall not use a wall-clock timestamp for this. | Authorization staleness uses `*_meta.updated_at` in milliseconds. The design admits "same-millisecond collision risk (two writes within 1 ms yield the same value → cache misses the second change)" (`design-docs/cache-improvement-design.md:406`). Owner inserts stamp the JVM clock (`POConverters.java:1662`), owner soft-deletes the DB clock (`OwnerMetaBaseSQLProvider.java:194`). | ID | T |
| CL-R9 | IMPL | mudcat shall take every stored timestamp and every expiry decision from the database clock. | "every server has its own clock, while the database provides a single clock all of them agree on" (`mapper/provider/DatabaseTimeSQL.java:26`). Change-log insert, retention and replay window all use it (`EntityChangeLogBaseSQLProvider.java:41,113`). Idempotency records are the counter-example: "no clock-skew grace period" (`design-docs/iceberg-idempotency-key.md:421`). | OP | I |
| CL-R10 | IMPL | mudcat shall write a change-stream record in the same transaction as the change that it describes. | `EntityChangeLogWriter.append` runs in the caller's transaction; a rollback removes both (`EntityChangeLogWriter.java:39`; `JDBCBackend.java:258-300`). The design lists "entity and row roll back together" as a test. | EV | T |
| CL-R11 | IMPL | mudcat shall deliver every committed change-stream record to each consumer, although sequence numbers commit out of order. | "IDs are unique but allocation order is not commit order; callers also replay recent rows to catch late commits below the cursor" (`EntityChangeLogBaseSQLProvider.java:42`). Gravitino adds a bounded replay sweep with de-duplication (`EntityChangeLogPoller.java:414-520`). Reference #11736 (gap-safe polling). | EV | T |
| CL-R12 | GAP | mudcat shall tell a consumer whose cursor is older than the retained history to resynchronise, and shall not skip silently. | Replay "does not guarantee delivery beyond the window or after log pruning" (`EntityChangeLogPoller.java:77-78`). A row that commits later than the 60 s window is lost until the cache TTL (1 h) expires it. | EV | T |
| CL-R13 | GAP | mudcat shall not advance a consumer cursor past a record that the consumer has not applied. | The poller "always moves the read position forward, even if a listener failed" because one cursor is shared (`EntityChangeLogPoller.java:34-53`). Each listener must then clear its whole cache. If the clear also fails the node serves stale data until TTL (`EntityCacheChangeLogListener.java:48-61`). | EV | T |
| CL-R14 | IMPL | When a consumer fails to apply a change, mudcat shall make the consumer discard the derived state that the change could affect, and shall count the event. | Fallback clear plus `fallbackCleared()` metric (`EntityCacheChangeLogListener.java:154-176`; `CatalogChangeLogListener.java`; `JcasbinChangeListener.java:236-243`). | OP | T |
| CL-R15 | IMPL | mudcat shall bound the age of every cache entry from the time of its write, not from its last read. | "With an access-based TTL a stale entry that keeps being read would never expire, so a single missed invalidation would become permanent on exactly the hottest keys" (`CaffeineEntityCache.java:495`; test `TestCaffeineEntityCacheExpiration.testExpiresAfterWriteNotAfterAccess`). Counter-example in the same code base: catalog wrapper cache uses expire-after-access (`CatalogManager.java:559`). | PF | T |
| CL-R16 | IMPL | mudcat shall cache a kind of record only after its invalidation rule is written down and tested. | Cache allowlist; "a new entity type may contain data that no existing invalidation path covers, so it must remain non-cacheable" (`SupportsEntityStoreCache.java` `put` javadoc; `BaseEntityCache.java:55`; tests `testRoleUserGroupAreNotCached`, `testModelAndModelVersionAreNotCached`). | OP | I |
| CL-R17 | IMPL | mudcat shall not cache a record that embeds data copied from another record, unless a change of the source invalidates the copy. | User, group, role embed role names and securable objects; model version holds a URI; function holds code. "A cached user still lists the dropped role" (`design-docs/...multinode-design.md:125-137`). | ST | I |
| CL-R18 | IMPL | mudcat shall key a cached derived result by the immutable version it was computed from, so that a moved pointer selects a different key. | Scan-plan cache key = snapshot ID + filter + projection + schema ID (`ScanPlanCacheKey.java`). | PF | T |
| CL-R19 | IMPL | When a cache entry copies a mutable pointer, mudcat shall compare the entry to the current pointer value before it serves the entry. | Iceberg table metadata cache compares cached `metadataFileLocation` to the catalog's current location on each hit and invalidates on mismatch (`BaseTableMetadataCache.java:71`). Immutable content, cheap probe. | PF | T |
| CL-R20 | IMPL | mudcat shall make the revocation of a grant effective at the next authorization check on every node. | Role, user and group data are version-validated per request, "No TTL is relied on for correctness" (`JcasbinAuthorizer.java:108-114`); design 4.2: "must take effect on the next auth request on any node, not after TTL expiry". | AC | T |
| CL-R21 | GAP | mudcat shall not authorise a request with a cached mapping from a name to an ID or with a cached owner that is older than the newest commit. | `metadataIdCache` and `ownerRelCache` are eventual; the design accepts "⚠️ ≤ 1 s window ... `enforce(id=100)` may ALLOW via orphan JCasbin policy" after a drop (`design-docs/cache-improvement-design.md:859,871`). Owner change on a peer: "old owner retains access / new owner blocked". The window is 3 s today (`Configs.java:469`). | AC | T |
| CL-R22 | GAP | mudcat shall accept and return a consistency token with each authorization check, so that a caller can require a check at least as new as a given commit. | Gravitino authorization has no token; TOCTOU between version probe and `enforce()` is "bounded to the duration of one auth request" and accepted (`design-docs/cache-improvement-design.md:877-929`). Zanzibar zookies and SpiceDB tokens fix this. | AC | T |
| CL-R23 | IMPL | When mudcat cannot read complete or current authorization data, mudcat shall deny the request. | Partial role load: no loaded marker, 10 s retry backoff, `hasDenyPolicy` reports a possible deny (`JcasbinAuthorizer.java:161,168`); after 3 reloads the check "fails closed". | AC | T |
| CL-R24 | IMPL | mudcat shall include the caller identity and the revision of each applicable policy in the cache key of every response that varies by caller. | Design for tag-based read restrictions: ETag identity includes "authenticated subject and identity revision; effective policy and tag revisions; schema revision; read-restriction signature"; a metadata-location-only `304` must not precede restriction resolution (`design-docs/tag-based-read-restrictions-for-iceberg-rest.md:727-740`). | UP | I |
| CL-R25 | IMPL | When mudcat caches a credential, mudcat shall remove it after a fixed fraction of its remaining life and shall not cache the absence of a credential. | Ratio 0.15 default: a cached credential is served only while at least 85 % of its life remains; absence expires at once (`CredentialCache.java:42-58`, `CredentialConfig.java:36`). Key includes the user and paths. | RD | T |
| CL-R26 | GAP | mudcat shall stop serving a cached credential for a principal when a grant of that principal is revoked. | The credential cache has no invalidation hook except `close()` (`CredentialCache.java:96-101`); the key holds the user, so a revoked user keeps a cached credential until it expires (inferred). | RD | T |
| CL-R27 | IMPL | mudcat shall take locks in one written total order, parent before child. | Tree lock, lifecycle lock, cache key (`CatalogManager.java:885`); catalog row before schema rows "so two overlapping cascades cannot deadlock" (`SchemaMetaService.java:596`); metalake cascade locks all catalog rows first (`MetalakeMetaService.java:496`). | OP | I |
| CL-R28 | IMPL | When mudcat creates a child, mudcat shall hold a lock on the parent row until the child's transaction ends. | `lockParentForChildWrite` / `doWithSchemaWriteLock`: "If callers locked the schema in one transaction and wrote the child in another, the lock would be released too early" (`SchemaMetaService.java:617-622`). A drop of the parent then waits or fails. | ST | T |
| CL-R29 | GAP | mudcat shall not rely on in-process locks for the correctness of any write. | Gravitino's own comments: "tree locks are local to one server" and a load can overwrite an alter's column IDs (#14279) (`TableOperationDispatcher.java:294,1463`); "without a distributed TreeLock, another node may recreate the same table" (`IcebergTableHookDispatcher.java:126`). Tree locks give a false sense of safety. | OP | A |
| CL-R30 | GAP | mudcat shall limit every lock wait with a timeout and fail the request with a retryable status. | `TreeLockNode.lock` blocks without a timeout (`TreeLockNode.java:152`). The deadlock checker only logs after 30 s (`LockManager.java:138`). | OP | T |
| CL-R31 | IMPL | mudcat shall report a database lock-wait timeout as a retryable conflict and shall not report it as an internal error. | `isLockWaitTimeout` per backend: MySQL 1205, H2 50200, PostgreSQL 55P03 (`ExceptionUtils.java:38-56,154`). A deadlock is not folded into this: "points at an inconsistent lock order, which should be fixed" (`:147`). | OP | T |
| CL-R32 | IMPL | mudcat shall build each aggregate in a response from one database statement snapshot. | Audit of torn reads: one SELECT now joins table, current version and columns; role and privileges; user and memberships. "A concurrent overwrite/drop may leave the reader with the complete older snapshot or with a not-found result; it must not produce a root from one state and dependent rows from another" (`design-docs/entity-store-consistent-reads-audit.md:25`). Test `TestConsistentEntityReads`. | ST | T |
| CL-R33 | GAP | mudcat shall build all data in one response from the same commit, including parent names and permissions. | The audit explicitly excludes it: "target renames and the parent-name reads are not a global snapshot of the hierarchy. This patch does not ... promise cross-entity snapshot isolation" (`...audit.md:45`). Batch get "has no snapshot covering the entire batch" (`:38`). | ST | T |
| CL-R34 | IMPL | A reader shall not store a value in a cache if an invalidation of the key or of an ancestor happened after the read began. | Epoch counter plus pending-write counters and a "publishing" block (`RelationalEntityStore.java:970-1010,1034-1116`). Tests `TestEntityCachePublicationRace.testParentDropMustFenceAnInflightChildPointRead`, `testInvalidationDuringPublicationRemovesJustCachedValue`. | PF | T |
| CL-R35 | IMPL | mudcat shall invalidate a cache entry before and after the write that changes it, and shall block cache refill of that key while the write is in flight. | `beginPendingWrite` invalidates and blocks; `endPendingWrite` invalidates again before it releases (`RelationalEntityStore.java:1034-1056`). Catalog manager: "Invalidating before the update creates a window where the background thread repopulates the cache with the old entity" (`CatalogManager.java:1282`). | PF | T |
| CL-R36 | IMPL | mudcat shall invalidate all cached descendants of a container together with the container, and shall not invalidate siblings. | Radix-tree prefix scan; tests `testInvalidateDoesNotEvictSiblingsWithSharedNamePrefix`, `testInvalidateWithDottedLevelsDoesNotCollide` (`TestCaffeineEntityCacheInvalidation.java:114,130,167`). | PF | T |
| CL-R37 | IMPL | mudcat shall encode a name path in any cache key or change-stream record with a length-prefixed or escaped form that cannot collide. | Dot-joined names collide: table `a.b` in schema `sch` and table `b` in schema `sch.a` both give `sch.a.b` (`CaffeineEntityCache.java:374`). Change log uses a versioned length-prefix encoding for names with dots (`EntityChangeLogNameIdentifierCodec.java:40-60`). | ST | T |
| CL-R38 | IMPL | mudcat shall start each node with an empty derived cache and a change-stream cursor that is read before the first cache fill. | New instance "initialize[s] its cursor from selectMaxChangeId() because its cache starts empty" (`EntityChangeLogBaseSQLProvider.java:44-49`); replay also scans rows below the start cursor (`EntityChangeLogPoller.java:240-262,515`). | OP | T |
| CL-R39 | IMPL | mudcat shall expose, per node, the newest change applied and the age of the oldest change not yet applied. | Metrics for DB tail, cursor, record lag, last successful poll age, listener failures, fallback clears (`design-docs/...multinode-design.md:344`; `metrics/source/EntityChangeLogMetricsSource.java`). | OP | D |
| CL-R40 | GAP | mudcat shall write the idempotency record in the same transaction as the commit that it protects. | Gravitino reserves a key, runs the mutation, then finalizes. "a 5xx is not proof that the catalog was unchanged"; "no heartbeat or in-flight renewal"; the lifetime must exceed the longest execution or "overlapping execution" is possible (`design-docs/iceberg-idempotency-key.md:363-399`). The in-memory store is "best-effort, single-node, dev/test only" (`:259`). With one PostgreSQL database mudcat can make this atomic. | ID | T |
| CL-R41 | GAP | When mudcat registers an external-format table, mudcat shall record the external version it observed with each registration update and shall reject an update based on an older observation. | Gravitino's own proposed fix for the load-versus-alter race (#14279): "the load may write only if the version and epoch are unchanged and no in-progress mark is set" (`TableOperationDispatcher.java:1463-1468`). Today a column rename can become drop plus add and "removes the renamed column's tags". | PL | T |
| CL-R42 | GAP | mudcat shall make a drop in an external catalog and the removal of its registration one atomic outcome, or shall expose the registration as unverified until reconciled. | "A true out-of-band drop can therefore leave a stale registration that requires separate cleanup" (`TableOperationDispatcher.java:475-479,534`). Iceberg path reconciles "best effort" (`IcebergTableHookDispatcher.java:129`). | PL | T |

Requirements that are **not** worth copying: relation caching (Gravitino removed it because of the reverse-index problem, `design-docs/...multinode-design.md:82-105`), a distributed Redis cache (designed only), and tree locks.

## 3. Quirks and gotchas ledger

- **Tree lock is JVM-local, so it fakes cross-node safety**
  - What happens: the tree lock serialises calls only inside one server. With several servers behind a balancer, concurrent alters and loads race. Comments name the issue (#14279).
  - Cause category: CONCURRENCY
  - Evidence: `core/.../catalog/TableOperationDispatcher.java:294` "no tree lock orders them across servers"; `:1463` "tree locks are local to one server"; `iceberg/.../IcebergTableHookDispatcher.java:126` "without a distributed TreeLock".
  - Mudcat implication: CL-R29, CL-R41.

- **Alter takes only a READ lock on the table**
  - What happens: a non-rename alter locks the table READ, so two alters and a load of the same table run in parallel inside one server. The code relies on the store's version check, not on the lock.
  - Cause category: CONCURRENCY
  - Evidence: `TableOperationDispatcher.java:309` `nameIdentifierForLock.equals(ident) ? LockType.READ : LockType.WRITE`; `:290-296` "The read lock on the table does not order this alter against a concurrent load or another alter".
  - Mudcat implication: CL-R4, CL-R29.

- **A rename needs a lock on the parent, because only one WRITE can be taken per call**
  - What happens: a rename within a schema write-locks the schema; a rename to another schema write-locks the catalog; a metalake rename write-locks the root. The cost is that one rename blocks every sibling.
  - Cause category: OTHER
  - Evidence: `TableOperationDispatcher.java:288` "the treeLock tool currently does not support acquiring write locks on two tables at the same time"; `MetalakeManager.java:364`.
  - Mudcat implication: use row-level locks on both parents in the order of CL-R27; mudcat renames change a mutable name pointer, not an ID.

- **No lock upgrade; code releases the lock before cleanup**
  - What happens: `ReentrantReadWriteLock` cannot upgrade READ to WRITE. Code that needs both releases the first lock, then takes the second, and rechecks.
  - Cause category: CONCURRENCY
  - Evidence: `TableOperationDispatcher.java:494` "Release the table lock before cleanup to avoid a lock upgrade."; `cleanUpOrphanedSchema` (`:1509`) "A queued schema writer also blocks new readers. Avoid queuing one behind another table's source DDL"; `cache/SegmentedLock.java:234` `withGlobalLock` throws instead of deadlocking.
  - Mudcat implication: CL-R27; design operations so that one transaction takes all its locks at once.

- **Lock waits have no timeout; deadlock detection only logs**
  - What happens: `lock()` blocks without limit. A scheduled checker warns when a thread holds a node longer than 30 s. It does not break the wait.
  - Cause category: CONCURRENCY
  - Evidence: `lock/TreeLockNode.java:152` `readWriteLock.readLock().lock()`; `lock/LockManager.java:138` `> 30000`.
  - Mudcat implication: CL-R30.

- **Tree lock bookkeeping is itself bounded memory**
  - What happens: one node per name segment. Above 50 % of `maxNodes` a janitor evicts idle nodes; above `maxNodes` new lock requests fail with `IllegalStateException`. Child order is shuffled so eviction is not biased. A failed `lock()` after `createTreeLock` may leave a reference count above zero for the nodes it never locked (inferred from `TreeLock.lock` unlocking only held nodes), which blocks eviction.
  - Cause category: OTHER
  - Evidence: `LockManager.java:166` (0.5 × max), `:294` "has reached the max node count"; `TreeLockNode.java:213` `Collections.shuffle`.
  - Mudcat implication: DB row locks do not have this failure. Keep lock state in PostgreSQL.

- **Lock order is documented only for tree lock, lifecycle lock, cache key**
  - What happens: the order is "tree lock, lifecycle lock, then the per-key cache operation". Cache SPI methods must not take another entity's lock: "nesting two entity locks can deadlock". Caffeine's removal listener must never wait for a segment lock, because it can run on a writer thread that holds Caffeine's eviction lock (`CallerRunsPolicy` on a full queue).
  - Cause category: CONCURRENCY
  - Evidence: `CatalogManager.java:885`; `cache/SupportsEntityStoreCache.java` `invalidateOnKeyChange` javadoc; `cache/CaffeineEntityCache.java:75` "The removal listener must therefore never wait for a segment lock."
  - Mudcat implication: CL-R27; keep derived-cache code free of nested locks.

- **Database locks use the same parent-first order, and H2 differs**
  - What happens: a schema create takes the catalog row `FOR SHARE`, an exclusive lock only for nested names that may create ancestors or for an overwrite. A schema drop and a metalake drop take parent rows `FOR UPDATE` first. H2 has no `FOR SHARE`, so H2 uses `FOR UPDATE`, which serialises what MySQL and PostgreSQL run in parallel.
  - Cause category: STORE
  - Evidence: `SchemaMetaService.java:564-575`; `storage/relational/mapper/provider/h2/UserMetaH2Provider.java:32` "H2 has no shared row-lock syntax"; `CatalogMetaBaseSQLProvider.java:153` `LOCK IN SHARE MODE` (MySQL syntax in the base provider).
  - Mudcat implication: CL-R27, CL-R28; mudcat is PostgreSQL only, so use `FOR KEY SHARE` / `FOR UPDATE` and test lock order with two connections.

- **Isolation is READ COMMITTED and reads do not lock**
  - What happens: reads that need several rows used to run as several statements and returned torn results (table without columns, role with another role's privileges). Fix: join all rows in one SELECT. Cross-entity snapshots are still not promised.
  - Cause category: STORE
  - Evidence: `session/SqlSessions.java:67`; `design-docs/entity-store-consistent-reads-audit.md:45` "not a global snapshot of the hierarchy"; `:38` batch get "no snapshot covering the entire batch".
  - Mudcat implication: CL-R32, CL-R33. Use `REPEATABLE READ` snapshots or explicit `pg_export_snapshot` for multi-record answers, tied to a commit ID (CL-R2).

- **Lock-wait timeouts are vendor specific, and deadlocks are excluded**
  - What happens: MySQL reports 1205 (SQLState varies by driver, 40001 is also a deadlock), H2 50200, PostgreSQL 55P03 only if `lock_timeout` is set. Statistics batches accept deadlocks as conflicts, other paths do not.
  - Cause category: STORE
  - Evidence: `utils/ExceptionUtils.java:38-56,147`; `service/StatisticMetaService.java:279-296,359-370` (deadlock 1213, 40P01, 40001).
  - Mudcat implication: CL-R31; set `lock_timeout` explicitly in PostgreSQL.

- **`exists` and `get` can say a peer-dropped entity is still there; `list` cannot**
  - What happens: `exists` returns `cached || backend.exists`, so a cached copy of an entity that another node dropped gives `true` until the poll catches up. `list` never reads the cache and gives the truth. The two disagree in the window. A non-overwriting `put` first clears a "peer-dropped identity still cached here". The catalog manager's "already exists" pre-check uses `store.exists` (a stale positive would reject a create, inferred).
  - Cause category: CONCURRENCY
  - Evidence: `RelationalEntityStore.java:329` `return cached || backend.exists(ident, entityType)`; `:348` "Clear any same-key stale row (for example a peer-dropped identity still cached here)"; `design-docs/...multinode-design.md:70` "`list(namespace)` skips the cache, so it is never stale"; `CatalogManager.java:897`.
  - Mudcat implication: CL-R2, CL-R3, CL-R33.

- **Cache fills can resurrect stale data unless fenced**
  - What happens: a reader loads v1, a writer commits v2 and invalidates, then the reader stores v1. Gravitino counts invalidations (epoch), blocks keys that have a write in flight (and all their descendants by prefix), and hides a freshly published entry until the publish can no longer be rolled back. Parent and child keys may use different lock stripes, so the epoch is global, not per key.
  - Cause category: CONCURRENCY
  - Evidence: `RelationalEntityStore.java:124-129,970-1010`; `TestEntityCachePublicationRace.java:66` `testParentDropMustFenceAnInflightChildPointRead` ("Enum identity hash codes differ across JVMs. Select keys on distinct real segments"); `docs/gravitino-server-config.md:405-408`.
  - Mudcat implication: CL-R34, CL-R35. Better: cache entries carry the commit ID they were read at, and a fill is rejected if the node's applied commit is newer.

- **Only some entity types may be cached, and the metalake on/off flag is the accepted exception**
  - What happens: allowlist: metalake, catalog, schema, table, topic, view, fileset, tag, policy, job. User, group, role (derived fields), model, model version, Semantic Model, function ("load-bearing content") are read from the store each time. The design lists six alters that give a **silently wrong** answer if stale, including a metalake **disable**: another node "still thinks the metalake is on and lets operations run". The design accepts that window.
  - Cause category: CONCURRENCY
  - Evidence: `cache/BaseEntityCache.java:55-66`; `design-docs/...multinode-design.md:298-315`; `metalake/MetalakeManager.java:142-147` (`metalakeInUse` reads through the cached `store.get`).
  - Mudcat implication: CL-R16, CL-R17. A lifecycle flag that gates writes (hold, takedown, disable) must be checked in the commit transaction, not through a cache.

- **For external catalogs the cache holds only IDs and audit fields**
  - What happens: table, view, topic and external schema metadata is read from Hive, Iceberg, JDBC or Kafka on each load; the cache supports only Gravitino's own ID and audit. That is why staleness is "safe" for them. A drop or rename in the source fails with not-found.
  - Cause category: MODEL
  - Evidence: `design-docs/...multinode-design.md:279`.
  - Mudcat implication: mudcat registers external formats and should keep the same split: content comes from the format, identity and policy from mudcat (CL-R41).

- **TTL: expire after write; `maxEntries` silently ignored; weight 0 never evicts**
  - What happens: default weigher on, so `maxEntries` has no effect. Metalake and catalog have weight 0 ("never evicted ... unless timeout"), schema 100, table-like 200, tag and policy 500. Budget `2 × (...)` is computed from assumed counts (10 metalakes, 200 catalogs each, 1000 schemas each). An entry heavier than the whole budget is never cached. Removal and index cleanup run asynchronously, so `size()` is an estimate.
  - Cause category: OTHER
  - Evidence: `cache/EntityCacheWeigher.java:54,68`; `docs/gravitino-server-config.md:362-367`; `CaffeineEntityCache.java:495`.
  - Mudcat implication: document that one knob overrides another, and expose cache sizing as bytes, not abstract weights.

- **Change-log IDs are not commit order**
  - What happens: `BIGSERIAL` / `AUTO_INCREMENT` IDs are allocated before commit. A transaction with a low ID can commit after a consumer has read past it. Gravitino re-scans the last 60 s of rows by `created_at`, de-duplicates by ID in a bounded set (100000), and starts at most one sweep per `max(pollInterval, window/4)`. A row that becomes visible later than window minus one sweep is missed. After a restart the de-dup set is empty, so startup history is delivered again.
  - Cause category: STORE
  - Evidence: `EntityChangeLogBaseSQLProvider.java:42`; `EntityChangeLogPoller.java:77-86,240-262,414-520`; `docs/gravitino-server-config.md:393` "recorded-age window ... using database time".
  - Mudcat implication: CL-R11, CL-R12. In PostgreSQL assign the stream position at commit (for example a serialising lock around "take position and commit", or the visibility-based approach used by logical decoding), not from a sequence.

- **A failed listener is not retried; the shared cursor moves on**
  - What happens: one poller, one cursor for all listeners. A failure in one listener must not hold the others back, so the cursor advances. Each listener repairs itself by clearing everything. If the clear fails too, only TTL protects the node. Catalog clear retires every catalog wrapper (connection pools, class loaders) and so "the clear only happens when a normal removal failed".
  - Cause category: CONCURRENCY
  - Evidence: `EntityChangeLogPoller.java:44-60`; `EntityCacheChangeLogListener.java:44-61,154-176`; `CatalogChangeLogListener.java:34-52`.
  - Mudcat implication: CL-R13, CL-R14; give each consumer its own durable cursor.

- **The change log carries the old name on ALTER; the contract is unwritten in code**
  - What happens: for rename and update the row holds the **pre-mutation** name; for drop it holds the dropped name. The authorization listener needs exactly that. "If a future change starts emitting the new post-rename name, this invalidation will silently miss and stale entries will only clear via LRU eviction." Peers that cached the new name see a cache miss, which is correct.
  - Cause category: COMPAT
  - Evidence: `JcasbinChangeListener.java:227-235`; schema comment `scripts/postgresql/schema-2.0.0-postgresql.sql:1190`.
  - Mudcat implication: change-stream records should name the entity by immutable ID plus the old and new names, so consumers need no convention.

- **Name paths with dots**
  - What happens: the first change-log format joined names with `.`. A dotted name made the key ambiguous. Now a versioned prefix with `len:value` pairs is written only when a level contains a dot or the legacy text starts with the prefix. The same collision exists in the cache index, which escapes `.` with `\u0001`. Hierarchical (nested) schema names add a second child boundary, the schema separator inside one level, so an invalidation scans two prefixes. `entity_full_name` is `VARCHAR(512)`; a long encoded name could exceed it (inferred).
  - Cause category: MODEL
  - Evidence: `EntityChangeLogNameIdentifierCodec.java:40-60`; `CaffeineEntityCache.java:374,399-410`; tests `testInvalidateWithDottedLevelsDoesNotCollide`, `testInvalidateHierarchicalSchemaCascadesToNestedSchemas`.
  - Mudcat implication: CL-R37.

- **Poller clocks: DB clock for the change log, JVM clock for the owner stream**
  - What happens: the change log stamps `created_at` with the DB expression. The owner poller uses a keyset cursor `(updated_at, id)` over `owner_meta`; owner inserts take `System.currentTimeMillis()` of the writing server and soft-deletes take the DB clock. A server whose clock is behind could write a row that the cursor has already passed (inferred; the cursor trusts `updated_at` order).
  - Cause category: CONCURRENCY
  - Evidence: `utils/POConverters.java:1662` `.withUpdatedAt(System.currentTimeMillis())`; `OwnerMetaBaseSQLProvider.java:463` `updated_at > #{lastConsumedUpdatedAt}`; `DatabaseTimeSQL.java:26`.
  - Mudcat implication: CL-R8, CL-R9.

- **Authorization uses three consistency levels in one component**
  - What happens: role and group membership are validated on every request by comparing a stored `updated_at`; name to ID and owner mappings are cached and invalidated by pollers. Same-millisecond writes can share one `updated_at`. After a drop, a peer may still map the old name to the old ID, and an orphaned allow policy for that ID may allow for up to one poll. Recreating a name gives a new ID, so a recreated object is denied by default.
  - Cause category: SECURITY
  - Evidence: `JcasbinAuthorizer.java:98-119`; `design-docs/cache-improvement-design.md:406,859-871`; `JcasbinAuthorizer.java:161` partial-load backoff.
  - Mudcat implication: CL-R20 to CL-R23. Immutable IDs help; a revoked grant must not depend on a name cache.

- **Authorization has bounded retry loops and fail-closed paths**
  - What happens: a role whose objects cannot all be resolved (for example a dropped table) is not recorded as loaded, is retried only every 10 s, and marks the role "incomplete" so list short-circuits do not hide a deny. A check that sees its role policies cleared by a concurrent change reloads at most 3 times and then fails closed. A clear-generation map is pruned, and a pruned generation forces one extra reload.
  - Cause category: CONCURRENCY
  - Evidence: `JcasbinAuthorizer.java:150-170,175-230`.
  - Mudcat implication: CL-R23. A Zanzibar-style store has the same hazards if it caches expansions.

- **Credential cache serves a fraction of the life, not the whole life**
  - What happens: credential TTL is 15 % of its remaining life at issue, so a client always gets at least 85 % of the lifetime left. Absence is never cached. The cache key has user, type and paths. A revocation does not purge it.
  - Cause category: SECURITY
  - Evidence: `CredentialConfig.java:36`; `CredentialCache.java:42-58`; `PathBasedCredentialContext.java:79`.
  - Mudcat implication: CL-R25, CL-R26.

- **Iceberg REST: the entity store is a shadow of the Iceberg catalog**
  - What happens: Iceberg REST commits go to the Iceberg catalog; Gravitino then imports the table and sets owner afterwards ("importTableAndSetOwner"), under the namespace's READ tree lock for creates only. A column-changing commit re-imports so the audit names the committing user. Drops reconcile "best effort" because another node may recreate the table between the catalog drop and the store delete.
  - Cause category: MODEL
  - Evidence: `IcebergTableHookDispatcher.java:70-135`; `IcebergNamespaceHookDispatcher.java:183-200` (namespace drop takes the top-level namespace WRITE lock).
  - Mudcat implication: mudcat's gateway must commit once, in its own database, and make the Iceberg view derived (CL-R29, CL-R42).

- **Idempotency state is outside the mutation's transaction**
  - What happens: reserve, run, finalize are three steps. A lost finalize leaves an in-flight record: a retry gets 409 with `Retry-After`. Expiry has no heartbeat, so a long mutation can outlive its record. Replicas with different clocks may reclaim a live key. In-memory store loses records on eviction and restart.
  - Cause category: CONCURRENCY
  - Evidence: `design-docs/iceberg-idempotency-key.md:363-399,421,259`.
  - Mudcat implication: CL-R40.

- **Optimistic conflicts exist in the public API contract, but only for some calls**
  - What happens: `ModelCatalog` says "Gravitino does not serialize calls ... from one server or from several" and tells callers to retry reads boundedly. Statistics updates fail whole with `OptimisticLockException` and no automatic retry. The Semantic Model REST call takes an `expectedRevision` from the client and checks it twice (REST layer and store). Table alters take no client-supplied precondition at all.
  - Cause category: SPEC
  - Evidence: `api/.../model/ModelCatalog.java:40-47`; `api/.../stats/SupportsStatistics.java:49-54`; `server/.../SemanticModelOperations.java:361-368`; `catalog/ManagedSemanticModelOperations.java:275-298`.
  - Mudcat implication: CL-R4, CL-R5; every mutating call takes requirements, as mudcat's commit model already requires.

- **Batch child deletes are limited by bind-parameter counts**
  - What happens: a cascade delete guards every child with `(id = ? AND current_version = ?)`. MySQL plans a long OR chain badly; PostgreSQL stops at 65 535 bind parameters (32 767 children). Chunk size is 500, and a short count raises a concurrent-child-modification conflict.
  - Cause category: STORE
  - Evidence: `service/OccWriteSupport.java:42-48,160-190`.
  - Mudcat implication: version-guarded cascades need chunking or a set-based `UPDATE ... FROM` in PostgreSQL.

- **Per-version table rows: overwrite must advance, not reset, the version**
  - What happens: the "insert on duplicate key update" path (overwrite or import) used to reset the version, which let a stale writer pass its CAS.
  - Cause category: CONCURRENCY
  - Evidence: `TableMetaBaseSQLProvider.java:250-257`.
  - Mudcat implication: CL-R7.

- **A load can write the store; loads and alters race on column IDs**
  - What happens: loading an external table compares catalog columns with stored columns and writes the difference. If the load read the catalog before a concurrent alter and the store after it, a column rename becomes drop plus add; the column loses its ID and its tags.
  - Cause category: CONCURRENCY
  - Evidence: `TableOperationDispatcher.java:1452-1468`; `:294-297`.
  - Mudcat implication: CL-R41. A read path must not write derived state without a commit requirement; mudcat enrichers write through commits.

## 4. Open questions, inconsistencies, half-implemented

- **Redis shared cache is only a design.** The multinode design describes `redis` (`SHARED`, Lua scripts, stale-write guard) and config keys `gravitino.cache.redis.*`. `CacheFactory.ENTITY_CACHES` has only `caffeine` (`cache/CacheFactory.java:30`) and no `*redis*` source exists under `core/src/main`. Only the `Coherence.SHARED` enum value and the "no listener for SHARED" branch (`RelationalEntityStore.java:210`) exist. Hence the only "strong read-your-writes" option in the design table is unavailable.
- **Config name drift.** Design says `gravitino.cache.impl` (`design-docs/...multinode-design.md:46`); code and docs say `gravitino.cache.implementation` (`Configs.java:594`). The docs say "Use the short name, not a fully qualified class name", but `CacheFactory` also accepts a class name (`ENTITY_CACHES.getOrDefault(name, name)`).
- **Design doc status.** `design-docs/cache-improvement-design.md` says at its top that its change-log pseudocode and one-second defaults "do not describe the current implementation", yet it still contains the three-tier authorization argument. Default poll is 3 s, not 1 s; the "≤ 1 s window" claims in sections 4.9 and 5.1 are therefore 3 s (`Configs.java:207,469`).
- **Schema comments are stale.** `entity_change_log.entity_type` comment lists "METALAKE | CATALOG | SCHEMA | TABLE | FILESET | TOPIC | MODEL | VIEW" and the table comment says "for targeted metadataIdCache invalidation", but the table now also carries tag, policy, job, domain, job template, semantic model rows (`JDBCBackend.java:1335`, `ModelMetaService.java:144`, `DomainMetaService.java:460`, `SemanticModelMetaService.java:489`). Operate code 3 (INSERT) is documented and "never re-used" but nothing writes it: creates emit no row.
- **Events written outside the central writer.** `JDBCBackend` is described as the single writer policy ("centralized", design `:232`), yet `ModelMetaService`, `DomainMetaService`, `SemanticModelMetaService`, `JobTemplateMetaService`, `SchemaMetaService:733`, `PolicyMetaService:560` also call `EntityChangeLogWriter.append`. Coverage rests on each service remembering (`JcasbinChangeListener.java:229` names the "services that log types it skips").
- **Create emits no change row.** The design assumes "no negative caching". If mudcat caches a "not found" for fast 404s, it must emit on create.
- **Catalog change-log events invalidate on every consumed event, including local ones.** "Name-based local mutation markers are not used because polling can race marker registration" (`docs/gravitino-server-config.md:402-404`). The cost is a local catalog reload (pool and class loader churn) on every catalog alter, accepted.
- **Table alter locks only READ.** Whether a stronger lock was intended is unclear (`TableOperationDispatcher.java:290-296` explains the choice and then points to #14279).
- **`purgeTable` locks the schema WRITE, `dropTable` the table WRITE.** No comment explains the difference (`:451-460` vs `:518-520`).
- **Redis stale-write guard, near-cache (L1 + L2) and precise relation caching are "later".** Roadmap phases 2 and 3 (`design-docs/...multinode-design.md:420-424`).
- **Replay can be disabled (`replayWindowSecs=0`)** and then late commits are missed with no signal. The docs recommend retention of at least ten polling intervals but do not say what a node offline longer than retention must do (its in-memory cache is empty after restart, but a node that is up and partitioned from the DB is not covered).
- **No fixed ordering between the three listeners.** They share one thread; a slow `JcasbinChangeListener` delays entity-cache invalidation (all "called synchronously on the poller thread", `EntityCacheChangeLogListener.java:41-45`).
- **Semantic Model REST pre-check unboxes `expectedRevision`.** `latest.revision() != expectedRevision` (`SemanticModelOperations.java:364`) compares an `int` with an `Integer`; the store call rejects a null later (`ManagedSemanticModelOperations.java:281`). Whether the REST layer validates null first was not checked.
- **JobManager multi-node.** `LocalJobExecutor` requires a shared staging directory across servers (`job/local/LocalJobExecutor.java:91,223,708`), another unstated multi-node precondition. Not analysed further (outside my scope).
