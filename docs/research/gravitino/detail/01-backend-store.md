> **Research note, not a requirement document.** Drafted by a code-reading agent from Apache Gravitino commit `9aa78cf` (2026-11-26), then spot-checked by the lead. Line numbers can drift by a few lines; search by file and symbol. Items marked `(inferred)` were not confirmed in code. The text does not follow Simplified Technical English. See [../README.md](../README.md) for the synthesis.

# Backend store (relational entity store, schema scripts, GC, change log)

Gravitino checkout 9aa78cf. Path shorthand used below (all repo-relative):
`R/` = `core/src/main/java/org/apache/gravitino/storage/relational/`; `CORE/` = `core/src/main/java/org/apache/gravitino/`; `S/` = `scripts/`; `PG`/`MY`/`H2` = `S/postgresql|mysql|h2/schema-2.0.0-<dialect>.sql`; `UPG` = `S/postgresql/upgrade-1.3.0-to-2.0.0-postgresql.sql`.
"(inferred)" marks conclusions I drew without a line that states them.

## 1. Decomposition

### 1.1 Layers (top to bottom)

```
EntityStore (CORE/EntityStore.java)  -- SPI: put/update(CAS-by-updater)/get/batchGet/list/delete(cascade)/batchPut/batchDelete
  EntityStoreFactory (CORE/EntityStoreFactory.java:36)  -- ENTITY_STORES map has ONE entry: "relational"
  RelationalEntityStore (R/RelationalEntityStore.java)   -- cache + pending-write fences + change-log poller/cleaner + GC owner
     EntityCache (CORE/cache/*)  Caffeine per node (Coherence.LOCAL_PER_NODE) | redis (SHARED) | NoOps
     CachedEntityIdResolver (CORE/cache/CachedEntityIdResolver.java)  name->id, all-or-nothing per hierarchy
     RelationalBackend (R/RelationalBackend.java)        -- SPI for the SQL side; also SupportsRelationOperations, SupportsDomainOperations, SupportsTableIdLookup
        JDBCBackend (R/JDBCBackend.java, 1360 lines)    -- big switch on EntityType -> *MetaService
           service/*MetaService (R/service/)             -- one per entity type; owns transaction + lock order + OCC
           service/*POStorageOps, HierarchicalConversionPOStorageOps, POStorageReadRouting  -- parent-id route vs full-name-join route
           mapper/*Mapper + mapper/*SQLProviderFactory   -- MyBatis; one SQL provider per dialect (base/ h2/ postgresql/), chosen by databaseId
           po/*PO, utils/POConverters.java (entity <-> row)  -- audit_info/properties are JSON strings
           session/SqlSessions, SqlSessionFactoryHelper  -- thread-local MyBatis session, DBCP2 pool, READ_COMMITTED
           converters/{H2,MySQL,PostgreSQL}ExceptionConverter -- vendor error code -> EntityAlreadyExists / IllegalArgument(too long)
  RelationalGarbageCollector (R/RelationalGarbageCollector.java) -- every node runs it
  EntityChangeLogPoller / Cleaner / CacheChangeLogListener (R/EntityChangeLog*.java) -- cross-node cache invalidation
  StringIdentifier (CORE/StringIdentifier.java) -- stamps entity ID into the external system's properties/comment
  EntityFieldLimits + Field.validate (CORE/EntityFieldLimits.java, CORE/Field.java:159) -- length limits that must equal column widths
  bulk/BulkManager (CORE/bulk/BulkManager.java) -- best-effort bulk with per-item result, max 100 items
  RandomIdGenerator (CORE/storage/RandomIdGenerator.java) -- 63 random bits
```

### 1.2 Schema (PostgreSQL 2.0.0, 52 tables; H2 and MySQL have the same table set)
- Entity roots: `metalake_meta`, `catalog_meta`, `schema_meta`, `table_meta`, `fileset_meta`, `topic_meta`, `model_meta`, `view_meta`, `function_meta`, `semantic_model_meta`, `tag_meta`, `policy_meta`, `user_meta`, `group_meta`, `role_meta`, `domain_meta`, `job_template_meta`, `job_run_meta`, `statistic_meta`.
- Version rows (history/snapshot, joined by `current_version`): `fileset_version_info`, `model_version_info`, `policy_version_info`, `function_version_info`, `view_version_info`, `semantic_model_version_info`, `table_version_info`, plus delta rows `table_column_version_info` (`column_op_type` 1 create, 2 update, 3 delete; PG:`COMMENT ON COLUMN table_column_version_info.column_op_type`).
- Relations (many-to-many or polymorphic, all with `deleted_at`): `user_role_rel`, `group_role_rel` (PG:367, 417), `role_meta_securable_object` (role -> (object id, type, privilege JSON)), `tag_relation_meta` (tag, object id, object type, tag_value), `policy_tag_relation_meta`, `owner_meta` (owner id/type, object id/type), `model_version_alias_rel`, `domain_rel` (child_id unique), `semantic_model_source_rel`, `idp_user_group_rel`.
- Infrastructure tables: `entity_change_log`, `iceberg_idempotency_meta` + `_generation_meta`, `iceberg_cleanup_job`, `scheduled_{expand,cron,commit}_tasks` (db-scheduler style, `task_data BYTEA`), `table_maintenance_action` (`finished_at=0` soft-slot unique, PG:1325), metrics tables.
- Common columns: `<x>_id BIGINT`, `<x>_name VARCHAR(128)`, `audit_info TEXT` (JSON), `current_version INT`, `last_version INT`, `deleted_at BIGINT NOT NULL DEFAULT 0` (epoch ms, 0 = live). Names unique as `UNIQUE (parent_id, name, deleted_at)` (PG:`schema_meta` line 96; metalake/catalog/tag/policy/role/user parent is `metalake_id`).
- ID model: random 63-bit `BIGINT` minted in Java (`CORE/storage/RandomIdGenerator.java:37`), plus a synthetic `id BIGINT IDENTITY` surrogate on relation/version tables.

### 1.3 Dialect handling
- `Configuration.setDatabaseId(h2|mysql|postgresql)` selects the SQL provider (`R/session/SqlSessionFactoryHelper.java`). `mapper/provider/base/` holds MySQL-flavoured SQL; `postgresql/` has about 38 overrides, `h2/` has 4. H2 runs in `MODE=MYSQL` (`R/database/H2Database.java:99`) and is the only backend that creates its own schema at start (`H2Database.java:58-66`); MySQL and PostgreSQL schemas are applied by the operator (docs/how-to-use-relational-backend-storage.md).
- Config keys: `gravitino.entity.store.relational.{jdbcUrl,jdbcDriver,jdbcUser,jdbcPassword,storagePath,maxConnections(100),maxIdleConnections(10),maxWaitMillis(1000)}`, `gravitino.entity.store.deleteAfterTimeMs` (default 7 d, allowed 10 min to 30 d, `CORE/Configs.java:76-78`), `gravitino.entity.store.versionRetentionCount` (1 to 10, `Configs.java:86-88`), `gravitino.entityChangeLog.{pollIntervalSecs(3), pollBatchSize(2000), replayWindowSecs(60), replayMaxEntries, retentionSecs(30 d), cleanupIntervalSecs(1 d)}`, `gravitino.cache.{enabled,implementation,maxEntries,expireTimeInMs,enableWeigher,lockSegments}`, `gravitino.bulk.maxItems` (100).
- Extension points: `RelationalBackend` SPI (docs say only `JDBCBackend` is supported), `EntityCache` (caffeine, redis), `MapperPackageProvider` via `ServiceLoader` (`SqlSessionFactoryHelper`), `EntityChangeLogListener` (third parties get incremental-only delivery unless they opt into replay).
- Tests run against real MySQL and PostgreSQL as well as H2 (`core/src/test/.../storage/relational/BackendTestSelector.java`, property `gravitino.core.test.backend`); `TestSQLScripts` loads fresh and upgraded schemas and re-runs the upgrade (`core/src/test/java/org/apache/gravitino/storage/TestSQLScripts.java:36-60`).

### 1.4 Neighbours
- Catalog dispatchers (`CORE/catalog/*OperationDispatcher.java`) call the store and stamp `StringIdentifier` into the external object; authorization (Jcasbin) keeps a name->id cache fed by the same change log (`R/EntityChangeLogPoller.java:53-65`); Iceberg REST keeps its own idempotency tables in the same database schema (`iceberg/iceberg-rest-server/.../idempotency/`).
- In-JVM `TreeLock` (`CORE/lock/LockManager.java`) is per process; cross-node safety comes from database row locks (inferred from the parent-row `FOR SHARE`/`FOR UPDATE` fences in `R/service/SchemaMetaService.java:564-700`).

## 2. Candidate requirements

Kind: IMPL = Gravitino does it; GAP = Gravitino fails or omits it and mudcat should do better.

### 2.1 Soft delete, unique keys, tombstones, GC

| Ref | Kind | Requirement | Rationale / Gravitino evidence | Area | Ver |
|---|---|---|---|---|---|
| BS-R1 | GAP | mudcat shall enforce name uniqueness among live children of one parent with a partial unique index that has no deletion marker in its key. | Gravitino puts `deleted_at` in every unique key (`PG:96`, `MY:62`) so a name can be reused after delete. Two deletes of one name in the same millisecond then collide; `DomainRelBaseSQLProvider.java:143-149` patches this for one table with `GREATEST(now, max(deleted_at)+1)`, all others use plain `DatabaseTimeSQL` (`DatabaseTimeSQL.java:40`). PG `CURRENT_TIMESTAMP` is constant inside a transaction, so two soft deletes in one transaction also tie. | ST | T |
| BS-R2 | GAP | mudcat shall keep the live ID of an entity unique without a deletion marker in the primary key. | Gravitino had to change the primary key of `schema_meta`, `table_meta`, `topic_meta`, `view_meta` to `(id, deleted_at)` in 2.0 so a retired external ID can be registered again (`UPG:21-34`, `PG:95,124`; test `TestSQLScripts.java:36-60`). Other roots keep `PRIMARY KEY (id)`. The key shape differs by table. | ID | I |
| BS-R3 | IMPL | mudcat shall make exactly one of two concurrent creations of the same live name succeed and shall report the other as a conflict. | Unique key violation is translated to `EntityAlreadyExistsException` for H2 (23505/1062), MySQL (1062), PostgreSQL (23505): `R/converters/H2ExceptionConverter.java:32`, `PostgreSQLExceptionConverter.java:30`, `MySQLExceptionConverter.java:30`. | ST | T |
| BS-R4 | IMPL | mudcat shall retain a deleted row for a configured period before it removes the row. | `deleteAfterTimeMs` default 7 d (`Configs.java:71-78`); GC deletes rows with `deleted_at > 0 AND deleted_at < now - period` (`R/RelationalGarbageCollector.java:83`). Tombstones double as change signals for owners (`OwnerMetaBaseSQLProvider.java:431`). | UP | T |
| BS-R5 | GAP | mudcat shall allow the retention period of deleted rows to be unlimited. | Gravitino caps it at 30 days and floors it at 10 minutes (`Configs.java:76-78,340-346`). A legal hold or takedown record cannot be kept in the same store beyond 30 days. | UP | T |
| BS-R6 | IMPL | mudcat shall remove expired rows in batches of bounded size, each batch in its own transaction. | `GARBAGE_COLLECTOR_SINGLE_DELETION_LIMIT = 100` (`Configs.java:112`); loop `while (deletedCount > 0)` per entity type (`RelationalGarbageCollector.java:83-114`); PG needs `DELETE ... WHERE id IN (SELECT id ... LIMIT n)` because it has no `DELETE ... LIMIT` (`UserMetaPostgreSQLProvider.java:153-159`). | PF | T |
| BS-R7 | GAP | mudcat shall make the garbage collector safe when several servers run it at the same time and shall run it on one server at a time. | Every node starts its own collector (`RelationalGarbageCollector.java:69-73`, `RelationalEntityStore.initialize`). No lease or leader election found (inferred); the change-log cleaner only randomises its start (`R/EntityChangeLogCleaner.java` doc at `initialDelayMs`). | OP | A |
| BS-R8 | GAP | mudcat shall not delete a published version because of a version-count limit. | `versionRetentionCount` max 10 prunes old `fileset`, `policy`, `function`, `semantic_model` versions (`Configs.java:82-88`; `JDBCBackend.deleteOldVersionData` at `JDBCBackend.java:616-663`). Table, model and view have no history of the root, so a table schema from last month is gone (`TableMetaService.java:206`: old version row is soft-deleted at each update). Conflicts with ID-3 and ID-8. | ID | T |
| BS-R9 | IMPL | mudcat shall soft-delete dependent rows in the same transaction as the entity that they depend on. | `SchemaMetaService.java:330-430` (cascade branch) soft-deletes about 25 kinds of dependent rows (owners, grants, tag links, models, views, statistics, semantic-model rows) in one `doMultipleWithCommit`; `TableMetaService.deleteTable` (`:359-366`) deletes the root first under CAS and the dependents in the same transaction. | UP | T |
| BS-R10 | GAP | mudcat shall not leave a relation row that refers to a deleted entity. | Relations refer to objects by `(metadata_object_id, type)` with no foreign key, so Gravitino sweeps orphans in the garbage collector (`R/mapper/OrphanedMetadataObjectRelationSQLProvider.java:34-87`, `RelationalGarbageCollector.java:85-101`). Between delete and sweep a recreated ID or a lookup sees stale grants; 2.0 shipped four `UPDATE ... NOT EXISTS` data repairs (`UPG:398-` four statements). | UP | T |
| BS-R11 | GAP | mudcat shall not let a new entity inherit ownership, tags or grants recorded for a retired entity with the same ID. | Same cause as BS-R10; `UPG:398-399` "Clear governance attached to retired external registrations before their IDs can be reused" and `MetadataObjectSQL.java:25-50` (`excludeOtherActive*` predicates stop a historical row from deleting another registration's metadata). | AC | T |

### 2.2 Concurrency, versions, transactions

| Ref | Kind | Requirement | Rationale / Gravitino evidence | Area | Ver |
|---|---|---|---|---|---|
| BS-R12 | IMPL | mudcat shall apply each update only if the stored version of the row equals the version that the writer read. | Every update and delete is `WHERE id = ? AND current_version = ? AND deleted_at = 0` (`TableMetaBaseSQLProvider.java:299-312`); zero rows -> `OccWriteSupport.updateWithVersion` (`R/service/OccWriteSupport.java`). | ID | T |
| BS-R13 | IMPL | mudcat shall tell a failed version check caused by a concurrent change apart from one caused by a missing or renamed entity. | `OccWriteSupport.writeFailure` re-reads under lock and throws `NoSuchEntityException` or `OptimisticLockException` (`OccWriteSupport.java:92-110`; `SchemaMetaService.java:680-697`). | IN | T |
| BS-R14 | IMPL | mudcat shall keep the concurrency token of an entity separate from the number of its stored content version. | Until 2.0, `current_version` was both the join key into `*_version_info` and the CAS value, so every alter wrote a snapshot even when nothing changed; `occ_version` was added for fileset and policy (`UPG:357-366`, `PG:197,701`). Relates to ID-17. | ID | T |
| BS-R15 | IMPL | mudcat shall block creation of a child under a parent that a concurrent transaction is deleting. | Child write takes `FOR SHARE` on the parent row, parent delete takes `FOR UPDATE`; delete runs first and then checks emptiness (`SchemaMetaService.java:434-440, 564-575`). Needed because the in-JVM tree lock does not cover other servers (inferred). | ST | T |
| BS-R16 | IMPL | mudcat shall take row locks in one fixed hierarchy order: tenant, parent collections from the root down, then entities by ascending ID. | `OwnerMetaService.assignOwner` doc `:271-273` ("metalake first ... object rows in stable ID order; principal then fenced"); catalog lock before schema lock to prevent cross-schema move versus cascade-delete deadlock (`SchemaMetaService.java:640-646`). | PF | I |
| BS-R17 | IMPL | mudcat shall report a lock-wait timeout as a conflict that the caller can retry and not as an internal error. | `ExceptionUtils.isLockWaitTimeout` matches MySQL 1205, H2 50200, PG 55P03 (`ExceptionUtils.java:39-45,154-156`) and `RelationalEntityStore` wraps writes in `lockWaitTimeoutAsConflict` (`RelationalEntityStore.java:367,397,495,512,526`). | IN | T |
| BS-R18 | GAP | mudcat shall configure a server-side lock timeout on every connection. | PG reports 55P03 "when lock_timeout is set" (`ExceptionUtils.java:42-45,143`), but Gravitino sets no `lock_timeout` or `innodb_lock_wait_timeout` itself (grep of `R/session/` found none; inferred); the behaviour depends on how the operator configured the database. | OP | T |
| BS-R19 | IMPL | mudcat shall return an entity together with its dependent rows from one statement snapshot. | `design-docs/entity-store-consistent-reads-audit.md:26` invariant; table+columns, role+privileges, user/group+memberships now one SELECT; test `TestConsistentEntityReads`. Before this, reads were two statements and could tear under READ COMMITTED. | QY | T |
| BS-R20 | IMPL | mudcat shall give the updater function the same snapshot that the version check later compares. | Same audit: "This also applies to the old entity supplied to an updater" (`...audit.md:26`); `TableMetaService.updateTable` (`:250-262`) reads once, CAS-writes with `oldTablePO.currentVersion`. | ID | T |
| BS-R21 | GAP | mudcat shall provide an atomic multi-entity write for each kind of entity, and not for statistics only. | `batchPut`/`batchDelete` support only TABLE_STATISTIC (`JDBCBackend.java:677-745`), `executeInTransaction` throws `UnsupportedOperationException` (`RelationalEntityStore.java:534`), batch get of users/roles loops one by one (`JDBCBackend.java:405`, TODO), batch get silently omits missing names. Relates to ID-7. | IN | T |
| BS-R22 | IMPL | mudcat shall split each statement that carries a list of IDs into chunks that stay below the bind-parameter limit of the database. | `CHILD_DELETE_BATCH_SIZE = 500` because "a long OR chain is slow to plan on MySQL and exceeds the 65,535 bind parameter limit of PostgreSQL beyond 32,767 children" (`OccWriteSupport.java:42-48`); inserts chunked at 1000 (`TableColumnMetaService.java:51`). | PF | T |
| BS-R23 | GAP | mudcat shall make a retried write that already committed return the original result without a second change. | Store writes have no idempotency key; only the Iceberg REST layer has one (`iceberg_idempotency_meta`, `PG:1437-1447`). A lost response to `put` or `update` gives `EntityAlreadyExists` or a version conflict (inferred). Relates to ID-16. | ID | T |
| BS-R24 | IMPL | mudcat shall record, for each idempotency key, the request that first used it and shall reject a different request that reuses the key. | `iceberg_idempotency_meta` has `http_method`, `request_uri`, `generation`, `http_status NULL while reserved`; interface doc: handler "must reject a different binding with 409" (`.../idempotency/IdempotencyStore.java:65-67`). | IN | T |
| BS-R25 | IMPL | mudcat shall give each idempotency reservation a fencing token and shall ignore a finalise or release call that carries a stale token. | `.../idempotency/IdempotencyStore.java:34-43` ("a caller that has lost its reservation cannot finalize or release a record that now belongs to someone else"); token from striped counter table (`JdbcIdempotencyStore.java:237-240`). | IN | T |

### 2.3 Relations (many-to-many, single-valued, polymorphic)

| Ref | Kind | Requirement | Rationale / Gravitino evidence | Area | Ver |
|---|---|---|---|---|---|
| BS-R26 | IMPL | mudcat shall hold each many-to-many link as one row of a link table with a uniqueness constraint over the two endpoints among live rows. | `user_role_rel UNIQUE (user_id, role_id, deleted_at)`, `group_role_rel`, `policy_tag_relation_meta`, `tag_relation_meta` (`PG:367-381, 417-430, 534-551`). Adding an existing link is an upsert (`UserRoleRelBaseSQLProvider.java:41-60`, `ON CONFLICT ... DO NOTHING` in `PolicyTagRelPostgreSQLProvider.java:39`). | ST | T |
| BS-R27 | GAP | mudcat shall index each link table on both endpoints from its first release. | Role deletion scans `user_role_rel` and `group_role_rel` by `role_id`; the index was added only in 2.0 ("Role deletion soft-deletes relation rows by role_id", `UPG:369-371`). | PF | I |
| BS-R28 | GAP | mudcat shall enforce by constraint that a single-valued relation has at most one live row per subject. | `owner_meta` is unique on `(owner_id, object_id, object_type, deleted_at)` (`PG:576`), so two owners per object are allowed; concurrent assignments created duplicates, the 2.0 upgrade merges them (`UPG:344-355`), and reads use `ORDER BY updated_at DESC, id DESC LIMIT 1` (`OwnerMetaBaseSQLProvider.java:447`). Fix in code: serialised through row locks (`OwnerMetaService.java:271-293`). | ST | T |
| BS-R29 | IMPL | mudcat shall resolve a reference to many entity IDs of mixed type to qualified names in one call per entity type and shall return no name for an ID whose parent is deleted. | `MetadataObjectService.TYPE_TO_FULLNAME_FUNCTION_MAP` (16 types) and `getTableObjectsFullName` returns `null` name when catalog/schema missing (`R/service/MetadataObjectService.java:84-110,430-449`). | QY | T |
| BS-R30 | IMPL | mudcat shall list the links of one entity in both directions by one call that takes the link type. | `SupportsRelationOperations.Type` (7 kinds) and `listEntitiesByRelation(RelationQuery)`; "the start entity is determined by identType" (`CORE/SupportsRelationOperations.java:33-47,91`). | QY | T |
| BS-R31 | GAP | mudcat shall keep the privileges of a grant in rows with a typed value per privilege and not in one JSON string column. | `role_meta_securable_object.privilege_names VARCHAR(81920)` JSON list + parallel `privilege_conditions` list, length-checked at read (`POConverters.java:1349-1357`); the column was widened from 256 to 81920 in 0.8.0 (`S/postgresql/upgrade-0.7.0-to-0.8.0-postgresql.sql:19-20`) after real roles overflowed. Cannot query "who has privilege X". | AC | I |

### 2.4 Names, IDs, limits, collation

| Ref | Kind | Requirement | Rationale / Gravitino evidence | Area | Ver |
|---|---|---|---|---|---|
| BS-R32 | GAP | mudcat shall mint IDs with at least 122 random bits or as UUIDv7 and shall report an ID collision as an internal error, not as a name conflict. | `UUID.randomUUID().getLeastSignificantBits() & MAX_ID` gives 63 bits (`RandomIdGenerator.java:37`); birthday collision at ~3e9 rows. A collision on the primary key is mapped by the same SQLState as a name clash and becomes "The TABLE entity: x already exists" (`PostgreSQLExceptionConverter.java:30-34`, inferred). Cross-checks ID-1. | ID | T |
| BS-R33 | IMPL | mudcat shall reject a value that is longer than its column before it reaches the database, with a message that names the field and the limit. | `EntityFieldLimits` ("These values must match the column definitions in scripts/{h2,mysql,postgresql}/schema-*.sql", `EntityFieldLimits.java:26-33`), enforced in `Field.validate` (`Field.java:159`); DB error is a backstop (`ValueTooLongExceptions.java`). | ST | T |
| BS-R34 | IMPL | mudcat shall count the length of a name in Unicode code points. | `value.codePointCount(0, value.length())` because "MySQL (utf8mb4) and PostgreSQL count a supplementary character such as an emoji as one character" (`EntityFieldLimits.java:71-74`; `NameValidationUtils.java:69`). | ST | T |
| BS-R35 | GAP | mudcat shall derive the validation limit and the column width of a field from one definition. | Three numbers exist for "name": API rule 64 (`NameValidationUtils.java:32`), store limit 128 (`EntityFieldLimits.java:34`, `VARCHAR(128)`), Kafka topic names 128 (`docs/kafka-catalog.md:81-85`); column names 300 (`UPG:49`, added for BigQuery). A schema path is stored in one `schema_name VARCHAR(128)`, separators included. | ST | I |
| BS-R36 | GAP | mudcat shall compare names by code point order with an explicit collation on the column. | MySQL tables declare `COLLATE=utf8mb4_bin` (`MY:32`); the PG and H2 scripts declare none (grep: no `COLLATE` in PG script), so equality and `ORDER BY` depend on how the database was created (inferred). Java re-checks tag values "independently of the database collation" (`R/service/TagMetaService.java:316-318`). | ST | T |
| BS-R37 | GAP | mudcat shall store each name in Unicode normalisation form NFC and compare it in that form. | No `java.text.Normalizer` in core, api, common or server modules (grep, inferred); `utf8mb4_bin` and byte-wise PG equality treat "é" (U+00E9) and "e"+U+0301 as different names. | ST | T |
| BS-R38 | IMPL | mudcat shall keep the name of an entity out of every reference between entities and shall reference parents by ID. | `table_meta.schema_id`, `catalog_id`, `metalake_id`; renaming a table changes one row. Hierarchical schema names break this: the full path is the name (`HierarchicalSchemaUtil.java:45`, prefix scan `LIKE ... ESCAPE '!'` at `SchemaMetaBaseSQLProvider.java:135`), so a rename of a nested ancestor must rewrite every descendant (inferred; no rename path found). | ST | A |
| BS-R39 | IMPL | mudcat shall escape pattern metacharacters in a name-prefix query and shall state the escape character in the statement. | `escapeLikeMetacharacters` with `ESCAPE '!'` "to keep the prefix match literal across MySQL, H2, and PostgreSQL" (`SchemaMetaBaseSQLProvider.java:132-135`). | QY | T |
| BS-R40 | GAP | mudcat shall bound the size of each JSON or text value stored in a row and shall reject a larger value with a named limit. | Column sizes differ by backend: MySQL `MEDIUMTEXT` (16 MiB), PG `TEXT` (1 GiB), H2 `CLOB` (`MY:24`, `PG` `properties TEXT`, `H2:24`); `TEXT(81920)` in MySQL becomes MEDIUMTEXT. No size check on `properties` found (inferred). | PF | T |
| BS-R41 | GAP | mudcat shall store creator, creation time, modifier and modification time of an entity in typed, indexable columns. | `audit_info TEXT` JSON blob on every root; when the change feed needed it, 1.3.0 added `updated_at` columns to `user_meta`, `role_meta`, `owner_meta`, `group_meta` and the index `(deleted_at, updated_at, metadata_object_id)` (`S/postgresql/upgrade-1.2.0-to-1.3.0-postgresql.sql:20-27`). | EV | I |

### 2.5 Hierarchical name -> ID resolution and caching

| Ref | Kind | Requirement | Rationale / Gravitino evidence | Area | Ver |
|---|---|---|---|---|---|
| BS-R42 | GAP | mudcat shall resolve a qualified name of any depth to its ID with one database round trip. | Gravitino resolves level by level: metalake id, catalog id, schema id, then leaf (`R/RelationalEntityStoreIdResolver.java:184-215`) or via a 4-way join per query (`SchemaMetaBaseSQLProvider.java:71-100`; audit says "repeats the metalake/catalog/schema/table/version root joins", `...audit.md:55`). Cost grows with depth; no throughput benchmark exists (`audit.md:55`). | PF | T |
| BS-R43 | IMPL | mudcat shall use a name->ID cache only if every ancestor of the name is also read from the cache in the same lookup. | `CachedEntityIdResolver` "All-or-nothing: never stitch a cached leaf id with parents from another resolution" (`CORE/cache/CachedEntityIdResolver.java:59-62`). | PF | T |
| BS-R44 | IMPL | mudcat shall write a change record, in the same transaction as the change, for each change that alters a name resolution. | `entity_change_log(metalake_name, entity_type, entity_full_name, operate_type, created_at)`, operate codes 1 ALTER, 2 DROP, 3 INSERT "stable and never re-used" (`PG:1176-1185`); ALTER stores the old name. | EV | T |
| BS-R45 | GAP | mudcat shall deliver change records to a consumer in commit order with no gap and no record lost to pruning. | Gravitino cannot: "IDs are unique but allocation order is not commit order" (`EntityChangeLogBaseSQLProvider.java:42-43`), so a 60 s replay window with dedup is used and docs concede "bounded compensation, not a lossless delivery protocol" (`docs/gravitino-server-config.md:410-430`). PG `CURRENT_TIMESTAMP` is the transaction start, so long transactions eat the window (same lines). Relates to EV area. | EV | T |
| BS-R46 | IMPL | mudcat shall rebuild a derived cache from the database when a node cannot apply an invalidation. | Listener failure -> "the whole cache is cleared instead, which is strictly stronger" (`R/EntityCacheChangeLogListener.java:51-58`); poller never retries a listener (`EntityChangeLogPoller.java:53`). Cache is derived state. | OP | T |
| BS-R47 | IMPL | mudcat shall refuse a retention of the change record that is shorter than a configured multiple of the poll interval. | `MIN_RETENTION_POLL_CYCLES = 10`; startup precondition rejects shorter values (`R/EntityChangeLogCleaner.java:54,76-79`). | OP | T |
| BS-R48 | IMPL | mudcat shall take every persisted timestamp that is compared across servers from the database clock. | `DatabaseTimeSQL` doc: "in a multi-node deployment every server has its own clock" (`DatabaseTimeSQL.java:23-31`); but 30+ providers still inline the MySQL flavour (same lines) and PG gets a separate expression (`:44`). | OP | I |
| BS-R49 | GAP | mudcat shall list entities in pages that use a key-set cursor over an ordering key that is stable under concurrent inserts. | Gravitino uses `ORDER BY user_id LIMIT ? OFFSET ?` (`UserMetaPostgreSQLProvider.java:114-140`); IDs are random so order means nothing, and concurrent writes shift OFFSET pages (inferred). | IN | T |
| BS-R50 | IMPL | mudcat shall reject a bulk request above a configured item count and shall report one result per item. | `BulkManager.checkBulkSize` and `BulkItemResult` (index, name, value or error), default 100 (`CORE/bulk/BulkManager.java:44-52`, `Configs.java:108`). | IN | T |
| BS-R51 | IMPL | mudcat shall stamp the ID of an entity into the metadata of an external object and shall treat a different stamped ID found at registration as a different object. | `StringIdentifier` (`gravitino.identifier`, format `gravitino.v1.uid<id>`; `CORE/StringIdentifier.java:36-53`); import probe compares `StringIdentifier.fromString(sourceId).id() == stored.id()` (`CORE/catalog/TableOperationDispatcher.java:608-618`); when the source cannot store it, identity is name-based (`:842-853`). | PL | T |
| BS-R52 | IMPL | mudcat shall refuse to adopt an ID that is active under another parent, even with an overwrite flag. | `rejectForeignSchemaIdentity`, "preserve its children and governance metadata until the caller explicitly reconciles" (`SchemaMetaService.java:522-560`); `RelationalBackend.insert` javadoc "even when overwritten is true". Relates to ID-6. | ID | T |

### 2.6 Operation, upgrade, dialects

| Ref | Kind | Requirement | Rationale / Gravitino evidence | Area | Ver |
|---|---|---|---|---|---|
| BS-R53 | GAP | mudcat shall support exactly one SQL dialect, PostgreSQL, in the catalog store. | Three dialects need 3 provider trees (about 38 PG and 4 H2 overrides), per-dialect upsert, `FOR SHARE`, `DELETE LIMIT`, JSON aggregate (`JSON_AGG` vs `JSON_ARRAYAGG`, `UserMetaPostgreSQLProvider.java:93`, `UserMetaH2Provider.java:43`), time expression, exception codes; and H2 "makes no consistency or durability guarantee" (`docs/gravitino-server-config.md:321`). | OP | I |
| BS-R54 | GAP | mudcat shall run its store tests against the same PostgreSQL major versions that it supports and not against an emulation. | H2 in `MODE=MYSQL` needs its own providers: no shared row lock (`UserMetaH2Provider.java:32`), `CAST(... AS CLOB)` in `ModelVersionMetaH2Provider.java:34-48`; docs list "PostgreSQL 12 to 16" as tested (`docs/how-to-use-relational-backend-storage.md:61`). | OP | T |
| BS-R55 | GAP | mudcat shall store the schema version in the database and shall refuse to start when it differs from the version the server needs. | `JDBCBackend.initialize` (`JDBCBackend.java:134-137`) does not check; `CURRENT_SCRIPT_VERSION` only picks the H2 file (`H2Database.java:58-66`); `metalake_meta.schema_version` holds a JSON entity-format enum, not the DDL version. Docs: operator must run the right scripts by hand. | OP | T |
| BS-R56 | IMPL | mudcat shall make each upgrade step safe to run again after a partial failure. | MySQL scripts guard each DDL with `information_schema` checks (DDL is not transactional); docs: "scripts can be rerun after fixing the cause of a partial failure" (`docs/how-to-upgrade.md:160-167`); `TestSQLScripts` replays the upgrade twice (`TestSQLScripts.java:44-84`). | OP | T |
| BS-R57 | GAP | mudcat shall produce, from every earlier release, a schema equal to the schema of a new installation, with equal constraint names and defaults. | Drift: `fileset_version_info.storage_location_name` is `DEFAULT 'default'` in a new schema (`PG:229`) but the upgrade adds `'unknown'` and drops the default (`S/postgresql/upgrade-0.8.0-to-0.9.0-postgresql.sql:20-25`); constraint is `uk_fid_ver_del` in H2 and `uk_fid_ver_sto_del` in MySQL (`H2:146`, `MY:138`). Docs ask operators to diff dumps by hand (`how-to-upgrade.md:97-120`). | OP | T |
| BS-R58 | GAP | mudcat shall allow a server of version N and a server of version N+1 to share one database during an upgrade. | Docs Step 1: "Shut down the Gravitino instance and restrict access to the ... database" (`docs/how-to-upgrade.md:22-27`); 2.0 renames keys, drops constraints, rewrites data (`UPG:21-34, 344-355, 398-`). Helm chart does not migrate (`how-to-upgrade.md` Helm note). Goal; feasibility TBR. | OP | D |
| BS-R59 | IMPL | mudcat shall not run a stricter validation on stored data than was in force when the data was written. | `fromPolicyPO` "Skip PolicyContent.validate() so stricter checks added after write do not break load, rename, comment, or enable of already-stored policies" (`POConverters.java:1586-1587`). | ST | T |
| BS-R60 | IMPL | mudcat shall not copy the text of a database error into an API response. | `ValueTooLongExceptions`: "not attached as the cause, since the error response serializes the whole stack trace and would expose the database error message" (`ValueTooLongExceptions.java:36-37`). Only this one path is protected; the default branches throw `IOException(sqlException)` (`PostgreSQLExceptionConverter.java:38`) (inferred exposure). | AC | T |

## 3. Quirks and gotchas ledger

- **Tombstone timestamps collide on the unique key**
  - What happens: Every name is unique over `(parent, name, deleted_at)`. Deleting and recreating a name, then deleting again within one millisecond (or twice in one PG transaction) violates the key. Only `domain_rel` is protected.
  - Cause category: STORE (PG `CURRENT_TIMESTAMP` is transaction start; MySQL/H2 ms resolution) / MODEL.
  - Evidence: `R/mapper/provider/base/DomainRelBaseSQLProvider.java:143` "PostgreSQL timestamps stay constant within a transaction, and millisecond clocks can collide on every backend"; `DatabaseTimeSQL.java:40,44`.
  - Mudcat implication: BS-R1.

- **Primary key of four root tables includes `deleted_at`, the others do not**
  - What happens: `schema_meta`, `table_meta`, `topic_meta`, `view_meta` have `PRIMARY KEY (id, deleted_at)`; the rest `PRIMARY KEY (id)`. Reason: the same external ID can be registered, retired and registered again; a plain primary key refused the second registration.
  - Cause category: MODEL (external IDs reused) / COMPAT.
  - Evidence: `UPG:21` "Preserve retired registrations while keeping each external ID unique among active rows"; `PG:95,124`; `core/src/test/.../TestSQLScripts.java:36-60`.
  - Mudcat implication: BS-R2, BS-R52.

- **PostgreSQL `ON CONFLICT` has one target, MySQL `ON DUPLICATE KEY` any unique key**
  - What happens: Overwrite semantics differ. Table upserts conflict on `(table_id, deleted_at)`; topic upserts conflict on `(schema_id, topic_name, deleted_at)` "so PostgreSQL preserves the stored topic ID, matching MySQL and H2". A same-name different-ID table overwrite needs a separate pre-step that deletes the stale table, with a warning log.
  - Cause category: STORE.
  - Evidence: `R/mapper/provider/postgresql/TableMetaPostgreSQLProvider.java:48`; `TopicMetaPostgreSQLProvider.java:110-113`; `R/service/TableMetaService.java:485-499`.
  - Mudcat implication: BS-R1/BS-R3; choose the natural key (name) as the single arbiter of overwrite.

- **PG `ON CONFLICT DO UPDATE` needs qualified columns**
  - What happens: Version bump is written `table.current_version + 1` because a bare name is ambiguous between stored and rejected row; PG also needs `GREATEST(...)` to stop an inconsistent legacy row moving a version backwards.
  - Cause category: STORE.
  - Evidence: `UserMetaPostgreSQLProvider.java:76-85`; `TableMetaPostgreSQLProvider.java:54-61`; `TopicMetaPostgreSQLProvider.java:121-125`.
  - Mudcat implication: Upsert by DB-derived version means the caller must re-read the stored version (`TableMetaService.java:187-191`); prefer explicit CAS (BS-R12).

- **H2 has no `FOR SHARE`**
  - What happens: The parent-fencing lock that is a shared lock on MySQL/PG becomes an exclusive `FOR UPDATE` on H2, so H2 serialises more than the real backends and hides lock-order bugs.
  - Cause category: STORE.
  - Evidence: `UserMetaH2Provider.java:32-34`.
  - Mudcat implication: BS-R54.

- **`DELETE ... LIMIT` does not exist in PostgreSQL**
  - What happens: GC batch delete is rewritten to `WHERE id IN (SELECT id ... LIMIT n)` for PG; H2's DELETE accepts LIMIT but not ORDER BY, so the PG change-log pruner has an ORDER BY the base does not.
  - Cause category: STORE.
  - Evidence: `UserMetaPostgreSQLProvider.java:153-159`; `R/mapper/provider/base/EntityChangeLogBaseSQLProvider.java:112-117` "H2's DELETE grammar accepts LIMIT but not ORDER BY".
  - Mudcat implication: BS-R6, BS-R53.

- **Bind-parameter ceiling in PostgreSQL**
  - What happens: Soft-deleting children one `(id = ? AND current_version = ?)` term each fails past 32,767 children; Gravitino chunks at 500.
  - Cause category: STORE.
  - Evidence: `R/service/OccWriteSupport.java:42-48`.
  - Mudcat implication: BS-R22.

- **Isolation is READ COMMITTED everywhere and MyBatis caches reads inside a session**
  - What happens: Sessions are opened at `READ_COMMITTED`; MyBatis's first-level cache can hand back a pre-lock read inside one transaction, so authorisation reads call `session.clearCache()` before reading. One service also comments that a read "observes the latest identities on backends using repeatable-read isolation" (MySQL default differs if the operator changes it).
  - Cause category: CONCURRENCY / STORE.
  - Evidence: `R/session/SqlSessions.java:67`; `R/utils/SessionUtils.java:96-101`; `R/service/SemanticModelMetaService.java:515`.
  - Mudcat implication: BS-R19, BS-R20; state the isolation level in the design and test at it.

- **Deadlock policy differs between write paths**
  - What happens: Ordinary writes treat a deadlock as a bug ("points at an inconsistent lock order, which should be fixed"); batch statistics inserts lock rows in plan order, so deadlocks are expected there and are reported as a conflict (PG 40P01/40001, MySQL 1213, H2 40001).
  - Cause category: CONCURRENCY / STORE.
  - Evidence: `R/utils/ExceptionUtils.java:147-148`; `R/service/StatisticMetaService.java:358-370`.
  - Mudcat implication: BS-R16, BS-R17.

- **Lock-wait timeouts are classified by vendor code, not SQLState**
  - What happens: MySQL reports a lock timeout as SQLState 40001 or HY000 depending on driver version, and 40001 is also deadlock; so the matcher uses vendor codes (MySQL 1205, H2 50200) and SQLState only for PG (55P03).
  - Cause category: STORE.
  - Evidence: `ExceptionUtils.java:141-146` javadoc and `:39-45`.
  - Mudcat implication: BS-R17.

- **A lock timeout can only be detected if the operator set one**
  - What happens: PG raises 55P03 only "when lock_timeout is set"; the server never sets it (inferred), so waiters otherwise block until the connection pool gives up (`maxWaitMillis` default 1000 ms applies to pool borrow, not to row locks).
  - Cause category: STORE / OTHER.
  - Evidence: `ExceptionUtils.java:143`; `SqlSessionFactoryHelper.java:93-94`.
  - Mudcat implication: BS-R18.

- **Auto-increment ID order is not commit order**
  - What happens: A change-log row with a lower ID can commit after the poller moved past it. Gravitino re-reads a recent time window and de-duplicates by ID; PG timestamps are transaction start, so long transactions fall outside the window; recovery needs up to one sweep (15 s) and is lossy beyond 60 s.
  - Cause category: CONCURRENCY / STORE.
  - Evidence: `EntityChangeLogBaseSQLProvider.java:42-43`; `EntityChangeLogPoller.java:76-80`; `docs/gravitino-server-config.md:410-430`.
  - Mudcat implication: BS-R45. For the mudcat change stream do not poll on a serial ID.

- **The change-log retention must exceed several poll cycles**
  - What happens: A shorter retention silently loses invalidations, so the server refuses to start with `retentionMs / 10 < pollIntervalMs`. Owner tombstones are also kept as change signals and their cut-off is kept "comfortably behind the present time".
  - Cause category: CONCURRENCY.
  - Evidence: `EntityChangeLogCleaner.java:38-40,76-79`; `OwnerMetaBaseSQLProvider.java:431-435`.
  - Mudcat implication: BS-R47; a tombstone used as an event is a second meaning for `deleted_at` (BS-R4 vs BS-R5).

- **Cache coherence is per node and name-keyed**
  - What happens: The entity cache is keyed by qualified name and per node; each write registers a "pending write" fence on the exact key and its prefixes, bumps an invalidation epoch, and a read publishes to the cache only if the epoch did not move. Rename of a container invalidates every cached descendant by prefix scan; other nodes learn within ~3 s (poll) or after the TTL (1 h) if a record is lost.
  - Cause category: CONCURRENCY / MODEL.
  - Evidence: `R/RelationalEntityStore.java:92-100,336-368,475-483`; `R/EntityCacheChangeLogListener.java:36-42`; `docs/gravitino-server-config.md:360-366`.
  - Mudcat implication: BS-R43, BS-R44. Cache keyed by ID with name->ID as a thin, separately invalidated map is the simpler design (design suggestion).

- **Overwrite does not necessarily keep the entity that the caller built**
  - What happens: After `put(e, true)` the cache is not filled with `e` because "the database may keep the identity and version of the row it already had".
  - Cause category: MODEL.
  - Evidence: `RelationalEntityStore.java:339-341`; `TableMetaService.java:187-191`.
  - Mudcat implication: BS-R12; return the stored row from every write.

- **Owner: the schema allowed two live owners**
  - What happens: `UNIQUE (owner_id, object_id, object_type, deleted_at)` does not bound owners per object, so concurrent `setOwner` calls produced duplicate live owners. The 2.0 upgrade soft-deletes all but the newest, and reads pick the newest by `(updated_at, id)`.
  - Cause category: CONCURRENCY / MODEL.
  - Evidence: `PG:576`; `UPG:344-355`; `OwnerMetaBaseSQLProvider.java:447`.
  - Mudcat implication: BS-R28.

- **Relations are polymorphic with no foreign key**
  - What happens: `(metadata_object_id, metadata_object_type)` in `owner_meta`, `tag_relation_meta`, `role_meta_securable_object`, `statistic_meta` cannot carry an FK, so cascades are hand-written per entity type (about 26 `doWithoutCommit` soft-delete calls in `SchemaMetaService` alone) and an orphan sweeper runs in GC. The sweeper has a special case for columns, because a dropped column keeps rows plus a newer DELETE row.
  - Cause category: MODEL.
  - Evidence: `SchemaMetaService.java:330-475`; `R/mapper/OrphanedMetadataObjectRelationSQLProvider.java:34-87,89-100`; `R/RelationalGarbageCollector.java:85-101`.
  - Mudcat implication: BS-R10, BS-R11. Typed link tables per target kind or a trigger would let PostgreSQL enforce this.

- **Table columns are versioned as deltas, other types as snapshots**
  - What happens: `table_column_version_info` stores per-version create/update/delete rows; the read joins "the maximum version no greater than the root's current version" per column and must keep DELETE markers in the selection until after the max is chosen. View, function and semantic-model versions are complete JSON snapshots. Old column rows are not pruned: `deleteOldVersionData` returns 0 for COLUMN ("not implemented multi-versions").
  - Cause category: MODEL.
  - Evidence: `design-docs/entity-store-consistent-reads-audit.md:51`; `JDBCBackend.java:623-637` (case list returns 0); `PG:view_version_info.columns TEXT` at line 981.
  - Mudcat implication: BS-R8; one version representation per kind (section 4).

- **Version retention is hard-bounded and prunes history**
  - What happens: `versionRetentionCount` default 1, max 10; the collector soft-deletes older fileset, policy, function and semantic-model versions after the rows are older than the deletion period.
  - Cause category: MODEL.
  - Evidence: `Configs.java:82-88`; `RelationalGarbageCollector.java:116-135`; `JDBCBackend.java:616-663`.
  - Mudcat implication: BS-R8.

- **`current_version` doubled as OCC token and history pointer**
  - What happens: Every fileset or policy alter had to bump `current_version` and write a new snapshot row even when nothing in the snapshot changed; `occ_version` split the roles in 2.0, with `DEFAULT 1` as the entire backfill.
  - Cause category: MODEL / COMPAT.
  - Evidence: `UPG:357-366`; `PG:197,701`.
  - Mudcat implication: BS-R14.

- **Name limit differs by layer**
  - What happens: Catalog and domain names are checked at 64 characters with pattern `\w[\w-]*`; entity fields at 128 (code points); columns at 300; a nested schema's whole path must fit 128 with separators; MySQL non-binary collations would compare differently. Limits in Java must be edited by hand when the SQL changes.
  - Cause category: MODEL / STORE.
  - Evidence: `NameValidationUtils.java:32-33`; `EntityFieldLimits.java:26-48`; `UPG:49`; `HierarchicalSchemaUtil.java:45`.
  - Mudcat implication: BS-R33 to BS-R36.

- **Hierarchical schema names use an ASCII-1 separator in one column**
  - What happens: Logical `A:B:C` is stored as `A\u0001B\u0001C` in a single `schema_name`; the external separator is configurable but never stored; descendants are found with `LIKE prefix% ESCAPE '!'`. Nested create may have to create missing ancestors, so it takes an exclusive catalog lock and serialises all schema creates under that catalog.
  - Cause category: MODEL.
  - Evidence: `HierarchicalSchemaUtil.java:25-45`; `SchemaMetaBaseSQLProvider.java:130-136`; `SchemaMetaService.java:564-575`.
  - Mudcat implication: BS-R38; use a parent pointer (adjacency list) or a materialised path of IDs, not a name path.

- **Model version "number or alias" is decided with `NumberUtils.isCreatable`**
  - What happens: The same path segment is a version number or an alias; the rule that an alias "cannot be a number or a number string" is only in API javadoc and the store switches on `isCreatable`, which also accepts hex, exponent and sign forms (`0x1F`, `1e3`) (inferred from the library's behaviour).
  - Cause category: MODEL.
  - Evidence: `R/service/ModelVersionMetaService.java:91,158,253`; `api/src/main/java/org/apache/gravitino/model/ModelCatalog.java:121`.
  - Mudcat implication: Keep version numbers and aliases in different namespaces of the API (separate path or prefix); relates to ST-area ref semantics.

- **Large text types are not equivalent**
  - What happens: `properties`, `audit_info` and every JSON blob are `MEDIUMTEXT` (16 MiB) on MySQL, `TEXT` on PG, `CLOB` on H2; `TEXT(81920)` on MySQL silently becomes `MEDIUMTEXT`; privilege lists were widened to 81920 after real roles overflowed 256.
  - Cause category: STORE.
  - Evidence: `MY:24,197`; `H2:206-207`; `PG:344`; `S/postgresql/upgrade-0.7.0-to-0.8.0-postgresql.sql:19-20`.
  - Mudcat implication: BS-R31, BS-R40.

- **MySQL index prefix on `partition_name`**
  - What happens: MySQL's primary key uses `partition_name(255)` while the column is `VARCHAR(1024)`; PG and H2 index the full value. Two partition names equal in their first 255 characters conflict only on MySQL.
  - Cause category: STORE.
  - Evidence: `MY:644-645` vs `PG:1117-1123`, `H2:653`.
  - Mudcat implication: Single-dialect (BS-R53) removes this class.

- **Partial indexes on PG, composite indexes on MySQL**
  - What happens: To keep tombstones "out of live metadata lists and relationship cleanup scans" PG got `... WHERE deleted_at = 0` indexes in 2.0; MySQL has no partial index, so it gets `(col, deleted_at)` composites. Tombstones are 7 days of every row ever deleted.
  - Cause category: STORE.
  - Evidence: `UPG:597-605`; `MY:174,189,205,302` (idx_*_live with `deleted_at`).
  - Mudcat implication: BS-R1 (partial unique index) and BS-R4 (keep tombstones out of hot indexes).

- **Upgrade scripts: fresh schema and upgraded schema drift**
  - What happens: Fileset version rows get `DEFAULT 'default'` in a new install but `'unknown'` backfill and no default after upgrade; the same unique constraint has different names per dialect; no code compares them, operators diff dumps by hand.
  - Cause category: COMPAT.
  - Evidence: `PG:229` vs `S/postgresql/upgrade-0.8.0-to-0.9.0-postgresql.sql:20-25`; `H2:146` vs `MY:138`; `docs/how-to-upgrade.md:97-120`.
  - Mudcat implication: BS-R57.

- **MySQL cannot roll back DDL, so upgrades are guarded by `information_schema` checks**
  - What happens: The MySQL 1.3-to-2.0 script is 1015 lines against 605 for PG because every step is wrapped in an existence check to be re-runnable.
  - Cause category: STORE.
  - Evidence: `wc -l S/mysql/upgrade-1.3.0-to-2.0.0-mysql.sql` = 1015 vs PG 605; `S/mysql/upgrade-0.8.0-to-0.9.0-mysql.sql:26-52`.
  - Mudcat implication: BS-R56; PostgreSQL has transactional DDL, so one transaction per migration is possible.

- **Schema is not versioned in the database; the server never checks**
  - What happens: The operator must apply `schema-X` or the chain of `upgrade-*` scripts by hand (except H2, which is auto-created); starting a server against the wrong schema fails at the first query, or worse, at the first write of a new column.
  - Cause category: OTHER.
  - Evidence: `JDBCBackend.java:134-137`; `H2Database.java:58-66`; `docs/how-to-use-relational-backend-storage.md:64-67`.
  - Mudcat implication: BS-R55.

- **DB error text can leak**
  - What happens: Value-too-long is converted to `IllegalArgumentException` without the SQL cause "since the error response serializes the whole stack trace"; the generic branch of each converter wraps the `SQLException` in `IOException`.
  - Cause category: SECURITY.
  - Evidence: `ValueTooLongExceptions.java:36-37`; `PostgreSQLExceptionConverter.java:38`.
  - Mudcat implication: BS-R60.

- **Batch operations are mostly unimplemented**
  - What happens: `batchPut` and `batchDelete` exist for statistics only; user and role batch get loop single gets and skip missing names silently; hard delete for some types is a TODO.
  - Cause category: OTHER.
  - Evidence: `JDBCBackend.java:405,601,709,742`.
  - Mudcat implication: BS-R21.

- **Post-delete action cannot be atomic with the default backend method**
  - What happens: The default `deleteAndGet` refuses a post-delete action because it "can only run the action once the delete is committed, which is the opposite of what the contract promises"; only specific services override it.
  - Cause category: CONCURRENCY.
  - Evidence: `R/RelationalBackend.java:278-282`.
  - Mudcat implication: Plan a before-commit hook point in the store transaction, so that credentials, replicas and authorisation tuples can be removed atomically (relates to UP-area requirements).

- **63-bit random IDs are also stored as MySQL `BIGINT UNSIGNED` and PG signed `BIGINT`**
  - What happens: The mask `& 0x7fff...` keeps IDs positive so the two column types hold the same set; sorting by ID is random order, and a primary key collision is indistinguishable from a name clash at the converter.
  - Cause category: STORE.
  - Evidence: `RandomIdGenerator.java:30,37`; `MY:51`, `PG:85`; `PostgreSQLExceptionConverter.java:30-34`.
  - Mudcat implication: BS-R32, BS-R49.

- **Same ID can sit in two schemas' history**
  - What happens: Many cleanup SQLs need `NOT EXISTS (live row with same ID in another catalog/metalake/schema)` so deleting an old registration does not delete the live registration's tags and grants.
  - Cause category: MODEL.
  - Evidence: `R/mapper/provider/MetadataObjectSQL.java:25-50,59-100`.
  - Mudcat implication: BS-R11; an ID that is never reused avoids all of this (ID-1, ID-2).

- **Idempotency record insert and retry run in separate transactions**
  - What happens: A failed `INSERT` on MySQL leaves locks that a later `UPDATE` in the same transaction would upgrade; Gravitino retries in a new transaction and uses a striped generation counter (`stripe = last two hex digits of the key`) for fencing tokens.
  - Cause category: STORE / CONCURRENCY.
  - Evidence: `iceberg/iceberg-rest-server/.../idempotency/JdbcIdempotencyStore.java:149,237-240`; `PG:1461-1464`.
  - Mudcat implication: BS-R24, BS-R25; on PostgreSQL use `INSERT ... ON CONFLICT DO NOTHING RETURNING` in one statement.

## 4. Open questions / inconsistencies / half-implemented

- `JDBCBackend.hardDeleteLegacyData`: `// TODO: Implement hard delete logic for these entity types.` sits after `case AUDIT: return 0` (`JDBCBackend.java:598-601`); which types are meant is unclear, so some tombstones may never be removed (inferred).
- `JDBCBackend.batchGet` for USER and ROLE: `// TODO: Add true batch SQL operations for users, roles, and views` (`JDBCBackend.java:405`).
- `RelationalEntityStore.executeInTransaction` throws `UnsupportedOperationException("Unsupported operation in relational entity store.")` (`RelationalEntityStore.java:534`) although the `EntityStore` interface advertises it; callers use per-service `doMultipleWithCommit` instead.
- Schema-version question: `metalake_meta.schema_version` stores `SchemaVersion.V_0_1` JSON (`CORE/meta/SchemaVersion.java`), while the SQL scripts use release numbers; nothing ties them together.
- `ModelVersionMetaService` passes `overwritten` through but logs "'overwritten' is not supported for model version meta, ignoring this flag" (`JDBCBackend.java:1208`): API flag silently ignored.
- `policy_relation_meta` (pre-2.0 direct policy bindings) is kept "including its existing data" while "the 2.0 server no longer reads direct object-policy assignments from this table" (`UPG:43-44`); a separate offline script `bin/migrate-policy-bindings.sh` converts it (`docs/how-to-upgrade.md:169-175`). A dead table remains in the schema.
- Column delta rows are never pruned and `column_op_type` DELETE rows stay; growth over many alters is unbounded (inferred from `deleteOldVersionData` returning 0 for COLUMN).
- `iceberg_idempotency_meta` and `iceberg_cleanup_job` have no `deleted_at` and no metalake or tenant column (`PG:1194-1210, 1437-1447`): a second tenancy convention inside the same database.
- `partition_statistic_meta` and metrics tables have no `deleted_at`, no tenant, no foreign key to `table_meta` (`PG:1115-1126`); table deletion must remove them explicitly. Not verified that it does.
- `table_version_info` has no primary key (`UNIQUE (table_id, version, deleted_at)` only; `deleted_at BIGINT DEFAULT 0` nullable, `PG:860-872`): a nullable `deleted_at` lets duplicates through because `NULL` is distinct in a unique index (inferred).
- Docs say "MySQL 5.7 or 8.0" is supported (`docs/how-to-use-relational-backend-storage.md:60`) while the base providers use `JSON_ARRAYAGG` (`UserMetaBaseSQLProvider.java:186`), which exists only from MySQL 5.7.22 (version detail from general knowledge, not from the repo).
- `STORE_TRANSACTION_MAX_SKEW_TIME` ("maximum skew time of transactions", default 2000 ms, `Configs.java:324-329`) is defined, but no main-code file other than `Configs.java` reads it; only tests stub it (grep; likely a leftover from a removed KV store, inferred). No KV store code remains: `ENTITY_STORES` has one entry (`EntityStoreFactory.java:36`).
- Pagination: user and group lists page by `OFFSET`; table and schema lists return everything (`JDBCBackend.list` calls `listTablesByNamespace` with no limit; `JDBCBackend.java:197`). Large schemas load all rows.
- The in-JVM `TreeLock` and the DB row locks both exist; I did not find a document stating which one is authoritative on a multi-node deployment (inferred from the DB fences).
- `docs/gravitino-server-config.md:321` says H2 has no consistency or durability guarantee, yet H2 is the default and `RelationalEntityStore` has a full test matrix for it; tests for H2 cannot prove PG/MySQL behaviour (see BS-R54).
