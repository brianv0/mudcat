> **Research note, not a requirement document.** Drafted by a code-reading agent from Apache Gravitino commit `9aa78cf` (2026-11-26), then spot-checked by the lead. Line numbers can drift by a few lines; search by file and symbol. Items marked `(inferred)` were not confirmed in code. The text does not follow Simplified Technical English. See [../README.md](../README.md) for the synthesis.

# Catalog framework and federation (ref prefix CF)

Scope read: `core/.../catalog/**`, `core/.../connector/**`, `core/.../metalake/**`, `core/.../hook`, `core/.../listener`, `core/.../utils/IsolatedClassLoader|ClassLoaderPool`, `catalogs/catalog-common` (classloader cleanup), capability classes of the Hive, Iceberg, JDBC, Glue, Kafka and Generic catalogs, `api/.../Catalog*, NameIdentifier, MetadataObject(s), rel/Column`, `scripts/postgresql/*2.0.0*`, `design-docs/{gravitino-glue-catalog,gravitino-logical-view-management,iceberg-supported-nested-namespace}.md`, `docs/{manage-catalogs-and-schemas,migration-guide,jdbc-catalog-connection-validation,lakehouse-generic-catalog,apache-hive-catalog}.md`, `rfc/**`.
All paths are repo-relative to `/home/user/apache/gravitino`. `(inferred)` marks things I concluded rather than read.

## 1. Decomposition

### 1.1 The federation model in one paragraph
Gravitino registers an external system (a "catalog": Hive Metastore, JDBC database, Iceberg catalog, Glue, Kafka) under a name, and exposes its objects through one API. For **external** catalogs the external system stays the source of truth for structure (schema, columns, partitioning, most properties, comment). Gravitino's relational store keeps a thin **registration** per object: a Gravitino-issued numeric ID, name, namespace, audit info and, for tables, the column list with column IDs (`core/.../catalog/TableOperationDispatcher.java:955-970`, `core/.../meta/TableEntity.java:50-62`). Everything Gravitino-only (owner, privileges, tags, policies, statistics, secrets) hangs off that ID. For **managed** catalogs (fileset, model, lakehouse-generic) Gravitino is the source of truth and the store holds everything (`core/.../catalog/ManagedTableOperations.java`, `api/.../CatalogProvider.java:20-35`). A per-connector `Capability` object says which mode applies per object type (`Capability.managedStorage(scope)`, `core/.../connector/capability/Capability.java:140`).

### 1.2 Layers and the dispatcher chain
Public chain, built in `core/.../GravitinoEnv.java:1105-1200`:

```
REST resource  (authorization filter runs here, before the chain; server/.../web/filter/GravitinoInterceptionService.java,
                server-common/.../authorization/MetadataIdConverter.java:157 re-uses CapabilityHelpers to normalise names)
  -> XxxEventDispatcher      core/.../listener/*EventDispatcher     pre-event / post-event / failure-event
  -> XxxNormalizeDispatcher  core/.../catalog/*NormalizeDispatcher  name validation, case folding, capability checks, physical-name resolution
  -> XxxHookDispatcher       core/.../hook/*HookDispatcher          owner assignment, authorization-plugin (Ranger etc.) grant/rename/remove, rollback
  -> XxxOperationDispatcher  core/.../catalog/*OperationDispatcher  locks, connector call, entity-store write, import, merge
       -> CatalogManager.doWithCatalogWrapper  (lease on cached CatalogWrapper, classloader swap)
            -> BaseCatalog -> CatalogOperations (connector SPI, loaded from isolated classloader) -> external system
```
Comment confirming the order: `GravitinoEnv.java:1115-1127` ("MetalakeEventDispatcher -> MetalakeNormalizeDispatcher -> MetalakeHookDispatcher -> MetalakeManager").
A second, **internal** chain (`internalTableDispatcher` etc., `GravitinoEnv.java:944-1060`) is Normalize -> Operation only: no hook, no event. Used by other server components (e.g. table creation calling `internalSchemaDispatcher` to import the parent schema, `TableOperationDispatcher.java:111-130`).
Catalog and metalake have the same shape but their innermost layer is `CatalogManager` / `MetalakeManager`, not an OperationDispatcher.

| Layer | Class | What it does |
|---|---|---|
| Event | `listener/TableEventDispatcher` | Emits `XxxPreEvent`, `XxxEvent`, `XxxFailureEvent` per operation; outermost, so it sees the caller's raw names. |
| Normalize | `catalog/CapabilityHelpers`, `*NormalizeDispatcher` | `withCapability` leases the catalog and reads its `Capability`; applies name spec, case rules, NOT NULL/DEFAULT checks, to columns, partitions, distributions, sort orders, indexes and alter changes (`CapabilityHelpers.java:132-175,723-800`). Loads/lists apply only case folding "for compatibility reasons" (`TableNormalizeDispatcher.java:66-75`). |
| Hook | `hook/TableHookDispatcher` | After create: sets creator as owner. Before/after rename and drop: updates the external authorization plugin (`TableHookDispatcher.java:100-185`). Catalog create: owner + "future grants", rolled back by force-drop on failure (`CatalogHookDispatcher.java:112-140`). |
| Operation | `catalog/OperationDispatcher` + subclasses | Tree locks, connector call, `StringIdentifier` handling, import-or-merge with the store, snapshot detachment, secret staging. |
| Manager | `catalog/CatalogManager` (2388 lines) | Catalog CRUD, cache of `CatalogWrapper`, leases, classloader pool, enable/disable, test connection, force drop. |

### 1.3 Key classes
- `core/.../catalog/CatalogManager.java`: lifecycle, cache (Caffeine, `expireAfterAccess` default 1 h, `Configs.java:200-205`), `CatalogWrapper` lease/retire (`:188-300`), `ClassLoaderPool` use (`:516`), `buildClassLoaderKey` (`:2065`), `lifecycleLock` (`:537`), change-log listener for multi-node cache invalidation (`:586-595`, `CatalogChangeLogListener.java`).
- `core/.../connector/BaseCatalog.java`: the SPI base class every provider extends. Abstract `newOps(config)`; optional `newCapability()`, `newProxyPlugin()`, `validateConfiguration(Map)`, `shouldValidateOnCreate()`, `validateOnCreate()`, `supportsPathBasedCredentials()`. Lazy double-checked `ops()`, `capability()`, `properties()`.
- `core/.../connector/CatalogOperations.java`: `initialize(config, info, propertiesMetadata)`, `testConnection(...)` (default throws `UnsupportedOperationException`, `:52-67`). Mixins: `SupportsSchemas`, `TableCatalog`, `ViewCatalog`, `FilesetCatalog`, `TopicCatalog`, `ModelCatalog`, `SupportsTableIdentifier` (`api/.../rel/SupportsTableIdentifier.java`), `SupportsTableNameResolution`, `CatalogDropAware`, `SupportsPathBasedCredentials`.
- `api/.../CatalogProvider.java`: SPI, `shortName()`; discovered by `ServiceLoader` inside the connector's classloader (`CatalogManager.java:2225-2241`, fails on 0 or >1 match).
- `core/.../connector/capability/Capability.java` (+ `CapabilityResult`): `columnNotNull`, `columnDefaultValue`, `caseSensitiveOnName(scope)`, `normalizeName(scope,name)`, `specificationOnName`, `specificationOnExistingName`, `managedStorage(scope)`, `supportsHierarchicalSchema`. Scopes: SCHEMA, TABLE, VIEW, COLUMN, FILESET, TOPIC, PARTITION, MODEL, FUNCTION, SEMANTIC_MODEL. There is **no** flag for auto-increment or for unique-name-in-catalog.
- `core/.../connector/PropertiesMetadata.java`, `PropertyEntry.java`, `BasePropertiesMetadata.java`, `BaseCatalogPropertiesMetadata.java`, `catalog/PropertiesMetadataHelpers.java`, `connector/HiddenPropertyMaskUtils.java`: per-entity-type property declarations (`required, immutable, hidden, reserved, prefix, decoder/encoder, default`) and validation/masking.
- `core/.../StringIdentifier.java`: the ID-in-external-metadata codec: property key `gravitino.identifier`, value `gravitino.v1.uid<long>`; or a trailing comment `(From Gravitino, DO NOT EDIT: gravitino.v1.uid<long>)`; `DUMMY_ID` = -1 for test connection (`:38-47`).
- `core/.../utils/IsolatedClassLoader.java`, `ClassLoaderPool.java`, `ClassLoaderKey.java`, `catalogs/catalog-common/.../ClassLoaderResourceCleanerUtils.java`.
- `core/.../metalake/MetalakeManager.java`: top-level container (the nearest thing to a tenant). Same in-use/force-drop discipline as catalogs.
- `core/.../catalog/EntityCombined{Table,Schema,...}.java`: read-through merge of "what the connector said" and "what the store says", with an `imported` flag.
- `api/.../NameIdentifier.java`, `Namespace.java`, `MetadataObject(s).java`: dot-joined hierarchical names; fixed hierarchy metalake.catalog.schema.object; `*` is reserved.

### 1.4 Config keys that matter
`gravitino.catalog.cache.evictionIntervalMs` (`Configs.CATALOG_CACHE_EVICTION_INTERVAL_MS`, 1 h), `CATALOG_LOAD_ISOLATED` (default true, off only for tests, `Configs.java:280-285`), `CATALOG_CLASSLOADER_SHARING_ENABLED` (`:287`), `TREE_LOCK_*` (`:380-399`), `gravitino.catalog.credential.backfillToProperties` (legacy plaintext escape hatch, `:719`). Per-catalog: `package`, `ops-impl`, `authorization-provider`, `in-use`, `metalake-in-use`, `cloud.name`, `cloud.region-code` (`BaseCatalogPropertiesMetadata.java:57-105`), `gravitino.bypass.*` prefix (passed to the connector with the prefix stripped, `BaseCatalog.java:114`), `verify-on-create` (JDBC), `lance.schema-refresh-mode`, `lance.allow-external-table-purge`. Provider-specific `<provider>.conf` in the catalog's `conf/` directory is merged under the request properties at create time (`CatalogManager.java:1767-1775`).

## 2. Candidate requirements

Areas: ID, ST, PL, IN, OP, AC, UP, PF per the brief. Ver = T/A/I/D.

### 2.1 Registration, identity and source of truth

| Ref | Kind | Requirement | Rationale / Gravitino evidence | Area | Ver |
|---|---|---|---|---|---|
| CF-R1 | IMPL | mudcat shall register an external catalog by recording its connection settings, without copying or moving the catalog's data. | `CatalogManager.createCatalog` persists only a `CatalogEntity` (`CatalogManager.java:833-960`); generic catalog docs "Register existing lakehouse tables without data movement" (`docs/lakehouse-generic-catalog.md:28`). | PL | D |
| CF-R2 | IMPL | mudcat shall record, for each metadata field of a registered object, whether mudcat or the external system holds the authoritative value. | Gravitino merges both views in `EntityCombinedTable`; for external tables the store holds only id, name, columns, audit (`TableOperationDispatcher.java:955-970`), all else is read through. Without the split, stale copies and lost updates follow (Q1 in section 4). | ST | I |
| CF-R3 | IMPL | mudcat shall give every registered external object a mudcat-issued identifier that does not depend on the object's name. | Entity IDs from `idGenerator.nextId()` (`TableOperationDispatcher.java:770,896`); privileges, tags, owners key on the ID (`EntityCombinedTable.java:57-58`). | ID | T |
| CF-R4 | GAP | mudcat shall bind a registration to a durable identifier read from the external object, so that an external rename does not create a second identity. | Gravitino falls back to name identity when the source cannot store its ID: "For some catalogs like PG, the identifier information is not stored in the table's metadata" (`TableOperationDispatcher.java:858-861`); MySQL schemas cannot carry a comment (`docs/jdbc-mysql-catalog.md:101`). Rename outside Gravitino then orphans the registration. | ID | T |
| CF-R5 | GAP | mudcat shall register and read an external object without writing to the external system. | Gravitino writes its ID into the source's properties or comments on create (`JdbcCatalogOperations.java:457-468,680-706`) and depends on it for rename/import recovery. Read-only sources get name identity only (CF-R4). | PL | T |
| CF-R6 | IMPL | mudcat shall allow only one active registration per external object. | Import is create-only (`store.put(entity,false)`, `TableOperationDispatcher.java:794-797`); PK `(table_id, deleted_at)` plus `UNIQUE(schema_id, table_name, deleted_at)` (`scripts/postgresql/schema-2.0.0-postgresql.sql:124-125`). | ID | T |
| CF-R7 | IMPL | mudcat shall allow an external object to be registered again after its registration was deleted, keeping the identifier the object carries. | 1.3->2.0 upgrade changed primary keys to `(id, deleted_at)` so "deleted registrations and a new active registration ... retain the same external identifier" (`docs/migration-guide.md:16-20`; `scripts/postgresql/upgrade-1.3.0-to-2.0.0-postgresql.sql:22-36`). | ID | T |
| CF-R8 | GAP | mudcat shall let an operator unregister one external object, without resetting every registration in its catalog. | Gravitino has no per-object unregister; the documented recovery from an identity conflict is force-drop and recreate of the whole catalog, losing privileges, tags and owners (`OperationDispatcher.java:108-130`; `docs/apache-hive-catalog.md:88-100`). | PL | T |
| CF-R9 | GAP | mudcat shall assign identifiers at registration time, not as a side effect of the first read. | Import happens inside `loadTable`/`loadSchema` (`TableOperationDispatcher.java:629-700`); "only the imported entities have the unique id, and based on this id, we can label and control the access" (`EntityCombinedTable.java:55-58`). Unloaded objects cannot carry tags or grants; reads need write locks. | ID | T |
| CF-R10 | IMPL | mudcat shall provide separate operations to create a new object and to register an existing one. | `createTable` vs `importTable`/`loadTable` (`TableDispatcher.java:47`; `TableOperationDispatcher.java:204-206,225-260`). | IN | T |
| CF-R11 | IMPL | mudcat shall not change or delete the external object when it deletes the registration of that object. | Catalog force-drop "does not drop the Hive Metastore databases, tables, views, or physical data" (`docs/apache-hive-catalog.md:91-92`); external-table drop leaves the dataset (`docs/lakehouse-generic-catalog.md:178-182`). | UP | T |
| CF-R12 | IMPL | mudcat shall delete data of an external object only when the catalog carries an explicit opt-in setting that defaults to off. | `lance.allow-external-table-purge` default false; purge fails "before removing table metadata or deleting data" (`docs/lakehouse-generic-catalog.md:53,184-187`). | UP | T |

### 2.2 Create, partial failure, sync

| Ref | Kind | Requirement | Rationale / Gravitino evidence | Area | Ver |
|---|---|---|---|---|---|
| CF-R13 | IMPL | mudcat shall check every request against its own store limits before it changes an external system. | `TableEntity.NAME.validate` before connector call (`TableOperationDispatcher.java:236`); tests `testUnicodeCommentsExceedingStorageFailBeforeSourceDdl` verify `createTable` is `never()` called (`TestTableOperationDispatcher.java:605-640`). | OP | T |
| CF-R14 | IMPL | mudcat shall state in the error when an external change succeeded but its registration failed, and name the recovery step. | "Table %s was created in the external catalog, but its registration in Gravitino failed ... use loadTable to import it. Do not repeat createTable" (`TableOperationDispatcher.java:984-992`); doc `manage-catalogs-and-schemas.md:~296-330`. | OP | T |
| CF-R15 | IMPL | mudcat shall reuse the identifier found on an unregistered external object when it registers that object. | `uid = stringId.id()` when the source carries one, else a new ID (`TableOperationDispatcher.java:760-771`; schema `SchemaOperationDispatcher.java:765-774`). Makes recovery idempotent. | ID | T |
| CF-R16 | GAP | mudcat shall check for an existing registration of the same name before it creates an external object, for every object type. | Schema create pre-checks the store ("source creation has not been attempted", `SchemaOperationDispatcher.java:215-235`); table create does not, and writes with `overwrite=true` after the source succeeded (`TableOperationDispatcher.java:976`). | OP | T |
| CF-R17 | GAP | mudcat shall write a registration only if the registration it read is still current. | Load and alter race: "Either of them can write a stale column list over this alter ... See #14279"; a tree lock "is local to one server" (`TableOperationDispatcher.java:264-269,1456-1468`). Fix named in code: operation epoch + in-progress mark in the store. | ID | T |
| CF-R18 | GAP | mudcat shall tell the caller when the stored copy of external metadata could not be refreshed. | `operateOnEntity` swallows `OptimisticLockException` and every store failure on external paths and returns null: "the stale Gravitino copy is the lesser problem" (`OperationDispatcher.java:366-406`). The caller is not told. | EV | T |
| CF-R19 | IMPL | mudcat shall keep the identifier of a column, with its tags, when the column is renamed through mudcat. | `resolveColumnNameChanges` replays renames in order so a rename is not stored as drop plus add (`TableOperationDispatcher.java:1214-1262`); test `testAlterTableKeepsColumnIdsAcrossRenames`. | ID | T |
| CF-R20 | IMPL | mudcat shall keep the stored columns when an external system reports no column list. | "Treating it as 'all columns dropped' would delete their tags, policies and owners" (`TableOperationDispatcher.java:1324-1336`); load fails with `IllegalStateException` instead (`:1046-1053`). | ST | T |
| CF-R21 | IMPL | mudcat shall keep a registration when a drop reports that the external object did not exist. | "A false result is ambiguous: ... renamed or dropped out of band. Preserve the registration" (`TableOperationDispatcher.java:475-480`); hook skips privilege removal (`TableHookDispatcher.java:174-176`). | OP | T |
| CF-R22 | IMPL | mudcat shall take column names from the external system's response when the system changes their case. | `withCatalogColumnNames` (`TableOperationDispatcher.java:1010-1030`). | ST | T |

### 2.3 Capabilities, naming, authorisation consistency

| Ref | Kind | Requirement | Rationale / Gravitino evidence | Area | Ver |
|---|---|---|---|---|---|
| CF-R23 | IMPL | mudcat shall hold, for each connector and object scope, a machine-readable description of its case rules, name rules, NOT NULL, default value, hierarchy and managed-storage behaviour. | `Capability` interface (`Capability.java:59-160`); per-connector classes (Hive: `HiveCatalogCapability.java:70-82`; MySQL `MysqlCatalogCapability.java:42`; PG `PostgreSqlCatalogCapability.java:40`). | IN | I |
| CF-R24 | IMPL | mudcat shall refuse a feature that the connector declares unsupported, and name the feature and the object in the error. | `Preconditions.checkArgument(capabilities.columnDefaultValue().supported() \|\| ..., message + " Illegal column: ")` (`CapabilityHelpers.java:766-783`). | IN | T |
| CF-R25 | IMPL | mudcat shall normalise a name to the external system's stored form before lookup, authorisation and storage. | `Capability.normalizeName` (`Capability.java:92-97`), applied in all Normalize dispatchers; Glue and Hive declare schema/table names case-insensitive (`GlueCatalogCapability.java`, `HiveCatalogCapability.java:70-77`). | IN | T |
| CF-R26 | IMPL | mudcat shall authorise a request against the identifier on which it then operates. | `resolvePhysicalTableName` is "the single resolution path shared by the authorization layer ... and the table dispatcher" (`CapabilityHelpers.java:73-76`; `MetadataIdConverter.java:157`). | AC | T |
| CF-R27 | IMPL | mudcat shall resolve a name that matches several external objects only by case to none of them. | `SupportsTableNameResolution` contract: "An ambiguous name must never be resolved to an arbitrary object" (`SupportsTableNameResolution.java:46-50`). | AC | T |
| CF-R28 | IMPL | mudcat shall apply creation-time name rules to new names and a relaxed rule to names that already exist. | `specificationOnExistingName` (`Capability.java:121`); PG accepts any 1-63 byte native name for existing objects (`PostgreSqlCatalogCapability.java:55-72`). Otherwise legacy objects could not be dropped. | OP | T |
| CF-R29 | GAP | mudcat shall define one maximum name length per object type and check it before contacting an external system. | Limits disagree: 64 chars (`NameValidationUtils.java:32`), 128 stored (`EntityFieldLimits.java:34`), PG 63 bytes, MySQL 64, Kafka topic 128, columns 300. | ST | A |
| CF-R30 | IMPL | mudcat shall forbid its qualified-name separator inside object names, or escape it. | Full names are dot-joined (`MetadataObjects.java:39-41`; `NameIdentifier.java:30`); default pattern `\w[\w/=-]*` excludes the dot (`Capability.java:163`). | ST | T |
| CF-R31 | GAP | mudcat shall represent external namespaces of any depth without a fixed level count. | Gravitino is fixed at metalake.catalog.schema.object; Iceberg nested namespaces are mapped to a flat "A:B:C" schema name with a configurable separator and `\u0001` in storage (`design-docs/iceberg-supported-nested-namespace.md:76-155`; `Capability.supportsHierarchicalSchema`). | ST | D |
| CF-R32 | IMPL | mudcat shall reject a write that the connector cannot store exactly, instead of dropping the part it cannot store. | `Column initial default value is read-only` (`CapabilityHelpers.java:789-793`); HMS Trino view with unrepresentable owner/runAsInvoker "is rejected rather than silently discarding it" (`docs/apache-hive-catalog.md:271`). | ST | T |
| CF-R33 | IMPL | mudcat shall pass external metadata keys through to clients unchanged, including keys that identify the data format. | Glue "Metadata passthrough guarantee" (`design-docs/gravitino-glue-catalog.md:~295-310`). | IN | T |
| CF-R34 | IMPL | mudcat shall choose, per object type and connector, whether to delegate to the external system, extend it, or hold the object itself. | Capability-driven tiers for views: delegate (Iceberg, Paimon), format-aware delegate (HMS, Glue), read-only (JDBC), managed (Hudi, Generic) (`design-docs/gravitino-logical-view-management.md:124-170`). | PL | I |
| CF-R35 | IMPL | mudcat shall hold in its own store any metadata the external system cannot hold, and shall say which metadata that is. | Schema properties incl. secret URNs persisted in `SchemaEntity` "so cleanup still works when the underlying catalog does not retain schema properties" (`SchemaOperationDispatcher.java:262-264`); tags/owner/stats key on ID. | ST | I |
| CF-R36 | IMPL | mudcat shall refresh the stored schema of a registered self-describing format from the data, under a stated per-catalog policy. | `SchemaRefreshMode` DECLARED_AND_EMPTY / VERSION_CHECK (`LanceTableOperations.java:112-160,515-600`). | ST | T |

### 2.4 Classloader isolation

| Ref | Kind | Requirement | Rationale / Gravitino evidence | Area | Ver |
|---|---|---|---|---|---|
| CF-R37 | IMPL | mudcat shall load each connector in its own class space, so that connector dependencies do not conflict with each other or with the server. | `IsolatedClassLoader` with `CustomURLClassLoader` (`IsolatedClassLoader.java:190-250`); `CATALOG_LOAD_ISOLATED` default true. Hive/Hadoop/JDBC bring conflicting jars. | OP | T |
| CF-R38 | IMPL | mudcat shall share one class space among catalogs whose isolation-relevant settings are equal, and release it when the last such catalog is gone. | `ClassLoaderPool` ref-counted; key = provider + 9 properties (`CatalogManager.java:165-178`; `ClassLoaderPool.java:30-110`), justified by Metaspace use. | PF | T |
| CF-R39 | IMPL | mudcat shall let operations that already started on a catalog finish before it releases that catalog's connector resources. | Lease/retire: "an operation can never observe a half-closed catalog" (`CatalogManager.java:188-300`; `CatalogLease.java`). | OP | T |
| CF-R40 | IMPL | mudcat shall unload a connector completely: deregister its JDBC drivers, stop its threads, clear its thread-locals, shutdown hooks and security providers. | `ClassLoaderResourceCleanerUtils` (`:54-460`); otherwise the loader leaks (Metaspace). | OP | T |
| CF-R41 | IMPL | mudcat shall return connector results to callers as plain data objects that hold no connector classes. | `ConnectorObjectSnapshot.detach` converts to DTOs before the lease closes (`ConnectorObjectSnapshot.java:36-60`; `OperationDispatcher.java:150-160`). | OP | T |
| CF-R42 | GAP | mudcat shall load connector code only from locations in an operator-set allow-list, never from a path given in a catalog property. | The `package` property names any directory whose jars are loaded (`CatalogManager.java:2191-2223`); `ops-impl` loads any class by name (`BaseCatalog.java:494-513`). I found no allow-list in `core` or `server` (`grep PROPERTY_PACKAGE`). Anyone able to create a catalog can run code. (inferred: authorisation of who may create catalogs is the only guard) | AC | I |
| CF-R43 | GAP | mudcat shall keep the list of connector classes that need isolation in the connector's own metadata, not in a core class. | `isCatalogClass` hard-codes 10 package prefixes (`IsolatedClassLoader.java:293-304`); Glue, StarRocks, ClickHouse packages are not in it (inferred: they work through the `ClassNotFoundException` fallback at `:240-247`). Its comment records a permanent `NoClassDefFoundError` when a prefix was missing. | OP | I |

### 2.5 Properties, secrets

| Ref | Kind | Requirement | Rationale / Gravitino evidence | Area | Ver |
|---|---|---|---|---|---|
| CF-R44 | IMPL | mudcat shall declare, for each property of a catalog or object type, its type, default and whether it is required, immutable, hidden or reserved. | `PropertyEntry` fields (`PropertyEntry.java:33-43`); `BaseCatalogPropertiesMetadata` entries. | ST | I |
| CF-R45 | IMPL | mudcat shall reject a create request that sets a reserved property, omits a required property, or gives a value that does not decode. | `PropertiesMetadataHelpers.validatePropertyForCreate` (`PropertiesMetadataHelpers.java` first method). | ST | T |
| CF-R46 | IMPL | mudcat shall reject a change to, or a removal of, an immutable or reserved property. | `validatePropertyForAlter` "is immutable or reserved, cannot be set/deleted". | ST | T |
| CF-R47 | GAP | mudcat shall reject a property key that no declaration covers, unless the key has a declared pass-through prefix. | `validatePropertyForAlter` does `continue` for undeclared keys (no check at all); only create-time required/reserved checks apply to declared ones. `gravitino.bypass.` is the only declared pass-through. | ST | T |
| CF-R48 | IMPL | mudcat shall mask the value of every hidden property, secret reference and undeclared sensitive-named property in responses. | `classifyHiddenProperties`: masks hidden, `jdbc-url` with credentials, secret URNs, undeclared keys with `secret/password/token/credential/access/account` (`HiddenPropertyMaskUtils.java:120-150`). | AC | T |
| CF-R49 | IMPL | mudcat shall reject the mask placeholder as an input value. | `validateNoMaskedPlaceholders` (`HiddenPropertyMaskUtils.java:93-105`); stops a read-modify-write client persisting `******`. | AC | T |
| CF-R50 | IMPL | mudcat shall omit from responses any property that is both reserved and hidden. | The identifier key `gravitino.identifier` is omitted, not masked (`HiddenPropertyMaskUtils.java:22-26`). | ST | T |
| CF-R51 | IMPL | mudcat shall keep secret values outside the catalog record and store only references to them. | Entity keeps `urn:` values; `createBaseCatalog` resolves to plaintext "for connector init only" (`CatalogManager.java:2054-2060`). | AC | I |
| CF-R52 | IMPL | mudcat shall not let a deleted or rotated secret block listing, disabling or dropping a catalog. | `toPlaintextOrMasked`: "a deleted/rotated secret must not block disable, drop, re-bind or list" (`CatalogManager.java:2056`). | OP | T |
| CF-R53 | IMPL | mudcat shall validate the complete proposed configuration of a catalog change before it persists the change. | `validateConfiguration(proposedConfiguration)` inside the `store.update` lambda (`CatalogManager.java:1340-1360`). | OP | T |

### 2.6 Catalog lifecycle

| Ref | Kind | Requirement | Rationale / Gravitino evidence | Area | Ver |
|---|---|---|---|---|---|
| CF-R54 | IMPL | mudcat shall verify the connection of a catalog at registration, with a bounded timeout, and refuse the registration if verification fails. | `validateOnCreate` (`BaseCatalog.java:268-270`); JDBC probe, `verify-on-create=true` default, connect+read timeouts (`docs/jdbc-catalog-connection-validation.md:~50-110`). | OP | T |
| CF-R55 | IMPL | mudcat shall let an operator skip connection verification at registration, for a catalog whose backend is temporarily down. | `verify-on-create=false`; "the connection is still opened lazily on first use". | OP | D |
| CF-R56 | IMPL | mudcat shall offer a connection test for a proposed registration and for proposed changes to an existing one, without storing either. | `testConnection(ident,type,provider,comment,props)` and `testConnection(ident, changes...)` build a temporary wrapper (`CatalogManager.java:966-1130`). | OP | D |
| CF-R57 | GAP | mudcat shall not hold a lock shared with other catalogs while it contacts an external system. | Docs: the create-time probe "runs under the metalake write lock ... would otherwise ... stall the whole metalake" (`docs/jdbc-catalog-connection-validation.md:~90`). Code takes the WRITE lock on the catalog node (`CatalogManager.java:880`), with ancestors read-locked; docs and code disagree (Q3). | PF | A |
| CF-R58 | IMPL | mudcat shall let an operator disable a catalog so that every read and write through it is refused while its registrations stay. | `PROPERTY_IN_USE`; `checkMetalakeAndCatalogInUse` throws `CatalogNotInUseException` (`BaseCatalog.java:316-341`); doc "A catalog that is not in use can only be listed, loaded, enabled, or dropped". | OP | T |
| CF-R59 | IMPL | mudcat shall refuse to drop an enabled catalog, or a catalog that holds user-created objects, unless the caller asks for force. | `dropCatalog` throws `CatalogInUseException` / `NonEmptyCatalogException` (`CatalogManager.java:1393-1420,1503-1520`). | OP | T |
| CF-R60 | IMPL | mudcat shall, on a forced drop of a catalog, remove every registration and governance record below it in one transaction. | `store.delete(ident, CATALOG, true)` cascade (`CatalogManager.java:1456`); docs: "clears owner, privilege, tag, and statistic associations" (`docs/migration-guide.md:32`). | UP | T |
| CF-R61 | GAP | mudcat shall compute the in-use state of a catalog from its parent at read time, not copy it into each catalog record. | `metalake-in-use` is copied into every catalog; "we can't make sure we can change all catalog properties in a transaction" (`MetalakeManager.java:549-552,589-592`; `CatalogManager.java:2315`). Partial failure leaves catalogs inconsistent. | OP | T |
| CF-R62 | IMPL | mudcat shall invalidate the cached connection of a catalog on every node when the catalog changes. | `CatalogChangeLogListener` consumes `entity_change_log` rows; on failure clears the whole cache (`CatalogChangeLogListener.java:60-110`). | OP | T |
| CF-R63 | GAP | mudcat shall assign the owner of a new object in the same transaction that creates the registration. | Owner set after create by the hook; failure for non-catalog types leaves an unowned object (`TableHookDispatcher.java:100-130`); catalogs are rolled back by `dropCatalog(force)` (`CatalogHookDispatcher.java:134-145`). | AC | T |
| CF-R64 | IMPL | mudcat shall emit a pre-event, a result event and a failure event for each catalog operation. | `TableEventDispatcher.java:95-125` pattern, repeated for all types. | EV | T |

## 3. Quirks and gotchas ledger

**Q1. Identity is born at the first read (lazy import)**
- An external table or schema is not in Gravitino until someone loads it. `loadTable` imports it as a side effect under a WRITE lock; `listTables` returns names only. Tags, grants and owners attach to imported entities only.
- Cause: MODEL (external namespace has no IDs) / CONCURRENCY (reads take write locks).
- Evidence: `core/.../catalog/EntityCombinedTable.java:55-58` "only the imported entities have the unique id, and based on this id, we can label and control the access"; `TableOperationDispatcher.java:629-700`.
- Mudcat implication: CF-R9; design warning: a registration pass must be explicit, and reads must not write.

**Q2. Identity is stored in the external system, in whatever field the system offers**
- Hive: table parameter `gravitino.identifier`. JDBC: appended to the table or schema comment as `(From Gravitino, DO NOT EDIT: gravitino.v1.uid123)` and stripped on load. Kafka: derived from the topic UUID. MySQL schemas: nowhere.
- Cause: MODEL.
- Evidence: `StringIdentifier.java:47` `"From Gravitino, DO NOT EDIT: "`; `JdbcCatalogOperations.java:489-500,563-570`; `hive-metastore-common/.../HiveShim.java:221` `table.getParameters().get(ID_KEY)`; `docs/jdbc-mysql-catalog.md:101` "does not support setting comment".
- Mudcat implication: CF-R4, CF-R5. A user editing the comment can break the link; the ID must be recoverable from mudcat's own store.

**Q3. JDBC comment updates must re-attach the ID**
- When a user updates a table comment through Gravitino, MySQL and PG code re-reads the old comment, extracts the ID and appends it to the new one; otherwise the link is lost.
- Cause: MODEL.
- Evidence: `PostgreSqlTableOperations.java:593-607`, `MysqlTableOperations.java:438-447`.
- Mudcat implication: CF-R5 (do not rely on in-band markers).

**Q4. Kafka identity is a truncated UUID**
- The topic ID is `topicId.getLeastSignificantBits() & MAX_ID`: 128 bits reduced to the generator's range.
- Cause: MODEL.
- Evidence: `catalogs/catalog-kafka/.../KafkaCatalogOperations.java:581-583`.
- Mudcat implication: collision risk is not documented or checked (inferred). CF-R6 (unique active registration) is the backstop. Use full-width IDs.

**Q5. Create is two non-atomic steps; recovery is by loading**
- Connector creates, then `store.put`. If the put fails: error text tells the user not to repeat create and to `loadTable`. The external object carries the ID so import adopts it.
- Cause: MODEL / CONCURRENCY (no distributed transaction).
- Evidence: `TableOperationDispatcher.java:976-992`; `docs/manage-catalogs-and-schemas.md:~296-300` "creation is not automatically replayed".
- Mudcat implication: CF-R14, CF-R15. mudcat's commit-with-requirements model could instead record intent first (inferred).

**Q6. Table create does not pre-check the store; schema create does**
- Schema create refuses when a registration exists "before source creation". Table create calls the connector first, then `store.put(..., overwrite=true)`; a conflict surfaces afterwards as `TableAlreadyExistsException` saying the object was already created externally.
- Cause: OTHER (inconsistency).
- Evidence: `SchemaOperationDispatcher.java:215-235` vs `TableOperationDispatcher.java:976-984`; test `testCreateTableReportsRegistrationConflict` (`TestTableOperationDispatcher.java:974`).
- Mudcat implication: CF-R16.

**Q7. Import conflicts cannot be fixed per object**
- If the external ID is already registered under another path (external rename, two catalogs on one metastore), import throws `UnsupportedOperationException` with a recovery recipe: force-drop and recreate the whole catalog registration, then reapply tags, grants and owners.
- Cause: MODEL.
- Evidence: `OperationDispatcher.java:108-130`; `docs/migration-guide.md:26-41`.
- Mudcat implication: CF-R8. Provide per-object unregister and re-bind.

**Q8. Tables do not follow an external rename; schemas do**
- Schema import detects that the old path is gone (`loadSchema` of the old name throws `NoSuchSchemaException`) and moves the registration; table and view imports never move it.
- Cause: OTHER.
- Evidence: `SchemaOperationDispatcher.java:783-806`; `docs/migration-guide.md:26-30`.
- Mudcat implication: CF-R4; decide one rule for all types.

**Q9. Primary keys had to be changed to allow re-registration**
- Active rows have `deleted_at=0`; soft-deleted rows keep the same ID, so the PK is `(id, deleted_at)`. `catalog_meta` still has `PRIMARY KEY (catalog_id)` because catalogs get fresh IDs on recreate; schema/table/topic/view keep the external ID.
- Cause: STORE.
- Evidence: `scripts/postgresql/schema-2.0.0-postgresql.sql:65,95,124-125`; `upgrade-1.3.0-to-2.0.0-postgresql.sql:22-36`; also "Earlier 2.0.0 snapshots with identifier-only primary keys also require these primary-key changes" (`docs/migration-guide.md:22`).
- Mudcat implication: if an ID is ever written into an external system, it outlives the row; soft delete plus reuse needs a composite key from day one. CF-R7.

**Q10. Store failures on external paths are logged and hidden**
- `operateOnEntity` returns null on `OptimisticLockException`, `NoSuchEntityException` and any other exception; the caller gets the connector's answer without Gravitino metadata. Reason given: retry could re-apply non-idempotent external changes.
- Cause: CONCURRENCY.
- Evidence: `OperationDispatcher.java:366-406`.
- Mudcat implication: CF-R18; mudcat's idempotency keys address the retry concern directly.

**Q11. Load/alter race loses column IDs, and tree locks are per server**
- A load that read the catalog before a concurrent alter and the store after it can write the stale column list, turning a rename into drop+add and deleting the column's tags. The comment says closing it needs a new store API (operation epoch, in-progress mark); tracked in #14279.
- Cause: CONCURRENCY.
- Evidence: `TableOperationDispatcher.java:264-269,1456-1468`; test `testLoadDoesNotUndoConcurrentColumnRename`.
- Mudcat implication: CF-R17.

**Q12. Column rename detection is heuristic**
- Stored columns are matched to catalog columns by name; renames are replayed from the change list; a catalog that changes the case of the new name is matched case-insensitively with a "claimed" set; renamed columns claim names first so a stale column with the same name does not steal the ID.
- Cause: MODEL.
- Evidence: `TableOperationDispatcher.java:1214-1300,1352-1360`; tests `testRenameIntoNameOfStaleStoredColumn`, `testAlterTableKeepsColumnIdWhenRenameDiffersInCase`.
- Mudcat implication: CF-R19. A rename done outside the catalog cannot be detected and loses the column ID (inferred).

**Q13. Connectors that return no column list**
- Load throws `IllegalStateException`; alter completes using stored columns; update keeps stored columns.
- Cause: SPEC (connector contract).
- Evidence: `TableOperationDispatcher.java:1046-1053,1324-1336`; tests `testLoadFailsWhenCatalogReportsNoColumns`, `testRenameCompletesWhenCatalogReportsNoColumns`.
- Mudcat implication: CF-R20.

**Q14. "Drop returned false" is ambiguous**
- The registration is kept, privileges are kept. A real out-of-band drop leaves a stale registration needing separate cleanup.
- Cause: MODEL.
- Evidence: `TableOperationDispatcher.java:475-480`; `TableHookDispatcher.java:174-178`.
- Mudcat implication: CF-R21.

**Q15. Core hard-codes provider names to decide if a catalog is empty**
- `containsUserCreatedSchemas` treats a lone `default` (Hive), `public` (PG) or Kafka schema as "not user created"; for MySQL (no schema comment) it assumes user-created to avoid data loss; otherwise it compares store with `listSchemas` and looks for an ID.
- Cause: OTHER (framework knows connectors).
- Evidence: `CatalogManager.java:1555-1612` (issue `#2314` cited at `:1563`).
- Mudcat implication: put "auto-created default containers" in connector metadata; extend Capability.

**Q16. Tree lock granularity trades visibility for throughput**
- Table create locks only the table node so creates in one schema run in parallel, but `listTables` "may briefly observe a table whose creation has not committed yet".
- Cause: CONCURRENCY.
- Evidence: `TableOperationDispatcher.java:244-250`; test `testCreateTableRunsConcurrentlyWithCreateOfAnotherTable`.
- Mudcat implication: list results are not a snapshot; mudcat's "every response names the commit it saw" removes the ambiguity.

**Q17. Capabilities can depend on a live connection**
- Hive `columnNotNull`/`columnDefaultValue` are supported only on Hive 3; the capability lazily asks the metastore for its version. A capability call can therefore open a connection.
- Cause: SPEC.
- Evidence: `HiveCatalogCapability.java:46-60,79-95`; `CapabilityHelpers.java:100-125` forbids callbacks from calling the backend or the store.
- Mudcat implication: CF-R23: capabilities must be cheap and cacheable; `catalog.capability()` is preloaded at wrapper init (`CatalogManager.java:1943-1944`).

**Q18. Case rules differ per scope and per backend, and sometimes per table format**
- Hive: schema/table/view/column case-insensitive. Iceberg: sensitive, except when the backend is Hive. Glue: schema and table insensitive; column case depends on table format and "this check cannot see" it. Normalisation is applied in Normalize; list/load apply only case folding.
- Cause: SPEC.
- Evidence: `HiveCatalogCapability.java:66-78`; `IcebergCatalogCapability.java:63-70`; `GlueCatalogCapability.java:55-72`; `docs/apache-hive-catalog.md:83-84`; `docs/lakehouse-iceberg-catalog.md:62-64` ("for rest backend, always use the same casing... Loading with different casing is rejected").
- Mudcat implication: CF-R25; a capability of the form scope x table-format is needed (inferred).

**Q19. Name limits disagree across five places**
- Default rule 64 chars; entity store `VARCHAR(128)`; PG 63 UTF-8 bytes; MySQL 64; Kafka 128; columns 300; table comments 4096. Imports of an external name longer than the store allows would fail at `store.put` (inferred; test `testImportNativeTableWithLongColumnName` pins 128/129/300 for columns).
- Cause: STORE.
- Evidence: `NameValidationUtils.java:32`; `EntityFieldLimits.java:34-49`; `scripts/postgresql/schema-2.0.0-postgresql.sql:116`; `PostgreSqlCatalogCapability.java:40`; `KafkaCatalogCapability.java:41-44`.
- Mudcat implication: CF-R29.

**Q20. The authorisation system leaks into naming**
- Kafka topic names cannot contain dots "because Gravitino authorization does not support them", and are capped at 128 "because of the metadata store's name length limit". These block registering real topics.
- Cause: OTHER.
- Evidence: `KafkaCatalogCapability.java:40-44`.
- Mudcat implication: CF-R30; qualified names with escaping, so any external name can be registered.

**Q21. Nested namespaces are squeezed into a flat schema name**
- Iceberg `a.b.c` namespaces become schema `a:b:c` (separator configurable), `\u0001` in the store, per-segment name checks, and the separator becomes reserved. Existing names that contain the separator block enabling.
- Cause: MODEL.
- Evidence: `IcebergCatalogCapability.java:90-140`; `design-docs/iceberg-supported-nested-namespace.md:76-155`.
- Mudcat implication: CF-R31.

**Q22. Authorised name and operated name must be the same**
- Case-folding catalogs can hold `FOO` and `foo`. The table is resolved to its physical name once, in a function shared by the authorisation filter and the dispatcher; resolution takes no lock.
- Cause: SECURITY.
- Evidence: `CapabilityHelpers.java:62-100`; `SupportsTableNameResolution.java:24-60`.
- Mudcat implication: CF-R26, CF-R27.

**Q23. The class-sharing rule is a hard-coded prefix list**
- `isSharedClass` = slf4j/log4j/`java.`/not-a-catalog-class; `isCatalogClass` lists 10 package prefixes. A missing prefix once caused a permanent cached `NoClassDefFoundError` for synthetic `$1` classes. Glue/StarRocks/ClickHouse packages are not listed (inferred: fall back to the isolated URLs via the CNFE catch).
- Cause: OTHER.
- Evidence: `IsolatedClassLoader.java:278-304`.
- Mudcat implication: CF-R43.

**Q24. Isolation dimensions are listed explicitly and each has a story**
- Pool key = provider + `package`, `authorization-provider`, Kerberos principal/keytab, `metastore.uris`, `jdbc-url`, `uri`, `fs.defaultFS`. Reasons: Hadoop UGI and `FileSystem.CACHE` are per-classloader static state, `DriverManager` is a global registry, HiveConf is static. `uri` was added because two MySQL-backed Iceberg catalogs would cross-contaminate the driver registry.
- Cause: ENGINE (Hadoop/JDBC statics).
- Evidence: `CatalogManager.java:149-178`.
- Mudcat implication: CF-R38.

**Q25. Pool operations serialise per key**
- Class-loader creation (jar scan) and final cleanup (thread interrupts, reflection) run inside `ConcurrentHashMap.compute`, so create/drop of catalogs with the same key serialise.
- Cause: CONCURRENCY.
- Evidence: `ClassLoaderPool.java:30-45`.
- Mudcat implication: acceptable for a handful of keys; note the assumption.

**Q26. The connector's loader must not be the context loader during store access**
- The wrapper swaps the context loader only around connector calls, because MyBatis resolves resources and JDBC drivers through it, "and a catalog that bundles its own JDBC driver would win those lookups".
- Cause: ENGINE.
- Evidence: `CatalogManager.java:796-806`; `OperationDispatcher.java:343-346` (no store call inside the connector callback).
- Mudcat implication: in-process plugins must never share the context loader with the core's database access. Prefer out-of-process connectors for new ones (inferred).

**Q27. Unloading a classloader is a long list of leak fixes**
- Drivers in `DriverManager`, Hadoop FS stats thread, threads whose class was defined by the loader, `ThreadLocal` values (including `SoftReference` in Jackson's `BufferRecycler`), JVM shutdown hooks, Log4j context listeners, `ResourceBundle` cache, JCA providers (AWS OpenSSL), MySQL abandoned-connection thread.
- Cause: ENGINE.
- Evidence: `catalogs/catalog-common/.../ClassLoaderResourceCleanerUtils.java:54-460`.
- Mudcat implication: CF-R40; a reason to keep connectors out of process.

**Q28. A catalog property can point the server at code**
- `package` = directory of jars to load; `ops-impl` = class name to instantiate; both are immutable but settable by anyone permitted to create a catalog. "The object you used is not stable, don't use it unless you know what you are doing."
- Cause: SECURITY.
- Evidence: `BaseCatalogPropertiesMetadata.java:57-71`; `CatalogManager.java:2191-2223`; `BaseCatalog.java:88-92,494-513`.
- Mudcat implication: CF-R42.

**Q29. Stack inspection decides whether the in-use check runs**
- `checkMetalakeAndCatalogInUse` uses `StackWalker` to see if `dropCatalog` is on the stack and skips the in-use check if so.
- Cause: OTHER (hack).
- Evidence: `BaseCatalog.java:331,484-487`.
- Mudcat implication: pass an explicit flag; do not infer intent from call stacks.

**Q30. In-use state is copied downward and updated non-atomically**
- Disabling a metalake loops over its catalogs setting `metalake-in-use`; a failure on one is logged and the rest continue, then the call throws "Failed to update in use status of the following catalogs". Force-drop temporarily re-enables a disabled metalake, then tries to restore it.
- Cause: CONCURRENCY / MODEL.
- Evidence: `MetalakeManager.java:419-423,448-452,549-552,589-592,656-686`.
- Mudcat implication: CF-R61.

**Q31. Property checks are lenient for undeclared keys, strict for declared ones**
- On alter, undeclared keys skip all checks (`continue`). Masking compensates by hiding undeclared keys whose name looks sensitive. `jdbc-url` is masked only if it embeds credentials.
- Cause: SECURITY.
- Evidence: `PropertiesMetadataHelpers.java` (`validatePropertyForAlter`); `HiddenPropertyMaskUtils.java:120-150`.
- Mudcat implication: CF-R47, CF-R48.

**Q32. Effective config is frozen into the catalog at create**
- Provider conf-file values (`<package>/conf/<provider>.conf`) and default JDBC driver are merged into the stored properties at create; later file edits do not apply. Old provider name `hadoop` is converted to `fileset` on every read.
- Cause: COMPAT.
- Evidence: `CatalogManager.java:1767-1775,2118-2150,2279-2310`.
- Mudcat implication: record effective configuration used at registration (reproducibility); keep a migration path for renamed providers.

**Q33. Create-time verification is opt-in per connector and uneven**
- JDBC, Iceberg and Glue opt in. Glue runs only local checks ("credentials that only AWS can reject must not block the create"). Hive, Kafka, Paimon, Hudi, fileset and model do not verify. Default `testConnection` throws `UnsupportedOperationException`.
- Cause: OTHER.
- Evidence: `GlueCatalog.java:72-90`; `JdbcCatalog.java:118-140`; `BaseCatalog.java:253-270`; `CatalogOperations.java:52-67`.
- Mudcat implication: CF-R54, CF-R56. Define a connection-test contract for every connector.

**Q34. Test connection uses a sentinel identity**
- Test-connection entities carry `DUMMY_ID` (-1); Kafka checks it to avoid creating the default schema during a test.
- Cause: OTHER.
- Evidence: `StringIdentifier.java:41`; `KafkaCatalogOperations.java:604-607`.
- Mudcat implication: pass an explicit dry-run flag.

**Q35. Owner is set after create; the failure policy differs by type**
- Table hook: `setOwner` runs after create; a concurrent drop is tolerated, any other failure propagates. Catalog hook: on failure `dropCatalog(force)` rolls back. Existing-object `EXIST_OK` creates must not change ownership (`createdTableId` is carried out of the dispatcher to prove this request inserted the row).
- Cause: CONCURRENCY.
- Evidence: `TableHookDispatcher.java:100-130`; `CatalogHookDispatcher.java:112-145`; `EntityCombinedTable.java:61-64`.
- Mudcat implication: CF-R63.

**Q36. Secrets are staged, then committed or rolled back around entity writes**
- Catalog/schema create stages secrets, creates, then commits; on failure after the external object exists, secrets are kept "because the schema may reference them". If the entity cannot be removed after a failed catalog create, the error says "drop the catalog with force before creating it again".
- Cause: CONCURRENCY.
- Evidence: `SchemaOperationDispatcher.java:196-215,300-312`; `CatalogManager.java:2365-2388`.
- Mudcat implication: the same three-phase pattern applies to credential material that mudcat issues (inferred).

**Q37. Column "initial default" is returned by connectors but cannot be set or stored**
- `ColumnEntity` has no initial-default field, and create/add-column with one is rejected: "catalogs may return it on load (e.g. Iceberg v3), but setting it ... is not supported by any catalog capability yet".
- Cause: MODEL / SPEC (Iceberg v3).
- Evidence: `CapabilityHelpers.java:785-796`; `core/.../meta/ColumnEntity.java` (no such field, grep).
- Mudcat implication: CF-R32; model the Iceberg v3 write-default / initial-default pair from the start.

**Q38. Dropping an external table leaves data; purge is gated; unprovision is best-effort**
- Drop of `external=true` skips the `unprovisionTableLocation` callback; Lance purge of an external table needs a catalog flag; the callback "is not guaranteed to be called at all" after a crash; the catalog does not record whether it provisioned a location.
- Cause: MODEL.
- Evidence: `docs/lakehouse-generic-catalog.md:172-200`.
- Mudcat implication: CF-R11, CF-R12; record per replica whether mudcat allocated it.

**Q39. Stored Lance columns can be stale; refresh is a policy**
- Default `DECLARED_AND_EMPTY` refreshes only tables flagged declared or with empty columns; `VERSION_CHECK` opens the dataset on every load.
- Cause: ENGINE (Spark staged create writes schema only to the dataset path).
- Evidence: `LanceTableOperations.java:112-160,515-560`.
- Mudcat implication: CF-R36; version-checking for self-describing formats is cheap with immutable versions.

**Q40. Glue and HMS: all table types visible, views hidden, view formats per engine**
- Glue lists Hive, Iceberg and Delta tables in one catalog and hides `VIRTUAL_VIEW` in phase 1; HMS view rows are decoded by engine detection (Trino, Spark, Flink, Hive) with a priority chain.
- Cause: SPEC.
- Evidence: `design-docs/gravitino-glue-catalog.md:~246-330`; `design-docs/gravitino-logical-view-management.md:330-372`.
- Mudcat implication: CF-R33, CF-R34, CF-R32.

**Q41. Events fire at the outermost layer, so internal imports are silent**
- A `loadTable` emits only Load events even when it wrote a registration (inferred from `TableEventDispatcher.java:107-125` and `OperationDispatcher` having no event hook).
- Cause: OTHER.
- Evidence: see above.
- Mudcat implication: every registration write should land in the change stream (EV).

## 4. Open questions, inconsistencies, half-implemented

1. **#14279 is open.** The load/alter race on stored columns is acknowledged with a named fix (operation epoch + in-progress mark) and not implemented (`TableOperationDispatcher.java:1456-1468`).
2. **Docs vs code on lock scope.** Docs say the create probe runs "under the metalake write lock"; code locks the catalog node WRITE (`CatalogManager.java:880`) with the metalake READ-locked. Either the doc is stale or the tree lock semantics differ from what I read. Not resolved.
3. **Schema create pre-checks the store, table create does not** (CF-R16). The doc says "External schema creation now checks the entity store before calling the source" and is silent on tables (`docs/manage-catalogs-and-schemas.md:~316-320`).
4. **Tables and views never follow external renames; schemas do** (Q8). No stated reason in code.
5. **No per-object unregister** (CF-R8). Docs call it "not supported" three times.
6. **Capability set is small.** The brief asked about auto-increment and unique-name flags; neither exists. `Column.autoIncrement()` passes through to connectors without a capability check (`CapabilityHelpers.java:709-720` checks only NOT NULL, default, name).
7. **`Catalog.Type.UNSUPPORTED` ("for test only")** is in the public enum (`Catalog.java:59-60`). Test seams in the API.
8. **`CONTRIB_CATALOGS_TYPES` is a hard-coded set** of three providers used to locate jars in test mode (`CatalogManager.java:147,2166-2178`); `GRAVITINO_TEST` env var changes jar paths (`:2156,2197`).
9. **Verify-on-create is inconsistent** (Q33); `BaseCatalog.validateConfiguration(Map)` default accepts anything "for compatibility" (`BaseCatalog.java:~240`).
10. **`isCatalogClass` omits several catalogs** (Q23); I inferred, not tested, that they work via the fallback path.
11. **Kafka ID derivation** (Q4) has no collision handling that I could find.
12. **Hive/Iceberg ID storage not read.** I confirmed Hive reads `gravitino.identifier` from table parameters (`HiveShim.java:221`). For Iceberg I found no `StringIdentifier` reference in `catalogs/catalog-lakehouse-iceberg/src/main`; I infer the property passes through as an ordinary table property (not verified).
13. **Internal chain skips Hook and Event layers** by design (`GravitinoEnv.java:944-1060`); who may call it is a convention, not enforced (inferred).
14. **`ops-impl` and `package` are unguarded** (CF-R42); the property comment calls the SPI "not stable".
15. **Listing a catalog's schemas on force=false drop can fail on connection errors**; the error suggests `force=true` (`CatalogManager.java:1520-1540`), i.e. an unreachable external system blocks a polite drop.
16. **Glue view support is not in v1** (`design-docs/gravitino-logical-view-management.md:488-531`) and the Glue design states Phase 2 (engine integration) and Phase 3 (Hive catalog mixed formats) as future work (`gravitino-glue-catalog.md:660-700`).
17. **RFC history.** `rfc/rfc-2` and `rfc/rfc-3` (KV store key encoding and MVCC transactions) are marked "REMOVED ... Removed KV store on: 2024-12"; the metadata system now runs on relational stores only. RFC-1 (`rfc/rfc-1/rfc-1.md`) listed "replace HMS later on" as a prerequisite (item 6); the shipped design federates HMS instead. A lesson on scope: the original goal was to be the metastore; the product that shipped is a federation layer.
