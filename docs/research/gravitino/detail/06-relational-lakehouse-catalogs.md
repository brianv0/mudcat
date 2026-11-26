> **Research note, not a requirement document.** Drafted by a code-reading agent from Apache Gravitino commit `9aa78cf` (2026-11-26), then spot-checked by the lead. Line numbers can drift by a few lines; search by file and symbol. Items marked `(inferred)` were not confirmed in code. The text does not follow Simplified Technical English. See [../README.md](../README.md) for the synthesis.

# Relational and lakehouse catalog providers (ref prefix RL)

Scope read: `catalogs/catalog-jdbc-*`, `catalogs-contrib/catalog-jdbc-*`, `catalogs/catalog-hive`, `hive-metastore-common`, `hive-metastore{2,3}-libs`, `catalog-glue`, `catalog-lakehouse-{iceberg,paimon,hudi,generic}`, `catalog-common`, plus `core/.../StringIdentifier.java`, `core/.../connector/capability/Capability.java` and `iceberg/iceberg-common` where the connectors depend on them. Checkout commit 9aa78cf. Nothing in the checkout was modified.

Path aliases used in section 3 (all repo-relative):

| Alias | Path |
|---|---|
| JC | `catalogs/catalog-jdbc-common/src/main/java/org/apache/gravitino/catalog/jdbc` |
| MY | `catalogs/catalog-jdbc-mysql/src/main/java/org/apache/gravitino/catalog/mysql` |
| PG | `catalogs/catalog-jdbc-postgresql/src/main/java/org/apache/gravitino/catalog/postgresql` |
| DO | `catalogs/catalog-jdbc-doris/src/main/java/org/apache/gravitino/catalog/doris` |
| SR | `catalogs/catalog-jdbc-starrocks/src/main/java/org/apache/gravitino/catalog/starrocks` |
| CH | `catalogs-contrib/catalog-jdbc-clickhouse/src/main/java/org/apache/gravitino/catalog/clickhouse` |
| HG | `catalogs-contrib/catalog-jdbc-hologres/src/main/java/org/apache/gravitino/catalog/hologres` |
| OB | `catalogs-contrib/catalog-jdbc-oceanbase/src/main/java/org/apache/gravitino/catalog/oceanbase` |
| HM | `catalogs/hive-metastore-common/src/main/java/org/apache/gravitino/hive` |
| HV | `catalogs/catalog-hive/src/main/java/org/apache/gravitino/catalog/hive` |
| IC | `catalogs/catalog-lakehouse-iceberg/src/main/java/org/apache/gravitino/catalog/lakehouse/iceberg` |
| ICC | `iceberg/iceberg-common/src/main/java/org/apache/gravitino/iceberg/common` |
| PM | `catalogs/catalog-lakehouse-paimon/src/main/java/org/apache/gravitino/catalog/lakehouse/paimon` |
| HU | `catalogs/catalog-lakehouse-hudi/src/main/java/org/apache/gravitino/catalog/lakehouse/hudi` |
| GN | `catalogs/catalog-lakehouse-generic/src/main/java/org/apache/gravitino/catalog/lakehouse` |
| GL | `catalogs/catalog-glue/src/main/java/org/apache/gravitino/catalog/glue` |
| CORE | `core/src/main/java/org/apache/gravitino` |

## 1. Decomposition

### 1.1 What these subsystems are

Every connector in scope is a "federating" provider. Gravitino holds a metadata record for a schema, table or view, and the real object lives in an external system (a SQL database, a Hive Metastore, AWS Glue, an Iceberg/Paimon backend). Each connector translates a **common relational model** (Gravitino `Table`, `Column`, `Type`, `Transform`, `Distribution`, `SortOrder`, `Index`, `TableChange`) into the external system's own DDL or API, and back. The one exception is `catalog-lakehouse-generic`, which stores its metadata **in Gravitino's own entity store** ("managed" tables) and only points at data (Lance, Delta). That connector is the closest analogue to mudcat's "register an external format" idea.

### 1.2 Component tree

```
catalog-common (constants, PropertyConverter, storage/credential property names)
core: connector/capability/Capability        <- per-catalog declared capabilities (SPI)
core: StringIdentifier                        <- id embedded in external object comment/parameters
core: catalog/ManagedTableOperations          <- base for tables stored in Gravitino's own store

catalog-jdbc-common
  JdbcCatalog / JdbcCatalogOperations         <- SPI entry; orchestrates; strips/adds StringIdentifier
  operation/JdbcTableOperations (1038 loc)    <- DDL generation, JDBC DatabaseMetaData reads, hooks
  operation/JdbcDatabaseOperations            <- schema ops; abstract createSysDatabaseNameSet()
  converter/JdbcTypeConverter (abstract)      <- toGravitino(JdbcTypeBean) / fromGravitino(Type)
  converter/JdbcColumnDefaultValueConverter   <- Expression <-> SQL default
  converter/JdbcExceptionConverter            <- SQLException -> Gravitino exception (MySQL codes by default)
  utils/DataSourceUtils, JdbcConnectionTimeouts, JdbcConnectorUtils (quoting, escaping)
  config/JdbcConfig                           <- jdbc-url, jdbc-driver, jdbc.pool.*, verify-on-create
  common/.../JdbcUrlUtils                     <- unsafe URL parameter denylist
 +- mysql, postgresql, doris, starrocks       <- subclass the above per dialect
 +- contrib: clickhouse, hologres, oceanbase  <- same pattern (OceanBase and StarRocks and Doris reuse MySQL-protocol parts)

hive-metastore-common
  client/HiveClientFactory                    <- tries HIVE3 isolated classloader, falls back to HIVE2
  client/HiveClientClassLoader                <- URLClassLoader per Hive version; shared/barrier class lists
  client/HiveShim (+ hive-metastore2-libs/HiveShimV2, hive-metastore3-libs/HiveShimV3)
  converter/HiveDataTypeConverter, HiveTableConverter
  kerberos/HmsKerberosClient                  <- keytab login, proxy user, delegation token
  CachedClientPool / HiveClientPool           <- clients cached per principal
catalog-hive: HiveCatalogOperations, HiveViewCatalogOperations, TrinoNativeViewCodec, HiveCatalogCapability

catalog-glue: GlueCatalogOperations (AWS SDK for Hive-format), GlueIcebergTableHelper (Iceberg SDK GlueCatalog for Iceberg-format), GlueTypeConverter, GlueExceptionConverter
catalog-lakehouse-iceberg: IcebergCatalogOperations -> IcebergBackend -> IcebergCatalogWrapper (iceberg-common) with backends HIVE | JDBC | MEMORY | REST | CUSTOM
catalog-lakehouse-paimon: PaimonCatalogOperations; backends FILESYSTEM | JDBC | HIVE | REST
catalog-lakehouse-hudi: read-only; backend HMS only
catalog-lakehouse-generic: GenericCatalogOperations dispatches by table property `table-format` to
    LakehouseTableDelegator implementations found by ServiceLoader: LanceTableDelegator, DeltaTableDelegator
```

### 1.3 Extension points and configuration keys

- **Capability SPI** (`CORE/connector/capability/Capability.java`): `columnNotNull`, `columnDefaultValue`, `caseSensitiveOnName`, `normalizeName`, `specificationOnName`, `specificationOnExistingName`, `managedStorage`, `supportsHierarchicalSchema`. The API is small. Most "not supported" signalling bypasses it and happens at execution time as exceptions (see RL-R14).
- **Optional mixin** `SupportsTableNameResolution` (`CORE/connector/SupportsTableNameResolution.java`): maps a normalised table name to the stored name when normalisation is not reversible.
- **JDBC subclass hooks**: `generateCreateTableSql`, `generateAlterTableSql`, `getTablePartitioning`, `getDistributionInfo`, `correctJdbcTableFields`, `calculateDatetimePrecision`, `supportsTableSortOrder`, `validateAlterTableTarget`, `createSysDatabaseNameSet`, `supportSchemaComment` (`JC/operation/JdbcTableOperations.java`, `JdbcDatabaseOperations.java:240-250`).
- **Generic lakehouse SPI**: `LakehouseTableDelegator` (`GN/generic/LakehouseTableDelegator.java`) with `tableFormat()`, `tablePropertyEntries()`, `createTableOps()`.
- **Hive version shims**: `HiveShim` base (Hive 2 behaviour) and `HiveShimV3` override, loaded from separate jar directories (`HM/client/HiveClientClassLoader.java:111`).
- **Config keys that matter**: `jdbc-url`, `jdbc-driver`, `jdbc-user`, `jdbc-password`, `jdbc-database` (mandatory for PostgreSQL), `jdbc.pool.*`, `verify-on-create`, `gravitino.bypass.*` (DBCP passthrough); `metastore.uris`, `list-all-tables` (Hive); `default-table-format`, `table-format-filter`, `warehouse` (Glue); `catalog-backend` (Iceberg/Paimon); table property `table-format` (generic); Iceberg `format-version`.
- **Property typing**: every connector declares `PropertyEntry` records marked `reserved` (server-owned, read-only), `immutable` (create only), `hidden`, `required`. The same mechanism carries format-native read-only facts (Iceberg `current-snapshot-id`, `default-spec-id`).

### 1.4 Neighbours

- **Core server**: runs connector calls under a per-metalake or per-table lock. A comment says the connection probe "runs under the metalake write lock" (`JC/JdbcCatalogOperations.java:293`). Connectors depend on that serialisation, because the external systems offer no versioned commit (JDBC, Hive).
- **Authorization** (other agent's area): `resolveTableName` is applied before authorisation and before the operation so that the authorised identifier is the operated identifier.
- **Iceberg REST server** (other agent's area): reuses `iceberg-common` `IcebergCatalogWrapper`; the same backends and the same JDBC and Hive workarounds apply.
- **Credential vending** (other agent's area): `catalog-common` holds `storage/*Properties` and `credential/config/*`. Not read in depth here.

## 2. Candidate requirements

Kind: IMPL = Gravitino implements it. GAP = Gravitino fails or omits it and mudcat should do better. Rationale cites `file:line`; aliases are in the header table. All requirements are generalised; "source" means an external system whose objects mudcat registers.

### 2.1 Type mapping

| Ref | Kind | Requirement | Rationale / Gravitino evidence | Area | Ver |
|---|---|---|---|---|---|
| RL-R1 | IMPL | mudcat shall store a source type that has no common-model equivalent as an opaque type that holds the native type string. | Every dialect falls through to `Types.ExternalType.of(nativeName)`: `MY/converter/MysqlTypeConverter.java:105-106` (default), `PG/converter/PostgreSqlTypeConverter.java:158-159`, `CH/converter/ClickHouseTypeConverter.java:178-179`, `GL/GlueTypeConverter.java:196-197`. A table with one exotic column still loads. | ST | T |
| RL-R2 | IMPL | mudcat shall write an opaque type back to the same source without change of its native string. | `fromGravitino` returns `ExternalType.catalogString()` (`MysqlTypeConverter.java:165-166`); Postgres validates it as a single type declaration so it cannot inject DDL (`PostgreSqlTypeConverter.java:205-221`, regex at `:60-80`). | ST | T |
| RL-R3 | GAP | mudcat shall publish, per source and per native type, whether the mapping to the common type is lossless, lossy or opaque. | Lossiness is stated only in code comments, never as data: `ClickHouseTypeConverter.java:135` ("Lossy: Gravitino has no half-precision float"), `MysqlTypeConverter.java:177` ("DecimalType does not represent unsignedness"), `MysqlTableOperations.java:215`. No machine-readable matrix exists. | ST | I |
| RL-R4 | GAP | mudcat shall round-trip a type read from a source through a write to the same source, or report the type as opaque. | MySQL read maps `BIT(1)`->boolean, `BIT(n)`->binary, but write maps boolean->`BIT` (`MysqlTypeConverter.java:51-55`, `:163-164`); `VARBINARY/ENUM/SET/BINARY(n)` collapsed to bare types until a second `information_schema` query repairs them (`MysqlTableOperations.java:215-262`). Postgres `serial/bigserial` read as plain integer (`PostgreSqlTypeConverter.java:87-94`) and lose sequence ownership (inferred). | ST | T |
| RL-R5 | IMPL | mudcat shall report a numeric type whose precision or scale exceeds the common model's range as opaque with the native precision and scale. | MySQL decimal >38 (`MysqlTypeConverter.java:177-186`); Postgres unconstrained `NUMERIC`, precision >38, negative scale (`PostgreSqlTypeConverter.java:117-135`); ClickHouse Decimal up to 76 (`ClickHouseTypeConverter.java:152-157`). | ST | T |
| RL-R6 | IMPL | mudcat shall state the time-zone semantics and fractional-second precision of every temporal type in its type descriptor. | MySQL `TIMESTAMP` (UTC-converted) vs `DATETIME` (`MysqlTypeConverter.java:80-93`); Iceberg accepts only precision 6 and, from format v3, 9 (`IC/converter/ToIcebergType.java:142-170`); Hive has no zone (`HM/converter/HiveDataTypeConverter.java:97`). | ST | T |
| RL-R7 | IMPL | mudcat shall report a temporal precision as unknown when the source driver cannot report it reliably. | `OB/operation/OceanBaseTableOperations.java:669` returns null below MySQL driver 8.0.16; MySQL recovers it from `information_schema` (`MysqlTableOperations.java:215-262`); base default `JC/operation/JdbcTableOperations.java:926-932` returns null. | ST | T |
| RL-R8 | IMPL | mudcat shall reject a requested type that the target cannot represent before it contacts the target, and the message shall name the type and the target. | Hive tz timestamp (`HiveDataTypeConverter.java:97`), Glue unsigned (`GlueTypeConverter.java:205`), Iceberg byte/short (`ToIcebergType.java:118`), ClickHouse `varchar(n)` (`ClickHouseTypeConverter.java:214`), Postgres nested list (`PostgreSqlTypeConverter.java:238`). | ST | T |
| RL-R9 | GAP | mudcat shall reject, not ignore, a requested attribute that the target cannot honour. | ClickHouse logs a warning and drops `nullOrdering` (`CH/operations/ClickHouseTableOperations.java:548`); Postgres drops list `elementNullable` (`PostgreSqlTypeConverter.java:232`); Hive stores sort direction only (`HM/converter/HiveTableConverter.java:262-268`). Iceberg, by contrast, throws. Policy is inconsistent between connectors. | ST | T |

### 2.2 Names, identifiers, case

| Ref | Kind | Requirement | Rationale / Gravitino evidence | Area | Ver |
|---|---|---|---|---|---|
| RL-R10 | IMPL | mudcat shall validate the name of an existing source object with rules separate from the rules for a new name. | `Capability.specificationOnExistingName` (`CORE/connector/capability/Capability.java`, default delegates to `specificationOnName`); overridden in MySQL, Postgres, Doris, Iceberg so objects created by other tools stay addressable (`MY/MysqlCatalogCapability.java:44-53`, `IC/IcebergCatalogCapability.java:96-110`). | ST | T |
| RL-R11 | IMPL | mudcat shall state for each source and object kind whether names are case-sensitive, and shall normalise names before authorisation and before dispatch. | `Capability.caseSensitiveOnName`/`normalizeName`; Hive (`HV/HiveCatalogCapability.java:70`), Glue (`GL/GlueCatalogCapability.java:63`), Iceberg-on-Hive (`IC/IcebergCatalogCapability.java:67`), Paimon-on-Hive (`PM/PaimonCatalogCapability.java:48`), Hudi. The same logical catalog type changes case rules with its backend. | ST | T |
| RL-R12 | IMPL | mudcat shall resolve a normalised name to the stored name before authorisation, so that the authorised object is the operated object, and shall never resolve an ambiguous name. | `CORE/connector/SupportsTableNameResolution.java` contract (exact match, then unique case-insensitive match, else unchanged); hook `JC/operation/TableOperation.java:173` (default identity; no connector overrides it yet). | AC | T |
| RL-R13 | IMPL | mudcat shall measure the length limit of a source name in the unit the source uses. | Postgres limit is 63 UTF-8 **bytes**, not characters (`PG/PostgreSqlCatalogCapability.java:57-59`); MySQL 64 characters (`MY/MysqlCatalogCapability.java:42`). | ST | T |
| RL-R14 | IMPL | mudcat shall quote every identifier it places in a generated statement by doubling the delimiter, and shall reject NUL. | `JC/utils/JdbcConnectorUtils.java:79-85` (`quoteIdentifier`), `:103-122` (`escapeSqlLiteral`); NUL rejected `:44-45`, `PostgreSqlCatalogCapability.java:57`. | ST | T |
| RL-R15 | GAP | mudcat shall reject reserved words that are invalid as identifiers in a source dialect. | Default JDBC capability has `// TODO: Validate the name against reserved words` and uses a SQLite pattern "for testing purposes" (`JC/JdbcCatalogCapability.java:39-43`). | ST | T |
| RL-R16 | IMPL | mudcat shall exclude the source's own system schemas from listings and shall refuse to create a schema with a system name. | `createSysDatabaseNameSet()`: MySQL `information_schema, mysql, sys, performance_schema` (`MY/operation/MysqlDatabaseOperations.java:35`), Postgres `pg_toast, pg_catalog, information_schema` (`PG/operation/PostgreSqlSchemaOperations.java:248`), Doris and StarRocks `information_schema` only, OceanBase adds `oceanbase`, Hologres adds `hologres, hg_internal` (`HG/operation/HologresSchemaOperations.java:157-164`). The list differs per dialect and is hard-coded. | ST | T |
| RL-R17 | IMPL | mudcat shall escape pattern characters when it asks a source for an object by name through a pattern-based metadata API. | MySQL treats `a_b` as a wildcard and may return `A_B` (`JC/operation/JdbcTableOperations.java:309-316`); Postgres escapes `%`, `_` and the escape char (`PG/operation/PostgreSqlSchemaOperations.java:140-148`). | ST | T |

### 2.3 Signalling "unsupported"; alter semantics

| Ref | Kind | Requirement | Rationale / Gravitino evidence | Area | Ver |
|---|---|---|---|---|---|
| RL-R18 | GAP | mudcat shall expose, per source and per operation, a machine-readable capability that a client can read before it sends a request. | `Capability` declares only six facts (not-null, default, case, name rules, managed storage, hierarchy). All alter-matrix gaps in section 3.2 are discovered by throwing at execution time. | IN | I |
| RL-R19 | GAP | mudcat shall return one distinct error code for "the source cannot do this" and another for "the input is invalid". | The same condition throws three types: `UnsupportedOperationException` (PG purge `PostgreSqlTableOperations.java:403`), `IllegalArgumentException` (MySQL remove-property `MysqlTableOperations.java:375`, Doris rename-column `DorisTableOperations.java:952`, Hologres `HologresTableOperations.java:462`), and `Preconditions` messages (Iceberg index `IcebergCatalogOperations.java:573`). | IN | T |
| RL-R20 | IMPL | mudcat shall validate a whole multi-change request before it applies the first change. | Lance `validateLanceTableChanges` before opening the dataset (`GN/lance/LanceTableOperations.java:879-881`, `:924-960`); Iceberg `validateSchema` before commit (`IC/ops/IcebergCatalogWrapperHelper.java:231`); Doris rejects add+delete of one index in one request (`DorisTableOperations.java:1041-1051`). | ST | T |
| RL-R21 | IMPL | mudcat shall refuse a table rename that is combined with other changes in one request. | JDBC (`JC/JdbcCatalogOperations.java:628`), Iceberg (`IC/IcebergCatalogOperations.java:456`), Paimon (`PM/PaimonCatalogOperations.java:453`). All three hit the same impedance: rename is a catalog operation, not a schema update. | ST | T |
| RL-R22 | GAP | mudcat shall declare, per source, whether a multi-change alteration is atomic, and on partial failure shall report which changes were applied. | Iceberg uses one `Transaction` (atomic). Lance "can't guarantee the atomicity" (`LanceTableOperations.java:871`). OceanBase applies one clause at a time (`OceanBaseTableOperations.java:204-222`). Hive 3 drops and re-adds constraints around `alter_table` and, on failure, tells the operator to repair by hand (`catalogs/hive-metastore3-libs/src/main/java/org/apache/gravitino/hive/client/hive3/HiveShimV3.java:211-247`). No common contract exists. | ST | T |
| RL-R23 | IMPL | mudcat shall not label a source failure as retryable when the failed request may have been partly applied. | `JdbcExceptionConverter.java:105-107`: lock timeouts and deadlocks stay generic because "HTTP 503 can trigger automatic retries of DDL requests that have already partially committed". Glue throttling is 503, concurrent modification is an optimistic-lock error (`GL/GlueExceptionConverter.java:259-272`). | OP | T |
| RL-R24 | IMPL | mudcat shall report a commit whose outcome is unknown as unknown and shall not report it as failed. | `GlueExceptionConverter.java:363-376` maps `CommitStateUnknownException` to "the change may have been applied; check the table before retrying". | OP | T |
| RL-R25 | GAP | mudcat shall classify source errors by structured codes (error code, SQLSTATE) and not by message text. | Mixed practice: MySQL numeric codes (`JdbcExceptionConverter.java:86-96`), Postgres SQLSTATE (`PostgreSqlExceptionConverter`), but `getMessage().contains("Access denied")` (`:102`) and Hive fallback on `"Invalid method name: 'get_catalogs'"` (`HM/client/HiveClientFactory.java:144`). | OP | T |

### 2.4 Identity and drift of registered external objects

| Ref | Kind | Requirement | Rationale / Gravitino evidence | Area | Ver |
|---|---|---|---|---|---|
| RL-R26 | GAP | mudcat shall keep the identity of a registered external object in its own database and shall not need to write a marker into the external object. | Gravitino writes `From Gravitino, DO NOT EDIT: (gravitino.v1.uid<n>)` into the object's comment (`CORE/StringIdentifier.java:47`; JDBC `JC/JdbcCatalogOperations.java:706`) or into HMS parameter `gravitino.identifier` (`HM/client/HiveShim.java:221`). If someone edits the comment the id is lost and the connector only logs a warning (`JC/JdbcCatalogOperations.java:566`, `:491`). | ID | I |
| RL-R27 | GAP | mudcat shall not store its own metadata in a user-visible field of an external object, unless an operator turns that on per source. | ClickHouse keeps the cluster name in the comment because `SHOW CREATE` omits `ON CLUSTER` (`CH/operations/ClickHouseClusterUtils.java:29-45`); every comment alteration must re-embed both id and cluster (`ClickHouseTableOperations.java:1286-1299`). Doris keeps schema comment in properties (`DO/operation/DorisDatabaseOperations.java:57`). | ST | I |
| RL-R28 | IMPL | mudcat shall record, for each registered external table, the source version at which its schema was captured. | Lance stores `lance.version` and refreshes columns when the dataset version differs (`GN/lance/LanceTableOperations.java:112-141`). | ST | T |
| RL-R29 | GAP | mudcat shall check the schema given at registration of an external table against the source, or shall mark the schema as unverified. | Delta: "Schema is provided by user in CREATE TABLE request (not read from Delta log)"; "User is responsible for ensuring schema accuracy" (`GN/delta/DeltaTableDelegator.java:44`, `DeltaTableOperations.java` header). Partitioning is likewise "metadata only". | ST | T |
| RL-R30 | IMPL | mudcat shall delete only the record, and never the data, when a registered external object is dropped, unless the registration granted data ownership. | Delta and external Lance drop removes metadata only; purge of external tables is refused (`LanceTableOperations.java:315-317`; Hive `HV/HiveCatalogOperations.java:1069`; Delta has no purge). | UP | T |
| RL-R31 | GAP | mudcat shall write the record of deletion before it removes the bytes, and shall record a failed byte deletion for retry. | Lance purge is "NOT atomic": metadata removal and dataset deletion can leave orphaned data, or the reverse (`LanceTableOperations.java:322-336`, `:362-374`). Iceberg purge was rewritten to read metadata first, drop the entry, then delete reachable files, because the library gave up on the first unreadable file (`ICC/ops/IcebergCatalogWrapper.java:329-346`). | UP | T |
| RL-R32 | GAP | mudcat shall not change catalog state in a read request, except an idempotent repair that uses a conditional update with a bounded retry count. | Lance `loadTable` rewrites columns from the dataset; loser of the version compare-and-swap retries up to 5 times with 10-100 ms randomised backoff (`LanceTableOperations.java:97-107`, `:685-700`) and finally raises an optimistic-lock error. Works, but a read can fail with a write conflict. | OP | T |
| RL-R33 | IMPL | mudcat shall refuse an alteration of a registered table whose stored schema has not been confirmed against the source. | `LanceTableOperations.java:289-294`: "Cannot alter Lance table ... because its dataset schema is not initialized or could not be loaded". | ST | T |

### 2.5 Multi-format namespaces, table-format features, read-only sources

| Ref | Kind | Requirement | Rationale / Gravitino evidence | Area | Ver |
|---|---|---|---|---|---|
| RL-R34 | GAP | mudcat shall record the table format of a registered table explicitly, and shall not infer it on each listing from engine-specific parameters. | Hive catalog removes Iceberg and Paimon tables with a metastore filter on `table_type` and Hudi tables on `provider`, and admits "Spark-managed Hudi tables that only expose `spark.sql.sources.provider=hudi` cannot be filtered here" (`HV/HiveCatalogOperations.java:420-446`). Hudi catalog detects tables by input-format class prefix (`HU/backend/hms/HudiHMSBackendOps.java:150`, `:214`). Glue reads `table_type` (`GL/GlueTable.java:194-199`). | ST | T |
| RL-R35 | IMPL | mudcat shall let an operator limit which table formats a shared-metastore source exposes. | Glue `table-format-filter` (`all, hive, iceberg, delta, parquet`) hides other formats from list, load, alter, drop and create (`GL/GlueCatalogPropertiesMetadata.java:63`, `:150-153`); Hive `list-all-tables` default false (`HV/HiveCatalogPropertiesMetadata.java:53`). | PL | T |
| RL-R36 | IMPL | mudcat shall write the default of every defaulted format option into the stored record at creation. | Iceberg: "Gravitino owns this default rather than deferring to the Iceberg library's own version-dependent default, and stamps it" (`IC/IcebergTablePropertiesMetadata.java:52-58`); allowed set 1-4 (`:68`). | ST | T |
| RL-R37 | IMPL | mudcat shall show format-native facts that have no common-model field as read-only properties in a format-specific namespace. | Iceberg `current-snapshot-id`, `default-spec-id`, `sort-order`, `identifier-fields`, `format-version`, `location` (`IC/utils/IcebergTablePropertiesUtil.java:44-67`); property entries are `reserved` (`IcebergTablePropertiesMetadata.java:78-100`). | ST | I |
| RL-R38 | GAP | mudcat shall provide a way to read and change format-native structure that the common model lacks (partition-spec history, snapshot refs, sort-order evolution). | The Iceberg connector accepts only schema and property changes (`IC/ops/IcebergCatalogWrapperHelper.java:230-279`); partition spec is create-only and the current spec id is the only evolution evidence exposed. Snapshots, branches and tags are not surfaced at all. | ST | I |
| RL-R39 | GAP | mudcat shall load a registered table that contains a partition transform, index type or part that the common model cannot represent, and shall mark that part as opaque. | Iceberg: void transform silently omitted (`IC/converter/FromIcebergPartitionSpec.java:86-88`); unknown transform throws and the table cannot be loaded (`:93`). Doris: native `NGRAM_BF` index throws during load (`DO/operation/DorisTableOperations.java:757`). ClickHouse instead omits unrepresentable legacy indexes with a log line (`CH/operations/ClickHouseTableOperations.java:2093-2115`). Three policies for one problem. | ST | T |
| RL-R40 | IMPL | mudcat shall mark each registered source as read-only or read-write and shall refuse a write to a read-only source before dispatch. | Hudi is read-only: create, alter, drop and schema ops throw "Not implemented yet" (`HU/backend/hms/HudiHMSBackendOps.java:124-202`). Delta has no alter or purge. Today this is expressed only by exceptions (see RL-R18). | IN | T |
| RL-R41 | IMPL | mudcat shall register a table format through a plug-in that supplies the format name, its property definitions and its operations. | `LakehouseTableDelegator` loaded by `ServiceLoader` (`GN/generic/LakehouseTableDelegatorFactory.java:44`); `table-format` is a required, immutable table property (`GenericTablePropertiesMetadata.java:50-54`). | IN | I |
| RL-R42 | IMPL | mudcat shall store a view's SQL text per dialect and shall not translate between dialects. | Design doc: "Gravitino will not automatically convert SQL between dialects" (`design-docs/gravitino-logical-view-management.md:59`). HMS holds one dialect per view, and Trino's "Presto View" payload is base64 JSON in `viewOriginalText` with `presto_view=true` (`HV/TrinoNativeViewCodec.java:51`; `HiveViewCatalogOperations.java:284-311`). `HiveCatalogOperations.java:1191` has an open TODO for Spark and Trino formats. | ST | I |

### 2.6 Connections, drivers, security, versions

| Ref | Kind | Requirement | Rationale / Gravitino evidence | Area | Ver |
|---|---|---|---|---|---|
| RL-R43 | IMPL | mudcat shall reject a connector configuration property that names a class to load or SQL to run at connect time. | `UNSAFE_POOL_PROPERTIES` (`connectionFactoryClassName`, `evictionPolicyClassName`, `driverClassName`, `url`, `username`, `password`, `initialSize`) (`JC/utils/DataSourceUtils.java:58-66`); H2 URL and driver barred because `INIT` runs arbitrary SQL (`:99`); `JdbcUrlUtils` denylist of MySQL and Postgres parameters such as `autoDeserialize`, `socketFactory` (`common/.../utils/JdbcUrlUtils.java:66-84`). | AC | T |
| RL-R44 | IMPL | mudcat shall not accept credentials inside a connection URL and shall not print the URL, or any part that can hold a credential, in an error. | `JdbcConfig.java:168` rejects credentials in `jdbc-url`; `hostPortFromJdbcUrl` returns `"unknown"` for forms like `(host=..,password=..)` (`JC/JdbcCatalogOperations.java:409` and its comment). | AC | T |
| RL-R45 | IMPL | mudcat shall bound the connect time and the read time of every outbound connection that it opens while it holds a lock that other requests need. | Create-time probe uses a one-off connection with both timeouts because it "runs under the metalake write lock" (`JC/JdbcCatalogOperations.java:290-298`); parameter names differ per driver family: `connectTimeout`/`socketTimeout`, PostgreSQL `loginTimeout` in whole seconds capped to avoid integer overflow, ClickHouse `connect_timeout`/`socket_timeout` (`JC/utils/JdbcConnectionTimeouts.java:101-114`, `:205-216`). | OP | T |
| RL-R46 | IMPL | mudcat shall let an operator register a source that is temporarily unreachable by skipping the reachability probe explicitly. | `verify-on-create=false` (`JC/config/JdbcConfig.java:157`; `docs/jdbc-catalog-connection-validation.md`). Driver/URL compatibility is still checked. | OP | D |
| RL-R47 | IMPL | mudcat shall load each source client library set in its own class loader and shall release the loader and its registered drivers on unregister. | Hive 2 and Hive 3 client jars in separate directories with a shared/barrier class list (`HM/client/HiveClientClassLoader.java:111`, `:235-260`); `JdbcDriverDeregisterer` defines a copy of itself inside the catalog loader because `DriverManager` filters by caller loader (`catalog-common/.../utils/JdbcDriverDeregisterer.java:19-30`). | OP | T |
| RL-R48 | GAP | mudcat shall detect the protocol version of a source with an explicit handshake and shall cache the result per source. | Hive: try Hive 3 first, then match the error text `Invalid method name: 'get_catalogs'` or `class not found` to fall back to Hive 2 (`HM/client/HiveClientFactory.java:126-148`). Doris: runtime `SELECT Version FROM FRONTENDS()` with `SHOW FRONTENDS` fallback, and `UnsupportedOperationException` if neither works (`DO/operation/DorisTableOperations.java:367-410`). | PL | T |
| RL-R49 | IMPL | mudcat shall derive the capability of a source from its detected version. | `HiveCatalogCapability.columnNotNull/columnDefaultValue` are supported only when the metastore is Hive 3 (`HV/HiveCatalogCapability.java:51-58`); Doris AUTO_INCREMENT needs 2.1.0 (`DorisTableOperations.java:331`). | PL | T |
| RL-R50 | IMPL | mudcat shall use the source's own conditional-update token when it has one. | Glue `UpdateTable` replaces the whole table, so the connector sends the `versionId` it read; `ConcurrentModificationException` becomes an optimistic-lock error (`GL/GlueCatalogOperations.java:661-664`, `GlueExceptionConverter.java:266`). | OP | T |
| RL-R51 | GAP | mudcat shall serialise changes to one external object when the source has no conditional-update token. | Hive: `// TODO(@Minghuang): require a table lock to avoid race condition` (`HV/HiveCatalogOperations.java:716`). JDBC alters read the table, then build `MODIFY COLUMN` clauses that restate the whole column from that snapshot (`MysqlTableOperations.java:593-700`); a concurrent change by another tool is overwritten (inferred). | OP | T |
| RL-R52 | IMPL | mudcat shall act on a source as the requesting principal when the source supports delegation, and as a service identity otherwise. | Hive: Kerberos login with keytab, proxy user per request, delegation token (`HM/client/HiveClientFactory.java:200-225`, `HM/kerberos/HmsKerberosClient.java:101-108`); Iceberg proxy `KerberosAwareIcebergCatalogProxy`. JDBC uses one pool and one service account. | AC | D |
| RL-R53 | IMPL | mudcat shall generate each source statement before it takes a pooled connection to execute it. | `JC/operation/JdbcTableOperations.java:419-423`: generators may load the table and borrow another connection; holding both let concurrent alters "exhaust the pool and wait for each other until the borrow timeout". | PF | T |
| RL-R54 | GAP | mudcat shall bound the size of every value it writes to a source whose limit is lower than the common model's limit. | Iceberg JDBC backend caps namespace property values at 1000 characters, enforced by the connector itself (`ICC/ops/IcebergCatalogWrapper.java:116`, `:1017-1031`). No general limit table exists. | ST | T |

## 3. Quirks and gotchas ledger

### 3.1 Identity, metadata smuggling, drift

- **Identity written into the external object's comment**
  - What happens: after create, JDBC tables and schemas carry a suffix `From Gravitino, DO NOT EDIT: (gravitino.v1.uid<n>)` in their native comment. `loadTable` strips it. If a DBA edits the comment, `loadTable` logs a warning and the table has no Gravitino id.
  - Cause: MODEL (no id field in the source).
  - Evidence: `CORE/StringIdentifier.java:47` `STRING_COMMENT = "From Gravitino, DO NOT EDIT: "`; `JC/JdbcCatalogOperations.java:566` `"The table {} comment {} does not contain Gravitino id attribute"`.
  - Mudcat implication: RL-R26. Keep identity in mudcat's PostgreSQL row. If a marker is ever needed, make it opt-in and verify by content hash, not by comment text.
- **Alter comment must preserve the marker**
  - What happens: MySQL, ClickHouse and others re-add the id when the user supplies a new comment without it; otherwise identity is lost silently.
  - Cause: MODEL.
  - Evidence: `CH/operations/ClickHouseTableOperations.java:1291-1296` `if (null == StringIdentifier.fromComment(newComment)) {... StringIdentifier.addToComment(identifier, newComment)`; `MY/operation/MysqlTableOperations.java` ("Last modified comment" block).
  - Mudcat implication: RL-R26, RL-R27.
- **Cluster topology stored in the comment**
  - What happens: ClickHouse does not persist `ON CLUSTER` anywhere queryable for non-Replicated objects, so the connector writes `[Gravitino] ch.cluster=<name>` on a second comment line and strips it on read.
  - Cause: SPEC (ClickHouse).
  - Evidence: `CH/operations/ClickHouseClusterUtils.java:29-31` "does not persist `ON CLUSTER` information in any queryable system table".
  - Mudcat implication: RL-R27. Source facts that cannot be read back must be recorded in mudcat's own store.
- **Declared schema vs actual schema (drift)**
  - What happens: Lance tables have a stored column list in Gravitino and a real schema in the dataset. Modes `DECLARED_AND_EMPTY` (default) and `VERSION_CHECK` decide when to refresh. A zero-column dataset is recorded with `lance.version` as a "confirmed empty" marker. Stale columns were once possible when the schema was cleared outside Gravitino.
  - Cause: MODEL/CONCURRENCY.
  - Evidence: `GN/lance/LanceTableOperations.java:112-141`; `:737-741` "Always use an empty column list ... causing future VERSION_CHECK loads to return stale metadata permanently".
  - Mudcat implication: RL-R28, RL-R32, RL-R33.
- **Delta registration trusts the caller**
  - What happens: the Delta connector requires `external=true` and `location`, stores the user's columns and identity partitions, and never reads the Delta log. Non-identity transforms, distribution, sort orders and indexes are refused.
  - Cause: MODEL.
  - Evidence: `GN/delta/DeltaTableOperations.java:174-176` `"Gravitino only supports creating external Delta tables for now"`; `:192` `"Delta table only supports identity partitioning."`
  - Mudcat implication: RL-R29. A catalog that lets the registrant assert the schema needs a "verified/unverified" flag.
- **Lance staged create**
  - What happens: Spark's staged-create flow makes a "declared" table with no schema; the schema arrives later and is pulled from the dataset on load. `EXIST_OK` cannot combine with `register`.
  - Cause: ENGINE (Spark).
  - Evidence: `GN/lance/LanceTableOperations.java:116-119`, `:226`.
  - Mudcat implication: a registered entity needs a "declared, not yet materialised" state (RL-R33).
- **Purge is two non-atomic steps**
  - What happens: metadata removal then dataset deletion; a crash between them gives orphan data or a dangling record. External Lance tables cannot be purged by default.
  - Cause: CONCURRENCY.
  - Evidence: `GN/lance/LanceTableOperations.java:322` `// NOTE: the two steps below ... are NOT atomic.`; `:315-317`.
  - Mudcat implication: RL-R30, RL-R31.
- **Iceberg purge stopped at the first unreadable file**
  - What happens: Iceberg drops the catalog entry first and then deletes files, giving up at the first missing manifest. The request then failed although the table was gone and no request remained that could delete the files.
  - Cause: SPEC (Iceberg library behaviour).
  - Evidence: `ICC/ops/IcebergCatalogWrapper.java:329-346`.
  - Mudcat implication: RL-R31. Delete bytes from a recorded list, tolerate partial failure, and record what remains.

### 3.2 ALTER support matrix and the odd operations

Legend: Y supported; N throws; P partial. Source is the throw site in the code.

| Operation | MySQL | Postgres | Doris | StarRocks | ClickHouse | Hologres | OceanBase | Hive | Iceberg | Paimon | Lance | Delta/Hudi |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| Rename table | Y (alone) | Y (alone) | Y (alone) | Y | Y (alone) | Y (alone) | Y (alone) | Y (also move schema) | Y (alone) | Y (alone) | N (inferred) | N |
| Rename column | Y (`CHANGE COLUMN`) | Y | N | Y | Y | Y | Y | Y | Y | Y | Y | N |
| Change type | Y (`MODIFY`) | Y (`SET DATA TYPE`, keeps collation) | Y (version-aware) | Y | Y | N | Y | Y | P (primitive promotion only) | Y | N | N |
| Column default | Y | Y | N (no handler; falls to "Unsupported table change type") | N (no handler) | Y | N | Y | Hive 3 only | N (capability) | N (via table properties) | N | N |
| Nullability | Y | Y | P (only NOT NULL -> NULL) | N (no handler) | Y | N | Y | Hive 3 only | P (make-required TODO) | Y | N | N |
| Column position | Y | N | Y | Y | Y | N | Y | P (not after partition columns) | Y | P (no default position) | N | N |
| Auto-increment | Y (add/alter) | N | N when adding; no alter handler | N when adding | N (version-gated) | N | Y | N | N | N | N | N |
| Set property | Y (only `engine`, `auto_increment`) | N | P | P | P (settings alone) | N | Y | Y | Y (non-reserved) | Y | via metadata | N |
| Remove property | N | N | N | N | P (`RESET SETTING`, alone) | N | N | Y | Y (non-reserved) | Y | via metadata | N |
| Add/drop index | Y (PK must be named `PRIMARY`) | Y (constraints only) | P (no PK/UK add) | N (no indexes at all) | P (no PK add) | N | Y | N (no indexes) | N | P (one PK only, create-time) | add index only | N |
| Partition change | N | N | via partition ops | via partition ops | N | N | N | via partition ops | N (create only) | N | N | N |
| Purge | N | N | N | N | N | N | N | Y (managed only) | Y | Y | Y (managed only) | N |
| Alter atomic | n/a | transaction (inferred) | one statement | one statement | one statement | one statement | **no** (per change) | HMS single call, constraints not | **yes** (transaction) | per SDK call | **no** | n/a |

Rename table is handled in the common JDBC path (`JC/JdbcCatalogOperations.java:610-630`, `:750-758`) and, for StarRocks, also inside its alter loop (`SR/operations/StarRocksTableOperations.java:163`). StarRocks has no handler for column comment, default or nullability (`StarRocksTableOperations.java:146-190`). Evidence for cells (all `file:line`): MySQL remove-property `MY/operation/MysqlTableOperations.java:375`; Postgres set/remove property and position `PG/operation/PostgreSqlTableOperations.java:418-421`, `:444`; Postgres auto-increment `:349`, `:793`; Doris rename column `DO/operation/DorisTableOperations.java:952`; Doris nullability `:1084`; Doris add auto-increment `:1140-1146`; Doris PK/UK add `:1398`; StarRocks indexes `SR/operations/StarRocksTableOperations.java:104`; StarRocks auto-increment `:513`; Doris has no `UpdateColumnDefaultValue` or `UpdateColumnAutoIncrement` branch (`DorisTableOperations.java:939-1000`); ClickHouse add PK `CH/operations/ClickHouseTableOperations.java:1377`, auto-increment `:1272-1276` ("added in ClickHouse 25.1. Since this PR is based on 23.x, we throw unsupported"), settings `:1481-1485`; Hologres `HG/operation/HologresTableOperations.java:459-487`, `:570-578`; OceanBase `OB/operation/OceanBaseTableOperations.java:204-250`; Hive partition-column and position limits `HV/HiveCatalogOperations.java:552-580`; Hive indexes `:646`; Iceberg default/nullability TODOs `IC/ops/IcebergCatalogWrapperHelper.java:103`, `:172`; Paimon `PM/utils/TableOpsUtils.java:108`, `:149`; Lance `GN/lance/LanceTableOperations.java:878-930`.

- **Rename column is not a universal operation**
  - What happens: Doris throws; MySQL needs `CHANGE COLUMN` with the whole column definition restated; Iceberg and Paimon rename by id and keep history; Hologres supports it but not type change.
  - Cause: SPEC (dialects).
  - Evidence: `DO/operation/DorisTableOperations.java:952` `"Rename column is not supported yet"`; `MY/operation/MysqlTableOperations.java:655-675`.
  - Mudcat implication: RL-R18. Capabilities per operation, not per catalog.
- **"Modify column" restates the whole column**
  - What happens: In MySQL every column alteration (comment, nullability, default, position, auto-increment) re-sends type, default, nullability, comment and auto-increment from a table snapshot. Pending changes in the same request must be merged (`withPendingDefaultValueAndNullability`).
  - Cause: SPEC (MySQL `MODIFY COLUMN`).
  - Evidence: `MY/operation/MysqlTableOperations.java:593-612`, `:703-760`.
  - Mudcat implication: RL-R51. This is a lost-update hazard against concurrent out-of-band changes (inferred).
- **PostgreSQL has no column position and no table-option alteration**
  - What happens: two separate exceptions say position is unsupported (`"does not support column position."` and `"... in Gravitino."`); set-property throws `"not supported yet"`. Comment alteration uses `COMMENT ON` statements, not `ALTER`.
  - Cause: SPEC.
  - Evidence: `PG/operation/PostgreSqlTableOperations.java:418`, `:444`, `:817`.
  - Mudcat implication: RL-R19.
- **Type change keeps the collation**
  - What happens: Postgres `SET DATA TYPE` resets the column collation; the connector re-reads the explicit collation and re-applies it for text targets, and omits it for non-text targets because Postgres rejects `COLLATE` there.
  - Cause: SPEC.
  - Evidence: `PG/operation/PostgreSqlTableOperations.java:684-698`.
  - Mudcat implication: a "change type" operation touches hidden column properties. Specify which are preserved.
- **Standalone unique index is not a constraint**
  - What happens: Postgres "delete index" through the table API refuses indexes that are not table constraints (including `IF EXISTS`), to avoid dropping a schema-scoped index of the same name.
  - Cause: SPEC / MODEL.
  - Evidence: `PG/operation/PostgreSqlTableOperations.java:549-560`.
  - Mudcat implication: the index model must say whether an index belongs to the table or the schema.
- **Doris and ClickHouse behave differently across versions**
  - What happens: Doris 2.1.5+ and 3.0.1+ reject `ADD COLUMN ... AUTO_INCREMENT`, earlier ones accept it but never generate values; Doris 4.0.6 removed `USING BITMAP`, so `BITMAP` is mapped to `INVERTED`; Doris 1.2.x has no `Index_type`, so the primary key is inferred from the index name `PRIMARY`; `BUCKETS AUTO` needs 1.2.2+; ClickHouse projections need 24.9+ (`system.projections`).
  - Cause: SPEC (server version).
  - Evidence: `DO/operation/DorisTableOperations.java:1140-1142`, `:749`, `:727-731`; `DO/converter/DorisExceptionConverter.java:129`; `CH/operations/ClickHouseTableOperations.java:1043-1044`.
  - Mudcat implication: RL-R48, RL-R49.
- **Dialect-specific default rendering**
  - What happens: MySQL needs `CURRENT_TIMESTAMP(6)` when the column has precision 6; Doris 2.1+ re-parses string defaults as double-quoted literals; default function names are checked against a regex before they are placed in SQL; binary defaults accept `0x...`.
  - Cause: SPEC.
  - Evidence: `MY/operation/MysqlTableOperations.java:467-478`; `DO/converter/DorisColumnDefaultValueConverter.java:103-110`; `JC/converter/JdbcColumnDefaultValueConverter.java:42-43`, `:60-66`.
  - Mudcat implication: store defaults as typed expressions, render per dialect, and test round trips of escapes.
- **Hologres is nearly append-only for schema**
  - What happens: Hologres (PostgreSQL-compatible) refuses change type, default, nullability, index add/delete, auto-increment, position, and any `NOT NULL`/default on `ADD COLUMN`.
  - Cause: SPEC.
  - Evidence: `HG/operation/HologresTableOperations.java:459-487`, `:570-578`.
  - Mudcat implication: "PostgreSQL-compatible" is not a capability profile; RL-R18.
- **OceanBase runs changes one by one**
  - What happens: because OceanBase "does not support some multiple changes in one statement", the connector overrides `alterTable` to run each change separately, regenerating SQL after each so it sees the updated table.
  - Cause: SPEC.
  - Evidence: `OB/operation/OceanBaseTableOperations.java:204-222`.
  - Mudcat implication: RL-R22.
- **ClickHouse alters are engine-specific**
  - What happens: `ORDER BY` is mandatory for MergeTree-family engines; partitioning only for MergeTree; table-setting changes cannot be mixed with other changes, and set/reset cannot be mixed with each other.
  - Cause: SPEC.
  - Evidence: `CH/operations/ClickHouseTableOperations.java:375-376`, `:530`, `:655`, `:1481-1485`.
  - Mudcat implication: table properties can change the legal set of other operations.

### 3.3 Type system and dialect quirks

- **MySQL `BIT` and boolean are not symmetric**
  - What happens: read `BIT(1)`/null size -> boolean; `BIT(n>1)` -> binary; write boolean -> `BIT`. `BINARY(n)` of any width reads as plain binary until corrected.
  - Cause: MODEL.
  - Evidence: `MY/converter/MysqlTypeConverter.java:51-55`, `:163-164`; `MY/operation/MysqlTableOperations.java:215`.
  - Mudcat implication: RL-R4.
- **Unsigned integers are first-class in MySQL and ClickHouse, absent in Glue/Hive/Iceberg**
  - What happens: Gravitino has signed and unsigned numeric types; MySQL and ClickHouse use them; Glue rejects unsigned on write; Iceberg rejects byte and short.
  - Cause: MODEL.
  - Evidence: `MysqlTypeConverter.java:57-71`; `GL/GlueTypeConverter.java:205`; `IC/converter/ToIcebergType.java:118`.
  - Mudcat implication: RL-R8. A common type system should say what widening a connector may apply, and require the caller to ask for it.
- **PostgreSQL numeric corner cases**
  - What happens: unconstrained `NUMERIC` has no precision; scale is stored as a signed 11-bit value, but the driver reports the unsigned form (`2045` for scale -3); arrays of numeric report a display width (131089) as precision.
  - Cause: SPEC + driver bug.
  - Evidence: `PG/converter/PostgreSqlTypeConverter.java:117-135`, `:251-256`.
  - Mudcat implication: RL-R5; read type metadata from catalog tables when the driver is wrong.
- **PostgreSQL arrays**
  - What happens: only one dimension is legal; element nullability cannot be declared; array type names start with `_`.
  - Cause: SPEC.
  - Evidence: `PG/converter/PostgreSqlTypeConverter.java:225-238`, `:240-265`.
  - Mudcat implication: RL-R9.
- **`bpchar` and `varchar` without length**
  - What happens: unbounded `varchar` maps to string; unbounded `bpchar` stays opaque because it ignores trailing spaces and is not equal to string.
  - Cause: SPEC.
  - Evidence: `PG/converter/PostgreSqlTypeConverter.java:138-150`.
  - Mudcat implication: semantic equivalence, not name similarity, decides the mapping.
- **ClickHouse type surface**
  - What happens: `DateTime` -> timestamp precision 0; `DateTime64(n)` for more; `Date32`, `IPv4`, `IPv6`, `BFloat16`, `Int128/256`, enums, wide decimals -> opaque; `varchar(n)` rejected.
  - Cause: SPEC.
  - Evidence: `CH/converter/ClickHouseTypeConverter.java:124-176`, `:214`.
  - Mudcat implication: RL-R1, RL-R8.
- **Hive type gaps**
  - What happens: no time zone on timestamp; struct fields cannot carry comments (HIVE-26593); NOT NULL and DEFAULT only since Hive 3, and stored as separate constraint objects.
  - Cause: SPEC.
  - Evidence: `HM/converter/HiveDataTypeConverter.java:97`, `:122-127`; `HV/HiveCatalogCapability.java:51-58`.
  - Mudcat implication: RL-R8, RL-R49.
- **Glue stores some Iceberg types as strings**
  - What happens: Glue's own column list is lossy for Iceberg tables (`TIME` stored as string); the connector replaces columns with ones derived from the Iceberg schema, which is authoritative.
  - Cause: MODEL.
  - Evidence: `GL/GlueTable.java:306-307`; `GlueIcebergTableHelper.java:100-108` (delegates to Iceberg SDK `GlueCatalog`).
  - Mudcat implication: when a registry and a format carry the same schema, mudcat must name which one wins (the format file) and re-derive.
- **Glue Hive-format columns are lower-cased and may collide**
  - What happens: Glue folds names; the connector lower-cases new column names and rejects tables whose column names collide after folding. Iceberg-format columns keep case.
  - Cause: SPEC.
  - Evidence: `GL/GlueCatalogOperations.java:864`, `:919`, `:1309-1316`.
  - Mudcat implication: RL-R11; case rule can depend on the format of the object, not just the catalog.
- **Iceberg field ids are assigned by the connector**
  - What happens: on create, ids come from ordinals in the root struct and a running counter for nested fields; the common `Column` carries no id.
  - Cause: MODEL.
  - Evidence: `IC/converter/ToIcebergType.java:50-66`.
  - Mudcat implication: for formats with column identity (Iceberg, Parquet field ids, Lance), keep the source column id next to the name (inferred need; Gravitino has no field for it).
- **Iceberg defaults and required columns**
  - What happens: column defaults are "WIP" and declared unsupported; adding a required column or making a column required is a TODO because it needs `allowIncompatibleChanges`.
  - Cause: SPEC.
  - Evidence: `IC/IcebergCatalogCapability.java:52`; `IC/ops/IcebergCatalogWrapperHelper.java:103`, `:172`.
  - Mudcat implication: RL-R18.
- **Iceberg distribution mode is not the common distribution**
  - What happens: common `NONE` means "client did not say", so the connector writes `write.distribution-mode`; `HASH` must have no expressions and requires partitioning; `RANGE` requires partitioning or sort order.
  - Cause: MODEL.
  - Evidence: `IC/IcebergTable.java:130-142`; `IC/IcebergCatalogOperations.java:598`.
  - Mudcat implication: separate "unspecified" from "none".
- **Iceberg partition transforms**
  - What happens: identity, bucket, truncate, year, month, day, hour map. Bucket takes one field. Void is dropped on read. Unknown throws.
  - Cause: MODEL.
  - Evidence: `IC/converter/ToIcebergPartitionSpec.java:60-82`; `FromIcebergPartitionSpec.java:86-93`.
  - Mudcat implication: RL-R39.
- **Hive partitions are identity-only and positional**
  - What happens: partition columns must be the last columns, in order; only identity transforms; sort columns keep direction only; bucketing is the only distribution.
  - Cause: SPEC.
  - Evidence: `HV/HiveCatalogOperations.java:523-535`; `HM/converter/HiveTableConverter.java:262-277`.
  - Mudcat implication: partition spec needs a per-format validity check at registration.
- **Paimon limits**
  - What happens: no sort orders; one index, and it must be a primary key; only HASH distribution (bucket number positive or `AUTO`); defaults only through table properties; no nested column updates; default position refused for position updates.
  - Cause: SPEC.
  - Evidence: `PM/PaimonCatalogOperations.java:379`, `:603`, `:613`; `PM/PaimonCatalogCapability.java:41`; `PM/utils/TableOpsUtils.java:108`, `:149`.
  - Mudcat implication: RL-R18.

### 3.4 Names, case, schemas

- **Same type, different case rule per backend**
  - What happens: Iceberg and Paimon are case-sensitive on object stores and REST but case-insensitive when backed by Hive; Hudi is always insensitive; the connector gets a `hiveBackend` flag.
  - Cause: SPEC (backend).
  - Evidence: `IC/IcebergCatalogCapability.java:65-68`; `PM/PaimonCatalogCapability.java:46-49`; `HU/HudiCatalogCapability.java:31-37`.
  - Mudcat implication: RL-R11. A registered table's name-matching rule is a property of the registration, not of the format.
- **Normalisation is not reversible**
  - What happens: the server lower-cases a name when a catalog is case-insensitive. A source that also keeps case-sensitive quoted names can then hold a table that listing returns but load cannot reach. `resolveTableName` was added for that case; the JDBC base returns the name unchanged and no dialect uses the hook yet.
  - Cause: SPEC / MODEL.
  - Evidence: `CORE/connector/SupportsTableNameResolution.java` (contract); `JC/operation/TableOperation.java:173`.
  - Mudcat implication: RL-R12. Keep the original spelling and a normalised key; never decide authorisation on one and operate on the other.
- **Existing names can be invalid for creation**
  - What happens: tables made by other tools can contain characters Gravitino would refuse. Dots and NUL are still refused, because Gravitino cannot express a dot in a name; column names may contain dots.
  - Cause: MODEL.
  - Evidence: `JC/utils/JdbcConnectorUtils.java:35-65`.
  - Mudcat implication: RL-R10. Names in the model must allow what sources allow, or registration must refuse with a clear reason.
- **Hierarchical namespaces**
  - What happens: Iceberg namespaces nest; Gravitino schemas are one level, so a logical name is joined with a configurable separator (`A:B:C`) and re-split; empty segments are refused. JDBC, Hive: flat.
  - Cause: MODEL.
  - Evidence: `IC/IcebergCatalogCapability.java:58`, `:117-134`; `IC/ops/IcebergCatalogWrapperHelper.java:303-320`.
  - Mudcat implication: decide early whether collection hierarchy depth is unbounded.
- **Schema operations are thin**
  - What happens: JDBC schema alter always throws; Postgres rejects schema properties and refuses non-empty `cascade` drop because `CASCADE` can delete objects outside the schema; Doris stores schema comment as a property; Iceberg rejects cascade; Hudi cannot create schemas.
  - Cause: SPEC / MODEL.
  - Evidence: `JC/JdbcCatalogOperations.java:515`; `PG/operation/PostgreSqlSchemaOperations.java:106`, `:169`, `:187`; `IC/IcebergCatalogOperations.java:361`.
  - Mudcat implication: cascade semantics must be defined by the catalog, not delegated to the source.
- **PostgreSQL `catalog` is a database, `schema` is a Gravitino schema; one connection, one database**
  - What happens: `jdbc-database` is mandatory; the driver 42.7+ matches `getSchemas` catalog strictly, so the connector uses `connection.getCatalog()`; a missing `USAGE` privilege shows as `3F000` "no such schema", and the connector re-checks to return Forbidden.
  - Cause: SPEC / driver.
  - Evidence: `PG/operation/PostgreSqlTableOperations.java:117-118`, `:150-165`; `PostgreSqlSchemaOperations.java:133-136`.
  - Mudcat implication: separate "does not exist" from "you may not see it" in source errors.

### 3.5 Connections, security, concurrency

- **Backend probe under the global write lock**
  - What happens: catalog create probes the backend while holding the metalake write lock. A host that accepts TCP and never answers would stall every request, so the probe uses a dedicated connection with read and login timeouts, not the pool.
  - Cause: CONCURRENCY.
  - Evidence: `JC/JdbcCatalogOperations.java:288-298`.
  - Mudcat implication: RL-R45. No source call under a tenant-wide lock without a hard deadline.
- **Connection validation via `isValid()`, not SQL**
  - What happens: DBCP caches a validation statement; MySQL Connector/J then tries to switch back to the database it was prepared on, which fails after `setCatalog()`, so healthy connections were discarded. Default is `Connection.isValid()`.
  - Cause: ENGINE (driver/pool).
  - Evidence: `docs/jdbc-catalog-connection-validation.md` ("The default avoids a connection-reuse problem").
  - Mudcat implication: pooling a connection per source needs tests with the real drivers, including `setCatalog()` changes.
- **Pool exhaustion deadlock**
  - What happens: generating DDL can load the table through another connection; holding both let concurrent alters on different tables wait for each other until the borrow timeout.
  - Cause: CONCURRENCY.
  - Evidence: `JC/operation/JdbcTableOperations.java:419-423`.
  - Mudcat implication: RL-R53.
- **Retry hazards of DDL**
  - What happens: MySQL DDL commits implicitly, so a retry of a half-applied request is not safe. Deadlock (1213) and lock timeout (1205) are deliberately not mapped to 503.
  - Cause: STORE.
  - Evidence: `JC/converter/JdbcExceptionConverter.java:105-107`.
  - Mudcat implication: RL-R23. Idempotency keys must cover source mutations, not only catalog writes.
- **JDBC URL as an attack surface**
  - What happens: URL parameters and DBCP properties can load classes, deserialise objects, enable `LOAD DATA LOCAL`, or run `INIT` SQL (H2). The connector denies a list of names, decodes the URL repeatedly (percent escapes, malformed escapes) before scanning, also scans `connectionProperties`, and bans H2 entirely.
  - Cause: SECURITY.
  - Evidence: `JC/utils/DataSourceUtils.java:42-66`, `:99-103`; `common/.../utils/JdbcUrlUtils.java:49-84`, `:134-138`.
  - Mudcat implication: RL-R43, RL-R44. Treat every connection-string parameter as code.
- **Driver jars are not bundled**
  - What happens: a missing driver class raises "was not found on the catalog classpath. Install the driver JAR in the catalog's libs directory." Classes are loaded in a per-catalog loader; `DriverManager` cannot see or deregister them from the server loader.
  - Cause: OTHER (licensing, classloading).
  - Evidence: `JC/utils/DataSourceUtils.java:187`; `catalog-common/.../utils/JdbcDriverDeregisterer.java:19-30`.
  - Mudcat implication: RL-R47.
- **Connection timeouts differ by driver**
  - What happens: MySQL and Postgres use `connectTimeout`/`socketTimeout` in ms, Postgres `loginTimeout` in whole seconds (rounded up and capped), ClickHouse uses snake_case. The connector will not override an operator-set value and matches names case-sensitively at parameter boundaries.
  - Cause: SPEC (drivers).
  - Evidence: `JC/utils/JdbcConnectionTimeouts.java:101-114`, `:205-216`.
  - Mudcat implication: one canonical timeout key, translated per driver, with tests.
- **Hive client version auto-detect**
  - What happens: the factory loads the Hive 3 client and calls `getCatalogs()`; if the metastore replies `Invalid method name: 'get_catalogs'` the Hive 2 client is used. The winning classloader is cached.
  - Cause: SPEC (Thrift API changed in Hive 3).
  - Evidence: `HM/client/HiveClientFactory.java:126-148`.
  - Mudcat implication: RL-R25, RL-R48.
- **Hive 3 constraints are separate objects**
  - What happens: `alter_table` carries no NOT NULL/DEFAULT, so the connector drops the old constraints, alters, then adds the new ones. It builds the desired set first, restores on failure, and after a late failure tells the operator to repair by hand.
  - Cause: SPEC.
  - Evidence: `catalogs/hive-metastore3-libs/src/main/java/org/apache/gravitino/hive/client/hive3/HiveShimV3.java:211-247`.
  - Mudcat implication: RL-R22.
- **Hive listing lies about table type and format**
  - What happens: HMS drops tables created without `tableType` when you filter by type; the connector lists all names, then subtracts views, then subtracts Iceberg/Paimon (by `table_type` parameter) and Hudi (by `provider` plus derived `_ro`/`_rt` names, matched exactly because `startsWith` would hit `<base>_root`). The filter call passes `MAX_TABLES = -1` (no cap) (`HV/HiveCatalogOperations.java:120`, `:436`).
  - Cause: SPEC / STORE.
  - Evidence: `HV/HiveCatalogOperations.java:383-392`, `:420-470`.
  - Mudcat implication: RL-R34.
- **Hive views: one dialect each, plus engine-private encodings**
  - What happens: a Trino view is `VIRTUAL_VIEW` with `presto_view=true`, comment `Presto View`, and base64 JSON; a Trino view's comment lives inside the payload, so `SetProperty comment` is refused; materialised Trino views are rejected as non-plain; SQL path in a Trino view cannot be modelled and blocks alteration.
  - Cause: ENGINE (Trino/Spark).
  - Evidence: `HV/TrinoNativeViewCodec.java:51-52`; `HV/HiveViewCatalogOperations.java:212-245`, `:284-311`.
  - Mudcat implication: RL-R42.
- **Hive SerDe-derived tables are read-only for columns**
  - What happens: for tables whose columns come from the SerDe (Avro schema), column changes are refused: "Update the schema through the table SerDe".
  - Cause: SPEC.
  - Evidence: `HV/HiveCatalogOperations.java:752-754`.
  - Mudcat implication: schema ownership can sit outside the table record; model "schema source".
- **Hive alter lacks a lock**
  - What happens: load, modify in memory, write back; the TODO says a table lock is needed.
  - Cause: CONCURRENCY.
  - Evidence: `HV/HiveCatalogOperations.java:716`.
  - Mudcat implication: RL-R51.
- **Glue update replaces the whole table**
  - What happens: the connector starts from the table as Glue holds it, changes only the requested parts, writes everything else back as read, and sends the `versionId`.
  - Cause: SPEC.
  - Evidence: `GL/GlueCatalogOperations.java:661-664`.
  - Mudcat implication: RL-R50.
- **Glue hosts several formats; Gravitino mixes SDKs**
  - What happens: Hive-format tables go through the AWS Glue SDK; Iceberg tables go through the Iceberg `GlueCatalog` so that `metadata.json` is written to S3 and `metadata_location` set. Two code paths convert types and partition specs, and a TODO admits the Iceberg conversions duplicate `catalog-lakehouse-iceberg` and "must give the same results". Iceberg tables need an S3 location; Iceberg-format partition operations are refused.
  - Cause: MODEL.
  - Evidence: `GL/GlueIcebergTableHelper.java:100-108`, `:335-339`; `GL/GlueCatalogOperations.java:457-461`; `GL/GlueTable.java:109-115`.
  - Mudcat implication: share one conversion library per format across all registries (RL-R34).
- **Glue error messages**
  - What happens: AWS messages can quote whole request values, so they are cut to 1024 characters; credential-chain failures are rewritten to name the connector's own properties.
  - Cause: SECURITY / OTHER.
  - Evidence: `GL/GlueExceptionConverter.java:69`, `:92-117`.
  - Mudcat implication: RL-R44.

### 3.6 Iceberg and Paimon backends

- **Iceberg JDBC backend control table race**
  - What happens: Iceberg's V1 view-support migration adds `iceberg_type` with a non-idempotent `ALTER TABLE ADD COLUMN`. Several catalogs that share one backend URI race and one fails; the connector detects the duplicate-column error by wording per database (MySQL, PostgreSQL, SQLite) and re-initialises once.
  - Cause: CONCURRENCY / STORE.
  - Evidence: `ICC/utils/IcebergCatalogUtil.java:225-239`, `:244-260`.
  - Mudcat implication: schema migrations of shared tables must be idempotent and safe under two concurrent starters (relevant to the single PostgreSQL schema).
- **Iceberg JDBC defaults the connector sets**
  - What happens: schema version `V1` (needed for views), strict mode true (so a missing namespace is a 404, not an implicit create), and retry on SQLSTATE `08S01` (MySQL `wait_timeout` drops idle connections).
  - Cause: SPEC / STORE.
  - Evidence: `ICC/utils/IcebergCatalogUtil.java:180`, `:184-192`; `ICC/ops/IcebergCatalogWrapper.java:657`.
  - Mudcat implication: gateway defaults must match the Iceberg REST spec even when the library default differs.
- **Iceberg JDBC namespace property length**
  - What happens: JDBC backend stores namespace properties in a limited column; the connector rejects values over 1000 characters up front.
  - Cause: STORE.
  - Evidence: `ICC/ops/IcebergCatalogWrapper.java:116`, `:1022-1032`.
  - Mudcat implication: RL-R54.
- **REST backend `warehouse` means something else**
  - What happens: on a REST backend `warehouse` picks a catalog on the remote server and is not a storage location; a wrong value raises `NoSuchWarehouseException`, which the connector rewrites to explain that.
  - Cause: SPEC.
  - Evidence: `IC/IcebergBackend.java:134-141`.
  - Mudcat implication: gateway/proxy mode must not reuse a key name with a different meaning.
- **GCS tokens are reminted before use**
  - What happens: the backend rebuilds the Iceberg client when `gcs.oauth2.token-expires-at` is within 5 minutes.
  - Cause: SECURITY.
  - Evidence: `IC/IcebergBackend.java:62`, `:71-100`.
  - Mudcat implication: credential expiry belongs in the credential object, not discovered by failure.
- **Iceberg read-only properties and reserved names**
  - What happens: set or remove of a reserved property is refused.
  - Cause: MODEL.
  - Evidence: `IC/ops/IcebergCatalogWrapperHelper.java:260-272` (`"... is not allowed to remove properties"`).
  - Mudcat implication: RL-R37.
- **Hudi has no write path**
  - What happens: only list and load work; the table is shaped from HMS parameters; every mutation throws "Not implemented yet".
  - Cause: OTHER (unfinished).
  - Evidence: `HU/backend/hms/HudiHMSBackendOps.java:124-202`; `HU/HudiTable.java:40`.
  - Mudcat implication: RL-R40.

## 4. Open questions, inconsistencies, half-implemented

- **No capability for the alter matrix.** `Capability` has six methods. The matrix in 3.2 is learned by throwing. Three exception types carry the same meaning. Client code (Spark, Trino, Flink connectors in other directories) must catch them one by one (inferred).
- **`resolveTableName` has a default only.** The hook exists, the server uses it, and no connector overrides it (`JC/operation/TableOperation.java:173`; grep shows no override in `catalogs/` or `catalogs-contrib/`).
- **JDBC default name rules are a test stub.** `JC/JdbcCatalogCapability.java:39-43` uses a SQLite pattern "for testing" and a TODO for reserved words.
- **Index load policy is inconsistent.** Doris throws on native `NGRAM_BF` during `loadTable` (`DorisTableOperations.java:757`); ClickHouse omits unrepresentable legacy indexes and logs (`ClickHouseTableOperations.java:2093`); Iceberg drops void transforms silently and throws on unknown ones.
- **Silent drops.** ClickHouse `nullOrdering` is ignored with a log line (`:548`); Postgres drops list element nullability (`PostgreSqlTypeConverter.java:232`); Hive stores no null ordering.
- **Iceberg required-column changes are TODO** (`IcebergCatalogWrapperHelper.java:103`, `:172`); column defaults described as "WIP" (`IcebergCatalogCapability.java:52`) although the code base targets Iceberg 1.11 with format v3 (`IcebergTablePropertiesMetadata.java:58-68`). Format v4 is admitted although "not yet finalized" (comment at `:60-68`).
- **Hive view formats**: only `hive` dialect is created; Trino and Spark encodings are a TODO pointing to a design doc (`HiveCatalogOperations.java:1191`). Meanwhile a Trino codec exists in `HiveViewCatalogOperations` and reads them. The create path and the alter path disagree about what is supported (inferred from reading both).
- **Hive alter has no table lock** (`HiveCatalogOperations.java:716`).
- **Doris and Hive version gating depends on a probe that can fail** (`DorisTableOperations.java:380-410` raises `UnsupportedOperationException` when the version cannot be read); the cost of a failed probe falls on a create.
- **Paimon Kerberos** is limited to the filesystem backend: `// TODO: Now we only support kerberos auth for Filesystem backend` (`PM/utils/CatalogUtils.java:111`).
- **Hudi** is a stub; the catalog exists only to list and load.
- **Glue/Iceberg duplicated conversions** need unification (`GlueIcebergTableHelper.java:335-339`).
- **ClickHouse expression defaults**: `// TODO clickhouse has bug which isExpression is false when is really expression` (`CH/converter/ClickHouseColumnDefaultValueConverter.java:164`).
- **JDBC `TODO(yunqing): check schema and catalog also`** when mapping columns (`JC/operation/JdbcTableOperations.java:327`): a column lookup by table name pattern may cross schemas.
- **Purge** is unsupported on every JDBC dialect ("please use drop table"), supported on Iceberg and Paimon, and refused on external tables elsewhere. The table-level API has no way to ask which applies.
- **Postgres `Set property`** says "not supported yet" while `TODO #804 will add properties` appears at `PostgreSqlTableOperations.java:259`.
- **Not checked**: Spark/Trino/Flink connector-side handling of these limits; Iceberg REST server specifics; credential vending in `catalog-common/credential`; `catalog-fileset` and `catalog-model` (other agents).
