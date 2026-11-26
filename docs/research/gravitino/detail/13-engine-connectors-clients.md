> **Research note, not a requirement document.** Drafted by a code-reading agent from Apache Gravitino commit `9aa78cf` (2026-11-26), then spot-checked by the lead. Line numbers can drift by a few lines; search by file and symbol. Items marked `(inferred)` were not confirmed in code. The text does not follow Simplified Technical English. See [../README.md](../README.md) for the synthesis.

# Multi-engine support: Spark / Trino / Flink connectors, Java and Python clients, MCP server (ref prefix EC)

Scope read: `spark-connector/**`, `trino-connector/**`, `flink-connector/**`, `clients/client-java`, `clients/client-python` (core, not gvfs), `clients/cli` (briefly), `mcp-server/**`, `docs/{spark,trino,flink}-connector/**`, `docs/gravitino-mcp-server.md`, `docs/how-to-use-gravitino-client.md`, `design-docs/spark-rest-catalog-registration.md`. Paths are repo-relative to `/home/user/apache/gravitino`. Statements marked `(inferred)` were not read directly.

## 1. Decomposition

### 1.1 The common architecture: "Gravitino for metadata, the native engine connector for I/O"
Every connector is a *wrapper catalog*. It asks Gravitino for schemas, tables and DDL. It then builds the engine's own native catalog (Spark `SparkCatalog`/`JDBCTableCatalog`/Kyuubi Hive, Trino's inner Hive/Iceberg/JDBC connector, Flink's Iceberg/Hive/JDBC/Paimon catalogs) from the Gravitino catalog properties, and the native catalog does all reads and writes.
- Spark: `spark-connector/spark-common/.../catalog/BaseCatalog.java` (abstract; DDL via `gravitinoCatalog()`, I/O via `sparkCatalog`, `loadSparkTable` at :893). One subclass per catalog kind: `hive/GravitinoHiveCatalog`, `iceberg/GravitinoIcebergCatalog`, `jdbc/GravitinoJdbcCatalog`, `glue/GravitinoGlueCatalog`, `paimon/GravitinoPaimonCatalog` (Spark 3.5 only).
- Trino: `trino-connector/src/common/main/.../GravitinoMetadata.java` wraps `internalMetadata` (the real connector's metadata). `catalog/CatalogConnectorMetadata.java` is the Gravitino-side DDL/lookup. One `*ConnectorAdapter` + `*MetadataAdapter` + `*DataTypeTransformer` + `*PropertyConverter` per provider (hive, iceberg, glue, jdbc/mysql, jdbc/postgresql, jdbc/trino, memory).
- Flink: `flink-connector/flink-common/.../catalog/BaseCatalog.java` (a Flink `Catalog` that delegates to a Gravitino catalog), one `Gravitino*Catalog(+Factory)` per provider.
- Consequence: there are two sources of truth for a table's schema (Gravitino's record and the physical store). The connectors have code to reconcile them (see 3.x "drift", "time travel").

### 1.2 How each engine discovers catalogs (three different models)
| Engine | Discovery | Refresh | Where |
|---|---|---|---|
| Spark | One `DriverPlugin` lists relational catalogs **once at driver start** with the *application* identity, then writes `spark.sql.catalog.<name>=<class>` into the SparkConf. Catalogs created later need a session restart. | Catalog metadata cache `expireAfterWrite` 300 s (`catalogCacheTtlSec`); client cache `expireAfterAccess` 3600 s | `spark-common/.../plugin/GravitinoDriverPlugin.java:115-133,145-208`; `catalog/GravitinoCatalogManager.java:113-117,272-283`; `GravitinoSparkConfig.java:97,100` |
| Trino | A single daemon thread polls Gravitino every `gravitino.metadata.refresh-interval-seconds` (default **10**) and issues `CREATE CATALOG` / `DROP CATALOG` **against Trino itself over an internal JDBC connection** (needs `catalog.management=dynamic`). Re-registers when `lastModifiedTime` advances. | 10 s | `catalog/CatalogConnectorManager.java:72-75,227-294,714-731`; `catalog/CatalogRegister.java`; `GravitinoConfig.java:265-270`; `docs/trino-connector/requirements.md:19` |
| Flink | Gravitino **is** Flink's `CatalogStore`. `getCatalog(name)` goes to the server on every call; no client cache. `CREATE CATALOG` of a `gravitino-*` type persists the catalog to Gravitino. Optional session store keeps other catalog types in memory, with shadowing rules. | none (always live) | `flink-common/.../store/GravitinoCatalogStore.java:56-129`; `store/GravitinoSessionCatalogStore.java:45-83` |
A fourth path (design-doc only for Iceberg, shipped for Lance): `GravitinoLakehouseRESTDiscoveryPlugin` lists catalogs from an Iceberg/Lance REST server and registers one Spark REST catalog per entry (`design-docs/spark-rest-catalog-registration.md`; `spark-common/.../plugin/restcatalog/*`).

### 1.3 Per-engine-version modules (why they exist)
- Spark: `spark-connector/{v3.5,v4.0,v4.1}`; shared code is compiled *into each version module* (not consumed as a jar) with per-line source dirs `spark-common/src/main/{spark35,spark40,spark41}`; classes that differ per version are injected via `plugin/SparkBindings.java`. Diffs: Iceberg `SparkTable` constructor changed in 4.1 (`spark41/.../SparkIcebergTableBase.java:37-47`, rejects branch writes :98-103); `ProcedureCatalog` moved package in Spark 4 (`iceberg/GravitinoIcebergCatalog.java:66-69`); Paimon only on 3.5; Doris read-only path only on 3.5 (`SparkBindings.java:48-53`); Glue only verified on 3.5 (`docs/spark-connector/spark-catalog-glue.md:43`).
- Trino: **six** modules `trino-connector-{469-472,473-477,478-479,480,481,482-483}` sharing `src/common*` dirs (`trino-connector/segment.gradle`). Examples of SPI breaks: `ConnectorSplitManager.getSplits` takes `DynamicFilter` up to 481 and `Set<ColumnHandle>` from 482 (`src/common-480-481/.../GravitinoSplitManager.java:32-39`, `src/common-482-483/.../GravitinoSplitManager.java:58`); `Type.getTypeSignature()` removed in 482 for `getTypeDescriptor()` (`src/common-482-483/.../util/TypeSignatures.java:27-41`); JDK toolchain 24 for 469-472, 25 for 473+ (`segment.gradle:44-46`).
- Flink: `flink-connector/{v1.19,v1.20,v2.1,v2.2}`; `utils/CatalogCompat.java` is a per-version hook for `CatalogTable` constructor, table serialization and the JDBC factory class that moved packages (`CatalogCompat.java:36-57`). Hive catalog is absent on Flink 2.x (`docs/flink-connector/flink-connector.md:17-18,30-35`, upstream FLINK-38031).
- Dependency hell is real: Glue needs a patched Hive 2.3.10 client because `aws-java-sdk-glue 1.12.31` needs a Jackson class removed in 2.12 (`spark-connector/build.gradle.kts:42-44`); Spark 4 ITs must force javax Jersey/HK2/validation back against Spark's jakarta ones (`v4.0/spark/build.gradle.kts:108-115`).

### 1.4 Credentials / identity per engine
- Spark: auth types simple/basic/oauth2/kerberos/token. `token` mode re-resolves the bearer token on every request (session conf beats application conf; token file beats token value) (`GravitinoDriverPlugin.java:406-482,489-502`). Clients and catalog metadata are cached per `GravitinoIdentity` (JWT `sub` claim unverified, else SHA-256 of token) so shared drivers do not leak metadata between users (`catalog/GravitinoIdentity.java`, `GravitinoCatalogManager.java:46-52`).
- Trino: `gravitino.client.session.forwardUser=true` forwards the Trino user (simple) or an extra-credential token (oauth2); per-user session cache (500 entries, 3600 s) (`security/GravitinoAuthProvider.java:53-66,178-245`; `GravitinoConfig.java:293-305`). Without a forwarded token it **falls back to the service identity** (`docs/trino-connector/authentication.md:189`).
- Flink: OAuth2/basic/Kerberos/simple only; OAuth/Basic must be configured explicitly (`flink-common/.../GravitinoCatalogManager.java:57-93`).
- Data-plane secrets: `propertiesWithSecrets()` and `CredentialInfos.nonExpiringCredentialInfo()` merge secrets and **only non-expiring** credentials into the native catalog config at initialisation (`BaseCatalog.java:905-942`; `api/.../credential/CredentialInfos.java:44-57`). Expiring (vended) credentials only work through the Iceberg REST path, so both Spark and Trino route every hive/jdbc-backed `lakehouse-iceberg` catalog through the Gravitino Iceberg REST server by default (`iceberg/GravitinoIcebergCatalog.java:122-210`; `docs/trino-connector/catalog-iceberg.md:23-40`).

### 1.5 Clients
- Java: `clients/client-java/.../GravitinoClientBase.java` (version check, `HTTPClient` build), `HTTPClient.java` (Apache HttpClient 5; `DefaultHttpRequestRetryStrategy(retryTimes, 1s)` at :153), `ErrorHandlers.java` (1640 lines; ~25 per-resource handlers that map `(code, type-string)` to exception classes), `GravitinoClientConfiguration.java`, `GravitinoVersion.java`.
- Python: `clients/client-python/gravitino/client/gravitino_client_base.py`, `utils/http_client.py` (urllib `build_opener()` per request), `exceptions/handlers/*`.
- CLI: `clients/cli` (177 main classes); destructive commands call `AreYouSure.really(force)`.
- Server cooperation: `X-Client-Version` header (`common/.../Version.java:35`; `HTTPClient.java:808`; `gravitino_client_base.py:55,69`), media type `application/vnd.gravitino.v1+json` (`server/.../FilesetOperations.java:91`), error codes 1001-1013 (`common/.../dto/responses/ErrorConstants.java:25-67`).

### 1.6 MCP server (`mcp-server/`, Python, FastMCP 3.4.5)
- 81 tools in 16 entity groups, each tagged by entity (`tools/__init__.py:38-75`). Tool functions are thin: `ctx...rest_client().as_table_operation().get_list_of_tables(...)` and return **JSON strings** (`tools/table.py:20-90`). It uses its own `httpx` REST client, not the Python client (`client/plain/*`).
- Middleware chain (`server.py:128-155`): `MetalakeArgumentMiddleware` (adds an optional `metalake` arg to every tool schema, `core/middleware.py:43-120`), `AuditMiddleware`, `LoggingMiddleware(include_payloads=True)`, `TimingMiddleware`, `ErrorHandlingMiddleware`.
- Auth (`core/context.py`, `core/setting.py`): stdio uses `--token` or OAuth2 client-credentials service identity. HTTP forwards the caller's `Authorization` header verbatim, and falls back to the service identity when the header is absent unless `--no-service-identity-fallback`. REST clients cached per (principal, metalake), max 128 (`context.py:37`).
- Extension point: `--include-tool-tags` allowlist (entity tags only). No plugin SPI.

### 1.7 Extension points and config keys that matter
- Spark: `SparkCatalogExtension` loaded via `SparkCatalogExtensionLoader` for providers the connector does not support (`SparkBindings.java:157-170`); `CatalogRegistrationPolicy` for REST registration; keys `spark.sql.gravitino.{uri,metalake,failFast,enableIcebergSupport,enablePaimonSupport,enableDorisSupport,authType,clientCacheMaxSize,clientCacheTtlSec,catalogCacheTtlSec,iceberg.rest-routing-enabled,iceberg.rest-uri,iceberg.reuseOAuth2}` (`GravitinoSparkConfig.java`); catalog property prefix `spark.bypass.` passes raw engine options through (`PropertiesConverter.java:29,48`).
- Trino: `gravitino.{uri,metalake,catalog-name-with-metalake,metadata.refresh-interval-seconds,trino.skip-catalog-patterns,trino.skip-version-validation,iceberg.rest-uri,iceberg.rest-routing-enabled,client.*}`, `trino.jdbc.*` (internal connection to Trino), `trino.bypass.*` per-catalog; `CatalogConnectorFactory` replaceable via `gravitino.catalog.connector.factory.class.name` (`GravitinoConfig.java`).
- Flink: `table.catalog-store.kind=gravitino`, `table.catalog-store.gravitino.gravitino.{uri,metalake,enableSessionCatalogSupport,excludedCatalogs,enableSessionCatalogShadowing,client.*}`; Flink `ServiceLoader` factories discovered by provider string (`GravitinoCatalogStore.java:178-235`).
- Client: `gravitino.client.{retryTimes,connectionTimeoutMs,socketTimeoutMs,maxConnections,maxConnectionsPerRoute}` (anything else throws, `GravitinoClientConfiguration.java:88-99`); env `GRAVITINO_VERSION_CHECK_DISABLED` (`GravitinoClientBase.java:55`).

### 1.8 Support matrix as encoded in code and docs (the "quirks" of the matrix)
| | Spark | Trino | Flink |
|---|---|---|---|
| Hive | yes (Kyuubi Hive connector; views read-only, materialised on driver `docs/spark-connector/spark-catalog-hive.md:66`) | yes, views in Presto-view format | 1.19/1.20 only; generic tables via property serialisation |
| Iceberg | opt-in flag `enableIcebergSupport`; DDL: no distribution/sort (`spark-catalog-iceberg.md:37`); no views, no metadata tables, no branch/tag/partition-field DDL, no atomic CTAS/RTAS | all `lakehouse-iceberg` routed via Iceberg REST; no `CREATE OR REPLACE TABLE AS SELECT` | no partition ops, no metadata tables, no UPDATE/DELETE, no `CREATE TABLE LIKE` (`flink-catalog-iceberg.md:25-34`) |
| Paimon | 3.5 only, FilesystemCatalog on HDFS only | not supported | yes |
| JDBC | MySQL/PG/Doris/StarRocks/OceanBase; Doris has a *separate read-only path* | MySQL, PostgreSQL, Trino-cluster | MySQL, PostgreSQL |
| Glue | 3.5 only | yes | no |
| Non-relational (fileset, model, topic) | not registered (`GravitinoCatalogManager.java:344-349` "Only support relational catalog") | marked UNSUPPORTED (`CatalogConnectorManager.java:696-707`) | no |
| Namespace depth | 1 (`BaseCatalog.java:838-841`) | 2 | 2 |

## 2. Candidate requirements

Kind: IMPL = Gravitino implements it; GAP = Gravitino fails or omits it, mudcat should do better.

| Ref | Kind | Requirement | Rationale / Gravitino evidence | Area | Ver |
|---|---|---|---|---|---|
| EC-R1 | GAP | mudcat shall publish a machine-readable table of engine, engine version, format and operation (read, write, DDL, view, time travel) with each cell marked supported or unsupported. | Support limits are scattered over ~14 docs pages and runtime throws: Paimon Spark 3.5 only (`docs/spark-connector/spark-catalog-paimon.md:18`), Hive absent on Flink 2.x (`docs/flink-connector/flink-connector.md:17-18`), Iceberg unsupported-op lists (`spark-catalog-iceberg.md:38-50`, `flink-catalog-iceberg.md:25-34`). | IN | I |
| EC-R2 | GAP | mudcat shall reject an operation that an engine integration cannot perform with one error code that names the operation and the integration. | Spark throws bare `UnsupportedOperationException` ("Not support ...", `SparkTypeConverter.java:116,203`; `BaseCatalog.java:578`). Trino uses `GRAVITINO_UNSUPPORTED_OPERATION` and `NOT_SUPPORTED` mixed (`GravitinoMetadata.java:171,280`; `CatalogConnectorMetadata.java:352`). Flink throws bare exceptions (`flink-common/.../BaseCatalog.java:1081-1083`). Three engines, three styles. | IN | T |
| EC-R3 | IMPL | mudcat shall serve every Iceberg table through its Iceberg REST interface, so that an engine with an Iceberg REST client needs no mudcat-specific plugin. | Spark and Trino default to routing hive/jdbc-backed Iceberg catalogs through the Iceberg REST server because only REST carries vended credentials (`GravitinoIcebergCatalog.java:122-133`; `docs/trino-connector/catalog-iceberg.md:23-31`). Flink propagates REST auth for the same reason (`flink-common/.../GravitinoIcebergCatalogFactory.java:152-155`). | IN | D |
| EC-R4 | IMPL | mudcat shall return the list of warehouses a caller can open from an endpoint on the Iceberg REST service, so that clients register them without per-catalog configuration. | Design doc: `GET {iceberg-rest}/gravitino/v1/management/catalogs`; "Spark never guesses catalog names" (`design-docs/spark-rest-catalog-registration.md:28-33,57-77`). Lance uses the protocol's own `namespace/list`. | IN | D |
| EC-R5 | GAP | mudcat shall filter every catalog, namespace and table listing returned to an engine by the caller's permissions. | Spark registers the catalog list with the *application* identity, so "a user may see a catalog name that they are not then allowed to open" (`docs/spark-connector/spark-connector.md:88-89`). The Lance root list "is not authorization-filtered" by design (`design-docs/spark-rest-catalog-registration.md:346-350`). | AC | T |
| EC-R6 | GAP | mudcat shall let an engine client re-resolve protocol endpoint URIs and catalog lists without restarting the client session. | Spark caches the discovered Iceberg REST URI for the life of the manager: "there is no refresh path" (`GravitinoCatalogManager.java:69-70`); the catalog list is fixed at driver start (`:272-283`); "Catalogs added later appear after a session restart" (`design-docs/spark-rest-catalog-registration.md:267`). Trino does refresh (`CatalogConnectorManager.java:714-725`). | PL | T |
| EC-R7 | IMPL | mudcat shall issue storage credentials per table, with an expiry time, in the response that resolves the table, and shall not place them in catalog definitions. | Each Iceberg REST table access "gets a freshly issued temporary credential" (`docs/trino-connector/catalog-iceberg.md:31-34`). Catalog-level credentials are only used by the connector when `expireTimeInMs == 0` (`CredentialInfos.java:44-57`). | RD | T |
| EC-R8 | GAP | mudcat shall not require an engine integration to hold long-lived storage secrets copied from catalog properties. | Spark and Trino copy catalog secrets into the native catalog once and keep them for the session (`BaseCatalog.java:925-942`; `CatalogConnectorManager.java:1066-1111`). Lance has "no vending equivalent today: secrets in catalog properties reach the client" (`design-docs/spark-rest-catalog-registration.md:343-344`). Iceberg routing refuses a native-FileIO warehouse with no `credential-providers`, so static keys would be lost (`GravitinoIcebergCatalog.java:165-177`). | RD | I |
| EC-R9 | IMPL | mudcat shall state, in the interface description of each engine integration, which identity the data-plane credentials belong to. | Spark: "Data is read by the executors using the credentials that the underlying catalog was built with, not the end user's token" (`docs/spark-connector/spark-connector.md:90-91`). Trino IRC path needs its own credential, "not reused automatically" (`docs/trino-connector/authentication.md:212-213`). | RD | I |
| EC-R10 | IMPL | mudcat shall evaluate each catalog request under the end user's identity whenever the engine presents a user token. | Spark `token` mode resolves the token per request (`GravitinoDriverPlugin.java:406-422,489-502`); Trino `forwardUser` builds a per-user client (`GravitinoAuthProvider.java:178-245`). | AC | T |
| EC-R11 | GAP | mudcat shall record in its audit log, for each request, whether it ran under the end user's identity or under a fallback service identity. | Trino silently reuses "the shared service metadata" when no token is forwarded (`docs/trino-connector/authentication.md:189`). MCP HTTP falls back to `--token` identity when no header (`mcp-server/mcp_server/server.py:158-187`). Neither marks the fallback in an audit record (`core/audit.py:56-110`). | AC | T |
| EC-R12 | GAP | mudcat shall by default reject a request that carries no end-user identity instead of substituting a service identity. | `--no-service-identity-fallback` is opt-in; the default logs "Do not expose this endpoint to untrusted callers" (`server.py:174-179`; `core/setting.py:66`). | AC | T |
| EC-R13 | IMPL | mudcat shall report all objects of one query that the caller may not access, each with the privilege it needs, in one error. | `RequiredPrivilegesCheck` aggregates denied tables from `AuthorizationTable` placeholders so the user sees every missing privilege at once (`authorization/RequiredPrivilegesCheck.java:30-70`; `BaseCatalog.java:351-362`). | AC | T |
| EC-R14 | GAP | mudcat shall let "create if not exists" decide existence without requiring read privilege on the existing object. | `tableExists` returns false on `ForbiddenException` so that `CREATE TABLE IF NOT EXISTS` can proceed for a user with only CREATE privilege (issue #9180) (`BaseCatalog.java:466-496`). This reports "does not exist" for an object that exists. | AC | T |
| EC-R15 | GAP | mudcat shall give each catalog an immutable ID, and every definition response shall carry a change token that a client compares without re-reading the definition. | Trino detects change by `lastModifiedTime` comparison `<=` (`CatalogConnectorManager.java:721-724`); Spark JDBC detects delete-and-recreate by comparing audit `createTime` (`jdbc/GravitinoJdbcCatalog.java:189-193`); Trino notes "dropped and recreated under the same name" (`CatalogConnectorManager.java:577-579`). Clocks and names stand in for identity. | ID | T |
| EC-R16 | IMPL | mudcat client integrations shall refuse I/O when a connection-affecting property of the catalog changed after the session read it. | Spark JDBC: "JDBC catalog %s changed after this Spark session initialized it. Restart the Spark session" (`GravitinoJdbcCatalog.java:179-199`). Prevents silent access to the old source. | PL | T |
| EC-R17 | GAP | mudcat shall document a maximum staleness bound for each engine integration and shall test it. | Trino 10 s poll (`GravitinoConfig.java:265-270`), Spark 300 s catalog TTL and fixed start list (`GravitinoSparkConfig.java:100`), Flink none. Spark's TTL exists so "a permission revoked in Gravitino must eventually stop being served from cache" (`GravitinoCatalogManager.java:113`), but eventually is unbounded for the start list. | PL | T |
| EC-R18 | IMPL | mudcat shall expose, per engine integration, the registration state of each catalog (registered, skipped, failed, unsupported) with the last error and last success time. | Trino system tables `catalog_status` and `load_status`; `CatalogRegistrationState` has REGISTERED/FAILED/SKIPPED/UNSUPPORTED/UNCHANGED (`catalog/CatalogRegistrationState.java:41-42,166-175`; `system/table/GravitinoSystemTable*.java`). | OP | D |
| EC-R19 | GAP | mudcat shall make a failed registry connection appear in the first engine error that depends on it. | Spark `failFast` defaults to false: failure logs a warning and "Gravitino catalogs will not be registered", so the user later sees only "catalog not found" (`GravitinoDriverPlugin.java:117-122,533-545`; `docs/spark-connector/spark-connector.md:52-58`). | OP | T |
| EC-R20 | IMPL | mudcat shall keep engine-session-only catalogs out of the shared registry and shall define precedence when a session name equals a shared name. | Flink `GravitinoSessionCatalogStore`: in-memory store first, `excludedCatalogs`, `enableSessionCatalogShadowing`, and a thread-local to carry the shadow decision between Flink calls (`store/GravitinoSessionCatalogStore.java:45-125`). Spark: user `spark.sql.catalog.<name>` always wins (`design-docs/spark-rest-catalog-registration.md:167-185`; `GravitinoDriverPlugin.java:157-163`). | PL | T |
| EC-R21 | GAP | mudcat shall mark each property as secret by declaration in its schema, not by a name pattern, and shall exclude secret values from logs, audit records and error messages. | Trino redaction is "best-effort keyword match on the property key; it cannot catch a sensitive value under a name that doesn't contain one of these words" (`CatalogRegister.java:64-70`). Spark: "Authentication failures can include credentials in exception messages" (`GravitinoDriverPlugin.java:538`). MCP logs tool payloads up to 1000 characters (`server.py:146`), which for `create_catalog` includes catalog properties (inferred). | UP | T |
| EC-R22 | GAP | mudcat shall not include server stack traces in error bodies returned to clients. | Trino must trim: "A failure received over REST carries the server stack trace after it" (`trino-connector/.../GravitinoErrorCode.java:182-189`). | AC | T |
| EC-R23 | GAP | mudcat shall define error types as one closed registry of stable identifiers from which server and all clients are generated. | Java client maps `(code, type simple-class-name string)` by hand in ~25 handlers: `errorResponse.getType().equals(NoSuchSchemaException.class.getSimpleName())` (`ErrorHandlers.java:375-410`, 1640 lines). Python repeats it (`clients/client-python/gravitino/exceptions/handlers/`). Trino re-maps again (`GravitinoErrorCode.java:130-170`). | IN | I |
| EC-R24 | GAP | mudcat shall accept a precondition with each mutation, and shall evaluate it in the same transaction as the write. | Conflict is code 1012 `OptimisticLockException`, but the client has nothing to send: the docs say "reload the latest metadata, reconsider ... Do not replay a stale update unchanged" (`docs/how-to-use-gravitino-client.md:185-210`). The one precondition, `expectedRevision` on semantic models, is checked by loading the latest revision and comparing before the write (`server/.../SemanticModelOperations.java:359-367`), which is not atomic (inferred). | IN | T |
| EC-R25 | GAP | mudcat shall let a client retry a mutation only with an idempotency key. | Java retries up to `retryTimes` (default 1) through Apache's idempotent-method rule (`HTTPClient.java:153`; `GravitinoClientConfiguration.java:109`) (inferred rule); Python never retries (`http_client.py:255`); MCP: "Do not retry writes: the upstream operation may already have committed" and tells the user to "check its result before retrying" (`plain_rest_client_operation.py:99-104`). No key exists to make this safe. | IN | T |
| EC-R26 | GAP | mudcat shall give all client libraries and the agent tool server one default for connection timeout, read timeout and retry count. | Java 180 s connect and socket, 1 retry (`GravitinoClientConfiguration.java:56,59,109`); Python 10 s, no retry, new opener per request, no pooling (`constants/timeout.py:18`; `http_client.py:255`); MCP 60 s read, 5 s connect (`setting.py:67`; `plain_rest_client_operation.py:140`). | IN | I |
| EC-R27 | GAP | mudcat shall define client and server compatibility by a range that the server advertises, not by comparing release numbers. | Rule is `client.major < server.major \|\| (same major && client.minor <= server.minor)` (`GravitinoVersion.java:51-54`): any older-major client is "compatible", a newer patch is fine, a newer minor is refused. The error text says only that the client is higher. Check is skippable by env var `GRAVITINO_VERSION_CHECK_DISABLED` (`GravitinoClientBase.java:55,286-297`). | IN | T |
| EC-R28 | IMPL | mudcat clients shall send their own version in every request, and the server shall branch on it only for a documented compatibility case. | `X-Client-Version` header; the single server-side use: fileset `subPath` is percent-decoded only for clients older than 1.0, and a missing header is treated as current (`FilesetOperations.java:257-259`; `server-common/.../Utils.java:382-390`). | IN | T |
| EC-R29 | IMPL | mudcat shall reject at creation any name that an engine's name resolution cannot address exactly, and any pair of names that an engine folds to one identifier. | Trino stores catalog names lower-case and quoted-or-not, so uniqueness is compared after folding (`CatalogRegister.java:470-510`), with `GRAVITINO_DUPLICATED_CATALOGS` on clash and "first one registered keeps it" across metalakes (`CatalogConnectorManager.java:540-560`). Spark lower-cases the database part when the session is case-insensitive (`GravitinoTableInfoHelper.java:125-135`). PostgreSQL docs: use unquoted identifiers or "names containing uppercase letters may not be found" (`docs/trino-connector/catalog-postgresql.md:19-27`). | ST | T |
| EC-R30 | GAP | mudcat shall declare for each engine integration the namespace depth it can address and the rule that projects deeper hierarchies onto it. | Spark connector accepts exactly one namespace level: "Doesn't support multi level namespaces" (`BaseCatalog.java:838-841`, :526-531); rename across databases unsupported (`:506`); Trino "Cannot rename table across schemas" (`CatalogConnectorMetadata.java:376`). mudcat has nested collections. | ST | A |
| EC-R31 | IMPL | mudcat shall declare, per engine, each type the engine cannot represent exactly, and shall reject a create-table request that needs such a type. | Unsigned integers widen (`SparkTypeConverter.java:119-144`, ulong to `decimal(20,0)`); UUID becomes string (`:160-162`); Hive has no `timestamp_ntz` (`SparkHiveTypeConverter.java:53-55`); Iceberg has no CHAR or VARCHAR(n) (`trino .../IcebergDataTypeTransformer.java:43-49`); Hive VARCHAR length cap (`HiveDataTypeTransformer.java:62`). | ST | A |
| EC-R32 | IMPL | mudcat shall reject a table definition that carries a feature it cannot store, and shall never store a degraded copy. | Flink computed columns, metadata columns and watermarks are rejected, because "storing them anyway would turn each such column into a plain physical column and drop the watermark, without any error" (`flink-common/.../BaseCatalog.java:906-946`; docs `flink-connector.md:21-24`). | ST | T |
| EC-R33 | GAP | mudcat shall store engine-specific table definitions as typed, engine-named representations kept apart from table properties. | Flink generic tables are serialised into Gravitino table properties and detected by `connector` / `is_generic` keys (`hive/FlinkGenericTableUtil.java:30-70`). Properties also carry `spark.bypass.*` / `trino.bypass.*` engine options (`PropertiesConverter.java:29,48`). | ST | I |
| EC-R34 | IMPL | mudcat shall store a view as one logical definition with a list of representations, each tagged with its engine dialect, and shall tell the client which dialects exist. | Views "may carry SQL representations for multiple engines (Hive, Spark, Flink, Trino)" (`docs/trino-connector/sql-support.md:58-62`). | ST | T |
| EC-R35 | GAP | mudcat shall return a view that lacks the caller's dialect as a listed view with a dialect-missing error, never as an absent one. | Trino: such a view "is silently invisible" (`docs/trino-connector/sql-support.md:61-63`). Flink lists views it cannot load (`flink-common/.../BaseCatalog.java:260-264` TODO; `flink-catalog-hive.md:113`). Spark materialises Hive views on the driver (`spark-catalog-hive.md:66`). The three engines disagree. | ST | T |
| EC-R36 | GAP | mudcat shall resolve a table at a named version and return the schema of that version. | Spark time travel: `loadTable(ident, version)` loads the Gravitino table at its *current* definition and the Spark table at the old snapshot, then combines them (`GravitinoIcebergCatalog.java:293-330`). Spark 4.1 builds a table for a branch but refuses writes to it (`spark41/.../SparkIcebergTableBase.java:98-103`). | QY | T |
| EC-R37 | GAP | mudcat shall keep the entity ID of a table across a replace operation and record the replace as a new version. | Trino `CREATE OR REPLACE TABLE AS SELECT` is rejected because the inner Iceberg connector "caches the table's UUID at query-plan time" and the new table has a new UUID (`GravitinoMetadata.java:272-281`). | ID | T |
| EC-R38 | IMPL | mudcat shall compare a registered schema with the schema found in the physical store at read time and shall refuse the read on drift, naming the column. | Spark Doris path validates Gravitino schema against Doris FE and JDBC metadata and "rejects schema drift and unsupported Doris types" (`DorisPhysicalSchemaValidator35.java`; `docs/spark-connector/spark-catalog-jdbc.md:28`). | ST | T |
| EC-R39 | IMPL | mudcat shall check authorisation on the metadata path before any physical access, and shall not load the physical table for a caller who is denied. | "Do not load the underlying Spark table here: that load bypasses Gravitino authorization" (`BaseCatalog.java:351-355,781-785`); Doris path "checks Gravitino authorization before physical access" (`spark-catalog-jdbc.md:28`). Without a data proxy the engine's own store access is the real enforcement point; see EC-R8. | AC | T |
| EC-R40 | IMPL | mudcat shall isolate every engine-version difference in one adapter per engine version and shall keep shared logic version-free. | Spark `SparkBindings` injects version classes (`plugin/SparkBindings.java:36-70`); Flink `CatalogCompat` (`CatalogCompat.java:36-57`); Trino six segment modules (`segment.gradle`). | OP | I |
| EC-R41 | GAP | mudcat shall publish for each engine release the exact versions of the engine format library that mudcat was tested with. | Spark: use only the matching Iceberg runtime; "Mixing Iceberg JARs from different versions ... may cause runtime errors" (`docs/spark-connector/spark-catalog-iceberg.md:17-28`); Flink: "Do not mix runtime JARs from different Flink minor versions" (`flink-connector.md:50`); Trino supports 469-483 with six modules and a JDK change (`segment.gradle:44-46`). Rate of churn: one Trino release roughly every 1-2 months. | IN | I |
| EC-R42 | IMPL | mudcat integrations shall detect the engine version at load time, fail with the supported range, and allow an explicit override that logs a warning. | `gravitino.trino.skip-version-validation` and the error "Supported versions are %d to %d" (`GravitinoConnectorFactory.java:232-251`; `GravitinoConfig.java:274-278`). | OP | T |
| EC-R43 | GAP | mudcat shall not let engine-side options override connection, routing or credential keys of a catalog. | Spark merge order is catalog `bypass` props, then converted props, then **application options last** (`PropertiesConverter.java:44-61`); the REST-routing path had to add `reapplyReservedRestProperties` so "options can never redirect a routed catalog" (`GravitinoIcebergCatalog.java:226-231`); Trino reserves four keys and logs when ignoring (`docs/trino-connector/catalog-iceberg.md:115-118`); JDBC docs list keys that cannot be overridden (`spark-catalog-jdbc.md:38`). | AC | T |
| EC-R44 | IMPL | mudcat shall reject path-segment names `.`, `..` and empty, and shall percent-encode names in every URL it builds. | MCP: "HTTP clients normalize dot segments, which can target another resource" (`mcp-server/mcp_server/client/plain/utils.py:28-33`); semantic-model docs repeat it (`docs/gravitino-mcp-server.md`, Semantic Models section). | IN | T |
| EC-R45 | GAP | mudcat shall classify each agent-facing tool as read, write or destructive in tool metadata, and an operator shall be able to expose a subset by class. | 36 of 81 MCP tools mutate (create/alter/drop/delete/run/cancel/link/associate/set), including `drop_catalog(force)`, `drop_table(purge)`, `run_job`. Only 7 declare `readOnlyHint` (`tools/domain.py:20-24`; `tools/semantic_model.py:29,56`). The only filter is `--include-tool-tags` by *entity* (`tools/__init__.py:38-55`), so exposing `table` exposes `drop_table`. | AC | T |
| EC-R46 | GAP | mudcat shall reject an unknown tool-selection value at start-up. | `--include-tool-tags` is an allowlist with no validation, and a test pins that an unknown tag exposes zero tools (`main.py:82-85`; `tests/unit/test_tool_tags.py:78-79`). The docs name the tag `statistics` but the code tag is `statistic` (`docs/gravitino-mcp-server.md:129-130` vs `tools/statistic.py:25`), so following the docs hides those tools silently. | OP | T |
| EC-R47 | GAP | mudcat shall provide one agent-facing search operation over names, descriptions and tags that returns identifiers and the commit it read, with a bounded result size. | The 81 tools are entity CRUD. There is no search in `mcp_server/` (`grep search` returns nothing). To find a table an agent must call `get_list_of_catalogs`, then `get_list_of_schemas`, then `get_list_of_tables`, then `get_table_metadata_details`, and every result is a JSON string whose layout lives in the docstring (`tools/table.py:20-90`). | QY | D |
| EC-R48 | GAP | mudcat shall write one audit record for each agent tool call with the principal, tool, target object identifiers, outcome class (denied, failed, succeeded) and the commit it made. | MCP audit records `{timestamp, principal, tool, outcome, error_type, metalake}` only; `outcome` is "deny" for **any** exception (`server.py:68-115`; `core/audit.py:56-110`). No target object, no arguments, no commit. | EV | T |
| EC-R49 | GAP | mudcat shall provide a dry-run mode for mutating agent tools that returns the effect without applying it. | CLI asks "Are you sure ... (Y/N)" unless `--force` (`clients/cli/.../AreYouSure.java:28-44`). The MCP tool path has only a `force` flag, no confirmation or preview (`tools/catalog.py:173-196`). | AC | D |
| EC-R50 | IMPL | mudcat shall forward an agent caller's own credential to the catalog on every network-transport tool call, and shall scope any client cache by principal. | MCP HTTP mode forwards the `Authorization` header verbatim; clients cached per (principal, metalake) and not closed while borrowed (`core/context.py:37,100-135,160-180`). | AC | T |
| EC-R51 | IMPL | mudcat shall let one agent tool server serve several tenants by naming the tenant on each call, and shall record the resolved tenant in the audit record. | Optional `metalake` argument added to every tool schema; a call with conflicting legacy and new arguments is rejected "silently picking one could send a write to the wrong tenant" (`core/middleware.py:43-120`; `audit.py:76-88`). | PL | T |
| EC-R52 | IMPL | mudcat shall return a typed registration status when an engine plugin cannot serve a catalog provider, and shall not abort the engine. | Spark skips with a warning and a hint "It may be served by a separate extension jar" (`GravitinoDriverPlugin.java:176-188`); Trino records UNSUPPORTED with the supported provider list (`CatalogConnectorManager.java:696-707`). | PL | T |

## 3. Quirks and gotchas ledger

- **Spark sees only the catalogs that existed at driver start, and as the wrong user**
  - The plugin lists catalogs once, with the application identity, and writes them into SparkConf. A new catalog needs a new session. A user may see a catalog name they cannot open. `SHOW CATALOGS` still lists only `spark_catalog` until `USE`.
  - Cause: ENGINE (Spark catalog manager has no dynamic registration) + SECURITY.
  - Evidence: `GravitinoCatalogManager.java:272-283` "Loads the relational catalogs visible to the application identity. This runs at driver init"; `docs/spark-connector/spark-connector.md:88-89,129`.
  - Mudcat implication: EC-R5, EC-R6, EC-R17.

- **Trino registers catalogs by calling `CREATE CATALOG` on itself**
  - A connector inside Trino opens a JDBC connection back to the coordinator and issues `CREATE CATALOG` per Gravitino catalog, polling every 10 s. It needs `catalog.management=dynamic`, a reachable coordinator, and `trino.jdbc.*` credentials. Secrets are kept out of the statement and added per node.
  - Cause: ENGINE (Trino has no SPI for a catalog source).
  - Evidence: `CatalogRegister.java:429-468`; `CatalogConnectorManager.java:570-573` "Registration deliberately carries only the visible properties. The resolved secrets are added by each node"; `requirements.md:19`.
  - Mudcat implication: EC-R18, EC-R21. A catalog-sync agent for engines with static catalogs must be a first-class component with health state.

- **The Trino refresh loop can die silently**
  - `scheduleWithFixedDelay` cancels the task forever on the first throw, and loading a plugin can raise `NoClassDefFoundError`, so the loop catches `Throwable`. A dead loop must not look healthy.
  - Cause: ENGINE/CONCURRENCY.
  - Evidence: `CatalogConnectorManager.java:288-293` "Catch Throwable, not Exception: scheduleWithFixedDelay silently cancels the task forever".
  - Mudcat implication: EC-R18 (queryable last-success time and consecutive failures).

- **Catalog change detection by clock, and by name**
  - Trino reloads when `getLastModifiedTime()` is newer; Spark JDBC detects a re-created catalog by comparing `createTime`. Neither has a catalog ID.
  - Cause: MODEL.
  - Evidence: `CatalogConnectorManager.java:721-724`; `GravitinoJdbcCatalog.java:189-193`.
  - Mudcat implication: EC-R15.

- **A reload failure can unregister a working catalog**
  - `reloadCatalog()` unregisters the old connector before registering the new one. If registration then fails, the catalog drops out of Trino, so the code re-checks the live map rather than the pre-try snapshot.
  - Cause: CONCURRENCY.
  - Evidence: `CatalogConnectorManager.java:610-617`, `:726-728`.
  - Mudcat implication: engine sync must register-then-swap, never drop-then-add (design warning; no new requirement).

- **Unqualified catalog names from two metalakes collide**
  - With `catalog-name-with-metalake=false` the first metalake to register a Trino name keeps it. The second is marked failed with a hint.
  - Cause: MODEL (engine has a flat catalog namespace; Gravitino has two levels).
  - Evidence: `CatalogConnectorManager.java:540-560`; `GravitinoConfig.java:155-160`; `docs` warn `DROP CATALOG` "may not work correctly when catalog names carry the metalake" (`GravitinoConnectorFactory.java:225-229`).
  - Mudcat implication: EC-R29, EC-R30 (tenant plus catalog must project to one engine name by a stated rule).

- **Trino lower-cases catalog names; PostgreSQL needs unquoted identifiers**
  - `SHOW CATALOGS` may return quoted names, so the code unquotes and lower-cases before comparing.
  - Cause: ENGINE/MODEL.
  - Evidence: `CatalogRegister.java:481-510`; `docs/trino-connector/catalog-postgresql.md:19-27`.
  - Mudcat implication: EC-R29.

- **Same Gravitino table, three engines, three type fates**
  - Spark widens unsigned ints and maps UUID to string; Hive has no `timestamp_ntz`; Iceberg has no CHAR/VARCHAR(n); Trino Hive caps VARCHAR length. PostgreSQL `timestamp` is read as Spark `TIMESTAMP` by default, so its value moves with the session time zone unless `preferTimestampNTZ=true`; PG `infinity` then fails with `long overflow`.
  - Cause: ENGINE/MODEL.
  - Evidence: `SparkTypeConverter.java:119-144,160-162`; `SparkHiveTypeConverter.java:53-55`; `IcebergDataTypeTransformer.java:43-49`; `HiveDataTypeTransformer.java:62`; `docs/spark-connector/spark-connector.md:210-229`.
  - Mudcat implication: EC-R31.

- **Spark JDBC pushdown can change query results**
  - Pushdown of string, timestamp and some date-part expressions is blocked because the source collation or time zone differs; string equality is sent only as a prefilter and rechecked in Spark; avg() decimals can differ.
  - Cause: ENGINE/STORE.
  - Evidence: `docs/spark-connector/spark-connector.md:173-205` "Some expressions stay in Spark, because the source could return different results"; `jdbc/SparkJdbcPushDownPolicy.java:121`.
  - Mudcat implication: design warning for any query-federation path; EC-R38 for the schema side.

- **Spark JDBC writes to PostgreSQL UUID/external columns are rejected up front**
  - Spark binds them as VARCHAR, which would not preserve the target type, so even NULL writes are refused.
  - Cause: ENGINE.
  - Evidence: `jdbc/SparkJdbcTable.java:133-138`; `docs/spark-connector/spark-connector.md:162-168`.
  - Mudcat implication: EC-R31 (reject, do not coerce).

- **Time travel combines the current Gravitino schema with a historic snapshot**
  - `loadTable(ident, version|timestamp)` loads the Gravitino table at its current definition and the Spark table at the old version.
  - Cause: MODEL (catalog record is mutable, snapshot is not).
  - Evidence: `GravitinoIcebergCatalog.java:293-330`.
  - Mudcat implication: EC-R36.

- **Branch-loaded tables write to the wrong place unless blocked (Spark 4.1)**
  - The 4.1 `SparkTable` constructor always builds the main branch, so write and delete methods would ignore the loaded branch; the wrapper throws on write.
  - Cause: ENGINE (version).
  - Evidence: `spark41/.../SparkIcebergTableBase.java:50-103`.
  - Mudcat implication: EC-R36, EC-R40.

- **Trino cannot `CREATE OR REPLACE TABLE AS SELECT`**
  - Replacing a table gives it a new UUID; the inner Iceberg connector cached the old UUID at plan time and throws "Table UUID does not match".
  - Cause: ENGINE/MODEL.
  - Evidence: `GravitinoMetadata.java:272-281`.
  - Mudcat implication: EC-R37.

- **A failed CTAS drops the table, even if another query now owns it**
  - The connector drops the created table on failure. The table is visible to other queries while the statement runs. If another query writes to it or recreates it, the connector still drops it. Hive managed tables are excepted.
  - Cause: CONCURRENCY.
  - Evidence: `docs/trino-connector/sql-support.md:48`.
  - Mudcat implication: design warning: staged creation with a commit that checks the table ID, not a name-based drop-on-failure.

- **Views: three engines, three behaviours**
  - Trino shows only views with a Trino SQL representation (others "silently invisible"); Flink lists every view but cannot load other dialects; Spark can only read Hive views, materialised on the driver. Trino also refuses a view whose default schema has no default catalog in multi-metalake mode.
  - Cause: MODEL/ENGINE.
  - Evidence: `docs/trino-connector/sql-support.md:58-73`; `flink-common/.../BaseCatalog.java:260-264`; `spark-catalog-hive.md:63-66`; `GravitinoMetadata.java:166-176`.
  - Mudcat implication: EC-R34, EC-R35.

- **`tableExists` lies when the caller lacks read privilege**
  - Returns false on `ForbiddenException` so that `CREATE TABLE IF NOT EXISTS` works for a CREATE-only user.
  - Cause: SECURITY/MODEL (existence check requires LOAD privilege).
  - Evidence: `BaseCatalog.java:466-496` ("See: .../issues/9180").
  - Mudcat implication: EC-R14.

- **Spark SQL-on-file paths collide with table lookup**
  - `SELECT * FROM parquet.\`data.parquet\`` reaches the catalog as a table name with dots or slashes; the Gravitino server splits the name and rejects it, so the connector turns that failure into `NoSuchTableException` to let Spark's file resolver run.
  - Cause: ENGINE/MODEL.
  - Evidence: `BaseCatalog.java:366-392` ("Dots are included because relative filenames such as data.parquet have no path separator").
  - Mudcat implication: EC-R29 (stated name grammar; a name lookup must fail with a typed not-found, not a validation error).

- **Spark `ALTER` batches with zero changes, and default-value ALTERs**
  - Spark sends no change when every requested change is a no-op; the connector returns the loaded table. Timestamp/date/string defaults cannot be combined with schema-changing batches because coercion must resolve against current column types.
  - Cause: ENGINE.
  - Evidence: `BaseCatalog.java:398-420`.
  - Mudcat implication: design warning for batch alter semantics (a batch applies against one starting version).

- **Flink silently dropped computed columns and watermarks (now rejected)**
  - Earlier behaviour: converted to plain physical columns, watermark lost, no error. Now `UnsupportedOperationException` with advice to create a Flink generic table.
  - Cause: MODEL.
  - Evidence: `flink-common/.../BaseCatalog.java:906-946`.
  - Mudcat implication: EC-R32.

- **Flink tables are stored as serialised properties**
  - A Flink "generic table" in the Hive catalog is detected by `connector`/`is_generic` property keys; Flink-created views get a `flink.schema.num-columns` property as the dialect marker.
  - Cause: COMPAT (reuses Hive Metastore convention).
  - Evidence: `hive/FlinkGenericTableUtil.java:30-70`; `docs/flink-connector/flink-catalog-hive.md:100-115`.
  - Mudcat implication: EC-R33.

- **Flink needs one catalog manager per JVM; a second with different config fails**
  - Evidence: `flink-common/.../GravitinoCatalogManager.java:117-125` "Creating GravitinoCatalogManager with different configuration is not supported". Spark does the same: `GravitinoCatalogManager.create` fails on duplicate (`spark-common/.../catalog/GravitinoCatalogManager.java:174`).
  - Cause: ENGINE.
  - Mudcat implication: design warning: client libraries should not be JVM singletons; two tenants in one process must work.

- **ServiceLoader failures are swallowed for optional providers (Flink)**
  - `ServiceConfigurationError`, `NoClassDefFoundError` and `LinkageError` are logged at debug while scanning factories, so a provider built against another minor disappears instead of erroring.
  - Cause: ENGINE (version).
  - Evidence: `store/GravitinoCatalogStore.java:211-225`.
  - Mudcat implication: EC-R41, EC-R52 (surface "provider unavailable and why").

- **REST auth was never propagated when loading a persisted Flink catalog (#11601)**
  - A REST/JDBC Iceberg catalog loaded from the store had `catalogBackend == null`, so auth was not copied to the Iceberg REST client. Fixed by propagating the Gravitino client auth to the REST client.
  - Cause: MODEL (two servers, two auth contexts).
  - Evidence: `flink-common/.../GravitinoIcebergCatalogFactory.java:122-128,152-155`; Trino: IRC credential is "not reused automatically ... surfaces as an authentication error rather than a missing-configuration error" (`docs/trino-connector/catalog-iceberg.md:100-113`).
  - Mudcat implication: EC-R3, EC-R9: one credential context for catalog and gateway, or a clear error when they differ.

- **Iceberg routing can strand static storage credentials**
  - A hive/jdbc-backed Iceberg catalog with an `s3`/`gs`/`abfs` warehouse and no `credential-providers` fails at initialisation, because routing replaces static keys with vended ones.
  - Cause: SPEC/COMPAT.
  - Evidence: `GravitinoIcebergCatalog.java:165-177`.
  - Mudcat implication: EC-R8.

- **A user token's identity for cache partitioning is read without verification**
  - JWT `sub` (or configured fields) read unverified; cache key must be "at least as fine-grained as the principal the server derives". An opaque token is hashed.
  - Cause: SECURITY.
  - Evidence: `catalog/GravitinoIdentity.java:26-39,86-100`.
  - Mudcat implication: EC-R10, EC-R50 (client caches keyed by principal; finer is safe, coarser leaks).

- **A session that sets the same token as the application is treated as inheriting it**
  - Spark copies application conf into each session, so "a session value equal to the application's is treated as inherited even when the session set it deliberately".
  - Cause: ENGINE.
  - Evidence: `GravitinoDriverPlugin.java:462-482`.
  - Mudcat implication: design warning: do not infer "who set it" from value equality.

- **Client version check rule accepts every older major**
  - `left[0] < right[0] || (== && left[1] <= right[1])`. The Java check runs lazily in a pre-connect handler on first request, Python checks in the constructor.
  - Cause: COMPAT.
  - Evidence: `GravitinoVersion.java:51-54`; `GravitinoClientBase.java:111-113`; `gravitino_client_base.py:76-77`.
  - Mudcat implication: EC-R27.

- **The server shapes responses by the client version (fileset subPath)**
  - Clients before 1.0 percent-encode `subPath`; the server decodes for them. A missing header is treated as current.
  - Cause: COMPAT.
  - Evidence: `FilesetOperations.java:257-259`; `server-common/.../Utils.java:382-390`.
  - Mudcat implication: EC-R28.

- **Python and Java clients behave differently on the same server**
  - Timeouts 10 s vs 180 s; Python has no retry, no pool, and a new `urllib` opener per request; Java retries once.
  - Cause: OTHER.
  - Evidence: `constants/timeout.py:18`, `http_client.py:255`; `GravitinoClientConfiguration.java:56,59,109`.
  - Mudcat implication: EC-R26.

- **Concurrent-change conflict has a code but no precondition**
  - 1012 is returned on concurrent alter/drop/link; "An idempotent delete may instead return `false` if another delete or rename wins".
  - Cause: CONCURRENCY.
  - Evidence: `ErrorConstants.java:61`; `docs/how-to-use-gravitino-client.md:185-210`.
  - Mudcat implication: EC-R24.

- **MCP tools: unknown tag hides everything; docs and code tag names differ**
  - See EC-R46. Evidence: `tests/unit/test_tool_tags.py:78`, `docs/gravitino-mcp-server.md:129` vs `tools/statistic.py:25`.
  - Cause: OTHER.

- **MCP: 'deny' means any exception**
  - Audit `outcome="deny"` for timeouts and bugs as well as authorisation failures; the module docstring admits it.
  - Cause: OTHER.
  - Evidence: `server.py:107-114`; `core/audit.py:79-82`.
  - Mudcat implication: EC-R48.

- **MCP write timeouts leave the operation in doubt**
  - On read timeout the tool says "The operation may still complete on the server; check its result before retrying."
  - Cause: CONCURRENCY.
  - Evidence: `client/plain/plain_rest_client_operation.py:96-104`.
  - Mudcat implication: EC-R25.

- **Server error bodies carry stack traces**
  - Trino keeps the first non-blank line of the message because REST failures "carry the server stack trace after it".
  - Cause: SECURITY.
  - Evidence: `GravitinoErrorCode.java:182-189`.
  - Mudcat implication: EC-R22.

## 4. Open questions / inconsistencies / half-implemented

- Iceberg REST discovery for Spark is partly shipped: the design doc says "an Iceberg provider and its catalog-listing endpoint remain future work" and only Lance is implemented (`design-docs/spark-rest-catalog-registration.md:72-73`). The Iceberg path in `GravitinoLakehouseRESTDiscoveryPlugin` listing call authenticates with static token or headers only, not the OAuth2 flow (`:218-224`). One URI per format per session (`:362-363`). Flink and Trino reuse is "future work" (`:356-361`).
- Spark: `todo: support default value` for column defaults on create (`BaseCatalog.java:833`) while `SparkTableChangeConverter` handles `SET DEFAULT` and says "DROP DEFAULT is not supported by Gravitino" (`SparkTableChangeConverter.java:176`).
- Spark `alterNamespace` accepts only `SetProperty`; every other namespace change throws (`BaseCatalog.java:572-582`).
- Trino: three `TODO`s for property converters ("Need to improve schema table and column properties") (`HiveConnectorAdapter.java:98`, `IcebergConnectorAdapter.java:197`, `IcebergPropertyMeta.java:57`); hidden columns not retrieved (`GravitinoMetadata.java:199`).
- Trino docs say only `catalog.management=dynamic` works, yet `GravitinoConfig` defaults it to `static` (`GravitinoConfig.java:64`); the default connector is unusable without a coordinator edit (inferred from `requirements.md:19`).
- Flink view listing returns views of all dialects (`BaseCatalog.java:260-264` TODO: "Consider filtering ... requires per-view property inspection which is expensive").
- Flink supports `OAuth2` and `Basic` auth only; Spark supports five types including Kerberos; the Trino `forwardUser` path supports only simple and oauth2 (`GravitinoAuthProvider.java:244`). Three engines, three auth sets (`GravitinoCatalogManager.java:57-93` in Flink).
- Spark `SHOW CATALOGS` shows only `spark_catalog` (`docs/spark-connector/spark-connector.md:129`): catalog listing is not discoverable from SQL.
- Paimon is Spark 3.5 only and FilesystemCatalog on HDFS only (`docs/spark-connector/spark-catalog-paimon.md:18,47`); "Returning user-specified configs is not supported" (`:32`).
- The Python client's `check_version` and the Java client's disagree on when they run (constructor vs first request). Java's `getVersion()` is deprecated in favour of `serverVersion()` (`GravitinoClientBase.java:180-190`).
- Error 1013 `SERVICE_UNAVAILABLE` and 1011 `UNAUTHORIZED` exist in `ErrorConstants.java:58,64`; the Java client maps `ServiceUnavailableException` only in one handler (`ErrorHandlers.java:1492`) (inferred: other handlers fall to the default).
- MCP depends on a pinned `fakeredis<2.38.0` because a transitive `pydocket` is broken (`mcp-server/pyproject.toml`, TODO #10754), and a Pydantic floor to keep legacy argument aliases working. A tool server pinned to `fastmcp==3.4.5` has no stable contract for tool names beyond the legacy `metalake_name` alias in `core/middleware.py:51`.
- MCP semantic-model tools expose "AI context" on metrics and are the only ones marked read-only with idempotent hints (`docs/gravitino-mcp-server.md`, Semantic Models; `tools/semantic_model.py:29`). It is the one place the server is shaped for LLM retrieval; the table tools are not.

## 5. Relevance notes for mudcat (not requirements)
- mudcat IN: engines converge on Iceberg REST as the only protocol that carries vended credentials. mudcat's Iceberg REST gateway should be the *default* engine path; a mudcat Spark/Trino/Flink plugin is needed only for non-Iceberg formats. Budget the cost of plugins: Gravitino carries 3 Spark, 6 Trino, 4 Flink build modules for roughly 3 engines.
- mudcat QY: engine integrations answer "list, load, create, alter, drop" only. None answer "which replica, which version, who may use for what". mudcat's resolution API will need a thin engine adapter that returns table plus credential plus commit in one response, because Gravitino's two-step (metadata from catalog, I/O with static secrets) is the source of EC-R8, EC-R14 and EC-R36.
- Agent retrieval: the MCP server is a REST-to-tool adapter with 81 CRUD tools and no search; an agent-facing mudcat tool surface should be small, read-first, search-led, and classify writes (EC-R45, EC-R47, EC-R48).
