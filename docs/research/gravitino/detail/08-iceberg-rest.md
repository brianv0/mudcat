> **Research note, not a requirement document.** Drafted by a code-reading agent from Apache Gravitino commit `9aa78cf` (2026-11-26), then spot-checked by the lead. Line numbers can drift by a few lines; search by file and symbol. Items marked `(inferred)` were not confirmed in code. The text does not follow Simplified Technical English. See [../README.md](../README.md) for the synthesis.

# Iceberg REST Catalog Server (Gravitino `iceberg/**`)  (ref prefix IR)

Paths are repo-relative to `/home/user/apache/gravitino`; `.../` abbreviates a long Java package path. "IRC" means the Iceberg REST Catalog server. "(inferred)" marks a conclusion drawn from code shape or from the absence of a symbol (grep), not from a quoted line.

## 1. Decomposition

### 1.1 What it is
The IRC (Iceberg REST Catalog) server is a Jersey/Jetty service that runs either as a **standalone process** or as an **auxiliary service** inside the Gravitino server (`iceberg/iceberg-rest-server/.../iceberg/RESTService.java:47` `SERVICE_NAME="iceberg-rest"`, servlet at `/iceberg/*`). It is a **thin adapter over the upstream Iceberg library**: almost every operation is `org.apache.iceberg.rest.CatalogHandlers.*` run against a real Iceberg `Catalog` object (JDBC, Hive, in-memory, custom class, or a remote REST catalog). The Iceberg catalog backend is the **source of truth for tables**. Gravitino's own entity store holds only a *mirror* (IDs, owners, tags, privileges), filled by hooks, and only when authorization is enabled (`RESTService.java` wires `IcebergTableHookDispatcher` only `if (authorizationContext.isAuthorizationEnabled())`).

### 1.2 Module tree
```
iceberg/
  iceberg-common/                 library shared with the Gravitino "lakehouse-iceberg" catalog
    ops/IcebergCatalogWrapper     CatalogHandlers facade: validation, purge, register, cache (1057 lines)
    utils/IcebergCatalogUtil      backend factory: MEMORY|JDBC|HIVE|REST|CUSTOM, GCS token mint, JDBC V1 migration race
    IcebergConfig                 all gravitino.iceberg-rest.* keys (idempotency, cache, cleanup, REST-backend auth)
    cache/                        TableMetadataCache (Caffeine, validated against metadata pointer)
    org/apache/iceberg/jdbc|hive|memory/*WithMetadataLocationSupport   classes planted in Iceberg's own packages
    utils/IcebergTableLocationUtils  location normalisation + overlap test + SQL LIKE patterns
  iceberg-rest-server/            the service
    RESTService                   wiring; per-resource dispatcher chain Event -> Hook -> Executor
    service/rest/                 JAX-RS resources: Config, Namespace, Table, TableRename, View, ViewRename, Health
    service/IcebergCatalogWrapperManager   Caffeine cache of one CatalogWrapperForREST per catalog name
    service/CatalogWrapperForREST          credential vending, scan planning, commit validation hook
    service/FederatedCatalogWrapper        REST backend (IRC1 -> IRC2): token forwarding, credential rewrite
    service/IcebergCommitValidation        server-side semantic checks, injected as an UpdateRequirement
    service/IcebergExceptionMapper         exception -> HTTP status table; storage-SDK message scrubbing
    service/idempotency/                   IdempotencyStore SPI, InMemory + Jdbc stores, IcebergIdempotencyManager
    service/cleanup/                       async purge: job table, worker pool, heartbeat lease
    service/dispatcher/                    Executor (talks to wrapper), HookDispatcher (entity import, owner), EventDispatcher
    service/provider/                      IcebergConfigProvider: static-config-provider | dynamic-config-provider
    service/restriction/                   IcebergReadRestrictionLoader SPI (ServiceLoader, no-op default)
    service/metrics/                       report queue + Dummy/JDBC store
    server/web/filter/*AuthzHandler        per-endpoint authorization beyond annotations (location overlap, rename)
  iceberg-rest-server-experiment/ same server on a forked Iceberg (com.datastrato 1.13.0) + tag-based read restrictions
  iceberg-rest-trino-it/          Trino smoke + authorization ITs
design-docs/: iceberg-idempotency-key, async-iceberg-rest-hard-deletion, iceberg-rest-server-side-scan-planning,
  iceberg-supported-nested-namespace, tag-based-read-restrictions-for-iceberg-rest, 3 maintenance-job designs
```

### 1.3 Request path
`HTTP -> IcebergAuthenticationFilter (JSON errors) -> RequestContextFilter/HttpAuditFilter -> [authz method interceptor: path params -> Gravitino identifiers -> expression] -> resource method -> IcebergIdempotencyManager.replayOrExecute (mutations only) -> Utils.doAs(principal) -> EventDispatcher -> HookDispatcher (authz mode only) -> Executor -> CatalogWrapperForREST -> Iceberg Catalog -> backend`.
Authorization runs **before** idempotency replay (annotation interception on the resource method), the replay runs inside the method body.

### 1.4 Mapping to the Gravitino core model (answers "separate catalog?")
- **Catalog routing**: IRC `prefix` (path) / `warehouse` (query on `/v1/config`) = **name of a Gravitino catalog with provider `lakehouse-iceberg`** (`provider/DynamicIcebergConfigProvider.java` `resolveProps`: `"lakehouse-iceberg".equals(catalog.provider())`). Namespace = Gravitino *schema*; nested levels are joined with a configurable separator (default `:`) into one schema name, stored with `\u0001` (`core/.../utils/HierarchicalSchemaUtil.java` header). Table = Gravitino table.
- **Not a separate catalog**: the Gravitino catalog and IRC share one Iceberg backend (same JDBC `iceberg_tables`, same warehouse). A table made through IRC appears in Gravitino by **lazy import** (`TableOperationDispatcher.importTable`, `core/.../catalog/TableOperationDispatcher.java:204`), and IRC calls the same import eagerly after create/register/commit (`IcebergTableHookDispatcher.importTableAndSetOwner`).
- **Two IDs**: Iceberg `table-uuid` (inside metadata) and the Gravitino entity id; rename keeps the entity id (`IcebergTableHookDispatcher.java` rename: `.withId(tableEntity.id())`).
- **Config provider**: `static-config-provider` reads catalogs from the server file, fixed at startup; `dynamic-config-provider` reads catalog properties from the Gravitino catalog manager (in-process in aux mode, via HTTP client otherwise). Authorization requires dynamic (`IcebergCatalogWrapperManager.java` `createCatalogWrapper`: "Authorization is enabled. Set ... dynamic-config-provider").
- **Reconfiguration**: wrapper cache with expire-after-access (`iceberg-rest.catalog-cache-eviction-interval`, default 3600000 ms, `IcebergConfig.java:219-224`), plus a catalog-removed listener in aux mode, plus GCS token expiry cap (`IcebergCatalogWrapperManager.java` `GCS_TOKEN_REFRESH_BUFFER_MS` 5 min).

### 1.5 Extension points and key config
SPIs: `IcebergConfigProvider`, `IdempotencyStore` (`idempotency-store-type` = `in-memory`|`jdbc`|class name), `IcebergMetricsStore`, `TableMetadataCache` (`table-metadata-cache-impl`), `ScanPlanCache` (`scan-plan-cache-impl`), `IcebergReadRestrictionLoader` (ServiceLoader), JAX-RS `extension-packages` (`IcebergConfig.REST_API_EXTENSION_PACKAGES`), credential providers (`credential-providers`), custom Iceberg `Catalog` (`catalog-backend=custom` + `catalog-backend-impl`).
Keys: `catalog-backend`, `warehouse`, `catalog-config-provider`, `default-catalog-name`, `idempotency-enabled` (default false), `idempotency-key-lifetime` (PT30M), `async-cleanup.*` (workers 2, delete-threads 4, batch 1000, poll 5 s, heartbeat-timeout 300 s, max-attempts 5, retention 720 h), `rest-backend-auth-type` (`user-token`|`service-account`), `rest-backend-storage-credential-source` (`remote`|`local`), `disable-rest-authz` (default true), `jdbc.strict-mode` (default true).

### 1.6 Spec surface actually served
`/v1/config` advertises (`rest/IcebergConfigOperations.java:66-99`): namespace list/load/create/update/delete/exists; table list/load/create/update/delete/rename/exists/register; report-metrics; table-credentials; view list/load/create/update/delete/rename/exists/**register**; scan-plan **submit** only when the backend supports it. **Not served**: multi-table commit (`/transactions/commit`), async plan fetch/cancel. `docs/iceberg-rest-service.md:41-44` lists "Multi-table transactions" and "View registration" as not implemented, but register-view is implemented and advertised (see 4.1).

## 2. Candidate requirements

Kind: IMPL = Gravitino does it; GAP = Gravitino omits or gets it wrong and mudcat should do better.

| Ref | Kind | Requirement (mudcat style) | Rationale / Gravitino evidence | Area | Ver |
|---|---|---|---|---|---|
| IR-R1 | IMPL | mudcat shall list in the Iceberg config response only the endpoints that it serves for that catalog. | Clients gate on the advertised set (pyiceberg refuses missing `.../tasks`, `design-docs/iceberg-rest-server-side-scan-planning.md:42-52`). Gravitino builds the list per backend: `IcebergConfigOperations.java:138-148`; test `TestIcebergConfigEndpointGating`. `.../tasks` route exists but is deliberately unadvertised (`CatalogWrapperForREST.java:646-650`). | IN | T |
| IR-R2 | GAP | mudcat shall pass the upstream Iceberg REST compatibility test suite on each release, using the published conformance kit. | No kit or upstream `CatalogTests` reference exists in `iceberg/**` tests (grep for `CatalogTests`, `RESTCompatibilityKit`, `conformance` found nothing) (inferred). Gravitino relies on ~90 hand-written unit tests and engine ITs (Spark, Trino), so spec drift is found by users (see 3.1-3.8). | IN | T |
| IR-R3 | GAP | mudcat shall serve the multi-table commit endpoint and apply all table changes of one request in a single atomic transaction. | Not implemented: `docs/iceberg-rest-service.md:43`; no route in `service/rest/`. Gravitino's Iceberg-library backends cannot do it; mudcat's single PostgreSQL store can. | IN | T |
| IR-R4 | IMPL | mudcat shall map each catalog failure to the HTTP status that the Iceberg REST specification gives for that failure, and return the standard error object with type, code and message. | `IcebergExceptionMapper.java:65-97` explicit table (e.g. `CommitFailedException`->409, `NamespaceNotEmptyException`->409, `NoSuchPlanTaskException`->404, `ServiceUnavailableException`->503, auth timeout->419). Subclasses inherit the parent status (`:111-124`). | IN | T |
| IR-R5 | IMPL | mudcat shall not return storage-SDK, parser or stack-trace text to the client in an error response. | SDK errors become fixed-text 400 or 503 (`IcebergExceptionMapper.java:233-274`: "The SDK message is never returned: it carries request ids"). JSON errors become a field-path message (`:209-231`). Stack trace only if `includeErrorStackTrace` (`IcebergRESTUtils.java:118, 526, 563-585`). | IN | T |
| IR-R6 | IMPL | mudcat shall answer a malformed or incomplete request body with status 400 and never with 500 or 406. | Interceptor converts parser `IllegalArgument/UnsupportedOperation/IllegalState/NPE` (`IcebergRequestBodyReaderInterceptor.java:36-52`); `IcebergRESTUtils.validateRequest` also binds partition and sort transforms (`:538-560`); tests `testInvalidRequestBodiesReturnBadRequest`, `testCreateWithInvalidTruncateReturnsBadRequest`. | IN | T |
| IR-R7 | IMPL | mudcat shall decode a multi-level namespace in a path or in the `parent` query using the unit-separator code point U+001F, URL-encoded as `%1F`. | `IcebergRESTUtils.java:98` `"%1F"`; used in every resource (`IcebergNamespaceOperations.java` `listNamespaces` decodes `parent`). Iceberg 1.11 allows a configurable separator; Gravitino pins it for wire stability (`IcebergRESTUtils.java:91-97`). | IN | T |
| IR-R8 | GAP | mudcat shall store a namespace as an ordered list of levels, so that levels containing '.' or ':' are distinct, legal and unambiguous. | Gravitino flattens levels with a separator (default `:`) and rejects '.' in any level (`IcebergRESTUtils.java:143-153`, `IcebergNamespaceHookDispatcher.java:122-140`) and, with authorization on, the separator too, because `["a:b"]` and `["a","b"]` would share owner and privileges: "whoever creates one would take over the other" (`IcebergNamespaceOperationExecutor.java:176-195`). Max flattened length is a column limit (`:136-140`). Both are Iceberg-valid names that Gravitino refuses. | ST | T |
| IR-R9 | IMPL | mudcat shall check every commit requirement against the newest committed state and answer 409 when a requirement fails. | `CatalogHandlers.updateTable` plus extra requirement `IcebergCommitValidation` "evaluates against the exact metadata the commit is built on, again on every retry" (`IcebergCommitValidation.java:64-70`); stale snapshot stays a conflict (`testStaleSnapshotRemainsACommitConflict`). | ID | T |
| IR-R10 | IMPL | mudcat shall check the semantic validity of a commit's updates before it accepts them: refs, removed snapshots, schema evolution, partition field IDs, default spec and sort order, known transforms. | Iceberg "applies some metadata updates without checking them" and leaves a table nobody can load (`IcebergCommitValidation.java:64-66`). Checks: snapshot removal vs refs (`:289-296`), `main` cannot be a tag (`:573`), field-id reuse and promotion rules (`:379-470`), removed spec still used by manifests (`:332-355`), default spec/sort id must exist else NPE (`IcebergCatalogWrapper.java:960`). | ST | T |
| IR-R11 | IMPL | mudcat shall assign the table's next partition-field ID to an added partition field that carries no `field-id`. | Spec makes `field-id` optional but Iceberg renumbers from 1000 and collides: `assignPartitionFieldIds` (`IcebergCommitValidation.java:147-215`); test `testAddSpecWithoutFieldIdsGetsTheTablesIds`. | ST | T |
| IR-R12 | IMPL | mudcat shall move a table's current-metadata pointer by compare-and-swap on the previous pointer and report 409 when the swap fails. | JDBC backend: `UPDATE ... WHERE metadata_location = old`, `updatedRecords == 1` else `CommitFailedException` (`iceberg-common/.../iceberg/jdbc/JdbcCatalogWithMetadataLocationSupport.java:199-216`). Cached metadata is re-validated against the pointer on each read (`cache/BaseTableMetadataCache.java:48-70`), which is what makes a multi-node cache safe. | ID | T |
| IR-R13 | IMPL | mudcat shall reject an Idempotency-Key that is not a canonical RFC 9562 UUIDv7 with status 400, and shall treat letter case of the key as irrelevant. | `IdempotencyKeys.java:55-107` (length 36, version 7, variant 10), lower-case fold to keep a retry with different case from re-running (`:84-99`). | IN | T |
| IR-R14 | IMPL | mudcat shall replay the stored response for a repeated Idempotency-Key with the same method, path and query, and answer 409 with Retry-After while the first request is still running. | `IcebergIdempotencyManager.java:145-199, 249-280`; 409 for a key used on another operation (`:262-269`); tests `TestIcebergIdempotencyManager` (`testRetryReplaysTheStoredResponse`, `testRetryWhileTheFirstRequestIsRunningIsRetryable`). | IN | T |
| IR-R15 | IMPL | mudcat shall advertise the idempotency key lifetime in the config response if and only if idempotency is enabled. | `IcebergConfigOperations.java:132-134`: absence of the field means "not supported" to clients. | IN | T |
| IR-R16 | IMPL | mudcat shall keep the key reusable after a 5xx, 401, 403 or 419 response and shall store and replay every other terminal response. | `NON_FINALIZABLE_STATUSES = {401,403,419}` and `< 500` rule (`IcebergIdempotencyManager.java:91, 186-190`): auth outcomes depend on credentials, not catalog state. | IN | T |
| IR-R17 | GAP | mudcat shall compare a digest of the request body when it replays an idempotency key and shall refuse a replay whose body differs. | Gravitino compares only method + path + sorted query: "Request bodies and behavior-changing headers are not compared ... replays the first response" (`IcebergIdempotencyManager.java:66-68`). A client bug that reuses a key for a different create body gets a silent success for the wrong body. | IN | T |
| IR-R18 | GAP | mudcat shall scope an idempotency record to the tenant and principal that created it. | Table has `idempotency_key` as sole primary key; no principal or catalog column (`scripts/postgresql/schema-2.0.0-postgresql.sql:1437-1447`); docs say "Keys must be globally unique" (`docs/iceberg-rest-service.md:193`). Authorization runs first, but a second authorised principal replaying a key on the same path receives the first principal's stored body (inferred). | AC | T |
| IR-R19 | GAP | mudcat shall not store or replay credentials that a response carries. | Replay stores `response.getEntity()` JSON in `response_summary` (`IcebergIdempotencyManager.java:286-294`); create-table with `X-Iceberg-Access-Delegation` returns vended credentials in that entity (`CatalogWrapperForREST.injectCredentialConfig:374-389`). So short-lived tokens are written to the database and replayed after they may have expired (inferred). | RD | I |
| IR-R20 | GAP | mudcat shall record the idempotency outcome in the same database transaction that commits the mutation. | Reservation, mutation and finalisation are three separate commits; "eviction, expiry, a crash, or a post-commit exception can permit re-execution" (`IcebergIdempotencyManager.java:67-71`); a lost ack leaves a reservation that blocks retries until expiry (`:167-177`). Single PostgreSQL makes atomic commit possible. | ID | T |
| IR-R21 | GAP | mudcat shall take every idempotency-expiry decision from the database clock. | "Replicas must synchronize system clocks: a fast clock can reclaim a key another replica still considers live. There is no clock-skew grace period" (`JdbcIdempotencyStore.java:50-58`); store takes `Clock.systemUTC()` (`:67-70`). Lease must also exceed mutation duration, no heartbeat (design 5.4). | OP | A |
| IR-R22 | IMPL | mudcat shall issue vended credentials only when the X-Iceberg-Access-Delegation list names `vended-credentials`, and shall ignore mechanisms it does not offer. | `IcebergTableOperations.isCredentialVending:784-794` (case-insensitive, comma list); `testAccessDelegationListIsServed`. Failing on unknown mechanism would break clients that send `vended-credentials,remote-signing`. | RD | T |
| IR-R23 | IMPL | mudcat shall scope each issued credential to the table's location, data-write location and metadata-write location, and to read or write according to the caller's privilege on the table. | `CatalogWrapperForREST.getCredential:403-425` (paths = `location`, `write.data.path`, `write.metadata.path`); privilege chosen by a live authorization check `IcebergTableOperationExecutor.java:218-231` (`writable ? WRITE : READ`). | RD | T |
| IR-R24 | IMPL | mudcat shall return issued credentials in the standard credential list with a refresh endpoint that names the credentials resource of the same table. | `injectCredentialConfig` adds both `storage-credentials` and flattened config for old clients (`CatalogWrapperForREST.java:374-389`); refresh path `v1/{catalog}/namespaces/{ns}/tables/{table}/credentials` (`common/.../CredentialPropertyUtils.java:282-304`); refresh keys differ for S3, GCS, ADLS and are omitted for OSS because the client ignores them (`IcebergRESTUtils.java:104-114`). | RD | T |
| IR-R25 | GAP | mudcat shall answer the credentials endpoint with an error, not with an empty list, when it cannot issue credentials. | `getTableCredentials` catches `ServiceUnavailableException` and returns `200 {}` (`CatalogWrapperForREST.java:263-266`); load-table silently omits credentials for local/HDFS or uncovered paths (`:445-455`). The client then fails later on the data read with a storage error. | RD | T |
| IR-R26 | IMPL | mudcat shall require load access to every existing table whose location equals, contains or lies inside an explicit location named in a create, staged create, register or set-location request. | `CreateTableAuthzHandler.java:47-60`; `IcebergTableLocationAuthzHelper.authorizeLocation`; segment-boundary overlap (`table` vs `table2` do not overlap) `IcebergTableLocationUtils.overlaps`; docs `iceberg-rest-service.md:180-197`. Reason: a credential is scoped to a prefix, so a table created over another table's prefix would be issued that table's data. Denial returns 403 without naming the source table. | AC | T |
| IR-R27 | GAP | mudcat shall find overlapping table locations with one indexed database query on every backend. | JDBC uses one `LIKE ... ESCAPE '!'` query over pointers (`JdbcCatalogWithMetadataLocationSupport.java:70-130`); all other backends recursively list every namespace and table, and read metadata when no pointer is available (`RegisterTableSourceResolver.java:105-170`), O(catalog). Non-standard metadata layouts are "outside this lookup's supported layout" (`docs:195`). | PF | T |
| IR-R28 | IMPL | mudcat shall refuse a register-with-overwrite whose metadata names a different table UUID than the destination table. | `CatalogWrapperForREST.registerTable:192` -> `checkOverwriteKeepsTableUuid:902`: "Iceberg would replace the table and then fail the request when it reads the replaced table back". | ID | T |
| IR-R29 | IMPL | mudcat shall create no catalog entry for a staged create, and shall create the table, its ID and its owner only when the staged commit (update with assert-create) succeeds. | `IcebergCatalogWrapper.createTable:301` -> `CatalogHandlers.stageTableCreate`; hook skips import/owner for staged (`IcebergTableHookDispatcher.java:83-87`) and does it on commit (`:100-115`); validation of the create runs as `CreateValidation` (`IcebergCommitValidation.java:122-126`). | ID | T |
| IR-R30 | IMPL | mudcat shall, on a drop with purge, delete every file reachable from the dropped table's metadata even when part of that metadata cannot be read. | Iceberg drops the entry then gives up on the first unreadable manifest list; Gravitino reads metadata first, drops, then falls back to `IcebergPurgeUtil.deleteReachableFiles`, honouring `gc.enabled=false` (`IcebergCatalogWrapper.java:322-348`, `IcebergPurgeUtil.java:44-90`). | UP | T |
| IR-R31 | IMPL | mudcat shall delete the bytes of a purged table through durable background jobs that survive restart, run on any replica, and delete leaf files before their parents. | `iceberg_cleanup_job` rows, CAS claim, heartbeat lease with CAS on last-written value (`design-docs/async-iceberg-rest-hard-deletion.md:286-470`); stores only `metadata_location`, not the file list; deletes by reachability from that old metadata so a recreated table is never touched (`IcebergTableOperationExecutor.java:131-155`). | UP | T |
| IR-R32 | IMPL | mudcat shall refuse to create or register a table under a name whose purge job is still running, with status 409. | `IcebergCleanupHelper.rejectIfBeingPurged:56-75` called from create and register executors; reason: default location `<warehouse>/<ns>/<table>` is reused by a recreate (design 5.7). | UP | T |
| IR-R33 | GAP | mudcat shall apply the purge-in-progress name block to every operation that can bind the name: create, register, staged-create commit and rename destination. | `rejectIfBeingPurged` has only two call sites (`IcebergTableOperationExecutor.java:77`, `IcebergNamespaceOperationExecutor.java:149`; grep). Staged-create commit (`updateTable`) and rename destination skip it (inferred). | UP | T |
| IR-R34 | GAP | mudcat shall keep a failed byte deletion visible, retryable and attached to the surviving record. | After `max-attempts` the job is `FAILED`, "the tombstone also lifts ... any files the job did not delete are now an orphaned leak ... no automatic re-drive" (design 5.6, `async-iceberg-rest-hard-deletion.md:496-505`). mudcat's principle "the record outlives the bytes" implies the failed state must block nothing but stay queryable and re-drivable. | UP | T |
| IR-R35 | IMPL | mudcat shall filter list responses by the caller's authorization before it paginates. | `IcebergTableOperations.listTable:159-170`, `IcebergNamespaceOperations.listNamespaces` same order; page token = last name (`IcebergPaginationHelper.java:97-125`). Without `pageToken` all results return even if `pageSize` is set, as the spec says. | AC | T |
| IR-R36 | GAP | mudcat shall page list endpoints by keyset in the database and shall not materialise the full list per page. | "full item list is materialized ... on every page request ... O(N^2) total work" (`IcebergPaginationHelper.java:43-47`); also misses items created before the cursor. | PF | T |
| IR-R37 | IMPL | mudcat shall ignore, not reject, a client write to a catalog-reserved property, and shall set the owner property from the authenticated principal. | `IcebergOwnerPropertyGuard.java:34-80`: Spark sends `owner` itself, rejecting would fail its statements; create overrides `owner` to the principal (`IcebergTableOperationExecutor.java:80-104`). | AC | T |
| IR-R38 | IMPL | mudcat shall require ownership of the source table and create privilege on the destination namespace for a rename across namespaces. | `RenameTableAuthzHandler.java:137-198`; same-namespace rename needs only modify privilege (`IcebergTableRenameOperations` annotation `MODIFY_TABLE_AUTHORIZATION_EXPRESSION`). | AC | T |
| IR-R39 | IMPL | mudcat shall resolve the Iceberg `warehouse` config parameter and the optional path prefix to exactly one catalog, and shall use a configured default catalog when both are empty. | `IcebergConfigOperations.getConfig:116-131` (`defaults.prefix = warehouse`), `IcebergRESTUtils.getCatalogName:623-630`, route `/v1/{prefix:([^/]*/)?}...`; prefix with '.' rejected (`IcebergMetadataAuthorizationMethodInterceptor.java:112`). | PL | T |
| IR-R40 | IMPL | mudcat shall apply a change to a catalog's storage configuration without a server restart. | Wrapper cache evicted by catalog-removed listener and by TTL (`IcebergCatalogWrapperManager.java:75-100`). Weaker for the static provider ("couldn't be changed", `docs:461`). | OP | D |
| IR-R41 | IMPL | mudcat shall return an entity tag for a loaded table that is derived from the commit pointer, the snapshot-view parameter and any caller-specific response state, and shall answer 304 when the tag matches. | `IcebergRESTUtils.generateETag` SHA-256 of metadata location + `snapshots` + representation seed (`:~330-372`); fast path skips table load but is disabled when a restriction loader may vary the response (`IcebergTableOperations.java:377-406`); `testLoadTableETagDiffersForSnapshotsParam`. | ID | T |
| IR-R42 | GAP | mudcat shall refuse table metadata with an active row filter or column mask to a client that has not declared that it enforces them. | Design says 406 without the capability header, 403 for an untrusted reader (`tag-based-read-restrictions-for-iceberg-rest.md:746-800`), but `X-Gravitino-Client-Capabilities` appears nowhere in code or docs (grep); `ExperimentalIcebergReadRestrictionLoader.load` returns restrictions unconditionally. | AC | T |
| IR-R43 | GAP | mudcat shall apply read restrictions on every path that returns data locations or credentials: load table, scan plan and credentials. | Loader is called only in `IcebergTableOperations.loadTable`; `planTableScan` and `getTableCredentials` use no loader (grep `readRestrictionLoader` shows RESTService, IcebergTableOperations, loader interface only). Design item 9 says governed credentials must not bypass restrictions (`:849-852`). | AC | T |
| IR-R44 | IMPL | mudcat shall fail closed when an applicable restriction cannot be resolved, and shall reject two different row filters on one table. | `ExperimentalIcebergReadRestrictionLoader.resolveForLoad` wraps all errors into one generic failure; `resolveRowFilter` "Conflicting row-filter policies"; unknown group is not "non-member" (`IdentityBindingContext.resolveGroupMembership`). | AC | T |
| IR-R45 | IMPL | mudcat shall resolve effective tags of a table and of each column, including tags inherited from containing namespaces, into restrictions bound to column field IDs. | Design "Target scope and tag inheritance" (`tag-based-read-restrictions-for-iceberg-rest.md:92-125`); loader binds by `field.fieldId()` and lists policies for table + all columns in one call (`ExperimentalIcebergReadRestrictionLoader.java:~100-125`). A policy bound to a non-matching inherited table fails the load, never skipped. | AC | T |
| IR-R46 | GAP | mudcat shall serve a scan-plan continuation token (`plan-task`) that any replica can redeem from the token alone and that is pinned to the planned snapshot. | Design: `base64url{table,offset,limit,scan(snapshot pinned)}`, tasks totally ordered by (file, start, length, seq numbers, manifest, position) (`iceberg-rest-server-side-scan-planning.md:327-460`). Code at this commit is the first iteration: every plan task is rejected and plan always returns all tasks inline (`CatalogWrapperForREST.java:646-663`) although the design header says "Implemented by #12194". | IN | T |
| IR-R47 | IMPL | mudcat shall accept a metrics report only for an existing table whose name matches the path, and shall report overload with status 503. | `IcebergTableOperationDispatcher.checkTableMetricsReport:108-140`; bounded queue, `offer` false -> 503 (`IcebergMetricsManager.recordMetric`; `IcebergTableOperations.java:489-496`). | EV | T |
| IR-R48 | IMPL | mudcat shall emit a pre-event, a post-event and a failure event for every Iceberg REST operation, carrying a copy of the request. | ~60 event classes `listener/api/event/Iceberg*{PreEvent,Event,FailureEvent}`; copy via JSON round trip with fallback to the original (`IcebergRESTUtils.cloneIcebergRESTObject:645-665`). | EV | I |
| IR-R49 | GAP | mudcat shall create a table's identity, owner and catalog entry in one database transaction. | Gravitino creates in the Iceberg backend, then imports into its entity store; a failed import leaves "the table exists in Iceberg but not in Gravitino" and surfaces an error (`IcebergNamespaceHookDispatcher.java:235-238`), later retry gets 409. Multi-node races are patched by "best-effort reconcile" after drop and rename (`IcebergTableHookDispatcher.java:126-130, 221-250`). | ID | T |
| IR-R50 | IMPL | mudcat shall give each table an identity that survives rename and shall keep it separate from the Iceberg table-uuid carried in metadata. | Hook renames the entity with `.withId(tableEntity.id())` (`IcebergTableHookDispatcher.java:170`); metadata `table-uuid` untouched; register-overwrite keeps the UUID (IR-R28). | ID | T |
| IR-R51 | IMPL | mudcat shall reject a table, view or property location whose storage scheme the catalog's storage cannot reach, with status 400. | A catalog with a remote warehouse rejects local paths in create, register, set-location, `write.data.path`, namespace `location` (`IcebergCatalogWrapper.java:892-945`); an S3-only catalog rejects non-S3 (`:900-910`). | IN | T |
| IR-R52 | IMPL | mudcat shall answer status 400, not 404, when a client-supplied metadata location does not exist or is not a metadata file. | 404 is reserved for missing namespace or table; request-supplied misses convert to `BadRequestException` (`IcebergCatalogWrapper.java:269-275, 360-366, 1044-1057`; `IcebergExceptionMapper.java:80-83`). | IN | T |
| IR-R53 | GAP | mudcat shall implement the Iceberg REST contract natively and shall not depend on non-public members of the Iceberg library. | Gravitino reads private fields by reflection (`JdbcCatalogWithMetadataLocationSupport.loadFields:226-236`), calls package-private `UnboundPartitionField` accessors with `setAccessible` (`IcebergCommitValidation.java:246-252`), resolves scan-response methods with `DynMethods` "on both supported Iceberg runtimes" (`IcebergRESTUtils.java:83-90, 394`), plants classes in `org.apache.iceberg.*`, and needs a fork (`com.datastrato` 1.13.0 vs apache 1.11.0, `gradle/libs.versions.toml:89-90`) for read restrictions. | IN | I |
| IR-R54 | IMPL | mudcat shall answer an existence probe on a table with the status "exists" for a caller who may create or reference tables in the namespace even without read privilege. | Engines probe before acting: "Adding ANY_CREATE_TABLE here as Spark calls tableExists before creating a table" (`AuthorizationExpressionConstants.java:76-82`); secondary `allowCheckExistence` expression (`AuthorizationExpression.java:68-80`). | AC | T |
| IR-R55 | GAP | mudcat shall generate its capability documentation from the registered routes. | Docs say register-view is "not implemented" (`docs/iceberg-rest-service.md:44`) yet route and advertisement exist (`IcebergNamespaceOperations.java:207-252`, `IcebergConfigOperations.java:98`); design header says plan-tasks "Implemented" but code rejects them. | OP | I |
| IR-R56 | IMPL | mudcat shall limit federation to a configured authentication mode and shall never forward a remote catalog's credentials to a client unless credential source is set to remote. | `FederatedCatalogWrapper` drops remote credential key prefixes (`:~75-95`), rewrites refresh endpoints to itself (`IcebergRESTUtils.rewriteLoadTableCredentials`), service-account mode requires local authorization (`validateServerContext`). Applies if mudcat proxies a remote Iceberg catalog. | RD | T |

## 3. Quirks and gotchas ledger

### 3.1 Engines drive spec behaviour: probes, owner property, defaults
- **Title**: Existence probes are authorised more loosely than reads.
- What happens: `HEAD table` and load-table's secondary check admit principals who can create tables or views, though they cannot read the table.
- Cause: ENGINE (Spark `tableExists` before `createTable`; view resolution probes `viewExists(tableName)`).
- Evidence: `server-common/.../AuthorizationExpressionConstants.java:76` `//  Adding ANY_CREATE_TABLE here as Spark calls tableExists before creating a table.`; view path also registers the view name as a TABLE identifier "so ANY_SELECT_TABLE ... matches when Spark probes viewExists(tableName)" (`IcebergMetadataAuthorizationMethodInterceptor.java:141-146`).
- Mudcat implication: IR-R54; design the Zanzibar model with a distinct "may probe name" relation.

- **Title**: Engines send reserved properties, so the server must drop them silently.
- What happens: Spark puts `owner` into create/replace; a 4xx would fail the statement.
- Cause: ENGINE.
- Evidence: `IcebergOwnerPropertyGuard.java:36-40` "Updates to the property are dropped rather than rejected: engines send it on their own".
- Mudcat implication: IR-R37.

### 3.2 Spec ambiguities that need server-side decisions
- **Title**: Optional `field-id` on added partition fields.
- What happens: spec says optional; Iceberg numbers from 1000 again and collides with existing fields.
- Cause: SPEC.
- Evidence: `IcebergCommitValidation.java:138` "The REST spec makes field-id optional, but Iceberg's metadata builder numbers such fields from 1000 again".
- Mudcat implication: IR-R11.

- **Title**: 404 means "no namespace/table", not "no metadata file".
- What happens: register-table with a metadata path that is missing returns 400; an Iceberg `NotFoundException` that escapes is left unmapped (500) on purpose because it can mean a lost server-side file.
- Cause: SPEC.
- Evidence: `IcebergExceptionMapper.java:80-83` "Iceberg NotFoundException is intentionally unmapped (defaults to 500)"; `IcebergCatalogWrapper.java:269-275`.
- Mudcat implication: IR-R52; keep a separate error class for "data store inconsistent" (missing manifest) vs "bad request".

- **Title**: Unsupported-operation exception means 406 in the mapper, but invalid table definitions raise it too.
- What happens: Iceberg throws `UnsupportedOperationException` for a transform a column type cannot take; the mapper's 406 is wrong for create and update (no 406 in those endpoints).
- Cause: SPEC (library exception semantics).
- Evidence: `IcebergExceptionMapper.java:89` `.put(UnsupportedOperationException.class, 406)`; `IcebergCatalogWrapper.java:303-308` "Iceberg reports an invalid table definition ... as UnsupportedOperationException. createTable has no 406 response."; `IcebergRESTUtils.java:547` "binding failures would otherwise become 406".
- Mudcat implication: IR-R4, IR-R6: write an explicit status per endpoint, not per exception class.

- **Title**: Schema evolution has no "empty table" exemption.
- What happens: Gravitino removed its earlier `hasData` gate; illegal evolution on an empty table now returns 4xx where it returned 200.
- Cause: SPEC / COMPAT.
- Evidence: `IcebergCommitValidation.java:366-373` "Dropping IRC's old `hasData` gate is therefore the Table-Spec-compatible behavior; empty-table clients that previously got 200 ... may now get 4xx."
- Mudcat implication: IR-R10; version the validation rules.

- **Title**: v3 date promotion not in Iceberg's own `isPromotionAllowed`.
- What happens: date -> timestamp is legal from format v3 but the library rejects it.
- Cause: SPEC (library lag).
- Evidence: `IcebergCommitValidation.java:433-436` `isV3DatePromotion`.
- Mudcat implication: IR-R53.

### 3.3 Commit semantics and conflict handling
- **Title**: Server validation is injected as a requirement so retries re-run it.
- What happens: library retry loops re-evaluate requirements on fresh base metadata, so a check coded outside the requirement would race.
- Cause: CONCURRENCY.
- Evidence: `IcebergCommitValidation.java:64-70` "a check that depends on the table's current refs therefore cannot be bypassed by a concurrent commit"; `testRefCreatedAfterTheClientReadIsProtected`.
- Mudcat implication: IR-R9, IR-R10: run validation inside the commit transaction against the locked newest state.

- **Title**: `RetryableValidationException` becomes 409.
- What happens: stale sequence numbers or row IDs map to `CommitFailedException` so clients retry.
- Cause: SPEC.
- Evidence: `IcebergCommitValidation.java:313-316` "Preserve CatalogHandlers' conflict response for stale sequence numbers and row ids."
- Mudcat implication: IR-R9.

- **Title**: Cache invalidated before and after a commit; correctness comes from the pointer check.
- What happens: a concurrent load during a commit can re-cache old metadata even if the commit fails; read path re-validates against the current pointer.
- Cause: CONCURRENCY.
- Evidence: `IcebergCatalogWrapper.java:546` "A load during the commit can cache old metadata, even if the commit ultimately fails."; `BaseTableMetadataCache.java:48-70`.
- Mudcat implication: IR-R12; never cache without a cheap freshness token.

- **Title**: Create-table transaction does not retry.
- What happens: staged-create commit is applied by hand in the federation path because the remote client API has no raw commit; "this is a hacky way to get TableOperations for an uncommitted table".
- Cause: MODEL (gateway over another catalog).
- Evidence: `FederatedCatalogWrapper.java:706`, `:783` "create transactions do not retry. if the table exists, retrying is not a solution"; the list of updates not re-applied (`shouldApplyMetadataUpdateAfterBuilder`, `:~800-845`) is long and brittle.
- Mudcat implication: a native Iceberg endpoint avoids re-expressing commits through another API (IR-R53).

- **Title**: Idempotent replays drop response headers.
- What happens: replayed create/update lacks the ETag header.
- Cause: SPEC / OTHER.
- Evidence: `IcebergIdempotencyManager.java:62-64` "Response headers are not stored, so a replayed create or update does not carry the ETag".
- Mudcat implication: store status, headers and body digest, or recompute from the commit id.

### 3.4 Idempotency details
- **Title**: A lost acknowledgement or a failed response write blocks the key until expiry.
- What happens: if retaining a terminal response fails, the reservation is kept so the mutation is not re-run; a retry gets 409 "being processed" until lifetime ends (30 min default).
- Cause: CONCURRENCY / STORE.
- Evidence: `IcebergIdempotencyManager.java:184-191` "Retain terminal outcomes even if recording the response fails: the mutation may have committed"; design 5.3.
- Mudcat implication: IR-R20 (atomic outcome record).

- **Title**: Fencing token per key from 256 striped counters.
- What happens: a per-key counter restarting at 1 would let a late owner touch a new reservation, so the generation comes from `iceberg_idempotency_generation_meta`, picked by the last two hex digits of the key, incremented in the same transaction as the reserve.
- Cause: CONCURRENCY.
- Evidence: `JdbcIdempotencyStore.java:60, 213-219` `nextGeneration`; `design-docs/iceberg-idempotency-key.md:313` "A per-key counter initialized to 1 would repeat after deletion".
- Mudcat implication: use a database sequence or `txid`-based fence.

- **Title**: Expired-row reuse needs a second transaction.
- What happens: reserve does INSERT; on duplicate key it rolls back and runs a separate "replace if expired" transaction because a failed INSERT's locks cannot be upgraded on MySQL; retry once.
- Cause: STORE (MySQL/PG lock behaviour).
- Evidence: `JdbcIdempotencyStore.java:117-143` "The separate transaction avoids upgrading a failed INSERT's MySQL locks."
- Mudcat implication: on PostgreSQL use `INSERT ... ON CONFLICT DO UPDATE ... WHERE expires_at <= now()`.

- **Title**: Purge SQL differs per dialect.
- What happens: batched delete: `DELETE ... LIMIT` on MySQL/H2, a subselect of candidate keys on PostgreSQL with expiry recheck; max 100 batches x 1000 rows per pass.
- Cause: STORE.
- Evidence: design `iceberg-idempotency-key.md:391` (5.4); `JdbcIdempotencyStore.java:61-62, 163-174`.
- Mudcat implication: single dialect, partition or `pg_cron`-free delete by index.

- **Title**: In-memory store silently loses keys under pressure; JDBC store unavailable standalone.
- What happens: Caffeine `maximumSize` (10 000) evicts reservations early; `jdbc` store needs the entity store's connection pool, so it fails in standalone mode.
- Cause: OTHER.
- Evidence: design `iceberg-idempotency-key.md:241` (3.5), `JdbcIdempotencyStore.java:100-110` message "not available in the standalone Iceberg REST server".
- Mudcat implication: only one durable store; no best-effort mode.

### 3.5 Credential vending
- **Title**: Credentials are scoped by prefix, so table locations are a security boundary.
- What happens: an explicit `location` on create/register/set-location can point at another table's prefix and be issued its credentials; Gravitino demands load access on every overlapping table first.
- Cause: SECURITY.
- Evidence: `CreateTableAuthzHandler.java:47-60`; `docs/iceberg-rest-service.md:180-197`.
- Mudcat implication: IR-R26, IR-R27.

- **Title**: Overlap test works only for the standard `<table>/metadata/<file>` layout.
- What happens: custom `write.metadata.path` falls outside the pointer-based lookup; register still checks exact pointer match.
- Cause: MODEL.
- Evidence: `IcebergTableLocationUtils.tableLocation:~92-100`; `docs:195` "outside this lookup's supported layout".
- Mudcat implication: store location prefixes as a separate indexed column.

- **Title**: Location normalisation is dialect-specific by scheme.
- What happens: `s3a://`/`s3n://` fold to `s3://`; `file:`, `file:///`, bare paths and `hdfs:`/`viewfs:` get dot-segment normalisation; object-store keys keep literal `..` and `//`.
- Cause: SPEC (Hadoop vs object store).
- Evidence: `IcebergTableLocationUtils.normalize:36-62` "Dot segments and repeated separators in object-store keys are preserved."
- Mudcat implication: normalise per storage-resource type; keep the original string too.

- **Title**: Credential refresh endpoints are per-credential-type and sometimes omitted.
- What happens: refresh keys exist for S3, GCS, ADLS; OSS tokens get none because the Iceberg Aliyun FileIO ignores the key; federated responses must have upstream refresh URLs stripped from both `storage-credentials` and flat `config` or they leak.
- Cause: ENGINE/SPEC.
- Evidence: `IcebergRESTUtils.java:104-114, 296-312`.
- Mudcat implication: IR-R24, IR-R56.

- **Title**: Local and HDFS locations never get credentials; the request still succeeds.
- What happens: `X-Iceberg-Access-Delegation` is only a statement of client capability, so the server may serve none.
- Cause: SPEC.
- Evidence: `CatalogWrapperForREST.java:439-455`.
- Mudcat implication: IR-R22, IR-R25.

- **Title**: Credential privilege is computed per request from live authorization.
- What happens: `FILTER_MODIFY_TABLE_AUTHORIZATION_EXPRESSION` decides READ vs WRITE; with authorization off, the check is made by the core helper (outcome not verified here) (inferred).
- Cause: SECURITY.
- Evidence: `IcebergTableOperationExecutor.java:218-231`.
- Mudcat implication: IR-R23; make "no authorizer" mean explicit full access, stated in config.

### 3.6 Names, namespaces and identity
- **Title**: Namespace aliasing through the flattening separator.
- What happens: `["a:b"]` and `["a","b"]` map to one Gravitino schema; same owner and privileges. Only refused when authorization is on.
- Cause: MODEL (impedance: list-of-levels vs flat string).
- Evidence: `IcebergNamespaceOperationExecutor.java:176-195`; `HierarchicalSchemaUtil.logicalToPhysical` is a plain `replace`, not an escape (`core/.../HierarchicalSchemaUtil.java:79-83`) although design text promises "each path segment is encoded" (`design-docs/iceberg-supported-nested-namespace.md:104`).
- Mudcat implication: IR-R8.

- **Title**: Dot and length limits on namespaces.
- What happens: any level with '.' is refused (`reserved as the qualified-name separator`); flattened name over `EntityFieldLimits.MAX_NAME_LENGTH` is refused; JDBC backend would otherwise store `a.b` as nested.
- Cause: MODEL / STORE.
- Evidence: `IcebergNamespaceHookDispatcher.java:119-145`.
- Mudcat implication: IR-R8; compare length limits in code points (`codePointCount`).

- **Title**: JDBC namespace property values cap at 1000 code points.
- What happens: Iceberg's table column is `VARCHAR(1000)`; longer value is a 400.
- Cause: STORE.
- Evidence: `IcebergCatalogWrapper.java:116, 1022-1032`.
- Mudcat implication: state per-field limits in the spec and test at the boundary.

- **Title**: REST-backend identifiers are case-sensitive and re-registration conflicts.
- What happens: re-loading an external object under a different casing is rejected; a source identifier can have only one active Gravitino registration; recovery is a catalog-wide force drop.
- Cause: COMPAT / MODEL.
- Evidence: `docs/lakehouse-iceberg-catalog.md:60-85`.
- Mudcat implication: ID: unique external identity per registration; allow unregister of one object.

- **Title**: Lazy import means entity IDs appear on first load.
- What happens: tables created outside IRC (Spark with Hive, Paimon) get a Gravitino ID only when first loaded; two Gravitino nodes can race (`EntityAlreadyExistsException` -> reload).
- Cause: MODEL / CONCURRENCY.
- Evidence: `core/.../TableOperationDispatcher.java:655-675`.
- Mudcat implication: IR-R49, IR-R50.

- **Title**: Nested namespace creation creates and owns missing ancestors.
- What happens: create `A:B:C` where only `A` exists claims ownership only of new `A:B` and `A:B:C`; locking is per top-level branch; leaf drop may leave empty ancestors depending on the backend.
- Cause: MODEL.
- Evidence: `IcebergNamespaceHookDispatcher.java:76-118, 197-215`.
- Mudcat implication: define parent semantics (implicit vs explicit) once.

### 3.7 Drop, purge and name reuse
- **Title**: Async purge is selected by a non-standard header and falls back silently.
- What happens: `X-Gravitino-Async-Purge: true` only; in standalone mode or for REST backends the request becomes a synchronous purge.
- Cause: SPEC (spec has only `purgeRequested`).
- Evidence: `IcebergRequestContext.java:41`; `IcebergTableOperationExecutor.java:121-129`; `FederatedCatalogWrapper.supportsAsyncPurge()` returns false.
- Mudcat implication: IR-R31; make deletion record-first and asynchronous always, return 204 quickly.

- **Title**: Failed purge frees the name and leaks files.
- What happens: see IR-R34.
- Cause: OTHER.
- Evidence: `design-docs/async-iceberg-rest-hard-deletion.md:496-505` "the tombstone also lifts".
- Mudcat implication: IR-R34.

- **Title**: Tombstone check misses two paths.
- What happens: only create and register consult the job table; staged-create commit and rename-to-name do not.
- Cause: CONCURRENCY (inferred).
- Evidence: `rejectIfBeingPurged` call sites (grep) only `IcebergTableOperationExecutor.java:77`, `IcebergNamespaceOperationExecutor.java:149`.
- Mudcat implication: IR-R33.

- **Title**: Drop namespace does not check emptiness itself.
- What happens: relies on Iceberg catalog to answer `NamespaceNotEmpty` (409); a TODO says to check.
- Cause: OTHER.
- Evidence: `IcebergNamespaceOperations.java:234` `// todo check if table exists in namespace after table ops is added`.
- Mudcat implication: enforce in the commit transaction.

### 3.8 Pagination, caching, metrics
- **Title**: Pagination starts only with an empty or present `pageToken`.
- What happens: `pageSize` alone returns everything and no next token; token is the last name, so items inserted before it are missed.
- Cause: SPEC.
- Evidence: `IcebergPaginationHelper.java:90-125`; tests `TestIcebergPaginationHelper`.
- Mudcat implication: IR-R36.

- **Title**: ETag must cover caller-dependent bytes.
- What happens: with read restrictions the same metadata location gives different bodies per subject; ETag includes a canonical restriction seed and the fast 304 path is switched off whenever the loader is enabled.
- Cause: SECURITY / SPEC.
- Evidence: `IcebergReadRestrictionLoader.java:34-41`; `IcebergTableOperations.java:377`; design "Caching" `tag-based-read-restrictions-for-iceberg-rest.md:725-742`.
- Mudcat implication: IR-R41.

- **Title**: `snapshots=refs` filtering must keep credentials and history.
- What happens: filtered response is rebuilt, so credentials, read restrictions, metadata location and removed-statistics must be copied.
- Cause: SPEC / MODEL.
- Evidence: `IcebergTableOperations.java:694-718`; tests `testFilterSnapshotsByRefsKeepsCredentials`; loader `preserveSnapshotFilteredFields`.
- Mudcat implication: build responses from one function.

- **Title**: Metrics queue drops reports on overload with 503.
- What happens: bounded `LinkedBlockingQueue` (1000), writer thread; clients may retry.
- Cause: OTHER.
- Evidence: `IcebergMetricsManager.java:~108-120`; `IcebergTableOperations.java:489-496`.
- Mudcat implication: IR-R47; metrics are lossy by contract, say so.

### 3.9 Backends and library coupling
- **Title**: JDBC catalog V1 migration races between catalogs sharing one `iceberg_tables`.
- What happens: first-start of two Iceberg JDBC catalogs on the same URI makes `ALTER TABLE ... ADD COLUMN iceberg_type` fail for the loser; Gravitino reinitialises once. JDBC defaults: V1 schema (views), `strict-mode=true` (missing namespace -> 404), extra retryable SQLState `08S01` for MySQL idle drops.
- Cause: STORE.
- Evidence: `IcebergCatalogUtil.java:170-245`.
- Mudcat implication: own schema migrations, no shared third-party tables.

- **Title**: Views unsupported on JDBC schema V0.
- What happens: `supportsViewOperations` checks `SchemaVersion != V0`; view endpoints are then not advertised.
- Cause: STORE.
- Evidence: `IcebergCatalogWrapper.java:651-662`.
- Mudcat implication: IR-R1.

- **Title**: Hive catalog needs impersonation context for every call.
- What happens: `UserGroupInformation` is reset on thread switch, so `getCatalogWrapper` calls `reloadHadoopConf()` on every request; commits must build and commit inside one proxied call.
- Cause: OTHER (Hadoop).
- Evidence: `IcebergCatalogWrapperManager.java:127`; `IcebergCatalogWrapper.buildAndUpdateTable` doc (`:~555-575`).
- Mudcat implication: not applicable to a PostgreSQL-native catalog; avoid ambient-thread identity.

- **Title**: Planting classes in Iceberg's packages.
- What happens: `org.apache.iceberg.jdbc.JdbcCatalogWithMetadataLocationSupport`, `...hive.HiveCatalogWithMetadataLocationSupport`, `...memory.MemoryCatalogWithMetadataLocationSupport` exist to reach package-private state ("Use Iceberg package to reuse JdbcUtil related classes").
- Cause: COMPAT / classloader.
- Evidence: `iceberg-common/.../iceberg/jdbc/JdbcCatalogWithMetadataLocationSupport.java:38`.
- Mudcat implication: IR-R53.

- **Title**: Two Iceberg runtimes.
- What happens: `iceberg-rest-server` uses Iceberg 1.11.0; `iceberg-rest-server-experiment` swaps in `com.datastrato:experimental-iceberg-*` 1.13.0 via dependency substitution, selected by classpath directory at deploy; service logs `IcebergBuild.fullVersion()` and code source at startup to prove which loaded.
- Cause: COMPAT.
- Evidence: `iceberg-rest-server-experiment/build.gradle.kts:28-56`; `RESTService.java` `serviceInit`; `docs:84-89`.
- Mudcat implication: IR-R53; read restrictions are a spec feature mudcat must implement without waiting for a library.

- **Title**: Aux-mode classloader isolation.
- What happens: IRC classes live outside the main server classpath, so static flags (`includeErrorStackTrace`) are set per service; JDBC mapper for idempotency is added to the core `SqlSessionFactory` at runtime because core "cannot discover mappers in the auxiliary service's class loader".
- Cause: OTHER.
- Evidence: `IcebergRESTUtils.java:116-118`; `JdbcIdempotencyStore.java:108-115`.
- Mudcat implication: ship the Iceberg gateway in the same process/module as the catalog core, no class-loader split.

### 3.10 Authorization inside the IRC path
- **Title**: Authorization is path-driven and evaluated before the body is parsed for most endpoints; body-aware handlers fill the gaps.
- What happens: annotations map path params to identifiers; handlers read the request body for create/update/register/rename/load to authorise locations, cross-namespace rename, and "load via view".
- Cause: SPEC (identifiers also appear in bodies).
- Evidence: `IcebergMetadataAuthorizationMethodInterceptor.java:180-205` (switch on `RequestType`).
- Mudcat implication: authorise on resolved IDs after parsing, in one place.

- **Title**: Federated REST backend skips local authorization by default.
- What happens: `disable-rest-authz` defaults true when the backend is another Gravitino IRC; skipped only when the connection forwards the user token; service-account connections are never skipped and require authorization enabled.
- Cause: SECURITY.
- Evidence: `IcebergConfig.java:236-243`; `IcebergMetadataAuthorizationMethodInterceptor.java:221-250`; `FederatedCatalogWrapper.validateServerContext`.
- Mudcat implication: IR-R56.

- **Title**: Read restrictions are experimental, off by default, and need authorization on.
- What happens: loader found by `ServiceLoader`; if present and authorization is off the server refuses to start.
- Cause: OTHER.
- Evidence: `RESTService.loadReadRestrictionLoader:264-271`.
- Mudcat implication: IR-R42..R45.

## 4. Open questions / inconsistencies / half-implemented

### 4.1 Docs vs code
- Register-view: docs say not implemented (`docs/iceberg-rest-service.md:44`); code implements and advertises it (`IcebergConfigOperations.java:98`, `IcebergNamespaceOperations.java:207-252`; hook import in `IcebergNamespaceHookDispatcher.java:260-275`).
- Scan planning: the design doc header says "Implemented by #12194" and describes `plan-tasks`, snapshot pinning and a `/tasks` advertisement; the code returns everything inline and rejects every plan task, and does not advertise `/tasks` (`CatalogWrapperForREST.java:646-663`; `IcebergConfigOperations.java:86-87`). pyiceberg `scan-planning-mode=server` therefore still fails.
- Client capability header `X-Gravitino-Client-Capabilities: read-restrictions.v1` and the 406/403 matrix exist only in the design (`tag-based-read-restrictions-for-iceberg-rest.md:746-800`); no code reads it.
- Nested namespace design promises per-segment encoding before `\u0001` flattening (`iceberg-supported-nested-namespace.md:104`); `HierarchicalSchemaUtil.logicalToPhysical` is a plain `String.replace` (`core/.../HierarchicalSchemaUtil.java:79-83`). Aliasing is patched in IRC only when authorization is enabled.
- `DynamicIcebergConfigProvider.java:311-313` says the wrapper manager "bypasses its cache" when authorization is enabled; `IcebergCatalogWrapperManager.getCatalogWrapper` always uses the Caffeine cache (stale comment, or an unimplemented safeguard).
- `docs/iceberg-rest-service.md:101` says the `gravitino.auxService.iceberg-rest.` prefix is deprecated; server code still handles both (`server-common/.../web/Utils.java:60`, `server/.../IcebergRESTServiceOperations.java:97`), so two spellings of every key must be tested.

### 4.2 Not implemented or deferred
- Multi-table transaction commit (docs:43). Async scan plan fetch and cancel. Purge events and a read-only purge management endpoint ("deferred to Phase 2", design 5.9). Async purge for views ("planned follow-up", 5.8). Namespace-level purge. Soft delete / undrop ("non-goal", design 3).
- Three maintenance job designs (expire snapshots, remove orphan files, rewrite manifests) are Spark jobs in `maintenance/jobs/` driven by Gravitino policy types (`api/.../policy/IcebergSnapshotExpirationContent.java`), outside the IRC server; the IRC path has no maintenance endpoint. They commit through the normal Iceberg catalog, so they are subject to the same commit validation only if routed through IRC (inferred, not read).

### 4.3 Risks to check before copying a pattern
- **Idempotency scope**: replay across principals and tenants is possible by design (IR-R18). Replay of credentials (IR-R19) is inferred from code path; confirm with a test.
- **Authorization when disabled**: with authorization off, hook dispatchers are not wired, so no entity import, owner or tombstone-name consistency exists; credential privilege resolves through `MetadataAuthzHelper.checkAccess` whose behaviour with no authorizer was not read here (inferred).
- **Clock-based leases** in idempotency and in cleanup heartbeat (`heartbeat_at` compared with `:now` from the app, design 5.5); skew between replicas shifts both.
- **O(catalog) overlap scan** on non-JDBC backends can run inside a create request (IR-R27).
- **Metrics report** accepted by name suffix match: `reportedTableName.endsWith("." + qualifiedTableName)` (`IcebergTableOperationDispatcher.java:135-139`) permits a report from an engine-qualified name; intentional leniency, but not a unique match.
- **No conformance kit run**: all behaviour above was pinned by Gravitino's own tests, not by the upstream suite (IR-R2).
