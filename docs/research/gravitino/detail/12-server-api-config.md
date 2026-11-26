> **Research note, not a requirement document.** Drafted by a code-reading agent from Apache Gravitino commit `9aa78cf` (2026-11-26), then spot-checked by the lead. Line numbers can drift by a few lines; search by file and symbol. Items marked `(inferred)` were not confirmed in code. The text does not follow Simplified Technical English. See [../README.md](../README.md) for the synthesis.

# Server, REST API surface, configuration, lifecycle, identity/naming rules (ref prefix SV)

Gravitino checkout: `/home/user/apache/gravitino` (commit 9aa78cf). All paths below are repo-relative. Lines marked `(inferred)` were deduced, not read directly.

## 1. Decomposition

```
bin/gravitino.sh.template            start/stop script (kill, 10 s wait, then kill -9)
conf/gravitino.conf.template         shipped defaults (differ from code defaults, see Q-17)
server/                              Jersey resources + exception mapping (HTTP surface)
  GravitinoServer.java               main(): load conf -> initialize -> start -> join -> stop
  web/VersioningFilter.java          Accept / Content-Type "application/vnd.gravitino.vN+json"
  web/ApiVersion.java                enum V_1, V_2; default V_1
  web/rest/*Operations.java          149 versioned @Produces; one class per resource
  web/rest/ExceptionHandlers.java    per-resource exception -> (HTTP status, error code, type)
  web/mapper/*ExceptionMapper.java   Jersey-level errors -> same ErrorResponse JSON
  web/filter/GravitinoInterceptionService.java  authz expression interception (only if authz on)
  web/filter/AccessControlNotAllowedFilter.java 405 on access-control APIs if authz off
server-common/                       shared by main server, Iceberg REST, Lance REST
  server/ServerConfig.java           2 own keys; everything else lives in core Configs
  web/JettyServer.java, JettyServerConfig.java   33 webserver keys (host, threads, TLS, CORS, trustedProxies)
  web/Utils.java                     response builders (the HTTP status table lives here)
  web/ServerHealth.java              sticky OOM flag
  web/HttpAuditFilter.java, RequestContextFilter.java  every request yields >= 1 audit event
  authentication/AuthenticationFilter.java   authenticator chain; health paths bypass it
common/                              DTOs, requests, responses, ErrorConstants, JsonUtils, Version
  dto/requests/*UpdatesRequest       {"updates":[{"@type":...}]} typed update lists
  dto/responses/ErrorResponse        {code, type, message, stack?}; BaseResponse {code:0}
api/                                 NameIdentifier, Namespace, change classes, exceptions
core/
  Configs.java                       62 ConfigBuilder entries (360 across 32 files in repo)
  GravitinoEnv.java                  singleton; wires stores, managers, dispatcher chains
  utils/NameValidationUtils.java     name syntax + 64-code-point limit (catalog, domain, metalake, default capability)
  EntityFieldLimits.java             DB column widths (128 / 300 / 256 / 4096) checked before the DB sees them
  metalake/MetalakeManager.java      metalake lifecycle (enable/disable/force drop)
  auxiliary/AuxiliaryServiceManager  loads Iceberg REST / Lance REST in isolated classloaders
  lock/LockManager (TreeLock)        in-JVM hierarchical locks (not distributed)
docs/open-api/*.yaml                 OpenAPI per resource family; docs/health-and-readiness.md; docs/migration-guide.md
dev/charts/gravitino                 Helm chart (probes, schema-migrate Job, PDB)
```

Request path: Jetty -> `RequestContextFilter` -> `HttpAuditFilter` -> `VersioningFilter` -> (CORS) -> custom filters -> `AuthenticationFilter` (`/api/*` only) -> Jersey -> `*Operations` -> `Utils.doAs(principal)` -> dispatcher chain `EventDispatcher -> NormalizeDispatcher -> HookDispatcher -> Manager` (`core/.../GravitinoEnv.java:1115`) -> entity store / catalog connector. `/configs` and metrics servlets sit outside `/api/*` and outside authentication (`GravitinoServer.java:267`).

Extension points: `gravitino.server.rest.extensionPackages` (extra Jersey packages), `gravitino.server.webserver.customFilters`, auxiliary services (`gravitino.auxService.names`, SPI `GravitinoAuxiliaryService` found by `ServiceLoader` with a per-service isolated classloader), authenticators, `gravitino.authorization.impl`, event listeners, audit writer/formatter.

Config keys that matter: `gravitino.entity.store.relational.*`, `gravitino.server.webserver.{httpPort,maxThreads,threadPoolWorkQueueSize,stopTimeout,idleTimeout,includeErrorStackTrace,enableCorsFilter,allowedOrigins,allowedMethods,trustedProxies}`, `gravitino.server.shutdown.timeout`, `gravitino.authenticators`, `gravitino.authorization.{enable,serviceAdmins}`, `gravitino.server.bulk.maxItems`, `gravitino.server.visibleConfigs`, `gravitino.entityChangeLog.*`, `gravitino.cache.*`, `gravitino.entity.store.{deleteAfterTimeMs,versionRetentionCount}`, `gravitino.schema.separator`, `gravitino.auxService.names`.

### Conventions as observed in code

- Base path `/api/...`. Resources nest by name: `/metalakes/{m}/catalogs/{c}/schemas/{s}/tables/{t}`. Identity is the name path. Numeric IDs are never exposed.
- Versioning is by media type, per operation: `application/vnd.gravitino.v1+json` on 149 operations; `v2` only on tag-value association (`server/.../MetadataObjectTagOperations.java:77`). Default when no version is named: V_1 (`ApiVersion.java:52`). Unsupported version gives 406 with a JSON `ErrorResponse` (`VersioningFilter.java:199`).
- Mutation conventions: create = `POST` (200 with entity body, not 201); alter = `PUT` with `{"updates":[...]}` where each update is a typed object (`rename`, `updateComment`, `setProperty`, `removeProperty`; `common/.../MetalakeUpdateRequest.java:30-40`); enable/disable = `PATCH` `{inUse}`; drop = `DELETE` returning 200 `{"code":0,"dropped":true|false}`; force-flag is `force` for catalog/metalake and `cascade` for schema.
- Error model: `{"code":<app code 1000..1100>,"type":"<ExceptionSimpleName>","message":...,"stack":[...]}`. Application code is coarse (15 values, `common/.../ErrorConstants.java:25-67`); the `type` string is the exception class simple name, and the Java client picks the exception class from `(code, type)` per resource (`clients/client-java/.../ErrorHandlers.java:424-474`).
- HTTP status table (`server-common/.../Utils.java`): 400 illegal argument, 401 unauthorized, 403 forbidden, 404 not found, 405 authz-disabled/wrong method, 409 already-exists / non-empty / in-use / not-in-use / optimistic-lock / unmodifiable-statistic, 500 internal, 501 unsupported operation, 502 backend connection failed, 503 dependency unavailable / health down. `tooManyRequests()` (429) is defined (`Utils.java:96`) but nothing in `server/` or `server-common/` calls it.
- No pagination anywhere: the only `@QueryParam`s are `details`, `force`, `cascade`, `purge`, `expand`, `nameContains`, job filters (`sortBy`, `sortOrder`, `startedAfter`...). List endpoints return whole arrays. The OOM section of `docs/health-and-readiness.md` names "an oversized list response" as a known trigger.
- Only one bulk surface: `/bulk/metalakes/{m}/users|groups|roles/add`, best-effort, capped by `gravitino.server.bulk.maxItems` = 100 (`core/.../bulk/BulkManager.java:53`), partial success reported per item.
- Health: `/api/health/{live,ready}` and `/api/health`, plus root aliases `/health*`; unauthenticated and not audited (`HealthCheckPathMatcher.java:52-59`).
- Config: static, loaded once from `gravitino.conf`; no reload endpoint. `/configs` exposes a fixed set of keys plus anything listed in `gravitino.server.visibleConfigs`.

## 2. Candidate requirements

| Ref | Kind | Requirement (mudcat style) | Rationale / Gravitino evidence | Area | Ver |
|---|---|---|---|---|---|
| SV-R1 | GAP | mudcat shall carry an expected-version precondition on every alter, drop and ref-move request. | Alter requests carry only a list of updates; only semantic-model alter carries `expectedRevision` (`common/.../SemanticModelUpdatesRequest.java:48,88`). Other alters are last-writer-wins, and a stale client silently overwrites. | ID | T |
| SV-R2 | GAP | mudcat shall reject a request that omits the precondition on a destructive or replacing update with a distinct error code. | Semantic model requires `expectedRevision` only when a definition is replaced (`SemanticModelUpdatesRequest.java:86-89`); property alters do not. Mudcat should make this uniform, not per-change-type. | ID | T |
| SV-R3 | IMPL | mudcat shall return a conflict error that names the current version when a precondition fails. | `OptimisticLockException` maps to 409 with its own code 1012 (`ExceptionHandlers.java:1336`, `ErrorConstants.java:61`) and message "is at revision %s, expected %s" (`SemanticModelOperations.java:364-367`). | ID | T |
| SV-R4 | GAP | mudcat shall accept an idempotency key on every create and commit request and replay the original response for a repeated key. | Create returns "already exists" on retry (`Utils.alreadyExists`, 409). A client that lost the response cannot tell its own create from a rival's. No idempotency key in any request DTO (searched `common/.../dto/requests`). | ID | T |
| SV-R5 | IMPL | mudcat shall return success with `dropped=false` when a drop targets an object that does not exist. | Missing object gives 200 `{"dropped":false}` (`TestSchemaOperations.java:518-531`; `MetalakeManager.java:411` "idempotent false result"). Retried deletes are safe. | ID | T |
| SV-R6 | GAP | mudcat shall return, with each drop, the commit that removed the object, or the commit that last observed its absence. | A bare boolean cannot distinguish "I dropped it" from "someone else already did". `DropResponse` has only `dropped` (+ legacy `deleted`, `DropResponse.java:36`). | ID | T |
| SV-R7 | IMPL | mudcat shall refuse to drop a non-empty container unless the request sets an explicit force flag. | Metalake: `NonEmptyMetalakeException` (`MetalakeManager.java:394-396`); schema: 409 `NON_EMPTY_CODE` (`TestSchemaOperations.java:480-497`). | ID | T |
| SV-R8 | GAP | mudcat shall use one force-flag name across all drop operations. | `cascade` on schema (`SchemaOperations.java:268`), `force` on catalog and metalake (`CatalogOperations.java:424`, `MetalakeOperations.java:~375`). Two spellings for one concept. | IN | I |
| SV-R9 | IMPL | mudcat shall require a container to be disabled before an unforced drop. | Metalake `in-use` property; drop of an enabled metalake without force throws `MetalakeInUseException` (`MetalakeManager.java:386-390`). | UP | T |
| SV-R10 | GAP | mudcat shall change the enabled state of a container and of its children in one transaction. | Disable flips the metalake row, then loops over catalogs; if any fails the metalake is already disabled and catalogs are inconsistent (`MetalakeManager.java:549-553`, comment "we can't make sure we can change all catalog properties in a transaction"). | OP | T |
| SV-R11 | GAP | mudcat shall derive the effective enabled state of a child from its parent at read time, and not copy it into the child. | Gravitino denormalises `metalake-in-use` into every catalog (`BaseCatalogPropertiesMetadata.java:46`, `MetalakeManager.java:656-690`), which is the cause of SV-R10's failure window. | ST | I |
| SV-R12 | IMPL | mudcat shall roll a failed forced drop back to the pre-drop enabled state. | `restoreDisabledState` after a force drop that re-enabled a disabled metalake (`MetalakeManager.java:422`, `:485-495`). | OP | T |
| SV-R13 | IMPL | mudcat shall negotiate the API version in a request header and shall answer an unsupported version with a structured error. | `VersioningFilter.java:94-200`: version from `Accept`, else `Content-Type`, else default; unsupported gives 406 JSON, not Jetty HTML. | IN | T |
| SV-R14 | GAP | mudcat shall state in the API definition the policy under which an API version is deprecated and removed. | `ApiVersion` has V_1 and V_2, but V_2 exists for one operation only (tag values). No sunset policy in `docs/` (migration guide is per-release prose: `docs/migration-guide.md:12-264`). | IN | I |
| SV-R15 | GAP | mudcat shall reject an `Accept` version value that is out of range with a 406 error, without raising a server error. | `Integer.parseInt(matcher.group(1))` on `\d+` (`VersioningFilter.java:183`) with an unanchored `find()` (`:94`). A version of 10+ digits throws `NumberFormatException` in a filter (inferred: no test covers it; `TestVersioningFilter` has no overflow case). | IN | T |
| SV-R16 | IMPL | mudcat shall return every error, including router, parse and filter errors, in the same JSON error shape. | `WebApplicationExceptionMapper`, `NotFoundExceptionMapper`, and `VersioningFilter` write `ErrorResponse` instead of Jetty HTML (`WebApplicationExceptionMapper.java` class doc, `VersioningFilter.java:190-205`). | IN | T |
| SV-R17 | GAP | mudcat shall define one machine-readable error code per distinct error condition, so that no client needs the error message or class name to tell two conditions apart. | The code is coarse and the client reads `type` strings (`ErrorHandlers.java:431-470`). Unsupported-operation code is reused for 409 and 405 "for compatibility" (`Utils.java:276-295`). Message trimming by `lastIndexOf("Exception: ")` (`ExceptionHandlers.java:1364`). | IN | I |
| SV-R18 | GAP | mudcat shall make the HTTP status and the application error code agree for every error. | `operationConflict` is 409 with the unsupported-operation payload; `methodNotAllowed` is 405 with the same payload (`Utils.java:276,292`). A client that reads only the code cannot tell the cases apart. | IN | T |
| SV-R19 | GAP | mudcat shall omit stack traces from error responses by default. | `includeErrorStackTrace` defaults to `true` and the template ships `true` (`JettyServerConfig.java:120-128`, `conf/gravitino.conf.template`) "only to avoid breaking legacy clients". | OP | I |
| SV-R20 | GAP | mudcat shall give every list endpoint a page-size limit, a continuation token and a maximum page size. | No list endpoint has a limit (see section 1). Docs admit OOM from oversized list responses and that one OOM marks the process unhealthy forever (`docs/health-and-readiness.md`). | PF | T |
| SV-R21 | GAP | mudcat shall cap the request body size and the number of entries in each batch request. | Only `/bulk` has a cap (`BulkManager.java:53`). `*UpdatesRequest` lists, property maps and search of names have no cap in the DTO `validate()` methods read. `JettyServerConfig` has header-size keys but no body-size key. | PF | T |
| SV-R22 | GAP | mudcat shall apply backpressure with HTTP 429 and a retry delay when the request queue is full. | `Utils.tooManyRequests()` exists and is unused; the worker queue is a fixed 100-entry `LinkedBlockingQueue` (`JettyServerConfig.java:131-136`, `JettyServer.java:479-486`). Overflow behaviour is the Jetty default (inferred). | PF | T |
| SV-R23 | IMPL | mudcat shall validate each request body with a `validate()` step that returns 400 before any state change. | Every `*Request` implements `RESTRequest.validate()`, called first inside the handler (`MetalakeOperations.java:~125`; `RequestValidation.java` converts `IllegalArgumentException` to `BadRequestException`). | IN | T |
| SV-R24 | GAP | mudcat shall reject unknown fields in request bodies, or shall list them in the response. | Update requests use `@JsonIgnoreProperties(ignoreUnknown = true)` (`MetalakeUpdateRequest.java:21`). A misspelt `"newComent"` is a silent no-op (inferred: `updateComment` validate "Always pass"). | IN | T |
| SV-R25 | GAP | mudcat shall apply one name syntax and one length limit to every named entity type, and shall state them in one place. | Metalake `\w+`, catalog and domain `\w[\w-]*`, default capability `\w[\w/=-]*` (`MetalakeNormalizeDispatcher.java:44`, `NameValidationUtils.java:33`, `Capability.java:163`). Length 64 enforced in code, columns are VARCHAR(128) (`EntityFieldLimits.java:34`). | ID | I |
| SV-R26 | IMPL | mudcat shall count name length in Unicode code points and shall enforce each limit before the database sees the value. | `codePointCount` "since MySQL (utf8mb4) and PostgreSQL count a supplementary character as one" (`EntityFieldLimits.java:70-73`). | ID | T |
| SV-R27 | GAP | mudcat shall define the name character set as a normalised Unicode form, with a stated case rule, or shall restrict it to ASCII explicitly. | Java `\w` without UNICODE flag is ASCII only (inferred), yet length is in code points, so the supplementary-character handling is unreachable for names. Case rule is a per-catalog capability `caseSensitiveOnName` (`CapabilityHelpers.java:297-310`). | ID | T |
| SV-R28 | GAP | mudcat shall apply a tightened name rule to create and rename only, and shall never apply it to load, list or drop. | Gravitino does this deliberately ("for compatibility reasons" `SchemaNormalizeDispatcher.java:85`; `MetalakeNormalizeDispatcher.java:95`; legacy dotted tag/policy names `NameIdentifierUtil.java:667`). Mudcat should state it as a rule, so legacy names stay addressable. | ID | T |
| SV-R29 | GAP | mudcat shall reserve system-managed names by a separate entity flag and not by a name prefix in the user namespace. | Policy-to-tag bindings use hidden tags named `gravitino_hidden_{policyId}` (`HiddenPolicyTag.java:31`); a user tag with that name makes the migration fail ("Hidden tag name collision", `docs/migration-guide.md:250`). | ST | T |
| SV-R30 | IMPL | mudcat shall reject a property key in a reserved namespace on create and alter, and shall hide reserved and sensitive property values on read. | `PropertiesMetadataHelpers.java:60` ("reserved and cannot be set"), `:96-110`; `HiddenPropertyMaskUtils.java:74` mask `******`. | ST | T |
| SV-R31 | GAP | mudcat shall return a read-modify-write-safe representation, so that a response can be sent back unchanged without destroying a secret. | Masked value `******` is rejected on write (`HiddenPropertyMaskUtils.validateNoMaskedPlaceholders`), so GET-then-PUT of properties fails; and keys are masked by name guess (`secret|password|token|credential|access|account`, class doc). | ST | T |
| SV-R32 | GAP | mudcat shall return an unmasked key list that depends only on declared property schema, not on key-name heuristics. | Undeclared key containing "access" or "account" is masked (`HiddenPropertyMaskUtils` class doc). A property named `account_region` is masked. | ST | T |
| SV-R33 | IMPL | mudcat shall serve separate liveness and readiness probes, and readiness shall exercise the database with a bounded timeout. | `HealthOperations.java:61-100`: `/live`, `/ready`, `/api/health`; store probe on a 1-4 thread pool with a 20-entry queue and 2 s timeout (`ServerConfig.java`, `HEALTH_ENTITY_STORE_PROBE_TIMEOUT_MS`). | OP | T |
| SV-R34 | GAP | mudcat shall serve the same health JSON, including status casing, from every component. | Gravitino and Lance emit `up`/`down`, Iceberg REST emits `UP`/`DOWN` (`docs/health-and-readiness.md:64`). | IN | T |
| SV-R35 | GAP | mudcat shall report a recoverable allocation failure as degraded for a bounded time and not as unhealthy until restart. | `ServerHealth` is a sticky flag; one oversized request marks every replica for restart (`ServerHealth.java:24`; `docs/health-and-readiness.md` "retrying the same oversized request against different replicas can cause those replicas to restart in succession"). | OP | A |
| SV-R36 | IMPL | mudcat shall bypass authentication, and shall skip audit logging, for health probe paths only. | `AuthenticationFilter.doFilter` and `HttpAuditFilter` both use `HealthCheckPathMatcher` (`HealthCheckPathMatcher.java:52-59`). | AC | T |
| SV-R37 | IMPL | mudcat shall write at least one audit event for every request, including requests rejected before the resource method runs, and shall not write two events for one outcome. | `HttpAuditFilter` class doc: fallback event only if no operation-layer event was fired; success + 5xx still logs both. Added after GH-12760 (`GravitinoServer.java:103`). | AC | T |
| SV-R38 | IMPL | mudcat shall record the client address from a forwarded-for header only when the connection comes from a configured trusted proxy. | `trustedProxies` (`JettyServerConfig.java:321`); 1.3 trusted the first X-Forwarded-For entry and was changed in 2.0 (`docs/migration-guide.md:57`). | AC | T |
| SV-R39 | GAP | mudcat shall refuse to start with a wildcard CORS origin when credentials are allowed. | Defaults `allowedOrigins=*` and `allowCredentials=true` (`JettyServerConfig.java:249,295`), although the CORS filter is off by default. | AC | I |
| SV-R40 | GAP | mudcat shall derive the CORS allowed-method default from the methods the API uses. | Default `GET,POST,HEAD,DELETE,PUT` has no `PATCH` (`JettyServerConfig.java:267`) while metalake, catalog, policy and domain use `PATCH` (`MetalakeOperations.java:204`); the doc string lists a third, different set (`:264`). | IN | T |
| SV-R41 | GAP | mudcat shall serve no unauthenticated endpoint that returns configuration, except a fixed, documented bootstrap set. | `/configs` is open (`GravitinoServer.java:267`) and also returns any raw key the operator lists in `visibleConfigs` (`ConfigServlet.java:80`). | AC | I |
| SV-R42 | IMPL | mudcat shall validate every configuration value at startup against a declared type and range and shall fail to start on a bad value. | `ConfigBuilder.checkValue(...)` on e.g. delete-after (10 min - 30 days) and version retention (1-10) (`Configs.java:76-84,332-365`). | OP | T |
| SV-R43 | GAP | mudcat shall classify each configuration key as startup-only or live, and shall list the keys that can lose data when changed. | 360 keys, all read at startup; `docs/gravitino-server-config.md` has no per-key reload column, only prose. `gravitino.entity.store.maxTransactionSkewTimeMs` has no check (`Configs.java:325-329`). | OP | I |
| SV-R44 | GAP | mudcat shall make the default shipped configuration identical to the code default for each key. | Template `maxThreads=200`, `minThreads=24` vs code `max(4*cpu,400)`, `max(min(2*cpu,100),4)` (`conf/gravitino.conf.template:35`, `JettyServerConfig.java:67-81`). Template `serviceAdmins=anonymous` (`:107`). | OP | I |
| SV-R45 | GAP | mudcat shall provide a command that checks the database schema version against the server at startup and refuses to start on a mismatch. | Schema is created automatically for H2 only; MySQL/PostgreSQL need manual scripts (`docs/gravitino-server-config.md:95`, `docs/how-to-use-relational-backend-storage.md:65`). No startup schema-version check found in `core/.../storage` (searched; only `metalake_meta.schema_version` per entity). | OP | T |
| SV-R46 | GAP | mudcat shall support a rolling upgrade across one minor version without stopping all servers. | Upgrade requires shutdown of every instance and a manual SQL script (`docs/how-to-upgrade.md:22`, `docs/migration-guide.md:217-264`). | OP | D |
| SV-R47 | IMPL | mudcat shall stop accepting new requests, drain in-flight requests, and only then close the authorizer, event listeners, store and connectors, in that order. | `GravitinoServer.stop()` "Stop the web server first" (`:295`), then `GravitinoEnv.shutdown()` closes aux services, listeners, store, catalogs, metrics, job and stats (`GravitinoEnv.java:845-905`) and `StatisticsHandler` drains Jetty (`JettyServer.java:187-191`). | OP | T |
| SV-R48 | GAP | mudcat shall release every started component when startup fails after a partial start. | `main()` calls `System.exit(-1)` on start failure (`GravitinoServer.java:323`) before the shutdown hook is registered; aux services and the store are not closed. `GravitinoEnv.start()` starts aux services before Jetty binds its port (`:840`, `GravitinoServer.java:285-288`). | OP | T |
| SV-R49 | GAP | mudcat shall use one shutdown time budget that is longer than the sum of its parts, and the stop script shall wait for it. | Jetty stopTimeout 30 s (`JettyServerConfig.java:84`), shutdown-hook sleeps `server.shutdown.timeout` = 3 s before stopping (`GravitinoServer.java:332`), stop script sends SIGKILL after ~10 s (`bin/gravitino.sh.template:73,97`). The script can kill the process while Jetty is still draining. | OP | T |
| SV-R50 | GAP | mudcat shall make each metalake-level or tenant-level create and drop independent of a global lock. | Metalake create and drop take the root tree lock in write mode (`MetalakeManager.java:173,396`); tree locks are in-JVM, so with two servers only the store row lock protects (`:399-400` comment). | PF | A |
| SV-R51 | GAP | mudcat shall allocate IDs that are collision-checked by the database and ordered by time. | `RandomIdGenerator` returns the low 63 bits of a random UUID (`RandomIdGenerator.java:37`); no time ordering. UUIDv7 (ID-1) fixes locality. | ID | I |
| SV-R52 | IMPL | mudcat shall deliver cache invalidations between servers through a table in the one database, and shall tolerate duplicate delivery. | `EntityChangeLog` polled every 3 s; "invalidation is idempotent" (`EntityChangeLogBaseSQLProvider.java:42-50`). | OP | T |
| SV-R53 | GAP | mudcat shall read a change stream by a cursor that cannot skip a late-committing row. | Auto-increment IDs are not commit order; Gravitino compensates with a 60 s recorded-age replay window that "is bounded compensation, not a lossless delivery protocol" (`docs/gravitino-server-config.md:410-430`; `EntityChangeLogBaseSQLProvider.java:42`). | EV | T |
| SV-R54 | IMPL | mudcat shall run the Iceberg gateway as an optional component that can run in the server process or alone, with the same configuration keys. | `AuxiliaryServiceManager` loads `iceberg-rest` / `lance-rest` with `gravitino.<name>.*` keys (deprecated `gravitino.auxService.<name>.*` still honoured, `:278`); standalone script `bin/gravitino-iceberg-rest-server.sh.template`. | IN | D |
| SV-R55 | GAP | mudcat shall map each Iceberg warehouse prefix to a tenant by request, not by server configuration. | The embedded Iceberg REST serves exactly one metalake per process (`gravitino-metalake` key; `Utils.java:449` "serves exactly one metalake"). | IN | T |
| SV-R56 | IMPL | mudcat shall let a server version branch on a client-version header to keep old clients working during a path-encoding change, and shall log the branch. | `FilesetOperations.java:257-259`: `isV1PlusClient = clientVersion == null \|\| >= 1` decides whether the sub-path is URL-decoded; header `X-Client-Version` (`common/.../Version.java:35`). | IN | T |
| SV-R57 | GAP | mudcat shall not log the effective auxiliary-service configuration at INFO. | `LOG.info("AuxService name:{}, config:{}, valid classpath:{}")` prints the whole prefix map (`AuxiliaryServiceManager.java:172`); that map may carry catalog-backend credentials (inferred: Iceberg REST keys such as JDBC password live under `gravitino.iceberg-rest.*`). | AC | I |

## 3. Quirks and gotchas ledger

**Q-1. Three name grammars for three entity types, one length limit enforced below the column width**
- Metalake: `\w+` (no hyphen). Catalog and domain: `\w[\w-]*`. Default catalog capability (schema, table, fileset, ...): `\w[\w/=-]*`. Length limit: 64 code points in code, but the DB columns and `EntityFieldLimits.MAX_NAME_LENGTH` are 128; table column names are 300. The latest change added the 64-point limit through `NameValidationUtils.nameValidationError` (tests pin `a`*64 legal and `a`*64+`?` illegal: `core/src/test/.../TestNameValidationUtils.java:65,110`; error "must not exceed 64 characters", `TestMetalakeNormalizeDispatcher.java:133`).
- Cause: COMPAT
- Evidence: `core/.../utils/NameValidationUtils.java:32-33` `DEFAULT_MAX_NAME_LENGTH = 64; NAME_PATTERN = "\\w[\\w-]*"`; `core/.../metalake/MetalakeNormalizeDispatcher.java:44` `Pattern.compile("\\w+")`; `core/.../connector/capability/Capability.java:163` `"\\w[\\w/=-]*"`; `core/.../EntityFieldLimits.java:34` `MAX_NAME_LENGTH = 128`.
- Mudcat implication: SV-R25, SV-R27. Allowed `/` and `=` in the default capability pattern (for Hive/Iceberg partition-like names) but not in metalake or catalog names; mudcat should state one grammar per entity type in one table.

**Q-2. Name rules apply on create and rename only, so legacy names live forever**
- Alter and drop of schemas apply only the case-sensitivity capability, and metalake drop skips name validation, so a name accepted by an older release stays usable. Tags and policies once accepted dotted names; since `.` became the qualified-name separator, `toMetadataObject` rejects them but a `...ForRead` variant keeps old rows readable.
- Cause: COMPAT
- Evidence: `core/.../catalog/SchemaNormalizeDispatcher.java:84-85` "for compatibility reasons, we only apply case-sensitive capabilities here"; `core/.../metalake/MetalakeNormalizeDispatcher.java:95` "we only validate the metalake name when creating and altering"; `core/.../utils/NameIdentifierUtil.java:667` "Use toMetadataObjectForRead when addressing an existing tag or policy with a legacy dotted name".
- Mudcat implication: SV-R28. Plan for names that a later rule would forbid but that must still resolve.

**Q-3. `.` is both a legal name character in places and the qualified-name separator**
- `NameIdentifier.parse` splits on `.`; metadata-object full names are dot-joined; catalog names containing a dot are a special case ("MetadataObjects.parse preserves single-level names, including embedded dots").
- Cause: MODEL
- Evidence: `api/.../NameIdentifier.java:76` `Iterable<String> parts = DOT.split(identifier)`; `core/.../NameIdentifierUtil.java:657` "'.' is reserved as the qualified-name separator"; `server/.../filter/ParameterUtil.java` ("MetadataObjects.parse preserves single-level names, including embedded dots", before `checkCatalog`).
- Mudcat implication: never use a printable separator in a path string that identity must round-trip through. Use structured path arrays in the API and a reserved control separator internally (Gravitino itself uses `\u0001` for hierarchical schema names, `core/.../utils/HierarchicalSchemaUtil.java:~37`). Also SV-R25.

**Q-4. Hidden tags named by prefix implement policies**
- Policy-to-tag association creates a tag `gravitino_hidden_{policyId}` in the user's tag namespace; the policy-listing API filters names with that prefix. A user tag with that name collides and fails the offline migration, forcing restore-from-backup.
- Cause: MODEL
- Evidence: `core/.../policy/HiddenPolicyTag.java:31` `PREFIX = "gravitino_hidden_"`; `docs/migration-guide.md:250` "A `Hidden tag name collision` error identifies the policy and tag and rolls back that policy".
- Mudcat implication: SV-R29. If mudcat implements usage policy through tags, use a typed internal relation, not a name convention.

**Q-5. Metalake enable/disable is denormalised into every catalog with no transaction**
- `in-use` is a property on the metalake and `metalake-in-use` is copied into each catalog. Disable writes the metalake row, then updates catalogs one by one; failures are only logged and an exception is thrown after the metalake is already flipped. Force-drop first re-enables a disabled metalake (so catalogs can be dropped through the normal path) and must restore the disabled state if the final delete fails.
- Cause: CONCURRENCY / MODEL
- Evidence: `core/.../metalake/MetalakeManager.java:549-553` "The only problem is that we can't make sure we can change all catalog properties in a transaction"; `:422` `restoreDisabledState(ident, temporarilyEnabled)`; `core/.../connector/BaseCatalogPropertiesMetadata.java:46` `PROPERTY_METALAKE_IN_USE`.
- Mudcat implication: SV-R10, SV-R11, SV-R12.

**Q-6. Cross-server safety of drop relies on the store, not on the lock**
- Gravitino's tree lock is in-JVM. The drop code lists catalogs as an "early check" and then lets the store recheck "under its metalake row lock" because "another server may create a catalog before this delete". A concurrent drop by another server is mapped to `false`.
- Cause: CONCURRENCY
- Evidence: `core/.../metalake/MetalakeManager.java:399-400`, `:411`; `core/.../lock/LockManager.java:30-36` (in-memory tree nodes, `gravitino.lock.maxNodes` = 100000).
- Mudcat implication: SV-R50. HA correctness needs database-level preconditions; an in-process lock is only an optimisation.

**Q-7. Optimistic concurrency exists in three places only, and each is bespoke**
- Semantic model alter takes `expectedRevision` (required when replacing the definition); model/statistics alter can throw `OptimisticLockException` when a concurrent change touched a version/statistic. Everything else (metalake, catalog, schema, table, fileset, tag, policy alter) has no precondition. The server performs a pre-check against the latest revision, then passes `expectedRevision` into the dispatcher (so the store can recheck).
- Cause: CONCURRENCY
- Evidence: `server/.../SemanticModelOperations.java:358-370` `if (latest.revision() != expectedRevision) throw new OptimisticLockException("Semantic Model %s is at revision %s, expected %s", ...)`; `api/.../model/ModelCatalog.java:45` "Conflicting operations may fail with OptimisticLockException ... Callers may retry reads with a bounded number of attempts"; `api/.../stats/SupportsStatistics.java:49` "Gravitino does not automatically retry the write".
- Mudcat implication: SV-R1, SV-R2, SV-R3.

**Q-8. Client does not map the optimistic-lock code in most resource handlers**
- Code 1012 is mapped in two client handlers (lines 1489 and 1622 of `ErrorHandlers.java`); the table, schema and catalog handlers have no case for it and fall to the generic REST error.
- Cause: OTHER
- Evidence: `clients/client-java/.../ErrorHandlers.java:1489,1622` vs `TableErrorHandler` at `:424-474` (no `OPTIMISTIC_LOCK_CONFLICT_CODE` case). Server maps `OptimisticLockException` for every resource in `BaseExceptionHandler` (`ExceptionHandlers.java:1336`).
- Mudcat implication: SV-R17. Generate client error mapping from one error table so a new code cannot be missed.

**Q-9. Error identity is (code, class-name string)**
- 15 numeric codes cover dozens of exceptions. Clients tell `NoSuchSchemaException` from `NoSuchTableException` by comparing `type` to the Java simple class name, and `ALREADY_EXISTS_CODE` always becomes `TableAlreadyExistsException` in the table handler. Messages are trimmed by cutting after the last `"Exception: "` (`ExceptionHandlers.java:1364`).
- Cause: MODEL
- Evidence: `clients/client-java/.../ErrorHandlers.java:437-443`; `server/.../ExceptionHandlers.java:1357-1371`.
- Mudcat implication: SV-R17. The wire contract should name the condition (a stable token) not a Java class.

**Q-10. 409 and 405 carry the "unsupported operation" code**
- `operationConflict` returns HTTP 409 but body code 1006 (UNSUPPORTED_OPERATION) so old clients can rebuild `UnmodifiableStatisticException`. `methodNotAllowed` (authz-disabled, wrong method) returns 405 with the same code 1006.
- Cause: COMPAT
- Evidence: `server-common/.../Utils.java:266-295` "The unsupported-operation error payload is retained so existing clients can reconstruct domain exceptions".
- Mudcat implication: SV-R18.

**Q-11. API version is per-operation and there is one v2 operation**
- The filter appends a versioned Accept to requests that name none (using Content-Type version if present, else v1). Resources declare `@Produces("application/vnd.gravitino.v1+json")` each. Only tag-value association declares v2 on both `@Consumes` and `@Produces`; domains and tags otherwise stay v1.
- Cause: COMPAT
- Evidence: `server/.../VersioningFilter.java:131-166`; `server/.../MetadataObjectTagOperations.java:77,273-274`; `docs/manage-domains-in-gravitino.md:622` "Tag-value requests use v2; Domain endpoints use the v1 API". OpenAPI states "the first version of the Gravitino REST API" (`docs/open-api/openapi.yaml:27`).
- Mudcat implication: SV-R13, SV-R14. Per-operation versions work but need a written policy; otherwise v2 is an accident that nobody can retire.

**Q-12. Version regex is unanchored and the parse is unbounded (inferred bug)**
- `application/vnd\.gravitino\.v(\d+)\+json` is matched with `find()`, so any header containing it matches (a test pins "header containing valid version as substring": `TestVersioningFilter#testDoFilterWithHeaderContainingValidVersionAsSubstring`). `Integer.parseInt` of a very long digit string throws before the filter's error path.
- Cause: OTHER
- Evidence: `server/.../VersioningFilter.java:94,183`.
- Mudcat implication: SV-R15.

**Q-13. Server branches on `X-Client-Version`**
- For fileset `listFiles` the server URL-decodes the sub-path only for clients older than 1.0. A client that sends no header is treated as current.
- Cause: COMPAT
- Evidence: `server/.../FilesetOperations.java:257-259`; `server-common/.../Utils.java:382-391`.
- Mudcat implication: SV-R56. Record what each client version was told so the branch can be deleted later.

**Q-14. `DropResponse` has a vestigial `deleted` field**
- Only role drop sets `deleted`, for old clients that read `deleted`; other drops set it to null.
- Cause: COMPAT
- Evidence: `common/.../dto/responses/DropResponse.java:36-40`.
- Mudcat implication: SV-R14. Response shapes outlive the clients that needed them.

**Q-15. Read-modify-write breaks on masked properties**
- GET masks hidden and name-guessed properties as `******`. PUT or POST with `******` is rejected with 400. A client that round-trips properties must drop masked keys. Masking by key-name keyword (`secret`, `password`, `token`, `credential`, `access`, `account`) also hides harmless keys.
- Cause: SECURITY / MODEL
- Evidence: `core/.../connector/HiddenPropertyMaskUtils.java:74` `MASKED_VALUE = "******"`, class doc lines 30-60; `core/.../catalog/PropertiesMetadataHelpers.java:52` `validateNoMaskedPlaceholders(properties)`.
- Mudcat implication: SV-R31, SV-R32.

**Q-16. Alter validation skips keys the metadata does not declare**
- On alter, `validatePropertyForAlter` only checks keys known to the catalog's property metadata; unknown keys pass. On create, unknown keys also pass unless reserved. So the reserved-prefix rule is enforced only for declared entries and prefix entries.
- Cause: OTHER
- Evidence: `core/.../catalog/PropertiesMetadataHelpers.java:96-99` `if (!propertiesMetadata.containsProperty(...)) continue;`.
- Mudcat implication: SV-R30. Decide per namespace: reject unknown, or allow unknown, and state it.

**Q-17. Shipped template and code defaults disagree**
- Template: `maxThreads=200`, `minThreads=24`, `includeErrorStackTrace=true`, `authorization.serviceAdmins=anonymous`, `authorization.enable=false`. Code: `maxThreads = max(4*cpu, 400)`; the CORS doc says methods "GET,POST,HEAD,DELETE" while the default is `GET,POST,HEAD,DELETE,PUT`.
- Cause: OTHER
- Evidence: `conf/gravitino.conf.template:35,107`; `server-common/.../JettyServerConfig.java:81,264-267`.
- Mudcat implication: SV-R44, SV-R40.

**Q-18. CORS default omits PATCH, which the API uses**
- With `enableCorsFilter=true`, a browser PATCH (metalake/catalog enable-disable, policy, domain) fails preflight unless the operator adds PATCH.
- Cause: SPEC
- Evidence: `JettyServerConfig.java:267` default list vs `MetalakeOperations.java:204` `@PATCH`; four resources use `@PATCH` (grep).
- Mudcat implication: SV-R40.

**Q-19. Shutdown has three disagreeing timeouts**
- `shutdown.timeout` (3 s) is slept in the JVM shutdown hook before stopping; Jetty `stopTimeout` is 30 s; `gravitino.sh stop` sends SIGTERM and SIGKILL after about 10 s. The hook also `Thread.sleep`s first, so stop begins late.
- Cause: OTHER
- Evidence: `GravitinoServer.java:331-335`; `JettyServerConfig.java:84-89`; `bin/gravitino.sh.template:72-98`.
- Mudcat implication: SV-R49.

**Q-20. Failed start leaks started services**
- `GravitinoEnv.start()` starts metrics, listeners and aux services (Iceberg/Lance listeners bind their ports), then `JettyServer.start()` binds the main port. If Jetty fails, `main` calls `System.exit(-1)` and the shutdown hook is not yet registered.
- Cause: OTHER
- Evidence: `GravitinoServer.java:285-288` (`gravitinoEnv.start(); server.start();`), `:318-323`; `GravitinoEnv.java:834-842`.
- Mudcat implication: SV-R48. Start the externally visible listener last and bind all ports before starting background work.

**Q-21. Startup order is load-bearing**
- `CatalogManager` must be built before `MetalakeManager` so force-drop can call `CatalogManager.dropCatalog`; `MetalakeManager`'s constructor preloads every metalake into the cache; `AuxiliaryServiceManager.serviceInit` runs after metadata dispatchers exist (Iceberg REST reads Gravitino state). Shutdown runs aux services, then listeners (so queued audit and built-in-template events finish), then the store, then catalogs.
- Cause: OTHER
- Evidence: `GravitinoEnv.java:1085-1089`, `:1096-1097`, `:848-860`; `MetalakeManager.java:109-113`.
- Mudcat implication: SV-R47. Write the dependency order into the design and test shutdown with queued events.

**Q-22. `initializeBaseComponents`, `initializeMetadataComponents`, `initializeFullComponents` are three profiles of one singleton**
- Iceberg REST (standalone) uses the base profile; the metadata profile "must be called on getInstance() ... Some metadata components read their dependencies directly from that singleton". Event listeners, audit, hooks, aux services and jobs are not initialised in the metadata profile.
- Cause: OTHER
- Evidence: `core/.../GravitinoEnv.java:247-262,266-280,283-291`.
- Mudcat implication: design the gateway as a library over the same services with explicit construction, not a singleton with profiles.

**Q-23. Health: sticky OOM state and cross-component sharing**
- Once an `OutOfMemoryError` is seen anywhere (including in a wrapped cause), all health endpoints stay 503 until restart. With Iceberg/Lance REST embedded, a shared `ServerHealth` marks all of them unhealthy. The docs warn that retrying an oversized request on other replicas can restart them one after another.
- Cause: OTHER
- Evidence: `server-common/.../ServerHealth.java:24-35`; `docs/health-and-readiness.md` ("Out-of-memory Failures").
- Mudcat implication: SV-R20, SV-R35.

**Q-24. Health status casing differs per component**
- Gravitino and Lance REST say `up`/`down`; Iceberg REST says `UP`/`DOWN`; Gravitino also adds `code: 0` to health JSON.
- Cause: COMPAT
- Evidence: `docs/health-and-readiness.md:62-66`.
- Mudcat implication: SV-R34.

**Q-25. Config and filters: which paths are authenticated is a per-path-spec decision**
- Authentication, CORS and custom filters attach to `/api/*` and the listed root paths; `/configs` stays open for the Web UI's OAuth bootstrap. GH-12760: servlets mounted outside `/api/*` once silently got no request-context or audit filters, and a test now checks that every servlet path is covered.
- Cause: SECURITY
- Evidence: `GravitinoServer.java:100-108,267-269`.
- Mudcat implication: SV-R37, SV-R41. Write an invariant test: every route has authentication, audit and error-shape coverage, or is on a named exemption list.

**Q-26. Access-control APIs exist but answer 405 when authorization is off**
- A Jersey filter is registered on a name binding only if `gravitino.authorization.enable` is false; it aborts with 405 and a message naming the config key.
- Cause: OTHER
- Evidence: `GravitinoServer.java:214-216`; `server/.../filter/AccessControlNotAllowedFilter.java` class doc and body.
- Mudcat implication: gate optional subsystems with a typed "not configured" error (mudcat: 501 with a stable token), not a method error.

**Q-27. Default authentication admits any user name; default admin is `anonymous`**
- With the shipped config, the `simple` authenticator trusts the supplied user name, plain HTTP, authorization off; the template sets `serviceAdmins = anonymous`. The docs call this "unfit for anything but local work".
- Cause: SECURITY
- Evidence: `docs/gravitino-server-config.md` (Local/Production sections, around line 40); `conf/gravitino.conf.template:107`; `Configs.java:300-322` (`authenticators` default `simple`).
- Mudcat implication: SV-R44. Ship no default that is unsafe on a public port; require explicit `--insecure-local` mode.

**Q-28. Database schema is not created or checked, except on H2**
- H2 is created automatically; MySQL and PostgreSQL need the operator to run `schema-x.y.z-*.sql` and put the JDBC driver in `libs/`. Upgrades need all servers stopped, a backup, and a per-version SQL script plus (for 2.0) an offline Java tool for policy bindings. A drifted schema is not detected at start (searched `core/.../storage` for a version check; none found).
- Cause: STORE
- Evidence: `docs/gravitino-server-config.md:95`; `docs/how-to-use-relational-backend-storage.md:65`; `docs/how-to-upgrade.md:22-30`; `docs/migration-guide.md:217-264`; `scripts/postgresql/upgrade-1.3.0-to-2.0.0-postgresql.sql`.
- Mudcat implication: SV-R45, SV-R46.

**Q-29. Soft deletion is the uniqueness mechanism**
- Every entity table has `deleted_at BIGINT NOT NULL DEFAULT 0` and `UNIQUE (parent_id, name, deleted_at)`; MySQL tables are created with `COLLATE=utf8mb4_bin` so names are case-sensitive on every backend.
- Cause: STORE
- Evidence: `scripts/postgresql/schema-2.0.0-postgresql.sql:38` `UNIQUE (metalake_name, deleted_at)`; `scripts/mysql/schema-2.0.0-mysql.sql:32` `COLLATE=utf8mb4_bin`. (Storage details belong to the storage report.)
- Mudcat implication: name uniqueness among live rows should be a partial unique index on PostgreSQL, and case behaviour must be set explicitly at schema level.

**Q-30. The change log's ID order is not commit order**
- The cache-invalidation log is polled by `id > cursor`, but IDs allocated by concurrent transactions commit out of order, so a row can appear below the cursor. Gravitino adds a second "replay" query over `created_at` in a 60 s window with a bounded in-memory dedupe set, and documents that it can still lose events.
- Cause: CONCURRENCY / STORE
- Evidence: `core/.../EntityChangeLogBaseSQLProvider.java:42` "IDs are unique but allocation order is not commit order"; `docs/gravitino-server-config.md:410-430` "This is bounded compensation, not a lossless delivery protocol"; PostgreSQL `CURRENT_TIMESTAMP` is transaction start.
- Mudcat implication: SV-R53. The change stream is mudcat's EV area: use a monotonic commit sequence assigned at commit (for example an advisory-lock-ordered sequence or `pg_current_snapshot` xmin horizon), not a bare sequence plus a window.

**Q-31. IDs are 63 random bits**
- `RandomIdGenerator.nextId` is the low 63 bits of `UUID.randomUUID()`. No time component, no per-table collision handling beyond the primary key.
- Cause: OTHER
- Evidence: `core/.../storage/RandomIdGenerator.java:37`.
- Mudcat implication: SV-R51; matches the design's UUIDv7 choice (ID-1).

**Q-32. Iceberg/Lance REST as auxiliary service: one metalake, isolated classloader, advertised URI guessing**
- Aux services load from a `classpath` list in an isolated classloader and share the event bus and `ServerHealth` with the main server. The embedded Iceberg REST serves one metalake; the main server advertises its URL by combining host/port config, replacing a wildcard host with the request's host ("assuming both services share a host"), and requires `advertised-uri` behind a proxy. Deprecated `gravitino.auxService.<name>.*` keys still work and the non-deprecated key wins.
- Cause: COMPAT / SPEC
- Evidence: `server-common/.../Utils.java:449` ("serves exactly one metalake"), `:~410-440` (wildcard host rule); `core/.../auxiliary/AuxiliaryServiceManager.java:278`; `docs/iceberg-rest-service.md:101-102`.
- Mudcat implication: SV-R54, SV-R55. Make the gateway a tenant-aware route, not one instance per tenant.

**Q-33. Bulk API is best-effort, not atomic**
- `/bulk/metalakes/{m}/users/add` returns successes and a `BulkError[]`; there is no all-or-nothing option and a hard cap of 100 items.
- Cause: OTHER
- Evidence: `server/.../BulkOperations.java:88` "best-effort bulk APIs"; `core/.../bulk/BulkManager.java:53-58`.
- Mudcat implication: SV-R21. Decide per batch API: atomic with a precondition, or partial with per-item results; never leave it implicit.

**Q-34. A 429 helper that nothing calls**
- `Utils.tooManyRequests()` exists; there is no rate limit and the Jetty work queue has a fixed bound of 100 (`threadPoolWorkQueueSize`).
- Cause: OTHER
- Evidence: `server-common/.../Utils.java:96`; `JettyServerConfig.java:131-136`.
- Mudcat implication: SV-R22.

**Q-35. Every response metered by name; every handler repeats the same try/catch**
- Each of the 149 operations repeats `@Timed`, `@ResponseMetered`, `Utils.doAs`, `try { ... } catch (Exception e) { return ExceptionHandlers.handleXxxException(...) }`, with a per-resource handler class (about 30). The result is that a new exception type needs a handler edit in each resource.
- Cause: OTHER
- Evidence: `server/.../MetalakeOperations.java:84-140` (pattern); `ExceptionHandlers.java:281-1290` (per-resource handlers).
- Mudcat implication: put error classification in one table keyed by condition; resources only throw.

## 4. Open questions / inconsistencies / half-implemented

- OQ-1. `MetalakePropertiesMetadata` describes `in-use` as "indicating the catalog is in use" (copy-paste from the catalog class). `core/.../metalake/MetalakePropertiesMetadata.java:39`.
- OQ-2. `UNKNOWN_ERROR_CODE = 1100` has the comment "Error codes for invalid state" and sits after 1013 with a gap; code 1000 (`REST_ERROR_CODE`) is used for every Jersey-level 4xx/5xx other than 400/403/405 (`WebApplicationExceptionMapper.java` `toErrorResponse` default branch). So a Jersey-level 406 or 415 has code 1000 with no `type` of a domain exception, while an unmatched route is 404 with code 1003; clients must handle both shapes.
- OQ-3. `tooManyRequests()` is unused; there is no documented overload behaviour of the 100-entry Jetty queue. Needs an experiment.
- OQ-4. Many list APIs return unbounded arrays; `docs/health-and-readiness.md` says an oversized list triggers an OOM and a sticky-down state. No server-side cap found. Unverified whether the Java client pages anything (the Java API has no pagination parameters in the signatures read).
- OQ-5. `gravitino.authenticator` (single) is `deprecated()` and aliased by `gravitino.authenticators`, but the template and old docs may still use it. `Configs.java:300-322`.
- OQ-6. `gravitino.entity.store.maxTransactionSkewTimeMs` (2000 ms default, no range check) names a transaction clock-skew bound; I did not trace where it is read. `Configs.java:325-329`.
- OQ-7. `ConfigServlet` publishes any raw key listed in `visibleConfigs` without checking for a secret-like name; unverified whether an operator warning exists. `ConfigServlet.java:76-85`.
- OQ-8. `AuxiliaryServiceManager.serviceStop` stops each service and rethrows only the first failure; `firstException` is an instance field never cleared. `AuxiliaryServiceManager.java:~225-245`.
- OQ-9. Semantic-model alter checks the revision in the resource and then passes `expectedRevision` to the dispatcher; I did not confirm that the store rechecks atomically (the REST pre-check alone would be a check-then-act). `SemanticModelOperations.java:358-380`.
- OQ-10. `docs/open-api/openapi.yaml:27` still says "the first version of the Gravitino REST API" although `info.version` is 2.0.0-SNAPSHOT and V_2 exists; the Accept-header negotiation rule appears only in the 406 response description (`openapi.yaml:463-470`).
- OQ-11. Helm chart has a `schema-migrate-job.yaml` template and a PodDisruptionBudget; I did not read whether the chart runs the migration automatically on upgrade or only on request (`dev/charts/gravitino/templates/schema-migrate-job.yaml`).
- OQ-12. `NameIdentifier.of(String...)` and `Namespace.of` check only non-null and non-empty levels. No character or length rule exists at the API layer, so the `api` module accepts names the `core` module rejects, and a catalog connector can receive them (inferred from `NameIdentifier.java` and `Namespace.java`).
