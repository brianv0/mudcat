> **Research note, not a requirement document.** Drafted by a code-reading agent from Apache Gravitino commit `9aa78cf` (2026-11-26), then spot-checked by the lead. Line numbers can drift by a few lines; search by file and symbol. Items marked `(inferred)` were not confirmed in code. The text does not follow Simplified Technical English. See [../README.md](../README.md) for the synthesis.

# Events, auditing, lineage/provenance, metrics (ref prefix EA)

Gravitino checkout 9aa78cf. All paths are repo-relative to `/home/user/apache/gravitino`. "core" = `core/src/main/java/org/apache/gravitino`.
Labels: `(inferred)` marks a conclusion I drew from code but did not see stated or tested.

## 1. Decomposition

### 1.1 Event system (core/listener)

```
REST resource (server/.../web/rest/*Operations)            <- authz interceptor runs HERE, before any event
  -> XxxEventDispatcher   (outermost wrapper, 20 of them: Metalake, Catalog, Schema, Table, View, Fileset, Topic,
                           Model, Function, Partition, Statistic, SemanticModel, Tag, Policy, Domain, Job,
                           AccessControl, Owner(OwnerEventManager))        core/.../listener/*EventDispatcher.java
  -> XxxNormalizeDispatcher -> XxxHookDispatcher -> XxxOperationDispatcher / Manager      (GravitinoEnv.java:1115-1232)
```
- Chain order is fixed in `GravitinoEnv.initPublicMetadataDispatchers` (`GravitinoEnv.java:1115,1122,1166,1175`): Event -> Normalize -> Hook -> Manager. The event layer therefore sees the request as the client sent it, before name normalisation and before ownership hooks.
- Each operation emits up to three events: `XxxPreEvent` (before), `XxxEvent` (success), `XxxFailureEvent` (exception). Pattern in `core/.../listener/TableEventDispatcher.java:94-107` (list), `:182-209` (alter). 351 event classes under `core/.../listener/api/event/**`; Iceberg REST has its own 80+ (`iceberg/iceberg-rest-server/.../listener/api/event/Iceberg*Event.java`, dispatcher `iceberg/service/dispatcher/IcebergTableEventDispatcher.java`). Built-in IdP events live in the `idp-basic` plugin (`plugins/idp-basic/.../idp/listener/`).
- Class model: `BaseEvent{user:String, identifier:NameIdentifier?, eventTime=currentTimeMillis at construction}` (`listener/api/event/BaseEvent.java:38-60`); `PreEvent extends BaseEvent` (status UNPROCESSED); `Event extends BaseEvent` (adds `remoteAddress`, auto-captured raw query parameters, final `customInfo()` = query params merged with `ownCustomInfo()`) (`Event.java:57-100`); `FailureEvent extends Event` (adds `Exception`); `SupportsChangingPreEvent` marker (only Iceberg pre-events implement it, e.g. `IcebergUpdateTablePreEvent`).
- `OperationType` enum (~230 values, `OperationType.java`), `OperationStatus {UNPROCESSED,SUCCESS,FAILURE,UNKNOWN}`, `EventSource {GRAVITINO_SERVER, ICEBERG..., LANCE...}`.
- `EventBus` (`listener/EventBus.java`): holds list of `EventListenerPlugin`; `dispatchEvent(BaseEvent)` branches on Pre / Failure / Event. Sets thread-local flags in `RequestContext` so the HTTP filter can suppress duplicates (`EventBus.java:154-189`).
- Listener SPI: `EventListenerPlugin` (`core/.../listener/api/EventListenerPlugin.java`, annotated `@DeveloperApi`; note: it is in core, not the `api` module): `init/start/stop`, `onPreEvent`, `onPostEvent`, `transformPreEvent`, `mode()` in {SYNC, ASYNC_ISOLATED, ASYNC_SHARED}.
- `EventListenerManager` loads `gravitino.eventListener.names` + `gravitino.eventListener.<name>.class`, wraps each plugin in `EventListenerPluginWrapper` (exception firewall), and routes by mode: SYNC straight into the bus list; ASYNC_ISOLATED gets its own `AsyncQueueListener`; ASYNC_SHARED shares one `AsyncQueueListener` named "default" (`EventListenerManager.java:122-148`). In-code listeners via `addEventListener` (used for `audit-log`, `builtin-job-template`).
- `AsyncQueueListener`: `LinkedBlockingQueue(queueCapacity)`, one daemon dispatcher thread per queue, `offer()` that drops on full, high-watermark 90 % (`AsyncQueueListener.java:48,214-220`). Config (code only, not in docs): `gravitino.eventListener.queueCapacity` default 3000, `dispatcherJoinSeconds` default 3 (`EventListenerConfig.java:36-50`).
- Auth-adjacent producers outside the dispatcher chain: `HttpAuditFilter` (server-common) emits `HttpRequestEvent` / `HttpRequestFailureEvent` fallbacks; `BaseMetadataAuthorizationMethodInterceptor` emits `AuthorizationDenialFailureEvent`.
- Consumers in tree: audit-log listener, `LineageSinkEventListener` (lineage/), `PostCommitMaintenanceListener` (plugins/maintenance, reacts to `IcebergUpdateTableEvent`), `BuiltInJobTemplateEventListener` (core/job), IdP dispatcher.

### 1.2 Audit (core/audit, common/audit)
- Audit is NOT a separate pipeline. `AuditLogManager.init` registers an anonymous `EventListenerPlugin` named `audit-log`, mode ASYNC_ISOLATED, that implements only `onPostEvent` (`audit/AuditLogManager.java:62-95`). So audit sees success and failure events, never pre-events.
- `Formatter` (`Event -> AuditLog`), `AuditLogWriter` (`doWrite(AuditLog)`); defaults `SimpleFormatterV2` (tab-separated) / `JsonAuditFormatter`, `FileAuditWriter` writes to logger `gravitino.audit`, file rotation and retention delegated to Log4j2 (`FileAuditWriter.java`). Config: `gravitino.audit.enabled` (default **false**, `Configs.java:517-522`), `gravitino.audit.formatter.className`, `gravitino.audit.writer.className`.
- `AuditLogRedactor`: single redaction point, applied at format time to `customInfo` only (`AuditLogRedactor.java`).
- Fields in an audit line: timestamp, user, operationType, identifier (name string), operationStatus, eventSource, remoteAddress, customInfo{query params, http.method/uri/status, auth.method/expression, event extras}, resultCount for list events (`v2/SimpleAuditLogV2.java:96-129`, `JsonAuditFormatter.java:63-84`). No request id, no event id, no tenant, no entity id, no before/after.
- Legacy `AuditLog.Operation` enum is `@Deprecated`; `v2/CompatibilityUtils` maps new `OperationType` onto it.
- `CallerContext` (common/audit): thread-local map filled from client headers (`InternalClientType`, `FilesetDataOperation`) by `FilesetOperations.java:411-423`; surfaces in `GetFileLocationEvent.context()` so GVFS clients declare what they are about to do (OPEN, CREATE, DELETE, ...).
- Client address: `RequestContextFilter` + `ClientAddressResolver` (trusted-proxy aware X-Forwarded-For parsing, `gravitino.server.webserver.trustedProxies`).

### 1.3 Lineage (lineage/ module)
```
POST /api/lineage (LineageOperations)  [authz: CAN_ACCESS_METADATA, RequestType.LINEAGE]
  -> LineageEventValidator (required fields)
  -> LineageService.dispatchLineageEvent : if sinkManager.isHighWatermark() -> 429
  -> LineageProcessor.process (default NoopProcessor)
  -> LineageSinkManager.sink -> *private EventBus* with EventWrapper<RunEvent> ("mockUser", NameIdentifier "mock")
  -> one ASYNC_ISOLATED LineageSinkEventListener per sink -> LineageSink {LineageLogSink | LineageHttpSink(OpenLineageClient, authType none|apiKey)}
```
- Config prefix `gravitino.lineage.`: `source` (http), `<src>.sourceClass`, `processorClass`, `sinks` (default `log`), `<sink>.sinkClass`, `queueCapacity` (10000 total, split equally per sink, `max(1, total/n)`) (`LineageConfig.java`, `LineageSinkManager.java:70-100`).
- Lineage is forwarded, not stored. Nothing is written to the entity store; no link from run to catalog object is persisted. Docs: "pluggable lineage framework to receive, process, and sink OpenLineage events" (`docs/lineage/gravitino-server-lineage.md`).
- Dataset naming is decided in the engine plugins (separate repo `gravitino-openlineage-plugins`, described in `docs/lineage/gravitino-spark-lineage.md`, `gravitino-flink-lineage.md`): namespace = metalake name, name = `catalog.schema.table` (or `catalog.schema.fileset` for GVFS paths); non-Gravitino datasets get `spark_catalog.*` or raw location. The server only parses that convention for authorisation: `server/.../filter/authorization/LineageAuthorizationExecutor.java`.

### 1.4 Metrics (core/metrics, server-common)
- Dropwizard `MetricRegistry` per `MetricsSource` (names: `gravitino-server`, `iceberg-rest-server`, `lance-rest-server`, `jvm`, `gravitino-catalog.<provider>.<metalake>.<catalog>`, `gravitino-relational-store`, `entity-change-log`). `MetricsSystem` merges them, exports JMX plus `/metrics` (JSON) and `/prometheus/metrics`; Prometheus name/label rewriting by `MapperConfig` rules and `GravitinoSampleBuilder` (regex split on `.`).
- `HttpServerMetricsSource` registers Jersey `InstrumentedResourceMethodApplicationListener` plus Jetty thread-pool gauges (`server-common/.../HttpServerMetricsSource.java`). Per-endpoint timers are named by `@Timed(name="<op>."+MetricNames.HTTP_PROCESS_DURATION)` / `@ResponseMetered` annotations on each REST method.
- `@Monitored` + `MethodMonitorAspect` (AspectJ) time arbitrary methods into total/success/failure.
- `IcebergMetricsManager` (iceberg-rest-server): queue (`LinkedBlockingQueue`, drop on full) of client-posted Iceberg scan/commit `MetricsReport`s, stores via `IcebergMetricsStore` (`dummy` or `jdbc`), retention by days.
- Change-log metrics `EntityChangeLogMetricsSource` (see 1.5).

### 1.5 Entity change log (storage/relational, the closest Gravitino analogue of mudcat's change stream)
- Table `entity_change_log(id BIGSERIAL, metalake_name, entity_type, entity_full_name, operate_type 1=ALTER|2=DROP|3=INSERT, created_at ms)` + index on `created_at` (`scripts/postgresql/schema-2.0.0-postgresql.sql:1176-1190`).
- Written by `EntityChangeLogWriter.append` **inside the mutating transaction** (`EntityChangeLogWriter.java:34`), only from 7 service classes (Schema, Model, Domain, SemanticModel, Policy, JobTemplate, JDBCBackend). Payload is name only (old name for ALTER); no actor, no new state.
- `EntityChangeLogPoller` (one per server process): poll every `gravitino.entityChangeLog.pollIntervalSecs`=3, batch `pollBatchSize`=2000, one shared cursor, listeners get batches, cursor always advances even when a listener throws; bounded "recent-history replay" (`replayWindowSecs`=60, `replayMaxEntries`=100000) re-reads recent rows to catch out-of-order commits of `BIGSERIAL` ids. `EntityChangeLogCleaner` prunes after `retentionSecs`=30 d and refuses a retention below 10 poll cycles.
- Consumers: entity cache, JCasbin authorizer id cache, catalog cache. Purpose is cross-node cache invalidation, not external subscription.

### 1.6 Docs read
`docs/gravitino-server-config.md` (Audit Logging ~547-610, Event Listeners ~620-660, trustedProxies ~190-215, entityChangeLog ~380-450), `docs/metrics.md`, `docs/lineage/*.md`, `docs/security/local-users-and-groups.md#audit-and-events`.

## 2. Candidate requirements

Kind: IMPL = Gravitino does it; GAP = Gravitino fails/omits, mudcat should do better. Areas: EV change stream, PR provenance, AC access control, OP operation, IN interfaces, RD data access, PF performance, ID identity.

| Ref | Kind | Requirement | Rationale / Gravitino evidence | Area | Ver |
|---|---|---|---|---|---|
| EA-R1 | IMPL | mudcat shall emit one terminal event, success or failure, for each mutating request, naming the principal, the operation and the object. | Every dispatcher wraps each mutation in Pre/Event/Failure: `TableEventDispatcher.java:182-209`, `GravitinoEnv.java:1115-1232` (20 dispatchers). | EV | T |
| EA-R2 | GAP | mudcat shall emit a named operation event for each credential issuance, naming the principal, the object and the credential scope. | `MetadataObjectCredentialOperations` has zero event references; credential vending falls to the generic `HttpRequestEvent` with `UNKNOWN` op and null identifier (`HttpRequestEvent.java:30-40,92-94`). Security-critical read is the least described. | RD | T |
| EA-R3 | GAP | mudcat shall derive the operation name of every API route from one route table, so that no route is recorded as an unknown operation. | Fallback `HttpRequestEvent` exists because routes lack events: credentials, secrets (`MetadataObjectSecretOperations`, `SecretsProviderOperations`), lineage ingest, bulk, `listPolicyAssociationsForTag` (`TagEventDispatcher.java:250`), `listTagAssociationsForPolicy` (`PolicyEventDispatcher.java:261`), `listSemanticModelRevisions` (`SemanticModelEventDispatcher.java:181`), all `*Exists` (`TableEventDispatcher.java:242`, `SchemaEventDispatcher.java:103`), all `import*` (`TableEventDispatcher.java:89`). `OperationType.SCHEMA_EXISTS` / `TABLE_EXISTS` exist with no core emitter. | EV | I |
| EA-R4 | IMPL | mudcat shall record every API request that reaches the server, including one rejected before authentication. | `HttpAuditFilter` sits before the auth filter and emits `HttpRequestFailureEvent` for 4xx/5xx and `HttpRequestEvent` for 2xx/3xx without an operation event (`HttpAuditFilter.java:60-80,157-230`); tests `TestHttpAuditFilter` `test401SendErrorEmitsEvent`, `testRuntimeExceptionEscapePromotesTo500AndEmitsEvent`. | AC | T |
| EA-R5 | IMPL | mudcat shall suppress the generic request record of a request for which an operation record exists. | Thread-local outcome flag (`RequestContext.java:138-141`), tests `testOperationSuccessFiredFlagPreventsHttpRequestEvent`, `testStaleOperationFailureFlagDoesNotSuppressCurrentRequestEvent`. | EV | T |
| EA-R6 | GAP | mudcat shall carry the immutable entity ID, not only the name, in every event about an entity. | `BaseEvent.identifier` is a `NameIdentifier` (`BaseEvent.java:38-47`); `TableInfo` has no id field (`info/TableInfo.java:42-50`); `entity_change_log` stores `entity_full_name`. A renamed entity splits its history. | ID | I |
| EA-R7 | GAP | mudcat shall assign each request an ID that appears in the response, in every event of that request and in every audit record. | No request/trace/correlation id in `core/listener`, `core/audit`, `server-common` (grep for requestId/traceId/correlation returns nothing relevant). Pre, success and HTTP-fallback events cannot be joined except by timestamp and thread. | OP | T |
| EA-R8 | GAP | mudcat shall give every event an ID that is unique and stable across redelivery. | No event id in `listener/`; the change-log poller dedups on table row id instead (`EntityChangeLogPoller` `rememberedIds`). Consumers of the in-memory bus cannot deduplicate. | EV | T |
| EA-R9 | GAP | mudcat shall record on every audit record the tenant, the authenticated principal, and the authentication method. | `BaseEvent.user` is a bare string; `HttpAuditFilter.resolveUser` falls back to the literal `"unknown"` (`HttpAuditFilter.java:258-270`). | AC | I |
| EA-R10 | IMPL | mudcat shall take the client address from the TCP peer, and from a forwarding header only when the peer is a configured trusted proxy. | `ClientAddressResolver` right-to-left walk, link-local never trusted, default no proxies; docs `gravitino-server-config.md:190-215`; test `testXForwardedForIgnoredFromUntrustedPeer`. | AC | T |
| EA-R11 | GAP | mudcat shall record on every mutation event the commit the request saw and the commit it made. | Alter events carry only `TableChange[]` + post-state (`AlterTableEvent.java:30-60`); no prior version, no version id. Only Iceberg's `IcebergUpdateTableEvent` carries requirements, updates and the new metadata (`IcebergUpdateTableEvent.java:30-55`). | EV | T |
| EA-R12 | IMPL | mudcat shall let a synchronous policy hook reject an operation before it changes state. | `ForbiddenException` from a SYNC listener's `onPreEvent` aborts the operation (`EventListenerPlugin.java:116-124`, `EventListenerPluginWrapper.java:82-90`, test `testForbiddenPreEvent`). | AC | T |
| EA-R13 | GAP | mudcat shall record a failure event for an operation that a policy hook rejected. | The pre-event call sits outside the `try` in every dispatcher (`TableEventDispatcher.java:95` before `:96`; `:184-186`), so a veto produces no operation failure event. Only the HTTP-level 403 fallback (op UNKNOWN, no identifier) remains. | AC | T |
| EA-R14 | IMPL | mudcat shall ignore a rejection raised by an asynchronous hook and record that it was ignored. | Async ForbiddenException is logged only (`EventListenerPluginWrapper.java:91-93`). The "record" half is missing: only a WARN. | EV | T |
| EA-R15 | IMPL | mudcat shall complete an operation and return its result when an event consumer fails on a success or failure event. | Wrapper swallows listener exceptions (`EventListenerPluginWrapper.java:70-77`); `EventBus.dispatchFailureEvent` catches all (`EventBus.java:186-195`). | EV | T |
| EA-R16 | GAP | mudcat shall not convert a committed mutation into a reported failure when event delivery fails. | Success event is dispatched inside the `try` whose `catch (Exception)` emits a failure event and rethrows (`TableEventDispatcher.java:186-208`). `EventBus` javadoc and `TestEventBus.testDispatchEventPropagatesExceptionsForRegularEvents` pin that a raw listener's exception propagates; only the wrapper prevents it. | EV | T |
| EA-R17 | GAP | mudcat shall keep the audit record of a mutation in the same database transaction as the mutation. | Audit rides the lossy in-memory bus: ASYNC_ISOLATED queue, drop on full (`AuditLogManager.java:90-93`, `AsyncQueueListener.java:214-220`), lost on crash. Contrast `EntityChangeLogWriter.java:34` (append "in the caller's transaction, so it commits or rolls back with the change"). | EV | T |
| EA-R18 | GAP | mudcat shall expose, for each event consumer, the queue depth, the count of dropped events and the processing lag. | Drops are only logged, at most once per 60 s (`AsyncQueueListener.java:180-203`); `EventListenerPluginWrapper` javadoc promises "recording metrics, recording slow event process" (`:35`) but records none; no `Counter` in `core/listener`. | OP | T |
| EA-R19 | GAP | mudcat shall document the capacity, the drop policy and the delivery guarantee of every event consumer mode. | `queueCapacity` (3000) and `dispatcherJoinSeconds` (3) appear in code only (`EventListenerConfig.java:36-50`); no docs hit. Docs say only "events can be dropped". Delivery is at-most-once. | OP | I |
| EA-R20 | IMPL | mudcat shall drain queued events at shutdown within a configured time limit, and report the number dropped. | `AsyncQueueListener.stop`: STOP_SIGNAL behind pending events, bounded join, interrupt, then WARN "drop N events" (`AsyncQueueListener.java:106-135,168-173`); tests `testStopDrainsQueuedEvents`, `testStopInterruptsDispatcherThatDoesNotFinishInTime`. | OP | T |
| EA-R21 | GAP | mudcat shall apply event transformations and synchronous hooks in the order the operator configured. | Javadoc says "Transformation order follows the event listener configuration sequence" (`EventListenerPlugin.java:129`) but `EventListenerManager.init` collects with `Collectors.toMap` into a `HashMap` (`EventListenerManager.java:83`), so iteration order is by hash `(inferred)`. | OP | T |
| EA-R22 | IMPL | mudcat shall isolate a slow event consumer from the other consumers by giving it its own queue on request. | `ASYNC_ISOLATED` vs `ASYNC_SHARED` ("One slow listener degrades the rest", `gravitino-server-config.md` Event Listeners table). | PF | T |
| EA-R23 | GAP | mudcat shall not rely on an in-memory event for the correctness of any derived index or maintenance job. | `PostCommitMaintenanceListener` schedules table maintenance from `IcebergUpdateTableEvent` on an ASYNC_ISOLATED queue (`PostCommitMaintenanceListener.java:70-100`); a dropped or crash-lost event is lost silently. Whether a periodic scan backstops it was not found `(inferred)`. | EV | A |
| EA-R24 | IMPL | mudcat shall write the change record in the transaction of the change it describes. | `EntityChangeLogWriter.java:34`. | EV | T |
| EA-R25 | GAP | mudcat shall make a change record visible to consumers only when no lower-numbered transaction can still commit. | `entity_change_log.id` is `BIGSERIAL`: commit order differs from id order. Gravitino compensates with a time-bounded replay of the last 60 s plus a bounded dedup set, and its doc admits "does not guarantee delivery beyond the window" (`EntityChangeLogPoller.java:~70-78`, config `replayWindowSecs`). A row committing after the window is lost. | EV | T |
| EA-R26 | GAP | mudcat shall keep each consumer's own cursor, retry a failing consumer from its cursor, and not block the other consumers. | One cursor per process, shared by all listeners, "always moves the read position forward, even if a listener failed"; recovery is "clear your whole cache" (`EntityChangeLogPoller.java:49-56`, `EntityChangeLogListener.java:onEntityChange`). | EV | T |
| EA-R27 | GAP | mudcat shall keep a change record until every registered consumer has passed it, or mark that consumer as lost. | Retention is time-based, 30 d default; the cleaner only refuses a retention under 10 poll cycles because a short one "silently lose[s] cache invalidations" (`EntityChangeLogCleaner.java:54,76-79`). | EV | T |
| EA-R28 | IMPL | mudcat shall expose the lag of each change-stream consumer in records and in seconds. | Metrics `record-lag`, `seconds-since-last-successful-poll`, `listener-failures.<class>-total`, `replay-records-total` (`docs/metrics.md`, Entity Change Log section). | EV | T |
| EA-R29 | IMPL | mudcat shall write change records for create, alter and drop with a stable numeric operation code that is never reused. | `operate_type` codes 1/2/3 "stable and never re-used" (`schema-2.0.0-postgresql.sql:1180`). | EV | I |
| EA-R30 | GAP | mudcat shall write change records for every entity type, including tags, policies, roles, grants and usage policies. | Only 8 entity types in the comment and 7 writers; authorisation-relevant changes are tracked elsewhere (JCasbin listener) `(inferred)`. | EV | I |
| EA-R31 | IMPL | mudcat shall record each denied request with the principal, the object and the rule that was evaluated. | `AuthorizationDenialFailureEvent` (`user, identifier, methodName, expression`), dispatched from the authz interceptor; the record never changes the 403 response (`BaseMetadataAuthorizationMethodInterceptor.java:428-445`). | AC | T |
| EA-R32 | GAP | mudcat shall record the rule or relationship that granted each access to restricted data. | Only denials carry an expression; granted reads carry only the operation event `(inferred)`. | AC | D |
| EA-R33 | IMPL | mudcat shall record read operations (load, list, resolve) as events with the result count for lists. | `LoadTableEvent`, `ListTableEvent(resultCount)`, `GetFileLocationEvent(actualFileLocation, subPath, context)`; `JsonAuditFormatter.java:75-81`. | AC | T |
| EA-R34 | IMPL | mudcat shall record in the audit entry of a data-location request the location it returned and the intent the client declared, marking the intent as client-asserted. | `GetFileLocationEvent.java` (`actualFileLocation`, `context`), `FilesetOperations.java:411-423` (headers -> `CallerContext`). The declared op (`FilesetDataOperation`) is unverifiable. | RD | I |
| EA-R35 | IMPL | mudcat shall redact secret values from audit records in one function, by key-name rule applied to the merged record. | `AuditLogRedactor.java` (exact keys + substrings password/secret/token/credential/apikey/accesskey/privatekey/auth/signature on separator-stripped key; never-sensitive literals). Tests `TestAuditLogRedactor.testSubstringMatchIgnoresSeparators`, `testCodebaseChosenAuthKeysAreNeverRedacted`. | AC | T |
| EA-R36 | GAP | mudcat shall not place a secret value in any event object handed to a consumer. | Events carry raw query parameters and raw catalog/table properties; redaction only in the two built-in formatters (`Event.java:37-45`, docs "a custom listener receives them **unredacted**"). `CatalogInfo`/`CreateCatalogEvent` have no masking code `(inferred)`; redactor lists `jdbc-password` as a key that reaches `customInfo`. | AC | I |
| EA-R37 | GAP | mudcat shall keep each operation type distinct in the audit record, and version the audit record schema. | `CompatibilityUtils` collapses `LIST_PARTITION_NAMES` -> `LIST_PARTITION`, `LIST_USERS_PAGED` and `COUNT_USERS` -> `LIST_USERS`, `COUNT_GROUPS` -> `LIST_GROUPS` (`CompatibilityUtils.java:113,154-155,169`); legacy `Operation`/`Status` fields kept `@Deprecated` beside `operationType`/`operationStatus`; Iceberg update reuses `OperationType.ALTER_TABLE`. | OP | I |
| EA-R38 | GAP | mudcat shall store the audit record in an append-only store whose retention does not delete records unseen by an auditor. | `FileAuditWriter` hands lines to Log4j2; default rotation deletes archives after 30 days or beyond 10 GB, oldest first (`gravitino-server-config.md` ~583-600). No integrity chain `(inferred)`. | AC | I |
| EA-R39 | GAP | mudcat shall write an audit record in every deployment, with no switch that turns audit off. | `gravitino.audit.enabled` default `false` (`Configs.java:517-522`). | OP | I |
| EA-R40 | IMPL | mudcat shall record the interface through which each operation arrived. | `EventSource` on every event; Iceberg and Lance servers install the same `HttpAuditFilter` with their own source (`HttpAuditFilter` ctor). | IN | T |
| EA-R41 | IMPL | mudcat shall exclude health probes from the audit log. | `HealthCheckPathMatcher`; `testHealthCheckPathsSkipped`. | OP | T |
| EA-R42 | IMPL | mudcat shall accept OpenLineage run events on an HTTP endpoint that follows the OpenLineage API. | `LineageOperations` `@Path("/lineage")`, 201 on accept (`LineageOperations.java:55-85`); docs example. | IN | T |
| EA-R43 | IMPL | mudcat shall reject an OpenLineage run event that lacks eventTime, producer, schemaURL, run id, job namespace, job name, or a dataset namespace or name. | `LineageEventValidator.java:36-72`; 400 via `Utils.illegalArguments`. | IN | T |
| EA-R44 | GAP | mudcat shall store each accepted OpenLineage run event durably before it returns success. | `LineageService.dispatchLineageEvent` returns `true` (HTTP 201) after enqueue only; processor default is no-op; sinks are log file or HTTP forward; sink exceptions are swallowed by the wrapper, no retry (`LineageService.java:66-77`, `LineageHttpSink.java:55-65`). Accepted lineage can vanish. | PR | T |
| EA-R45 | GAP | mudcat shall link each stored lineage run to the catalog objects it names, by entity ID, and keep the link when the object is renamed. | Datasets are resolved to `MetadataObject` only to evaluate permissions (`LineageAuthorizationExecutor.java:127-150`); nothing is persisted, so lineage cannot be queried from a table. | PR | T |
| EA-R46 | IMPL | mudcat shall map an OpenLineage dataset to a catalog object by namespace equal to the tenant and name equal to the object path, with a dataset-type facet selecting the object type. | `getMetadataType`: facet `datasetType` in {TABLE, VIEW, FILE/FILESET, MODEL, TOPIC}, default TABLE (`LineageAuthorizationExecutor.java:99-124`); `MetadataObjects.parse(dataset.getName(), type)` (`:147`); doc table for Spark in `gravitino-spark-lineage.md`. | PR | T |
| EA-R47 | GAP | mudcat shall keep a dataset that does not resolve to a catalog object as an external reference with its original namespace and name. | Unresolved names fail the whole event as 400 ("Unsupported dataset type", malformed name) when authorization is on; with authorization off any namespace is accepted (`TestLineageOperations.testAuthorizationDisabledAcceptsGenericNamespaces`). Engines emit `spark_catalog.*` or raw HDFS paths for foreign data (docs). | PR | T |
| EA-R48 | IMPL | mudcat shall reject a lineage event whose dataset namespace differs from its job namespace. | `LineageAuthorizationExecutor.java:155-160`; test `testRejectCrossOrganizationDatasetAtEveryPosition`. | AC | T |
| EA-R49 | IMPL | mudcat shall authorise a lineage event against every input and output dataset it names, and accept an event with none. | `execute()` loops all targets (`:76-95`); tests `testAuthorizeAllInputAndOutputDatasets`, `testPermissionFailureAtEveryDatasetPosition`, `testAllowDatasetlessEvent`. | AC | T |
| EA-R50 | GAP | mudcat shall record a lineage claim about an output dataset only when the sender may write that dataset or holds a run token for that run. | Output datasets need metadata visibility only (`TestLineageAuthorizationExecutor.testOutputRequiresMetadataVisibilityOnly`, authorizer asked for `SELECT_TABLE`): any reader can assert it produced a table. `CAN_ACCESS_METADATA` is the single expression (`LineageOperations.java:59`). | PR | T |
| EA-R51 | GAP | mudcat shall record the authenticated sender of each lineage event, separately from the producer the event claims. | `Utils.doAs` only sets the thread principal; the forwarded `RunEvent` is unchanged, `producer` is client-supplied, and the private bus uses `EventWrapper("mockUser", "mock")` (`EventWrapper.java:41`). | PR | T |
| EA-R52 | IMPL | mudcat shall return the same response for a tenant that does not exist and a tenant the caller cannot see. | `TestLineageAuthorizationExecutor.testMissingAndInaccessibleDynamicMetalakeHaveSameResponse` (403 both, same code/type/message). | AC | T |
| EA-R53 | IMPL | mudcat shall answer a lineage post with a retryable status when its queue is above a high-water mark. | 90 % of per-sink capacity -> `429` (`LineageService.java:71`, `LineageOperations.java:81`, docs "High Watermark Status"). | PF | T |
| EA-R54 | GAP | mudcat shall not let one slow lineage consumer cause rejection of events for the other consumers. | `EventBus.isHighWatermark` is `anyMatch` over all sink queues (`EventBus.java:127`); the check precedes `sink()` non-atomically, and a full queue still drops silently (`AsyncQueueListener.java:214-220`). | PF | T |
| EA-R55 | GAP | mudcat shall retain each OpenLineage facet it does not interpret, unchanged, with its `_producer` and `_schemaURL`. | Server interprets only `datasetType`; the event is round-tripped through two different `OpenLineage` model classes by Jackson (`lineage/Utils.getClientRunEvent`). Whether unknown facets survive was not tested `(inferred)`. | PR | T |
| EA-R56 | GAP | mudcat shall mark a lineage run as abandoned when no terminal event arrives within a time limit TBD. | Flink doc: a killed JobManager emits no COMPLETE/FAIL; consumers "can rely on a timeout since the last START" (`gravitino-flink-lineage.md` Known Limitations). Server has no run state at all. | PR | T |
| EA-R57 | IMPL | mudcat shall expose, for each named operation, the request count by HTTP status class and the latency quantiles. | `gravitino_server_2xx_responses_total{operation=..}`, `..http_request_duration_seconds{quantile=..}` (`docs/metrics.md`); `MetricsSystem.getMetricNameAndLabelRules`. | OP | D |
| EA-R58 | GAP | mudcat shall instrument every route through one interceptor, not per-method annotations. | `@Timed` is hand-placed: `SemanticModelOperations.java` has 10 HTTP methods and 7 `@Timed`; the script `grep -c` over `server/.../rest/*.java` shows only that file mismatching, i.e. the rule holds by discipline. | OP | I |
| EA-R59 | GAP | mudcat shall require authentication on every endpoint that exposes metrics. | Docs: "Standalone metrics endpoints are unauthenticated, so enable them only on a trusted network" (`docs/metrics.md:17`); `/metrics` mounted on the raw servlet context outside Jersey and the auth filter (`JettyServer.java:83`, `GravitinoServer.java:101-107`). | AC | T |
| EA-R60 | GAP | mudcat shall label metrics with tenant and catalog as label values, never as segments of a dotted metric name. | `GravitinoSampleBuilder` regex `gravitino-catalog\.([^.]+)\.([^.]+)\.([^.]+)\.(.+)` (`GravitinoSampleBuilder.java:64-66`): a name containing `.` mis-parses `(inferred)`. | OP | T |
| EA-R61 | IMPL | mudcat shall drop a client-posted metrics report when its queue is full, and say so in a log entry naming the report. | `IcebergMetricsManager.recordMetric` returns false and logs "Drop Iceberg metrics because metrics queue is full" (`IcebergMetricsManager.java:130`). | PF | T |
| EA-R62 | GAP | mudcat shall expose a request, status and latency metric for each mounted path, including paths mounted outside the main router. | `/metrics`, `/prometheus/metrics`, `/configs` were mounted outside the audited path and had to be added later ("See GH-12760", `GravitinoServer.java:96-107`). | OP | I |

## 3. Quirks and gotchas ledger

- **Pre-event veto is invisible to audit**
  - A SYNC listener's `ForbiddenException` stops the operation, but the dispatcher called `dispatchEvent(pre)` before its `try`, so no `XxxFailureEvent` is produced. Audit shows only the generic 403 `HttpRequestFailureEvent` with op `UNKNOWN`, no identifier. (Authorization denial from the REST interceptor, by contrast, does get its own event.)
  - Cause: OTHER. Evidence: `core/.../listener/TableEventDispatcher.java:95-96` `eventBus.dispatchEvent(new ListTablePreEvent(...)); try {`.
  - Mudcat implication: EA-R13.

- **A veto only works in SYNC mode; an async listener throwing Forbidden is logged and ignored**
  - Same exception, different outcome by `mode()`. Also the first vetoing listener stops the `forEach`, so later listeners (including the async audit queue, which ignores pre-events anyway) do not see that pre-event.
  - Cause: OTHER. Evidence: `EventListenerPluginWrapper.java:82-93` `if (Mode.SYNC.equals(mode())) { ... throw e; } printExceptionInEventProcess(...)`; `EventBus.java:206` `eventListeners.forEach(...)`.
  - Mudcat implication: EA-R12, EA-R14.

- **Exception contract in `EventBus` doc disagrees with real behaviour**
  - Javadoc: success-event listener exceptions "are propagated to the caller" and a test pins it with a raw listener. In production every listener is wrapped by `EventListenerPluginWrapper`, which swallows `Exception`. Two layers state opposite rules.
  - Cause: OTHER. Evidence: `EventBus.java:99` "`Event` (success events): Listener exceptions are propagated"; `EventListenerPluginWrapper.java:70-77` `catch (Exception e) { printExceptionInEventProcess(...)`.
  - Mudcat implication: EA-R15, EA-R16.

- **Success event is dispatched inside the `try` that emits a failure event**
  - If post-event dispatch ever threw (an `Error`, or an unwrapped listener), the catch emits a `FailureEvent` for an operation that already committed, and the client gets an error.
  - Cause: CONCURRENCY/OTHER. Evidence: `TableEventDispatcher.java:187-208` (success dispatch at 189, `catch (Exception e)` at 200).
  - Mudcat implication: EA-R16.

- **Listener order is a hash order, not configuration order**
  - Javadoc on `transformPreEvent` promises config order. The manager builds the listener map with `Collectors.toMap` (HashMap).
  - Cause: OTHER. Evidence: `EventListenerManager.java:83` `Collectors.toMap(listenerName -> listenerName, ...)`; `EventListenerPlugin.java` "Transformation order follows the event listener configuration sequence". `(inferred)` for the HashMap consequence.
  - Mudcat implication: EA-R21.

- **Audit is lossy by construction**
  - Audit uses the shared event machinery: bounded queue (default 3000), `offer()` drops the newest event when full, drop is logged at most once a minute, shutdown gives 3 s to drain, crash loses the queue. Audit write errors are WARN-logged and swallowed.
  - Cause: CONCURRENCY. Evidence: `AsyncQueueListener.java:214-220` `if (queue.offer(baseEvent)) { return; } logDropEventsIfNecessary();`; `AuditLogManager.java:84-87` `LOG.warn("Failed to write audit log {}", event, e)`.
  - Mudcat implication: EA-R17, EA-R18, EA-R19.

- **Entity change log: serial ids commit out of order**
  - A consumer that reads "id > cursor" can skip a row that commits later with a lower id. Gravitino added a time-windowed replay, a dedup set (evictions permit duplicates), a replay-sweep schedule and 10 metrics for it. Rows that commit outside the 60 s window are lost.
  - Cause: STORE/CONCURRENCY. Evidence: `EntityChangeLogPoller.java` class doc "Replay catches out-of-order commits within a configured record-age window ... It does not guarantee delivery beyond the window"; `schema-2.0.0-postgresql.sql:1177` `id BIGSERIAL PRIMARY KEY`.
  - Mudcat implication: EA-R25.

- **The poller advances the cursor even when a listener failed**
  - The contract is "listener must clear its whole cache on error". A new listener that cannot self-heal is a design error by the class doc itself.
  - Cause: OTHER. Evidence: `EntityChangeLogPoller.java:49-56` "then always moves the read position forward, even if a listener failed"; `EntityChangeLogListener.java` "A failed callback is not retried".
  - Mudcat implication: EA-R26.

- **Retention can silently outrun a slow node**
  - Cleaner uses wall time only; guard is "retention >= 10 x poll interval". A node down longer than retention loses invalidations with no signal.
  - Cause: OTHER. Evidence: `EntityChangeLogCleaner.java:76-79` "can prune change records before every node consumed them and silently lose cache invalidations".
  - Mudcat implication: EA-R27.

- **Change-log append is in-transaction, events are not**
  - Same codebase, two disciplines: the cache-invalidation log is transactional, the user-facing event/audit path is post-commit, in-memory and lossy. A crash after commit and before enqueue loses the audit record of a committed change.
  - Cause: OTHER. Evidence: `EntityChangeLogWriter.java:34` vs `TableEventDispatcher.java:187-189` (event after `dispatcher.alterTable` returned).
  - Mudcat implication: EA-R17, EA-R24.

- **Events reach listeners with unredacted query parameters and properties**
  - `Event` constructor copies every request query parameter into `customInfo`; redaction exists only inside the two audit formatters. A custom listener forwarding `customInfo()` leaks `?token=`. This design note was added after an earlier version let subclasses overwrite `customInfo()` and lose the captured parameters (final method now).
  - Cause: SECURITY. Evidence: `Event.java:37-45` "every ... EventListenerPlugin ... receives these parameters unredacted"; `Event.java:48` "an earlier version of this class let subclasses override customInfo() directly".
  - Mudcat implication: EA-R36.

- **Generic fallback events exist because coverage is incomplete**
  - Credential, secret, lineage and `*Exists` requests are audited only as `HttpRequestEvent(UNKNOWN, null identifier)`: method, URI, status, query. The object appears only inside the URI string. Docs warn about volume (an Iceberg client polling `/v1/config`).
  - Cause: OTHER. Evidence: `HttpRequestEvent.java:30-40` "for which no operation-layer success Event was dispatched ... a REST endpoint that has not (yet) been wired"; `MetadataObjectCredentialOperations.java` has no Event reference.
  - Mudcat implication: EA-R2, EA-R3.

- **Dispatchers silently skip some methods**
  - `importTable/importSchema/importView`, all `*Exists`, `listPolicyAssociationsForTag`, `listTagAssociationsForPolicy`, `listSemanticModelRevisions`, `isServiceAdmin` call the delegate with no events. `importSemanticModel` throws `UnsupportedOperationException`.
  - Cause: OTHER. Evidence: `TableEventDispatcher.java:89-91` `return dispatcher.importTable(ident, auditInfo);`; `SemanticModelEventDispatcher.java:241-242`.
  - Mudcat implication: EA-R3.

- **Thread-local request context is the glue**
  - Remote address, query params, outcome flags and "audit extras" are `ThreadLocal`s; `takeAuditExtras()` is consumed only by create/alter/load dispatchers. Work moved to another thread loses them; stale flags on pooled threads needed explicit resets and tests.
  - Cause: CONCURRENCY. Evidence: `RequestContext.java:53-60` (four ThreadLocals), `HttpAuditFilter.java:173` "Defensive cleanup at request entry in case a pooled thread leaked stale state"; test `testStaleOperationFailureFlagDoesNotSuppressCurrentRequestEvent`.
  - Mudcat implication: EA-R5, EA-R7 (carry an explicit request context object).

- **Success + 5xx and failure + 2xx outcomes are both possible for one request**
  - An operation can succeed and the HTTP response fail (serialisation): the filter then emits a failure event in addition to the success event, by design. If any failure event fired, a 2xx request gets no fallback success record.
  - Cause: OTHER. Evidence: `HttpAuditFilter.java` class doc "Success + 5xx edge case"; `RequestContext.markOperationSuccessFired` does not override FAILURE (`:138-141`); test `testSuccessEventFollowedBy5xxStillEmitsHttpFailureEvent`.
  - Mudcat implication: EA-R5.

- **`XForwardedFor` handling is an auditing security issue**
  - Any caller can forge the header; the fix walks right-to-left over trusted proxies; malformed entries stop the walk at the last trusted hop; link-local addresses are never trusted; with no proxy list, the proxy's address is recorded for every request.
  - Cause: SECURITY. Evidence: `ClientAddressResolver.java` class doc rules 1-5; `docs/gravitino-server-config.md:190-210`.
  - Mudcat implication: EA-R10.

- **Lineage 201 means "queued", not "kept"**
  - 201 is returned after enqueue to per-sink queues; the processor is a no-op; sink exceptions (HTTP sink down) are logged by the wrapper and dropped; a full queue drops silently. The 429 high-watermark check is separate and racy.
  - Cause: OTHER. Evidence: `LineageService.java:66-77` `if (sinkManager.isHighWatermark()) return false; ... sinkManager.sink(newEvent); return true;`; `LineageHttpSink.java:55-65`.
  - Mudcat implication: EA-R44, EA-R54.

- **Lineage reuses the event bus through a fake event**
  - `EventWrapper<T>` is an `Event` with user `"mockUser"` and identifier `mock`; `LineageSinkManager` builds a private `EventListenerManager` and translates lineage config into event-listener config strings.
  - Cause: MODEL. Evidence: `EventWrapper.java:41` `super("mockUser", NameIdentifier.of("mock"));`; `LineageSinkManager.java:60-100`.
  - Mudcat implication: use a generic typed envelope, not entity events, for non-entity streams.

- **Output datasets need only read-level visibility**
  - A lineage event naming a table as an output is authorised with the same expression as an input (`CAN_ACCESS_METADATA`; test shows `SELECT_TABLE` checked on an output). Any user who can see a table can assert that their job wrote it.
  - Cause: SECURITY. Evidence: `TestLineageAuthorizationExecutor.java` `testOutputRequiresMetadataVisibilityOnly` (stubs `Privilege.Name.SELECT_TABLE` for `output(DATASET_NAME)`).
  - Mudcat implication: EA-R50, EA-R51.

- **Lineage dataset identity is a dot-joined string with a type facet**
  - `datasetType` facet picks the entity kind; default TABLE when the facet is absent; "FILE" and "FILESET" both map to FILESET; any other value is a 400. The name is parsed with `MetadataObjects.parse`, splitting on dots. A name with a dot inside a segment is ambiguous `(inferred)`.
  - Cause: MODEL. Evidence: `LineageAuthorizationExecutor.java:99-124,147`.
  - Mudcat implication: EA-R46, EA-R47, EA-R60.

- **Lineage tenant check only when authorization is on**
  - With authorization disabled, generic namespaces (any string) pass; with it on, `dataset.namespace` must equal `job.namespace`, and `job.namespace` is interpreted as the metalake. Engines must therefore set `spark.openlineage.namespace=<metalake>`.
  - Cause: SPEC/ENGINE. Evidence: `TestLineageOperations.java:95-105`; `LineageAuthorizationExecutor.java:155-160`; `gravitino-spark-lineage.md` config (`spark.openlineage.namespace=${metalakeName}`).
  - Mudcat implication: EA-R47, EA-R48; bind namespace to tenant at import, not in client config.

- **Engines define the identifier mapping, outside this repo**
  - The Spark/Flink plugins live in a separate repo (`datastrato/gravitino-openlineage-plugins`, `docs/lineage/gravitino-spark-lineage.md`). They rewrite engine identifiers to `catalog.schema.table`, with `useGravitinoIdentifier=false` as an escape. Flink cannot resolve temporary tables/views (planner gives a null object path); Flink1 has no SQL lineage; no column lineage in Flink; killed jobs emit no terminal event.
  - Cause: ENGINE. Evidence: `gravitino-flink-lineage.md` "Known Limitations" (anonymous sources, forceful termination).
  - Mudcat implication: EA-R47, EA-R56; resolution should be server-side from a physical location (symlinks facet) as well.

- **One slow lineage sink rejects all lineage**
  - `isHighWatermark()` is true when any async queue exceeds 90 % of its share of `queueCapacity / numberOfSinks`, so adding sinks shrinks each queue.
  - Cause: CONCURRENCY. Evidence: `EventBus.java:127` `asyncQueueListeners.stream().anyMatch(AsyncQueueListener::isHighWatermark)`; `LineageSinkManager.java:75-82`.
  - Mudcat implication: EA-R54.

- **Metrics: per-method annotation coverage and timers that read zero**
  - Endpoint metrics exist only if the author added `@Timed`/`@ResponseMetered`; 3 of 10 HTTP methods in `SemanticModelOperations` lack `@Timed`. Duration timers used a 60 s sliding window and read zero for rarely-called operations; replaced by an exponentially-decaying reservoir, and the old config key is deprecated.
  - Cause: OTHER. Evidence: `docs/gravitino-server-config.md:313` "`gravitino.metrics.timeSlidingWindowSecs` Deprecated, no longer used ... keep reporting a real duration for far longer"; `LineageOperations.java:56-58` annotation pattern.
  - Mudcat implication: EA-R58.

- **Metrics sources are registered by name and replaced racily**
  - Catalog metric sources are re-created when a cached catalog expires; a Caffeine removal listener unregisters asynchronously, so the old source's unregister may run after the new one registered. `unregister` therefore compares object identity.
  - Cause: CONCURRENCY. Evidence: `MetricsSystem.java` `register` javadoc (steps 1-5), `unregister` identity check ("it's not the same object that registered").
  - Mudcat implication: any per-tenant/catalog metric lifecycle must tolerate re-registration; use labels on one long-lived registry.

- **Metrics endpoints bypass the auth and audit chain**
  - `/metrics`, `/prometheus/metrics`, `/configs` are mounted on the shared servlet context, outside Jersey; request-context and audit filters were bolted on later (GH-12760); standalone Iceberg/Lance metrics are unauthenticated.
  - Cause: SECURITY. Evidence: `GravitinoServer.java:96-107`; `JettyServer.java:76-83`; `docs/metrics.md:17`.
  - Mudcat implication: EA-R59, EA-R62.

- **Audit operation names collapse in the legacy enum**
  - `COUNT_USERS`, `LIST_USERS_PAGED` log as `LIST_USERS`; `LIST_PARTITION_NAMES` as `LIST_PARTITION`; `GET_FILESET_LOCATION` as `GET_FILE_LOCATION`. The v2 text formatter prints `operationType()`, but the JSON formatter prints both `operation` and `operationType`, so consumers see two vocabularies.
  - Cause: COMPAT. Evidence: `CompatibilityUtils.java:113,154-155,169`; `JsonAuditFormatter.java:66-69`.
  - Mudcat implication: EA-R37.

## 4. Open questions / inconsistencies / half-implemented

- Whether unknown OpenLineage facets survive the server -> client class round trip in `Utils.getClientRunEvent` (`lineage/Utils.java`) is untested in `lineage/src/test` (only `TestLineageConfig`, `TestLineageService`, `TestLineageOperations`, sink tests). Facet preservation is therefore unknown (EA-R55).
- `LineageProcessor` is the only enrichment seam and the only implementation is `NoopProcessor`. There is no in-tree code that attaches lineage to catalog objects, stores it, or exposes a lineage read API. The feature is receive-and-forward by design.
- `SemanticModelEventDispatcher.importSemanticModel` throws `UnsupportedOperationException("Parsed imports are internal; use importOssieDocument ...")` while `OperationType.IMPORT_SEMANTIC_MODEL` and an Import event exist: two import paths, one audited.
- `OperationType.SCHEMA_EXISTS`, `TABLE_EXISTS`, `VIEW_EXISTS` exist and are mapped in `CompatibilityUtils`, but `SchemaEventDispatcher.schemaExists`, `TableEventDispatcher.tableExists` and the view equivalent emit nothing. Only Iceberg `*ExistsEvent`s emit them.
- `EventListenerPluginWrapper` Javadoc promises "recording metrics, recording slow event process" (`:35`); neither exists.
- `AuditLogManager.init` is skipped entirely when audit is off, so the `HttpAuditFilter` still runs but `eventBus` has no audit listener. A deployment with only a custom listener still pays the filter cost `(inferred)`.
- `EntityChangeLog` covers 8 entity types and 7 writers; whether tag, policy-association, owner, role and grant changes are intentionally absent (they use the JCasbin listener) or forgotten is not stated.
- Pre-event transformation (`transformPreEvent`) is implemented in the bus and plugin SPI but only the Iceberg events implement `SupportsChangingPreEvent`; the native API has no mutable pre-events. An Iceberg dispatcher calls `transformedEvent.get()` unconditionally (`IcebergTableEventDispatcher.java:147-150`).
- `EventListenerPlugin` Javadoc in core says `onPreEvent` for ASYNC modes may change resources "in the event"; async pre-events are a copy dispatched later, so no effect is possible. Doc inconsistency.
- No documented SLO or ordering guarantee between listeners; ASYNC_SHARED listeners share one thread, so a slow one delays others, and no per-listener timeout exists.
- Time source: `eventTime` is `System.currentTimeMillis()` of the node that built the event; multi-node ordering of events across servers is by wall clock only.

### Relevance to mudcat (summary)
- **EV change stream**: copy the transactional-outbox discipline of `entity_change_log` (EA-R24), fix its three weaknesses (late-commit gap, shared cursor, time-based retention: EA-R25..R27), and add event id, entity id, request id, actor, commit before/after (EA-R6..R8, R11). Do not copy the in-memory post-commit bus as a source of truth for audit or derived state (EA-R17, EA-R23).
- **Audit**: audit should be one subscriber of the same durable stream, including reads, denials, credential issuance (EA-R2) and vetoes (EA-R13); redact at the edge and never put secrets in event objects (EA-R35, R36).
- **PR / OpenLineage import**: Gravitino gives the namespace=tenant, name=object-path, datasetType-facet convention, per-dataset authorisation and 429 back-pressure (EA-R46, R48, R49, R53) but stores nothing and links nothing (EA-R44, R45). mudcat's import must persist runs, link by entity ID, keep unresolved datasets as external references, attribute the sender, and require write rights or a run token for outputs (EA-R47, R50, R51).
