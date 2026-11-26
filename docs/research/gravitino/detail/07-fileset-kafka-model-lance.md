> **Research note, not a requirement document.** Drafted by a code-reading agent from Apache Gravitino commit `9aa78cf` (2026-11-26), then spot-checked by the lead. Line numbers can drift by a few lines; search by file and symbol. Items marked `(inferred)` were not confirmed in code. The text does not follow Simplified Technical English. See [../README.md](../README.md) for the synthesis.

# Non-tabular catalogs: fileset, messaging (Kafka), model registry, Lance (ref prefix FS)

All paths are repo-relative to `/home/user/apache/gravitino` (commit 9aa78cf). Schema version in force: `scripts/postgresql/schema-2.0.0-postgresql.sql`. Line numbers were opened and checked; items marked `(inferred)` were not run.

## 1. Decomposition

### 1.1 Components

```
api/.../file/{Fileset,FilesetCatalog,FilesetChange}.java     API types: Fileset.Type {MANAGED, EXTERNAL}, named locations
api/.../messaging/{Topic,TopicCatalog,TopicChange,DataLayout}.java   DataLayout is an empty @Unstable interface
api/.../model/{Model,ModelCatalog,ModelVersion,ModelVersionChange}.java
catalogs/catalog-fileset/   FilesetCatalogOperations (2068 lines): create/alter/drop, location math, FS cache, credential context
catalogs/hadoop-common/     FileSystemProvider SPI (ServiceLoader), HDFS/local providers, config bypass, credential-provider SPI
catalogs/hadoop-auth/       Kerberos login for HDFS (not analysed in depth)
bundles/{aws,gcp,aliyun,azure,tencent}/   per-scheme providers: s3a, gs, oss, abfss, cosn  (+ token generators = credential vending)
catalogs/catalog-kafka/     KafkaCatalogOperations: live AdminClient; topics are NOT rows in the Gravitino store until first load
catalogs/catalog-model/     ModelCatalogOperations: models + versions + aliases + named URIs; all in the entity store
catalogs/catalog-lakehouse-generic/.../lance/LanceTableOperations.java   Lance dataset = a Gravitino TABLE with format=lance
lance/lance-common/         GravitinoLanceTableOperations / NamespaceOperations (REST-to-Gravitino mapping), storage-option resolution
lance/lance-rest-server/    Jersey resources implementing the Lance namespace REST spec (v1/table/{id}/..., v1/namespace/{id}/...)
clients/filesystem-hadoop3/ GVFS (Hadoop FileSystem for gvfs://fileset/<catalog>/<schema>/<fileset>/<subpath>)
clients/client-python/gravitino/filesystem/   GVFS for fsspec; same design, separate implementation
core/.../catalog/{Fileset,Topic,Model}OperationDispatcher.java   server-side glue: locks, secrets, import-on-load for topics
core/.../storage/relational/service/{FilesetMetaService,ModelVersionMetaService}.java   versions, OCC, retention
server/.../rest/{FilesetOperations,MetadataObjectCredentialOperations}.java   REST, authorization expressions, credential endpoint
```

### 1.2 Data flow for a fileset path (the closest thing to "resolve a name to a replica")

1. Client path `gvfs://fileset/{catalog}/{schema}/{fileset}[/sub]`. Parsed by regex `^(?:gvfs://fileset)?/([^/]+)/([^/]+)/([^/]+)(?>/[^/]+)*/?$` (`clients/filesystem-hadoop3/.../GravitinoVirtualFileSystemUtils.java:55`). The metalake comes from client config, not the path.
2. For EVERY filesystem call the Java client asks the server `GET .../filesets/{f}/location?sub_path=..&location_name=..` (`BaseGVFSOperations.getActualFilePath`, `DefaultGVFSOperations.java` each method). The server loads the fileset, picks a location, appends the sub-path, validates, returns a raw string (`FilesetCatalogOperations.getFileLocation`, :1840-1913).
3. The client separately loads fileset + schema + catalog, merges properties (catalog < schema < fileset < client conf < `fs.path.config.*`), optionally fetches vended credentials, then builds or reuses a Hadoop `FileSystem` cached by (scheme, authority, UGI) (`BaseGVFSOperations.getActualFileSystemByLocationName`, :816-870; cache :960).
4. Data bytes go client to storage. The server never reads the bytes. It does list, mkdir and recursive delete itself (`listFiles` :404, create :611, drop :818) with catalog-level credentials.

### 1.3 Extension points and config keys that matter

- `FileSystemProvider` SPI via `ServiceLoader` (`catalogs/hadoop-common/.../FileSystemProvider.java`); duplicate scheme throws (`FileSystemUtils.java` loader). Catalog property `filesystem-providers` is declared but never read (see 4.3).
- `SupportsCredentialVending`, `GravitinoFileSystemCredentialsProvider`, `SupportsPathBasedCredentials` (`getPathContext`, `FilesetCatalogOperations.java:1970`).
- Client: `fs.gravitino.operations.class`, `fs.gravitino.hook.class` (pre/post hooks per op, `GravitinoVirtualFileSystemHook.java`), `fs.gravitino.current.location.name` / env `CURRENT_LOCATION_NAME`, `fs.gravitino.enableCredentialVending`, `fs.gravitino.autoCreateLocation`, `fs.gravitino.filesetMetadataCache.enable` (default false, `GravitinoVirtualFileSystemConfiguration.java:226-230`), `fs.path.config.<name>[.<key>]`.
- Catalog: `location`, `location-<name>`, `disable-filesystem-ops` (immutable), `default-filesystem-provider`, `filesystem-conn-timeout-secs` (default 6). Fileset: `placeholder-<x>`, `default-location-name` (immutable), `allow-existing-location-as-managed` (immutable).
- Lance catalog (generic lakehouse): `lance.allow-external-table-purge`, `lance.schema-refresh-mode`, `lance.storage.*`.

### 1.4 Storage shape (PostgreSQL schema 2.0.0)

- `fileset_meta(fileset_id, fileset_name VARCHAR(128), type, current_version, last_version, occ_version, deleted_at, UNIQUE(schema_id, fileset_name, deleted_at))`.
- `fileset_version_info(fileset_id, version, storage_location_name VARCHAR(256) DEFAULT 'default', storage_location TEXT, properties TEXT, deleted_at, UNIQUE(fileset_id, version, storage_location_name, deleted_at))`: one row per location per version. Versions are not exposed in the API and are pruned by `deleteFilesetVersionsByRetentionCount` (`FilesetMetaService.java:309`).
- `model_meta(... model_latest_version INT, ...)`, `model_version_info(model_id, version, model_version_uri_name VARCHAR(256), model_version_uri TEXT, UNIQUE(model_id, version, uri_name, deleted_at))`, `model_version_alias_rel(model_id, model_version, alias VARCHAR(128), UNIQUE(model_id, alias, deleted_at))`.
- `topic_meta(...)`: comment, properties, audit only. Partition counts and configs are read live from Kafka.
- All soft delete with `deleted_at BIGINT DEFAULT 0`; background purge by "legacy timeline".

## 2. Candidate requirements

Kind: IMPL = Gravitino does it; GAP = Gravitino fails or omits it, mudcat should do better. Ver: T test, A analysis, I inspection, D demonstration.

### 2.1 Ownership of bytes, deletion

| Ref | Kind | Requirement | Rationale / Gravitino evidence | Area | Ver |
|---|---|---|---|---|---|
| FS-R1 | IMPL | mudcat shall record, for each registered data location, whether mudcat owns the stored bytes. | `Fileset.Type` MANAGED vs EXTERNAL: "data will be deleted when the fileset object is deleted" vs "will not" (`api/.../file/Fileset.java` enum Type). Lance registered tables forced external (`LancePropertiesUtils.java:isExternalTable`). | UP | I |
| FS-R2 | IMPL | mudcat shall default the ownership flag of a newly registered location to "not owned". | Lance REST create, declare and register all set `PROPERTY_EXTERNAL=true` (`GravitinoLanceTableOperations.java:192,224,251`); docs "Tables created, declared or registered through Lance REST are external". | UP | T |
| FS-R3 | IMPL | mudcat shall provide an unregister operation that removes the record and never touches the bytes. | Lance `deregister` vs `drop`; deregister refuses managed tables rather than risk data loss (`GravitinoLanceTableOperations.java:deregisterTable`, comment "violating the deregister contract"). | UP | T |
| FS-R4 | IMPL | mudcat shall refuse to purge bytes of a not-owned location unless an administrator of the storage resource has enabled purge for it. | Purge of external Lance data needs catalog property `lance.allow-external-table-purge`, never a table property (`LanceTableOperations.java:310-318`; docs `lance-rest-service.md:138-146`). Added after registered datasets could be deleted by whoever registered the path. | UP | T |
| FS-R5 | GAP | mudcat shall require a privilege separate from data write to change the ownership flag of a registered location. | `updateType` flips MANAGED/EXTERNAL; docs: a caller with `WRITE_FILESET` "can convert in either direction, including changing whether a subsequent drop deletes the data" (`docs/manage-fileset-metadata-using-gravitino.md` "Change the Fileset Type"). `WRITE_FILESET` = "Read, write, and alter" (`docs/security/access-control.md:204`). | AC | T |
| FS-R6 | GAP | mudcat shall state in every drop response whether bytes were removed. | With `disable-filesystem-ops=true` a MANAGED drop silently keeps bytes ("even after conversion to MANAGED", same doc); test `testDropSchemaCleansSecretsWithoutDeletingStorage` pins both outcomes (`TestFilesetCatalogOperations.java:1300-1333`). Same API, two meanings. | UP | T |
| FS-R7 | GAP | mudcat shall persist a deletion intent for the bytes before deleting them, and shall keep the record until deletion is confirmed. | Gravitino deletes storage inside the metadata transaction, so a storage failure rolls the record back, but earlier locations are already gone ("The locations removed up to that point are gone for good", `FilesetCatalogOperations.java:818-845`); no recovery for the "NOT atomic" Lance window (`LanceTableOperations.java:322-325,362-365`: "There is currently no recovery mechanism"). Fits "record outlives the bytes". | UP | T |
| FS-R8 | GAP | mudcat shall not hold a database transaction open while it deletes stored bytes. | "recursive storage delete runs inside that transaction, holding the fileset rows and a pooled connection ... enough concurrent drops can hold up the connection pool" (`FilesetCatalogOperations.java:1095-1104`). | PF | A |
| FS-R9 | IMPL | mudcat shall refuse to register a new owned location that already holds data, unless the caller sets an explicit override. | `allow-existing-location-as-managed`, default false; message tells the caller to register it EXTERNAL (`FilesetCatalogOperations.java:563,592-607`). Emptiness test is `listStatusIterator().hasNext()`. | UP | T |
| FS-R10 | GAP | mudcat shall reject an owned location that equals, contains or lies inside the owned location of another record. | Only fileset-parent-of-schema-location is checked (`validateLocationHierarchy`, :1128-1153). Docs: conversion "does not protect data from deletion through another managed fileset with an overlapping location" (`manage-fileset-metadata-using-gravitino.md`). Test `testCreateManagedFilesetWithNestedNewLocations` shows nested locations inside one fileset are accepted. | UP | T |
| FS-R11 | IMPL | mudcat shall validate every location of a request before it creates any directory. | "Formalize and validate every location before creating any directories" (:569-570); test `testCreateManagedFilesetPreflightsAllLocations` asserts no earlier location is created on rejection. | OP | T |
| FS-R12 | GAP | mudcat shall tell the caller of a purge or drop which locations were removed when it fails part-way. | Drop stops at first failure and throws a plain IOException; earlier deletions are not reported (`deleteManagedFilesetStorage` :818-845). | UP | T |

### 2.2 Locations, templates, validation

| Ref | Kind | Requirement | Rationale / Gravitino evidence | Area | Ver |
|---|---|---|---|---|---|
| FS-R13 | IMPL | mudcat shall let one data object carry several named locations, with exactly one marked as default when there is more than one. | `createMultipleLocationFileset`; `setDefaultLocationIfAbsent` (:696-736) rejects a missing or unknown default for >1 locations. Model versions carry named URIs with `default-uri-name` (`ModelCatalogOperations.java:internalGetModelVersionUri`). Docs: "a copy per region or per storage system". | PL | T |
| FS-R14 | IMPL | mudcat shall resolve a location template once, at registration, and store the resolved location. | Placeholders `{{catalog}} {{schema}} {{fileset}} {{custom}}` replaced at create (`calculateFilesetPath` :1397-1438), result stored in `FilesetEntity.storageLocations`. Later catalog or schema location edits do not move existing filesets. `storage locations cannot be changed after creation` (docs). | ST | T |
| FS-R15 | IMPL | mudcat shall reject a location template that contains an empty or unresolvable placeholder. | `checkPlaceholderValue` (:1348-1361) and `replacePlaceholders` "No value found for placeholder: " (:1452). | ST | T |
| FS-R16 | IMPL | mudcat shall resolve location precedence in a fixed order: explicit location, then template from the container, then container path plus name. | Four documented cases in code (:1403-1437); explicit location wins. | ST | T |
| FS-R17 | IMPL | mudcat shall reject a sub-path that contains a parent-directory segment or a NUL character before any storage call. | `getFileLocation` :1843-1848; test `testListFilesCannotReachSiblingSentinel` asserts no filesystem lookup happens (`TestFilesetCatalogOperations.java:3343-3373`). | RD | T |
| FS-R18 | IMPL | mudcat shall decide that a resolved path lies inside a location by comparing parsed path ancestry, not string prefixes. | `isWithinStorageLocation` (:1923-1930); comment ":1876 Compare normalized Path ancestors ... rather than string prefixes". Sibling `root-sibling` test. | RD | T |
| FS-R19 | IMPL | mudcat shall treat percent signs, plus signs, non-ASCII characters and backslashes in a sub-path as literal characters of a name. | Tests `testListFilesPreservesLiteralEncodedNames` (`%2e%2e`) and `testListFilesPreservesLiteralBackslashes` (`TestFilesetCatalogOperations.java:3376-3420`). | RD | T |
| FS-R20 | IMPL | mudcat shall refuse to rename the root of a registered location through a data operation. | `RENAME` on `""`, `/`, `/.`, `//` rejected (:1898-1903); test `testRenameCannotTargetRootAliases`. Rename across two objects is also rejected (`DefaultGVFSOperations.java:125-133`). | RD | T |
| FS-R21 | IMPL | mudcat shall reject object names that contain a path separator when the name appears in a virtual path. | `FilesetCatalogCapability.specificationOnName` rejects `/` (`FilesetCatalogCapability.java:38-43`). | ST | T |
| FS-R22 | IMPL | mudcat shall require a URI scheme on every registered location. | When server filesystem operations are disabled the scheme must be present (:547-557); GVFS refuses a scheme-less actual path (`BaseGVFSOperations.java:947`). Otherwise a catalog default provider supplies a scheme, which makes the stored value depend on configuration. | ST | T |
| FS-R23 | GAP | mudcat shall store each location in one canonical form that does not depend on catalog configuration. | With filesystem operations enabled the path is `makeQualified` against the live filesystem (`:576-577`); with them disabled the user's spelling is stored (`:559`). Trailing slash is added on catalog and schema locations (`ensureTrailingSlash` :1281) then stripped by Hadoop `Path`. | ST | T |
| FS-R24 | GAP | mudcat shall publish the list of location schemes it resolves, and every reference client shall accept the same list. | Java providers: file, hdfs, s3a, gs, oss, cosn, abfss (`bundles/*/.../*FileSystemProvider.java`). Python handlers: file, hdfs, s3a, gs, oss, abfss, no cosn (`gvfs_storage_handler.py:755-762`). `s3://` and `s3n://` resolve in neither. Error text: "Unsupported scheme: %s ... all supported schemes" (:1524). | IN | T |
| FS-R25 | GAP | mudcat shall apply one rule for choosing the default location in the server and in every client. | Server `getFileLocation`: one location means that one; else property (:1855-1861). Server credentials: header, else single, else property (:1999-2027). Java client: property only, no single-location fallback (`BaseGVFSOperations.java:840`). Python: property, else `unknown` (`gvfs_base_operations.py:679`). Legacy filesets have no `default-location-name` and no migration adds it (no match in `scripts/`). | PL | T |
| FS-R26 | IMPL | mudcat shall let a caller name the location (replica) to resolve and shall use the default when none is named. | `location_name` query parameter and `fs.gravitino.current.location.name` / env var (`how-to-use-gvfs.md:68-69,193-196`). The choice is the client's, not computed from client position. | PL | T |

### 2.3 Resolution, credentials, caching

| Ref | Kind | Requirement | Rationale / Gravitino evidence | Area | Ver |
|---|---|---|---|---|---|
| FS-R27 | IMPL | mudcat shall issue storage credentials scoped to the path of the selected location, with read scope or write scope decided by the caller's privilege. | `PathBasedCredentialContext(user, writePaths, readPaths)` (`CredentialOperationDispatcher.getPathBasedCredentialContexts` :141-157); privilege chosen by `FILTER_WRITE_FILESET_AUTHORIZATION_EXPRESSION` (`MetadataObjectCredentialOperations.java:123-132`). | RD | T |
| FS-R28 | GAP | mudcat shall scope a credential to the object and operation requested, not to a whole storage resource. | Lance returns the catalog's own static keys in `storage_options` for any describe: "shared configured credentials, not temporary credentials restricted to the caller's table privileges" (`lance-rest-service.md:436-440`; `GravitinoLanceTableOperations.java:151`, `GravitinoLanceNamespaceWrapper.java:134-159`). | RD | T |
| FS-R29 | GAP | mudcat shall scope a credential below the location root when the request names a sub-path. | Vended credential covers the whole fileset location; `getPathContext` uses `getTargetLocation` only (:1970-1988). | RD | T |
| FS-R30 | GAP | mudcat reference clients shall not reuse a storage session obtained under one grant for another object's grant. | Java FS cache key = (scheme, authority, UGI), not credentials or fileset (`BaseGVFSOperations.java:178-225,960`); test `testCacheReuseAcrossMultipleFilesets` asserts the same instance is shared for two filesets. Credentials for fileset A are baked in at creation (:1239). Python includes credentials in the key (`gvfs_base_operations.py:99-160`). Server-side cache has the same key shape (`FilesetCatalogOperations.java:221-261`; NOTE ":226 Changes to the configuration of the fileset will not affect the cached filesystem"). | RD | T |
| FS-R31 | IMPL | mudcat clients shall refresh a credential before its stated expiry, using a configurable fraction of its remaining life. | Python `_calculate_credential_expire_time` = now + remaining x ratio (`gvfs_base_operations.py:781-802`); Java uses a provider that re-fetches (`DefaultGravitinoFileSystemCredentialsProvider.java`). | RD | T |
| FS-R32 | GAP | mudcat shall not give the catalog server a credential that can delete data in every location of every tenant. | Server-side list, mkdir and recursive delete run with catalog-level credentials and a shared per-user FS cache (`FilesetCatalogOperations.java:818,1479-1495`). | AC | A |
| FS-R33 | GAP | mudcat shall let a client resolve a name once and reuse the result for a period stated in the response, naming the commit it was resolved at. | Java GVFS makes one REST call per filesystem call; metadata cache is off by default and has no expiry, only size bounds (`FilesetMetadataCache.java:131-160`). `OperationScope` dedups loads within one call only (`BaseGVFSOperations.java:617-643`). | PF | T |
| FS-R34 | GAP | mudcat shall derive access audit for data from credential issue and storage-side logs, not from client self-reported operation names. | The data operation is a client-supplied header (`BaseGVFSOperations.setCallerContextForGetFileLocation` :925-934) and the server only acts on `RENAME` (:1885-1909). `getFileLocation` needs only the load-fileset expression, i.e. any read or write grant (`FilesetOperations.java:376`). | EV | I |
| FS-R35 | GAP | mudcat shall return a typed error that distinguishes "object missing" from "not permitted" from "location name unknown" on every data operation. | Java GVFS maps NoSuchCatalog/Fileset/LocationName all to `FileNotFoundException`; `delete` then returns `false`, and `mkdirs` rewraps as IOException (`BaseGVFSOperations.java:583-606`, `DefaultGVFSOperations.java:99,153,211`). | IN | T |
| FS-R36 | GAP | mudcat shall decode request parameters the same way for every client version. | Server decodes `sub_path` and `location_name` again only for clients older than major version 1 (`FilesetOperations.java:399-405`). | IN | T |

### 2.4 Models, versions, aliases

| Ref | Kind | Requirement | Rationale / Gravitino evidence | Area | Ver |
|---|---|---|---|---|---|
| FS-R37 | IMPL | mudcat shall number versions of an object in sequence and never reuse a number after deletion. | `model_latest_version` incremented in the same statement that bumps the OCC counter (`ModelMetaBaseSQLProvider.java:316-325`); `INIT_VERSION=0` (`ModelCatalogOperations.java:66`). Test `testConcurrentLinkModelVersionAllocatesDistinctVersions`. | ID | T |
| FS-R38 | IMPL | mudcat shall reject deletion or alteration of an object when a child version changed after the caller read it. | `ModelCatalog` javadoc: "deleteModel and alterModel fail this way when a version of the model was linked, altered or deleted after the model was read, so that a drop never removes a version it did not observe" (`api/.../model/ModelCatalog.java:38-48`). | ID | T |
| FS-R39 | GAP | mudcat shall move a ref name from one version to another in one atomic commit. | Docs "link a new version, move the alias" (`docs/model-catalog.md:46-53`). Code: alias unique per model; an add on version B while A holds it fails; the move is remove-from-A then add-to-B in two calls (`ModelVersionMetaService.java:349-364`, UNIQUE at schema line 680). `updateAliases` with the same name in add and remove cancels out (`ModelCatalogOperations.java:494-497`; test `testUpdateModelVersionAliasesOverlapAddAndRemove`). (inferred for the two-call window) | ID | T |
| FS-R40 | GAP | mudcat shall keep ref names and version identifiers in disjoint syntaxes and enforce that on every operation that creates a ref. | Link rejects numeric aliases via `NumberUtils.isCreatable` (`ModelVersionLinkRequest.java:92`); the update-aliases request only checks non-null (`ModelVersionUpdateRequest.java:279-285`) and the entity checks length only (`ModelVersionEntity.java:143-147`). `isCreatable` also accepts `0x1F`, `1e3`, `1L` (`ModelVersionMetaService.java:91,158,253,549`). | ID | T |
| FS-R41 | GAP | mudcat shall never change the artifact digest or structure of a published version. | Docs: "A new release is a new version rather than an edit of an existing one" (`model-catalog.md:42-44`), yet `UpdateUri`, `AddUri`, `RemoveUri` rewrite a version's URIs (`ModelCatalogOperations.java:632-642`) and the update path soft-deletes and re-inserts the full URI set (`ModelVersionMetaService.java:316-330`). No digest is stored anywhere. | ID | T |
| FS-R42 | GAP | mudcat shall store a content digest for each registered artifact and verify it at registration. | `model_version_uri TEXT`; `linkModelVersion` checks only "not blank" (`ModelVersionLinkRequest.java:96-107`). No scheme check, no existence check, no digest. | ID | T |
| FS-R43 | IMPL | mudcat shall reject a version whose location set becomes empty. | "Model version URI cannot be empty" (`ModelCatalogOperations.java:507`). | ST | T |
| FS-R44 | GAP | mudcat shall report a change that did nothing as an error. | `doUpdateUri` uses `Map.replace` and `doAddUri` uses `putIfAbsent`, so updating an unknown URI name or adding an existing one is silently a no-op but still bumps audit time (`ModelCatalogOperations.java:632,636`). | ST | T |

### 2.5 Live-system objects (Kafka), external formats (Lance)

| Ref | Kind | Requirement | Rationale / Gravitino evidence | Area | Ver |
|---|---|---|---|---|---|
| FS-R45 | IMPL | mudcat shall read the existence and structure of a live-system object from that system and store only mudcat-owned metadata. | Kafka `loadTopic` reads partitions, replication and configs through AdminClient; only comment and audit live in `topic_meta` (`KafkaCatalogOperations.java:loadTopic`; dispatcher `importTopic` `TopicOperationDispatcher.java:294-352`). | ST | D |
| FS-R46 | IMPL | mudcat shall assign a catalog identifier to an externally created object on first observation. | "import on load": `loadTopic` of an object not in the store writes a `TopicEntity` (`TopicOperationDispatcher.java:107-130`). | ID | T |
| FS-R47 | IMPL | mudcat shall reject registration of an external identifier that is already registered under another catalog identifier. | `retainedExternalIdentifierConflictException`: "does not automatically reconcile conflicting external registrations, and it does not support metadata-only unregister" (`OperationDispatcher.java:101-130`). | ID | T |
| FS-R48 | GAP | mudcat shall not derive its identifiers by truncating an external identifier. | `StringIdentifier.fromId(topicId.getLeastSignificantBits() & MAX_ID)` (`KafkaCatalogOperations.java:581-583`), "no way to store the Gravitino ID in Kafka". 128-bit UUID reduced to a masked 64-bit long. | ID | A |
| FS-R49 | GAP | mudcat shall not present the time of observation as the creation time of an external object. | `loadTopic` and `alterTopic` build `AuditInfo` with `creator=current principal`, `createTime=Instant.now()` (`KafkaCatalogOperations.java:244-252,365-375`); only the first import freezes it. | PR | T |
| FS-R50 | GAP | mudcat shall distinguish removing the catalog record of a live object from deleting the live object. | `dropTopic` always calls `deleteTopics` on the broker (`KafkaCatalogOperations.java:386`); no external or unregister mode, unlike fileset EXTERNAL and Lance deregister. | UP | T |
| FS-R51 | IMPL | mudcat shall refuse changes that would destroy information in a live object (partition count may only grow). | "Cannot reduce partition count" (`KafkaCatalogOperations.java:504-508`); replication factor immutable with TODO (`KafkaTopicPropertiesMetadata.java:44`). | ST | T |
| FS-R52 | IMPL | mudcat shall register an existing external dataset only after it verifies that the dataset is readable and of the declared format. | `validateExistingDatasetLocation` opens the dataset before any overwrite (`LanceTableOperations.java:1042-1098`, call at :250-256); docs "A plain directory or missing dataset is rejected before any existing table is removed". | ST | T |
| FS-R53 | IMPL | mudcat shall keep three distinct entry points: register an existing dataset, create an empty dataset, declare a record with no dataset. | `RegisterTable`, `CreateTable`, `DeclareTable` (`lance-rest-service.md:62-77`; `lance.register`, `lance.declared` flags `LanceConstants.java`). | ST | D |
| FS-R54 | GAP | mudcat shall reject a request for a version, branch or tag it cannot honour, with the same error for all three. | Branch and tag: rejected with a message (`LanceTableOperations.java:410-417`; comment explains "ignoring it would silently apply a mutation meant for a branch to the table itself"). `version`: accepted, logged as a warning, latest returned (`GravitinoLanceTableOperations.java:109-120`; doc admits "latest metadata even when a historical version is requested"). | IN | T |
| FS-R55 | GAP | mudcat shall record, with every schema or structure snapshot taken from an external format, the format version it was taken at, and shall mark the snapshot stale when the version differs. | `lance.version` is stored; default `DECLARED_AND_EMPTY` mode does not compare it, only `VERSION_CHECK` does (`LanceTableOperations.java:126-150,282-326`). Register "does not read the schema from the underlying Lance dataset" (:434). `describe` hard-codes `managedVersioning=false` (`GravitinoLanceTableOperations.java:152`). | ST | T |
| FS-R56 | GAP | mudcat shall not change catalog state while it serves a read request. | `loadTable` opens the dataset and writes repaired columns with a 5-attempt CAS retry and random 10-100 ms back-off (`LanceTableOperations.java:96-103,497-545`). | OP | T |
| FS-R57 | GAP | mudcat shall limit registration to locations that the caller's tenant may register. | Docs: registration "does not verify storage ownership"; purge opt-in "permits deletion at caller-supplied locations" (`lance-rest-service.md:144-146`; `lakehouse-generic-lance-table.md:189`). Any caller with CREATE_TABLE can point a table at any readable path. | AC | T |
| FS-R58 | IMPL | mudcat shall not let object-level properties override the endpoint or credentials of the storage resource. | `resolveLanceStorageOptions`: "A key set in the catalog properties keeps the catalog value; table properties can only add keys" because otherwise "requests signed with the catalog credentials" go to a creator-chosen endpoint (`LancePropertiesUtils.java:104-120`). | AC | T |
| FS-R59 | GAP | mudcat shall return the same authorization decision for the same request on every deployment profile of one interface. | Lance standalone does not forward identity and has no per-user authorization; docs warn results "can differ from auxiliary mode" and cite issue #13089 (`lance-rest-service.md:344-368`). | AC | T |
| FS-R60 | IMPL | mudcat shall serve the Lance namespace REST interface over registered Lance datasets, with a fixed mapping from namespace levels to catalog hierarchy. | Root = metalake, 1 level = catalog, 2 = schema, 3 = table; delimiter `$` (`NamespaceWrapper.java:26`; docs). Catalog of provider `lakehouse-generic` is auto-created by CreateNamespace (`GravitinoLanceNameSpaceOperations.java:247-265`). | IN | D |
| FS-R61 | GAP | mudcat shall carry file-like and array-like objects in a model that does not require column lists. | Lance datasets, filesets and models live in table-, schema- or fileset-shaped entities; Arrow schemas are translated to Gravitino column types, with an `ExternalType` escape for unknown ones (`LanceDataTypeConverter.java:239-250`; `LanceBlobTypes.java`). | ST | I |
| FS-R62 | IMPL | mudcat shall pass through, unchanged, a field of an external format that it cannot map to its own types. | `Types.ExternalType` and "A blob field in any other layout is left to the Arrow JSON representation so that it round-trips unchanged" (`LanceBlobTypes.java` header, `LanceDataTypeConverter.java:239-243`). | ST | T |

## 3. Quirks and gotchas ledger

- **Q1. Drop means different things depending on a flag and a type.**
  - A MANAGED fileset drop recursively deletes the directories; an EXTERNAL drop does not; with `disable-filesystem-ops=true` nothing is deleted even for MANAGED. The caller cannot tell from the response.
  - Cause: MODEL / OTHER.
  - Evidence: `FilesetCatalogOperations.java:1110` `if (!disableFSOps && filesetEntity.filesetType() == Fileset.Type.MANAGED)`; docs "With disable-filesystem-ops=true, dropping a fileset removes only its metadata, even after conversion to MANAGED".
  - Mudcat: FS-R6, FS-R1.

- **Q2. A metadata edit can arm data deletion.**
  - `updateType(MANAGED)` changes nothing on storage but makes the next drop delete the data. Needs only `WRITE_FILESET`. The same applies to `dropSchema(cascade)`.
  - Cause: SECURITY / MODEL.
  - Evidence: `FilesetChange.java:55-66` "Changing to MANAGED accepts management of the data, including deleting it when the fileset is dropped"; `access-control.md:204`.
  - Mudcat: FS-R5.

- **Q3. Two filesets may overlap and one drop can delete the other's data.**
  - Locations are free strings. Only "fileset location is ancestor of schema location" is rejected. A MANAGED fileset registered on a path another fileset uses deletes it on drop. Registration of a non-empty path as MANAGED is blocked by default (a later hardening), so the path of danger is an empty path that is filled later, or `allow-existing-location-as-managed=true`.
  - Cause: MODEL.
  - Evidence: `FilesetCatalogOperations.java:1128-1153`; docs "does not protect data from deletion through another managed fileset with an overlapping location".
  - Mudcat: FS-R10, FS-R9.

- **Q4. Storage delete runs inside the metadata transaction.**
  - A fileset drop with a big tree holds DB rows and a connection for the length of the recursive delete. Failure rolls the record back but earlier locations of a multi-location fileset are already deleted.
  - Cause: CONCURRENCY / STORE.
  - Evidence: `FilesetCatalogOperations.java:1101-1104` "enough concurrent drops can hold up the connection pool"; :818-821.
  - Mudcat: FS-R7, FS-R8, FS-R12.

- **Q5. REST-created Lance tables cannot be dropped by default.**
  - Create, declare and register always set `external=true`; `drop` calls `purgeTable`; purge of an external table throws unless the catalog opts in. So `DropTable` on a table you just created answers HTTP 406. The documented escape is `deregister`.
  - Cause: COMPAT (hardening after data-loss risk) / SECURITY.
  - Evidence: `GravitinoLanceTableOperations.java:192` `createTableProperties.put(Table.PROPERTY_EXTERNAL, "true")`; `:purgeTable(tableIdentifier)` in dropTable; `LanceTableOperations.java:316`; docs "Tables created, declared or registered through Lance REST are external, so this default also applies to them."
  - Mudcat: FS-R2, FS-R3, FS-R4.

- **Q6. Lance REST hands out the catalog's own storage keys.**
  - Describe, create and declare responses put `storage_options` built from catalog properties plus secrets (static credential keys included) into the body for any caller who can describe the table. Per-user scoped vending "is not implemented".
  - Cause: SECURITY.
  - Evidence: `GravitinoLanceTableOperations.java:149-151`; `GravitinoLanceNamespaceWrapper.java:147-154` "Lance does not use catalog getCredentials / credential vending"; `lance-rest-service.md:436-440`.
  - Mudcat: FS-R28, FS-R27.

- **Q7. Java GVFS shares one FileSystem across filesets in the same bucket, including its credentials.**
  - Cache key is scheme + authority + UGI. A path-scoped credential fetched for fileset A is baked into the FileSystem that fileset B (same authority) then reuses. Python avoids this by hashing credentials into the key. Behaviour pinned by a test that asserts reuse.
  - Cause: SECURITY / ENGINE (Hadoop FileSystem cache semantics).
  - Evidence: `BaseGVFSOperations.java:960`; `TestGvfsBase.java:439-490` "Both should be the same instance since they share scheme/authority/user"; `gvfs_base_operations.py:99-160`. Effect on scoped credentials is (inferred).
  - Mudcat: FS-R30.

- **Q8. Default location rule is implemented four times, differently.**
  - Server `getFileLocation` falls back to the single location; server credential path does the same; Java client requires the `default-location-name` property; Python falls back to `unknown`. Legacy filesets have no property and no migration writes it.
  - Cause: COMPAT.
  - Evidence: `FilesetCatalogOperations.java:1855-1861` and `:2010-2016` ("mainly used for backward compatibility"); `BaseGVFSOperations.java:840`; `gvfs_base_operations.py:679`; `DefaultGravitinoFileSystemCredentialsProvider.java:getTargetLocation`. Java failure for a legacy fileset is (inferred).
  - Mudcat: FS-R25.

- **Q9. "unknown" is a reserved location name; the schema default is "default".**
  - Pre-0.9 filesets had one unnamed location. The upgrade added `storage_location_name DEFAULT 'unknown'` then dropped the default. The 2.0 schema re-adds `DEFAULT 'default'`, which code never uses. The reserved name `unknown` is a magic value that leaks into API ("unnamed location") and into client config rules.
  - Cause: COMPAT.
  - Evidence: `scripts/postgresql/upgrade-0.8.0-to-0.9.0-postgresql.sql:21`; `schema-2.0.0-postgresql.sql:229`; `Fileset.java` `LOCATION_NAME_UNKNOWN = "unknown"`; same for `ModelVersion.URI_NAME_UNKNOWN`.
  - Mudcat: FS-R13 (give the default location a real name from the start).

- **Q10. Sub-path hygiene lives in one function, and the tests show how much it took.**
  - Checks: NUL, any `..` segment (before Hadoop `Path` normalises it away), ancestry on normalised paths including scheme and authority, rename of root aliases (`/`, `/.`, `//`). Original spelling is returned to the client (`file:///storage/fileset/dir/./file`).
  - Cause: SECURITY.
  - Evidence: `FilesetCatalogOperations.java:1843-1882`; comment ":1843 Validate before constructing a Hadoop Path, which normalizes away parent directory segments"; tests `TestFilesetCatalogOperations.java:3300-3420`.
  - Mudcat: FS-R17, FS-R18, FS-R19, FS-R20.

- **Q11. Client strips the location prefix by string length.**
  - `getFileStatus`/`listStatus` compute the fileset root as `actualPath.substring(0, len - subPath.length())`, then `replaceFirst` the prefix. The server returns the raw spelling but the client re-parses it as a Hadoop `Path`, which drops a trailing slash, so a sub-path with a trailing slash would be off by one. `(inferred)`
  - Cause: MODEL / ENGINE.
  - Evidence: `DefaultGVFSOperations.java:169-172,189-192`; `BaseGVFSOperations.convertFileStatusPathPrefix` :713-735.
  - Mudcat: FS-R23, FS-R18. Resolve to a structured (location, relative path) pair, not a concatenated string.

- **Q12. Object stores disagree about trailing slashes on directories.**
  - OSS `info()` returns directory names with a trailing slash, S3 without; Python tests carry the workaround comment. Hadoop 3.4 changed the S3A marker default from "delete" to "keep"; Gravitino pins it back to `delete` because older clients take a kept marker for an empty directory.
  - Cause: ENGINE.
  - Evidence: `clients/client-python/tests/integration/test_gvfs_with_oss.py:365-376`; `catalogs/hadoop-common/.../Constants.java` `S3_DIRECTORY_MARKER_RETENTION` comment.
  - Mudcat: directory emptiness and listing must not be part of the catalog contract (FS-R9 uses it only as a guard).

- **Q13. Config for a location is picked by first match in a HashMap.**
  - `fs.path.config.<name>=<scheme://authority>` is matched to a path by base location; the loop `break`s on the first hit, so two names for the same base location give an order-dependent result. Location names in this config cannot contain `.` or `_` (docs), but fileset location names can contain anything up to 256 characters. The word "location name" means two different things in the two places.
  - Cause: OTHER.
  - Evidence: `FilesetUtil.java:getUserDefinedFileSystemConfigs` (loop with `break`; `!suffix.contains(".")`); `how-to-use-gvfs.md:78`; server name check only blank + length (`FilesetCatalogOperations.java:463-477`).
  - Mudcat: name every concept once; location names should be validated against one grammar (FS-R13).

- **Q14. One REST call per filesystem call.**
  - Every `open`, `create`, `getFileStatus` re-resolves through the server, then separately loads catalog/schema/fileset for properties. `OperationScope` removes duplicate loads inside one call only. Metadata cache default off; when on, it has no expiry.
  - Cause: PERF / MODEL.
  - Evidence: `DefaultGVFSOperations.java:48-57`; `BaseGVFSOperations.java:617-643`; `FilesetMetadataCache.java:131-160`; config default `false`.
  - Mudcat: FS-R33.

- **Q15. Client-reported data operation is audit input and the only server-side input for one rule.**
  - The header `fileset-data-operation` is set by the client; the server uses only `RENAME` for a safety rule; unknown values log a warning and pass. Access control on `getFileLocation` does not distinguish read from write.
  - Cause: SECURITY.
  - Evidence: `FilesetCatalogOperations.java:1885-1909`; `BaseGVFSOperations.java:925-934`; `FilesetDataOperation.java` enum.
  - Mudcat: FS-R34.

- **Q16. Kafka identity is a truncated UUID and "creation time" is now.**
  - Kafka cannot store a Gravitino ID, so the 128-bit topic UUID is masked to a long. `loadTopic` builds creator and create time from the current caller and clock each time; only `importTopic` freezes it in `topic_meta`.
  - Cause: MODEL.
  - Evidence: `KafkaCatalogOperations.java:243-252,581-583`; `TopicOperationDispatcher.java:294-340`.
  - Mudcat: FS-R48, FS-R49.

- **Q17. Dropping a topic always deletes the topic.**
  - There is no external-type flag for topics; `dropTopic` deletes from the broker and returns false only when Kafka says the topic is unknown. The dispatcher then removes the store row regardless of the broker result, for unmanaged catalogs.
  - Cause: MODEL.
  - Evidence: `KafkaCatalogOperations.java:384-396`; `TopicOperationDispatcher.java:255-292` ("In all situations, we try to delete the schema from the store").
  - Mudcat: FS-R50.

- **Q18. Kafka names Kafka accepts, Gravitino rejects.**
  - Gravitino restricts topic names to `[a-zA-Z0-9_-]{1,128}` ("Gravitino authorization does not support dots"). `listTopics` returns whatever the broker lists, so dotted topics appear in lists and fail other operations `(inferred)`. Also `a_b` and `a.b` collide on some brokers.
  - Cause: SPEC / STORE.
  - Evidence: `KafkaCatalogCapability.java:23-58`; `KafkaCatalogOperations.java:listTopics`; `kafka-catalog.md:79-91`.
  - Mudcat: identifier grammar of the catalog must be a superset of any external system that it imports from, or imports must map names explicitly.

- **Q19. Kafka config providers could read the server's environment.**
  - `gravitino.bypass.config.providers*` is rejected because Kafka resolves `${provider:path:key}` through server-side providers. Existing catalogs with them fail until fixed.
  - Cause: SECURITY.
  - Evidence: `KafkaCatalogOperations.java:CONFIG_PROVIDERS_KEY`, `checkNoConfigProviders`; `kafka-catalog.md:30-40`.
  - Mudcat: any "pass-through property" mechanism needs a deny list for features that evaluate expressions.

- **Q20. Model alias syntax is validated in one of two places.**
  - Link rejects numeric aliases with `NumberUtils.isCreatable` (accepts `0x10`, `1e3`, `1L`, `07`). `updateAliases` does not run that check, so a numeric alias can be added later, and then lookup by `"1"` is read as version 1. `(inferred from absence of check)`
  - Cause: MODEL.
  - Evidence: `ModelVersionLinkRequest.java:88-93`; `ModelVersionUpdateRequest.java:279-285`; resolution `ModelVersionMetaService.java:91`.
  - Mudcat: FS-R40.

- **Q21. A moving alias is two writes.**
  - Docs sell "move the alias". The unique key is (model, alias); adding it to B while A has it fails. Remove from A, then add to B leaves a window where the alias resolves to nothing. An add and remove of the same name in one change cancels out.
  - Cause: MODEL / CONCURRENCY.
  - Evidence: `model-catalog.md:46-53`; `schema-2.0.0-postgresql.sql:680`; `ModelCatalogOperations.java:494-497`.
  - Mudcat: FS-R39.

- **Q22. "Immutable release" in docs, mutable URIs in code.**
  - `UpdateUri`/`AddUri`/`RemoveUri` rewrite a released version's locations. The service replaces the whole URI set (soft delete plus insert), so previous values are only in soft-deleted rows. No digest exists to tell the artifact changed.
  - Cause: MODEL.
  - Evidence: `model-catalog.md:42-44`; `ModelVersionMetaService.java:316-330`; `ModelCatalogOperations.java:632-642`.
  - Mudcat: FS-R41, FS-R42.

- **Q23. Concurrency in model versions is solved with a parent-row counter.**
  - Link, alter and delete of a version all lock the schema "fence", then bump the parent model's `current_version` and `model_latest_version`. A conflict surfaces as `OptimisticLockException`; reads can also fail that way and callers are told to retry a bounded number of times.
  - Cause: CONCURRENCY.
  - Evidence: `ModelVersionMetaService.java:480-512`; `ModelCatalog.java:38-48`; tests `testConcurrentLinkModelVersionWithSameAliasHasOneWinner`.
  - Mudcat: one commit pointer per parent gives mudcat's "requirements checked against newest state" cheaply, at the cost of serialising all version writes of a model (FS-R37, FS-R38).

- **Q24. Lance branch/tag refused, version silently ignored.**
  - `describe` with `branch` or `tag` returns 400; with `version` it logs a warning and answers with the latest. A time-travel read returns current data under HTTP 200.
  - Cause: SPEC (lance-namespace 0.11.1 added the fields).
  - Evidence: `LanceTableOperations.java:410-417`; `GravitinoLanceTableOperations.java:109-120`.
  - Mudcat: FS-R54.

- **Q25. Lance schema is a cache that may be stale and may be written on read.**
  - Stored columns are filled lazily from the dataset when a table is declared-only, has no columns, or (opt-in) the dataset version changed. A zero-column dataset is recorded with a "confirmed empty" version marker that suppresses further opens. Concurrent loads race with a 5-attempt CAS retry.
  - Cause: MODEL / CONCURRENCY.
  - Evidence: `LanceTableOperations.java:96-103,282-326,547-596`.
  - Mudcat: FS-R55, FS-R56.

- **Q26. Lance delete is metadata and data in two steps, order depends on type.**
  - Managed: drop metadata first, then delete dataset. External with purge opt-in: same. Crash between the steps leaves an orphan dataset; "There is currently no recovery mechanism". Create has a compensating `Dataset.drop` if metadata insert fails.
  - Cause: CONCURRENCY.
  - Evidence: `LanceTableOperations.java:322-325,362-365,470-483`.
  - Mudcat: FS-R7.

- **Q27. Namespaces in Lance REST are really the Gravitino hierarchy.**
  - Exactly three levels, `$` default delimiter (some handlers hard-code `"$"` instead of the constant). CreateNamespace at level 1 auto-creates a Gravitino catalog with provider `lakehouse-generic`. A schema drop with cascade preserves external datasets. `overwrite` of a namespace is an alter of properties, not a replace.
  - Cause: SPEC / MODEL.
  - Evidence: `GravitinoLanceTableOperations.java:123-124`; `GravitinoLanceNameSpaceOperations.java:257-296`; `LanceTableOperations.java:200,262` (`@DefaultValue("$")`); `lance-rest-service.md:98-106`.
  - Mudcat: FS-R60.

- **Q28. Authorization differs between Lance deployment modes.**
  - Auxiliary mode forwards the caller and applies endpoint rules; standalone uses a service identity and the backend's rules. Default `simple` auth accepts any user name and some malformed Basic headers resolve to anonymous.
  - Cause: SECURITY.
  - Evidence: `lance-rest-service.md:340-368`; issue #13089 cited there.
  - Mudcat: FS-R59.

- **Q29. Python GVFS ABS path test is an enum f-string.**
  - `actual_path.startswith(f"{StorageType.ABS}")` formats the enum as `StorageType.ABS`, not `abfss`, so the guard is always false and the normalisation always runs. `(inferred; not executed)`
  - Cause: OTHER.
  - Evidence: `clients/client-python/gravitino/filesystem/gvfs_storage_handler.py:725`.
  - Mudcat: none; test all scheme branches in both clients.

- **Q30. Credential for the server probe is registered in a static in-memory map.**
  - `testConnection` for a fileset catalog fetches real credentials and stores them in `InMemoryFileSystemCredentialsProvider` keyed by a handle, unregistering in `finally`.
  - Cause: OTHER.
  - Evidence: `FilesetCatalogOperations.java:1690-1748,1685-1687`.
  - Mudcat: connectivity checks must use the same credential path as real access, and must not leave state.

## 4. Open questions, inconsistencies, half-implemented

### 4.1 TODO and "not supported" markers

- `FilesetCatalogOperations.java:179` "TODO: Move the file system cache to hadoop-common and reuse it in BaseGVFSOperations": two caches with different key rules (Q7).
- `FilesetCatalogOperations.java:936` "we need to invalidate the related fileset cache when the schema rename change is supported": schema rename is not supported for filesets.
- `KafkaCatalogOperations.java:448,457` TODO schema alter and drop; `createSchema` throws UnsupportedOperationException ("the default schema already includes all topics"). One schema per cluster, so the namespace level adds nothing.
- `KafkaTopicPropertiesMetadata.java:44` TODO make replication factor mutable.
- `LanceTableOperations.java:308` (REST) TODO only column rename is implemented for alter columns; `Only RENAME alteration is supported currently` (:385-394). Drop columns works.
- Lance index operations, query, insert, versions listing, tags and branches are not in the REST set (`lance-rest-service.md:60`: "Index operations are not supported yet").
- `DataLayout` (api) is an empty `@Unstable` interface; `createTopic` ignores its `dataLayout` argument (`KafkaCatalogOperations.java:createTopic`). Topic schema and serialisation format are not modelled at all.
- `Fileset.storageLocations()` default throws "does not support storageLocations" (`Fileset.java`), i.e. the API had the single `storageLocation()` first and the map was added later (Q9).

### 4.2 Code versus docs

- Docs: model versions are not edited (`model-catalog.md:42-44`). Code: URIs are editable (Q22).
- Docs for `DropTable` say "Permanently deletes metadata and data files when permitted by the catalog policy"; the table at `lance-rest-service.md:62` lists "Delete a table including both metadata and data" without the caveat. REST-created tables are external, so the first form is the real one (Q5).
- Docs: location names in `fs.path.config.<name>` "must not contain . or _"; no server check. `fs.path.config` also uses "name" for a storage-endpoint profile, not a fileset location name (Q13).
- `Fileset.java` Javadoc on `storageLocation()` describes five cases, with "5) null value" for a managed fileset with nothing set. `storageLocations()` Javadoc says case 5 is "illegal", and the code throws (`FilesetCatalogOperations.java:519-523`).
- `DefaultGravitinoFileSystemCredentialsProvider` splits `metalake.catalog.schema.fileset` on `.`; names with dots would break it `(inferred; Gravitino name rules not checked here)`.

### 4.3 Declared but unused

- Catalog properties `fileset-cache-eviction-interval-ms`, `fileset-cache-max-size`, `filesystem-providers` are in `FilesetCatalogPropertiesMetadata.java:54,64-68,173,197,205` and read nowhere in `catalogs/catalog-fileset/src/main` (grep). Providers come from `ServiceLoader` regardless.
- Python GVFS has no `cosn`; Java has.
- `fileset_version_info.storage_location_name DEFAULT 'default'` (schema 2.0.0) is never exercised (Q9).
- Fileset versions exist in the store with retention pruning but have no API; a caller cannot ask what a fileset looked like before.

### 4.4 Questions for the design

1. Should the catalog hold an "owned" flag on the location or on the replica? Gravitino puts it on the whole fileset, so one fileset cannot mix an owned primary and a not-owned mirror. (FS-R1, FS-R13)
2. How does a client choose a location? Gravitino: by client config or environment variable, no server hint. mudcat wants the catalog to resolve names to replicas; what inputs may the choice use (site, tenant, tier)? (FS-R26)
3. Is sub-path authorisation needed? Gravitino has none: one privilege covers the whole location. (FS-R29)
4. What is the unit of a Lance dataset in mudcat: a version with a structure descriptor and a location, as the README says, or a table? Gravitino stores columns and a `lance.version` property on a table; branches and tags are rejected. (FS-R54, FS-R55, FS-R61)
5. Should "import on first observation" (Kafka) run on reads? Gravitino imports on `loadTopic` under a write lock after a read lock, with conflict handling for HA nodes (`TopicOperationDispatcher.java:107-130`). A read that writes complicates the change stream. (FS-R46, FS-R56)
6. Python and Java GVFS differ in behaviour (cache keys, scheme set, default-location fallback). Mudcat conformance tests should run one table of cases against every client. (FS-R24, FS-R25, FS-R30)
