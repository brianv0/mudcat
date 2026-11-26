> **Research note, not a requirement document.** Drafted by a code-reading agent from Apache Gravitino commit `9aa78cf` (2026-11-26), then spot-checked by the lead. Line numbers can drift by a few lines; search by file and symbol. Items marked `(inferred)` were not confirmed in code. The text does not follow Simplified Technical English. See [../README.md](../README.md) for the synthesis.

# Metadata enrichment and governance objects: tags, policies, statistics, semantic model, domains/glossary (ref prefix TP)

Source: Apache Gravitino checkout `/home/user/apache/gravitino` (commit 9aa78cf). All paths below are repo-relative to that checkout unless noted. `P` = `core/src/main/java/org/apache/gravitino`.

## 1. Decomposition

**Shape of the whole area.** Tags, policies, statistics, semantic models and domains are all "side metadata" that points at a primary object (table, schema, column...) by stable numeric ID plus a type string. There is no foreign key. Every cascade is hand-written SQL. Policies never attach to objects directly any more; they attach to tags, tags attach to objects, and objects inherit tags down the container hierarchy. Domains and (designed, not built) glossary terms are layered on top of tags.

```
Metadata object (catalog/schema/table/column/fileset/topic/model/view/function/semantic_model)
   ^  tag_relation_meta (tag_id, metadata_object_id, metadata_object_type, tag_value)      [no FK, soft delete]
Tag (tag_meta; flat, metalake-scoped; allowed_values JSON; 128-char name)
   ^  policy_tag_relation_meta (policy_id, tag_id, selector JSON)                           [no FK]
Policy (policy_meta + policy_version_info; typed content JSON; enabled flag)
Domain (domain_meta.backing_tag_id) -> membership = "object's effective tags contain backing tag"
Statistic (statistic_meta: object-level, TABLE only) | partition_statistic_meta (table_id, partition, name)
SemanticModel (semantic_model_meta.current_version/active_version + semantic_model_version_info snapshots
               + semantic_model_source_rel ranges + semantic_model_metric_meta identities)
```

Key classes (all under `P/` unless noted):

| Concern | Class | Role |
|---|---|---|
| Tag CRUD, assignment | `tag/TagManager.java` | validation, allowed values, supported object types (`:77`), calls store relation ops |
| Tag inheritance | `tag/EffectiveTagResolver.java` | walks object then ancestors, first assignment per tag name wins (`:256`, `:296`); batch variant reads each distinct ancestor once (`:189`) |
| Tag storage | `storage/relational/service/TagMetaService.java` | row-locking assignment transaction (`:537`), tag delete cascade (`:773`), orphan-safe lookups (`:1192`) |
| Policy CRUD | `policy/PolicyManager.java` | create/alter/enable/disable; content validation (`:179`, `:429`) |
| Policy selection | `policy/ObjectPolicyResolver.java` | effective tags -> policy-tag relations -> selector match -> enabled filter -> dedupe by policy ID |
| Policy selectors | `api/.../policy/AllValuesSelector.java`, `TagValueSelector.java`; JSON in `policy_tag_relation_meta.selector` | tag presence or exact value |
| Policy content types | `api/.../policy/Policy.java` (`BuiltInType`), `AccessControlContent`, `RowFilterContent`, `ColumnMaskContent`, `Iceberg*Content`, `PolicyContents.CustomContent` | typed payloads, each with `validate()` |
| Restricted SQL for row filter / column mask | `policy/sql/*` (ANTLR parser, `RestrictedSqlLimits`: depth 16, nodes 1024, params 256, string 4096 B, IN 1000) | engine-neutral expression grammar |
| Hidden policy tag | `policy/HiddenPolicyTag.java` | server-managed tag `gravitino_hidden_<policyId>` per policy, used to migrate legacy direct bindings |
| Policy lifecycle SPI | `policy/hook/PolicyLifecycleHooks.java` | single registered hook run inside the open store transaction at create/update/delete |
| Legacy migration | `policy/PolicyBindingMigration*.java` | offline conversion of direct object-policy rows into tag relations |
| Table statistics | `stats/StatisticManager.java`, `storage/relational/service/StatisticMetaService.java` | per-object name -> value, optimistic-version batch write |
| Partition statistics | `stats/storage/PartitionStatisticStorage` SPI; `JdbcPartitionStatisticStorage`, `LancePartitionStatisticStorage`, memory impl; config `gravitino.stats.partition.storageFactoryClass` (`Configs.java:731`) and prefix `gravitino.stats.partition.storageOption.` | pluggable backend, default JDBC on the entity DB |
| Stats writers | `maintenance/updaters/.../GravitinoStatisticsUpdater.java`, `maintenance/jobs/.../IcebergUpdateStatsAndMetricsJob.java` | external jobs write through the public REST/Java API (`custom-` prefix) |
| Semantic model storage | `storage/relational/service/SemanticModelMetaService.java`, `SemanticModelSourceService.java` | revision snapshots, source ranges, retention |
| Domains | `domain/DomainManager.java`, `DomainMembershipResolver.java`; `storage/relational/service/DomainMetaService.java`, `DomainRelService.java` | backing-tag membership, one-level hierarchy |
| Orphan sweep | `storage/relational/service/OrphanedMetadataObjectRelationService.java` + `storage/relational/RelationalGarbageCollector.java` | periodic soft delete of owner/tag/statistic/securable rows whose object is gone |
| Glossary, ontology, KB article | design docs only (no `GlossaryTerm`, no KB code; ontology has only api stubs and exceptions) | see section 4 |

Extension points and config keys that matter:
- `PartitionStatisticStorageFactory` SPI; `gravitino.stats.partition.storageFactoryClass` (`Configs.java:731`, default JDBC factory; Lance alternative).
- `PolicyLifecycleHooks.register` (single hook, throws `IllegalStateException` if two extensions register: `policy/hook/PolicyLifecycleHooks.java`).
- `SemanticModelPublicationPolicy` (design: deployment-installed policy that makes saves drafts instead of published; the only way to publish).
- `gravitino.entity.store.semanticModel.versionRetentionCount` (default 100, range 1-1000; `Configs.java:368`) versus `gravitino.entity.store.versionRetentionCount` (default 1, max 10; `Configs.java:84-88`) used for everything else, policies included.
- `gravitino.entity.store.deleteAfterTimeMs` default 7 days (`Configs.java:68`); GC runs every max(10 min, deleteAfter/10) (`storage/relational/RelationalGarbageCollector.java:73`).
- Tag-based access control is behind one server flag, default off (design `design-docs/tag-based-access-control.md`, "Enabling the feature").

Neighbours: authorization (`TagPolicyMatcher`, tag-conferred privileges via `AccessControlContent`), Iceberg REST (read restrictions from row-filter/column-mask policies, `design-docs/tag-based-read-restrictions-for-iceberg-rest.md`), table maintenance service (consumes `system_iceberg_*` policies via `ObjectPolicyResolver`), change stream (listener events per operation, `listener/api/event/{policy,stats}`), entity cache (`RelationalEntityStore.java:763`).

## 2. Candidate requirements

Kinds: IMPL = Gravitino does it; GAP = Gravitino omits or gets it wrong. Mudcat refs: ST-17 tag inheritance, ST-22 expectations/statistics, UP usage policy.

### 2.1 Tags: inheritance, values, identity

| Ref | Kind | Requirement | Rationale / Gravitino evidence | Area | Ver |
|---|---|---|---|---|---|
| TP-R1 | IMPL | mudcat shall attach each tag assignment to the immutable ID of the tagged entity, so that renaming the entity keeps its tags. | Relations key on `metadata_object_id`; tests pin "A rename keeps the column id" (`core/src/test/.../TestTagMetaService.java:2352`). Needed for ST-17. | ST | T |
| TP-R2 | IMPL | mudcat shall resolve the tags of an entity by walking the entity and then each ancestor, nearest first. | `EffectiveTagResolver.java:282-300`; `MetadataObjectUtil.getParentMetadataObjects` `:202`. | ST | T |
| TP-R3 | IMPL | mudcat shall mark each resolved tag as direct or inherited and name the entity that supplied it. | `ResolvedTag{tag, source}` and `copyWithInherited(index>0)` (`EffectiveTagResolver.java:107`, `:297`). Mudcat needs the source for explainability and policy provenance. | ST | T |
| TP-R4 | GAP | mudcat shall define whether a tag assignment on a descendant replaces or adds to an inherited assignment of the same tag, and shall let a tag definition declare "non-overridable" for restriction-bearing tags. | Gravitino is always nearest-wins by tag name, values included (`EffectiveTagResolver.java:256`; `design-docs/tag-assignment-values.md:219-240`). Its own read-restriction design says `data_access=public` on one table "overrides the inherited value" and un-selects the policy (`design-docs/tag-based-read-restrictions-for-iceberg-rest.md:126-132`). A descendant owner can therefore weaken a parent's embargo, legal hold or consent tag. Mudcat UP needs sticky inheritance. | UP | T |
| TP-R5 | GAP | mudcat shall compute inherited tags in one shared component that every consumer calls (listing, policy selection, access checks, change stream). | Gravitino has two implementations: `EffectiveTagResolver` (core, batched) and a REST copy that does one query per ancestor and merges by `putIfAbsent` (`server/.../MetadataObjectTagOperations.java:182-208`). The design doc itself demands one resolver (`design-docs/policy-on-tag.md:826-838`). | ST | I |
| TP-R6 | IMPL | mudcat shall resolve inherited tags for a list of entities in a bounded number of store reads, independent of list length. | `resolveWithSources(metalake, List)` reads each distinct ancestor once, grouped by type (`EffectiveTagResolver.java:189-262`); `TagMetaService.listDirectTagsByObjectIds` batches by 500 (`SupportsDomainOperations.java:63`). | PF | T |
| TP-R7 | IMPL | mudcat shall inherit tags through nested namespaces, including intermediate namespaces that have no stored entity. | `getParentMetadataObjects` inserts synthetic ancestor schemas for hierarchical names (`MetadataObjectUtil.java:208-219`); `loadTags(..., allowMissing=true)` returns empty for a missing ancestor (`EffectiveTagResolver.java:311`). Needed for Iceberg nested namespaces. | ST | T |
| TP-R8 | IMPL | mudcat shall let a tag assignment carry zero, one or many string values, with at most one valueless assignment per tag per entity. | Unique key `(tag_id, metadata_object_id, metadata_object_type, tag_value, deleted_at)` (`scripts/postgresql/schema-2.0.0-postgresql.sql:534-547`); mixed valued and valueless in one request is rejected (`tag/TagManager.java:531`). | ST | T |
| TP-R9 | IMPL | mudcat shall let a tag definition restrict assignment values to a declared allowed set, or to no value. | `TagEntity.validateAllowedValue` (`meta/TagEntity.java:160-186`): NULL = any, `[]` = none. | ST | T |
| TP-R10 | GAP | mudcat shall let an authorised caller change the allowed values of a tag, and shall report which existing assignments become invalid. | Gravitino made allowed values immutable after creation "to avoid validating existing assignments" (`design-docs/tag-assignment-values.md:267-275`), and `alterTag` re-stores the old constraint (`TagManager.java:605`). Real governance vocabularies evolve. | ST | T |
| TP-R11 | GAP | mudcat shall enforce the maximum length of a tag value in the core model, not only in the transport layer. | 256-char limit sits in DTOs (`common/.../TagValuesAssociateRequest.java:38`, `TagCreateRequest.java:38`), while `TagValue.of` only checks non-blank (`api/.../tag/TagValue.java:51`). Embedded callers hit the DB error instead. | ST | T |
| TP-R12 | GAP | mudcat shall return tag-value lists in a documented, stored order. | Design promises a `value_order` column (`tag-assignment-values.md:567`); schema has none (`grep value_order` returns nothing) and reads have no `ORDER BY` (`TagMetadataObjectRelBaseSQLProvider.java:45-66`). | ST | T |
| TP-R13 | IMPL | mudcat shall make a tag assignment change atomic: lock the tag rows in ID order, validate values, then write all relation rows in one transaction. | `associateTagValuesWithMetadataObject` (`TagMetaService.java:537-733`), `lockTagsForAssignment` locks "in tag ID order... instead of deadlocking" (`:1010-1037`). | ST | T |
| TP-R14 | IMPL | mudcat shall ignore an assignment to a tag that was dropped or renamed while the request ran, and shall fail only when the target entity is gone. | `TagManager.java:469-472`; `TestTagMetaService.java:256` and `:845`. | ST | T |
| TP-R15 | IMPL | mudcat shall reject dots in tag and policy names and shall keep legacy dotted names readable. | `NameIdentifierUtil.checkMetadataObjectName` (`utils/NameIdentifierUtil.java:651`); `testReadTagsForLegacyDottedNames` (`TestTagMetaService.java:194`). | ST | T |
| TP-R16 | IMPL | mudcat shall hide system-owned tags from user listings and refuse user creation of tags in the reserved prefix. | `HiddenPolicyTag.isReservedName` filters lists and rejects names (`server/.../TagOperations.java:125`, `:561-581`). | ST | T |
| TP-R17 | GAP | mudcat shall report to a caller that a tag list is incomplete because some tags are not visible to that caller, and shall not let an invisible nearer tag silently hide a visible inherited tag of the same name. | Gravitino merges first, then authorization-filters, and returns `hasInaccessibleTags` (`MetadataObjectTagOperations.java:210-219`); the hidden nearer tag has already shadowed the inherited one. | AC | T |
| TP-R18 | GAP | mudcat shall list the entities that carry a tag with a page size and a continuation token. | `listAssociatedMetadataObjectsForTag` returns all rows, no limit (`TagMetaService.java:413-442`); `TagOperations` has no paging parameters. A tag on a catalog expands to everything. | PF | T |

### 2.2 Cascades, dangling references, orphans

| Ref | Kind | Requirement | Rationale / evidence | Area | Ver |
|---|---|---|---|---|---|
| TP-R19 | IMPL | mudcat shall retire all tag, owner, statistic and policy-binding rows of an entity in the same transaction that retires the entity. | Per-entity cascade code: `TableMetaService.java:515-540`, `SchemaMetaService.java:389-405`, `PolicyMetaService.java:215-250`, `TagMetaService.java:773-809`. | UP | T |
| TP-R20 | GAP | mudcat shall define the cascade once, from a registry of relation tables, so that adding an entity type cannot miss a cascade. | Gravitino hand-writes a UNION over every entity type for each container (catalog/schema/table) in SQL (`TagMetadataObjectRelBaseSQLProvider.java:296-460`); every new type (view, function, semantic model) had to be added to each, and some services still skip statistics (`ViewMetaService.java:430` deletes tags only). | ST | I |
| TP-R21 | IMPL | mudcat shall run a periodic sweep that retires relation rows whose target entity no longer exists, as a backstop to inline cascades. | `OrphanedMetadataObjectRelationService.java:50-135` (owner, tag, statistic, securable), driven by `RelationalGarbageCollector.java:84-100`; semantic model delete relies on it (`SemanticModelMetaService.java:464`). | OP | T |
| TP-R22 | IMPL | mudcat shall skip, not fail on, relation rows whose target entity was deleted after the relation was read. | `TagMetaService.java:1218-1222`: "Metadata object may be deleted asynchronously... skip". | PF | T |
| TP-R23 | IMPL | mudcat shall not hand a dropped entity's tags to a new entity that reuses the name (including a column). | `TestTagMetaService.java:2401`: "A new column that reuses the dropped column's name does not inherit its tag"; deleted tag IDs are never reusable (`TagMetaService.java:939`, `TestTagMetaService.java:225`). | ID | T |
| TP-R24 | GAP | mudcat shall bind references by immutable ID, and shall also record the name used when the reference was made, so that drift is detectable. | Gravitino semantic models store source ID in `semantic_model_source_rel` but the definition keeps the old name; after a source rename lookups still work while definition text lies (`gravitino-semantic-model-source-lookup-design.md:395-402`, `:347`). | ST | A |
| TP-R25 | GAP | mudcat shall fail a column rename that would lose the column's identity, instead of turning it into drop plus add. | Glossary design admits "a stale catalog snapshot can turn a rename into drop/add and lose the column ID and its bindings, as with column tags", issue #14279 (`design-docs/glossary-design.md:329-336`). | ID | T |

### 2.3 Policies

| Ref | Kind | Requirement | Rationale / evidence | Area | Ver |
|---|---|---|---|---|---|
| TP-R26 | IMPL | mudcat shall attach usage policies only to tags, and derive the policies of an entity from its effective tags. | Policy-on-tag target model: object-side attachment only through tags (`design-docs/policy-on-tag.md:34-75`); `ObjectPolicyResolver.java`. Maps onto UP + ST-17. | UP | T |
| TP-R27 | IMPL | mudcat shall bind a policy to a tag with a selector that matches tag presence or one exact tag value. | `AllValuesSelector`, `TagValueSelector`; selector JSON in `policy_tag_relation_meta` (`schema-2.0.0-postgresql.sql:745-768`); matching in `ObjectPolicyResolver.java` `matches()`. | UP | T |
| TP-R28 | IMPL | mudcat shall select a policy when at least one of its tag relations matches, and shall return the entities that caused the match. | `MatchState.record` accumulates sources; "Non-matching relations do not negate matching relations" (`policy-on-tag.md:739-762`; `ObjectPolicyResolver.java` `MatchState`). | UP | T |
| TP-R29 | IMPL | mudcat shall treat a disabled policy as absent during selection. | `.filter(match -> match.policy().enabled())` in `ObjectPolicyResolver.selectMatches`. | UP | T |
| TP-R30 | IMPL | mudcat shall fail closed when two distinct row-filter or two distinct column-mask policies apply to one target, and shall not merge them or choose by priority. | `policy-on-tag.md:764-771`; `tag-based-read-restrictions-for-iceberg-rest.md:691-706` ("Any loading, parsing... or conflict error aborts the governed load"). Resolver itself only selects and dedupes. | UP | T |
| TP-R31 | GAP | mudcat shall specify, per policy type, how multiple matching policies combine (union, intersection, deny wins or conflict) before shipping the type. | Gravitino left conflict handling to each future consumer ("conflict check should be added as a shared helper", `policy-on-tag.md:770`; checklist item `:900` unchecked). Access policies are allow-only so unions are safe (`tag-based-access-control.md:101`). | UP | A |
| TP-R32 | IMPL | mudcat shall validate policy content when it is created or its content changes, and shall not re-validate stored content on rename, comment change or enable/disable. | `PolicyManager.java:179`, `:336`, `:427-437`. Prevents tightened validation from locking out an operator who must disable a legacy bad policy. | UP | T |
| TP-R33 | IMPL | mudcat shall bound the size and nesting of any expression stored in a policy. | `RestrictedSqlLimits.java` (depth 16, 1024 nodes, 256 params, 4096-byte strings, 1000 IN items). | UP | T |
| TP-R34 | IMPL | mudcat shall forbid changing the policy type, and for built-in types the allowed entity types, on content update. | `PolicyManager.java:399-417`. | UP | T |
| TP-R35 | GAP | mudcat shall retain every version of a policy that was ever in force, and shall let a caller read the version that applied at a given commit. | Policy versions are kept under the shared retention count, default 1, max 10 (`Configs.java:84-88`; `PolicyMetaService.java:278-306`). Only the current policy content survives; a past decision cannot be explained. | UP | T |
| TP-R36 | IMPL | mudcat shall limit the privileges a tag-bound access policy can confer to an explicit allowlist, so a new privilege confers nothing until added. | `AccessControlContent.PERMITTED_PRIVILEGES` (allowlist, "fails closed"); `tag-based-access-control.md:156-195`. | AC | T |
| TP-R37 | IMPL | mudcat shall read tag and policy state fresh on each authorisation check, with no cross-request cache. | `tag-based-access-control.md:391-419` ("Tag state is not cached across requests"); cost is paid on every request. | AC | A |
| TP-R38 | GAP | mudcat shall record decision provenance (tag, policy version, source entity) for every access or restriction decision a tag caused, including allows. | Gravitino emits events on denial only; "Nothing records that a tag granted access" (`tag-based-access-control.md:104`, `:664-672`). | UP | D |
| TP-R39 | IMPL | mudcat shall run policy create, update and delete extension hooks inside the policy transaction and shall allow one hook only. | `policy/hook/PolicyLifecycleHooks.java` (`register` fails on a second hook). | OP | I |

### 2.4 Statistics

| Ref | Kind | Requirement | Rationale / evidence | Area | Ver |
|---|---|---|---|---|---|
| TP-R40 | IMPL | mudcat shall require producer-defined statistic names to carry a fixed prefix, and shall reserve every other name for the system. | `Statistic.CUSTOM_PREFIX="custom-"`, max 128 chars (`api/.../stats/Statistic.java:34-37`); enforced in `StatisticOperations.java:514-525` for table and partition updates. | ST | T |
| TP-R41 | IMPL | mudcat shall write a batch of statistics for one entity atomically, and shall reject the whole batch when a concurrent writer changed any statistic in it. | `StatisticMetaService.writeStatisticsWithVersion` (`:100-180`); `OptimisticLockException`, "Gravitino does not automatically retry" (`api/.../SupportsStatistics.java:46-52`). Fits ST-22 enrichers racing each other. | ST | T |
| TP-R42 | GAP | mudcat shall tie each statistic to the immutable version (snapshot) of the data it describes, and shall state when a statistic is stale against the current version. | Gravitino statistics are `name -> value + audit` only (`Statistic.java`), no snapshot or as-of field; staleness cannot be judged. Iceberg-specific `table_snapshot_metrics(table_id, policy_id, snapshot_id)` exists outside the generic model (`schema-2.0.0-postgresql.sql:1339`). | ST | T |
| TP-R43 | GAP | mudcat shall store statistics for every entity kind, not only tables. | `StatisticEntity.getStatisticType` throws for anything but TABLE (`meta/StatisticEntity.java:94-100`) though `statistic_meta` stores a `metadata_object_type` and the API says "fileset... can implement" (`SupportsStatistics.java:28`). | ST | T |
| TP-R44 | GAP | mudcat shall check that the target entity exists and shall retire partition statistics when the entity is dropped. | `JdbcPartitionStatisticStorage` resolves the table ID but nothing outside `stats/` touches `partition_statistic_meta` (no cascade from `TableMetaService`); the table is soft deleted and rows stay (grep over `core/src/main` finds no other reference). The partition table also has no `deleted_at`, no metalake ID and no audit of deletion. | UP | T |
| TP-R45 | GAP | mudcat shall serialise concurrent partition-statistic writers by a store-level guarantee, not by a process-local lock. | `StatisticManager.updatePartitionStatistics` takes the in-memory tree lock because the Lance backend "deletes and then appends in two commits" (`StatisticManager.java:286-290`; `LancePartitionStatisticStorage.java:245-250`). Two servers behind one catalog lose that protection. | PF | T |
| TP-R46 | IMPL | mudcat shall bound the statistics in one partition-statistic update. | `Preconditions.checkArgument(total <= maxStatisticsPerUpdate)` (`LancePartitionStatisticStorage.java:216-224`). | PF | T |
| TP-R47 | GAP | mudcat shall compare partition names for range queries with one defined ordering that does not depend on database collation. | JDBC range filter uses SQL `>=`/`<=` on `partition_name` text (`JdbcPartitionStatisticStorage.java:400-428`); MySQL primary key indexes only a 255-char prefix of the 1024-char name (`scripts/mysql/schema-2.0.0-mysql.sql:644`). (inferred: prefix uniqueness collisions on long partition names) | ST | T |

### 2.5 Semantic models and revisions

| Ref | Kind | Requirement | Rationale / evidence | Area | Ver |
|---|---|---|---|---|---|
| TP-R48 | IMPL | mudcat shall store each saved definition of a curated model as an immutable numbered revision starting at 1, with contiguous numbers. | `semantic_model_version_info UNIQUE(semantic_model_id, version, deleted_at)`; `last_version` allocates (`schema-2.0.0-postgresql.sql:1012-1060`); "revisions are numbered contiguously" (`gravitino-semantic-model-revision-design.md:490`). | ID | T |
| TP-R49 | IMPL | mudcat shall keep two pointers per model, latest and active, and shall serve every consumer read from the active revision. | `current_version` and `active_version` columns; "All consumers see the same revision" (`revision-design.md:169-172`). Mirrors mudcat immutable versions plus mutable pointers. | ID | T |
| TP-R50 | IMPL | mudcat shall require a replacement of a definition to name the revision it was based on, and shall reply with a conflict when the latest revision has moved. | `expectedRevision` check (`SemanticModelMetaService.java:659-663`); user journey 3 (`revision-design.md:96-103`). | ID | T |
| TP-R51 | IMPL | mudcat shall make a rename a change to the entity name only, creating no revision, and shall refuse a rename combined with other changes. | `revision-design.md:443-445`: "A rename must be the only change in its request". | ID | T |
| TP-R52 | IMPL | mudcat shall report the revision number in every read of a versioned definition. | Load response adds `revision`, `status`, publisher (`revision-design.md:49-51`, `:520-530`). Matches the "every response names the commit" idea. | ID | I |
| TP-R53 | IMPL | mudcat shall keep the active revision and every explicitly published revision out of retention, and shall return a distinct "removed by retention" error for a collected revision. | `softDeleteSemanticModelVersionsByRetentionLine(... activeVersion ...)` (`SemanticModelMetaService.java:424`); message `:693`. | UP | T |
| TP-R54 | GAP | mudcat shall keep the stored publication states and the documented publication states identical. | Design lists `PUBLISHED_EXPLICITLY` that is "kept while the model exists" (`revision-design.md:252`, `:268`); code enum has only `DRAFT` and `PUBLISHED_ON_SAVE` (`storage/relational/po/SemanticModelVersionInfoPO.java:44-49`); only retention-line protection by status is therefore unreachable. | OP | I |
| TP-R55 | IMPL | mudcat shall maintain reverse source-to-consumer associations as revision ranges `[from_version, to_version)` written in the same transaction as the definition. | `semantic_model_source_rel` with `from_version`, `to_version=0 open` (`schema-2.0.0-postgresql.sql:1357-1380`); lookup predicate `revision-design.md:469-474`. | ST | T |
| TP-R56 | GAP | mudcat shall version governance relations (tags, policies, owners) together with the definition, or shall state per read which governance was in force. | Revision design Non-Goal 7: "A revision read returns the definition in effect, not the governance in effect at that time" (`:80-88`). | UP | A |
| TP-R57 | IMPL | mudcat shall resolve a reference to a source entity by ID at write time and shall keep the association when the source is renamed or deleted. | Lifecycle table (`source-lookup-design.md:343-358`). | ST | T |

### 2.6 Domains, glossary, ontology

| Ref | Kind | Requirement | Rationale / evidence | Area | Ver |
|---|---|---|---|---|---|
| TP-R58 | IMPL | mudcat shall derive collection membership from tags that inherit, with no separate membership store. | `domain-design.md` Goals 2 and 6; `DomainMembershipResolver` over `EffectiveTagResolver`. | ST | T |
| TP-R59 | IMPL | mudcat shall bind a collection to its backing tag by tag ID, never by name, so that renaming keeps membership and recreating a same-named tag does not revive it. | `domain-design.md:62` ("compare tag IDs, never tag names"); `domain_meta.backing_tag_id`. | ID | T |
| TP-R60 | IMPL | mudcat shall list inherited members only on explicit request, and shall report for each member the relation (direct, inherited from X, via subdomain). | `expand` defaults to false (`domain-design.md:242`, `:157-162`); the "Cost" note says expansion visits every recorded leaf. | PF | T |
| TP-R61 | GAP | mudcat shall answer "which entities have tag X, directly or inherited" from an index, not by expanding containers at query time. | Gravitino batches (500 / 5000) and semijoins to survive (`DomainMembershipResolver.java:120-200`; `SupportsDomainOperations.java:63-66`) and still documents unbounded cost. | PF | T |
| TP-R62 | GAP | mudcat shall define the effect on references when a glossary term, vocabulary item or ontology concept is renamed or removed, before the feature ships. | Glossary is a 0.7 draft, "future implementation work" (`glossary-design.md:91-92`); ontology design allows reference breakage in unevaluated expressions (`gravitino-ontology-mapping-design.md:75-79`). No code exists for either. | ST | A |

## 3. Quirks and gotchas ledger

- **Nearest tag wins by name, so a child can weaken a parent**
  - What happens: effective tags are merged by tag name, nearest first; the first assignment, including its values, replaces all farther ones. A policy selected by `TAG_VALUE("restricted")` on a schema stops applying on a table tagged `data_access=public`. The Iceberg read-restriction design calls this the "exclusion pattern".
  - Cause: MODEL.
  - Evidence: `P/tag/EffectiveTagResolver.java:256` `effectiveTags.putIfAbsent(tag.name(), ...)`; `design-docs/tag-based-read-restrictions-for-iceberg-rest.md:126-132` "overrides the inherited value and does not match that selector".
  - Mudcat implication: TP-R4 (non-overridable inheritance for UP tags), TP-R3 (source reported).

- **ALL_VALUES cannot express exclusion**
  - What happens: an `ALL_VALUES` selector matches whenever the tag is present, so a nearer assignment of the same tag does not un-select the policy, while `TAG_VALUE` does. Two selectors give opposite override behaviour for the same tag.
  - Cause: MODEL.
  - Evidence: `design-docs/tag-based-read-restrictions-for-iceberg-rest.md:129-132` "This exclusion pattern does not work with `ALL_VALUES`".
  - Mudcat implication: TP-R4, TP-R31.

- **Two inheritance implementations disagree on cost and on hidden tags**
  - What happens: the REST list path walks ancestors with one dispatcher call each and filters hidden tags before merging; the core resolver (used for policies and authorization) is batched. They agree only by convention.
  - Cause: OTHER (design drift).
  - Evidence: `server/.../MetadataObjectTagOperations.java:190-208` `tags.putIfAbsent(...)` per parent; `design-docs/policy-on-tag.md:826` "should use the same resolver".
  - Mudcat implication: TP-R5.

- **Authorisation filtering happens after the shadowing merge**
  - What happens: a nearer tag the caller cannot see still hides an inherited tag with the same name; the response only sets `hasInaccessibleTags`.
  - Cause: SECURITY.
  - Evidence: `MetadataObjectTagOperations.java:210-219` (merge at `:190-208`, filter after).
  - Mudcat implication: TP-R17.

- **No value is stored as the empty string, and docs still say NULL**
  - What happens: the schema uses `tag_value VARCHAR(256) NOT NULL DEFAULT ''`; reads convert back with `NULLIF(tag_value,'')`. The design doc describes `tag_value IS NULL` and a `value_order` column that do not exist. The sentinel is what lets the UNIQUE key reject duplicate valueless rows (inferred: NULLs are distinct in unique indexes; the test `testRejectDuplicateValuelessTagAssignment` expects a `SQLException` from a second row).
  - Cause: STORE.
  - Evidence: `scripts/postgresql/schema-2.0.0-postgresql.sql:538` `tag_value VARCHAR(256) NOT NULL DEFAULT ''`; `TagMetadataObjectRelBaseSQLProvider.java:48` `NULLIF(te.tag_value, '') as assignmentValue`; `design-docs/tag-assignment-values.md:143`, `:567-574`; `TestTagMetaService.java:1626-1649`.
  - Mudcat implication: TP-R8, TP-R12; use a typed "no value" and a partial unique index rather than a sentinel.

- **Deleted-at is part of every unique key, so soft delete forces unique names to be re-checked in code**
  - What happens: unique keys are `(…, deleted_at)`; deleted rows keep their ID and block reuse of that ID. Overwrite of a deleted tag ID throws "reserved by a deleted tag; use a new ID".
  - Cause: STORE.
  - Evidence: `TagMetaService.java:938-940`; `TestTagMetaService.java:225-252`.
  - Mudcat implication: TP-R23; mudcat IDs are UUIDv7 minted once, so never reuse.

- **Soft-deleted relation rows are physically removed only after a delay, by two collectors**
  - What happens: inline cascades soft delete (`deleted_at = now`); the periodic sweep soft deletes orphans; a third pass hard deletes rows older than `deleteAfterTime` (7 days default), 100 rows per call. Tests count 1 active row versus 21 total rows after one schema drop.
  - Cause: OTHER.
  - Evidence: `Configs.java:68`, `:112`; `RelationalGarbageCollector.java:73-100`; `TestTagMetaService.java:2297-2299` ("assertEquals(21, countAllTagRel...)").
  - Mudcat implication: TP-R21; mudcat UP deletion (takedown) needs a hard-purge guarantee separate from a 7-day timer.

- **Cascade SQL is a UNION over every entity type, per container**
  - What happens: dropping a catalog, schema or table soft-deletes tag relations with a hand-built UNION that mentions table, topic, fileset, column, model, view, function and semantic model, each with an "exclude other active catalog" guard. A new entity type that is not added silently leaves tags behind until the sweep.
  - Cause: MODEL (polymorphic reference without FK).
  - Evidence: `TagMetadataObjectRelBaseSQLProvider.java:296-460`; `MetadataObjectSQL.java:19-70`.
  - Mudcat implication: TP-R20, TP-R21.

- **Historical entity rows may be re-registered under a different parent**
  - What happens: cascade SQL has to exclude IDs that currently belong to another catalog ("keep historical entity rows from deleting another registration's metadata"), so an entity ID can be live under catalog B while an old row under catalog A is retired.
  - Cause: MODEL.
  - Evidence: `storage/relational/mapper/provider/MetadataObjectSQL.java:21-33`; `design-docs/glossary-design.md:456-459` (same-name recreation "indistinguishable from the old registration" for external catalogs).
  - Mudcat implication: TP-R23; for federated registrations mudcat needs registration identity separate from content identity.

- **Delete of a tag cleans its relations in the same transaction, but only the tag delete is checked**
  - What happens: the version-checked soft delete of the tag row is first and decides the outcome; the other cleanups are blanket "soft delete by tag ID" statements whose zero-row results carry no information. A losing delete rolls back before touching dependants.
  - Cause: CONCURRENCY.
  - Evidence: `TagMetaService.java:776-781` comment, `:783-803`; `testStaleTagDeleteRollsBackRelationshipCleanup` (`TestTagMetaService.java:1026`).
  - Mudcat implication: TP-R19.

- **Read-committed insert race on missing rows**
  - What happens: overwrite (upsert-style put) of a missing tag cannot fence a concurrent insert at READ_COMMITTED, so a duplicate-key error is converted to a retryable concurrent-modification error and the transaction rolls back.
  - Cause: STORE / CONCURRENCY.
  - Evidence: `TagMetaService.java:137-143` "A missing-row locking read does not fence a concurrent insert at READ_COMMITTED."
  - Mudcat implication: mudcat idempotency keys: map duplicate-key on first insert to "retry", not "already exists".

- **Store reports missing tag and missing object with one exception type**
  - What happens: `NoSuchEntityException` is raised for both; the manager rebuilds the expected message string to tell them apart.
  - Cause: MODEL.
  - Evidence: `tag/TagManager.java:374-376`, `:629-640` ("the message is the only thing that tells them apart").
  - Mudcat implication: typed error codes in the API contract, not message parsing.

- **Policy "enabled" and content share one version row; retention default is 1**
  - What happens: every content change inserts a `policy_version_info` row, but the shared GC keeps only the newest `versionRetentionCount` (default 1, max 10) versions. No API reads old policy versions.
  - Cause: OTHER.
  - Evidence: `Configs.java:84-88`; `PolicyMetaService.java:278-306`, `:444-451`.
  - Mudcat implication: TP-R35.

- **Validation tightened over time must not block disable or rename**
  - What happens: `enablePolicy`, rename and comment changes skip content validation ("must not re-validate legacy stored content") because old stored content can fail new checks.
  - Cause: COMPAT.
  - Evidence: `PolicyManager.java:336`, `:427`.
  - Mudcat implication: TP-R32; also keep a content schema version in each policy.

- **Legacy direct object-policy bindings became hidden tags**
  - What happens: each policy gets a server-managed tag `gravitino_hidden_<policyId>` (empty allowed values, stable across renames); migration converts old object-policy rows into assignments of that tag. REST filters the tag out of lists, but listing code in several places must remember to.
  - Cause: COMPAT.
  - Evidence: `policy/HiddenPolicyTag.java:38-60`; `PolicyManager.java:100-127`; `scripts/postgresql/upgrade-1.3.0-to-2.0.0-postgresql.sql:43-44` (old table kept, no longer read).
  - Mudcat implication: design warning: a "breaking model change" needed an offline migration tool and a compatibility window; avoid two attachment paths from the start (TP-R26).

- **Policy supportedObjectTypes is deprecated but still validated**
  - What happens: `PolicyContent.validate()` requires a non-empty `supportedObjectTypes()` while policy-on-tag says the field no longer filters anything; update still compares the set for built-in types.
  - Cause: COMPAT.
  - Evidence: `api/.../policy/PolicyContent.java` `validate()`; `PolicyManager.java:407-417`; `design-docs/policy-on-tag.md:197-205`.
  - Mudcat implication: do not put entity-type applicability in policy content when attachment is by tag.

- **Policy to tag relation cannot change its selector in place**
  - What happens: adding an existing (policy, tag) pair conflicts regardless of selector; to change a selector remove and re-add. The unique key is `(policy_id, tag_id, deleted_at)`.
  - Cause: MODEL.
  - Evidence: `policy-on-tag.md:301-304`; `schema-2.0.0-postgresql.sql:757`; `TagManager.java:261`.
  - Mudcat implication: allow atomic replace of a relation selector in one commit.

- **Statistic names equal under MySQL collation but not in Java**
  - What happens: MySQL's collation ignores trailing spaces, so `"custom-a "` and `"custom-a"` hit the same unique key; Gravitino rejects such a write as an illegal name instead of a retryable conflict.
  - Cause: STORE.
  - Evidence: `StatisticMetaService.java:133-153`; `StatisticSQLProviderFactory.java:37-39` (`namesIgnoreTrailingSpaces` true for MySQL).
  - Mudcat implication: normalise or forbid trailing whitespace in all names; test on PostgreSQL only needs the rule stated.

- **Batch statistic writes deadlock by plan order**
  - What happens: one multi-row statement locks rows in the database's plan order, so two batches can deadlock; the whole batch is rolled back and reported as an `OptimisticLockException` to the caller.
  - Cause: CONCURRENCY.
  - Evidence: `StatisticMetaService.java:279-296` ("deadlocks are expected and accepted as conflicts too"; PostgreSQL 40P01, MySQL 1213, H2 40001).
  - Mudcat implication: TP-R41; sort rows (Gravitino sorts names at `:127`) and still treat deadlock as retryable conflict.

- **Statistics are table-only although the table is polymorphic**
  - What happens: the table has `metadata_object_type`, the API mentions filesets, cascades exist for topic, model, fileset, but `getStatisticType` accepts TABLE only.
  - Cause: MODEL.
  - Evidence: `meta/StatisticEntity.java:94-100`; `TopicMetaService.java:385`, `FilesetMetaService.java:552`, `ModelMetaService.java:524` (cleanup for types that can never have rows).
  - Mudcat implication: TP-R43.

- **Partition statistics bypass the entity store and the cascade**
  - What happens: partition stats live in `partition_statistic_meta` (or a Lance dataset) keyed by `table_id`, written through a separate storage SPI. Dropping the table does not delete them; reading resolves the table ID first so orphans are unreachable, not freed.
  - Cause: MODEL.
  - Evidence: `JdbcPartitionStatisticStorage.java:386-393`; no reference to the table outside `stats/` (grep); schema `schema-2.0.0-postgresql.sql:1115-1136` has no `deleted_at`.
  - Mudcat implication: TP-R44.

- **Lance backend cannot upsert**
  - What happens: update = delete, then append, in two commits; compaction of many small commits is a TODO.
  - Cause: ENGINE.
  - Evidence: `LancePartitionStatisticStorage.java:228-229` "TODO: The small updates and deletion may cause performance issues", `:245-246` "Lance Java API doesn't support the upsert operations".
  - Mudcat implication: TP-R45; do not choose a stats backend whose write is not atomic.

- **Revision GC versus publication status drift**
  - What happens: the design makes explicitly published revisions immune to retention; the code has no `PUBLISHED_EXPLICITLY` value, so only the active revision is protected. Default per-model retention is 100 (not 1).
  - Cause: OTHER (design ahead of code).
  - Evidence: `revision-design.md:264-286` vs `SemanticModelVersionInfoPO.java:44-49`; `Configs.java:368-376`.
  - Mudcat implication: TP-R53, TP-R54.

- **Semantic-model deletion leaves tag and owner rows to the sweep**
  - What happens: deleting a model soft-deletes versions and sources inline but leaves owner, tag and securable relations to the orphan collector, unlike tables.
  - Cause: OTHER.
  - Evidence: `SemanticModelMetaService.java:464-466` "cleaned up by OrphanedMetadataObjectRelationService during garbage collection".
  - Mudcat implication: TP-R19, TP-R21 (one rule: inline for correctness, sweep as backstop).

- **Definition text and association table can disagree**
  - What happens: the model keeps `Dataset.source` as a name string and a separate ID association; after a source rename the lookup by new name returns the dataset while the definition still shows the old name. Source deletion keeps the association and lookup through the missing name fails.
  - Cause: MODEL.
  - Evidence: `gravitino-semantic-model-source-lookup-design.md:395-402`, `:347-349`.
  - Mudcat implication: TP-R24, TP-R57.

- **Derived indexes are keyed by the active revision only**
  - What happens: under a deferred policy, metric listing and source lookup read `active_version`; metric identity rows are retired only when absent from both active and latest, to avoid dropping relations a consumer still sees.
  - Cause: MODEL.
  - Evidence: `revision-design.md:458-474`; `schema-2.0.0-postgresql.sql:1089-1112` (`semantic_model_metric_meta` has no version column).
  - Mudcat implication: TP-R49, TP-R55.

- **Metric rename is a delete plus an add with a new ID**
  - What happens: a metric renamed in a definition gets a new internal ID; term bindings and tags on the old ID are lost; re-adding a removed name gets a new ID and must not revive old bindings.
  - Cause: MODEL.
  - Evidence: `design-docs/glossary-design.md:428-437`.
  - Mudcat implication: TP-R25 (identity for sub-entities inside a revisioned document needs an explicit "renamed from").

- **Tag locking dance protects against parent drop**
  - What happens: tag creation takes a shared lock on the metalake row; metalake drop takes it exclusively; the metalake version is deliberately not compared so unrelated edits do not fail creates.
  - Cause: CONCURRENCY.
  - Evidence: `TagMetaService.java:890-915`.
  - Mudcat implication: tenant/org drop must fence child creates (TP-R13).

- **Row-filter and column-mask policies bind by field ID at load time**
  - What happens: the same expression is bound separately per descendant table; a table without exactly one compatible field fails the governed load. A tag on a schema is "intentionally broad" and can break loads for every table.
  - Cause: MODEL / SPEC (Iceberg field IDs).
  - Evidence: `design-docs/tag-based-read-restrictions-for-iceberg-rest.md:104-124`.
  - Mudcat implication: TP-R30; restriction tags on containers must have a dry-run that lists affected loads.

- **Policy-on-tag selectors do not validate against allowed values**
  - What happens: a `TAG_VALUE("fiance")` selector with a typo is syntactically valid and silently never selects; selector validation is "syntactic only".
  - Cause: MODEL.
  - Evidence: `design-docs/policy-on-tag.md:305-308` (rule 5).
  - Mudcat implication: add a validation warning when a selector value is outside a tag's allowed set (TP-R9).

## 4. Open questions / inconsistencies / half-implemented

1. **Glossary, KB articles, ontology are design only.** `design-docs/glossary-design.md` is "0.7 Draft" with a "design PR" note that T1-T19 "describe future implementation work"; no `GlossaryTerm` class exists (grep). The ontology API package has stubs and `Ontology*Exception` classes and one test, no storage (`api/src/main/java/org/apache/gravitino/ontology/`). KB article design (`gravitino-kb-article-design.md`) has no code. Treat as unvalidated.
2. **Design docs and schema disagree** on tag value storage (NULL vs `''`, `value_order` absent) and on `PUBLISHED_EXPLICITLY` (see ledger). Anyone reading only the designs would build the wrong thing.
3. **Column identity across external catalogs** is an open bug, issue #14279, with advice to "avoid concurrent loads/alters during column rename and verify bindings afterwards" (`glossary-design.md:329-336`).
4. **Tag-based access control is default-off and has four open questions** (OQ-1..OQ-4: where tags are evaluated, composition with RBAC deny, deleting a referenced role, authority to confer access through a tag) (`tag-based-access-control.md:722-830`). It also notes `ApplyTag.canBindTo` accepts only METALAKE and TAG so delegation cannot be scoped to a subtree (`:578-593`).
5. **Policy conflict helper is unimplemented**: checklist "Add a shared fail-closed conflict helper" is unchecked (`policy-on-tag.md:900`); the resolver only dedupes.
6. **Compatibility window for direct object policies is open ended**: GET APIs merge legacy direct policies with tag-derived ones, direct beats inherited (`policy-on-tag.md:609-613`); the end date is not stated.
7. **`Statistic.reserved()` and `modifiable()` exist but are hardwired**: `CustomStatistic` returns `reserved=false`, `modifiable=true` (`stats/StatisticManager.java` inner class), and `UnmodifiableStatisticException` is only produced in test mocks (`server/src/test/.../TestStatisticOperations.java:434`). Reserved statistics are a promise without an implementation.
8. **`TagManager.alterTag` rebuilds `allowedValues` from the old entity** (`TagManager.java:605`); there is no path to correct a typo in an allowed value other than drop and recreate, which deletes all assignments (`TagMetaService.java:773`).
9. **Domain backing-tag value column is kept but unused** ("kept in storage for a possible future extension"), and shared tags across domains via values from 2.0.0 development builds need a manual four-step migration (`domain-design.md:61`).
10. **Listing performance is not paged anywhere in this area**: tags, tag-to-objects, policy-to-tags and domain members return full arrays; domain design flags expansion cost under "Open Questions" (`domain-design.md:162`). Batch constants 500 / 5000 / 10 000 (`SupportsDomainOperations.java:63-66`, `TagMetaService.java:87`) are tuning knobs, not contracts.
11. **Retention: one global count** (default 1) governs policy versions, table versions and others, while semantic models got their own count (default 100). No per-type retention for policies; no "keep forever" class for audit-relevant objects.
12. **Authorisation on tag assignment for policy-conferred access** is "metalake-wide `CREATE_POLICY`" only; a `scope` field was dropped from `AccessControlContent` as "not a security boundary" (`tag-based-access-control.md:104`, scope row).
