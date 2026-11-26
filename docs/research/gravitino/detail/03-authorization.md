> **Research note, not a requirement document.** Drafted by a code-reading agent from Apache Gravitino commit `9aa78cf` (2026-11-26), then spot-checked by the lead. Line numbers can drift by a few lines; search by file and symbol. Items marked `(inferred)` were not confirmed in code. The text does not follow Simplified Technical English. See [../README.md](../README.md) for the synthesis.

# Authorization (AZ)

Scope read: `core/.../authorization/**`, `core/.../hook/**`, `authorizations/**` (common, ranger, chain), `server-common/.../server/authorization/**`, `api/.../authorization/**`, `server/.../web/filter/GravitinoInterceptionService.java`, `scripts/postgresql/schema-2.0.0-postgresql.sql`, `docs/security/*`, four design docs. Paths are repo-relative to `/home/user/apache/gravitino`. Items marked `(inferred)` were not directly observed in code.

## 1. Decomposition

### 1.1 Layers (request path, outside in)

```
HTTP request
 └─ server/.../web/filter/GravitinoInterceptionService.java      AOP interceptor on every REST method carrying @AuthorizationExpression
     ├─ validateCurrentUserAndActiveRoles()  user must be a member of the metalake; X-Gravitino-Active-Roles checked (L376)
     ├─ AuthorizeExecutorFactory.create(...)  chooses executor per request type (load table, associate tag, run job, lineage ...)
     └─ AuthorizationExpressionEvaluator     (server-common/.../authorization/expression/)  OGNL over authorizer calls
         └─ GravitinoAuthorizer (core SPI)   authorize / deny / isOwner / isSelf / isMetalakeUser / hasDenyPolicy / findUnheldRoles
             ├─ JcasbinAuthorizer (default, 2363 lines, server-common/.../authorization/jcasbin/)
             └─ PassThroughAuthorizer (allow-all; bootstrap only; auto-adds the caller as user)
List endpoints → MetadataAuthzHelper.filterByExpression() → short-circuit | batch preload + per-entity OGNL on thread pool
Mutation path → core dispatchers → *HookDispatcher (core/.../hook/) → OwnerDispatcher / AuthorizationUtils.authorizationPlugin*() → catalog AuthorizationPlugin (Ranger / JDBC / chain)
```

Authorization is an HTTP-layer concern. The core dispatchers have no check; only the REST interceptor does. Every other entry point (Iceberg REST, lineage, MCP) must wire its own check (`ICEBERG_*` expressions in `AuthorizationExpressionConstants.java`).

### 1.2 Components and key classes

| Component | Path | Responsibility |
|---|---|---|
| Privilege model | `api/.../authorization/Privilege.java`, `Privileges.java` (1906 lines), `SecurableObject(s).java`, `Role.java`, `Owner.java` | 44 privilege names, each with `canBindTo(type)`; allow/deny condition; securable object = (type, full name, privileges). |
| Authorizer SPI | `core/.../authorization/GravitinoAuthorizer.java` | Decision port plus cache-invalidation callbacks (`handleRolePrivilegeChange`, `handleMetadataOwnerChange`, `handleEntityNameIdMappingChange`). Impl chosen by `gravitino.authorization.impl` (`Configs.java:408`). |
| Request scratchpad | `core/.../authorization/AuthorizationRequestContext.java` | Per-request memo of decisions, name-to-id, owner, user/group info, tag lookups. Must not outlive one request. |
| Built-in engine | `server-common/.../jcasbin/JcasbinAuthorizer.java` + `server-common/src/main/resources/jcasbin_model.conf` | Two in-memory Casbin enforcers (allow, deny). Policy row = `(roleId, type, objectId, privilege, eft)`. `e = some(allow) && !some(deny)`. |
| Caches | `JcasbinLoadedRolesCache`, `CachedUserRoleRels`, `CachedGroupRoleRels`, `JcasbinAuthorizationLookups` | Version-validated (role/user/group `updated_at` probe per request) and eventually consistent (name-to-id, owner). |
| Cache invalidation | `JcasbinChangeListener.java`, table `entity_change_log` (schema-2.0.0:1176), `owner_meta.updated_at` cursor | Poll every `gravitino.authorization.jcasbin.changePollIntervalSecs` (default 3, `Configs.java:469`). |
| Expression language | `expression/AuthorizationExpressionConverter.java` (macro expansion to OGNL), `AuthorizationExpressionConstants.java` (425 lines), annotation `@AuthorizationExpression` (161 uses in `server/`) | `ANY(PRIV, SCOPE...)`, `X::OWNER`, `X::PRIV`, `X::SELF`, `ANY_<PRIV>` macros that bake in the ancestor list and the deny exclusion, `CAN_ACCESS_METADATA`, `SERVICE_ADMIN`, `METALAKE_USER`. |
| Ownership | `core/.../authorization/OwnerManager.java`, `OwnerDispatcher`, `owner_meta` table | One owner (user or group) per object. Created by hook dispatchers. |
| Grants | `PermissionManager.java`, `RoleManager.java`, tables `role_meta`, `role_meta_securable_object`, `user_role_rel`, `group_role_rel` | Role holds a list of (object id, type, privilege names CSV, conditions CSV). Roles bind to users and groups. |
| Hooks | `core/.../hook/*HookDispatcher.java` (15 classes) | After create: set creator as owner. After drop/rename: call plugin `onMetadataUpdated(remove/rename)`. Catalog create: `FutureGrantManager` replays metalake-level grants into the new plugin. |
| External push-down SPI | `core/.../connector/authorization/{Role,UserGroup,Metadata}AuthorizationPlugin.java`, `BaseCatalog.initAuthorizationPluginInstance` | One plugin per catalog (`authorization-provider` catalog property). Callbacks: `onRoleCreated/Updated/Deleted`, `onGrantedRolesToUser/Group`, `onOwnerSet`, `onMetadataUpdated`, `onUser/GroupAdded`. All return `Boolean`. |
| Ranger plugins | `authorizations/authorization-ranger/` | `HadoopSQL` service (schema/table/column) and `HDFS` service (paths). Role = Ranger role; owner = policy item. |
| JDBC plugin | `authorizations/authorization-common/.../jdbc/` | `GRANT` SQL to MySQL/PG-style databases. |
| Chain plugin | `authorizations/authorization-chain/.../ChainedAuthorizationPlugin.java` | Fans each callback to N plugins in order, e.g. Hive SQL service plus HDFS path service. |
| Tag-based access | `design-docs/tag-based-access-control.md`, `core/.../authorization/TagPolicyMatcher.java`, `api/.../policy/AccessControlContent` | Draft. Policy of type `system_access_control` bound to a tag confers allow-only privileges to listed roles. |
| Role assumption | `design-docs/gravitino-role-assumption.md`, header `X-Gravitino-Active-Roles` | Subtractive narrowing of the role set per request. Implemented in `JcasbinAuthorizer` (`narrowByActiveRoles`). |
| Read restrictions | `design-docs/tag-based-read-restrictions-for-iceberg-rest.md` | Row filter and column mask resolved from tag policies into Iceberg REST `read-restrictions`; client must declare `X-Gravitino-Client-Capabilities`. |

### 1.3 Privilege model summary

- Securable hierarchy: Metalake > Catalog > Schema (nestable via `A:B:C`) > Table | View | Topic | Fileset | Model | Function | Semantic Model. Domain, Tag, Policy, Job Template, Role, Job are metalake-level. Columns are not securable (`docs/security/access-control.md` "Columns do not appear at all").
- Privilege applicability is declared per privilege (`canBindTo`, e.g. `Privileges.java:107` `MANAGE_GRANTS_SUPPORTED_TYPES`). A grant on an ancestor covers all descendants.
- Privileges are never granted to users directly, only to roles; roles go to users and groups.
- Ownership is separate from privileges: single owner, user or group, inherited by containment (owner of a catalog passes every `ANY(OWNER, METALAKE, CATALOG)` clause; `JcasbinAuthorizer.java:487` schema ownership also propagates down nested schemas).
- Deny: `Privilege.Condition.DENY` wins over any allow at any level, in any role (`docs/security/access-control.md:171`). Implemented as a `!ANY(DENY_x, <all scopes>)` conjunct inside each `ANY_x` macro (`AuthorizationExpressionConverter.java:224-240`).
- Traversal privileges `USE_CATALOG` and `USE_SCHEMA` are separate from data privileges; `SELECT_TABLE` on a schema alone authorizes nothing (`docs/security/access-control.md:161`).
- Superuser: `gravitino.authorization.serviceAdmins` list in config (`Configs.java:480`); can only create metalakes. Not a role.
- Group membership comes from the identity-provider token of the current request (`JcasbinAuthorizer.java:1371`), not from a stored member list.

### 1.4 Extension points and config keys that matter

`gravitino.authorization.enable` (default false), `.impl`, `.serviceAdmins`, `.threadPoolSize` (default 100), `.jcasbin.cacheExpirationSecs` (3600), `.jcasbin.roleCacheSize` (10000), `.ownerCacheSize` / `.metadataIdCacheSize` (100000), `.jcasbin.changePollIntervalSecs` (3). Catalog props: `authorization-provider` (`ranger`, `chain`, jdbc), `authorization.ranger.*`, `authorization.chain.plugins`. Request headers: `X-Gravitino-Active-Roles`, `X-Gravitino-Client-Capabilities`.

## 2. Candidate requirements

Kind: IMPL = Gravitino does this; GAP = Gravitino fails or omits and mudcat should do better. Area codes follow the brief (AC access control, UP usage policy, RD data access, IN interfaces, PF performance, OP operation, QY queries).

### 2.1 Privilege model and hierarchy

| Ref | Kind | Requirement | Rationale / Gravitino evidence | Area | Ver |
|---|---|---|---|---|---|
| AZ-R1 | IMPL | mudcat shall evaluate a permission on an object against the grants held on that object and on each ancestor in its containment path. | Inheritance is the core model: every `ANY_x` macro lists METALAKE, CATALOG, SCHEMA, TABLE (`AuthorizationExpressionConverter.java:240-300`). Maps to a Zanzibar `parent` relation. | AC | T |
| AZ-R2 | GAP | mudcat shall define each permission, the object types it applies to and its ancestor scopes in one table, not in per-endpoint expression text. | Adding one privilege touches: enum bit (`Privilege.java:189`), a `GenericPrivilege` class and a type set (`Privileges.java:1878`), a regex macro (`AuthorizationExpressionConverter.java:224-402`), per-endpoint constants (`AuthorizationExpressionConstants.java`, 425 lines), the list short-circuit registry (`MetadataAuthzHelper.java:107-190`), `CAN_ACCESS_METADATA` branches, Ranger mapping, docs. `LOAD_TABLE`, `LOAD_VIEW`, `LOAD_FILESET`, `LOAD_TOPIC` repeat the same shape by hand. | AC | I |
| AZ-R3 | IMPL | mudcat shall reject a grant that binds a permission to an object type to which the permission does not apply. | `canBindTo` per privilege; docs: "Binding a privilege to a type not listed for it is rejected" (`docs/security/access-control.md:182`). Also catalog-type check, e.g. a table privilege on a messaging catalog (`AuthorizationUtils.java:345-370`). | AC | T |
| AZ-R4 | IMPL | mudcat shall let an explicit deny of a permission at any level of the path override every allow of the same permission at any level, in any role. | `docs/security/access-control.md:171`; `JcasbinAuthorizer.java:1125` (inactive roles still deny). Test `testDenyRoleFromGroupOverridesAllowOnUser`. | AC | T |
| AZ-R5 | GAP | mudcat shall state, in one rule, whether ownership of an object or of an ancestor overrides a deny, and apply that rule to every operation. | Owner clauses sit outside the deny conjunct, so an ancestor owner bypasses deny (`ANY(OWNER, METALAKE, CATALOG) \|\| ...`, `AuthorizationExpressionConstants.java:55-62`). A leaf owner does not bypass a traversal deny (`testHasSetOwnerPermissionRejectsDenyUseCatalogForTableOwner`, `TestJcasbinAuthorizer.java:6320`) but a catalog owner does (`:6396`). `MANAGE_GRANTS` deny is bypassed by owner (`:6269`). Rule is implicit and spread over three code paths. | AC | T |
| AZ-R6 | GAP | mudcat shall reject, at write time, a role that carries both allow and deny for the same permission on the same object, or define the result in the specification. | `Privilege.java:220` says DENY takes effect when both exist; `AuthorizationUtils.checkDuplicatedNamePrivilege` (`:293`) throws "Doesn't support duplicated privilege name %s with different condition". Doc and code disagree. | AC | T |
| AZ-R7 | IMPL | mudcat shall store the canonical permission name when a client submits a deprecated alias, and evaluate both names identically. | `CREATE_MODEL` and `REGISTER_MODEL` share a bit (`Privilege.java:109`); `replaceLegacyPrivilegeName` in `AuthorizationUtils.java:301`. Without canonicalisation bit values stop identifying names (Javadoc `Privilege.java:100-118`). | AC | T |
| AZ-R8 | GAP | mudcat shall store each grant as one row per (subject, object ID, permission, effect). | Gravitino stores `privilege_names VARCHAR(81920)` and `privilege_conditions VARCHAR(81920)` as comma lists per (role, object) (`schema-2.0.0-postgresql.sql:344-345`). It cannot answer "who holds P on O" in SQL; the engine loads whole roles into memory. The `highBits/lowBits` bitmask in `Privilege.Name` is never read (grep `getLowBits` finds only the definition). | AC | I |

### 2.2 Ownership

| Ref | Kind | Requirement | Rationale / Gravitino evidence | Area | Ver |
|---|---|---|---|---|---|
| AZ-R9 | IMPL | mudcat shall let a user or a group own an object, and let the current owner or a higher-level owner transfer ownership. | `OwnerManager.setOwner` (`:67`); `hasSetOwnerPermission` (`JcasbinAuthorizer.java:734`). | AC | T |
| AZ-R10 | GAP | mudcat shall enforce at most one owner per object with a database constraint. | `owner_meta` unique key is `(owner_id, metadata_object_id, type, deleted_at)` (`schema-2.0.0:576`), which allows two owners of one object. `getOwner` throws `IllegalStateException` "The size of the owner's name %s must be 1" (`OwnerManager.java:303`). | AC | T |
| AZ-R11 | GAP | mudcat shall set the creator as owner in the same transaction that creates the object. | Hooks create first, then set owner: `TableHookDispatcher.java:118-128` ("Table was dropped before its owner could be set"), `AccessControlHookDispatcher.java:220` (role), `SchemaHookDispatcher.java:87` (needs a catalog write lock to serialise probe, create and owner). A crash between the two leaves an ownerless object that only an ancestor owner can reach. | AC | T |
| AZ-R12 | GAP | mudcat shall refuse to create a tenant without an owner. | Metalakes made while authorization was off have no owner and become unusable when it is turned on (`docs/security/access-control.md:620`); the fix is a pass-through authorizer and a restart (`:625-655`). | OP | T |
| AZ-R13 | IMPL | mudcat shall confine the platform-operator list to server configuration and limit it to creating tenants. | `serviceAdmins` list, "The list adds exactly one ability, creating a metalake" (`docs/security/access-control.md:62`); `AccessControlManager.isServiceAdmin` is a list lookup. | AC | I |

### 2.3 Identity binding and lifecycle

| Ref | Kind | Requirement | Rationale / Gravitino evidence | Area | Ver |
|---|---|---|---|---|---|
| AZ-R14 | IMPL | mudcat shall reference objects, roles, users and groups in grants and ownership by ID, not by name. | `role_meta_securable_object.metadata_object_id`, `owner_meta.owner_id`. Test `testRecreatedMetalakeDoesNotInheritOldOwnershipOrPermissions` (`TestJcasbinAuthorizer.java:5754`), `testRecreatedGroupWithSameNameDoesNotReuseOldRoleCache` (`:4323`). A dropped-and-recreated name gets no old grants. | AC | T |
| AZ-R15 | IMPL | mudcat shall delete all grants, ownership and tag links of an object in the transaction that deletes the object. | `SchemaMetaService.java:382-392` soft-deletes `OwnerMetaMapper`, `SecurableObjectMapper`, `TagMetadataObjectRelMapper` rows with the schema rows; same for catalog, metalake, user, group. | AC | T |
| AZ-R16 | GAP | mudcat shall authorise on object IDs resolved once per request, not on names normalised by a connector. | `MetadataIdConverter.normalizeMetadataObject` calls the catalog's `Capability` (case rules) before every name-to-id lookup (`MetadataIdConverter.java:128`). A failing connector makes the check "unresolved" and forces the deny-guard path (`JcasbinAuthorizationLookups.java`, `normalizationFailed`). Authorization depends on connector availability. | AC | I |
| AZ-R17 | GAP | mudcat shall carry an object path as an ordered list of components in authorization, never as a dot-joined string. | Names containing `.` cannot be authorised; one such name in a list rejects the whole list with 400 (`docs/security/access-control.md:76-87`; `TestMetadataAuthzHelper.java:132` `testFilterRejectsDottedExternalObjectName`). | AC | T |

### 2.4 Groups, roles, role assumption

| Ref | Kind | Requirement | Rationale / Gravitino evidence | Area | Ver |
|---|---|---|---|---|---|
| AZ-R18 | GAP | mudcat shall derive group membership from the credential of the current request and shall not publish derived membership as state shared between requests. | Early design put `g(user, role)` rows in the shared enforcer; two interleaved requests with different IdP group claims injected an allow or removed a deny. Now the model comment says "No shared user/group membership is consulted" (`jcasbin_model.conf:18-19`) and roles are unioned per request (`JcasbinAuthorizer.java:1114`). Tests `testInterleavedGroupRequestsMustNotInjectAllow` (`TestJcasbinAuthorizer.java:4847`), `...MustNotRemoveDeny` (`:4893`), `...MustNotExposeDeniedTable` (`:5246`). | AC | T |
| AZ-R19 | IMPL | mudcat shall let a request declare a subset of its caller's roles as active and shall evaluate allows against that subset only. | Role assumption design (`design-docs/gravitino-role-assumption.md` section 3-4); `narrowByActiveRoles` (`JcasbinAuthorizer.java:974`). Gives a blast-radius limit for pipelines and agents. | AC | T |
| AZ-R20 | IMPL | mudcat shall keep every deny of the caller's roles in force when it narrows the active roles. | `JcasbinAuthorizer.java:1125` "Inactive roles still contribute DENY policies"; `docs/security/access-control.md:495`. | AC | T |
| AZ-R21 | IMPL | mudcat shall reject a request that declares an active role the caller does not hold, with the same response whether the role exists or not. | 403 for both (`design-docs/gravitino-role-assumption.md` 3.4; `GravitinoInterceptionService.java:376`). Prevents role-name probing. | AC | T |
| AZ-R22 | IMPL | mudcat shall apply the active-role set to list results and to the access level of each credential it issues. | "Every decision in the request is narrowed" (`docs/security/access-control.md:497-501`): narrowed caller is vended a read-only credential. | RD | T |
| AZ-R23 | GAP | mudcat shall state whether access derived from ownership is narrowed by the active-role set. | Gravitino left it open: Option A (ownership always applies, shipped), B, C (`design-docs/gravitino-role-assumption.md` section 5). Owner keeps access under `NONE` (`docs/security/access-control.md:496`). A least-privilege agent run by an owner is not narrowed. | AC | A |

### 2.5 List and filter

| Ref | Kind | Requirement | Rationale / Gravitino evidence | Area | Ver |
|---|---|---|---|---|---|
| AZ-R24 | IMPL | mudcat shall return from a list request only the entries the caller may see, and shall not fail the request for entries the caller may not see. | `docs/security/access-control.md:164-166`; `MetadataAuthzHelper.filterByExpression`. | AC | T |
| AZ-R25 | GAP | mudcat shall authorise a list of N entries in a number of store round trips that does not depend on N. | Default path is one OGNL evaluation per entity on a pool of 100 threads (`MetadataAuthzHelper.java:625`, `Configs.java:106`). Gravitino added three mitigations: per-request memo, a batch entity preload (`preloadToCache`, `:690`), and a parent-scope short-circuit (`:368`). Tests count queries "independently of list size" (`TestPrincipalListQueryCount.java:70`). | PF | T |
| AZ-R26 | GAP | mudcat shall define its authorization port with a batch check that takes at least TBD (object, permission) pairs and a lookup that returns the objects of a type on which a subject holds a permission. | Gravitino's SPI is single-object (`GravitinoAuthorizer.authorize`); batching is rebuilt on top with threads. SpiceDB and OpenFGA provide BatchCheck and LookupResources. | IN | I |
| AZ-R27 | IMPL | mudcat shall not return an unfiltered list on the strength of a parent-level grant if a deny can apply to any listed entry. | `allVisibleViaParentScope` asks `hasDenyPolicy` for the privileges of each access path and falls back to per-object checks (`MetadataAuthzHelper.java:368-440`). The SPI default for an engine that cannot answer is `true` (`GravitinoAuthorizer.java:153`). | AC | T |
| AZ-R28 | GAP | When the authorization engine fails while it filters a list, mudcat shall fail the request and shall not drop the entry. | `doFilter` catches every exception, logs, and returns `null` for that entry (`MetadataAuthzHelper.java:643`). An engine outage looks like an empty list. | AC | T |

### 2.6 Consistency, caching, failure

| Ref | Kind | Requirement | Rationale / Gravitino evidence | Area | Ver |
|---|---|---|---|---|---|
| AZ-R29 | GAP | mudcat shall evaluate each decision against one consistent snapshot of grants and shall make a revocation visible to every decision that starts after the revoking commit on any node. | Role, user and group bindings are version-validated per request (`updated_at` probe), but name-to-id and owner caches are eventually consistent with a 3 s poll and 3600 s TTL (`JcasbinAuthorizer.java:98-122`, `Configs.java:430,469`). A stale owner entry on a peer node keeps an ex-owner authorised for up to one poll interval (inferred from the cache contract). | AC | T |
| AZ-R30 | GAP | mudcat shall return a revision token with each write to the authorization store and shall accept a minimum revision on a check. | Gravitino has none; per-request memo "must not reuse decisions made before" a mutation (`AuthorizationRequestContext.java` class Javadoc) and `AuthorizationRequestScope.bindIfRead` limits reuse to GET (`AuthorizationRequestScope.java:73`). Zanzibar calls this a zookie. | AC | T |
| AZ-R31 | GAP | mudcat shall evaluate a check without loading all grants of the caller's roles into process memory. | The in-memory mirror needs: generation counters, a policy write lock, partial-load backoff of 10 s (`:161`), a reload limit of 3 (`:168`), single-flight loading with no waiter timeout (`:1653`), "incomplete role" markers. 107 tests in `TestJcasbinAuthorizer.java` are about eviction and reload races. | PF | A |
| AZ-R32 | IMPL | When mudcat cannot determine a decision, mudcat shall deny the request, and shall treat an unreadable deny as applying. | `unstablePolicyResult`: false for allow, true for deny (`JcasbinAuthorizer.java:986`). Unresolved DENY object installs a catalog-wide deny guard (`:2206`). Unknown error in the interceptor returns 500, no access (`GravitinoInterceptionService.java:333`). | AC | T |
| AZ-R33 | IMPL | mudcat shall define for each optional authorization-port operation the result an engine returns when it cannot answer, and the result shall be the more restrictive. | `hasDenyPolicy` default `true`; `findUnheldRoles` default empty; `handle*` callbacks default no-op (`GravitinoAuthorizer.java`). | IN | I |
| AZ-R34 | GAP | mudcat shall keep cache-invalidation callbacks out of the authorization port. | `GravitinoAuthorizer` mixes decisions with `handleRolePrivilegeChange`, `handleMetadataOwnerChange`, `handleEntityNameIdMappingChange`. A Zanzibar backend needs none of them. | IN | I |

### 2.7 External enforcement points (Ranger, JDBC, chain)

| Ref | Kind | Requirement | Rationale / Gravitino evidence | Area | Ver |
|---|---|---|---|---|---|
| AZ-R35 | GAP | mudcat shall write a change for an external enforcement point to an outbox in the transaction that commits the grant, and shall retry delivery until it succeeds or a person cancels it. | Grants commit first, then the plugin is called in the request thread (`RoleManager.java:81-88`, `PermissionManager.java:462`). A plugin exception becomes an HTTP error with the grant already committed. Role delete runs the opposite order: plugin first, then store delete (`RoleManager.java:111-130`). | PL | T |
| AZ-R36 | GAP | mudcat shall record, for each grant and each external enforcement point, the state of the last push (pending, applied, failed) and shall expose it. | Plugin callbacks return `Boolean`, and the lambdas discard it (`AuthorizationUtils.java:580-593`, `PermissionManager.java:129`). `RangerAuthorizationPlugin.java:552,563` swallow grant/revoke-role failures "to support idempotent operation". | PL | T |
| AZ-R37 | GAP | mudcat shall provide an operation that compares the grants of the catalog with the policy state of each external enforcement point and reports every difference. | No reconcile code exists; the only repair path is a manual re-set of the owner to re-create Ranger roles (`docs/security/authorization-pushdown.md` "Migrate Owner Roles from 1.x to 2.0"). Ranger policies are editable by hand. | OP | D |
| AZ-R38 | GAP | mudcat shall reject a grant that an external enforcement point cannot enforce, and shall list the permissions each enforcement point can enforce. | `translatePrivilege` returns without a trace for unmapped privileges ("Ignore unsupported privileges", `RangerAuthorizationHDFSPlugin.java:410`; `RangerAuthorizationHadoopSQLPlugin.java:560`); `CREATE_CATALOG` ignored (`:562`); `CREATE_TABLE` on a table throws only there (`:670`). The role holds a privilege that nothing enforces. | AC | T |
| AZ-R39 | GAP | mudcat shall map one permission to at most one external action, and shall not map a navigation permission to a data-read action. | `USE_CATALOG` and `USE_SCHEMA` map to Ranger `SELECT` (`RangerAuthorizationHadoopSQLPlugin.java:73-78`), so a catalog-level `USE_CATALOG` grants SQL SELECT on `*` schemas in Ranger while Gravitino semantics say it only permits traversal. `MODIFY_TABLE` maps to five Ranger actions. | AC | I |
| AZ-R40 | IMPL | mudcat shall qualify the name of each role or policy item that it creates in an external system with the tenant ID and the object ID. | 1.x used shared roles `GRAVITINO_METALAKE_OWNER_ROLE`; two catalogs on one Ranger service leaked owner rights; 2.0 suffixes with the entity ID and needs a manual migration (`docs/security/authorization-pushdown.md`; `RangerAuthorizationPlugin.java` `metalakeId`, `catalogId`). | PL | I |
| AZ-R41 | GAP | When one change goes to several external enforcement points, mudcat shall report which points applied it and which did not. | `ChainedAuthorizationPlugin.chainedAction` returns `false` at the first `false` and skips the rest; an exception from plugin 2 leaves plugin 1 applied; no rollback (`ChainedAuthorizationPlugin.java:192-199`). Table grant and path grant can disagree. | PL | T |
| AZ-R42 | GAP | mudcat shall state the propagation delay bound of each external enforcement point and shall show it in the grant state. | Ranger plugins in engines poll admin (`RangerBaseE2EIT.java:193`); tests sleep 1 s and force a refresh. Gravitino documents no bound. | OP | A |
| AZ-R43 | IMPL | When an object that has external grants is dropped or renamed, mudcat shall give the plugin the storage locations that it captured before the change. | Hooks read locations first, then call the dispatcher, then the plugin (`TableHookDispatcher.java:98-103,174-184`); a `false` drop result keeps the grants. | PL | T |
| AZ-R44 | IMPL | mudcat shall replay the grants held at a higher level onto a new child enforcement point when it is registered. | `FutureGrantManager.grantNewlyCreatedCatalog` (`:42`). Metalake-level grants fan out to every catalog (`AuthorizationUtils.java:231-258`). | PL | T |
| AZ-R45 | IMPL | When a catalog has an authorization provider configured but no live plugin, mudcat shall fail the operation and shall not treat the catalog as unprotected. | `BaseCatalog.getAuthorizationPlugin` throws (`BaseCatalog.java:363-375`): "Silently treating that state as authorization being disabled could leave stale grants". | OP | T |

### 2.8 Tags, usage policy, credentials, audit

| Ref | Kind | Requirement | Rationale / Gravitino evidence | Area | Ver |
|---|---|---|---|---|---|
| AZ-R46 | IMPL | mudcat shall let a tag, bound to a rule, confer a listed set of allow permissions on the objects carrying the tag, including by inheritance, to callers who hold a listed role. | `design-docs/tag-based-access-control.md` (Summary, Evaluation); `TagPolicyMatcher.match`. Draft in Gravitino. Avoids per-object grants for "certified" or "pii". | AC | T |
| AZ-R47 | IMPL | mudcat shall limit the permissions that a tag can confer to data permissions on the tagged object, and shall exclude traversal and administrative permissions. | Allowlist, fails closed when a new privilege is added (`tag-based-access-control.md` "Permitted privileges"); `MANAGE_GRANTS` via tag would let any role in the rule grant beneath the catalog "after the applier's own authority was revoked". | AC | T |
| AZ-R48 | GAP | mudcat shall let a tag, a policy or a legal-hold record impose a restriction that no grant can override. | Gravitino tag access control is allow-only: "A deny that a descendant tag cannot undo is a separate problem" (`tag-based-access-control.md` "Not in this version", row `DENY`). Read-restriction design only narrows rows and columns after a table-level allow (`tag-based-read-restrictions...md` Non-Goals 1). Embargo, consent and takedown need a restrictive effect. | UP | T |
| AZ-R49 | GAP | mudcat shall read the effective tags of an object at most once per decision, and shall not read them once per level of the ancestor walk. | Design records the quadratic cost if the tag check sits inside the per-level `authorize` (`tag-based-access-control.md:309`); Gravitino reads tag state on every request, no cache (`:393`). | PF | T |
| AZ-R50 | GAP | mudcat shall record each decision that allows access to a protected object, with the grant path (role, tag, owner, ancestor) that allowed it. | Only `AuthorizationDenialFailureEvent` exists (`GravitinoInterceptionService.java:304,389,428`). The tag design lists "Audit of tag-conferred access" as not done (`tag-based-access-control.md:107`). `diagnoseDenial` logs at debug only (`JcasbinAuthorizer.java:2284`). | AC | T |
| AZ-R51 | GAP | mudcat shall answer, for an object, which subjects hold a given permission and by which path. | No endpoint does it. `listRoleNamesByObject` returns only roles bound directly to the object (`RoleManager.java:147`); ancestors, owners, group claims and tags are not merged. | QY | D |
| AZ-R52 | IMPL | mudcat shall return a not-found response to a caller that may not know an object exists, and shall use one permission for existence probes. | `PROBE_TABLE_LIKE` (`Privilege.java`, docs); `allowCheckExistence` in `AuthorizationExpression.java`; Iceberg probes (`AuthorizationExpressionConstants.java:77` "Spark calls tableExists before creating a table"). | IN | T |
| AZ-R53 | IMPL | mudcat shall decide whether a credential grants read or write from the permissions the caller holds on the object at issue time. | `tag-based-access-control.md` "Credential vending": Iceberg REST uses `ANY_MODIFY_TABLE` to choose; generic endpoint uses `ANY_WRITE_FILESET`. | RD | T |
| AZ-R54 | IMPL | mudcat shall omit long-lived static secrets from a response unless the caller holds a separate explicit permission for them, and shall not fail the request for that omission. | `USE_SECRETS` and `INCLUDE_CREDENTIAL_SECRETS` soft-filter (`Privilege.java:171`). | RD | T |
| AZ-R55 | GAP | mudcat shall return required read restrictions only to a reader that declared the capability to enforce them, and shall otherwise refuse the read. | Gravitino requires `X-Gravitino-Client-Capabilities: read-restrictions.v1`; non-goal 5: "Guaranteeing that a reader without Iceberg read-restriction support can safely read governed tables" is not met. Default off. | IN | T |

## 3. Quirks and gotchas ledger

- **Grants are committed before the external enforcement point is told.**
  - What happens: `PermissionManager`/`RoleManager` write the role to the store, then call the Ranger/JDBC plugin. If the plugin throws, the caller gets an error and the DB keeps the grant. Role deletion does the reverse (plugin first, then store).
  - Cause: OTHER (no distributed transaction, no outbox).
  - Evidence: `core/.../RoleManager.java:81` `store.put(roleEntity, false)` then plugin; `:117-124` plugin then `store.delete`; `PermissionManager.java:462` `authorizationPluginCallbackWrapper.execute()` after `store.update`.
  - Mudcat implication: AZ-R35, AZ-R36.

- **Plugin return value is discarded.**
  - What happens: every plugin method returns `Boolean` (false = "not applied"), but the call sites are `BiConsumer` lambdas that ignore it. Ranger `validAuthorizationOperation` returning false silently skips the whole operation.
  - Cause: OTHER.
  - Evidence: `AuthorizationUtils.java:580-593` `consumer.accept(authorizationPlugin, ...)`; `RangerAuthorizationPlugin.java:1073-1090`.
  - Mudcat implication: AZ-R36, AZ-R38.

- **Ranger owner-role failures are swallowed.**
  - What happens: granting or revoking the owner role in Ranger logs a warning and continues, "to support idempotent operation". A Ranger outage during `onOwnerSet` for a catalog leaves Gravitino saying the user is owner and Ranger saying nobody is.
  - Cause: OTHER.
  - Evidence: `RangerAuthorizationPlugin.java:550-565` `// Ignore exception, support idempotent operation`.
  - Mudcat implication: AZ-R36, AZ-R37.

- **Unmapped privileges silently do nothing in Ranger.**
  - What happens: privileges outside `privilegesMappingRule()` (everything except catalog/schema/table privileges for SQL; no topic, view, model, function privileges) are skipped without error. The Gravitino role shows the privilege; Ranger enforces nothing.
  - Cause: MODEL (impedance mismatch).
  - Evidence: `RangerAuthorizationHadoopSQLPlugin.java:560-565`; `RangerAuthorizationHDFSPlugin.java:410` `// Ignore unsupported privileges`.
  - Mudcat implication: AZ-R38.

- **`USE_CATALOG` and `USE_SCHEMA` become `SELECT` in Ranger.**
  - What happens: a navigation privilege is pushed as a SQL read. `USE_CATALOG` on a catalog adds `SELECT` on schema `*`. Meaning differs between the native path and the pushed path.
  - Cause: MODEL.
  - Evidence: `RangerAuthorizationHadoopSQLPlugin.java:73-78`.
  - Mudcat implication: AZ-R39.

- **One Ranger policy per resource; read-modify-write without a lock.**
  - What happens: Gravitino folds many roles into one Ranger policy per resource and rewrites the whole policy on each change. More than one policy for a resource is an error. No lock or version check is visible, so concurrent grants on the same table can lose an update `(inferred)`.
  - Cause: STORE (Ranger model) plus CONCURRENCY.
  - Evidence: `RangerAuthorizationPlugin.java:230` `"Each metadata object can have at most one policy."`; `doAddSecurableObject` `:858-920` finds, mutates, `updatePolicy`. No `synchronized` in `RangerAuthorizationPlugin.java` or `RangerHelper.java` (grep).
  - Mudcat implication: AZ-R35, AZ-R37.

- **Ranger rename is rewriting policy names and resources by hand.**
  - What happens: renaming a schema rewrites `{schema}`, `{schema}.*`, `{schema}.*.*` policies; a table move to another schema needed a special case; if the target policy exists the rename logs "already exists" and skips.
  - Cause: MODEL (Ranger policies are name-keyed; Gravitino grants are ID-keyed).
  - Evidence: `RangerAuthorizationHadoopSQLPlugin.java:120-200`, `:215-223`.
  - Mudcat implication: AZ-R14, AZ-R40 (do not key external policies by name where an ID can be used).

- **Shared Ranger owner roles leaked rights across catalogs.**
  - What happens: 1.x shared role `GRAVITINO_CATALOG_OWNER_ROLE` across catalogs on one Ranger service; membership gave access outside the owned object. 2.0 renamed to `_<id>` and needs the operator to re-set every owner.
  - Cause: COMPAT / SECURITY.
  - Evidence: `docs/security/authorization-pushdown.md` section "Migrate Owner Roles from 1.x to 2.0"; `RangerHelper.java:432` legacy role removal.
  - Mudcat implication: AZ-R40.

- **Chain of plugins stops at the first `false`, no rollback.**
  - What happens: Hive SQL plugin applied, HDFS plugin failed: the table grant exists and the path grant does not (or the reverse on revoke). `onRoleDeleted` in the chain first runs `onRoleUpdated(remove all)` then each plugin's delete.
  - Cause: OTHER.
  - Evidence: `ChainedAuthorizationPlugin.java:192-199`, `:117-124`.
  - Mudcat implication: AZ-R41.

- **Engines see Ranger changes after a poll, not at commit.**
  - What happens: integration tests force a policy refresh and then sleep 1 s because the engine-side Ranger plugin polls the admin.
  - Cause: ENGINE.
  - Evidence: `RangerBaseE2EIT.java:193-205` "a poll can come later than the sleep below".
  - Mudcat implication: AZ-R42.

- **Group membership in the shared enforcer was a real vulnerability.**
  - What happens: mapping user to role in shared Casbin state let concurrent requests with different token group claims see each other's roles. Fixed by per-request role union and subject = role id.
  - Cause: CONCURRENCY / SECURITY.
  - Evidence: `jcasbin_model.conf:18-19`; `JcasbinAuthorizer.java:1114-1115` "Shared g(user, role) rows cannot represent concurrent requests carrying different IdP group claims"; test `:4847`.
  - Mudcat implication: AZ-R18.

- **In-memory policy mirror needs a consistency protocol.**
  - What happens: eviction of a role's rows mid-request could turn an allow into a deny or, worse, drop a deny. Fixes: clear-generation counters, reload up to 3 times then fail closed, partial-load marker, 10 s retry backoff, deny existence scan for lists, single-flight loads with no timeout (one stuck loader delays all waiters).
  - Cause: CONCURRENCY.
  - Evidence: `JcasbinAuthorizer.java:161,168,1653-1655`; tests `testDenyPoliciesEvictedMidRequestStillDeny` (`:3100`), `testReloadReadFailureMustNotLoseDeny` (`:3128`).
  - Mudcat implication: AZ-R31, AZ-R29.

- **Unresolvable DENY object becomes a catalog-wide deny.**
  - What happens: if a role's deny names an object that cannot be resolved to an ID (dropped table still referenced, connector error), the engine installs the deny on the whole catalog until the next retry. The role is "partially loaded" and re-probed every 10 s.
  - Cause: OTHER (fail-closed design).
  - Evidence: `JcasbinAuthorizer.java:2206-2225`; `PARTIAL_ROLE_LOAD_RETRY_MS` `:161` comment "A role can stay unresolvable indefinitely — for example when it still references a dropped table".
  - Mudcat implication: AZ-R32, AZ-R14 (role rows can point at objects that no longer resolve; ID-keyed rows plus cascade delete avoid it).

- **Name-to-ID caches need an entity change log, and the log must store the pre-mutation name.**
  - What happens: peers invalidate cached name-to-id entries by polling `entity_change_log`. If a writer logs the post-rename name, invalidation silently misses until LRU eviction. Failed invalidation clears the whole cache.
  - Cause: CONCURRENCY.
  - Evidence: `JcasbinChangeListener.java:226-232` "entity_change_log.full_name must be the pre-mutation name"; `:364,377` `invalidateAll()` on failure; schema-2.0.0:1190.
  - Mudcat implication: AZ-R29, AZ-R16 (resolve once to an ID so there is no name cache).

- **Authorization depends on connector name rules.**
  - What happens: before every ID lookup the object name is normalised by the catalog `Capability` (case folding). A failing connector marks the lookup "normalization failed", which is treated differently for allow (false) and deny (true).
  - Cause: MODEL.
  - Evidence: `MetadataIdConverter.java:120-150`; `JcasbinAuthorizer.java:1081-1085`.
  - Mudcat implication: AZ-R16.

- **Dots in names break authorization.**
  - What happens: full names are dot-joined; an external table named `a.b` cannot be authorised and a list containing one returns 400 for all entries.
  - Cause: MODEL.
  - Evidence: `docs/security/access-control.md:76-87`; `TestMetadataAuthzHelper.java:132`.
  - Mudcat implication: AZ-R17.

- **Owner is set after the object exists.**
  - What happens: create commits, then `setOwner` runs by name. A concurrent drop yields `NoSuchMetadataObjectException`, logged and swallowed; a crash leaves an ownerless object. `setOwner` has a heuristic to tell "object gone" from "owner gone".
  - Cause: CONCURRENCY.
  - Evidence: `TableHookDispatcher.java:118-128` ("Identity-bound initialization and its transfer/drop-recreate races are outside this EXIST_OK fix"); `OwnerManager.java:115` "This owner-exists check is a heuristic".
  - Mudcat implication: AZ-R11.

- **Hierarchical schema creation needs a catalog write lock to assign ownership.**
  - What happens: creating `A:B:C` auto-creates `A` and `A:B`; ownership is assigned only to ancestors this request created, after a probe, under a catalog WRITE tree lock, one `schemaExists` per level.
  - Cause: CONCURRENCY.
  - Evidence: `SchemaHookDispatcher.java:87-130`, `:140-150`.
  - Mudcat implication: AZ-R11 (single transaction removes the lock).

- **`setOwners` swallows plugin errors per object; `setOwner` does not.**
  - What happens: batch path logs and continues; single path throws after the DB write. Two behaviours for one operation.
  - Cause: OTHER.
  - Evidence: `OwnerManager.java:190-208` vs `:105-120`.
  - Mudcat implication: AZ-R36.

- **Owner and deny precedence differ by path.**
  - What happens: ancestor owner bypasses deny; a table owner is stopped by deny `USE_CATALOG` when setting owner; owner bypasses deny `MANAGE_GRANTS`.
  - Cause: OTHER.
  - Evidence: `TestJcasbinAuthorizer.java:6269,6320,6396`.
  - Mudcat implication: AZ-R5.

- **ALLOW+DENY in one role: doc and code disagree.**
  - What happens: `Privilege.java:220` says deny takes effect; `checkDuplicatedNamePrivilege` rejects the pair.
  - Cause: OTHER.
  - Evidence: `AuthorizationUtils.java:293`.
  - Mudcat implication: AZ-R6.

- **Inheritance is hardcoded in regex macros.**
  - What happens: `ANY_SELECT_TABLE` is textually replaced with `ANY(SELECT_TABLE, METALAKE, CATALOG, SCHEMA, TABLE) && !ANY(DENY_SELECT_TABLE, ...)`. A privilege without a macro silently falls through as an unexpanded token and throws at OGNL parse time. Macros use `String.replaceAll`, so a name that is a prefix of another would corrupt it `(inferred; none found today)`.
  - Cause: OTHER.
  - Evidence: `AuthorizationExpressionConverter.java:224-402`.
  - Mudcat implication: AZ-R2.

- **Each endpoint carries its own copy of the traversal rule.**
  - What happens: `LOAD_TABLE`, `CREATE_TABLE`, `MODIFY_TABLE`, `DROP_TABLE`, view and fileset variants repeat `ANY(OWNER, METALAKE, CATALOG) || SCHEMA_OWNER_WITH_USE_CATALOG || ANY_USE_CATALOG && ANY_USE_SCHEMA && ...`. Iceberg variants differ by one privilege (`ANY_CREATE_TABLE` added because Spark calls `tableExists` before create).
  - Cause: ENGINE.
  - Evidence: `AuthorizationExpressionConstants.java:55-100`.
  - Mudcat implication: AZ-R2, AZ-R52.

- **List short-circuit is a second copy of the policy.**
  - What happens: for speed, each list type has a hand-written registry of parent-scope paths and the deny privileges that can invalidate each path. It must be kept in step with the per-object expression.
  - Cause: OTHER (performance workaround).
  - Evidence: `MetadataAuthzHelper.java:107-190`; tests `testListShortCircuit*FallsBackWhenDenyMayExist`.
  - Mudcat implication: AZ-R25, AZ-R26.

- **List filter drops entries on any engine error.**
  - What happens: exception in the per-entity task is logged and the entry omitted.
  - Cause: OTHER.
  - Evidence: `MetadataAuthzHelper.java:643-646`.
  - Mudcat implication: AZ-R28.

- **Audit exists only for denials.**
  - What happens: `AuthorizationDenialFailureEvent` fires on deny; allow decisions leave no record; tag-conferred access would be invisible.
  - Cause: OTHER.
  - Evidence: `GravitinoInterceptionService.java:304`; `tag-based-access-control.md:107`.
  - Mudcat implication: AZ-R50.

- **Authorization failure returns 500, list returns empty.**
  - What happens: system error in the interceptor returns "Authorization failed due to system internal error", a different shape from 403/404.
  - Cause: OTHER.
  - Evidence: `GravitinoInterceptionService.java:333-338`.
  - Mudcat implication: AZ-R28, AZ-R32.

- **Pass-through authorizer auto-creates users.**
  - What happens: `PassThroughAuthorizer.isMetalakeUser` adds the caller as a user when absent; used as the bootstrap path for enabling authorization.
  - Cause: SECURITY.
  - Evidence: `PassThroughAuthorizer.java:95-103`; `docs/security/access-control.md:625-655`.
  - Mudcat implication: AZ-R12 (avoid a mode in which any caller is accepted).

- **Role recreated under the same name fails closed.**
  - What happens: a request that probed role version N then reads a role with a different ID treats the role as unreadable and denies everything it carried.
  - Cause: CONCURRENCY.
  - Evidence: `JcasbinAuthorizer.java:1750-1757`; `testRoleRecreatedUnderSameNameFailsClosed` (`:2619`).
  - Mudcat implication: AZ-R14.

- **Privilege bit mask is dead.**
  - What happens: `Privilege.Name` has `highBits/lowBits`, bit 43 used of 64, aliases share bits; nothing reads them.
  - Cause: COMPAT.
  - Evidence: `Privilege.java:189,204,213`; grep for `getLowBits` finds no caller.
  - Mudcat implication: AZ-R7, AZ-R8.

## 4. Open questions, inconsistencies, half-implemented

- **Docs ahead of code: user `externalId` and `enabled`.** `docs/security/access-control.md:58,372` describe `externalId` and an `enabled` flag that suspends a user. `api/.../authorization/User.java` has only `name`, `id`, `roles`; no `enabled` read in `server-common/.../authorization` or `core/.../authorization` (grep). Treat as unimplemented in this checkout `(inferred)`.
- **Tag-based access control is a draft.** `tag-based-access-control.md` "Status: draft for discussion"; only `TagPolicyMatcher` and per-request tag caches (`AuthorizationRequestContext.java`) exist. `DENY`, user/group conditions, scope, audit, dry-run are all listed out of scope. Open questions OQ-1 to OQ-4 (where tags are evaluated, tag allows vs RBAC deny, deleting a referenced role, who may apply a rule).
- **Ownership under role assumption is undecided** (`gravitino-role-assumption.md` section 5). Narrowing does not apply to catalogs with pushed-down authorization (Hive/JDBC via Ranger), which "never see the active-role declaration" (section 6). Role-assumption is a native-path feature only.
- **Function privileges: no push-down needed** because functions are fully managed (`gravitino-function-privilege.md` "Authorization Pushdown — Not Applicable"). Same applies to models, tags, policies in this system: the external plugins only cover tables, schemas, filesets.
- **Column-level control does not exist.** `docs/security/access-control.md` says columns have no controls; Ranger code has `COLUMN` resource types (`RangerHadoopSQLMetadataObject`) and design docs add tag-based column masks for Iceberg REST. Native column grants remain a gap.
- **Read restrictions need a trusted reader.** `tag-based-read-restrictions-for-iceberg-rest.md` Non-Goal 5 and "A capability declaration does not establish reader trust". Enforcement is outside the catalog; an old client or a direct file reader bypasses it.
- **Jcasbin unique key.** `owner_meta` unique key permits multiple owners; code assumes one. Check whether the service layer deletes the previous owner row in the same transaction (`insertRelation(..., true)` overwrite flag) before relying on it.
- **Role with deny on a dropped object.** Behaviour depends on a retry loop; no cleanup of the role row when the object is dropped through an external system (the cascade covers drops done through Gravitino only).
- **Plugin-side drift when objects change outside Gravitino.** `TableHookDispatcher.dropTable` keeps privileges when the dropper returns false because the external table was renamed out of band (`:174`). Out-of-band renames leave Ranger policies pointing at the old name `(inferred)`.
- **`enableAuthorization()` default false.** `Configs.java:402`. With it off, hooks still run owner assignment only if `internalOwnerDispatcher` is non-null; creator ownership is therefore absent for objects made while authorization is off, which is why the enable procedure exists.
- **Service-admin check bypasses membership.** The list is read directly (`AccessControlManager.java:321`), before any metalake membership exists; a name collision between an IdP identity and a configured admin name is a risk the doc does not discuss `(inferred)`.

## 5. Notes for mudcat's port design (relating Gravitino to Zanzibar-style authorization)

- Gravitino's model maps cleanly to relation tuples: `object#parent@container`, `object#owner@subject`, `role#member@subject`, `object#priv_allow@role`, `object#priv_deny@role`. Inherited allow = `parent->priv`; deny needs an exclusion (`allow - deny` over the whole path) which Zanzibar-style engines express with exclusion and arrows, but with cost on lookups. Gravitino spent most of its 2300-line engine on exactly this: deny-aware list pruning (`hasDenyPolicy`).
- Request-scoped role set (active roles) maps to caller-supplied context or a filtered subject set. Keep it in the request, as Gravitino learned (AZ-R18).
- The Gravitino port is both decision API and cache-invalidation hooks. mudcat should keep the port to `Check`, `BatchCheck`, `Lookup(resources|subjects)`, `Write(relationships) -> revision`, and a way to read the minimum revision (AZ-R26, AZ-R30, AZ-R34).
- Usage policy (licence, consent, embargo, legal hold, takedown) is not representable in Gravitino's tag access control (allow-only). It should run as a second stage after grants (AZ-R48).
- Credential issue is a decision point of its own (AZ-R22, AZ-R53, AZ-R54): the caller's grants at issue time, narrowed by the active roles, choose read or write, and static secrets are gated by a separate permission.
