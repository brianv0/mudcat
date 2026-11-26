# Candidate requirements from Apache Gravitino

Status: candidates. This document states requirements for mudcat that come from the code of Apache Gravitino commit `9aa78cf`. The owner of the project has not accepted any of them. The synthesis is in [README.md](README.md).

## Conventions

- The style follows [../../requirements.md](../../requirements.md): "shall", one need in each requirement, a rationale and one verification method (T test, A analysis, I inspection, D demonstration). TBD and TBR mark values that are not known.
- **Candidate IDs.** An ID such as G-ID-3 is a candidate ID. When the owner accepts a candidate, it gets a permanent ID in `docs/requirements.md`, and the candidate ID is retired. A candidate ID is not permanent, and a requirement in `docs/requirements.md` never refers to it.
- **Rationale.** The rationale names the Gravitino evidence with the reference of a candidate in a detail file. The prefix of the reference gives the file: BS 01, CL 02, AZ 03, CR 04, CF 05, RL 06, FS 07, IR 08, EA 09, TP 10, JM 11, SV 12, EC 13. A reference such as BS-R1 is a row in `detail/01-backend-store.md`.
- **Status.** "New" means that no mudcat requirement states the rule. "Tightens X" means that requirement X states the rule in part, and the candidate makes it more exact. "Alternative" marks a rule that mudcat could meet in more than one way.
- **Not a copy.** Many Gravitino rules exist because of its design: mutable objects, three SQL dialects, one metalake. The candidates here are the ones that also apply to mudcat. [README.md](README.md) section 9 lists the differences.

Candidate prefixes: G-ID identity and immutability, G-ST structure and metadata, G-PR provenance, G-AC access control, G-UP usage policy and deletion, G-RD data access and credentials, G-PL placement and federation, G-IN interfaces, G-OP operation, G-EV change stream and audit.

## Identity and immutability

| Cand. | Requirement | Rationale | Ver. | Status |
|---|---|---|---|---|
| G-ID-1 | mudcat shall enforce the uniqueness of a name among the live children of one parent with a partial unique index that has no deletion marker. | Gravitino puts `deleted_at` in each unique key. Two deletes in one millisecond collide (BS-R1). | T | New |
| G-ID-2 | mudcat shall not let a new entity inherit the owner, tags or grants of a retired entity that had the same ID. | A deleted ID can register again in Gravitino, and relation rows keep the ID (BS-R2, BS-R11, CF-R7). | T | New |
| G-ID-3 | mudcat shall accept an expected-version precondition on each request that alters an entity, drops an entity or moves a ref. | Only one Gravitino alter has a precondition. All others are last-writer-wins (SV-R1, SV-R2, EC-R24). | T | Tightens ID-7 |
| G-ID-4 | mudcat shall keep the concurrency token of an entity separate from the number of its content version. | Gravitino added `occ_version` in release 2.0, because one column served as the history pointer and as the token (BS-R14). | I | New |
| G-ID-5 | mudcat shall compare a digest of the request body when it replays an idempotency key, and shall refuse a replay with a different body. | The Gravitino replay compares the method, path and query only. The code says that bodies "are not compared" (IR-R17). | T | Tightens ID-16 |
| G-ID-6 | mudcat shall scope an idempotency record to the tenant and the principal that made it. | The Gravitino key is the only primary key of its table (IR-R18). | T | Tightens ID-16 |
| G-ID-7 | mudcat shall write an idempotency record in the transaction of the commit that it protects, and shall take its expiry from the database clock. | Gravitino reserves, runs and finalises in three steps, and expires by the clocks of replicas (CL-R40, IR-R20, IR-R21). | T | Tightens ID-16 |
| G-ID-8 | mudcat shall not store, or replay, a credential that a response contains. | The stored Gravitino response can hold vended credentials (IR-R19, inferred). | T | Tightens ID-16 |
| G-ID-9 | mudcat shall move the name of a ref from one version to another in one atomic commit. | A Gravitino model alias moves in two writes with no transaction (FS-R39). | T | New |
| G-ID-10 | mudcat shall keep ref names and version identifiers in disjoint syntaxes on every operation that creates or renames a ref. | Gravitino refuses a numeric alias on link only, so an update can make one (FS-R40). | T | New |
| G-ID-11 | mudcat shall verify the content digest of an artifact at registration, and shall never change the digest of a published version. | A Gravitino model URI can change, and no digest exists (FS-R41, FS-R42). | T | Tightens ID-4 |
| G-ID-12 | mudcat shall report a collision of an ID as an internal error, and not as a name conflict. | Gravitino mints 63-bit random IDs. A collision shows as a conflict of names (BS-R32). | T | New |
| G-ID-13 | mudcat shall make a drop of an object that does not exist succeed, and shall return the commit that last observed its absence. | Gravitino returns `dropped: false` with no commit (SV-R5, SV-R6). | T | Tightens ID-20 |
| G-ID-14 | mudcat shall number the versions of an entity in sequence and never reuse a number after a deletion. | Gravitino model versions do this, and a retention limit of 10 versions of other types breaks the history (FS-R37, BS-R8). | T | New |

## Structure and metadata

| Cand. | Requirement | Rationale | Ver. | Status |
|---|---|---|---|---|
| G-ST-1 | mudcat shall let a tag definition declare that a descendant cannot replace an assignment of that tag. | A Gravitino child assignment replaces the parent one with its values, so a table can set `data_access=public` below a restriction (TP-R4). | T | Tightens ST-17 |
| G-ST-2 | mudcat shall compute the inherited tags of an entity in one component that each consumer calls. | The Gravitino list path repeats the logic and filters after the merge, so an invisible tag hides a visible one (TP-R5, TP-R17). | I | Tightens ST-17 |
| G-ST-3 | mudcat shall tie each statistic to the immutable version that it describes, and shall mark the statistic stale for a newer version. | Gravitino statistics carry no version. The optimizer CLI caused stale-statistics bugs (TP-R42, JM-R41). | T | Tightens ST-22 |
| G-ST-4 | mudcat shall define the cascade of a drop from one registry of relation tables, and shall run a periodic sweep as a backstop. | Gravitino writes each cascade by hand, with about 25 kinds of rows for one schema drop, and partition statistics are missed (TP-R19 to TP-R22, BS-R10). | T | New |
| G-ST-5 | mudcat shall store a source type that has no common equivalent as an opaque type that holds the native string, and shall write it back unchanged. | Gravitino uses `ExternalType(native string)` as the escape for each dialect (RL-R1, RL-R2, FS-R62). | T | Tightens ST-3 |
| G-ST-6 | mudcat shall publish, for each source and each native type, if the mapping to the common type is lossless, lossy or opaque. | The Gravitino mappings are lossy and asymmetric. MySQL `BIT`, `BINARY` and `ENUM` needed a second query (RL-R3). | I | New |
| G-ST-7 | mudcat shall reject a requested attribute that the target cannot honour, and shall not ignore it. | ClickHouse ignores `nullOrdering` in Gravitino (RL-R9). | T | New |
| G-ST-8 | mudcat shall record with each schema or structure taken from an external format the version of the format at that time, and shall mark the schema as unverified when it was not checked. | A Delta schema is asserted by the caller. Lance compares `lance.version` on load (RL-R28, RL-R29, FS-R55). | T | New |
| G-ST-9 | mudcat shall load a registered table that has a part the common model cannot represent, and shall mark that part opaque. | An unknown Iceberg transform or a Doris `NGRAM_BF` index makes a Gravitino table unloadable (RL-R39). | T | New |
| G-ST-10 | mudcat shall record the table format of a registered table explicitly. | Gravitino infers the format from engine parameters on each listing (RL-R34). | I | New |
| G-ST-11 | mudcat shall mark a system-managed entity with a flag, and shall not use a name prefix in the user namespace. | Gravitino keeps policy bindings in tags named `gravitino_hidden_<id>`. A collision needs a restore from backup (SV-R29). | I | New |
| G-ST-12 | mudcat shall reject a property key in a reserved namespace on create and alter, and shall hide the value of a secret property on read. | Gravitino reserves property prefixes and masks values (SV-R30, CR-R34). | T | New |
| G-ST-13 | mudcat shall return a representation of an entity that a client can send back unchanged with no loss of a secret. | A masked value `******` is refused on write, so a read followed by a write fails (SV-R31). | T | New |
| G-ST-14 | mudcat shall apply one name syntax and one length limit to every entity type, state them in one place and count the length in Unicode code points. | Gravitino has three grammars and limits of 64, 128 and 300 in different layers (SV-R25 to SV-R27, CF-R29). | T | New |
| G-ST-15 | mudcat shall apply a tightened name rule on create and rename only, and never on load, list or drop. | Gravitino does this, and legacy names stay usable (SV-R28). | T | New |
| G-ST-16 | mudcat shall enforce a single-valued relation, such as the owner, with a constraint. | The Gravitino owner rule had no constraint. Release 2.0 merges duplicate owners (BS-R28). | T | New |
| G-ST-17 | mudcat shall require a fixed prefix on each statistic name that a producer defines, and shall reserve all other names for itself. | Gravitino requires the prefix `custom-` (TP-R40). | T | New |

## Provenance

| Cand. | Requirement | Rationale | Ver. | Status |
|---|---|---|---|---|
| G-PR-1 | mudcat shall store each accepted OpenLineage run event durably before it returns success. | Gravitino forwards lineage and stores nothing. HTTP 201 means "queued" (EA-R44). | T | Tightens PR-6 |
| G-PR-2 | mudcat shall link a stored lineage run to the entities that it names by entity ID, keep the link when an entity gets a new name, and keep an unresolved dataset as an external reference. | Gravitino maps a dataset by namespace and name and links nothing (EA-R45, EA-R46, EA-R47). | T | Tightens PR-6 |
| G-PR-3 | mudcat shall record a lineage claim about an output dataset only when the sender may write that dataset or holds a run token for the run. | Gravitino needs only read visibility for an output dataset (EA-R49, EA-R50). | T | New |
| G-PR-4 | mudcat shall record the authenticated sender of a lineage event separately from the producer that the event claims. | Gravitino does not record the sender (EA-R51). | T | New |
| G-PR-5 | mudcat shall record each compaction, manifest rewrite, snapshot expiry and orphan removal as an activity with its input and output files and the resulting snapshot. | Gravitino records counters only, and has no lineage call in maintenance (JM-R39, JM-R40). | T | Tightens PR-7 |

## Access control

| Cand. | Requirement | Rationale | Ver. | Status |
|---|---|---|---|---|
| G-AC-1 | mudcat shall return a revision token with each write to the authorization store, and shall accept a minimum revision on each check. | Gravitino name-to-ID and owner mappings are eventually consistent, so a drop can briefly allow access through an orphan policy (AZ-R30, CL-R21, CL-R22). | T | Tightens AC-7 |
| G-AC-2 | mudcat shall write each change for an external enforcement point to an outbox in the transaction of the grant, and shall retry the delivery until it succeeds or a person cancels it. | Gravitino commits the grant, then calls the plugin and discards its result (AZ-R35). | T | New |
| G-AC-3 | mudcat shall record, for each grant and each external enforcement point, the state of the last push, and shall expose it. | The Gravitino chain stops at the first `false` and does not roll back (AZ-R36, AZ-R41). | T | New |
| G-AC-4 | mudcat shall provide an operation that compares the grants of the catalog with the policy of each external enforcement point and reports each difference. | Nothing in Gravitino detects drift (AZ-R37). | D | New |
| G-AC-5 | mudcat shall reject a grant that an external enforcement point cannot enforce, and shall list what each point can enforce. | Ranger maps `USE_CATALOG` to SQL `SELECT` (AZ-R38, AZ-R39). | T | New |
| G-AC-6 | mudcat shall evaluate each access decision against one consistent snapshot of the grants, and shall not load all grants of a caller into process memory. | The Gravitino memory mirror needs a 2,363-line authorizer and 107 race tests (AZ-R29, AZ-R31). | A | Alternative |
| G-AC-7 | mudcat shall define each permission, its object types and its ancestor scopes in one table. | Gravitino writes inheritance in regex macros and a second registry. A new privilege touches about eight places (AZ-R2). | I | New |
| G-AC-8 | mudcat shall state one rule for the effect of ownership on an explicit deny, and shall apply it to each operation. | In Gravitino an owner of an ancestor bypasses a deny, and an owner of the leaf does not (AZ-R5, AZ-R6). | T | New |
| G-AC-9 | mudcat shall derive group membership from the credential of the current request, and shall not share derived membership between requests. | A shared enforcer state with membership from one request let another request add an allow or drop a deny (AZ-R18). | T | New |
| G-AC-10 | mudcat shall record each decision that allows access to a protected object with the grant path (role, tag, owner or ancestor), and shall answer which subjects hold a permission on an object and by which path. | Gravitino records denials only, and cannot answer "who can read X" (AZ-R50, AZ-R51). | T | Tightens AC-8 |
| G-AC-11 | mudcat shall reject a bearer token that has no expiry claim, and shall check the issuer of each bearer token against a configured issuer. | The Gravitino static-key validator sets a signing key and a clock skew only (CR-R26, CR-R27). | T | New |
| G-AC-12 | mudcat shall derive the principal from the issuer and the subject together, and shall reject a principal that the mapping rule does not match. | The Gravitino regex mapper returns the original principal when the pattern does not match, and a test fixes this (CR-R28, CR-R29). | T | New |
| G-AC-13 | mudcat shall refuse to start with an authenticator that accepts unverified identities beside one that verifies identities. | In Gravitino `simple` and `basic` share the `Basic` header, and the first match wins (CR-R30). | T | New |
| G-AC-14 | mudcat shall store a local password with a standard password hash with a recorded cost, and shall hash it again at login when the cost is below the current cost. | Gravitino uses SHA3-512 with 5,000 iterations and has no upgrade path (CR-R32). | I | New |
| G-AC-15 | mudcat shall apply access control to each catalog definition, whatever the source of the definition. | Gravitino loads catalog definitions from a path in a property (CR-R25, CF-R42). | T | New |
| G-AC-16 | When the authorization engine fails while mudcat filters a list, mudcat shall fail the request, and shall not drop the entry. | The Gravitino `doFilter` catches each exception and returns `null` for the entry, so an engine outage looks like an empty list (AZ-R28). | T | New |
| G-AC-17 | mudcat shall authorize a list of N entries in a number of store round trips that does not depend on N. | Gravitino evaluates one expression for each entity on a pool of 100 threads, and added a memo, a batch preload and a parent-scope short-circuit (AZ-R25, AZ-R26, AZ-R27). | T | New |

## Usage policy and deletion

| Cand. | Requirement | Rationale | Ver. | Status |
|---|---|---|---|---|
| G-UP-1 | mudcat shall record for each registered location if mudcat owns the stored bytes, shall default the flag to "not owned", and shall need a separate privilege to change it. | A Gravitino fileset flag MANAGED or EXTERNAL decides deletion, and `WRITE_FILESET` alone can change it (FS-R1, FS-R2, FS-R5). | T | New |
| G-UP-2 | mudcat shall provide an unregister operation that removes the record and never touches the bytes. | Gravitino has this for external objects, and hardened Lance after the fact (FS-R3, RL-R30). | T | New |
| G-UP-3 | mudcat shall purge the bytes of a not-owned location only when an administrator of the storage resource has enabled purge for it. | Gravitino Lance needs an opt-in by an administrator (FS-R4). | T | New |
| G-UP-4 | mudcat shall store the intent to delete bytes before it deletes them, shall keep the record until deletion is confirmed, and shall not hold a database transaction open during the deletion. | Gravitino deletes bytes inside the transaction, and it is not atomic over several locations (FS-R7, FS-R8, RL-R31). | T | Tightens UP-8 |
| G-UP-5 | mudcat shall state in each drop response if bytes were removed, and shall list the locations that a failed purge removed. | Gravitino with `disable-filesystem-ops=true` drops a MANAGED fileset and deletes nothing, and says nothing (FS-R6, FS-R12). | T | New |
| G-UP-6 | mudcat shall reject an owned location that equals, contains or lies inside the owned location of another record, and shall find an overlap with one indexed query. | Overlap is a security boundary, because credentials are scoped by prefix. The Gravitino lookup scans the catalog on some backends (FS-R10, IR-R26, IR-R27). | T | New |
| G-UP-7 | mudcat shall let a tag, a policy or a legal-hold record impose a restriction that no grant can override. | Gravitino tag access control is allow-only (AZ-R48, TP-R4). | T | New |
| G-UP-8 | mudcat shall keep each version of a policy that was ever in force, and shall return the version that applied at a given commit. | Gravitino keeps 1 version by default (maximum 10), so a past decision cannot be explained (TP-R35, BS-R5). | T | Tightens UP-13 |
| G-UP-9 | mudcat shall fail closed when two distinct row-filter or column-mask policies apply to one target, and shall define how each policy type combines before it ships the type. | Gravitino fails closed for conflicts of these two types (TP-R30, TP-R31). | T | New |
| G-UP-10 | mudcat shall block the creation of an entity at a name while a purge of that name runs, on every operation that can bind the name. | Gravitino blocks create and register only, not rename or a staged-create commit (JM-R54, IR-R33). | T | New |
| G-UP-11 | mudcat shall keep a record of the files that a failed purge left, until an operator closes the record. | Gravitino lifts the name tombstone after a FAILED purge and the files leak (JM-R55, IR-R34). | T | New |
| G-UP-12 | mudcat shall not expire a snapshot, and shall not delete a file, by an age or count rule when a ref, a resolution, a citation or another catalog record names it. | Gravitino expiry has age and retain-last fields only, and leaves refs to Iceberg (JM-R48). | T | Tightens ID-9 |
| G-UP-13 | mudcat shall learn the set of writers that are in flight from the catalog, and shall not infer it from the age of a file. | Gravitino orphan removal uses a 3-day window and a lexical prefix of the scan root (JM-R45, JM-R47). | T | New |
| G-UP-14 | mudcat shall let an administrator cancel a requested purge during a hold period of TBD hours. | Gravitino async purge is a hard delete with no undrop and no hold (JM-R56). | D | New |

## Data access and credentials

| Cand. | Requirement | Rationale | Ver. | Status |
|---|---|---|---|---|
| G-RD-1 | mudcat shall issue a credential only for a prefix inside a storage root registered for the storage resource of the replica, whatever location the client supplied. | Gravitino takes the scope from client table metadata (`location`, `write.data.path`) with no containment check (CR-R1, IR-R26). | T | Tightens RD-3 |
| G-RD-2 | mudcat shall reject, or escape, a wildcard or an expression character in a location before it builds a cloud access policy. | Gravitino puts paths unescaped in a GCS CEL condition and in S3, OSS and COS ARNs (CR-R2). | T | New |
| G-RD-3 | mudcat shall let a caller ask for read-only access, a narrower prefix and a shorter lifetime, and shall issue the intersection of the request and the grant. | Gravitino chooses read or write from the privileges of the caller. A read-only job gets a write credential (CR-R3, CR-R4). | T | New |
| G-RD-4 | mudcat shall refuse a request when the storage provider cannot express the scope exactly, and shall check the size of the generated policy against the limit of the provider. | Gravitino checks no policy-size limit (CR-R5, CR-R6). | T | New |
| G-RD-5 | mudcat shall not fall back from a scoped credential to a long-lived key, and shall need a separate grant to return a long-lived key. | Gravitino falls back when `credential-providers` is missing. It has the separate grant (CR-R15, CR-R16). | T | New |
| G-RD-6 | mudcat shall state the longest delay between a revocation and the end of every credential already issued, and shall limit the lifetime of credentials for affected data to that delay. | STS and SAS tokens cannot be revoked. The Gravitino cache serves a token for 3,600 seconds by default (CR-R8). | A | Tightens UP-7 |
| G-RD-7 | mudcat shall not return to a client a credential of its own database or control plane, and shall not hold a credential that can delete data in each location of each tenant. | An Iceberg JDBC catalog in Gravitino returns the password of its backing database. The Lance server shares static keys (CR-R17, FS-R32). | I | New |
| G-RD-8 | mudcat shall record each issuance of a credential with the caller, the run token, the object, the scope, the expiry and the cloud session identifier. | Gravitino does not audit issuance on the fileset endpoint (CR-R19, EA-R2). | T | Tightens AC-8 |
| G-RD-9 | mudcat shall return with each credential its issue time, its lifetime in seconds and the endpoint where a client can renew it, and clients shall ignore a credential of an unknown type. | Gravitino sends an absolute expiry only. Clients refresh by their own clock, and the ratios 0.15 and 0.5 are the only defence against skew (CR-R11, CR-R12, CR-R13). | T | New |
| G-RD-10 | mudcat reference clients shall not reuse a storage session that was obtained under one grant for the grant of another object. | The key of the Java GVFS file-system cache has no fileset (FS-R28, FS-R30). The leak is inferred. | T | New |
| G-RD-11 | mudcat shall put a unique, valid identifier of the caller into the session name of each cloud credential. | Gravitino does this, and each provider has a different name rule (CR-R18). | T | New |
| G-RD-12 | mudcat shall check that a named location in a credential request is a registered location of the object, and shall use the default location when no name is sent. | Gravitino has this check (CR-R21, FS-R26). | T | New |

## Placement and federation

| Cand. | Requirement | Rationale | Ver. | Status |
|---|---|---|---|---|
| G-PL-1 | mudcat shall register an external object, and read it, with no write to the external system. | Gravitino writes its ID into a Hive parameter or a JDBC comment. A comment edit loses the identity (CF-R5, RL-R26, RL-R27). | T | New |
| G-PL-2 | mudcat shall assign the identifier of a registration when it registers the object, and shall bind it to a durable identifier of the external object. | Gravitino assigns an ID at the first load, under a write lock. An external rename can make a second identity (CF-R4, CF-R9). | T | New |
| G-PL-3 | mudcat shall let an operator unregister one external object, with no reset of the other registrations of its catalog. | A Gravitino identity conflict needs a forced drop of the whole catalog (CF-R8). | T | New |
| G-PL-4 | mudcat shall record the external version that each registration update observed, and shall reject an update that rests on an older observation. | A load that races with an alter drops column IDs in Gravitino (CL-R41, CF-R17, issue 14279). | T | New |
| G-PL-5 | mudcat shall make a drop in an external catalog and the removal of its registration one outcome, or shall show the registration as unverified until it is reconciled. | Gravitino create and drop are two steps, and the recovery is a text in the error (CL-R42, CF-R14, IR-R49). | T | New |
| G-PL-6 | mudcat shall create the identity, the owner and the catalog entry of a table in one database transaction. | Create-then-import can leave an unowned table in Gravitino (IR-R49). | T | New |
| G-PL-7 | When mudcat federates a remote catalog, it shall let the operator choose to forward the caller token or to use a service account, and with a service account it shall authorize locally. | Gravitino offers both modes (CR-R24, CR-R43). | T | New |
| G-PL-8 | mudcat shall tell the caller when it could not refresh the stored copy of external metadata. | Gravitino swallows store failures on external paths (CF-R18). | T | New |
| G-PL-9 | mudcat shall load connector code only from locations in an operator allow-list, and shall never load it from a path in a catalog property. | The Gravitino properties `package` and `ops-impl` load code with no allow-list (CF-R42). | I | New |
| G-PL-10 | mudcat shall use the same location resolution rule in the server and in each reference client. | The Gravitino rule for the default location exists in four versions (FS-R25, FS-R24). | T | New |
| G-PL-11 | mudcat shall store each location in one canonical form that does not depend on catalog configuration, and shall need a URI scheme on every location. | Gravitino resolves a location by configuration (FS-R22, FS-R23). | T | New |
| G-PL-12 | mudcat shall resolve a location template once, at registration, and shall store the resolved location. | Gravitino resolves `{{catalog}}` placeholders once (FS-R14). | T | New |

## Interfaces and standards

| Cand. | Requirement | Rationale | Ver. | Status |
|---|---|---|---|---|
| G-IN-1 | mudcat shall pass the Iceberg REST compatibility kit on each release, and shall record the version of the kit and of Iceberg that it used. | The Gravitino tests are the only evidence, and no run of the upstream kit exists (IR-R2). | T | Tightens IN-14 |
| G-IN-2 | mudcat shall implement the Iceberg REST contract with no use of a non-public member of the Iceberg library. | Gravitino uses reflection on private fields and classes planted in `org.apache.iceberg.*` (IR-R53). | I | New |
| G-IN-3 | mudcat shall store a namespace as an ordered list of levels, so that levels that contain `.` or `:` stay distinct. | In Gravitino, `["a:b"]` and `["a","b"]` alias to one schema (IR-R8). | T | New |
| G-IN-4 | mudcat shall apply a read restriction on each path that returns a data location or a credential, and shall refuse table metadata with an active restriction to a client that did not declare that it enforces it. | The Gravitino restriction applies to load-table only. The capability header is not implemented (IR-R42, IR-R43). | T | New |
| G-IN-5 | mudcat shall generate the list of advertised endpoints and the capability documentation from the registered routes. | Gravitino docs and code disagree on register-view and on scan-plan tasks (IR-R55). | I | Tightens IN-15 |
| G-IN-6 | mudcat shall check the semantic validity of each commit, and shall run the check again on each retry. | Gravitino injects its check as an `UpdateRequirement` so that it runs on each retry (IR-R9 to IR-R11). | T | New |
| G-IN-7 | mudcat shall return each error, including a router, a parse and a filter error, in one JSON shape with one code for each condition, and the status and the code shall agree. | Gravitino shares code 1006 between 409 and 405 and has a class name in `type` (SV-R16 to SV-R18). | T | New |
| G-IN-8 | mudcat shall give each list endpoint a page size, a maximum page size and a continuation token, and shall limit the size of a request body and of a batch. | Gravitino has no pagination and no body limit (SV-R20, SV-R21). | T | New |
| G-IN-9 | mudcat shall answer 429 with a retry delay when its request queue is full. | `tooManyRequests()` exists in Gravitino and no caller uses it (SV-R22). | T | New |
| G-IN-10 | mudcat shall state a policy for the deprecation and removal of an API version, and shall answer an unsupported version with a structured error. | Gravitino negotiates by media type, and only one operation uses v2 (SV-R13 to SV-R15). | I | Tightens IN-2 |
| G-IN-11 | mudcat shall publish as data a table of engine, engine version, format and operation, with each cell marked supported or not supported. | The Gravitino matrix has gaps (Paimon on Spark 3.5 only, Hive absent on Flink 2.x) (EC-R1, EC-R41). | I | New |
| G-IN-12 | mudcat shall give each entity an immutable ID, and shall return a change token that a client can compare with no new read of the definition. | Trino compares `lastModifiedTime`, and Spark JDBC compares `createTime`, because a catalog has no ID (EC-R15). | T | New |
| G-IN-13 | mudcat shall define the compatibility of a client and a server by a range that the server advertises, and shall give all clients one default for timeout and retry. | The Gravitino client accepts any older major version. Java and Python differ in timeout (EC-R26, EC-R27). | T | New |
| G-IN-14 | mudcat shall classify each MCP tool as read, write or destructive, shall let an operator expose a subset by class, and shall offer one search tool that returns identifiers and the commit that it read. | The Gravitino MCP server has 81 tools, 36 that mutate and 7 with a read-only hint. Exposing `table` exposes `drop_table`. It has no search tool (EC-R45 to EC-R48). | I | Tightens IN-6 |
| G-IN-15 | mudcat shall expose, for each source and each operation, a capability that a client can read before it sends a request, and shall return one distinct error for "the source cannot do this". | Gravitino signals this by three exception types, and the `Capability` SPI has six methods (RL-R18, RL-R19). | T | New |
| G-IN-16 | mudcat shall serve every Iceberg table through its Iceberg REST gateway, so that an engine with an Iceberg REST client needs no mudcat plugin. | Spark and Trino use the Iceberg REST server of Gravitino to get vended credentials (EC-R3, EC-R7). | D | Tightens IN-5 |
| G-IN-17 | mudcat shall filter each listing that it returns to an engine by the permissions of the caller. | Spark lists catalogs once, at driver start, under the application identity. Trino polls every 10 seconds (EC-R5, EC-R6). | T | New |

## Operation

| Cand. | Requirement | Rationale | Ver. | Status |
|---|---|---|---|---|
| G-OP-1 | mudcat shall store the schema version in the database, and shall refuse to start when it differs from the version that the server needs. | Gravitino has no version table and no check at start (BS-R55, SV-R45). | T | New |
| G-OP-2 | mudcat shall produce, from each earlier release, a schema equal to the schema of a new installation, and shall make each upgrade step safe to run again. | A new Gravitino installation and an upgraded one differ in the default of `storage_location_name` (BS-R56, BS-R57). | T | New |
| G-OP-3 | mudcat shall not use an in-process lock for the correctness of any write. | Gravitino tree locks cannot order writes between servers. Its comments say so (CL-R29). | I | New |
| G-OP-4 | mudcat shall not change catalog state in a read request, except an idempotent repair that uses a conditional update with a bounded number of retries. | Gravitino imports an object on first load and refreshes Lance columns on load (RL-R32, FS-R56). | T | New |
| G-OP-5 | mudcat shall release every component that started when startup fails, and shall use one shutdown time budget that is longer than the sum of its parts. | A failed Gravitino start calls `System.exit` before the shutdown hook. Three shutdown timeouts disagree (SV-R48, SV-R49). | T | New |
| G-OP-6 | mudcat shall release the in-flight slot of a maintenance run when the job record reaches a terminal state, with no help from the job. | A Gravitino slot is freed only by a best-effort call from the job. A dead pod blocks the table for ever (JM-R22). | T | New |
| G-OP-7 | mudcat shall store no secret in plaintext in its database. | The only secret provider that ships with Gravitino is in memory, and there is no cipher code (CR-R37). | I | New |
| G-OP-8 | mudcat shall decide that a property is a secret from a declared schema, and shall use one redaction rule set for API responses, logs, events and audit records. | Gravitino redacts by a substring of the key name, with a separate list for audit logs (CR-R35, CR-R36, EA-R35, EA-R36). | I | New |
| G-OP-9 | mudcat shall compute the enabled state of a child from its parent at read time, and shall not copy it into the child. | Gravitino copies the metalake state into each catalog with no transaction (SV-R10, SV-R11, CF-R61). | T | New |
| G-OP-10 | mudcat shall run its store tests against the PostgreSQL major versions that it supports, and not against an emulation. | Gravitino runs H2 in MySQL mode, and each dialect needed workarounds (BS-R53, BS-R54). | I | Tightens OP-1 |
| G-OP-11 | mudcat shall allow a server of version N and a server of version N+1 to share one database during an upgrade. | Gravitino upgrades need all servers stopped (BS-R58, SV-R46). | T | New |
| G-OP-12 | mudcat shall classify each configuration key as startup-only or live, and shall make the shipped default equal to the code default. | The Gravitino template sets `maxThreads=200` where the code default differs, and it sets `serviceAdmins=anonymous` (SV-R43, SV-R44). | I | New |
| G-OP-13 | mudcat shall commit a job record before it hands the job to an executor, and shall never replay a submission whose outcome is unknown. | Gravitino registers the job first and looks the execution up by job ID (JM-R1, JM-R3, JM-R4). | T | New |
| G-OP-14 | mudcat shall run a job with a credential that is scoped to the entities of that run, and shall authorize a job callback by the run that it names. | Gravitino jobs run under one static identity, and callbacks need a metalake-wide privilege (JM-R32, JM-R33). | T | Tightens PR-4 |
| G-OP-15 | mudcat shall index each link table on both endpoints from its first release. | A Gravitino release shipped without an index for role deletion (BS-R27). | I | New |
| G-OP-16 | mudcat shall resolve a qualified name of any depth to its ID in one database round trip. | Gravitino stores a hierarchical schema name as a path string in one `VARCHAR(128)` (BS-R42). | T | Tightens PF-4 |

## Change stream and audit

| Cand. | Requirement | Rationale | Ver. | Status |
|---|---|---|---|---|
| G-EV-1 | mudcat shall make a change record visible to consumers only when no transaction with a lower position can still commit. | Gravitino sequence IDs commit out of order, so it reads a 60-second window again, and its docs say that late rows can be lost (EA-R25, BS-R45). | T | Tightens EV-13 |
| G-EV-2 | mudcat shall tell a consumer whose cursor is older than the retained history to resynchronise, and shall never skip the gap in silence. | Gravitino retention is by time, and a consumer past it misses rows (CL-R12). | T | New |
| G-EV-3 | mudcat shall keep a cursor for each consumer, shall retry a failing consumer from its own cursor, and shall not block the other consumers. | The Gravitino cursor is shared and moves on when a listener fails (EA-R26, CL-R13). | T | New |
| G-EV-4 | mudcat shall keep a change record until each registered consumer has passed it, or has been marked lost. | Gravitino prunes the log by age (EA-R27). | T | New |
| G-EV-5 | mudcat shall write the audit record of a mutation in the transaction of the mutation. | The Gravitino audit log is an in-memory listener with a queue of 3,000 that drops on overflow, while its change log is transactional (EA-R17, EA-R24). | T | Tightens AC-8 |
| G-EV-6 | mudcat shall put the entity ID, the request ID, the event ID, the tenant, the authenticated principal and the commits that the request saw and made in each event and each audit record. | Gravitino events carry names only, with no request ID, event ID, tenant or commit (EA-R6 to EA-R11). | T | New |
| G-EV-7 | mudcat shall give each route a named operation from one route table, so that no operation is recorded as unknown. | Credential issuance, secrets, `*Exists` and `import*` appear as an unknown operation with a null identifier in Gravitino (EA-R2, EA-R3). | I | New |
| G-EV-8 | mudcat shall record a failure event for an operation that a policy hook rejected, and shall not report a committed mutation as failed when the delivery of an event fails. | A veto by a synchronous listener leaves a generic 403, and listener exceptions are swallowed in production (EA-R13, EA-R16). | T | New |
| G-EV-9 | mudcat shall apply listeners and hooks in the order that the operator configured. | Gravitino listeners run in the order of a `HashMap` (EA-R21). | T | New |
| G-EV-10 | mudcat shall take the client address from the TCP peer, and from a forwarding header only when the peer is a configured trusted proxy. | Gravitino does this, and it is good (EA-R10, SV-R38). | T | New |
| G-EV-11 | mudcat shall not put a secret value in an event object that it hands to a consumer. | Gravitino events carry unredacted properties, and only audit formatters redact (EA-R36). | T | New |
