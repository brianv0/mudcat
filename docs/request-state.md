# State that each API request reads and writes, and its consistency class

Status: draft for decision. For each request of the HTTP interface, this document lists the state that the request touches, the consistency class that the state needs, and where mudcat gets it today. The consistency class is a decision of the design. Where the code today gives a stronger class than the recommendation, the row says so: those rows are the reads that a cache or an external service can take away from the database.

Sources: the handlers in `src/api.rs` and the functions that they call, and the traces of the connection to PostgreSQL (see [performance.md](performance.md), "Round trips to the database"). Round trips are counted for five requests; the others are not measured yet.

## Consistency classes

| Class | Guarantee | How mudcat gives it | A weaker class would cause |
|---|---|---|---|
| **Serialized** | The read and the write that depends on it are one step: no other writer comes between them | A lock that the writer holds to its commit, or a unique constraint | A lost update, or two writers that both succeed where one must fail |
| **Strong** | The read sees every change that was committed before the statement started | A read from the primary database, in the transaction of the request | A wrong result that the request cannot repair |
| **Bounded staleness** | The read is at most 10 seconds behind (the bound is a decision that is not in requirements.md yet; AC-7 says "each check that starts after the removal") | A cache in the server with an age limit, or an external service in its normal mode | Access that continues, or a grant that is not seen, for at most the bound |
| **Eventual, checked** | The read can be behind without a limit, and each result is checked with a Strong read before the answer leaves | A derived index, then the catalog | Nothing wrong in the answer; a result can be missing while the index is behind |
| **Immutable** | The state cannot change after it is written | Nothing: any copy is correct | Nothing: it can be cached without a limit |

Two notes on Strong. A transaction of mudcat reads at the level READ COMMITTED, so each statement sees what was committed before that statement, and two statements of one request can see different states. Where a request needs one state for all its reads, the row says "one snapshot". And Strong is read-your-writes only because every server reads the primary; a read replica would make it Bounded staleness.

## State that every request touches

| State | Read or write | Class | Why | Source today |
|---|---|---|---|---|
| Tenant of the endpoint (hostname to tenant) | Read | Bounded staleness | A new tenant is found on a miss; a renamed endpoint is rare | Registry in the process; the table on a miss |
| Token (signature, audience, expiry) | Read | Immutable for the signature; expiry by the clock | A token is valid until it expires; mudcat has no revocation list | Verified in the process, no database |
| Location and status of the tenant (home marker) | Read | **Strong** for a read; **Serialized** for a write | A stale location loses writes and answers from an empty database | The database, in the first statement of each transaction; a write reads it again with the fence held |
| Row-level security (tenant of the transaction) | Read | **Strong** | Isolation between tenants; never cached | The database, each statement |
| Audit entry of the decision | Write | Strong (durable when the request ends) | A denial must stay in the log when the request fails (AC-8) | The database, a transaction of its own |

## State of an access decision

Each request below makes one or more access decisions. A decision reads this state (15 round trips today as its own transaction, 12 inside another transaction):

| State | Class recommended | Class today | Why |
|---|---|---|---|
| Groups of the caller, and the age of each membership | Bounded staleness | Strong | A removed member keeps access for the bound |
| Grants on the tenant, the artifact, the access root | Bounded staleness | Strong | A removed grant keeps access for the bound |
| Exclusion of the caller from an artifact | Bounded staleness | Strong | The same as a grant |
| Condition of a grant (embargo date) | Strong, from the clock | Strong | The date is in the tuple; only the clock decides |
| Position of the object (version to artifact to access root) | Bounded staleness; Immutable if a collection cannot move to another access root | Strong | A stale root gives the grants of the old root |
| Compartments from tags | Bounded staleness | Strong | To confirm: removal of a person from a compartment |
| Compartment of a legal hold on invalidated content | **Strong** | Strong | Held content that is invalidated must be closed to readers at once |

The rows where the two columns differ are the reads that a decision cache, or an external authorization service, can take away from the database.

## Requests

### Reads

| Request | State | Class | Source today | Round trips today |
|---|---|---|---|---|
| `GET /entities/{id}`, `GET /entities/{id}/jsonld` | Kind of the entity; one access decision; the state document of the entity | Decision: Bounded staleness. Document: Strong | Database | Not measured |
| `POST /query` | Access roots that the caller can read; collections, artifacts, versions, the typed attribute index, payloads; the effective-policy cache for `permitted()`; the lineage relations for `used_in()` and `derived_from()` | Roots: Bounded staleness. Rows: Strong. Policy cache: Strong for invalidations and expiry, which are read with it | Database | 8 for a first page |
| `POST /search` | The search index; then, for each result, the newest version, the invalidation set and one bulk access decision; one audit entry | Index: **Eventual, checked**. Checks of the results: invalidation Strong, access Bounded staleness | Index tables in the database, then the catalog | Not measured |
| `POST /resolve` | Refs to snapshots; manifests; selections; the invalidation set; usage grants of every source; jurisdiction of the caller and of each collection; replicas and their state; access decisions for each access root. Writes: the resolution record, credentials, audit | Refs: Strong. Snapshots and manifests: **Immutable**. Invalidations: **Strong**. Usage policy: Strong (to confirm: an ended grant must stop the next resolution). Replicas: Strong. Access: Bounded staleness | Database; manifests from the manifest store | 54 for 10 members |
| `POST /resolutions/{id}/again` | The recorded resolution; then invalidations, policy, replicas and access as for a resolution | The recorded resolution: Immutable. The rest as `POST /resolve` | Database | Not measured |
| `GET /lineage/{id}` | One access decision; activities, usage and derivation relations up to the limits | Decision: Bounded staleness. Relations: Strong | Database | Not measured |
| `GET /datasets/{id}/changes` | One access decision on the newer snapshot; the manifests of both snapshots | Decision: Bounded staleness. Snapshots: **Immutable** | Database; manifest store | Not measured |
| `GET /snapshots/{id}/export` | One access decision (export); the snapshot, its sources and their duties | Decision: Bounded staleness. Snapshot: Immutable. Duties: Strong | Database | Not measured |
| `GET /stream` | Position and floor of the stream; published events; for a subscription, the identity of the subscriber and what it can read | Events: Strong up to the horizon of the relay, in commit order. Filter: Bounded staleness | Database | Not measured |
| `POST /stream/backfill` | State documents of all entities that the subscriber can read, and the position that they are consistent with | Documents and position: Strong, one snapshot. Filter: Bounded staleness | Database | Not measured |
| `GET /iceberg/v1/config`, `/namespaces`, `/namespaces/{ns}/tables/{table}` | Collections and datasets that the caller can read; the head snapshot of a table | Access: Bounded staleness. Head: Strong | Database | Not measured |
| `GET /openapi.json`, `GET /context.jsonld` | None | Immutable | The process | 0 |

### Writes

Each write is one transaction: its rows, its commit record and its events are committed together or not at all. A write reads the home marker again with the fence held.

| Request | State read | State written | Class of the reads | Round trips today |
|---|---|---|---|---|
| `POST /register` | Collection by path and its access root; write permission on it; the artifact by name; a version with the same checksum | Artifact, version, replica, identifiers, identity index, entities, events | The same-checksum lookup: **Serialized**, by a unique constraint (registration is idempotent). Collection: Strong. Permission: Strong today; could be Bounded staleness | 18 for one artifact |
| `POST /commit` | The dataset; write permission; the branch ref; the members that the commit names in the parent snapshot; the methods of the members | Snapshot, manifest references, ref and its history, entities, events | Ref: **Serialized**, under its row lock (the requirement of a commit). Parent snapshot: Immutable. Permission: Strong today; could be Bounded staleness | 35 for one member |
| `POST /lineage/openlineage` | Datasets and versions that the event names; write permission | Activity, usage, generation, the run record as a version, events | Named inputs: Strong | Not measured |
| `POST /stream/subscriptions` | Read permission on the scope | Subscription with the identity of the subscriber | Permission: Bounded staleness | Not measured |

The order of events of one tenant is Serialized too: each write takes its order number under the commit lock of the tenant.

### The MCP endpoint

`POST /mcp` calls the operations above (search, query, resolve, lineage, describe) for an agent. Each call has the state and the consistency class of the request that it stands for.

## What follows

- **Serialized, in the database only:** the home marker of a write, a branch ref under a commit, the same-checksum lookup of a registration, the order of events.
- **Strong, never cached:** the home marker of a read, row-level security, the invalidation set, the compartment of a legal hold.
- **Bounded staleness, the decision cache:** groups, grants, exclusions, the access roots that a caller can read. The cache is keyed by tenant, caller and access root, and an entry is not used after the bound. A server that cannot refresh an entry denies (AC-6).
- **Immutable, cached without a limit:** published snapshots and their manifests, recorded resolutions, token signatures.
- **Requirements to change if the bound is accepted:** AC-7 ("each check that starts after the removal") and the check of T4.2 that no cached decision was used.

## Open points for the owner

1. Compartments from tags: Bounded staleness, or Strong when a person is removed from a compartment?
2. Usage grants of a source: must an ended or withdrawn grant stop the next resolution (Strong), or within the bound?
3. Is the bound fixed at 10 seconds, or a setting of a tenant with 10 seconds as the highest value?
4. Can a collection move to another access root? If it cannot, the position of an object is Immutable and can be cached without a limit.
5. Does any request need one snapshot for all its reads (REPEATABLE READ) where it has READ COMMITTED today? A resolution reads its refs, the invalidation set and the policy in separate statements.
