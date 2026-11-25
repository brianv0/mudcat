# State that each API request reads and writes

Status: draft for decision. For each request of the HTTP interface, this document lists the state that the request touches, how fresh that state must be, and where mudcat gets it today. The column "Freshness" is a decision of the design. Where the code today is stricter than the recommendation, the row says so: those rows are the places where a cache or an external service can take a read away from the database.

Sources: the handlers in `src/api.rs` and the functions that they call, and the traces of the connection to PostgreSQL (see [performance.md](performance.md), "Round trips to the database"). Round trips are counted for five requests; the others are not measured yet.

## Freshness classes

| Class | Meaning | A stale value causes |
|---|---|---|
| **Current** | The value that is committed when the request starts | A wrong result that the request cannot repair |
| **Bounded** | At most 10 seconds old (the bound is a decision that is not in requirements.md yet; AC-7 says "each check that starts after the removal") | Access that continues, or a grant that is not seen, for at most the bound |
| **Derived** | A copy that follows the catalog with a delay, and that the catalog corrects before the answer leaves | Nothing wrong in the answer; a result can be missing for the delay |
| **Fixed** | Cannot change after it is written | Nothing: it can be cached without a limit |

## State that every request touches

| State | Read or write | Freshness | Why | Source today |
|---|---|---|---|---|
| Tenant of the endpoint (hostname to tenant) | Read | Bounded | A new tenant is found on a miss; a renamed endpoint is rare | Registry in the process; the table on a miss |
| Token (signature, audience, expiry) | Read | Fixed for the signature; expiry by the clock | A token is valid until it expires; mudcat has no revocation list | Verified in the process, no database |
| Location and status of the tenant (home marker) | Read | **Current** | A stale location loses writes and answers from an empty database | The database, in the first statement of each transaction; a write reads it again with the fence |
| Row-level security (tenant of the transaction) | Read | **Current** | Isolation between tenants; never cached | The database, each statement |
| Audit entry of the decision | Write | Current | A denial must stay in the log when the request fails (AC-8) | The database, a transaction of its own |

## State of an access decision

Each request below makes one or more access decisions. A decision reads this state (15 round trips today as its own transaction, 12 inside another transaction):

| State | Freshness recommended | Freshness today | Why |
|---|---|---|---|
| Groups of the caller, and the age of each membership | Bounded | Current | A removed member keeps access for the bound |
| Grants on the tenant, the artifact, the access root | Bounded | Current | A removed grant keeps access for the bound |
| Exclusion of the caller from an artifact | Bounded | Current | The same as a grant |
| Condition of a grant (embargo date) | Current, from the clock | Current | The date is in the tuple; only the clock decides |
| Position of the object (version to artifact to access root) | Bounded, or Current if a collection can move to another access root | Current | A stale root gives the grants of the old root |
| Compartments from tags | Bounded | Current | To confirm: removal of a person from a compartment |
| Compartment of a legal hold on invalidated content | **Current** | Current | Held content that is invalidated must be closed to readers at once |

The rows where the two columns differ are the reads that a decision cache, or an external authorization service, can take away from the database.

## Requests

### Reads

| Request | State | Freshness | Source today | Round trips today |
|---|---|---|---|---|
| `GET /entities/{id}`, `GET /entities/{id}/jsonld` | Kind of the entity; one access decision; the state document of the entity | Decision: Bounded. Document: Current | Database | Not measured |
| `POST /query` | Access roots that the caller can read; collections, artifacts, versions, the typed attribute index, payloads; the effective-policy cache for `permitted()`; the lineage relations for `used_in()` and `derived_from()` | Roots: Bounded. Rows: Current. Policy cache: Current for invalidations and expiry, which are read with it | Database | 8 for a first page |
| `POST /search` | The search index; then, for each result, the newest version, the invalidation set and one bulk access decision; one audit entry | Index: **Derived**. Checks of the results: invalidation Current, access Bounded | Index tables in the database, then the catalog | Not measured |
| `POST /resolve` | Refs to snapshots; manifests; selections; the invalidation set; usage grants of every source; jurisdiction of the caller and of each collection; replicas and their state; access decisions for each access root. Writes: the resolution record, credentials, audit | Refs: Current. Snapshots and manifests: **Fixed**. Invalidations: **Current**. Usage policy: Current (to confirm: an ended grant must stop the next resolution). Replicas: Current. Access: Bounded | Database; manifests from the manifest store | 54 for 10 members |
| `POST /resolutions/{id}/again` | The recorded resolution (Fixed); then invalidations, policy, replicas and access as for a resolution | As `POST /resolve`; the inputs do not move | Database | Not measured |
| `GET /lineage/{id}` | One access decision; activities, usage and derivation relations up to the limits | Decision: Bounded. Relations: Current | Database | Not measured |
| `GET /datasets/{id}/changes` | One access decision on the newer snapshot; the manifests of both snapshots | Decision: Bounded. Snapshots: **Fixed** | Database; manifest store | Not measured |
| `GET /snapshots/{id}/export` | One access decision (export); the snapshot, its sources and their duties | Decision: Bounded. Snapshot: Fixed. Duties: Current | Database | Not measured |
| `GET /stream` | Position and floor of the stream; published events; for a subscription, the identity of the subscriber and what it can read | Events: Current up to the horizon of the relay. Filter: Bounded | Database | Not measured |
| `POST /stream/backfill` | State documents of all entities that the subscriber can read, and the position that they are consistent with | Documents and position: Current, one snapshot. Filter: Bounded | Database | Not measured |
| `GET /iceberg/v1/config`, `/namespaces`, `/namespaces/{ns}/tables/{table}` | Collections and datasets that the caller can read; the head snapshot of a table | Access: Bounded. Head: Current | Database | Not measured |
| `GET /openapi.json`, `GET /context.jsonld` | None | Fixed | The process | 0 |

### Writes

A write reads the home marker again with the fence held, and writes its commit record and its events in the same transaction as its rows.

| Request | State read | State written | Freshness of the reads | Round trips today |
|---|---|---|---|---|
| `POST /register` | Collection by path and its access root; write permission on it; the artifact by name; a version with the same checksum | Artifact, version, replica, identifiers, identity index, entities, events | Collection and permission: Current today; permission could be Bounded. The same-checksum lookup: **Current** (registration is idempotent) | 18 for one artifact |
| `POST /commit` | The dataset; write permission; the branch ref, locked; the members that the commit names in the parent snapshot; the methods of the members | Snapshot, manifest references, ref and its history, entities, events | Ref: **Current**, under its lock (the requirement of a commit). Permission: could be Bounded | 35 for one member |
| `POST /lineage/openlineage` | Datasets and versions that the event names; write permission | Activity, usage, generation, the run record as a version, events | Named inputs: Current | Not measured |
| `POST /stream/subscriptions` | Read permission on the scope | Subscription with the identity of the subscriber | Permission: Bounded | Not measured |

### The MCP endpoint

`POST /mcp` calls the operations above (search, query, resolve, lineage, describe) for an agent. Each call has the state and the freshness of the request that it stands for.

## What follows

- **The decision cache** covers every row marked Bounded: groups, grants, exclusions, access roots that a caller can read. It is keyed by tenant, caller and access root, and an entry is not used after the bound. A server that cannot refresh an entry denies (AC-6).
- **Never cached:** the home marker, row-level security, the invalidation set, the compartment of a legal hold, a branch ref under a commit, the same-checksum lookup of a registration.
- **Cached without a limit:** published snapshots and their manifests, recorded resolutions, token signatures.
- **Requirements to change if the bound is accepted:** AC-7 ("each check that starts after the removal") and the check of T4.2 that no cached decision was used.

## Open points for the owner

1. Compartments from tags: Bounded, or Current when a person is removed from a compartment?
2. Usage grants of a source: must an ended or withdrawn grant stop the next resolution (Current), or within the bound?
3. Is the bound fixed at 10 seconds, or a setting of a tenant with 10 seconds as the highest value?
4. Can a collection move to another access root? If it cannot, the position of an object is Fixed and can be cached without a limit.
