# Handoff notes

State of the work on the branch `claude/happy-mendel-k47pg0` at 2026-11-25, written so that a new session, or this one after its context is compacted, can continue without the conversation. The other documents say what the code does; this one says what is decided, what is open and what is next.

## Where things are

- Everything is committed and pushed. CI (`ci` workflow: build, `cargo test`, the smoke scale of the evals) passes at the head.
- Evals: 280 of 280 variants at the full scale, recorded with its commit in `evals/results/latest-full.json`. That run is at 344481b; the changes after it (round trips, the second batch of checks) passed the smoke and quick scales only.
- Mutation gate: 23 seeded defects, all detected (`evals/results/mutation-gate.txt`, run by the second verification session; the reviewing session ran 4 of the 10 new ones again and got the same).
- No pull request exists. None was asked for.

## Decisions that the owner made in conversation, not yet in requirements.md

1. **Access control may be eventually consistent with a bound of about 10 seconds.** This changes AC-7 ("each check that starts after the removal") and the check of T4.2 that no cached decision was used. Neither is edited yet.
2. **The request-state document lives at `docs/request-state.md`** and is written in consistency classes (Serialized, Strong, Bounded staleness, Eventual and checked, Immutable).
3. **Some lookups of an access decision should become joins.** A path cache like the VFS layer of Datacat is a possible later step, not the first one.
4. **Leader election, if it is ever needed, can be in PostgreSQL.** FoundationDB is not wanted for on-premises deployments, and OP-1 names PostgreSQL as the only service.

## Open questions for the owner

From `docs/request-state.md`:

1. Compartments from tags: Bounded staleness, or Strong when a person is removed from a compartment?
2. A usage grant that ends: must it stop the next resolution, or within the bound?
3. Is the bound fixed at 10 seconds, or a setting of a tenant with 10 seconds as the highest value?
4. Can a collection move to another access root? If not, the position of an object is Immutable.
5. Does any request need one snapshot for all its reads (REPEATABLE READ)?

From earlier in the work:

6. Round-trip budgets as requirements. PF-3 to PF-6 are still TBD. Proposed: a read at most 2 round trips, a write at most 3, for any size.
7. Should the rules for a tenant move with more than one process, and the corrected rule for the order of events, be written into `hosted.md` and `architecture.md`? They are in `docs/implementation.md` ("Findings") only.
8. The short list of things that no result of an experiment may change (asked; the answer was "not sure yet").

## Next work, in order

1. When questions 1 to 3 are answered: reword AC-7 and the check of T4.2.
2. Rework the access decision into joins: one statement for the position of the object (shared with an external adapter), one for grants, one for compartments. Lower its budget in `tests/round_trips.rs` (15 today; two or three expected, not measured).
3. Trace register, commit and resolve again (18, 35 and 54 round trips today) and fold their statements the same way.
4. Add the decision cache for the reads of the class Bounded staleness, with a test that fails if a revocation lasts longer than the bound.
5. Only then decide on a path cache.

## Likely defect, not confirmed

`src/query.rs` filters the results of a query by the access roots that the caller can read, and by nothing else. It has no code for the exclusion of a caller from an artifact or for compartments, so a query can list an artifact that the caller cannot read. Search checks each result again; a query does not. No test shows it yet. Whether results that are datasets have the same gap is not checked.

## Things to know

- **Other sessions.** `session_01FgpRrps2mV1GX4hH72qjPc` (first verification batch) is idle and waits for a go-ahead that is no longer needed. `session_01FgpRrsnL5eSyvgmg8FUAcB` (second batch) is done and merged. Both can be archived; the owner decides. Their branch is `claude/happy-mendel-k47pg0-verify`, and all of it is merged.
- **Do not share one cargo target directory between two checkouts.** Cargo gives both the same artifact names, and the older checkout then builds against the library of the newer one without an error.
- **The wire tracer** (a TCP proxy and a decoder of the PostgreSQL protocol, two Python scripts) is not in the repository. It made the round-trip traces. It is easy to write again; say so if it should be kept.
- **Limits that are documented and not solved:** a move whose process stops leaves the tenant read-only until an operator resets its status; a dedicated database is on the same server as the shared one; all server processes of an instance need the same data directory; a server must start again after a change of the schema (`docs/implementation.md`, "Not implemented").
- **`evals/README.md`** has "What the evals do not detect" and "Not yet done". The detection rate that carries information is 3 of 10 on the second sample, before its checks were strengthened.
