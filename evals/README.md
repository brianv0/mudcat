# mudcat evals

Status: definitions only. There is no mudcat implementation yet, and no runner. The evals come from the five grounding use cases in [docs/use-cases.md](../docs/use-cases.md). They have three layers.

| Layer | File | Content |
|---|---|---|
| Invariants | [invariants.yaml](invariants.yaml) | 16 statements that are true after each step of each eval |
| Task templates | [tasks.yaml](tasks.yaml) | 15 tasks with variation. `generate.py` expands them into 280 variants |
| Reference scenarios | [scenarios/](scenarios/) | 31 fixed sequences of actions, which show one possible path through a task |

Run `python3 evals/check.py` from the repository root. It validates the three layers, and prints the coverage and the gaps.

## How a task is designed

A task is a simulated interaction with the system. An actor has a goal. The actor can be a person, a pipeline or an AI agent with the mudcat tools. The task says nothing about the steps. A runner builds the start state, gives the goal to the actor, and then examines the end state.

These rules make a task effective:

1. **The check is on the outcome, not on the steps.** A check is a statement about the end state: what the catalog contains, what the actor received, what the audit log and the change stream show. Two different sequences of calls that give the same end state get the same result.
2. **Parameters make the start state.** The fixture has parameters. A variant is one set of values. An actor cannot pass a task from memory of an earlier variant.
3. **An oracle gives the expected outcome.** Each template has rules that compute the outcome from the parameters. The rules are a small decision table for one part of the design. If a rule is wrong, the design has an error, and the correction is in one place.
4. **Each outcome has a minimal pair.** A minimal pair is two variants that differ in one value and have different outcomes. A system that always permits, or always refuses, fails one variant of each pair. `generate.py` refuses a template in which an outcome has no minimal pair.
5. **Each axis has a role.** A *decision axis* changes the expected outcome or the checks. A *cover axis* must not change the outcome: the number of members, the number of writers, the interface. The generator makes all combinations of the decision values. For cover axes, it makes sure that each value occurs with each outcome. A variant also records that its outcome is the same for each value of the cover axes.
6. **No axis without effect.** `generate.py` refuses a decision axis that never changes the outcome or the checks, an oracle rule that decides no variant, and an outcome that no variant has.
7. **Checks include what must not occur.** Many defects are an effect that is too large: a removal that changes an old snapshot, a revocation that hides a document from a different user. Each task has checks for effects that must be absent.
8. **Distractors.** Some fixtures contain items that look relevant and are not: a second dataset with related elements, an artifact with the same identifier in a different tenant. A check makes sure that they did not change the answer.
9. **Invariants apply to each variant.** A variant can pass its own checks and still break a general rule. The runner checks the invariants of the template after each step.

## Task templates

```yaml
- id: T3.1
  use_case: 3
  name: policy-gate
  goal: Get data for an action and a purpose. ...
  axes:                          # decision axes
    action: [train, evaluate]
    purpose: [research, commercial]
    sources: [all-permit, one-lacks-purpose, ...]
  cover:                         # cover axes
    members: [10, 10000]
  exclude:                       # combinations that make no sense
    - {...}
  fixture: A dataset with ... {sources} ... {action} ...
  distractors:
    - ...
  oracle:                        # the first rule that matches gives the outcome
    - if: {sources: one-lacks-purpose, purpose: commercial}
      then: {outcome: refused}
    - else: {outcome: permitted}
  checks:
    - outcome: refused           # only for variants with this outcome
      when: {action: train}      # only for variants with these values
      check: The error names the source ...
  invariants: [INV-9, INV-14]
  verifies: [UP-1, UP-2]         # requirement IDs from docs/requirements.md
  reference: UC3-01              # a scenario that shows one possible sequence
```

| Task | Goal of the actor | Decision axes | Cover axes | Variants |
|---|---|---|---|---|
| T1.1 resolve-and-read | Get the data of a dataset for a training job | structure, storage mode, invalidated members | number of members | 30 |
| T1.2 concurrent-commits | Many writers change one branch, and no change is lost | kind of change, overlap of members | number of writers | 7 |
| T1.3 stage-near-compute | Have a verified copy near the cluster, then resolve there | fault in the transfer, place option | kind of target, number of members | 12 |
| T2.1 combine-methods | Publish one dataset from two | methods, declared equivalence, step | | 8 |
| T2.2 leakage-check | Find out if test data is related to training inputs | criterion, number of related elements | | 12 |
| T2.3 reproduce-later | Get the training inputs again after one year | change since the first resolution | | 7 |
| T3.1 policy-gate | Get data for an action and a purpose | action, purpose, state of the sources, scope | | 40 |
| T3.2 removal | Remove content at one of five levels | level, legal hold, point of removal, storage | | 60 |
| T3.3 content-summary | Make the summary of training content | kinds of source, base models, completeness of records | number of sources | 20 |
| T4.1 stream-consumer | Keep an outside index equal to the catalog | state of the consumer, rollbacks | number of writers | 9 |
| T4.2 retrieval-after-change | The next request shows a change at once | kind of change, kind of request | | 18 |
| T5.1 tenant-isolation | Get only the data of the own tenant | token, endpoint, target | interface | 28 |
| T5.2 embargo | Read a dataset that has an embargo date | time, actor, data in a DOI snapshot or not | | 12 |
| T5.3 membership-source | Access follows the membership of an outside team | event, kind of grant | kind of request | 9 |
| T5.4 tenant-move | Move a tenant to its own database | fault, consumer, writes during the move | number of versions | 8 |

Variants for each use case: 49, 27, 120, 27 and 57.

`generate.py` has these options:

| Command | Result |
|---|---|
| `python3 evals/generate.py` | Validates the templates and prints the number of variants, the outcomes and the minimal pairs |
| `python3 evals/generate.py --show T3.1` | Prints each variant of one template: parameters, expected outcome, checks |
| `python3 evals/generate.py --pairs T3.1` | Prints the minimal pairs of one template |
| `python3 evals/generate.py --write` | Writes all variants to `evals/generated/variants.jsonl`, one JSON object for each line |

The file `variants.jsonl` is derived. It is not in the repository.

## Invariants

An invariant is a statement that is true after each step of each eval, for example "a published snapshot never changes its members or its Merkle hash". Each invariant has an `observe` statement, which says how a runner examines it. Invariants are the base for a later layer of evals: random sequences of actions, where the runner checks only the invariants.

## Reference scenarios

A scenario is one fixed sequence: a start state, steps with an actor and an action, and the result that mudcat must give after each step. Scenarios show how the parts of the system work together in one path. They do not vary, and they prescribe the steps. Thus they are examples and not the primary evals. Each task template refers to one scenario.

| File | Use case | Scenarios |
|---|---|---|
| [uc1-curation.yaml](scenarios/uc1-curation.yaml) | 1. Curation of video and sensor data | 6 |
| [uc2-science-training.yaml](scenarios/uc2-science-training.yaml) | 2. Training data for scientific models | 6 |
| [uc3-rights-removal.yaml](scenarios/uc3-rights-removal.yaml) | 3. Rights and removal for training content | 7 |
| [uc4-retrieval.yaml](scenarios/uc4-retrieval.yaml) | 4. Governed retrieval for assistants | 6 |
| [uc5-facility.yaml](scenarios/uc5-facility.yaml) | 5. A shared facility with many small groups | 6 |

```yaml
- id: UC1-01                 # use case and number
  title: ...
  variant: happy-path        # happy-path, concurrency, policy, removal, time, placement, isolation or failure
  given:                     # the start state, as statements
    - ...
  steps:
    - actor: site-agent      # who acts
      do: register           # one action from the vocabulary below
      with: {...}            # parameters of the action
      expect:                # results that mudcat must give
        - ...
  verifies: [ST-1, RD-2]     # requirement IDs from docs/requirements.md
  gaps:                      # design or requirements that do not exist yet
    - ...
```

### Action vocabulary

This vocabulary is the first sketch of the operations of mudcat. It is not an interface definition.

| Group | Actions |
|---|---|
| Namespace and storage | `create_collection`, `register`, `verify`, `report_inventory`, `add_rule`, `retire_replica` |
| Metadata | `declare_field`, `write_metadata`, `tag`, `enrich`, `review` |
| Datasets | `commit`, `publish`, `create_ref`, `clone`, `select`, `changes`, `export`, `mint_doi` |
| Policy | `set_terms`, `declare_equivalence`, `invalidate`, `purge`, `erase`, `place_hold`, `release_hold` |
| Access | `grant`, `revoke`, `sync_membership`, `sign_in`, `create_tenant`, `move_tenant` |
| Use | `resolve`, `read`, `record_run`, `publish_model`, `evaluate`, `check_leakage` |
| Find | `search`, `query`, `lineage`, `report` |
| Stream | `subscribe`, `backfill`, `poll` |
| Simulation | `time_passes`, `fault` |

## What the evals show about the design

- Each scenario lists its `gaps`: the design or the requirements that it needs and that the documents do not yet contain. `check.py` prints all gaps.
- Each oracle is a decision that the design must make. Some oracles contain decisions that the documents do not yet state:
  - An add-only commit succeeds with no retry.
  - A legal hold blocks purge and erasure, but not invalidation.
  - A resolution with a strict place option fails when the copy near the cluster is not complete.
  - A summary of training content states which runs have no resolution.
- The tasks and the invariants cover 63 of the 128 requirements. Requirements for performance and for federation have no task yet.

## Not yet done

- A runner, and a simulated mudcat that the runner can use before an implementation exists.
- Random sequences of actions with the invariants as the only checks.
- Tasks with a measured result: time, number of calls, cost. All checks are now true or false.
- Tasks for the reserve use cases.
