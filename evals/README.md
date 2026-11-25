# mudcat evals

Status: scenarios only. There is no mudcat implementation yet, and no runner. Each eval is a simulated interaction with the system: a start state, a sequence of actions, and the results that mudcat must give. The evals come from the five grounding use cases in [docs/use-cases.md](../docs/use-cases.md).

## Files

| File | Use case | Evals |
|---|---|---|
| [uc1-curation.yaml](uc1-curation.yaml) | 1. Curation of video and sensor data | 6 |
| [uc2-science-training.yaml](uc2-science-training.yaml) | 2. Training data for scientific models | 6 |
| [uc3-rights-removal.yaml](uc3-rights-removal.yaml) | 3. Rights and removal for training content | 7 |
| [uc4-retrieval.yaml](uc4-retrieval.yaml) | 4. Governed retrieval for assistants | 6 |
| [uc5-facility.yaml](uc5-facility.yaml) | 5. A shared facility with many small groups | 6 |

`check.py` validates the files: structure, action names, and requirement IDs. Run `python3 evals/check.py` from the repository root.

## Shape of an eval

```yaml
- id: UC1-01                 # use case and number
  title: ...
  variant: happy-path        # see the list of variants below
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

An eval passes when every `expect` statement is true. A runner will later map each action to a call of the mudcat interface, and each `expect` statement to a check.

## Variants

Each use case has evals of different kinds, so that the evals test more than the normal path.

| Variant | Tests |
|---|---|
| `happy-path` | The normal sequence, from start to end |
| `concurrency` | Two or more actors that act at the same time |
| `policy` | A request that usage policy or access control must refuse or change |
| `removal` | Deletion, invalidation, purge or erasure, and their effects |
| `time` | Changes in time: expiry, embargo, history, new versions |
| `placement` | Replicas, sites, jurisdiction and storage that mudcat does not control |
| `isolation` | Tenants, groups and subscriptions that must not see the data of others |
| `failure` | A fault in a part of the system or outside it |

## Action vocabulary

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

Each eval lists its `gaps`: the design or the requirements that it needs and that the documents do not yet contain. `check.py` prints all gaps. They are the next items for the model, the architecture and the requirements.
