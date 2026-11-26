# mudcat open items

Status: working notes. This document holds what the design discussion decided or proposed and what the other documents do not yet contain. It also holds the decisions that need a confirmation, and the conventions of the project. Remove an item when the other documents contain it.

## 1. Agreed in discussion, not yet in the documents

### The reason for the project

The README states five questions that mudcat answers. It does not yet state why the project exists. The agreed points, whose wording is not final:

1. AI organisations, and startups most of all, move from databases and data lakes to files.
2. Science moves in the other direction at the same time: to tabular data and data lakes. High-energy physics is the clearest example.
3. The common direction is multimodal data, and one system that manages all of it is useful.
4. One place for access control and policy, for each format and each location, is good for all parties.
5. Iceberg won the contest of table formats. Many of its ideas also apply to files.
6. Iceberg describes storage and not meaning. It never had strong metadata. Thus a catalog must hold the metadata.
7. The metadata in the catalog must be the source of truth for knowledge graphs and for retrieval by assistants.

### Partitions and layout

- A dataset can have **partition specs**, with evolution as in Iceberg: a new spec applies to new manifests, and old manifests keep their spec.
- Only built-in transforms at first: identity, bucket, truncate, and the transforms on dates and times. Custom functions, for example in WebAssembly, are a later option.
- Names: a **manifest** is a list of members. An **element list** is a list of the elements of one version. mudcat has no separate manifest list. Each manifest carries the ID of its partition spec and a summary of its partition values.
- The content hash of a snapshot does not depend on the layout. A change of the partition spec, or a merge of manifests, does not change the hash.
- Partitions and versions follow the principle "optional structure has explicit defaults": data with no partition spec has one default partition.

### Rules for datasets

- A dataset has members **by version**. A member is a version of an artifact, not the artifact.
- A dataset can contain another dataset **by snapshot**.
- A **follow rule** makes a dataset take the newest version of an artifact, or the newest snapshot of another dataset, in a new snapshot of its own. A follow rule is not the same as a Rucio subscription.
- A single artifact that is used with no dataset is an **implied dataset** with one member.
- The definition of a dataset: a named, versioned set of artifacts that mudcat treats as a unit. It is what Datacat called a dataset group. It is not always a collection.

### Storage

- **Enforcement levels** for a storage resource: `catalog` (mudcat issues all credentials), `site` (the site applies rules that mudcat gives it) and `none` (mudcat only records). On HPC file systems, policy is an audit and not a prevention. A site can synchronise rules to Lustre, GPFS or WEKA, but each one differs.
- **Mounts:** a storage resource can appear at a path of the collection namespace.
- **Version IDs of the provider:** a replica records the version ID that an object store with versions gives it, for example in S3.
- Management modes (managed, external, reference) are in the model.

### Retrieval

- **Retrieval** is an action in usage grants, next to training and evaluation.
- Provenance has a **retrieval role**, so that an answer of an assistant can refer to the versions that it used.
- A **graph gateway** serves the catalog as the source of truth for knowledge graphs.

### Tenants and organisations

- An **organisation level** above tenants, for a company or a collaboration with several tenants.
- **Soft tenants** in a community instance: small groups that share one instance with less isolation than full tenants.
- The tenant ID can be derived from a signing key of the tenant. This is not decided.
- A clone of a tenant shares manifests and data by reference. But the catalog rows of the clone are new rows in PostgreSQL, so a clone of a large tenant is not cheap. The documents must state this.

### Sign-in and membership for small groups

- **Linked identities:** one person, several sign-in methods.
- **Membership connectors** for GitHub organisations and teams, GitLab groups, Slack workspaces and Google Workspace groups. CILogon gives identity only, and no group membership.
- Sign-in from a terminal with the device flow of GitHub. A personal access token is the fallback, also on HPC systems.
- A **pipeline identity** for jobs that run with no person.

### Concepts that are absent

- **Splits:** the training, validation and test parts of a dataset, as Croissant describes them.
- **Cards:** a dataset card and a model card as generated descriptions.
- **Job and run:** pipelines and runs are in the model. A job that is not a run of a pipeline has no concept of its own.

### Names that are not final

- Storage resource, or storage location.
- "Version" for an artifact and "snapshot" for a dataset: keep both, or use one word.
- Representation as an entity, or as a relation between snapshots.

### Database engines

The discussion considered CockroachDB, and FoundationDB with an SQL layer, as engines that make hierarchies fast. Datacat needed a virtual file system cache for that. The decision is PostgreSQL first. The question is open for the largest deployments.

## 2. Decisions that need a confirmation

The assistant made these decisions while it wrote the documents and the evals. Each one is in a document, but the owner of the project did not confirm it.

| Decision | Where |
|---|---|
| "Tenant" is the unit of authority, and "instance" is a running deployment | model, section 1 |
| A discovered version has an unknown source that grants nothing. It cannot be used for training until an owner claims it | model, section 1 |
| A person with no access to a discovered dataset does not find it in a search. Goods showed metadata to each engineer | search.md, evals T6.2 |
| A legal hold blocks purge and erasure, but not invalidation | evals T3.2 |
| A resolution with a strict place option fails when the copy near the cluster is not complete | evals T1.3 |
| Onward use means a deployment, a published snapshot or a use by a different pipeline. An evaluation in the same run does not count | model, section 4 |
| The description of a published snapshot does not change. A correction goes on the dataset | model, section 4 |
| mudcat sets `sameAs` only when two content hashes are equal, and refuses a statement of a person that says otherwise | evals T8.3 |
| A request for own data and public data together is refused completely when the terms of the public data do not permit it | evals T8.4 |
| When public content changes with no new version, mudcat keeps the old checksums and makes new versions | evals T8.5 |
| A public snapshot needs a name and a description of 50 to 5000 characters | model, section 4 |
| Lifetime classes are `durable`, `intermediate` and `cache` | model, section 4 |
| One conflict refuses a registration commit completely. No mode commits a part | architecture, Registration commits |
| The options of the Iceberg compatibility kit: server-side retry on, location override on managed storage only, names with a dot off | evals/conformance |
| The gateway has no views and no commits to more than one table in its first scope | architecture, Conformance |
| The latency target of 200 ms for search comes from a rule for interactive systems that the assistant gave from memory | search.md |
| A control mark always propagates, with the most restrictive value, to statistics and embeddings also | model, section 3 |
| Controlled data is a use case in reserve, with a profile, and not a grounding use case | use-cases.md |
| The operators of an instance get the same access checks as all other principals | requirements, AC-10 |
| A controlled tenant has its own database, and cannot be in the shared tier | requirements, OP-25 |
| mudcat stores no password of a user. Sign-in for small groups with tokens of an outside service needs a check against this | requirements, AC-16 |

## 3. Known gaps

- **No implementation and no runner.** The evals are definitions. Nothing runs them.
- **The search benchmark has no generator** for its corpora and queries. Fifteen of its 21 targets are estimates.
- **The reference scenarios list 62 gaps.** `python3 evals/check.py` prints them.
- **Open in the registration commit:** how staged entries become visible in PostgreSQL in a short transaction, and the time after which a staged part expires.
- **Open in commits:** a read as of a commit can wait for the relay that orders commits. Nobody measured this wait.
- **Not checked:** the licences of the ACORDAR and NTCIR test collections, and the release of Iceberg whose compatibility kit mudcat must pass.
- **Many values are TBD or TBR** in requirements.md.
- **A sketch of the database schema exists outside the repository,** by decision. It is older than the tenant key, the commit record and the registration commit, and its outbox sequence is not in commit order.

## 4. Conventions of the project

- **Language.** The documents use ASD Simplified Technical English, with the technical names of the model. `python3 tools/ste_check.py docs/*.md` reports long sentences, passive voice and some words that are not approved. Its word list is indicative only.
- **Not all data is tabular.** Each concept must work for files, archives, arrays and streams.
- **Requirements** follow the NASA guidance: "shall", one need in each requirement, a rationale and a verification method. An identifier is permanent. Values that are not known are TBD or TBR.
- **IDs are UUIDv7.**
- **No field-specific semantics in the core documents.** They go in fields.md.
- **Terms from earlier systems are welcome,** from Datacat and iRODS most of all. Agreed names: collection, artifact, version, replica, storage resource, identifier, activity, metadata payload, tag, tag ref and branch ref, enricher, delete marker, tenant, instance.
- **Use cases** take the use cases of large systems such as those at Google, and not their scale. The grounding use cases do not come from Datacat, the Fermi telescope or LSST.
- **The database schema is not in the repository** until the owner of the project asks for it.
- **Development process:** constitution-based, spec-driven development, with Spec Kit or a similar tool. The design principles in the README are the draft of the constitution.
- **Diagrams** are in D2 with the ELK layout. The D2 program is not in the repository.
- **Checks before each commit:** `python3 evals/check.py`.
- **Research notes** are in the repository brianv0/datacat, branch `claude/cool-keller-domhwr`, under `research_notes/` and `reports/`.
- **The record of the discussion** is in `docs/discussion/`. It has each question and each answer, not edited. The two answers about the database schema are not in it.
