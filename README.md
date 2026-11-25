# mudcat

**mudcat** is a design for an open-source **mu**ltimodal **d**ata **cat**alog. It is meant for scientific data management, data science and AI training data. It is a successor in spirit to SLAC's [Datacat](https://github.com/slaclab/datacat). It takes ideas from iRODS, Rucio, the Rubin Data Butler, DataHub and the lakehouse catalogs.

Status: design sketch. There is no code yet.

## What it is for

For any piece of data and any model, mudcat answers five questions:

1. **What is it?** Its structure, schema and metadata, in any format: tables, arrays, streams, hierarchical files or opaque objects.
2. **Where did it come from?** Its source, and the provenance chain that produced it.
3. **Where are its copies?** Its replicas on storage resources, across clouds, regions, HPC centres and tape, with verified checksums.
4. **Who may use it, and for what?** Access control, and usage policy such as licences, consent, embargo, legal holds and takedowns.
5. **What was built from it?** Derived datasets, training sets, models, evaluations and, in science, the experiments that tested a model's predictions.

## Documents

| Document | Contents |
|---|---|
| [docs/model.md](docs/model.md) | The conceptual data model: RDF vocabularies (PROV-O, DCAT, CSVW, and ODRL for terms of use), Zanzibar-style access control, and how it maps to Datacat, iRODS, Rucio and the Butler |
| [docs/architecture.md](docs/architecture.md) | A high-level architecture: components, storage planes, main flows, deployment profiles and a build order |
| [docs/hosted.md](docs/hosted.md) | How mudcat can run as a hosted service: control plane and data plane, credentials, tenancy, regions and operations |
| [docs/requirements.md](docs/requirements.md) | The system requirements, written to the NASA guidance for requirements, with a rationale and a verification method for each |
| [docs/fields.md](docs/fields.md) | Guidelines for each scientific and technical field: domain semantics, schema profiles, formats and examples |

The research behind the design is in the Datacat fork, at [brianv0/datacat, branch `claude/cool-keller-domhwr`](https://github.com/brianv0/datacat/tree/claude/cool-keller-domhwr). Its `reports/` directory holds the two reports and `research_notes/` holds the notes that support them.

## Design principles

- **Register, don't replace.** Lance, Parquet, Iceberg, Zarr, HDF5, MCAP and the formats of each field stay as they are. mudcat describes them, tracks their copies and records their use.
- **Standard vocabularies first.** The model uses PROV-O, DCAT 3, CSVW and ODRL. mudcat defines its own terms only where no standard term exists. JSON-LD is the exchange format. RDF describes the model, but it is not the storage engine.
- **Not everything is a table.** Every version carries a structure descriptor. mudcat lists elements only when the data itself lists them. It addresses arrays and streams by coordinates and time spans instead.
- **Immutable things, mutable pointers.** Versions, snapshots and provenance records never change. Names, refs and lifecycle states can change.
- **Optional structure has explicit defaults.** Versions, refs, partitions, representations, replicas, instances and schemas each have a real default value. Data with no such structure needs no extra work, and the structure can be added later without migration. Requests can omit these dimensions. Records that must be reproducible state them explicitly. Physical dimensions never affect identity or content hashes.
- **Outside the read path.** mudcat resolves names to replicas and issues credentials. It never proxies data reads.
- **Pluggable authorization.** mudcat models access control as Zanzibar-style relationships. mudcat itself can serve access control, or an external service such as SpiceDB or OpenFGA can serve it. The external service can be self-hosted or managed. mudcat always evaluates usage policy.
- **Small to run.** The only necessary dependency is a single PostgreSQL database. Search, vector and identity indexes are optional. They are derived, and you can build them again.

## Development process

The project will build mudcat with constitution-based, spec-driven development. It will use GitHub Spec Kit or a similar tool (not yet chosen). The documents here are design input, not specifications:

- **Constitution.** The design principles above are the first draft of the project constitution.
- **Requirements.** The requirements document gives the inputs for the specifications.
- **Specifications.** Each step in the architecture's build order is to become a feature specification. The plan and tasks for that step then come from the specification.

## Non-goals

mudcat is not a storage format, a query engine, a transfer tool, a workflow engine or an experiment tracker. It integrates with those.

## Name

mudcat is short for multimodal data catalog. The name also refers to Datacat, and to the catfish of the same name.
