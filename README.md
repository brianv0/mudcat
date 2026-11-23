# mudcat

**mudcat** is a design for an open-source **mu**ltimodal **d**ata **cat**alog. It is meant for scientific data management, data science and AI training data. It is a successor in spirit to SLAC's [Datacat](https://github.com/slaclab/datacat), and it takes ideas from iRODS, Rucio, the Rubin Data Butler, DataHub and the lakehouse catalogs.

Status: design sketch. There is no code yet.

## What it is for

For any piece of data and any model, mudcat answers five questions:

1. **What is it?** Its structure, schema and metadata, whatever the format: tables, arrays, streams, hierarchical files or opaque objects.
2. **Where did it come from?** Its source, and the provenance chain that produced it.
3. **Where are its copies?** Its replicas on storage resources, across clouds, regions, HPC centres and tape, with verified checksums.
4. **Who may use it, and for what?** Access control, and usage policy such as licences, consent, embargo, legal holds and takedowns.
5. **What was built from it?** Derived datasets, training sets, models, evaluations and, in science, the experiments that tested a model's predictions.

## Documents

| Document | Contents |
|---|---|
| [docs/model.md](docs/model.md) | The conceptual data model, expressed with RDF vocabularies (PROV-O, DCAT, CSVW, ODRL), and how it maps to Datacat, iRODS, Rucio and the Butler |
| [docs/architecture.md](docs/architecture.md) | A high-level architecture: components, storage planes, main flows, deployment profiles and a build order |

The research behind the design is in the Datacat fork, at [brianv0/datacat, branch `claude/cool-keller-domhwr`](https://github.com/brianv0/datacat/tree/claude/cool-keller-domhwr). Its `reports/` directory holds the two reports and `research_notes/` holds the supporting notes.

## Design principles

- **Register, don't replace.** Lance, Parquet, Iceberg, Zarr, HDF5, FITS, ROOT, MCAP and the rest stay as they are. mudcat describes them, tracks their copies and records how they were used.
- **Standard vocabularies first.** The model is expressed with PROV-O, DCAT 3, CSVW and ODRL. mudcat defines its own terms only where no standard term exists, and JSON-LD is the exchange format. RDF describes the model; it is not the storage engine.
- **Not everything is a table.** Every version carries a structure descriptor. Elements are listed only when the data itself lists them: arrays and streams are addressed by coordinates and time spans.
- **Immutable things, mutable pointers.** Versions, snapshots, pinned training-set specifications and provenance records never change. Names, tags and lifecycle states do.
- **Outside the read path.** mudcat resolves names to replicas and issues credentials. It never proxies data reads.
- **Small to run.** A single PostgreSQL database is the only required dependency. Search, vector and identity indexes are optional, derived and rebuildable.

## Development process

mudcat will be built with constitution-based, spec-driven development, using GitHub Spec Kit or a similar tool (not yet chosen). The documents here are design input, not specifications:

- **Constitution.** The design principles above are the starting draft of the project constitution.
- **Specifications.** Each step in the architecture's build order is meant to become a feature specification, with its plan and tasks derived from it.

## Non-goals

mudcat is not a storage format, a query engine, a transfer tool, a workflow engine or an experiment tracker. It integrates with those.

## Name

mudcat is short for multimodal data catalog. The name is also a nod to Datacat, and to the catfish of the same name.
