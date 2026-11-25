# Design discussion, part 01: Research on earlier systems, and the first design

This file is a record of the design discussion: each question, and the answer as it was given at that time. Later decisions can replace what an answer says. The documents in `docs/` hold the present design, and `docs/open-items.md` holds what is not yet in them. Paths under `/tmp` and commit IDs in the answers refer to the state at that time.

## 1. research datacat. evaluate similar systems (irods, rucio, amundsen, data hub). I want to b

**Question**

> research datacat. evaluate similar systems (irods, rucio, amundsen, data hub). I want to build a better system for both real science and data science.

**Answer**

Research is underway: five parallel threads are running (the datacat codebase itself, iRODS, Rucio, Amundsen + DataHub, and the wider landscape and design patterns). Once they all report back, I'll have them synthesized into a single report with a recommendation for what the better system should look like.

## 2. especially for ai data too

**Question**

> especially for ai data too

**Answer**

The report is done and sent to you; it's also at `/home/user/datacat/reports/Datacat and data catalog systems.md`, committed and pushed to `claude/cool-keller-domhwr` with the six sets of research notes.

**Recommendation: build a successor that keeps Datacat's concepts and replaces its implementation.** Datacat's core model (dataset → version → per-site replica, typed metadata, a readable filter language) is the right foundation, and none of the systems reviewed combines it with schemas, lineage, discovery and AI-asset tracking.

- **Datacat:** the concepts hold up, but the implementation doesn't scale — metadata is spread over nine attribute-value tables and search walks the folder tree in Java. Porting it to current platforms costs about as much as rewriting the server.
- **iRODS and Rucio:** both know where bytes are but describe them poorly. Every major Rucio user (ATLAS, CMS, DUNE, Rubin) pairs it with a second metadata catalog.
- **Amundsen:** archived in September 2026, so it is only a reference design.
- **DataHub:** the best metadata model and very active, but a heavy stack (Kafka plus Elasticsearch/OpenSearch) with no concept of replicas, checksums or sites.
- **The open niche:** one transactional store linking physical replicas, versioned datasets, and the models and evaluations built from them. For AI data that means recording the same dataset version in several formats across sites, sample-level manifests, train/validation/test usage edges, and licence and takedown status that propagates to derived models.
- **Proposed shape:** a single service on PostgreSQL with schema-validated JSON metadata, stable opaque IDs, and existing standards on every outward interface (Iceberg REST, OpenLineage, Croissant, OIDC, MCP). The report lays out a five-phase build path, with replica placement rules last.

Three caveats on the evidence:
- **Old snapshot:** this checkout is a March 2016 snapshot that was read but not built or run; upstream `slaclab/datacat` may have fixed some of the defects.
- **Blocked sources:** many primary sites (irods.org, rucio.cern.ch, arxiv.org, docs.datahub.com) were blocked by this session's network allowlist, so much of the external evidence comes from search extracts, not full pages.
- **Unproven scale:** no source shows 10^10 file records in a single catalog, so the Postgres design is untested at that size.

The report names two risks to retire before committing heavily: whether the Postgres design holds at 10^8–10^9 files on real queries, and whether you have an anchor user. It argues the second matters more.

## 3. Does the adoption of data lake, especially data lakes over heterogeneous database systems,

**Question**

> Does the adoption of data lake, especially data lakes over heterogeneous database systems, change anything?  what about cloud-first companies, first class bucket support, regions, etc...

**Answer**

Yes — it leaves the core intact (stable IDs, the logical/physical split, the lineage graph, the Postgres core) but changes three things in the design, and narrows where the system is worth building. This is from my own knowledge, not a new research pass.

#### Data lakes: stop cataloguing files the table format already tracks

Iceberg, Delta, Lance and Icechunk each keep their own file inventory with snapshots. For those, the catalog should hold a pointer to the format's manifest and not duplicate a row per file. That gives three ownership modes instead of one:

| Mode | Example | What the catalog owns |
|---|---|---|
| Managed files | FITS, HDF5, ROOT, video shards | Per-file versions, checksums, replicas |
| Manifest-backed | Iceberg/Delta tables, Zarr stores, Lance datasets | Pointer to current snapshot; files indexed lazily |
| Federated | Tables in a live Postgres, Snowflake, BigQuery | A cached mirror of schema and ownership, with a sync timestamp |

Replicas also move up a level. A copy of a table in another region is "this table as of snapshot N", so replica state needs a snapshot lag, not just verified/suspect.

#### Heterogeneous databases: be the catalog the engines consult

Federated sources are mutable and have no versions, checksums or replicas, so only the discovery, lineage and policy side of the model applies to them. The catalog needs connection entities with brokered credentials, and it should expose everything through Iceberg REST so Trino, Spark and DuckDB do the cross-system joins. Do not build a query engine. Datacat's "foreign metadata" plugins would then be ordinary federated tables.

#### Cloud-first: a "site" becomes a bucket with attributes

- **Location model:** replace the flat site with provider → account → region → bucket → prefix, carrying storage class (archive tiers play the role of tape), egress cost and jurisdiction.
- **Replica resolution:** Datacat's (version, site) view becomes "the cheapest copy near the caller's compute". For AI this is the main case, since GPUs are often in a different cloud from the data.
- **Residency:** placement rules need negative constraints, such as "must not leave the EU".
- **Ingestion:** bucket event notifications plus periodic inventory reconciliation replace polling crawlers. Checksums and object version IDs come from the store instead of being computed.
- **Placement:** rules compile to the provider's own replication and lifecycle configuration. The catalog observes and verifies; it does not move bytes. This makes placement much cheaper than the Rucio-style engine in the report.
- **Access control:** ACLs are only real if the bucket is reachable solely through catalog-vended, prefix-scoped credentials.
- **Scale:** buckets with billions of objects push cold data towards prefix-level registration backed by an inventory manifest, not a row per object.
- **Availability:** a single Postgres writer is a single-region dependency. Multi-region needs read replicas or federated per-region catalogs, and I have not designed that.

#### Where this narrows the niche

For a cloud-first company whose data is mostly tables, Unity Catalog, Polaris or Gravitino already cover it, and this system adds little. The case holds for file and multimodal AI data in buckets, and for hybrid setups spanning clouds, HPC centres and GPU providers.

If cloud-first users are your anchor, Iceberg REST, credential vending and provider-delegated placement should move into phase 2 of the build path. I have not folded any of this into the report yet.

## 4. multimodal data catalog is probably a good way to anchor this

**Question**

> multimodal data catalog is probably a good
> way to anchor this

**Answer**

I agree, and it's a better anchor than either "science file catalog" or "AI data catalog" alone, because multimodal is the one case both audiences share: a survey with images, spectra and catalogues has the same shape as a robotics or video-text corpus.

It also gives the model a single unifying idea: **a dataset is a table of samples whose cells can be scalars or pointers into files** (a byte range, a chunk, a frame span). A plain table is the case with no pointers, a file collection is the case with one pointer column, and multimodal is the general case. Files and replicas stay underneath as the physical layer.

What that changes in the plan:

- **Build order:** sample manifests, representations (the same data as HDF5, Parquet or tar shards) and no-copy views move from phase 4 to phases 1–2. The Iceberg facade and placement rules move later.
- **Discovery:** the UI has to preview images, video, spectra and tensors, and filter by metadata and by embedding similarity. Text search over descriptions is not enough.
- **Access:** loaders for PyTorch, Ray, Daft and Hugging Face `datasets` become day-one clients, alongside the CLI.
- **Annotations:** labels and captions become layered, versioned sets over the same samples, since they change far more often than the raw data.
- **Science formats:** reference FITS, HDF5 and Zarr in place without rewriting them. That is the part the AI-native tools don't do.

Two cautions:

- **Crowded space:** Hugging Face Hub, LanceDB, DataChain, Daft and FiftyOne all work here. The differentiator would be replica and region awareness across clouds and HPC, plus lineage through to models and evaluations. I could not verify the current state of DataChain or Lance's namespace spec, so check those before committing.
- **Not a storage format:** keep manifests in Parquet or Lance and stay a catalog.

The report still reflects the earlier framing; say if you want it revised around this anchor and the cloud points from my last answer.

## 5. how can we build provenance into it and make it first class

**Question**

> how can we build provenance into it and make it first class

**Answer**

Make it a write-path rule instead of a feature: **no version can be published unless the catalog knows what produced it and from what.** Everything else follows from enforcing that at one gate.

#### The model

Use W3C PROV's three node types, and only draw edges between immutable things:

- **Entity:** any immutable version — dataset version, file version, representation, view, annotation set, model, evaluation result.
- **Activity:** a run, recording code commit, container image digest, parameters, seeds, and start/end time.
- **Agent:** a person, service, instrument, or model (for synthetic data and machine-generated labels).

Edges are `used` (with a role: train, validation, test, reference), `generated_by`, and `derived_from`. Because both ends are immutable, lineage never goes stale or gets orphaned by a rename, which is DataHub's recurring problem.

Ingest is an activity too, so every chain ends at a typed origin: an instrument and observation, a crawl with URL and timestamp, a vendor delivery, an annotator pool.

#### Three granularities, stored differently

| Level | Stored as | Why |
|---|---|---|
| Dataset version | Edge rows in Postgres | Small, traversed with recursive queries |
| File | Parent-file edges in the per-file tables | What science workflows already record |
| Sample | Columns in the sample manifest (`source_sample_ids`, transform step) | Billions of edges cannot be graph rows |

Sample lineage mostly comes free if sample IDs are stable and carried through transforms. Views record their predicate plus the digest of the resolved sample list, so membership is always recomputable.

#### Capture has to be automatic

Hand-declared provenance ends up empty. There are three capture sources, and each edge is tagged with which one produced it:

- **Observed:** the catalog already resolves names to credentials and URLs, so every read inside a run is recorded as a `used` edge with no user effort.
- **Declared:** an SDK run context, where outputs are published.
- **Imported:** OpenLineage events from Spark, Airflow and dbt, plus workflow engines and MLflow runs.

```python
with catalog.run(code="git:abc123", image="sha256:…") as run:
    ds = run.open("surveys/desi@v3", role="train")   # recorded
    run.publish("embeddings/desi-clip", files=out)   # edges closed here
```

Working versions may have gaps; publishing requires a closed chain back to published inputs or declared external sources.

#### Make it verifiable

Identify each version by the digest of its manifest, record input digests in the activity, and sign the result as an in-toto/SLSA attestation. That gives a tamper-evident chain from raw data to model, using an existing standard.

#### What it buys

- **Propagation:** licence, consent, embargo, takedowns and "suspect replica" flags flow downstream along the edges.
- **Contamination checks:** whether a model's training closure intersects an evaluation set.
- **Staleness:** which derived datasets have inputs with newer versions.
- **Storage policy:** anything with a complete recipe is rebuildable, so it needs fewer replicas than raw data.

#### Limits to accept

- **Reproducibility:** it is best-effort. Record seeds and environment, and mark non-deterministic steps as such.
- **Access control:** provenance needs its own ACLs, because it reveals who read what.
- **Coverage:** observed capture only sees reads that go through the catalog, so direct bucket access leaves gaps unless credentials are vended exclusively.

## 6. i was just thinking that at a minimum files, groups/folders, and some others probably need

**Question**

> i was just thinking that at a minimum files, groups/folders, and some others probably need unstructured or semi structure blob for prob or
> we need to formalize it into a schema but that it needs to be strongly integrated into search

**Answer**

Do both, split by what each part is for: formalise the skeleton, leave the payload semi-structured, and index both through the same search path as ordinary metadata.

#### What to formalise and what to leave as a blob

| Part | Form | Reason |
|---|---|---|
| Edges: what was used, what was generated, by which activity, in what role | Relational columns with real foreign keys | Traversal, propagation and the publish gate all need joins; IDs buried in a blob get orphaned |
| Activity core: agent, time, code commit, image digest | Fixed columns | Every activity has them and they are the common filters |
| Everything else: parameters, environment, instrument settings, crawl details | JSON document, optionally validated against a registered schema per activity type | Varies without limit; a rigid schema means people leave it empty |

This is the same pattern as user metadata: free-form JSON by default, with registered keys promoted to typed, indexed columns when they start to matter.

#### Where it attaches

- **Activities:** the blob lives on the activity, and files point to it. A run that writes 10,000 files then has one record, not 10,000 copies that drift.
- **Files and other nodes:** allow an inline `origin` blob as shorthand. Underneath, it creates an implicit ingest activity, so the simple case stays simple.
- **Folders and groups:** container-level provenance acts as an inherited default, the way ACLs do: "everything under `/raw/run42` came from this instrument".

#### Making it part of search

- **Same filter language:** provenance fields are addressable like metadata, e.g. `prov.params.threshold > 0.5 and origin.instrument = "LSSTCam"`.
- **Traversal operators:** `ancestors(...)`, `descendants(...)` and `derived_from(<query>)` compose with ordinary filters, as MetaCat's query language does with parents and children.
- **Denormalised facets:** each entity's search record carries a summary of its lineage: root origins, producing pipeline and code version, upstream licences. The common questions then need no graph walk and show up as facets in the UI.
- **Full text:** unregistered blob keys still get containment and text indexing, so nothing is unsearchable.

A typical query would then read: `exposure > 30 and derived_from(path:/raw/2025/** and origin.instrument = "LSSTCam") and prov.code.version < "2.3"`.

One limit: precomputed ancestry is only affordable at the dataset-version level. File-level ancestry should be walked on demand or inherited from the file's dataset version, otherwise the closure table grows faster than the catalog.

## 7. can you read the last data bugler design and see if it influences how you might think of a

**Question**

> can you read the last data bugler design and see if it influences how you might think of all
> of
> this

**Answer**

I'm reading the latest Rubin Data Butler design material now (the 2024–2026 design notes and the `daf_butler` repository), with a focus on provenance, the query system, the client/server split and what its authors say they would change. I'll come back with how it shifts the design once that's in.

## 8. when you get back, I really want you to go much harder into thinking about what a multimod

**Question**

> when you get back, I really want you to go much harder into thinking about what a multimodal data catalog for AI should look like especially, for frontier labs and for AI-for-science startups

**Answer**

The Butler read is in, and yes, it changes several things. There is no single "Butler v2" document: the design is evolving through narrow tech notes from 2024–2026, plus code. The two frontier-lab and AI-for-science threads are still running, so the deeper design comes after those.

#### What the Butler team now says about its own design

- **Multi-site:** their draft diagnosis is "We are building a distributed system, but the data management system doesn't know that the system is distributed." Each site runs an independent Butler, bridged through Rucio and Kafka. Collection structure and metadata corrections don't propagate, and failures are silent. This is the strongest outside confirmation of putting replicas in the same store as the catalog.
- **Immutability:** the newest note (October 2026) proposes immutable collection snapshots identified by a Merkle-style hash of their members, because mutable collections can't be replicated reliably or pinned.
- **Provenance:** they moved it out of database tables into one provenance graph file per run, itself a catalogued dataset. Schema changes became much easier and the catalog stays ignorant of pipeline internals.
- **Ancestry queries:** they note naive ancestry is useless, because a shared calibration links every raw file to every output.
- **Concurrent writers:** inserts from hundreds of workers took up to ten minutes. The fix was a single batching writer fed by a queue.
- **Access control:** authorization is still per repository only, and the remote client is read-only. Per-collection ACLs remain an open question.
- **Checksums:** they are off by default and never verified.

#### How that shifts my thinking

| Earlier position | Revised |
|---|---|
| Activity payload as a JSON document in Postgres | Keep only the edge skeleton and searchable summary in Postgres; store the full run record (configs, logs, per-task detail) as a catalogued file |
| Ancestry as plain graph traversal | Edges need roles, and traversal must exclude or down-weight ubiquitous inputs such as calibrations, base models and tokenizers by default |
| Synchronous transactional writes for everything | Bulk registration goes through a batched ingest path from day one; workers never write rows individually |
| Publish as a state transition | Publish also freezes membership under a content hash, so a dataset version is verifiable and replicable by ID |
| One file, one record | Many logical items inside one physical artifact is normal (Butler now zips small files and addresses members by fragment), which matches the sample-pointer model |

Two things they got right that I would copy directly: time-ordered UUIDs for index locality, and storing the reader/format name with each dataset so old data stays readable without configuration.

One thing I'd deliberately do differently: the Butler has no version field, and expresses versions through collection search order. Their own portability note says this blocks incremental workflows, so explicit versions stay.

The notes are in `research_notes/Datacat and data catalog systems/butler_latest_design.md`. Two of the sources (the multi-site diagnosis and the writeable-Butler note) are unmerged drafts, so treat them as working-group opinion, not adopted design.

I've written the deeper design up as a document and sent it to you. It is also at `/home/user/datacat/reports/Multimodal data catalog design.md`, committed and pushed with the three new sets of research notes.

**The position:** this should not be a file catalog with AI features added. It should register Lance, Iceberg, Zarr and similar formats as physical forms, and own the state nobody else holds: content identity across datasets, legal and policy state, the training recipe, lineage from data through model to evaluation, and replica placement.

The decisions that matter most:

- **Samples live in manifests, not Postgres.** Postgres tracks shards, snapshots, sources, recipes and runs; sample tables stay in columnar files registered by hash. A row per file does not survive "hundreds of billions of little files".
- **Legal state attaches to sources.** Each sample carries a source ID (a crawl, a licence deal, a vendor campaign, an instrument run, a simulation method), so policy is evaluated over millions of sources, with takedowns as per-sample exceptions.
- **The training recipe is a first-class object.** Resolving a recipe freezes the exact data and weights, and is where policy is enforced (expired licences, opt-outs, never-train data). No public system models this.
- **Contamination and leakage are one mechanism.** "No training sample is related to a test sample by relation R" covers both leaked benchmark items and proteins that are 90% identical to the test set.
- **Placement is about complete snapshots at sites.** The useful statement is "this resolved training set is complete and verified at this cluster", including entries for enclave data the catalog can never fetch.
- **Deletion is a process with receipts.** Takedowns enumerate and remove every copy; legal holds override deletion.

For science specifically, the design adds the measurement method as a structured record, versioned assays, retained negative results, and the loop from model prediction to experiment to the next training set.

**Who to build for first:** AI-for-science startups and mid-size video and robotics labs. Frontier labs are hiring to build this in-house, which confirms the need but makes them unlikely first adopters.

Two things you should know before relying on this:

- **Evidence is thin on lab internals.** Most of it is job postings, court filings and talks seen as search extracts; several gaps are inferred from nobody claiming to fill them.
- **This is no longer a Datacat successor.** It keeps three Datacat ideas (logical datasets separate from replicas, resolving the right copy for the caller's location, the filter language) and little else.

The document ends with eight acceptance queries the system must answer in one step, and the open risks. The biggest risk is the content-identity index at 10^11 samples, which should start with exact hashes only.

## 9. use standard technical english but feel free to include prior terms from the systems you r

**Question**

> use standard technical english but feel free to include prior terms from
> the systems you researched, including datacat and irods in particular. Remember that not all data is tabular

**Answer**

I've rewritten the design document in standard technical English, using the established terms, and removed the assumption that a dataset is a table. The revised file is sent to you and is at `/home/user/datacat/reports/Multimodal data catalog design.md`, committed and pushed.

**Terminology.** The document now opens with a table mapping each term to its Datacat, iRODS, Rucio and Butler equivalents: collection, data object, version, replica, storage resource, dataset, snapshot, metadata (attribute–value–unit), view, replication rule and zone. Only three terms are new, because no prior system has them: source, training-set specification, and similarity criterion.

**Non-tabular data.** Each data object version carries a structure descriptor extracted by a format plugin. The design recognises seven structures:

| Structure | Examples | Addressed by |
|---|---|---|
| Opaque object | Model weights, proprietary instrument files | Nothing; the whole object |
| Container of members | tar, HDF5 groups, ROOT trees | Member path |
| Records | Parquet, Lance, JSON Lines | Key or row range |
| N-dimensional array | Zarr, netCDF, FITS cubes | Hyperslab over named dimensions |
| Stream | MCAP, video, audio, waveforms | Channel and time span |
| Graph | Knowledge graphs, meshes | Key set |
| Composite | A robot episode, a microscopy plate | Several of the above, joined on time or an identifier |

Two consequences run through the rest of the design:

- **Elements are listed only when the data lists them.** Records and members go in manifest files. Arrays and streams are described by their coordinate system, with no enumeration.
- **A sample is defined in the training-set specification, not in the data.** For an array it is a windowing rule and for a stream it is a clip boundary, so the same data object yields different samples under different specifications.

This also changed content identity (per-chunk checksums and coordinate identity for arrays), leakage checks (range overlap on coordinates, which needs no listing), and element-level lineage (the recorded selector).

**More of iRODS and Datacat is retained than the previous revision implied:** the collection hierarchy, per-replica state and checksums, typed attribute–value–unit metadata, inherited group permissions, tickets, views, and zones for federation. Two deliberate departures from iRODS are stated: the catalog does not proxy reads, and it has a small fixed set of policy enforcement points in place of a general rule engine.

## 10. also csvw/ lsst/felis can help provide a model for describing tabular data too

**Question**

> also csvw/ lsst/felis can help provide a model for describing tabular data too

**Answer**

Agreed, and they fit well as the descriptor for the record structure. I've added this to the design document (committed and pushed); it is written from my existing knowledge of CSVW and Felis, not a fresh read of either spec.

CSVW and Felis already define what a record descriptor needs: a table group containing tables, with columns carrying a datatype, unit, nullability, description and a semantic annotation (a property URL in CSVW, an IVOA UCD in Felis), plus primary and foreign keys. Adopting that model does four things for this design:

- **Semantic columns:** the per-column annotation is where units and domain identifiers are declared, which the content-identity index and similarity criteria depend on.
- **Declared joins:** foreign keys express the link from element metadata to reference tables of runs, exposures or calibrations. That makes Datacat's "foreign metadata" part of the schema instead of a plugin.
- **One descriptor for several things:** the same model describes data tables, manifests, annotation sets and the registry of metadata attributes, since an attribute definition is a column definition.
- **Generated interfaces:** Felis already produces database DDL and TAP_SCHEMA, which gives a route to an IVOA TAP service and to Croissant export.

Two limits shape how to use them:

- **Physical types:** CSVW is oriented to CSV and handles nested types poorly, so Arrow or Iceberg schemas should supply physical types while CSVW/Felis supply the logical and semantic layer.
- **Arrays and streams:** neither covers them. Those descriptors should follow the CF conventions and Zarr metadata.

## 11. for Felis in particular, let's abstract that to an RDF/prov/CSVW concept where possible. b

**Question**

> for Felis in particular, let's abstract that to an RDF/prov/CSVW concept where possible.  but yeah. I'd like for you to sketch out a high-level architecture of this system. I'd like for you to split out. We will call the new system "mudcat". Separate out model concepts to their own file.  I created a new repo for the ideas: brianv0/mudcat

**Answer**

No separate answer. The work for this message is in the answer to a later message.

## 12. we will use constitutional spec-driven development to build the system too, maybe speckit 

**Question**

> we will use constitutional spec-driven development to build the system too, maybe speckit or something else, I'm not sure. Probably not a concern for the ideas, but let's get them in there

**Answer**

I've written the mudcat design as three documents and pushed them to `brianv0/mudcat` on branch `claude/cool-keller-domhwr`. The repo was empty, so GitHub has probably made that branch the default; you may want to rename it to `main`.

- **`README.md`**: what mudcat is for (the five questions it answers), design principles, non-goals and links to the research. It also has a short "Development process" section, per your message: the design principles are the starting draft of a constitution, and each build step is meant to become a feature specification. I added no Spec Kit scaffolding, since the tool isn't chosen.
- **`docs/model.md`**: the conceptual model on its own.
  - **Standard vocabularies:** written in PROV-O, DCAT 3, CSVW and ODRL. A placeholder `mc:` namespace is used only where no standard term fits, and RDF is the exchange format, not the storage.
  - **Felis abstracted:** the core concept is now a "field" (`mc:Field`), covering table columns, array variables, stream channels and metadata attribute definitions. Record structures are CSVW table groups, and Felis is an import/export profile mapped onto CSVW. CF conventions, Croissant and AnnData get the same treatment.
  - **Non-tabular data:** parts of arrays, streams and files are addressed with W3C Web Annotation selectors and Media Fragments.
  - **Mapping table:** every concept is mapped to its Datacat, iRODS, Rucio and Butler equivalent.
- **`docs/architecture.md`**: the high-level architecture.
  - **Storage planes:** PostgreSQL catalog, manifests in object storage, derived indexes, and the data itself.
  - **Components:** a diagram plus a description of each component, and the five policy enforcement points.
  - **Main flows:** register data in place, publish a snapshot, train, evaluate, take down a source.
  - **Planning sections:** three deployment profiles, a scale envelope, proposed technology choices, a build order and open questions.

Two choices in the model you may want to look at first:

- **Standard terms:** I mapped the Felis and CSVW details and the PROV, DCAT, ODRL and Web Annotation terms from my own knowledge. I didn't check them against the specs in this session, so verify them before they go into a specification.
- **Restricted ODRL:** ODRL is the policy language, but the model evaluates only a small, enumerated subset. Anything outside it is stored and flagged for human review, not enforced.
