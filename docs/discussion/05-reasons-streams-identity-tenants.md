# Design discussion, part 05: The reason for the project, the change stream, sign-in for small groups, and tenants

This file is a record of the design discussion: each question, and the answer as it was given at that time. Later decisions can replace what an answer says. The documents in `docs/` hold the present design, and `docs/open-items.md` holds what is not yet in them. Paths under `/tmp` and commit IDs in the answers refer to the state at that time.

## 73. does databricks have some kind of solution like this?

**Question**

> does databricks have some kind of solution like this?

**Answer**

Yes. Unity Catalog is Databricks' answer, and it covers much of the same ground: a three-level namespace (catalog, schema, object), one permission model, lineage, and tables, files, models and functions governed side by side. It differs from mudcat mainly in how it treats files and copies. What I can confirm today comes from two quick searches of the Databricks documentation; rows marked "from memory" are not checked.

| Area | Databricks (Unity Catalog) | mudcat |
|---|---|---|
| Non-tabular data | **Volumes**: a governed directory of files in any format. "Volumes are intended for path-based data access only" ([docs](https://docs.databricks.com/volumes/)) | Each file or store is an artifact with versions, checksums and replicas |
| Files as values | A new **FILE type** (Beta): a table column holding a governed reference to a file, with metadata such as path and size. `FILE EXTERNAL` points at existing volume files; `FILE MANAGED` copies them into managed storage ([docs](https://learn.microsoft.com/azure/databricks/unstructured/file)) | Element lists and selectors: rows that point into files |
| File versions | I found nothing in the docs about versioning files in volumes | Explicit versions; maps onto S3 object versions |
| Existing buckets | **External volumes** add governance without copying. But Unity Catalog "does not manage the lifecycle and layout" of those files, and doesn't govern reads made directly against cloud storage ([docs](https://learn.microsoft.com/en-us/azure/databricks/volumes/)) | Same limit: enforcement only holds if access goes through issued credentials |
| Lineage | Runs across volumes, tables, pipelines and model fine-tuning ([announcement](https://www.databricks.com/fr/blog/announcing-general-availability-unity-catalog-volumes)); captured automatically inside Databricks compute (from memory) | PROV lineage with roles, captured from any engine through issued credentials |
| Models | Registered models with versions and aliases in the same namespace (from memory) | Models are artifacts with versions and refs |
| Tags and policy | Governed tags with tag-based access policies (from memory) | Tags with inheritance, plus usage rights on sources |
| Table versions | Delta time travel; Iceberg REST endpoint for tables (from memory) | Snapshots and refs; planned Iceberg REST catalog |
| Sharing across organisations | Delta Sharing (from memory) | Federation between instances |

**Where it stops short of mudcat:**

- **No replicas.** A volume is one location. There is no "same content, several verified copies" model. HPC file systems, tape and other clouds sit outside it.
- **No usage rights.** It has access control and tags, but nothing like sources with licences, purposes and duties, or takedowns that reach derived models.
- **Files are paths, mostly.** The FILE type is the first step toward per-file records, and it's Beta.
- **Best inside Databricks.** Automatic lineage and enforcement depend on Databricks compute. A Ray job on another cloud reading the bucket directly is invisible to it.
- **Open-source version is narrower.** The open-source Unity Catalog had fewer features than the product when we researched it.

**What's worth borrowing:**

- **The FILE type.** A typed column whose values are governed file references, with row-level access deciding who can open the file. That is a cleaner way to express mudcat's element lists and annotation sets to SQL engines.
- **Managed versus external volumes.** This is the "who leads" distinction I raised for S3 buckets, already named.

For a team already on Databricks whose data is tables plus files in one cloud, Unity Catalog is probably enough. mudcat's case rests on what it lacks: replicas across sites, usage rights, and engine-neutral lineage.

Sources:
- [What are Unity Catalog volumes? (Microsoft Learn)](https://learn.microsoft.com/en-us/azure/databricks/volumes/)
- [Volumes (Databricks docs)](https://docs.databricks.com/volumes/)
- [FILE type and unstructured data (Microsoft Learn)](https://learn.microsoft.com/azure/databricks/unstructured/file)
- [Work with unstructured data (Databricks docs)](https://docs.databricks.com/aws/en/unstructured/)
- [General availability of Unity Catalog volumes (Databricks blog)](https://www.databricks.com/fr/blog/announcing-general-availability-unity-catalog-volumes)

## 74. Managed versus external volumes is a good concept. Is Volume a useful concept to adopt in 

**Question**

> Managed versus external volumes is a good concept. Is Volume a useful concept to adopt in mudcat?

**Answer**

The distinction is worth adopting; the Volume object itself is not. A Unity Catalog volume bundles three things that mudcat already keeps separate, and bundling them would break replicas.

**What a volume bundles, and where mudcat already has it:**

| A volume is… | mudcat already has |
|---|---|
| A name in the namespace | A collection |
| One storage directory | A storage resource, at prefix level |
| An access boundary | An ACL root, with the rule that ACL roots map to their own prefixes |

Why I'd leave the object out:

- **One volume means one location.** In mudcat the same collection can have copies in a cloud bucket, an HPC file system and a GPU provider's cache. A volume has no way to say that.
- **Volumes are path-only.** They have no per-file versions, checksums or lineage. That is the gap mudcat exists to fill.
- **The word is taken.** To most engineers a volume is a disk: Docker, Kubernetes, EBS.

**What to adopt: managed versus external, as a property of a storage resource.** The same idea already exists in most tools, which suggests it's a real concept:

- **iRODS:** files in the vault versus files registered in place.
- **W&B:** uploaded artifact files versus reference artifacts.
- **Iceberg REST:** created tables versus registered tables.
- **lakeFS:** the managed store versus imports.

That property settles the S3 gaps I listed last time (who leads, mutable keys, drift, purge):

| | Managed | External |
|---|---|---|
| Who writes | Only jobs holding write credentials mudcat issued | Anyone with bucket access; mudcat watches events |
| Key layout | mudcat chooses it, write-once | Whatever is there |
| Overwrite of a key | Cannot happen | New version if the bucket keeps versions; otherwise the old replica is lost |
| A delete in the bucket | An error to repair | An event to record |
| Unknown files | Dark data, to remove | Candidates to register |
| Dropping from the catalog | Bytes are removed | Bytes stay (unregister) |
| Purge for a takedown | mudcat deletes and records a receipt | mudcat deletes only if it holds permission; otherwise it records that it couldn't and notifies the owner |
| Guarantee | Immutable, checksum verified at write | Best effort; replicas can become suspect |

Two additions beyond what Databricks has:

- **A third mode, reference.** Data mudcat can describe but never touch: an enclave, a partner's bucket, a public dataset on someone else's storage. It can't verify or delete there. The model already allows such entries; this would name the mode.
- **A binding from a collection to a storage resource.** This is the one useful thing a volume has that mudcat doesn't name. It would say "new artifacts in this collection are written here" (managed) or "files that appear under this prefix are registered into this collection" (external). A collection could have several bindings, one per location, which is how it differs from a volume. "Mount" is the obvious name.

This stays with the explicit-defaults principle: a storage resource defaults to external, the safest assumption about a bucket someone already has.

Nothing is changed in the documents. If you want it in, it's a `management` attribute on storage resources (managed, external or reference), a mount entity, and the provider version ID on replicas.

## 75. Give me a full recap of where we are again, the ideas we have, the decisions we've made

**Question**

> Give me a full recap of where we are again, the ideas we have, the decisions we've made

**Answer**

mudcat is a design, not code: six documents in `brianv0/mudcat`, with the research behind them in `brianv0/datacat`. Both are on branch `claude/cool-keller-domhwr`, and everything committed is pushed. A second layer of decisions exists only in this conversation.

#### What exists

- **Research (`brianv0/datacat`):** nine sets of notes and two reports covering Datacat's code, iRODS, Rucio, Amundsen and DataHub, the wider landscape, AI data, the Rubin Butler's latest design, and how frontier labs and AI-for-science groups manage data.
- **Design (`brianv0/mudcat`):**
  - `README.md`: purpose, principles (the draft constitution), process.
  - `docs/model.md`: the conceptual model.
  - `docs/architecture.md`: components, flows, diagrams, build order.
  - `docs/requirements.md`: about 100 "shall" requirements, each with a rationale and a verification method.
  - `docs/hosted.md`: running it as a service.
  - `docs/fields.md`: field-specific guidance.

#### Why build it

- **Datacat** had the right core (logical files with versions and copies at many sites) and an implementation that can't scale.
- **iRODS and Rucio** track bytes but describe them poorly. **DataHub and Unity Catalog** describe data but have no replicas or usage rights.
- **The gap:** one store that links copies, datasets, models and evaluations, with licence and takedown status following the lineage. The first audience is multimodal AI data, for AI-for-science groups and mid-size multimodal labs.

#### The model as committed

- **Instance:** one independently run catalog.
- **Collections** (folders) hold **artifacts**: a file, archive, array store or table. Artifacts have immutable **versions**, and versions have **replicas** on **storage resources**.
- **Structure:** each version has a structure descriptor (records, array, stream and so on). **Selectors** address parts of it. An element list exists only where the data lists its own parts.
- **Metadata:** held per namespace, with an owner and a record of who asserted each value. **Tags** are governed classifications. **Enrichers** compute metadata. **Statistics and hints** steer execution.
- **Datasets:** named sets with immutable **snapshots**, **branch refs and tag refs**, Iceberg-style commits, and a content hash.
- **Rights:** usage rights attach to **sources** (grants, constraints, duties), with the original terms kept in ODRL. Access control is separate, modelled as Zanzibar-style relationships.
- **Removal:** delete marker, invalidation, purge, erasure, plus legal holds. Time travel shows history but applies today's policy.
- **Provenance:** PROV, with a role on every use. A **resolution** records a job's exact inputs and the policy decision; this is where usage policy is enforced.

#### The architecture as committed

- **Storage:** one PostgreSQL database per instance, with rebuildable indexes fed by a change log.
- **Commits:** they change rows only, and a background worker seals manifests later.
- **Site agents:** they work next to the data, registering, verifying, enriching and deleting.
- **Read path:** mudcat is never in it. Engines such as Ray get a plan and credentials, then read storage directly.
- **Authorization:** pluggable, either built in or through SpiceDB or OpenFGA.
- **Federation:** rules for when to federate, delegate or replicate, including shared custody between institutions.
- **Hosted service:** the vendor runs the control plane and customers keep the data, and optionally their own metadata.

#### Decisions made

| Decision | Status |
|---|---|
| Every identifier is a UUIDv7 | Committed |
| Simplified technical English; NASA-style requirements; field material in its own document | Committed |
| Principle: optional structure has explicit defaults; physical choices never affect identity | Committed |
| "Artifact" replaces "data object" | Committed |
| "Instance" replaces zone; "enricher" replaces augmenter | Committed |
| Tag means classification; refs are branch refs and tag refs | Committed |
| No views, no canonical replica | Committed |
| No training-set specifications; runs record their inputs, as ML trackers do | Committed |
| "Tombstone" dropped; delete marker keeps its S3 meaning | Committed |
| Partition specs with Iceberg-style evolution; transforms stay built in | Agreed, not written |
| Manifests follow Iceberg: each carries a spec and partition summaries; no separate manifest list | Agreed, not written |
| Content hash is independent of storage layout | Agreed, not written |
| An Iceberg table is an artifact; its snapshots are versions | Agreed, not written |
| Dataset membership is by version; datasets nest by snapshot; a single artifact can stand in for a dataset | Proposed |
| Writable Iceberg REST catalog with a basic scope | Proposed |
| Managed, external and reference storage; mounts; provider version IDs on replicas | Proposed |
| Database schema sketch (about 48 tables) | Scratchpad only, at your request |

#### Still open

- **Collection or namespace:** whether to rename the folder level, and the rules for hierarchy (segment names, depth, engines that allow one level).
- **Smaller names:** alias → identifier; activity → run in the API; storage resource → storage location; the metadata document → aspect.
- **Structure:**
  - Should "version" replace "snapshot" for datasets?
  - Is a representation just a dataset version with a "representation of" link?
- **Missing concepts:** splits, job versus run, run grouping, dataset and model cards.
- **Two changes I made on judgement, for you to confirm:**
  - **Resolution record:** I kept one so the policy check has a place to live.
  - **Invalidation record:** takedowns are named this, not delete marker.

#### Risks as they stand

Butler, Iceberg, Lance and Hugging Face's storage already show the base scale. What is unproven is our own combinations: snapshot membership held as sealed manifests plus unsealed rows, policy checks per manifest, and lineage stitched across tools. There is still no anchor user.

#### Next steps I'd take

1. Write the agreed and proposed items into the documents.
2. Settle the open names in one pass.
3. Define a minimal first release.
4. Benchmark the membership mechanism.
5. Start the spec-driven process from the README principles.

## 76. I think for the why , I might try to characterize it more as this: Why? AI, especially ent

**Question**

> I think for the why , I might try to characterize it more as this:
>
> Why?
> AI, especially enterprise and startups, are arguably moving more towards files from databases and data lakes.
> Arguably, Science, also at the same time moving more towards tabular data and data lakes (especially HEP).
>
> I believe the trend overall is towards multi-modal data, and a comprehensive system to manage it is good.
>
> A comprehensive places to control access and policies, despite the format, is arguably good for everyone involved.
>
> Iceberg has arguably won the data lake tabular format wars, for better or worse. It has many good ideas that are also applicable to files.

**Answer**

That framing is stronger than mine, because it explains why now, not just what's missing. Each point holds up against the research, with two wording risks and one gap.

**How each claim holds up:**

| Your claim | Evidence we have | Caveat |
|---|---|---|
| AI is moving towards files | Video labs train from object storage on Lance; robotics data is packed video and logs; web corpora are "hundreds of billions of little files"; Databricks just added a FILE column type | Labs haven't left data lakes. They run a lake for analytics and a second, larger estate of files beside it. "Towards files" is safer than "from databases" |
| Science is moving towards tabular data and lakes | Tahoe-100M and weather archives converted to Parquet; a UK lab study chose an Iceberg REST catalog; a DOE programme uses Iceberg | See the HEP note below |
| The trend is multimodal | Both sides end up with tables, files and arrays together | This is really *convergence*, which is a stronger claim than a trend |
| One place for access and policy helps everyone | Court filings, licence deals and the EU rules on the AI side; consent and enclaves on the science side | Databricks makes the same pitch for Unity Catalog. Ours differs by also covering *where the copies are* |
| Iceberg won, and its ideas apply to files | Polaris, Snowflake, Databricks and AWS all converged on it | Not every idea transfers: invisible compaction doesn't |

**Two wording risks:**

- **"Especially HEP."** What our notes show for HEP is a move to columnar analysis formats, not to Iceberg-style lakes. The lake evidence comes from other sciences and from lab infrastructure groups. I'd say "columnar formats and data lakes" and drop "especially HEP", unless you have evidence I don't.
- **"Data lake" means something else in HEP.** There, a data lake is a federation of storage sites (the WLCG and ESCAPE "data lake"), not a table store. HEP readers may misread the sentence.

**One gap:** the draft says "despite the format" but not "despite the location". Location is what the existing catalogs lack, and it matters because GPUs are rarely where the data is.

**A tightened version,** keeping your five points:

> **Why**
>
> 1. **AI is moving towards files.** Training data is increasingly video, audio, images, documents and logs in object storage, beside the tables in a data lake.
> 2. **Science is moving towards tables.** Research data is increasingly columnar and cloud-optimised, and data lakes are appearing beside the file archives.
> 3. **Both are converging on multimodal data.** Each side now holds tables, files and arrays together, and needs one system that manages all of them.
> 4. **Access and policy belong in one place,** whatever the format and wherever the copies are. That serves the people who own the data, the people who use it, and the people who must answer for its use.
> 5. **Iceberg won the table format contest, and its ideas apply to files.** Snapshots, safe commits, partition evolution, statistics and a standard catalog interface work for files too. Files have had no equivalent.

A one-line summary that follows from point 5: *mudcat brings to files what Iceberg brought to tables, and governs both in one catalog.*

I haven't put this in the README. The Datacat lineage could follow it as a short "Where it comes from" note, so the opening leads with the argument and not the history.

## 77. Iceberg never had strong metadata support though.

**Question**

> Iceberg never had strong metadata support though.

**Answer**

Agreed, and it sharpens the argument. Iceberg's "metadata" is almost entirely physical. It describes how a table is stored, not what the data means or who may use it.

**What Iceberg actually holds** (I checked the spec earlier this session for the manifest and statistics parts; the rest is from memory):

| Kind | Iceberg has | Iceberg lacks |
|---|---|---|
| Structure | Schema with field IDs, partition specs, sort orders | — |
| Statistics | Column bounds and counts per file; sketches in Puffin files | — |
| History | Snapshots, a summary per snapshot, branch and tag refs | — |
| Description | Table and namespace properties (plain string pairs); a doc string per column | Typed attributes, units, semantic meaning, validation |
| Per-file description | Nothing a user can set | Any way to say "this file is run 6200, quality good" |
| Governance | Nothing | Tags, owners, sources, licences, usage rules |
| Lineage | Row lineage in version 3 | Which job or model used what |

Everything in the right-hand column is left to whichever catalog sits on top: Unity, Polaris, Glue or DataHub. Each does it differently, so there's no standard.

**Why that was acceptable for tables but isn't for files.** A table carries its own description: if "run" and "quality" matter, they're columns. A video, an HDF5 file or a tar shard can't do that. The description has to live in the catalog. That's exactly what Datacat, iRODS and Rucio's companion catalogs were built to hold, and where they were strong.

So the two traditions solved opposite halves:

- **Iceberg** solved state: versions, safe commits, layout, statistics. It never solved description.
- **Science catalogs** solved description: typed metadata per file, searchable. They never solved versions or safe commits.

That gives the "why" a cleaner final step. I'd replace point 5 with two:

> 5. **Iceberg won the table format contest, and its mechanics apply to files.** Snapshots, safe commits, partition evolution and statistics work for files too.
> 6. **But Iceberg describes storage, not meaning.** A table carries its description in its columns. A file cannot, so the catalog must hold it. Scientific catalogs such as Datacat did this well for decades, without Iceberg's mechanics. mudcat joins the two.

And the one-line summary becomes: *mudcat combines Iceberg's mechanics with a scientific catalog's metadata, for files and tables alike.*

One consequence for the design: mudcat's metadata can't travel through the Iceberg REST interface, which only carries string properties. Engines connecting that way will see the tables but not the tags, sources or typed attributes. Those need mudcat's own API, and that's a reason for a table owner to use mudcat instead of a plain Iceberg catalog.

Still not in the README; tell me when you're happy with the wording and I'll add it.

## 78. also, the metadata should arguably become the source of truth especially for building know

**Question**

> also, the metadata should arguably become the source of truth especially for building knowledge graphs and/or a direct source for RAG

**Answer**

Yes, with one boundary: mudcat should be the source of truth *about the data*, and knowledge graphs and RAG indexes should be projections of it. It shouldn't become the store of domain knowledge itself.

**Why the design already fits this:**

- **It's graph-shaped.** The model uses PROV, DCAT and CSVW, every entity has a stable ID, and JSON-LD export is a requirement. A knowledge graph of the data is close to a direct export.
- **It links outward.** Reference data, semantic annotations and identifier templates connect artifacts to domain entities such as a compound, an instrument or a gene. Those are the edges a domain knowledge graph needs.
- **Projections are already the rule.** Search and vector indexes are derived and rebuildable, and fed by the change log. A knowledge graph or a RAG index is one more consumer.
- **Agents are already a client.** The MCP server and description embeddings exist for exactly this.

**What makes it usable as a source of truth, and not just a source:**

- **Who said it.** Every value records whether a person asserted it, an enricher computed it, or a model inferred it. AI-written descriptions stay "proposed" until reviewed. A RAG pipeline must be able to filter on that, or model-written text feeds the next model.
- **Who owns it.** A namespace can have a single authorised writer, such as a laboratory system. mudcat is then the authoritative *copy*, and the record says where truth originates.
- **Permissions travel.** Retrieval has to be trimmed to what the caller may read, and customer-held attributes never leave the customer.
- **Takedowns travel.** An invalidation has to remove content from every graph and vector index built from it.

**What's missing in the design:**

| Gap | Addition |
|---|---|
| Retrieval isn't a governed use | A `retrieve` action beside `train` and `evaluate`, so a licence can permit training but not retrieval, or the reverse |
| No lineage for answers | A retrieval role on usage, so an agent's answer can cite the exact artifact versions it drew on |
| Documents aren't chunked in the model | Chunks as elements with selectors; embeddings as an annotation set tied to the embedding model's version |
| No graph interface | A graph gateway (SPARQL or a property-graph export) as a projection, never as the store |
| External indexes can't be forced to forget | Subscribers to the change log must honour invalidation events; the agreement terms should say so |

**The boundary.** "Gene X regulates gene Y" is content inside a dataset, not catalog metadata. If mudcat starts holding facts like that, it becomes a knowledge base and competes with the graphs it should feed. Its job is to say which datasets support that fact, where they came from, and who may use them.

This would be a seventh point in the "why":

> 7. **The catalog is the source of truth about the data.** Knowledge graphs, search indexes and retrieval for AI agents are projections of it. Each projection inherits identity, origin, permissions and takedowns from the catalog.

Nothing here is written into the documents yet. In the README it would be point 7; in the model, the `retrieve` action and role; in the architecture, the graph gateway and the subscriber rule.

## 79. Okay, re-recap all of this

**Question**

> Okay, re-recap all of this

**Answer**

Since the last recap, the argument for the project has changed (your reframing plus two additions), and one more storage idea has been proposed. No files have changed. The design documents are as they were at commit 02f7e89 in `brianv0/mudcat`.

#### The why (new, not yet in the README)

1. **AI is moving towards files.** Training data is increasingly video, audio, images, documents and logs in object storage, beside the tables in a data lake.
2. **Science is moving towards tables.** Research data is increasingly columnar and cloud-optimised, with data lakes appearing beside file archives.
3. **Both are converging on multimodal data,** and need one system that manages tables, files and arrays together.
4. **Access and policy belong in one place,** whatever the format and wherever the copies are.
5. **Iceberg won the table format contest, and its mechanics apply to files:** snapshots, safe commits, partition evolution, statistics.
6. **But Iceberg describes storage, not meaning.** A table carries its description in its columns; a file can't, so the catalog must. Scientific catalogs such as Datacat did this well, without Iceberg's mechanics. mudcat joins the two.
7. **The catalog is the source of truth about the data.** Knowledge graphs, search and retrieval for AI agents are projections of it, inheriting identity, origin, permissions and takedowns.

In one line: *mudcat combines Iceberg's mechanics with a scientific catalog's metadata, for files and tables alike.*

Two cautions on wording:

- **HEP:** our evidence for HEP is a move to columnar formats, not Iceberg-style lakes.
- **"Data lake":** in HEP the term means a federation of storage sites.

#### The design as committed

- **Core:** an **instance** holds **collections**, which hold **artifacts** (file, archive, array store, table). Artifacts have immutable **versions**, with **replicas** on **storage resources**.
- **Structure and metadata:**
  - Structure descriptors and selectors cover non-tabular data.
  - Metadata is typed and kept per namespace, with **tags**, **enrichers**, statistics and hints.
- **Datasets:** immutable **snapshots**, **branch refs and tag refs**, Iceberg-style commits, a content hash.
- **Rights and removal:**
  - Usage rights sit on **sources**; access control is Zanzibar-style and pluggable.
  - Removal has four levels: delete marker, invalidation, purge, erasure.
- **Provenance:** PROV with roles. A **resolution** records a job's exact inputs and the policy decision.
- **Architecture:**
  - One PostgreSQL database per instance.
  - Site agents work next to the data.
  - mudcat stays out of the read path.
  - Federation rules, and a hosted mode where customers keep their data.
- **Documents:** README, model, architecture, requirements (about 100), hosted service, field guidance.

#### Decisions

| Decision | Status |
|---|---|
| UUIDv7 everywhere; simplified technical English; NASA-style requirements | Committed |
| Principle: optional structure has explicit defaults; physical choices never affect identity | Committed |
| Names: artifact, instance, enricher; tag = classification; branch refs and tag refs | Committed |
| Removed: views, canonical replica, training-set specifications, the word "tombstone" | Committed |
| Partition specs with Iceberg-style evolution; built-in transforms only | Agreed, not written |
| Manifests as in Iceberg (spec ID plus partition summaries); no separate manifest list | Agreed, not written |
| Content hash independent of storage layout | Agreed, not written |
| An Iceberg table is an artifact; its snapshots are versions | Agreed, not written |
| The seven-point "why" | Drafted, not written |
| Dataset rules: membership by version, nesting by snapshot, a lone artifact can stand in | Proposed |
| Writable Iceberg REST catalog, basic scope | Proposed |
| Storage modes (managed, external, reference), mounts, provider version IDs on replicas | Proposed |
| Retrieval as a governed use; chunks as elements; a graph gateway; subscribers honour invalidations | Proposed |
| Database schema sketch | Scratchpad only |

#### Still open

- **Names:**
  - collection or namespace, with hierarchy rules;
  - alias → identifier;
  - activity → run;
  - storage resource → storage location;
  - metadata document → aspect.
- **Structure:**
  - "version" in place of "snapshot" for datasets;
  - whether a representation is just a linked dataset version.
- **Missing concepts:** splits, job versus run, run grouping, dataset and model cards.
- **To confirm:** the resolution record I kept, and "invalidation record" for takedowns.

#### Risks

- **Unproven combinations:** the base scale is shown elsewhere (Butler, Iceberg, Lance). What isn't shown is our own mix: membership as manifests plus unsealed rows, policy checks per manifest, lineage across tools.
- **Scope:** mudcat must stay the source of truth about data, not become a knowledge base.
- **No anchor user yet.**

#### Next steps

1. Put the "why" in the README.
2. Write the agreed and proposed items into the model, architecture and requirements.
3. Settle the open names in one pass.
4. Define a minimal first release and benchmark the membership mechanism.
5. Start the spec-driven process.

## 80. collection instead of namespace, identifier instead of alias, activity.

**Question**

> collection instead of namespace, identifier instead of alias, activity.

**Answer**

All three are settled, and the one that needed changes is pushed (commit 2f3430f).

- **Collection** stays. No change needed.
- **Activity** stays. No change needed.
- **Identifier** replaces alias, in every document.

The rename forced one extra distinction. The documents already used "identifier" for an entity's UUID, so the two would have collided. Each entity now has:

- **One ID:** the immutable UUIDv7, published as an IRI.
- **Any number of identifiers:** a name in a scheme, such as a path, a DOI, or a name in Rucio or Hugging Face. Each points to one ID, and most can change. Content hashes are also published as identifiers, and those never change.

I changed every place that meant the UUID to say "ID" (element ID, plan ID, the ID the producing instance minted), and updated requirements ID-1, ID-2 and ID-6 to match.

The remaining open names are storage resource versus storage location, and the name for a metadata document (aspect was my suggestion). The two structural questions are also still open: "version" versus "snapshot" for datasets, and whether a representation is just a linked dataset version.

## 81. metadata payload, or just metadata for short. We can support metadata on multiple entities

**Question**

> metadata payload, or just metadata for short. We can support metadata on multiple entities

**Answer**

Done and pushed (commit 2cb043b). The model now defines a **metadata payload**, "metadata" for short, as the set of attribute values one entity has in one namespace.

- **One unit:** mudcat writes, validates and stores each payload as a whole.
- **Many per entity:** an entity can carry several payloads, one per namespace: one a person wrote, one from a format plugin, one from a laboratory system. Two writers never change the same payload.
- **Origin per payload:** who asserted it (a person, an enricher or a model) is recorded on the payload, not on each value. This is a small change from before.

I read "metadata on multiple entities" as "every kind of entity can have it", so the list in the model now covers collections, artifacts, versions, replicas, datasets, snapshots, sources, activities, models and storage resources, plus elements through annotation sets. If you meant one payload shared by several entities, that isn't in the model. Today the closest thing is a payload on a collection, which everything under it inherits.

The architecture now says each payload is stored as one JSON document keyed by entity and namespace.

Still open: storage resource versus storage location, "version" versus "snapshot" for datasets, and whether a representation is just a linked dataset version.

## 82. we will really want basically CDC to stream out many types of events, especially for e.g. 

**Question**

> we will really want basically CDC to stream out many types of events, especially for e.g. ingestion into a search index and a good model for that too for fuzzier searching I think

**Answer**

Both are now in the design and pushed (commit 856e699): a "Change stream" section and a "Search" section in `docs/architecture.md`, plus eleven requirements (EV-1 to EV-11). The design already had an outbox and a line saying "publish CloudEvents"; this gives it an actual event model.

#### Change stream

- **Model-level, not row-level.** An event describes a change to an entity, not to a database row, so the stream stays stable when the schema changes.
- **Exactly the committed changes.** Events are written in the same transaction as the change.
- **Envelope.** CloudEvents, with a versioned type and JSON Schema, plus four fields that matter to consumers:
  - `sequence`: position in the stream;
  - `commit`: groups the events of one commit;
  - `revision`: lets a consumer ignore an event it already applied;
  - `actor`: who made the change.
- **Eight event families:** collections and artifacts; replicas and storage; metadata; datasets; access and policy; removal; provenance; placement.
- **Two streams from one outbox:**
  - **Change events** say what happened, with before and after values, kept for a retention period.
  - **State documents** carry the complete current description of an entity after each change, keyed by entity ID. An index builder needs only this one, and never has to call back for details.
- **Backfill.** A new or recovered consumer asks for every state document it can read, then continues on the live stream without a gap. This is also how mudcat rebuilds its own indexes.
- **Removal events.** An invalidation or erasure sends one, and subscribers are obliged to delete their copy.
- **Permissions.** A subscription only receives events for entities its identity can read. Customer-held values never appear.
- **No broker required.** The built-in transport is an HTTP feed with a position. Kafka, NATS and cloud event services are optional connectors.

#### Search

The state document is also the search document, so the stream and the index share one model. It has ten parts: identity, description, metadata payloads (typed from the field definitions), tags including inherited ones, structure, statistics, lineage, policy, use, and access. Structure includes column names, so you can find a table by one of its columns.

Fuzzy matching combines four kinds:

- **Lexical:** ranked full text. Names and paths are split at separators and case changes, so `rawFrames_2026` matches "raw frames".
- **Tolerant:** spelling errors, and completion as you type.
- **Semantic:** vector embeddings of the document's text, with synonyms drawn from field annotations and field vocabularies.
- **Similar entity:** "more like this one", from shared fields, tags, lineage neighbours and embeddings.

Ranking fuses the lexical and vector scores, then adds what only a catalog knows: how much an entity is used, whether it is published and reviewed, and how recent it is.

Two rules keep it safe:

- **Exact queries stay authoritative.** They go to PostgreSQL. Search is for when you don't know the exact name.
- **Results are re-checked.** The index can lag, so access control and the invalidation set are checked again before results are returned.

PostgreSQL covers all of this in small deployments (full text, trigrams, `pgvector`). Larger ones feed OpenSearch or similar from the state stream, and the same stream feeds the knowledge graphs and retrieval indexes from your earlier point.

One scope decision to confirm: individual versions don't get their own search document by default. Each artifact carries a summary of its versions. A document per version would mean around 10^9 documents.

## 83. address comments on 856e6997681b5ad3382cd410275d124b64a6a1c2

**Question**

> address comments on 856e6997681b5ad3382cd410275d124b64a6a1c2

**Answer**

All four comments on 856e699 are addressed and pushed as commit 752ae7a. I didn't reply on GitHub. None of the performance design below has been measured; the numbers in the new requirements are placeholders.

**1. How do we keep lineage operators and policy predicates fast?** Two new sections in the architecture.

- **Lineage operators:**
  - **Traversal scope:** the graph they walk holds datasets, snapshots, models and activities, so millions of relations, not billions. File and element lineage stays in run records and manifests, and is joined in only after the set is small.
  - **Fixed paths:** `used_in(<model>, role=training)` is a fixed three-step join, not a traversal.
  - **Limits:** every traversal has a depth and node limit. Passing one returns an error that says so, never a partial result.
  - **Planning:** for `derived_from(<query>)`, the planner starts from whichever side is smaller.
  - **Shortcuts:** each entity keeps its root sources and producing activity as indexed columns, so "everything from source X" is a lookup. Datasets and snapshots also get a full ancestor table; artifacts and versions don't, because it would grow too large.
  - **Jobs:** a traversal too long for a request runs as a background job.
- **Policy predicates:**
  - **Single artifacts:** the check is a join to the usage-grants table, which is small because policy sits on sources.
  - **Snapshots:** a cache holds the (action, purpose) pairs that all of a snapshot's sources permit. It is updated only for snapshots that use a changed source.
  - **Expiry:** dates are compared at query time, so an expired licence takes effect at once.
  - **Authority:** the predicate is a discovery filter. The full policy check still runs at resolution.

**2. Does "snapshot committed" include files added and removed, as in Iceberg?** Iceberg's snapshot summary carries counts and sizes, not file lists. I checked the spec: `added-data-files`, `deleted-data-files`, `added-files-size` and so on. Its lists live in manifest entries. mudcat now does the equivalent:

- **Always:** the event carries the operation, plus counts and total sizes of members added and removed.
- **Small commits:** up to a configured number of members (1,000, say), the event also carries their IDs.
- **Large commits:** the event links to a new "changes between snapshots" interface, which returns the added and removed members in pages.

**3. Are GIN indexes enough with a lot of metadata keys?** No, not alone. GIN handles any number of keys for equality, but it has no ranges or sorts, its updates are expensive, and PostgreSQL estimates JSONB conditions poorly. That hurts most at 10^9 version payloads. The new "Metadata indexes" section uses four kinds:

| Metadata | Index |
|---|---|
| Rich entities (artifacts, datasets, sources…) | GIN on each payload |
| Declared attributes that queries use often | A typed attribute index: one derived table of (attribute, typed value, entity). A new attribute needs no new database index |
| A few of the most-used attributes | Their own columns |
| Everything else on versions | No database index; the values are manifest columns, scanned by a columnar engine with pruning |

The payload stays the source of truth, and the other three are derived. One risk is stated in the document: every condition on the typed index is another join, which was Datacat's weak point. Queries with many conditions over very many versions should go to the columnar path. External search engines have field limits too, so undeclared attributes go into the state document as key–value pairs.

**4. Drop Cedar.** Done: the adapter row and the mention in the schema paragraph are gone. Access control is Zanzibar-style only.

**New requirements:**
- **PF-6:** ancestry query time.
- **PF-7:** an explicit error when a traversal passes its limit.
- **PF-8:** a policy predicate on a snapshot without reading its members.
- **PF-9:** a new attribute needs no new database index.
- **EV-12:** the changes-between-snapshots interface.

## 84. Does Iceberg Rest Catalog support events too actually?

**Question**

> Does Iceberg Rest Catalog support events too actually?

**Answer**

Not yet. The Iceberg REST catalog spec has no events endpoint today, but one has been proposed and is still open. I checked both the current spec file and the project's pull requests.

**What I found:**

- **The spec:** the current `rest-catalog-open-api.yaml` on Iceberg's main branch contains no events, notification or webhook endpoint.
- **The proposal:** pull request #12584, "REST Spec: Events endpoint", opened in March 2025, is still open. An implementation pull request (#15990, April 2026) is also open, and several earlier attempts were closed without merging.
- **Polaris has its own:** the Polaris catalog has built events outside the spec:
  - merged: a Kafka publishing extension (July 2026), a shared CloudEvents JSON mapper (October 2026), and documentation of "events and their limits for auditing" (November 2026);
  - still open: a webhook listener with signing.

I could only read the titles and states of these, not the proposal's contents, because this session's access to that repository is limited to search.

**Two things in Iceberg that are sometimes mistaken for events:**

- The spec's **metrics endpoint** lets clients report scan and commit metrics *to* the catalog. It doesn't deliver changes to subscribers.
- Iceberg's Java library has in-process **listeners** for scans and commits. From memory, these are a client-side feature, not part of the REST interface.

**What this means for mudcat:**

- **No standard to conform to yet.** Our change stream, built on CloudEvents with typed events, is the right call for now. Polaris moving to CloudEvents suggests the field is heading the same way.
- **I was wrong twice on this.** My earlier mapping table claimed an Iceberg events endpoint existed; I corrected that in the re-review, and this check confirms the correction. The basic-scope list I proposed for a writable Iceberg catalog also listed "the events endpoint" as out of scope, as if it existed. That list is only in this conversation, not in the documents.
- **Worth tracking.** If the proposal merges, mudcat's Iceberg gateway could expose table-level events in that shape, as a projection of our stream. From the title, it looks like a query endpoint ("what changed since X") rather than a push stream, which would map onto our HTTP feed with a position. That is a guess until someone reads the proposal.

## 85. Many science projects don't have a good source of truth for authentication/authorization/I

**Question**

> Many science projects don't have a good source of truth for authentication/authorization/IAM, especially smaller ones. Can you make a suggestion on how to onboard those small use cases?

**Answer**

My suggestion: let mudcat be the source of truth for group membership itself, and borrow only *login* from providers researchers already have. Small projects usually lack a list of who is in the collaboration, not a way to log in. The details below are from my knowledge of these services, not checked this session, so confirm terms and pricing before committing to one.

#### The approach

1. **Borrow login, don't run it.** Accept sign-in from identities researchers already hold:
   - ORCID: free, and it follows a person between institutions.
   - Their university, through a research federation proxy such as CILogon or eduGAIN.
   - GitHub or Google.

   mudcat needs only a small sign-in broker for this, no user database or passwords.
2. **Make a person separate from a login.** A mudcat principal can link several identities. People change institutions; their ORCID and their project membership shouldn't change with them.
3. **Own the groups.** The built-in authorization adapter already stores group membership. Add what a principal investigator actually needs:
   - Invitations by email or ORCID.
   - A few roles: owner, writer, reader.
   - Membership expiry dates, with a periodic "is this person still on the project?" review.
4. **Trust by invitation, not by attribute.** Anyone can create an ORCID or GitHub account, so access should come from being invited by an owner, never from an email domain or a claimed affiliation.
5. **Give batch jobs a simple path.** Small projects won't manage certificates. Offer three things:
   - Device-code login for HPC login nodes.
   - Personal tokens with expiry.
   - Scoped service accounts for pipelines and site agents.
6. **Start from a project template.** Creating a project makes a collection, three groups (owners, writers, readers), an ACL on the collection, and a public-or-private switch. Nobody starts by writing ACLs.
7. **Let mudcat stand in for cloud IAM.** With managed storage, mudcat holds the one bucket role and issues short-lived credentials. Members never need cloud accounts.

#### A ladder, so nobody has to start at the top

| Level | Identity | Groups | Suits |
|---|---|---|---|
| 0 | None, or one admin token | None | A laptop or a single user |
| 1 | ORCID, GitHub or Google through the built-in broker | Managed in mudcat, by invitation | A lab or small collaboration |
| 2 | Institutional login through a research federation | mudcat, or synced from a collaboration registry such as COmanage or Globus Groups | Multi-institution projects |
| 3 | Enterprise identity provider with SCIM | External, optionally with SpiceDB or OpenFGA | Facilities and companies |

The ACL model is identical at every level. Moving up changes where logins and memberships come from, not the grants. This follows the explicit-defaults principle.

#### Limits to be honest about

- **POSIX file systems.** On a shared HPC file system mudcat can't enforce anything; Unix permissions do. The realistic option is for the site agent to record the mapping to Unix groups and report drift, not to pretend to control access.
- **Identity linking is an attack surface.** Linking a second login to an account needs proof of both, or it becomes a takeover path.
- **Low-assurance logins.** ORCID and GitHub prove little about who someone is. Controlled-access data (patient or export-controlled) needs level 2 or above.

#### What the design would need

- Principals with linked identities.
- Built-in group management, with invitations and expiry.
- Device-code login and personal tokens.
- Project templates.
- A bundled sign-in broker.

None of this is in the documents yet.

## 86. yeah, Probably Github/Gitlab and/or Slack, or Google Groups would be best if we could make

**Question**

> yeah, Probably Github/Gitlab and/or Slack, or Google Groups would be best if we could make that work. I worked heavily with CILogon and that won't work for any kind of group membership, but it could work for identity at least.

**Answer**

That works, and it fits how small projects actually operate: their real membership list is already a GitHub organisation or a Slack workspace. I'd build it as **membership connectors**: a mudcat group can be bound to an external group, and mudcat keeps the membership in sync. Login and membership stay separate, so CILogon or ORCID can supply identity while GitHub supplies the groups. The provider details below are from my own knowledge, not checked this session; the plan and tier limits especially need confirming.

#### How well each source would work

| Source | What binds to a mudcat group | Sync | Main limits |
|---|---|---|---|
| **GitHub** | A team in an organisation; or "people with write access to repository X" | App installed on the organisation: API plus webhooks on membership changes | Needs an organisation owner to install the app. Outside collaborators are per repository, not in teams |
| **GitLab** | A group or subgroup, with access level mapped to role (for example Developer and above can write) | API; group claims at login; member webhooks may need a paid tier | Works for self-hosted instances too, which many labs run |
| **Slack** | A user group, or a *private* channel | Events for joins and leaves | User groups are a paid feature. Public channels must not count: anyone can join one and grant themselves access. Guests and shared-channel members need explicit handling |
| **Google Groups** | A group in a Workspace or Cloud Identity domain | Directory or Cloud Identity API with a service account | Needs a domain admin, which small projects often can't get. Consumer groups (`@googlegroups.com`) have no membership API, so they can't work |

So I'd rank them GitHub and GitLab first, Slack second, and Google Groups third and only for Workspace domains.

#### Making it safe

- **Match on the provider's user ID, never on email.** A person links their GitHub or Slack account to their mudcat principal by signing in to it once. That link is the proof.
- **Bind only deliberate groups.** Teams, user groups and private channels were created on purpose. Workspace-wide or organisation-wide membership is too casual to grant data access.
- **Bound the staleness.** Each synced membership records when it was last confirmed. Webhooks make removal take seconds; a scheduled full sync catches missed events. If a source can't be reached for longer than a set limit, its groups stop granting access.
- **Re-check at the sensitive moment.** When mudcat issues storage credentials, it can ask the provider directly whether the person is still a member. GitHub and GitLab make that a single cheap call.
- **Show effective access.** Owners need a page listing who can read what and through which external group, because team membership sprawls.

#### Two extras the forges give you

- **Pipeline identity without secrets.** GitHub Actions and GitLab CI issue signed identity tokens to jobs. mudcat can trust "workflows in repository X on branch main may write to collection Y", so small projects never manage service keys.
- **Access requests in Slack.** A request to join a group can go to a channel with approve and deny buttons, which is where a small team would handle it anyway.

#### Limits

- **Assurance:** these sources are fine for ordinary research data but not for controlled-access data.
- **Dependence:** mudcat's access now depends on the provider's API, rate limits and uptime.
- **Privacy:** member lists are copied into mudcat, which some institutions will want to know about.

In the design this would be a `membership source` on groups in the built-in authorization adapter, plus linked identities on principals. Neither is in the documents yet.

## 87. yeah HPC with those credentials gets even messier

**Question**

> yeah HPC with those credentials gets even messier

**Answer**

Agreed. HPC breaks most of what we just designed, because an HPC centre already has its own identity system and its own enforcement, and neither knows anything about GitHub or Slack. What follows is from my own knowledge of how centres typically run; every centre differs, and I haven't checked any specifics.

#### Why it's messier

- **A different identity.** On a cluster you are a Unix account issued by the centre, usually with its own multi-factor login. It has no link to your GitHub or ORCID identity.
- **A different enforcer.** Files on Lustre or GPFS are protected by Unix permissions. mudcat can't vend credentials for a path, so its ACLs are only advisory there.
- **Headless, delayed jobs.** A job may start days after submission, on nodes with no browser and often no outbound network. Long-lived tokens end up in home directories on shared file systems, which many centres forbid.
- **Shared production accounts.** Pipelines often run under a collaboration account, so "who did this" is lost.

#### What I'd do

1. **Let the site be the authority for its own bytes.** Mark the storage resource as site-enforced. mudcat records which Unix group corresponds to which collection, and the site agent reports drift between Unix permissions and mudcat ACLs. It doesn't try to enforce.
2. **Use the allocation's Unix group as a membership source.** For many small projects this is the most accurate member list they have, because a PI must add people there before they can compute. The site agent can read it and attest it, alongside the GitHub and Slack connectors.
3. **Link identities on the cluster, once.** A user runs a link command on a login node. The site agent confirms their Unix account locally, for example from the peer credentials of a local socket, or a Slurm MUNGE credential. The user confirms their mudcat identity with a device code. After that, mudcat knows the two are the same person.
4. **Let the cluster vouch for the job.** Jobs authenticate to the local site agent as their Unix user, with no stored token. The agent exchanges that for a short-lived mudcat token for the linked principal, limited to that site. This is the HPC equivalent of the pipeline identity tokens from GitHub Actions.
5. **Resolve before submitting, report afterwards.** At submission, mudcat resolves the inputs to a manifest of paths the job can read offline. The job writes its run record to a spool directory, and the agent uploads it when the job ends. Compute nodes never need to reach mudcat.
6. **Use Globus where a site already has it.** Globus already maps federated identities to local accounts at many centres. It can serve remote access to HPC storage in place of anything mudcat would build.

#### Limits that remain

- **Policy becomes audit, not prevention.** Someone with Unix read access can read a file whatever mudcat's usage policy says. mudcat can record and flag it, not stop it.
- **The site agent is trusted.** A compromised agent could impersonate any linked user at that site, which is why its tokens must be valid only for that site.
- **Deployment needs the centre's consent.** A persistent agent has to be approved by the centre, and each centre has different rules.
- **Purges need the right Unix account.** The agent can only delete what its account may delete.

None of this is in the documents. It would touch the site agent, the storage-resource modes (a "site-enforced" variant of external), and the membership sources.

## 88. a PAT might work, which probably brings github to the forefront, maybe google workgroups

**Question**

> a PAT might work, which probably brings github to the forefront, maybe google workgroups

**Answer**

A GitHub token would work, and there's a direct precedent: HashiCorp Vault's GitHub login takes a personal access token with the `read:org` scope and maps organisation teams to policies ([Vault docs](https://developer.hashicorp.com/vault/docs/auth/github)). That does put GitHub first. I'd make one change to the idea: prefer a device login that issues the token *to mudcat*, and keep a raw PAT as the fallback for unattended use. Apart from the Vault behaviour, the details here are from my own knowledge of GitHub and Google, not checked this session.

#### Why a GitHub token suits HPC

- **Headless.** No browser is needed on the cluster, and researchers already keep GitHub credentials there for git.
- **One answer for identity and groups.** The token proves who you are, and the same API call path tells mudcat which teams you're in.
- **Revocation lives at GitHub.** Tokens can expire, and for organisations with single sign-on they must be explicitly authorised.
- **Nothing for mudcat to mint or store.**

#### The problem with a raw PAT

Any valid token for a member of the organisation would log in. Vault states this plainly: "Any valid GitHub access token with the read:org scope for any user belonging to the Vault-configured organization can be used." So a token created for some other purpose, and leaked from a CI log or a laptop, would also open mudcat.

#### What I'd build

1. **Device login as the default.** The mudcat CLI runs GitHub's device flow as mudcat's own GitHub app, asking only for organisation membership. The user types a code on any device. The resulting token is issued to mudcat's app, so mudcat can reject tokens issued to anything else.
2. **PAT as the fallback.** For unattended pipelines and accounts that can't do a device login. mudcat checks the token's scopes and refuses over-scoped ones. That works for classic tokens, which report their scopes; I'm not sure fine-grained tokens expose theirs, so that needs testing.
3. **Exchange, don't reuse.** Either kind of token is swapped once for a short-lived mudcat token. The GitHub token isn't sent on every request, and GitHub's rate limits don't matter.
4. **Exchange on the login node.** Compute nodes often can't reach GitHub or mudcat. The exchange and the input resolution happen at submission, and the job carries a token scoped to that job.
5. **Pipeline tokens for CI.** GitHub Actions jobs use their built-in identity tokens, with no PAT at all.

This replaces the "cluster vouches for the job" idea from my last message for most small projects. That design needed the HPC centre to approve a trusted agent; this needs nothing from the centre.

#### Google Workspace groups as the second option

- **Login** works headlessly through Google's device flow.
- **Membership** needs a mudcat service account that a Workspace admin has allowed to read groups. That admin step is the barrier for small projects.
- **Fit:** it suits institutions that already run on Google Workspace. It doesn't work for consumer groups.

#### What stays true on HPC

- A token in a home directory on a shared file system is still exposed to that site's administrators. Short expiry and narrow scope limit the damage; nothing removes it.
- On POSIX storage, Unix permissions still decide who can read. mudcat's policy there is a record, not a barrier.

This would be the first-class onboarding path: GitHub organisation, teams bound to mudcat groups, device login, PAT fallback, Actions identity for pipelines. It isn't in the documents yet.

Sources:
- [GitHub auth method (HashiCorp Vault docs)](https://developer.hashicorp.com/vault/docs/auth/github)

## 89. > * Policy becomes audit, not prevention. Someone with Unix read access can read a file wh

**Question**

> > * Policy becomes audit, not prevention. Someone with Unix read access can read a file whatever mudcat's usage policy says. mudcat can record and flag it, not stop it.
>
> Yes that's true. We can't do anything about that. Institutions could have some kind of syncing mechanism but it's a mess and depends on whether it's lustre or weka or GPFS or....

**Answer**

Agreed, and I'd write that limit into the design as a stated property instead of trying to engineer around it.

**What mudcat should say about each storage resource:**

| Enforcement | Where | What mudcat guarantees |
|---|---|---|
| Catalog-enforced | Object storage reachable only through credentials mudcat issues | Access control and usage policy are both prevented at read time |
| Site-enforced | POSIX file systems at an HPC centre | Nothing at read time. mudcat records the intended access, reports drift, and still enforces policy on what *it* does: resolution, credential issue elsewhere, export, publication |
| None | Reference data mudcat can't touch | Description and lineage only |

That way a data owner can see at a glance which copies are actually protected. A policy can also say "controlled data may only have replicas on catalog-enforced storage", which is a rule mudcat *can* enforce, through replication rules.

**On syncing permissions:** I'd keep it out of the core for the reason you give. Group limits, ACL flavours and tooling differ between Lustre, GPFS, Weka and the rest, and each centre has its own rules about who may change them. The most mudcat should offer is a hook in the site agent, where an institution can plug in its own script to apply or check permissions. mudcat's own job stops at reporting the difference between what it intends and what the file system says.

**One partial way out,** where it exists: some centres put an S3-compatible gateway in front of the same file system. Access through that gateway can use vended credentials, so those reads become catalog-enforced even though the bytes sit on a POSIX system. I haven't checked how common or how complete those gateways are, so treat it as something to ask each site, not a plan.

None of the last few topics is in the documents yet: the onboarding ladder, membership connectors, GitHub device login, and these enforcement levels.

## 90. Let's simulate database schemas for mudcat again. similarly, compare in particular to iceb

**Question**

> Let's simulate database schemas for mudcat again. similarly, compare in particular to iceberg rest catalog schemas (I guess for JDBC impl)

**Answer**

This answer described a sketch of the database schema. The owner of the project asked that the schema stay outside the repository, so the answer is not in this record.

## 91. as an aside, datacat needed a VFS implementation to make hierarchical things fast. I think

**Question**

> as an aside, datacat needed a VFS implementation to make hierarchical things fast. I think hierarchical things are good, but it's worth consideration especially if you need a VFS cache and invalidation and support multiple api servers to scale out. not sure if this is a concern for irc
> running on cockroach db or something could be an option and or foundationdb with sql front end ?

**Answer**

Short version: mudcat shouldn't need a VFS, the Iceberg catalogs mostly sidestep the problem, and I'd stay on PostgreSQL but write the schema so a distributed SQL database remains possible. What I say below about Polaris's schema and the Iceberg endpoints I checked today; what I say about CockroachDB, YugabyteDB and FoundationDB is from memory.

#### Why Datacat needed the VFS, and why mudcat shouldn't

Datacat stored only name and parent, so resolving a path took one query per segment, ACLs were worked out by walking up the tree in Java, and globs walked the tree. The in-memory tree hid all that, at the cost of a per-server cache that went stale.

mudcat can make each of those a single indexed lookup:

- **Path to collection:** store the full path for collections only. There are at most millions of them.
- **Artifact:** look it up by (collection, name). No per-artifact path string is needed.
- **ACL:** each collection already carries its ACL root, so there is no walk.
- **Subtree queries:** a prefix scan on the chain of ancestor IDs.

That corrects my schema sketch, which stored a path identifier per artifact. At 10^9 artifacts, renaming a top-level collection would rewrite 10^9 rows. With paths on collections only, a rename rewrites collection rows and nothing else.

A cache is then an optimisation, never a requirement:

- **Stateless servers.** Any cached collection entry carries its revision and is dropped when the change stream passes it.
- **Read your own writes.** A commit returns its stream position, and a server reading from a replica waits until the replica has reached it. This is the same idea as Zanzibar's consistency tokens.
- **Where fan-out really bites:** derived indexes, not the database. Changing a tag on a top-level collection shouldn't re-emit a search document for everything beneath it. Search documents should carry their ancestor IDs and resolve inherited tags at query time.

#### Is it a concern for Iceberg REST catalogs?

Only mildly:

- **Namespaces are few and shallow,** and the REST spec has no namespace rename. It has table and view rename only, so the subtree-rewrite case never arises.
- **Polaris** stores a parent ID per entity and resolves level by level. It carries `entity_version` and `grant_records_version` counters, which look designed for validating a server-side cache.
- **Lakekeeper** stores the whole namespace path as an array with a unique index, which is the materialised approach.

#### CockroachDB, or FoundationDB with a SQL layer?

They solve horizontal write scale and multi-region availability, not hierarchy. My view:

| Option | For | Against |
|---|---|---|
| PostgreSQL (reference) | Everything the sketch uses; small teams can run it | One writer per instance |
| CockroachDB | Scales writes; built-in changefeeds would suit the change stream; Polaris ships a CockroachDB schema | Lacks some features the sketch uses (exclusion constraints, possibly `ltree`); its licence is no longer open source, which matters for an open project |
| YugabyteDB or Citus | Closer to PostgreSQL; Citus fits the cheap tables, which are already hash-partitioned by artifact | More to operate |
| FoundationDB | Proven for catalog metadata at Snowflake; its directory layer makes renames cheap | No maintained SQL layer that I know of. You build indexes and the query language yourself, and transactions are capped at about five seconds |

What I'd do:

1. **Don't target several databases now.** Datacat's Oracle, MySQL and HSQLDB support left it with duplicated code and one dialect that actually worked.
2. **Keep the option open cheaply.** Avoid PostgreSQL-only features where a plain alternative exists: ID paths as text with a prefix index instead of `ltree`, and "one open row" unique indexes instead of exclusion constraints.
3. **Scale in this order:**
   - a bigger server and read replicas;
   - one instance per tenant and region, which the design already has;
   - sharding only the cheap tables (versions, replicas, payloads, relations);
   - distributed SQL only if a single instance truly needs multi-region writes.
4. **Treat FoundationDB as a hosted-service option at very large scale,** not a default.

None of this is in the documents or the schema file yet.

## 92. do i need system transaction ids?

**Question**

> do i need system transaction ids?

**Answer**

Yes, but as mudcat's own commit record, not as the database's internal transaction IDs exposed in the model. Your question also exposes a bug in my schema sketch. This is from my knowledge of how PostgreSQL behaves; I haven't tested it this session.

**What mudcat needs:**

| Thing | Purpose |
|---|---|
| **Commit ID** | One per catalog transaction. Groups the events of a commit, anchors audit, and stamps history rows |
| **Position** | A gap-free number in true commit order. Used as the change-stream cursor and as the read-your-writes token |
| **Revision per entity** | Optimistic concurrency and idempotent consumers. Already in the sketch; this part needs no global ID |

**The bug.** The outbox in my sketch takes its `sequence` from an identity column. PostgreSQL hands those numbers out when a row is inserted, not when the transaction commits. So transaction A can take 10, B take 11, and B commit first. A consumer reads 11, moves its cursor past 10, and never sees A's event. The stream would silently lose changes under concurrent writers.

**The fix needs the database's transaction ID once, internally:**

1. Each outbox row records its transaction ID (`pg_current_xact_id()`).
2. The relay reads only rows whose transaction is older than every transaction still in progress, so nothing earlier can appear later.
3. The relay then stamps the final, dense position as it publishes. It is the only writer of positions, so they are in commit order by construction.

The alternative is to read the write-ahead log through logical decoding, which is in commit order already. Either way, consumers see only mudcat's position.

**Why not expose the database's IDs:**

- **They aren't durable identity.** They don't survive a logical dump and restore, or a move to another database.
- **They aren't portable.** CockroachDB has timestamps and changefeeds instead. If the storage layer provides "position" as an abstract value, that door stays open.

**History has the same issue.** My history tables use `now()` for `valid_from`. In PostgreSQL that is the transaction's *start* time, so two overlapping commits can appear in the wrong order. History rows should carry the commit ID, and a small commit table (ID, position, time, actor) should map times to positions. "As of time T" then means "as of the last commit at or before T", which is well defined. Iceberg does the same with its snapshot log and sequence numbers.

So the sketch needs three changes:

- a commit table;
- a transaction ID on outbox rows, with the relay assigning positions;
- commit IDs in place of timestamps on history rows.

None of this is applied yet, to the scratchpad schema or the documents.

## 93. can you read a bit about Salesforce DBs globally unique keys (cidr/vldb) and see if they a

**Question**

> can you read a bit about Salesforce DBs globally unique keys (cidr/vldb) and see if they also may be a good fir

**Answer**

I read both Salesforce papers, plus Pat Helland's earlier one. The idea of tenant-leading globally unique keys fits mudcat, but only in one place, and the mechanism that makes it powerful at Salesforce doesn't transfer to PostgreSQL. Reading them needed a new network environment ("mudcat research", allowing vldb.org), which this session now uses.

#### What Salesforce does

- **The "galactic keyspace".** All data sits in one ordered key space, called galactic "because including unique Tenant IDs makes keys unique across all SalesforceDB instances". The application guarantees tenant IDs are unique across the fleet.
- **Key layout.** A row's key is the tenant ID, then the table ID, then the rest of the primary key. Every multi-tenant table and every index on one must lead with the tenant ID.
- **What that buys.** A tenant's data is one contiguous key range. Storage is immutable, so dropping, moving or cloning a tenant is a metadata operation: the database hands out references to existing files, restricted to a key range. A sandbox clone gets a new tenant ID, and reads translate the old ID to the new one on the fly. Each tenant can also have its own encryption key.
- **Tenant isolation.** The application adds the tenant predicate; the database checks that it is present whenever a tenant context is set.
- **Commit order.** One coordinator assigns 64-bit commit numbers that approximate nanosecond time. It keeps a "commit epoch" and a "visible epoch", advanced about every millisecond, and readers only see up to the visible epoch.
- **Their main lesson.** "Immutability changes everything": caches are trivially correct, and nodes share storage without coordination.

Helland's 2024 paper adds the application's side: don't update the same key concurrently, and organise changes by commit time first, then by key.

#### Does it fit mudcat?

| Idea | Fit | Why |
|---|---|---|
| Tenant ID to make keys globally unique | Not needed | UUIDv7 IDs are already unique everywhere. Salesforce needs the prefix because the rest of its keys aren't |
| Tenant ID as the leading key column | Only for a shared hosted tier | With one database per instance it's pure overhead. For thousands of small tenants in shared tables it's the right design, and better than my "one schema each" suggestion |
| Metadata-only tenant move and clone | Doesn't transfer | It depends on immutable files under the database. PostgreSQL would copy rows |
| Tenant predicate enforced by the database | Fits | PostgreSQL row-level security does the same job |
| Commit epoch and visible epoch | Fits, and confirms the fix | It is the same "only read below the horizon of in-flight commits" rule I proposed for the change stream. A time-like position would also make "as of time T" natural |
| References to immutable files, restricted by key range | Worth borrowing | A dataset clone, a selection or a federated copy could reference existing manifests with a restriction instead of rewriting them |

#### What I'd do

1. **Decide the shared tier now, because it's hard to retrofit.** If many small tenants in one database is a real goal, put an instance column at the front of every key from the start. A self-hosted install would have one value, and a compact number mapped to the instance's ID keeps the cost to a few bytes per row. If it isn't a goal, skip it.
2. **Borrow restricted manifest references** for cheap clones and selections.
3. **Keep the commit-position design** from my last answer, since it matches theirs.

One limit on this reading: the papers don't describe how tenant IDs themselves are generated, or the classic 15- and 18-character Salesforce record IDs.

Sources:
- [SalesforceDB: A Cloud-native Multi-tenant OLTP Database (VLDB 2026)](https://www.vldb.org/pvldb/vol19/p4426-arora.pdf)
- [A Multi-tenant Relational OLTP Database at Salesforce (CIDR 2026)](https://vldb.org/cidrdb/papers/2026/p28-arora.pdf)
- [Scalable OLTP in the Cloud: What's the BIG DEAL? (CIDR 2024)](https://www.vldb.org/cidrdb/papers/2024/p63-helland.pdf)

## 94. especially for some value of "tenant" in a cloud offering

**Question**

> especially for some value of "tenant" in a cloud offering

**Answer**

Yes, and the useful question is which unit "tenant" should be. I think it should be the **instance**, with the customer organisation one level above it and projects one level below. The instance is the only unit in mudcat that satisfies what makes tenant-leading keys pay off at Salesforce.

**What a key-leading tenant has to be:**

| Property | Organisation | Instance | Project (top-level collection) |
|---|---|---|---|
| Every row belongs to exactly one | No: an organisation can have instances in several regions | Yes | Mostly |
| Almost every query stays inside one | No | Yes | No: datasets, lineage and groups routinely cross projects |
| References across it are rare and by ID | — | Yes: that is federation | No |
| It is what you move, clone, drop, bill and encrypt | Bill only | Yes | Quota only |

So the three levels in a cloud offering would be:

- **Organisation:** billing, the identity provider, users, groups and their membership sources (the GitHub teams and so on). It is shared by all of the organisation's instances.
- **Instance:** the data authority, pinned to a region, and the leading key column.
- **Collection:** projects inside an instance, separated by ACLs and quotas only.

The two Iceberg catalogs I read today do the same thing with two levels. Polaris leads its keys with `realm_id` and also carries a `catalog_id`; Lakekeeper has projects above warehouses, with `warehouse_id` in its keys.

**That gives two kinds of tenancy to offer:**

- **Soft tenant: a project in a shared instance.** A "community" instance holds many small labs as top-level collections. Sharing is a grant, lineage crosses projects freely, and discovery is shared. This is the Hugging Face model, and it suits the small science projects we discussed for onboarding.
- **Hard tenant: your own instance.** You get your own policy authority, region, keys, identity provider and customer-held metadata. Sharing with others means federation. Thousands of small hard tenants can still sit in shared tables, because the instance leads every key.

**What it changes in the design:**

- **Replace "one schema each" for small tenants** with shared tables, a leading instance column, and row-level security set per connection.
- **Keep UUIDv7 as the global ID.** The leading column can be a compact number local to each database, mapped to the instance's ID. Moving a tenant copies its rows and remaps that number.
- **Scope the change stream per instance.** Positions are per instance, not per database.
- **Split authorization data.** Group membership sits at organisation level and is copied to each region; grants on collections sit in the instance.
- **Promote by size or need.** A tenant that outgrows the shared tier, or needs its own encryption keys or per-tenant restore, moves to a dedicated database. PostgreSQL can't do those per tenant inside shared tables.

One caution: PostgreSQL has no equivalent of Salesforce's metadata-only move, so a tenant move is a real copy and needs a quiet period, as theirs does.

None of this is in `hosted.md` yet. It still says one database per instance, with a schema each for very small ones.

## 95. per-tenant endpoints probably worth well for the connection establishment without making a

**Question**

> per-tenant endpoints probably worth well for the connection establishment without making all applications tenant aware

**Answer**

Agreed. A per-instance endpoint puts the tenant in the connection, so the SDK, Ray jobs, site agents and Iceberg clients just talk to "their" catalog URL and never carry a tenant parameter.

**How it would work:**

- **One hostname per instance,** for example `<instance>.<region>.mudcat.example`. The gateway reads the tenant from the hostname, sets the tenant context for row-level security, and routes to the right database.
- **Every interface under that host.** The REST API, the Iceberg REST catalog, the change stream and the MCP server all live there. Iceberg's REST spec already expects this: its paths carry a `{prefix}` that the server hands out from its config endpoint, which is how existing catalogs route to a warehouse.
- **Stable when a tenant moves.** Promoting a tenant from shared tables to a dedicated database changes the gateway's routing, not the client's URL.
- **Self-hosted is the same shape.** One endpoint, one instance.

**What else it gives you:**

- **Login knows where to go.** The hostname tells the sign-in page which organisation's identity provider to use.
- **Tokens bound to a tenant.** A token's audience is the instance endpoint, so a token for one instance can't be replayed against another.
- **Browser isolation.** Each tenant's web UI is a separate origin.
- **Per-tenant controls at the edge.** These are rate limits, quotas, private network endpoints for enterprises, and custom domains.
- **Residency by construction.** The hostname resolves to the region where the instance lives.

**Things to get right:**

- **Hostname isn't proof.** The gateway must check that the token's audience matches the tenant from the hostname, and row-level security stays on underneath.
- **Tenant context per transaction.** With pooled database connections, the context must be set for each transaction, never for the session.
- **Hostnames leak.** Tenant names show up in DNS and certificate logs, so use opaque names where the customer's identity is sensitive.
- **Entity IRIs must not depend on the hostname.** Custom domains and moves would break them. Use a stable base for identifiers, with the endpoint only as where they resolve.
- **Cross-instance work needs another door.** Organisation-wide search, billing and admin need an organisation-level endpoint, or the client has to fan out.
- **Soft tenants share an endpoint.** In a community instance, projects are paths under one host, so ACLs are the only separation there.

This belongs in `hosted.md`, alongside the tenant model from my last answer; neither is written in yet.

## 96. what if I want to serve many tenants from one instance though? these would just be fancy g

**Question**

> what if I want to serve many tenants from one instance though? these would just be fancy gatewts whixh set the tenant and the seve wouks need to know thay

**Answer**

Yes. One running service can serve many tenants, the per-tenant endpoints are just routing in front of it, and the server has to know which tenant each request is for. The aim is for it to know in exactly one place.

**How the server learns the tenant:**

- **From the request itself, not from a header the gateway adds.** The server takes the tenant from the hostname and checks it against the audience of the validated token. A gateway-injected header would have to be trusted, and it becomes a spoofing risk if anything can reach the server directly.
- **One layer sets it.** The outermost request handler resolves the tenant and opens a tenant-scoped database transaction. Row-level security does the filtering from there, so the code underneath never mentions tenants. A request in tenant A with an ID from tenant B simply finds nothing.

**Where it still has to be tenant-aware:**

| Place | Why |
|---|---|
| Background workers | No request to take the tenant from; each job must carry it |
| Caches | A cache key without the tenant is the classic leak, so caches should be created per tenant |
| Change stream | Positions and subscriptions are per tenant |
| Storage and keys | Manifest prefixes, storage credentials and encryption keys are per tenant |
| Search indexes | A tenant filter on every query, or separate indexes |
| Limits and logs | Rate limits, quotas, metrics and audit are per tenant |
| Cross-tenant features | Sharing or federation between two tenants on the same service must be an explicit, audited path |

There should also be an isolation test suite that runs every endpoint as two tenants and tries each one's IDs against the other.

**Your wording exposes a naming problem.** The documents now use "instance" for the logical catalog, the thing with its own IDs and authority. You just used it for the running service, which is the more natural reading. I'd split them:

- **Instance:** a running deployment (service plus database). This is what an operator runs.
- **Catalog:** the logical unit inside it, with its own IDs, policy authority and key prefix. This is what a tenant gets.
- **Collection:** projects inside a catalog.

A self-hosted install is then one instance with one catalog, which fits the explicit-defaults principle. Federation happens between catalogs, whether or not they share an instance. Polaris (realm, catalog) and Lakekeeper (project, warehouse) have the same two levels.

Do you want that split? It would mean renaming "instance" to "catalog" in most of the model, and keeping "instance" for the deployment.

## 97. "One layer sets it" that's what i want i guess. Self-hosted would not need that extra laye

**Question**

> "One layer sets it" that's what i want i guess. Self-hosted would not need that extra layer if it's just one tenant

**Answer**

Agreed, with one refinement: keep the layer in self-hosted, but make it trivial. Removing it would mean two code paths and two schemas, and those are where the bugs would hide.

**How I'd do it:**

- **A pluggable tenant resolver.** Hosted uses the hostname-and-token resolver. Self-hosted uses a single-tenant resolver that always returns the default tenant, with no hostname logic and no configuration. This follows the explicit-defaults principle: one tenant is a real value, not an absence.
- **The same schema everywhere.** The tenant column exists in both, holding one value in a self-hosted install. As a compact number it costs about four bytes per row and per index entry, roughly 4 GB per billion-row table and each of its indexes.
- **Row-level security can stay off for one tenant.** With a single tenant there is nothing to filter, so self-hosted can skip the policies and the per-transaction setting. Turning them on becomes a deployment switch, not a migration.

**What that buys a self-hosted site later:**

- **A second tenant without migrating.** For example a sandbox copy for testing, or a collaborating lab that needs its own policy authority.
- **A clean move into a hosted service.** The rows already have the right shape, so only the tenant number needs remapping.

It also means the isolation tests run against the same code that self-hosted users run, so a self-hosted install that later adds a tenant isn't the first to exercise that path.

The naming question from my last message is still open: whether "instance" should mean the running deployment, with "catalog" for the per-tenant unit. I've used "tenant" here to avoid deciding it. None of the tenancy design from the last few messages is in the documents yet.

## 98. Decide the shared tier now, because it's hard to retrofit. If many small tenants in one da

**Question**

> Decide the shared tier now, because it's hard to retrofit. If many small tenants in one database is a real goal, put an instance column at the front of every key from the start. A self-hosted install would have one value, and a compact number mapped to the instance's ID keeps the cost to a few bytes per row. If it isn't a goal, skip it.
> 	2.	Borrow restricted manifest references for cheap clones and selections.
> 	3.	Keep the commit-position design from my last answer, since it matches theirs.
>
> yes, yes,yes. tenant is cryptographically generated and unique.
>
> Immutability and cheap copies be a design goal,
> but it will probably mean less foreign key constraints in some cases (for copy on write I guess). Similar to Salesforce DB. Probably a fine trade off though

**Answer**

All three decisions, the cryptographic tenant ID and the foreign-key trade-off are now in the documents and pushed (commit 62be272).

**One naming call I made for you.** You hadn't answered the instance-versus-catalog question, so I followed your own usage:

- **Tenant** is the unit of authority: its own data, policy decisions and ID. It replaces what the documents called "instance".
- **Instance** now means a running deployment, serving one tenant or many.

Say if you'd prefer a different word for either.

**What went in:**

- **Tenant ID.** It is cryptographically random, globally unique, and first in every key and every index. Records keep their keys when a tenant moves to another database. I wrote "random" with at least 122 random bits, which is a random UUID. If you meant an ID derived from a tenant's signing key, that's a small change, and it would let federated events be verified against the tenant ID itself.
- **One layer sets the tenant.** A tenant resolver sits at the outermost request layer. Hosted reads the hostname and checks the token's audience; self-hosted uses a resolver that always returns the single tenant. The schema is identical in both, and row-level security can stay off with one tenant. Background workers, caches, the change stream, storage and search must carry the tenant themselves, and the architecture lists them.
- **Per-tenant endpoints.** Added to `hosted.md`, with the rule that entity IRIs don't depend on the hostname.
- **Cheap copies as a principle.** It is now in the README's constitution draft as "Immutable data, cheap copies". The model gets a **manifest reference**, which can restrict a manifest to a key or partition range, to a commit position, or map it to another tenant. Clones, selections and sandbox tenants share data through these.
- **Fewer foreign keys.** References to immutable records, and references between tenants, are not database foreign keys. Three things keep them correct:
  - immutable targets can't change;
  - nothing is removed while any tenant references it;
  - a background check reports dangling references.

  Foreign keys remain between mutable rows within one tenant.
- **Commit positions.** Each catalog transaction writes a commit record, and the relay assigns stream positions in commit order. History rows carry commit IDs instead of wall-clock times.
- **Requirements.** New: ID-11 to ID-13, OP-12 to OP-15, EV-13 and EV-14. OP-8 is rewritten, because "a database per tenant" is no longer the rule.

**What `hosted.md` now admits:**

- **Leak risk in shared tables.** A mistake in row-level security or a cache key could expose one tenant's data to another. The isolation test suite is the defence.
- **Per-tenant restore.** In the shared tier it means restoring the database elsewhere and copying that tenant's rows.
- **Upgrades.** Schema migrations now apply per database, not per tenant.

**Still not written:**

- **Tenancy:** the organisation level above tenants, and soft tenants in a community instance.
- **Onboarding:** membership connectors, GitHub device login, and the enforcement levels for HPC storage.
- **Storage:** managed and external storage modes, mounts, and provider version IDs.
- **Schema file:** the scratchpad schema still has no tenant column or commit table.

## 99. salesforce db doesn't mention foreign key constraints being relaxed?

**Question**

> salesforce db doesn't mention foreign key constraints being relaxed?

**Answer**

Correct, it doesn't. I searched all three papers: none mentions relaxing foreign keys. The VLDB paper points the other way, listing "validating table constraints such as uniqueness or referential integrity" as part of executing an insert. So SalesforceDB keeps referential integrity as an ordinary SQL constraint.

Checking this also showed that I had written something wrong into the model yesterday. I said "a foreign key would block a cheap copy". That isn't true inside one database:

- **A clone only adds rows.** It creates new references to manifests that already exist, and a foreign key from a reference to its manifest is perfectly valid.
- **The foreign key helps.** It stops a manifest being removed while something still references it, which is the rule we wanted anyway.

What Salesforce does relax is at the storage layer, not in SQL. Immutable storage files are shared between tenants and between databases through extent references, and the paper names the resulting problem: making sure "extents shared across databases don't get prematurely deleted while one of the databases is still actively accessing the extent". That is reference tracking outside the database's constraint system, and it is the real parallel to our manifest references.

**What I've corrected** (commit 910a9fe, pushed):

- **Inside a tenant, foreign keys stay.** Each one includes the tenant ID, so a row can't point at another tenant's row by mistake. That is a standard multi-tenant safeguard.
- **Five kinds of reference can't be foreign keys:**
  - references to another tenant's records, because each tenant must be able to move to another database on its own;
  - references to another instance (federation);
  - references that can point to more than one kind of entity, such as a snapshot member;
  - references to elements, which live in manifest files;
  - references to erased records.
- **Some are left out for speed** in the billion-row tables for versions and replicas.
- **The fallback rules are unchanged:** immutable targets, no removal while referenced, background checks for dangling references, and erasure records.
- **The README principle** now says only that references between tenants have no foreign key.

So the trade-off you accepted is smaller than we both assumed. Cheap copies cost almost no referential integrity. What does cost some is tenant mobility and federation.

## 100. Am i misinterpreting something?

**Question**

> Am i misinterpreting something?

**Answer**

Not really. Your design instinct was right; the only thing that didn't hold was that the Salesforce papers say it. I also over-corrected in my last message.

**What you got right:**

1. **Copy-on-write between tenants does cost foreign keys in PostgreSQL.** Take a cheap sandbox clone, where tenant B reads tenant A's rows until B changes them. B's rows then point at parents that physically carry A's tenant ID, so a foreign key that includes the tenant ID can't be satisfied until the parent is copied. SalesforceDB avoids this because it translates tenant IDs *below* the SQL layer, so its constraints see a complete tenant. PostgreSQL has no such layer. The model's "copy for another tenant" case is exactly this, and those references can't be foreign keys.
2. **Immutable, versioned rows weaken what a foreign key means.** When nothing is deleted and a "delete" is a marker, a foreign key never fires. It also can't say "the parent was valid when the child was written". That is a temporal constraint, which ordinary foreign keys don't express.
3. **Salesforce the platform, as opposed to SalesforceDB.** From memory, the older Force.com architecture paper describes customer objects stored in generic tables, with relationships enforced by the application and not by database foreign keys. If that's what you were recalling, it is real. It just isn't in the three papers I read, so treat it as unverified.

**Where the papers differ from the claim:** they never mention relaxing foreign keys, and the VLDB paper lists referential integrity among the constraints it validates on insert. That is the whole discrepancy.

**Where I overstated.** I said cheap copies cost "almost no referential integrity". That's true only for clones inside one tenant, where a clone just adds references to existing manifests. It isn't true for tenant-level clones, which is probably the case you had in mind.

**A gap this exposes.** The model says a sandbox tenant "refers to the manifests of the source tenant through a tenant mapping". That works for manifests and data files, which are immutable files. It doesn't make the *catalog rows* cheap to clone: collections, artifacts, metadata payloads and grants would still be copied row by row in PostgreSQL. A sandbox with cheap rows would need either an overlay read path, which brings the foreign-key cost above, or a real copy. The documents don't yet say which, and I'd state that plainly instead of implying tenant clones are cheap end to end.
