# Design discussion, part 03: Removal and time travel, snapshots, maintenance, the hosted service, and Ray

This file is a record of the design discussion: each question, and the answer as it was given at that time. Later decisions can replace what an answer says. The documents in `docs/` hold the present design, and `docs/open-items.md` holds what is not yet in them. Paths under `/tmp` and commit IDs in the answers refer to the state at that time.

## 28. what about delete markers/tombstones or similar? Time travel?

**Question**

> what about delete markers/tombstones or similar? Time travel?

**Answer**

Both needed tightening. The design had tombstones and "as of T" resolution, but didn't separate the different meanings of "delete" or say how deletion interacts with history. Both are now defined in `docs/model.md`, with matching changes to the deletion workflow in `docs/architecture.md` and six new requirements (UP-9 to UP-14). All of it is pushed.

#### Five kinds of removal

The key distinction is direction in time. A forward operation affects only later states, like an Iceberg delete file or an S3 delete marker. A retroactive operation also changes what you can do with history.

| Operation | Effect | Direction | Bytes |
|---|---|---|---|
| Remove from a dataset | A new snapshot without the member | Forward | Kept |
| Delete a data object | A **delete marker**: an empty version becomes `latest`; old versions still resolve | Forward | Kept until retention ends |
| Invalidate | A **tombstone** on content, a source or a snapshot | Retroactive | Kept until purge |
| Purge | Bytes removed from every replica, with a receipt per replica | Retroactive | Removed |
| Erase | The catalog record removed, leaving only a minimal tombstone (ID, date, reason) | Retroactive | Removed |

The decisions behind it:

- **One zone-wide invalidation set.** A takedown doesn't write a new snapshot of every affected dataset. It goes into a single set that the resolver and the pinning service apply to every snapshot, old or new, so it takes effect at once, including for training sets already pinned.
- **Forward exclusions stay per dataset.** An exclusion list held in one dataset, such as one source's opt-outs, is the equivalent of an Iceberg deletion vector. It affects only the snapshots that contain it.
- **Hashes survive purges.** A snapshot keeps its Merkle hash after its content is purged, so the hash still proves what it contained. The one exception is erasure, when the hash itself could identify a person.
- **Legal holds block purge and erasure.** Where the storage has native retention locks (for example, S3 object lock), mudcat applies the hold through them.
- **Purging versioned buckets.** On storage with object versioning, the site agent also deletes the old object versions, so purged bytes are really gone.

#### Time travel

1. **You get history, not past permissions.** An "as of T" lookup returns membership and metadata as they were at T, but applies today's access control, usage policy and invalidation set. Time travel can't recover content that was taken down after T.
2. **Audit is a separate path.** "What was permitted at T?" is answered from recorded decisions: the policy decision stored with each pinned specification, with its authorization consistency token, and the audit log. Past decisions are never recomputed.
3. **Retention limits time travel honestly.** Published snapshots are kept forever. A lookup that needs an expired unpublished snapshot fails with an error that says so; it never quietly resolves to a different snapshot.
4. **Mutable state keeps its history.** Refs, aliases, ACLs, grants, replica states and lifecycle status keep a history of their changes, so an old path in a script still resolves to what it named at T. Validity periods that belong to the data, such as a licence term or an embargo date, are ordinary attributes. There is no second time dimension.

The deletion workflow in `docs/architecture.md` now follows these steps:

1. Check legal holds.
2. Add the tombstone to the invalidation set.
3. Find every occurrence.
4. Purge, with a receipt per replica.
5. Erase, where the request needs it.
6. Make redacted successor snapshots, where their owners want them.
7. Flag affected models for a person to decide.
8. Send the tombstone to the zones that hold copies.

## 29. maybe snapshots should be per-collection? I'm not sure how Iceberg does it

**Question**

> maybe snapshots should be per-collection? I'm not sure how Iceberg does it

**Answer**

Iceberg keeps snapshots per table. A table has its own schema, its own snapshot history and its own commits, while a namespace (Iceberg's folder) has no snapshots at all. Atomic changes across several tables exist only as an extra "commit several tables at once" operation in the REST catalog. Catalog-wide versioning comes from other tools built on top: Project Nessie and lakeFS give git-style commits and branches over a whole catalog or repository.

mudcat already matches Iceberg, with the dataset in the table's role. So I'd keep snapshots per dataset, and give collections three things instead:

- **History:** the change history of collection membership, aliases and refs is already recorded, so "list `/raw/2026/**` as of T" works without any snapshot objects.
- **Atomic changes:** one conditional commit can add or change many data objects across collections in one transaction. A run's thousand output files therefore appear together or not at all.
- **Snapshots on request:** a user can snapshot a collection subtree as of a time. mudcat makes a dataset whose snapshot holds the versions the subtree had then, with a Merkle hash, so it can be published and cited. This is the same operation as the Butler team's collection-snapshot proposal.

Automatic snapshots per collection would cause two problems:

- **Contention at the root:** a hash over a subtree changes with every write anywhere under it. Every writer would then race to update the same root ref, and concurrent writers would conflict constantly. This is the price of Nessie's catalog-wide commits.
- **Snapshot churn:** a folder that receives data continuously would get a snapshot for every change. The Butler team lists one huge, growing collection of raw data as a failure case. For that shape of data, a dataset series with one snapshot per period (one a day, for example) works better.

I've added this to section 4 of `docs/model.md` and added requirement ID-10 for snapshots on request; both are pushed.

## 30. address some of the commit comments in the for 4287094

**Question**

> address some of the commit comments in the for 4287094

**Answer**

I've addressed both comments on commit 4287094 and pushed the changes as commit 0ecd484. I didn't reply on GitHub.

**D2 diagrams.** The component diagram in `docs/architecture.md` is now a ` ```d2 ` block. I also converted the entity diagram in `docs/model.md` so the two match, and added refs to it, since they're now part of the model. Both pass `d2 validate`, and I rendered them to check that the layout reads correctly. GitHub's Markdown view doesn't render D2 (it renders Mermaid), so on GitHub these show as code. They render in tools that support D2.

**"Conditional commits" naming.** No, that isn't Iceberg's term. In the REST catalog a commit carries **requirements** (assertions about current state, such as `assert-ref-snapshot-id`) and **updates**, and Iceberg describes the scheme as optimistic concurrency. The architecture and model now use those terms, under "Commits with requirements". I'm confident of the requirements/updates shape and `assert-ref-snapshot-id`, but I didn't check the spec text in this session.

**Slow commits.** I agree it's a real risk to design around, and a REST catalog only partly helps. An Iceberg commit writes new manifests, a manifest list and a metadata file to object storage before it swaps the pointer, usually taking seconds. A writer that loses a conflict writes them again, and many small commits leave many small files and snapshots to compact and expire. A REST server applies the updates for the client, but the file writes remain.

The new "Commit cost" section keeps those writes out of mudcat's commit path:

- **Commits change rows only.** A commit is a database transaction that writes no files.
- **A compactor seals manifests later.** Members added or removed by recent commits stay as rows until a background compactor writes them into manifest files. A snapshot is then its sealed manifests plus any rows not yet sealed.
- **The Merkle hash is computed at publication,** not on every commit.
- **Appends don't conflict.** An update that only adds members is applied to the newest snapshot of its branch without a client retry. Removals and replacements keep strict requirements.
- **Ingest batches commits.** The ingest writer makes one commit per dataset per batch interval, so a high registration rate doesn't produce a snapshot per file.

I added requirement PF-5 for commit latency, with the numbers left TBD until there is something to measure.

## 31. i mean, visually it's still not compact

**Question**

> i mean, visually it's still not compact

**Answer**

The diagrams are now compact, and the change is pushed as commit 74fe34f.

| Diagram | Before (pixels) | After (pixels) |
|---|---|---|
| Architecture components | about 1600 × 2100, mostly empty space | 1353 × 2100, with no empty columns |
| Entity overview | 976 × 3542, a tall thin chain | two diagrams: 1890 × 667 (data, datasets and sources) and 2243 × 440 (training and evaluation) |

The changes were:

- **Layout engine:** both diagrams now set the ELK engine in the D2 file, which packs boxes much more tightly than the default.
- **Lists folded into boxes:** clients, gateways, movers and storage are now single labelled boxes, as in the original ASCII diagram, instead of rows of small nodes.
- **Grids:** the services and the change-log consumers are laid out as grids, and the edges between individual services are gone. Those edges were what pushed the layout wide.
- **Long edges removed:** "built-in, or SpiceDB · OpenFGA" is now part of the authorization port's label, replacing a separate external box. "Clients read it directly, with issued credentials" is now part of the storage label, replacing an edge across the whole page.
- **Entity overview split in two.** Its longest chain (evaluation result → evaluation → model → training run → pinned specification → representation → snapshot → version → source → policy) is ten boxes long. No layout can make that compact in one picture, so it is now a data diagram and a training diagram. The training diagram reads left to right: "pinned as", "used by", "generated".

I rendered each version to check it, and the final images are in `/tmp/claude-0/-home-user/f19f2493-b6c8-549b-9163-710160764721/scratchpad/` (`arch.png`, `core.png`, `train.png`). The training diagram is wide and short, so it scales down on narrow screens.

## 32. Table maintenance services/compaction are common pieces in iceberg deployments. I guess th

**Question**

> Table maintenance services/compaction are common pieces in iceberg  deployments. I guess they are site agents?

**Answer**

Partly. Site agents are the right home for the maintenance that touches bytes on storage. But Iceberg-style maintenance has a second half, on the catalog's own metadata, which belongs to a central worker. I've split it that way in a new "Maintenance" section in `docs/architecture.md`, and pushed it.

| Task | Iceberg equivalent | Who does it in mudcat |
|---|---|---|
| Seal recent member rows into manifest files; merge small manifests | Rewrite manifests | **Catalog maintenance**, a new central background worker |
| Expire unpublished snapshots that nothing refers to; remove manifests only they used | Expire snapshots | Catalog maintenance |
| Verify checksums; find dark data (files with no replica record) and lost replicas | Remove orphan files | **Site agents**, by comparing storage inventories with replica records |
| Compact small data files; rewrite files to drop purged content | Rewrite data files | A recorded transform, run by site agents or external compute |
| Maintain registered Iceberg, Delta or Lance tables | — | **Delegated** to the table's own engine; mudcat records the new table snapshot |

The main difference from Iceberg is that compaction is not invisible. In Iceberg, compaction swaps data files under the same logical table and readers notice nothing. In mudcat, files are immutable versions with checksums, replicas and lineage, so a silent rewrite would break provenance and verification. Compaction is therefore a transform activity that makes a new representation of the same snapshot:

- The logical members don't change, so pinned training-set specifications stay valid.
- The activity is recorded in provenance, with its inputs and outputs.
- Replication rules move to the new representation, and the old representation's replicas retire under their normal retention.

A rewrite that drops purged content changes the members, so it makes a redacted successor snapshot instead.

Catalog maintenance runs both on a schedule and from the change log. It seals a dataset's rows when they pass a size or age limit, and runs expiry daily. It never changes a published snapshot or its Merkle hash.

I also renamed the diagram's "Change-log consumers" box to "Background workers (change log and schedules)" and added catalog maintenance to it. There are three new requirements:

- **PL-6:** report dark data and lost replicas.
- **PR-7:** record each compaction or rewrite as an activity.
- **OP-6:** sealing never changes a snapshot's members or hash.

## 33. How could this run as a cloud service/saas?

**Question**

> How could this run as a cloud service/saas?

**Answer**

It splits naturally into a **control plane** that you run and a **data plane** that stays with each customer, because mudcat was already designed to stay out of the read path. I've written this up as a new document, `docs/hosted.md`, with a D2 diagram, and pushed it.

**The split:**
- **Vendor control plane:** metadata, provenance, policy and access decisions. Specifically the API, query compiler, resolver, policy engine, pinning service, authorization port, ingest writer, catalog databases, background workers, gateways, hosted MCP server and web UI. It never reads or stores customer data.
- **Customer data plane:** the bytes, plus everything that has to read them: site agents with their format plugins, movers, compaction compute, and an optional credential broker. Extraction, fingerprinting and checksum checks happen next to the data, and only the results go to the vendor.
- **Connections:** site agents open outbound connections and lease their tasks, like self-hosted CI runners, so customers open no inbound ports. Clients read storage directly with credentials mudcat issues.

**Storage credentials are the main trust question.** Each zone picks one of three modes:

| Mode | How it works | Can the vendor reach the bytes? |
|---|---|---|
| Delegated role | The customer grants a cloud role limited to named prefixes; the resolver gets short-lived scoped credentials | Yes, within that role, and visible in the customer's audit log |
| Credential broker | A small service in the customer's account issues the credentials | No. This is the "no data access" mode regulated customers will need |
| Agent-signed URLs | For HPC and on-premises storage, the site agent signs URLs or issues tokens | No |

**Tenancy:**
- **Unit of isolation:** the zone, since zones are already the unit of authority and federation.
- **One database per zone:** each zone gets its own PostgreSQL database on shared clusters. Backup, restore, migrations, deletion and customer-managed keys then work one tenant at a time, and rows can't leak between tenants. Very small zones can share a database, one schema each.
- **Four tiers:** shared, dedicated, operated inside the customer's own cloud account, and self-hosted. All four run the same release.

**Regions:** each zone is pinned to one region, because metadata (file names, sources, consent status) is sensitive too. The vendor runs one control plane per region, and a tenant's zones in different regions federate like any other zones.

**Also covered in the document:**
- **Manifests:** stored per tenant, or in a bucket the customer provides.
- **Indexes:** separate for each tenant.
- **Fairness:** a queue and limits for each tenant.
- **Operations:** migrations applied tenant by tenant with canaries, and per-zone point-in-time recovery.
- **Audit:** the audit log streams to the customer's own security tools.
- **Exit:** a full zone exports in a form the self-hosted release can import.
- **Pricing:** catalog entities, indexed elements, resolutions and connected agents, never data volume, since the vendor doesn't hold the data.

I added requirement RD-5 (no data access in broker mode) and OP-7 to OP-11 (same release hosted and self-hosted, a database or schema per zone, outbound-only agents, full export, metadata residency). I also added a "Hosted" row to the architecture's deployment profiles.

**Risks the document lists:**
- **Delegated role:** this mode gives the vendor a path to customer bytes. The credential broker closes it, but the customer then has a component to run.
- **HPC start-up:** HPC jobs that resolve names at start-up depend on the control plane being available. Manifests and credentials that a job can cache for its lifetime reduce this.
- **Closed enclaves:** some enclaves permit no outbound connections. They need an approved relay, or a self-hosted zone that federates with the hosted ones.
- **Many databases:** one database per zone needs early investment in automation.

## 34. what if the customer controls some of the metadata?

**Question**

> what if the customer controls some of the metadata?

**Answer**

The design can support that, but "the customer controls some metadata" can mean two different things, and each needs a different mechanism. I've added both to `docs/hosted.md`, with matching changes to the model and requirements, and pushed them.

**1. Authority: a customer system is the only writer of some metadata.** An example is a laboratory system that owns assay results. The vendor may still store a copy.
- A customer can own an attribute namespace and name the only principals allowed to write to it, such as the laboratory system's service account.
- mudcat rejects every other write to that namespace, including writes from its own background workers.
- The record of who asserted each value already shows the customer system as the writer.

**2. Custody: some metadata must never leave the customer's environment.** Examples are consent status, patient attributes, proprietary structures or confidential file names. Each attribute definition now has one of three custody levels:

| Custody level | Where values are stored | What the vendor can do with them |
|---|---|---|
| Vendor-held | The zone's database in the control plane | Everything: queries, sorting, facets, policy checks |
| Customer-encrypted | The vendor stores encrypted values; only the customer holds the key | Equality matches only, through a keyed hash the customer computes (a "blind index"); no range queries, sorting or text search |
| Customer-held | A metadata store in the customer's own environment | Nothing; the vendor only knows that the attribute exists for an entity |

Blind indexes reveal which entities share a value. Customers must accept that for each attribute they encrypt. Rare values that could identify a person must be customer-held.

**The customer gateway.** The optional credential broker from the earlier design becomes a customer gateway. It holds the customer's keys and serves customer-held metadata. Like the site agents, it opens its own outbound connection, so the customer still opens no inbound port. Each part of the system works with it like this:

- **Queries are split.** The vendor evaluates the conditions on vendor-held attributes. The gateway evaluates the conditions on customer-held attributes and returns only matching IDs, never values. The two results are joined by ID.
- **The web UI.** The browser fetches customer-held values straight from the gateway with the user's own token, so the values never pass through the vendor.
- **Usage policy.** A check that depends on customer-held data, such as consent at pinning, goes to the gateway. The gateway returns a signed decision. mudcat records the decision and signature in the pinned specification, without the values.
- **Search.** The vendor's indexes cover only vendor-held fields. A customer can run its own index behind the gateway.
- **If the gateway is down,** only queries and pinnings that need customer-held metadata fail. Everything else keeps working.

**For larger amounts of sensitive metadata,** custody levels suit a few sensitive attributes. If whole datasets are sensitive, the customer can run a small self-hosted zone and federate it with its hosted zones, under the normal rule that each zone decides about its own data.

The new requirements are ST-9 to ST-12:
- **ST-9:** writes to a namespace from principals its owner didn't name are rejected.
- **ST-10:** customer-held values are stored only in the customer's environment.
- **ST-11:** conditions on customer-held attributes are evaluated only through the gateway.
- **ST-12:** policy decisions are recorded without the customer-held values.

## 35. how does snowflake do some things ?!

**Question**

> how does snowflake do some things ?!

**Answer**

From my own knowledge, not checked against Snowflake's documentation in this session, so verify details before relying on them. Snowflake answers several of the questions we've just worked through, and in most places it is the opposite case to mudcat: it owns the storage and the compute, while mudcat owns neither.

| Snowflake feature | How it works | What it means for mudcat |
|---|---|---|
| **Three layers** | A multi-tenant "cloud services" layer (metadata, optimiser, security, transactions), separate compute warehouses, and storage in cloud object stores | Our control plane is their cloud services layer. We have no compute layer, and storage stays with the customer |
| **Metadata store** | FoundationDB, a transactional key-value store shared across accounts | We chose one PostgreSQL database per zone. Theirs scales to very many accounts with less to operate; ours is simpler to build and isolates tenants physically. Ours costs more to operate at large tenant counts |
| **Time Travel and Fail-safe** | Dropped or changed data recoverable for 1–90 days (`UNDROP`, `AT`/`BEFORE` queries), then 7 days of vendor-only recovery | A bounded recovery window, unlike our "keep published snapshots forever". We could adopt an undo window for catalog deletions before purge runs. That is a small addition |
| **Zero-copy cloning** | A clone shares the storage blocks of its source; only changes are written | Same idea as our snapshots sharing unchanged manifests, and as snapshots of a collection on request. Nothing new |
| **Tri-Secret Secure** | Data keys are wrapped by both Snowflake's key and a customer-managed key; revoking the customer key makes the account's data unreadable | Stronger than per-attribute encryption: a customer can cut the vendor off from a whole zone at once. Worth adding as a zone-level option to `hosted.md` |
| **Secure data sharing** | A provider grants another account read access to objects; nothing is copied. Cross-region sharing needs replication | Inside one hosted region, sharing between tenants can be a grant, not full federation. Across regions it falls back to our federation feed, as theirs falls back to replication |
| **Tag-based policies** | Masking and row-access policies attach to tags, and tags attach to columns and objects, so classifying data applies the policy | Better than setting custody per attribute definition. A classification tag such as "personal" would set the custody level and default duties for every attribute carrying it |
| **Access history** | Records which columns each query read, as an audit and lineage source | The same as our "observed" usage relations from issued credentials |
| **Aggregation and projection policies** | Results allowed only as aggregates over at least N rows; some columns usable in filters but not returned | Directly useful for enclaves and controlled access ("counts below 20 are not published"). It belongs in the usage-policy vocabulary and the customer gateway |
| **Clean rooms** | Two parties run approved queries over joined data without seeing each other's rows | Matches our partner and enclave cases. It's a query-engine feature, so mudcat would only record and enforce which approved activities may use which sources |
| **Iceberg tables and Polaris (Open Catalog)** | Snowflake can manage or read Iceberg tables in customer storage, and donated Polaris, its Iceberg REST catalog, to Apache | Confirms the "customer owns storage, vendor owns catalog" split for tables. mudcat's Iceberg gateway could also register with a Polaris catalog |
| **Organisations, accounts and regions** | An account lives in one region and cloud; organisations span accounts; replication and failover groups copy objects between them | Same shape as our zone pinned to a region, with federation between zones |

On the evidence, I'd adopt four of these, in order of value:

1. **Tag-driven policy:** classification tags set the custody level and default duties.
2. **Zone-level customer keys** in the hosted service, on the Tri-Secret Secure model.
3. **Aggregation limits** in the usage-policy vocabulary, for enclaves and controlled-access data.
4. **In-region sharing between tenants** as a grant, keeping federation for cross-region and self-hosted zones.

An undo window before purge is a smaller fifth candidate. I haven't changed the documents for any of these yet.

## 36. i meant more like augmentation / plugin metadata

**Question**

> i meant more  like augmentation / plugin metadata

**Answer**

Snowflake handles added metadata through four mechanisms: governed tags, automatic classification, data metric functions, and native apps. I've borrowed the parts that fill a gap in mudcat and pushed them. As before, the Snowflake details are from my own knowledge, not checked against its documentation.

**How Snowflake does it:**
- **Object tags.** A tag is a schema object with an owner and a list of allowed values. It is applied to accounts, databases, schemas, tables and columns, and is inherited down that hierarchy. Newer releases can also carry tags along lineage to derived tables; I'm not sure of the current details. Masking and row-access policies attach to tags, so tagging a column applies the policy.
- **Automatic classification.** A built-in classifier, which customers can extend with their own, writes system tags for semantic and privacy categories. Its results are just tags.
- **Data metric functions.** These are built-in or user-written functions attached to a table, run on a schedule, and their results land in one central place. They are plug-in quality metadata.
- **Native apps.** A third party's code is installed into the customer's account and runs there under the customer's grants, with its outputs staying in that account. The code goes to the data, not the other way round.
- **Generated descriptions.** An AI feature proposes table and column descriptions, which a person accepts.

**What I added to mudcat:**

- **Tags** (`docs/model.md`, section 3). A tag is an owned classification attribute with a closed list of values. Tags have three behaviours:
  - a tag on a collection applies to everything under it, unless a lower level sets another value;
  - the tag's owner can make it propagate along lineage, as duties do;
  - custody levels, default duties and access conditions can attach to a tag value, so classifying data applies the policy.
- **Augmenters**, a general plugin model for computed metadata. Format plugins are now one kind of augmenter; others compute classifications, quality metrics, fingerprints, embeddings and AI-written descriptions. Each augmenter declares:
  - its inputs, including whether it needs the bytes or only the metadata;
  - its outputs, which can only go into namespaces the augmenter owns, so two plugins can't overwrite each other;
  - its trigger: registration, a change, a schedule or a request;
  - where it runs;
  - whether a person must review its output first.

  Every value records the augmenter and version that produced it. Output that needs review stays "proposed" and hidden until a person accepts it, which is the default for AI-generated descriptions. A new augmenter version recomputes its outputs in the background, and the old values stay until the new ones are complete.
- **Where augmenters run** (`docs/architecture.md`):

  | Place | Augmenters |
  |---|---|
  | Site agents, in a sandbox | Those that read bytes |
  | Control-plane workers | Those that read only metadata; first-party only |
  | Customer gateway | Those that read customer-held metadata |

  Third-party augmenters never run in the control plane. As with Snowflake's native apps, they run in the customer's environment under the customer's grants.
- **Requirements ST-13 to ST-17:** augmenters write only to their own namespaces; every output carries its augmenter and version; reviewed output stays hidden until accepted; third-party code runs only in the customer's environment; tags are inherited down collections.

What I didn't borrow is Snowflake's practice of running user code inside the vendor's service. In mudcat the control plane runs only first-party code. That keeps the "vendor never touches the bytes" guarantee from the hosted design.

## 37. let's say you wanted to use this system with ray to achieve some goal over the multimodal 

**Question**

> let's say you wanted to use this system with ray to achieve some goal over the multimodal
> data. What are some ways you might use it?

**Answer**

Ray fits mudcat well, because the two have complementary jobs:

- **mudcat** decides *what* data, *which* copy, *whether it's permitted*, and records *what happened*.
- **Ray** does the compute and moves the bytes, without the catalog in the read path.

The natural integration is a Ray Data **datasource** that reads a pinned specification and a **datasink** that registers outputs with provenance. Here are the patterns I'd expect, roughly in order of how often they'd come up.

#### 1. Curation: compute metadata over a dataset

Select data with a mudcat query, run GPU models in Ray, and write the results back as an **annotation set**, so the raw data is never rewritten. Typical outputs are captions, quality scores, safety labels, embeddings and classifier tags.

```python
import ray, mudcat  # the mudcat SDK is hypothetical; the Ray calls are real APIs

sel = mudcat.query('/video/**', where='duration > 5 and tag:license == "cc-by"')
ds = ray.data.read_datasource(mudcat.ray.Source(sel, structure="stream", clip="10s"))

scored = ds.map_batches(AestheticScorer, concurrency=16, num_gpus=1, batch_size=64)

scored.write_datasink(mudcat.ray.AnnotationSink(
    dataset="video-corpus", name="aesthetic-v2", augmenter="aesthetic-scorer@2.1"))
```

- **Inputs:** the datasource turns each selector into a Ray read task: a byte range, a tar member, an array window or a video clip.
- **Outputs:** they are written as a new snapshot that shares every other member with its parent.
- **Provenance:** the run, the model version and the inputs are recorded through the run token, with no extra code. This is the augmenter model running on external compute.

#### 2. Training with a pinned specification

```python
spec = mudcat.pin("specs/vlm-mix@main", action="train", purpose="research")  # policy checked here
plan = mudcat.resolve(spec, near="cluster:us-east-gpu")                       # nearest verified copies

trainer = TorchTrainer(train_loop, scaling_config=ScalingConfig(num_workers=256, use_gpu=True),
                       datasets={"train": ray.data.read_datasource(mudcat.ray.Source(plan))})
result = trainer.fit()
mudcat.publish_model("models/vlm-base", result.checkpoint, run=plan.run)
```

- **Policy at pinning:** licences, opt-outs and the invalidation set are applied before Ray starts. A job never sees excluded data.
- **Parallel resolution:** each part of the resolution plan becomes a group of Ray read tasks, so workers fetch parts in parallel and the catalog never sits in the data path.
- **Locality:** if the data isn't complete near the cluster, a replication rule stages it first. The job can wait until the pinned specification is complete and verified on that site.
- **Recorded lineage:** the checkpoint is registered as a model version, linked to the pinned specification. "Which data trained this model" becomes a recorded fact.

#### 3. Evaluation and batch inference

- **Leakage check first:** check the evaluation suite against the model's training specification under a similarity criterion before running anything.
- **Run it:** run inference with `map_batches` on actor pools.
- **Record it:** write metrics as an `EvaluationResult` and per-element predictions as an annotation set. Errors can then be analysed with ordinary metadata queries.

#### 4. Building the content-identity index and deduplication

- **Fingerprint at scale:** Ray computes exact hashes, MinHash for text and perceptual hashes for images and video across the whole zone.
- **Index build:** this is the batch job that builds the content-identity index, the hardest part of the design to scale.
- **Duplicates:** near-duplicate groups become an annotation set. Specifications can filter on it, for example "one per group".

#### 5. Format conversion and compaction

- **New representations:** convert native files such as HDF5, MP4 or raw logs into training shards (Lance, Parquet or WebDataset), or pack many small files into large shards.
- **Each one is recorded:** the output is registered as a new representation of the same snapshot, with the transform recorded. This is the compaction that the maintenance section assigns to "external compute".
- **Science data:** the same pattern reads array windows (hyperslabs over Zarr or netCDF) and writes analysis-ready shards, using the sample rule from the specification.

#### 6. Mining and active learning

- **Find candidates:** use vector search over an embedding annotation set, for example "scenes like these failures". That gives a selection: a subset of a snapshot that copies nothing.
- **Rank them:** Ray applies model uncertainty or a scenario classifier.
- **Label and train:** the selection goes to labelling, and the labels come back as a new annotation set that the next specification includes.
- **For AI-for-science,** this is the loop from model to candidate to experiment, recorded as lineage across model generations.

#### 7. Takedowns at scale

- **Mark affected shards:** after a takedown, the deletion workflow uses the identity index to find every affected shard.
- **Rewrite in parallel:** Ray rewrites the shards without the removed elements, producing redacted successor snapshots.
- **Purge:** site agents then purge the old bytes and record a receipt per replica.

#### What mudcat would need to provide for this

| Piece | Why |
|---|---|
| `mudcat.ray.Source`: a Ray `Datasource` over resolution parts, with readers for each structure kind (byte range, member, hyperslab, time span) | Without it, every team writes its own read code and loses provenance |
| `mudcat.ray.Sink` and `AnnotationSink`: Ray `Datasink`s that write through bulk Arrow registration, batched and idempotent | Ray retries tasks, so registration must tolerate duplicates (OP-5 already requires this) |
| Locality hints: map storage resources to Ray node labels | Lets Ray schedule read tasks near their replicas |
| Credential refresh inside long jobs | Training runs outlive short-lived credentials |
| Run-token capture in the Ray workers | Gives observed lineage without user code |

None of these is in the design documents yet. They'd fit as a short "Ray integration" section in the architecture, and as an early entry in the build order, since a Ray integration and a data-loader integration are what make mudcat useful to the AI-for-science and multimodal teams we picked as first users.

## 38. what about more advanced ways? what if you had file-specific metadata or collection specif

**Question**

> what about more advanced ways? what if you had file-specific metadata or collection specific metadata which might influence  execution in someway, the way iceberg stores stats, bloom folters, etc...

**Answer**

The bigger win is to let metadata steer execution, as Iceberg does, and not only describe the data. I've added this to the design as **statistics** and **hints**, a planner that uses them, and Ray connectors that consume the plan. It's pushed.

One correction to the premise: Iceberg keeps per-file column bounds and counts in its manifests, partition summaries in its manifest lists, and table-level sketches (such as distinct-value counts) in Puffin files. The Bloom filters live inside Parquet files, not in Iceberg metadata. mudcat follows the same layering, but it has to cover data that isn't tabular.

#### Statistics at four levels

| Level | Examples | Kept in |
|---|---|---|
| Element | Size, duration, decode cost, label counts, quality score | Manifest columns |
| File or object | Attribute bounds, time span, array extent, sources, tag values, codec, measured decode time | A `stats` namespace on the version |
| Manifest | Bounds and counts over its entries; a Bloom or xor filter of fingerprints and IDs; a bitmap of sources and tag values; an embedding centroid | A summary next to each manifest |
| Snapshot | Distinct-value and quantile sketches, histograms, counts per source and per tag | A statistics file, possibly in Puffin format |

Four rules keep them safe:

- **Bounds are conservative.** Filters may give false positives but never false negatives, so pruning never changes a result.
- **They never go stale,** because they describe immutable versions, manifests and snapshots.
- **Augmenters compute them,** with their version recorded, so they are recomputed when an augmenter changes.
- **Engines can feed them back.** A job can report measured decode times or files it couldn't read, and the next plan improves.

#### What the statistics let you do

- **Prune before reading.** Skip manifests, then files, by time span, array extent, tag set or attribute bounds. A query such as "night-time scenes with pedestrians" opens only the shards whose summaries allow it.
- **Check policy per manifest, not per element.** If every source a manifest uses permits the action and purpose, the whole manifest passes in one check. Only mixed manifests, or ones the invalidation set can touch, get a per-element check. This is what makes pinning feasible at 10^11 elements.
- **Answer contamination and takedown checks cheaply.** Fingerprint filters show which manifests might contain an item, so only those are scanned.
- **Route vector search.** Embedding centroids let a similarity search open only the shards near the query.
- **Compute mixtures without scanning.** Counts per source and per tag give exact component sizes, so mixture weights become sample rates directly. Quantile sketches support stratified sampling and curricula ordered by difficulty.
- **Size tasks by cost, not file count.** Read tasks are balanced on bytes, element counts and measured decode cost, so a few huge or slow videos don't hold up the job.
- **Process only what's new.** Snapshot change summaries let an augmenter handle just the members a new snapshot added.
- **Order tape recalls.** Tasks are sorted by tape and position, and site agents stage files in batches.

#### Hints set by people or policy

Hints sit on collections, datasets or tag values, and inherit down collections like tags do:

- access pattern (sequential or random);
- required layout (sort order, chunk shape);
- required compute location (for example, a region for data with a residency tag);
- special hardware (for example, GPU video decode);
- cache class (keep on local NVMe during training).

The planner turns them into scheduling constraints. Data tagged for residency then only runs on nodes in its region.

#### With Ray

Each task in a resolution plan comes with its expected bytes, row count, cost and replica location. The read connector passes these to Ray's own planner as estimates, and passes placement constraints as node labels. It also refreshes credentials during long jobs and carries the run token, so lineage is captured without user code. The write connector registers outputs idempotently, since Ray retries tasks, and reports observations back as statistics.

**Where it went in the docs:**
- **`docs/model.md` section 4:** statistics and hints.
- **`docs/architecture.md`:** "Planning with statistics" and "Compute-engine integration".
- **Requirements:**
  - **RD-6:** prune only on proof that nothing inside matches.
  - **RD-7:** each plan task returns its size, row and location estimates.
  - **RD-8:** Ray connectors that pass the run token.
  - **UP-15:** the per-manifest policy shortcut is allowed only when every source permits the action and nothing is invalidated.
