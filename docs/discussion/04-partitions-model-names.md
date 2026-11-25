# Design discussion, part 04: Partitions and manifests, the model, the Iceberg REST interface, and names

This file is a record of the design discussion: each question, and the answer as it was given at that time. Later decisions can replace what an answer says. The documents in `docs/` hold the present design, and `docs/open-items.md` holds what is not yet in them. Paths under `/tmp` and commit IDs in the answers refer to the state at that time.

## 39. what do you suppose the database schema would look like? Don't commit it, just lmk

**Question**

> what do you suppose the database schema would look like? Don't commit it, just lmk

**Answer**

This answer described a sketch of the database schema. The owner of the project asked that the schema stay outside the repository, so the answer is not in this record.

## 40. mudcat doesn't have a manifest list though I guess.

**Question**

> mudcat doesn't have a manifest list though I guess.

**Answer**

Right. There's no separate manifest-list file. In the sketch, `ds.snapshot_manifest` together with the summary columns on `ds.manifest` does the manifest list's job, but in PostgreSQL rows, not in a file. That's deliberate, but it hides one real problem and loses one useful property.

**Why Iceberg has a manifest list.** There's no database, so a snapshot has to be a file. The manifest list names the snapshot's manifests and carries a summary of each, such as partition bounds and file counts, so a reader can skip manifests without opening them. In mudcat the database holds that index, and keeping it in rows is what keeps commits to row writes, with no file written per commit.

**The problem the join table hides.** Each snapshot repeats one row for every manifest it shares. A dataset with 10^4 manifests and 10^5 snapshots would need 10^9 `snapshot_manifest` rows that say almost nothing new. Iceberg pays the same cost, since every manifest-list file lists all of a snapshot's manifests, but it pays in cheap object storage.

**The property it loses.** Iceberg snapshots are self-contained in files. Anyone with storage access can read or verify one without the catalog. mudcat needs that too, for three things:
- federation and export;
- third parties checking a Merkle hash without our database;
- the Iceberg gateway, which must produce Iceberg metadata files anyway.

**What I'd change:**

1. **A manifest list becomes its own immutable, content-addressed entity.** Snapshots point at a list instead of each owning rows:

   ```sql
   CREATE TABLE ds.manifest_list (
     id uuid PRIMARY KEY DEFAULT uuidv7(),
     digest bytea NOT NULL UNIQUE,         -- hash of its entries; feeds the snapshot Merkle hash
     entries bigint NOT NULL,
     bounds jsonb, source_bitmap bytea,    -- aggregated summaries, one level up
     uri text                               -- set when written out as a file
   );
   CREATE TABLE ds.manifest_list_entry (
     list_id uuid NOT NULL REFERENCES ds.manifest_list,
     child_manifest_id uuid REFERENCES ds.manifest,
     child_list_id uuid REFERENCES ds.manifest_list,   -- lists can nest
     PRIMARY KEY (list_id, coalesce(child_manifest_id, child_list_id))
   );
   ALTER TABLE ds.snapshot ADD COLUMN manifest_list_id uuid REFERENCES ds.manifest_list;
   -- drop ds.snapshot_manifest; sealed_base_id becomes "the snapshot whose list this is"
   ```

   That primary key won't load as written: PostgreSQL doesn't allow expressions in a primary key, the same error the earlier sketch hit on `prov.edge`. It needs either a single nullable-free child column with a kind flag, or a unique expression index in place of the primary key.

2. **Lists change only when the compactor seals rows, not on every commit.** A commit adds member-delta rows and reuses its parent's list unchanged. Sealing makes a new list.
3. **Lists nest, so sealing is cheap.** A large dataset's list is a shallow tree of lists. Sealing rewrites only the changed path, O(changed manifests + depth), the same structural sharing the design already uses for manifests. Per-list summaries let the planner prune whole subtrees, which Iceberg's single flat list can't do.
4. **Publication writes the list out as a file.** Publishing a snapshot also writes its manifest lists to the manifest store, next to the manifests. A published snapshot is then fully described by files, and its Merkle hash is just the root list's digest. Unpublished snapshots never need the file.

This drops the 10^9-row join table and makes the snapshot hash fall out of the structure naturally. Published snapshots become portable without the database.

Following your earlier instruction, I haven't committed anything. The schema file in the scratchpad still has the old `snapshot_manifest` table, and the model and architecture documents still describe snapshots as "manifests plus unsealed rows", without this list layer.

## 41. or basically partition

**Question**

> or basically partition

**Answer**

Yes, mostly. Partitioning is the better way to build that tree, because it gives the levels a meaning. Nested manifest lists with no meaning are just a balanced index. If the levels are partition values, the same structure also prunes queries, localises change and lines up with policy. That's what Iceberg does: its manifest list carries per-partition summaries, and manifests are grouped by partition.

**How it would look:**
- **A partition spec on each dataset**, using Iceberg-style transforms over member attributes. Examples:
  - `day(registered_at)` for a growing series;
  - `source`;
  - `bucket(64, data_object_id)` to bound fan-out;
  - a collection path prefix;
  - a tag value.
- **Levels follow the spec.** Each level of the list tree is one partition key, and each leaf is the set of manifests for one combination of partition values.
- **A commit reseals only its partitions.** Appends to a time-partitioned series touch only the newest day. A takedown touches only the partitions its fingerprints fall in.
- **Specs can change.** Each manifest records the spec version it was written under, so old manifests stay valid, as in Iceberg's partition evolution.

**Why it fits the rest of the design:**
- **Snapshot size:** the Butler's one huge, growing raw collection becomes a series partitioned by day, which was their own suggested fix.
- **Policy at pinning:** with `source` as the top-level key, the shortcut that skips per-element checks becomes exact. A whole partition is permitted or not, with no mixed manifests to fall back on.
- **Custody and residency:** partitioning by ACL root or region keeps each manifest within one access boundary and one jurisdiction. That's what makes prefix-scoped credentials and residency rules easy to enforce.
- **Pruning:** time-range, source and tag predicates skip whole subtrees using per-partition summaries, before any manifest is opened.

**Where it can go wrong, as in Iceberg:**
- **Too many small manifests.** A high-cardinality key, such as one partition per data object, recreates the small-file problem. Use bucketing to bound the count.
- **Skew.** One huge source or one busy day makes one partition dominate.
- **The wrong key.** Pruning only helps queries that filter on the partition key. Statistics still cover the rest.
- **Conflicting goals.** Source-first helps policy, and time-first helps appends and series. A two-level spec such as `source / day` gets both, at the cost of more manifests. That should be a per-dataset choice with a sensible default, perhaps `bucket(n, data_object_id)`, plus time for series.

**What changes in the schema sketch:**

```sql
ALTER TABLE ds.dataset  ADD COLUMN partition_spec jsonb;          -- versioned: [{field, transform}, …]
ALTER TABLE ds.manifest ADD COLUMN spec_version int,
                        ADD COLUMN partition_value jsonb;         -- e.g. {"source": "…", "day": "2026-11-24"}
-- manifest_list_entry is keyed by partition value at each level, with per-partition summaries
```

This is about partitioning the members. PostgreSQL table partitioning of `snapshot_manifest` wouldn't fix the row multiplication on its own. Partitioning the members is what makes each snapshot's list small and shared.

As before, nothing is committed. The design documents don't yet mention manifest lists or partition specs, and the scratchpad schema doesn't have these columns.

## 42. what would be needed to support partitioning evolution, same as iceberg?

**Question**

> what would be needed to support partitioning evolution, same as iceberg?

**Answer**

Most of what Iceberg needs, mudcat needs too. One thing is much easier for mudcat, and one thing I said earlier has to change.

#### What Iceberg does

- **Specs are kept, not replaced.** Each spec has an ID, and table metadata keeps every spec, not just the current one.
- **Each manifest records its spec ID**, and each data file carries its partition values under that spec. New writes use the current spec, and old files are never rewritten.
- **Partition fields have their own stable IDs**, and point to source columns by field ID, so renaming a column doesn't break a spec.
- **The planner projects each query predicate through every spec it meets,** turning a row filter into a partition filter for each spec, and combines the results.
- **Partitioning is hidden.** Queries filter on source columns, never on partition columns.

#### What mudcat would need

1. **A spec registry for each dataset.** Each spec is immutable, has a stable `spec_id`, and lists fields of the form (partition field ID, source field ID, transform). The current spec is a pointer, and changing it is a commit whose requirement asserts the current spec ID (Iceberg has `assert-default-spec-id`). Source fields are referenced by their stable field ID, which requirement ST-8 already gives us.
2. **Transforms with a fixed, versioned definition.** Iceberg's `identity`, `bucket[N]`, `truncate[W]`, `year`, `month`, `day` and `hour`, plus mudcat ones such as `source`, `path_prefix[d]` and `tag[name]`. Each transform needs three things:
   - an exact apply function that never changes. Bucket hashing especially must be pinned forever; Iceberg pins a specific Murmur3 variant;
   - an **inclusive projection**, which maps a row predicate to the partitions that might match. This is used for pruning;
   - a **strict projection**, which identifies the partitions where every member matches. The per-manifest policy shortcut (UP-15) needs this: a whole partition may be accepted only if the strict projection proves every member's source permits the action.
3. **The spec ID and partition values on each manifest.** Partition values are stored keyed by partition field ID, not by position. Dropping a field then needs none of the "void" transforms Iceberg v1 needed.
4. **Lists grouped by spec, then by partition.** The top level of a snapshot's manifest-list tree is the spec ID; the levels below follow that spec's fields. Summaries are kept per spec and per partition.
5. **A planner that works across specs.** For each spec in a snapshot, project the predicate, prune that spec's subtree, then combine the results. Time travel needs nothing extra, because old manifests carry their own spec.
6. **Hidden partitioning at registration.** mudcat computes partition values from member metadata when it seals rows, so writers and Ray sinks never need to know the spec. This means the spec's source attributes must be present at registration, so they should be required by the dataset's schema. A missing value goes into a null partition.
7. **A repartition maintenance task.** It rewrites old manifests under the current spec, with no change to members.

#### Easier in mudcat

In Iceberg, the partition spec also decides how data files are laid out. Evolution matters there because old data stays laid out the old way. In mudcat, the spec only groups catalog metadata, and the data bytes never move. Repartitioning is therefore a metadata-only background job: expensive at 10^11 elements, but always possible. Keeping several specs side by side is an optimisation and a way to preserve history, not a necessity. Hints can still ask writers to align file layout with the spec, so pruning lines up with storage.

#### A correction to what I said earlier

I said a published snapshot's Merkle hash could simply be the digest of its root manifest list. That's wrong once specs can evolve. Repartitioning or resealing would then change the hash of a snapshot whose members haven't changed, which breaks requirement OP-6. The two hashes must be separate:

- **Content hash:** independent of layout, computed over the members only. It can be a Merkle tree over members in a canonical order (sorted by element or version ID, not by partition), which still updates in O(log n). Or it can be an order-independent set hash that updates on each add or remove, such as a lattice hash; that would need its security properties checked before use.
- **Storage digests:** the hashes of manifests and manifest lists, used for integrity and deduplication of those files. They change when the layout changes, and nothing outside mudcat depends on them.

#### In the schema sketch

```sql
CREATE TABLE ds.partition_spec (
  dataset_id uuid NOT NULL REFERENCES ds.dataset,
  spec_id    int  NOT NULL,
  fields     jsonb NOT NULL,   -- [{partition_field_id, source_field_id, transform, transform_version, name}]
  created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (dataset_id, spec_id)
);
ALTER TABLE ds.dataset  ADD COLUMN current_spec_id int;
ALTER TABLE ds.manifest ADD COLUMN spec_id int, ADD COLUMN partition jsonb;  -- keyed by partition_field_id
-- manifest_list_entry: keyed by (list_id, spec_id, partition) with per-partition summaries
-- ds.snapshot: content_hash (layout-independent) kept separate from manifest_list digest
```

Federation, export and the Iceberg gateway would also have to carry the specs with any exported lists, so other readers can interpret them. Transforms that exist only in mudcat (source, tag, path prefix) would appear to Iceberg readers as identity transforms on derived columns.

Nothing is committed, as you asked. The design documents still describe the snapshot hash and manifests as before; the hash correction above is the one change I'd make there regardless of partitioning.

## 43. I feel like partitioning and versioning are kind of similar in that you want them to be tr

**Question**

> I feel like partitioning and versioning are kind of similar in that you want them to be transparent/ignorable when you don't really define them (when there's just one version or one partition), but you need to be able to support them later in some way

**Answer**

Yes. I'd treat that as a general design principle, because the same pattern shows up in many places in mudcat, not just those two:

| Dimension | Default when undefined | When it starts to matter |
|---|---|---|
| Version | One version; "latest" | A file is corrected or reprocessed |
| Ref | One branch, `main` | Curation and publication separate |
| Partition | One partition (an empty spec) | A dataset gets large or mixes sources |
| Representation | One, the native form | Training needs shards |
| Replica | One copy | A second site or cluster |
| Zone | One zone | A partner or a second institution |
| Schema or profile | Undeclared attributes | A community standard applies |
| Custody | Vendor-held | Sensitive attributes |

The rules that make "ignorable now, supported later" work:

1. **The default is a real value, not an absence.** An unpartitioned dataset has spec 0 with no fields, which is what Iceberg does. A single file is version 1, and an unbranched dataset still has `main`. Adding a second value is then an ordinary addition with no migration. "Null means default" splits every code path in two, and that cost appears exactly when the dimension starts to matter.
2. **Requests may omit a dimension; records must not.** A request without a version or a partition resolves through a default rule: latest on `main`, all partitions, the native representation, the nearest verified replica. Datacat's views already worked this way. But anything that must be reproducible records the resolved values explicitly: pinning, publication, provenance and citations. Otherwise a second version silently changes what an old reference meant. In short: floating in requests, explicit in records.
3. **Adding a value never changes existing identities.** A version 2 doesn't change version 1's ID. A new representation doesn't change the snapshot. A new partition spec doesn't change the content hash. That last one is why the content hash has to be separate from the manifest layout, as I corrected last time.
4. **An unused dimension costs close to nothing.** One partition means the planner skips a level. One version means no extra lookup. The rows exist but are trivial.
5. **The UI and API show a dimension only once it has more than one value.** A user with single files in one bucket never sees versions, branches, partitions or replicas.
6. **All three changes are supported: introduce, evolve, collapse.** Going from one value to two, changing a spec, and going back to one (for example, expiring all old versions) must each be an ordinary operation.

There is one place where versioning and partitioning differ, and it matters:

- **Semantic dimensions** change what the data is: version, snapshot, ref, annotation set. They are part of identity and the content hash, they are pinned and cited, and time travel follows them.
- **Physical dimensions** change only how the data is stored or found: partition, representation, replica, manifest layout. They never enter identity or the content hash. The planner chooses them freely, and provenance records them only as what a job actually read.

Partitioning should therefore be fully transparent: a user should never need to know it exists, even after it evolves. Versioning should be transparent only until it matters. After that, records must always state which version was used.

As a constitution principle, it could read:

> **Optional structure has explicit defaults.** Versions, refs, partitions, representations, replicas, zones and schemas each have a real default value, so that data with no such structure needs no extra work, and the structure can be added later without migration. Requests can omit these dimensions. Records that must be reproducible state them explicitly. Physical dimensions never affect identity or content hashes.

I haven't added it to the README's principles, which are the constitution draft, or committed anything.

## 44. that's a nice add to the constitution, you could add that

**Question**

> that's a nice add to the constitution, you could add that

**Answer**

I added it to the design principles in `README.md`, which are the draft constitution. It goes right after "Immutable things, mutable pointers", since the two belong together, and I pushed it to `claude/cool-keller-domhwr`:

> **Optional structure has explicit defaults.** Versions, refs, partitions, representations, replicas, zones and schemas each have a real default value. Data with no such structure needs no extra work, and the structure can be added later without migration. Requests can omit these dimensions. Records that must be reproducible state them explicitly. Physical dimensions never affect identity or content hashes.

It passes the STE check. I committed only `README.md`; nothing from the schema or partitioning discussion is in the repo yet.

## 45. in terms of partitioning, you know Iceberg has a pretty good set of partition specs and mo

**Question**

> in terms of partitioning, you know Iceberg has a pretty good set of partition specs and more recently added geospatial. In terms of massive scientific datasets, that's probably good enough I'd guess. Are there any reasons why you might really push beyond that? I could imagine custom functions (WASM) to possibly help for some things, mostly you just wan to truncate something or bucket it for the most part

**Answer**

I agree: Iceberg's set is close to sufficient, and I'd resist WASM partition transforms. The reasons to go further are real but narrow, and most of them are better handled elsewhere in the design.

**What Iceberg has.** Identity, `bucket[N]`, `truncate[W]`, `year`, `month`, `day` and `hour`, plus `void`. Its format version 3 added geometry and geography types with bounding-box statistics. From memory, I'm not sure spatial partition transforms themselves were adopted, or only proposed; one such proposal was an XZ2 spatial curve. Version 3 also left room for transforms that take several source columns. Check both against the current spec.

**Where you might want more, and the better answer in each case:**

| Need | Example | Better answer |
|---|---|---|
| Spatial cells | Sky regions, earth tiles, coordinate boxes | A cell-ID function, then `truncate`. HEALPix, S2, H3 and Hilbert or Z-order curves all produce hierarchical integer IDs, so truncating to a level is the partition. The core can include one generic transform (a space-filling curve over N numeric fields). The field-specific curves (HEALPix, S2, H3) belong in field profiles |
| Several source fields | (site, day), (run, detector) | Multi-field `identity` or `bucket`, if Iceberg version 3 gives it a spec. Otherwise, nested partition levels |
| A lookup in reference data | Run → run period; compound → scaffold | An **augmenter** writes the derived value as an attribute, then the spec uses `identity` on it. A transform that reads mutable reference data isn't deterministic |
| Learned groupings | Embedding cluster, scenario class | **Statistics**, not partitions: centroids in manifest summaries. A partition key that depends on a model forces repartitioning every time the model changes |
| Time windows over streams and arrays | Hour-long clips, tiles of a grid | `hour`/`day`, or `truncate` on coordinates. Already covered |

**Why not WASM transforms:**

- **No pruning without projections.** A transform is useful for pruning only if the planner can turn a query predicate into a partition predicate. That needs inclusive and strict projections, and an arbitrary function can't provide them. A custom transform would have to declare them too, or declare itself monotonic, and those declarations would then be trusted code.
- **Determinism forever.** Partition values must come out the same years later. WASM is mostly deterministic, but floating-point and NaN edge cases, runtime versions and library drift are real risks, and each transform version would have to be kept for life.
- **Interoperability.** Iceberg readers, and the Iceberg gateway, can't evaluate mudcat's custom functions, so those partitions would be opaque outside mudcat.
- **Cost at scale.** Running a sandbox call per element at 10^11 elements, at sealing and at planning, is not free.

**The design rule I'd take from this:** keep partition transforms few, built in, deterministic and projectable. Push domain logic into augmenters that write derived attributes, and partition on those attributes with `identity`, `truncate` or `bucket`. WASM still fits mudcat, but as a sandbox for augmenters, where any determinism is checked once at write time and the output is stored.

The stakes are also lower in mudcat than in Iceberg. Partitioning only groups catalog metadata, and statistics (bounds, filters, centroids) recover much of the pruning a weak spec misses. A poor partition choice costs some efficiency; it can't cost correctness, and repartitioning moves no data.

I haven't changed the documents. If you want this recorded, it's a short paragraph in the model, plus the spatial-cell transforms as entries in `fields.md`.

## 46. Summarize our line of thinking here

**Question**

> Summarize our line of thinking here

**Answer**

The thinking came in three phases: research, the mudcat design, and the recent thread on schema and partitioning.

#### 1. Research: why build something new

- **Datacat had the right core and the wrong implementation.** The core was a logical namespace, datasets with versions and per-site replicas, typed metadata, views, a readable query language, and group ACLs inherited down folders. The implementation used attribute-value tables, a temporary table per search, and a cache inside each server.
- **The other systems each cover half the problem.**
  - iRODS and Rucio know where bytes are, but describe them poorly. Every large Rucio user pairs it with a second metadata catalog.
  - DataHub describes data well, but has no replicas and a heavy stack of services.
  - Amundsen is archived.
  - The Rubin Butler's own design notes admit it doesn't know the system is distributed.
- **The open niche** is one store that links replicas, datasets, models and evaluations, with integrity, licence and takedown status following the lineage. Multimodal AI data is the anchor, for AI-for-science startups and mid-size multimodal labs first.

#### 2. The mudcat design (pushed to `brianv0/mudcat`)

- **Model.**
  - Expressed in PROV-O, DCAT, CSVW and ODRL.
  - Every identifier is a UUIDv7.
  - Not everything is a table: each version has a structure descriptor, and selectors address parts of files, arrays and streams.
  - Metadata is held per namespace, with tags, augmenters and statistics.
  - Snapshots are immutable, with branches and tags, Iceberg-style commits and change summaries.
  - Usage rights attach to sources.
  - "Delete" is split into five operations, from removal from a dataset to erasure.
  - Time travel shows history but applies today's policy.
- **Access and policy.**
  - Access control uses Zanzibar-style relationships behind a pluggable port (built-in, SpiceDB or OpenFGA).
  - ODRL holds only the terms as written.
  - Grants sit on containers by default and on single objects only as exceptions.
- **Architecture.**
  - One PostgreSQL database per zone, with derived indexes, a transactional outbox, and site agents that work next to the data.
  - The catalog stays out of the read path.
  - There is a decision framework for when to federate, delegate or replicate, including shared custody among sovereign institutions.
  - Hosted operation splits a vendor control plane from the customer's data plane, with customer-held metadata.
  - Statistics-driven planning and Ray connectors.
- **Process.**
  - Written in Simplified Technical English, with NASA-style requirements.
  - Field-specific material is moved to `fields.md`.
  - The README principles are the draft constitution for spec-driven development.

#### 3. Recent thread: schema, manifest lists, partitioning (not committed)

1. **Schema sketch.** About 48 tables in 8 schemas, split into a rich tier and a cheap tier. Elements stay in manifests, outside PostgreSQL. It loads cleanly in a test instance, and lives only in the scratchpad.
2. **No manifest list.** The join table from snapshots to manifests was standing in for Iceberg's manifest list, and it would multiply rows: 10^9 rows for 10^4 manifests across 10^5 snapshots. The fix is immutable, shared manifest lists that change only at sealing and are written as files at publication.
3. **Partitioning is the right shape for that tree.** If list levels are partition values, the tree also prunes queries, localises changes, and lines up with policy (by source), custody (by ACL root) and residency.
4. **Partition evolution, as in Iceberg.**
   - Needs: a spec registry, stable partition field IDs, transforms with fixed definitions plus inclusive and strict projections, a spec ID on every manifest, planning across specs, and partition values computed at registration so writers never see them.
   - Easier in mudcat, because partitioning only groups metadata and repartitioning never moves data.
   - **Correction:** the content hash must not depend on manifest layout, so it is separate from the manifest-list digest.
5. **Versioning and partitioning share a pattern.** Both should be invisible until needed and easy to add later. This became the constitution principle "Optional structure has explicit defaults", the only item from this thread that is committed. A further distinction: semantic dimensions (version, ref) are pinned and recorded, while physical dimensions (partition, representation, replica) never touch identity.
6. **Transforms beyond Iceberg.** Iceberg's set is nearly enough. Spatial cells reduce to "compute a cell ID, then truncate". Lookup-based or learned keys belong in augmenters or statistics. WASM belongs in augmenters, not partition transforms, because a partition transform must be deterministic for good, give projections for pruning, and stay readable by Iceberg tools.

#### Open, not yet in the documents

- **Model and architecture:** manifest lists and partition specs; partition evolution; the content-hash correction; the transform policy.
- **Schema:** the schema sketch itself.
- **Iceberg spec:** check two points against the current specification: what version 3 added for geospatial partitioning, and whether it supports transforms with several source columns.

## 47. would manifest lists actually go in? it seems like may not necessarily need to go? the par

**Question**

> would manifest lists actually go in? it seems like may not necessarily need to go? the partition specs for sure. Maybe the manifest list needs to go, but maybe there's a better name?

**Answer**

Partition specs should definitely go in. The manifest list I'd drop as a separate concept and name, and keep only what it was doing, under a better name.

**What we actually need.** Each snapshot needs a cheap way to say which manifests hold its members, grouped by partition, while sharing everything that didn't change. Iceberg needs a manifest-list file because it has no database and every commit must produce files. mudcat has neither constraint, so most of the reasons for the list disappear:

| Reason Iceberg has a manifest list | Still applies to mudcat? |
|---|---|
| A snapshot must be a file | No. The database holds snapshots |
| Per-manifest summaries for pruning | Partly. Summaries per partition and per manifest can be database rows |
| A file per commit | No. Commits are row writes, and only sealing touches manifests |
| Portability and verification without the catalog | Only for export and federation, and the content hash is now independent of layout. Export can generate files on demand; the Iceberg gateway does this anyway |
| Bounding the size of each node | Partitioning already does this. The spec decides how much each partition holds, so nested lists aren't needed |

**What remains is a flat map**, from (spec, partition value) to a set of manifests, with a summary per partition:

- **Shared, not copied.** A partition entry is immutable and shared. A seal creates new entries only for the partitions it touched, and a snapshot that doesn't seal reuses its parent's map.
- **Cost per seal.** Each seal writes pointer rows proportional to the number of partitions, not the number of manifests. Iceberg rewrites its whole manifest list on every commit, so this is no worse, and seals are batched and infrequent.
- **No nesting.** If one partition grows too large, the remedy is to evolve the spec (bucket more finely), not to add tree levels.

**Naming.** It's purely physical, so the name should say that:

| Name | For | Against |
|---|---|---|
| **Layout** | Says it's physical; fits the "physical dimensions never touch identity" rule; neutral about the mechanism | Slightly generic |
| Partition map | Precise about what it maps | Odd for an unpartitioned dataset, though spec 0 with one partition is still a map |
| Member index | Says what it's for | "Index" is already used for derived search and identity indexes |
| Tree (as in git) | Familiar for structural sharing | Suggests nesting we no longer need |
| Manifest list | Iceberg users recognise it | Suggests a file and a commit-time object, which ours isn't |

I'd choose **layout**: "a snapshot has a content hash, its identity, and a layout, how its members are stored". Its parts would be **layout entries**, one per partition. Schema-wise:

```sql
CREATE TABLE ds.layout_entry (            -- immutable; shared across snapshots
  id uuid PRIMARY KEY DEFAULT uuidv7(),
  dataset_id uuid NOT NULL, spec_id int NOT NULL, partition jsonb NOT NULL,
  manifest_ids uuid[] NOT NULL,
  bounds jsonb, source_bitmap bytea, fingerprint_filter bytea   -- per-partition summary
);
CREATE TABLE ds.snapshot_layout (         -- one pointer row per partition, written at sealing only
  snapshot_id uuid NOT NULL, entry_id uuid NOT NULL REFERENCES ds.layout_entry,
  PRIMARY KEY (snapshot_id, entry_id)
);
-- unsealed changes stay in ds.member_delta; ds.snapshot_manifest goes away
```

If even the pointer rows become a measured problem, a snapshot can point to its sealed base's layout and keep only its own changes. That's an optimisation to add later, not part of the starting design.

Nothing is committed. To record this, the model would get partition specs and layouts, plus the content-hash correction, and the architecture's commit-cost and maintenance sections would use "layout" where they now say "list of sealed manifests".

## 48. ah so it's the manifests we technically need, but we don't have that?

**Question**

> ah so it's the manifests we technically need, but we don't have that?

**Answer**

We do have manifests, but the design uses the word for two different things. That's probably why the layout question felt slippery.

**Two kinds of "manifest" in the documents now:**

1. **Element manifests**, defined in model section 2 under "Elements and selectors" (`mc:Manifest`). A columnar file listing the elements inside one or more versions: records, tar members, clips. Each row has an element ID, source, selector, fingerprints and lineage fields. These exist only where the data enumerates its elements, they hold the ~10^11 rows that never go into PostgreSQL, and they're registered as ordinary versions. Iceberg has nothing like them; the closest relatives are a Lance or WebDataset index, or a Croissant record set.
2. **Member manifests**, which arrived later with the Iceberg ideas (structural sharing, commit cost, maintenance). A file listing the members of a snapshot, meaning versions and annotation sets, with summaries for each. This is the direct equivalent of an Iceberg manifest: Iceberg lists data files, mudcat lists versions, which are its "data files". The compactor writes these when it seals `member_delta` rows. Model section 4 says "a snapshot refers to its members through manifests" without saying it means this second kind.

So the full chain, by analogy with Iceberg:

| Iceberg | mudcat |
|---|---|
| Table metadata, snapshot | Snapshot (a database row with a content hash) |
| Manifest list | **Layout**: a map from partition to member manifests (database rows) |
| Manifest: data files with column stats | **Member manifest**: versions with per-version stats |
| Data file | Version (a file or object) |
| Rows inside the file (Parquet's own indexes) | **Element manifest**: elements inside versions, for any structure |

**Do we need member manifests as files?** Mostly yes. A large dataset's snapshot can have 10^8 or more members. Keeping membership as rows for every snapshot is the multiplication problem again, and the database would grow with every snapshot. Sealing members into shared, immutable files keeps the database small, keeps commits as row writes, and gives the planner per-manifest summaries to prune on. For small datasets, the unsealed `member_delta` rows may be all there ever is, which fits the explicit-defaults principle.

**Proposed naming:**
- **Manifest** means only the member kind, the Iceberg sense, so anyone who knows Iceberg reads it correctly.
- **Element list** replaces the element kind. The architecture document already says "element lists" in places, after the STE pass.
- **Layout** is the per-snapshot map from partition to manifests.

The chain then reads cleanly: a snapshot has a layout, the layout points to manifests, a manifest lists versions with statistics, and a version may have an element list. Statistics roll upward: element lists give per-version statistics, those become manifest summaries, and those become partition summaries in the layout.

Nothing is committed. In the documents, this means:
- renaming `mc:Manifest` in model section 2 to an element-list class;
- defining member manifests, layouts and partition specs in section 4;
- aligning the architecture and the schema sketch with those terms.

## 49. can you describe the model a bit more clearly to me?

**Question**

> can you describe the model a bit more clearly to me?

**Answer**

Here's the model as it stands now, including the layout and naming changes we just discussed (those aren't in the documents yet). It helps to read it as layers, each building on the one below.

#### 1. Identity

Every entity has an immutable UUIDv7 minted by its zone. Names, paths, DOIs and external IDs are aliases that point at an ID, so renaming never breaks anything.

#### 2. Files and copies: the physical core (from Datacat and iRODS)

```
Collection (folder) ─contains─▶ Data object ─has─▶ Version ─has─▶ Replica ─on─▶ Storage resource
```

- **Collection:** a folder in the logical namespace. Access control and inherited defaults usually sit here.
- **Data object:** one logical file or object, however many revisions it has.
- **Version:** one immutable revision, identified by its content checksum. Writing again makes a new version.
- **Replica:** one physical copy of a version, on one storage resource, with its own state (registered → verified → suspect → lost or retired).
- **Storage resource:** a bucket, prefix, file system or tape system, with region, jurisdiction and cost.

#### 3. What is inside a file

- **Structure descriptor:** each version has one, in one of seven kinds: opaque, container, records, array, stream, graph, composite.
- **Fields:** the common unit of description, covering table columns, array variables, stream channels and metadata attributes. Records are described with CSVW tables.
- **Selectors:** address part of a version (a byte range, member path, row range, array window or time span) without listing anything.
- **Element list:** for data that does enumerate its parts (rows, tar members, clips), a columnar file listing them. This is the only place the ~10^11 elements live.

#### 4. Describing things: metadata

- **Metadata:** one document per (entity, namespace). Namespaces have owners, and each value records who asserted it.
- **Tags:** owned classifications with a closed list of values. They inherit down collections, can follow lineage, and policies can attach to them.
- **Augmenters:** versioned plugins that compute metadata into their own namespaces. Format readers are one kind; others produce classifiers, embeddings, quality scores and AI-written descriptions.
- **Statistics and hints:** statistics are computed, cautious summaries used to make execution cheaper. Hints are set by people: access pattern, required region, hardware.

#### 5. Datasets: the versioned unit people work with

```
Dataset ─has─▶ Snapshot ─contains─▶ versions + annotation sets
              ▲   │
       Ref ───┘   ├─ content hash  (what it contains: identity)
(branch/tag)      └─ layout ─▶ manifests ─▶ versions   (how it is stored: physical)
```

- **Dataset:** a named, versioned set of data objects (a Datacat group, a Rucio dataset).
- **Snapshot:** an immutable state of a dataset. Every change makes a new one, with a parent and a change summary. Its **content hash** covers only its members, so it proves what the snapshot contains.
- **Refs:** branches move as writers commit; tags are fixed. Publishing means tagging a snapshot.
- **Annotation sets:** derived columns added on top (captions, scores, labels), without rewriting the data.
- **Physical side, invisible to users:**
  - a **partition spec** (Iceberg-style, able to change over time) groups members;
  - a **layout** maps each partition to **manifests**;
  - a manifest lists versions with their statistics.

  Changing any of this never changes the content hash.
- **Representation:** one physical form of a snapshot (native files, Parquet shards and so on). Replicas are byte-identical only within one representation.
- **Selection:** a no-copy subset of a snapshot, defined by a predicate.

#### 6. Where data came from and what it may be used for

- **Source:** where data came from, such as a crawl, a licensed delivery, a human-data campaign, model-generated output, an instrument run, or a simulation. Usage rights attach here, not to folders.
- **Usage rules:** grants (an action and a purpose), constraints (expiry, jurisdiction) and duties (attribution, deletion). Duties follow lineage, all the way into models. The original licence or contract text is kept with the source, ideally in ODRL.
- **Access control:** a separate question: who may read or change things. It is modelled as Zanzibar-style relationships, usually granted on collections and datasets.
- **Removal:** five levels.
  - **Forward:** remove from a dataset; delete marker.
  - **Retroactive:** invalidate; purge the bytes; erase the record.
  - **Legal holds** block purge and erasure.

#### 7. What happened: provenance (W3C PROV)

**Activities** (ingest, transform, training, evaluation, experiment) **used** entities in a role (training, test, calibration and so on) and **generated** new ones. Shared inputs such as calibrations or tokenizers are marked ubiquitous, so ancestry queries skip them by default.

#### 8. AI on top

```
Specification ─pinned as─▶ Pinned specification ─materialised as─▶ Representation
                                    │ used (training)
                               Training run ─generated─▶ Model ─used by─▶ Evaluation ─▶ Result
```

- **Specification:** a declarative training set: components, filters, weights, sample rule, seed.
- **Pinning:** freezes it to exact snapshots and enforces usage policy, recording the decision.
- **Similarity criteria:** let mudcat check that test data doesn't leak into training data, whether by matching hashes or by scientific similarity.

#### 9. Keeping copies where they're needed

- **Replication rules:** state how many copies of a snapshot or representation must exist, and on which storage.
- **Completeness records:** state whether a site holds a complete, verified copy.

#### Rules that cut across every layer

- **Immutability:** things are immutable; only pointers change (refs, aliases, ACLs, status).
- **Explicit defaults:** every optional dimension has a real default (one version, `main`, one partition, one replica), so simple data stays simple.
- **Semantic and physical dimensions:**
  - **semantic** ones are pinned and recorded: version, snapshot, ref, annotation set;
  - **physical** ones are chosen freely and never affect identity: partition, layout, manifest, representation, replica.

#### A short example

A robotics team registers MP4 videos and a Parquet file of actions.

1. Each file becomes a data object with a version, and one replica in their bucket.
2. The Parquet file gets a records descriptor and an element list of episodes. Each MP4 gets a stream descriptor.
3. They form a dataset. The first snapshot goes on `main` and is tagged `v1`.
4. An augmenter adds captions as an annotation set, which makes snapshot 2.
5. A specification selects "episodes longer than 10 seconds, licence permits training". Pinning freezes snapshot 2 and records the policy decision.
6. Ray reads the clips directly from the bucket using the plan.
7. The resulting model is linked to that pinned specification. If a source is later taken down, mudcat knows which model used it.

## 50. do you think we could or shojld implement the iceberg rest api?

**Question**

> do you think we could or shojld
> implement the iceberg rest api?

**Answer**

We could, and part of it is worth doing. Implementing the full, writable Iceberg REST catalog is the part I'd avoid, at least at first.

#### Four levels of "implementing Iceberg REST"

| Level | What it means | Effort | Value |
|---|---|---|---|
| **A. Register external Iceberg tables** | Tables stay in Polaris, Lakekeeper, Glue, Unity or Snowflake. mudcat records each table snapshot as a version or representation, with lineage, sources and policy. It serves no Iceberg API | Low | High: follows "register, don't replace" and the delegation table |
| **B. Read-only projection** | mudcat serves the read side of the REST API (list namespaces and tables, load table, credential vending, possibly scan planning). It generates Iceberg metadata for its own Parquet data: element lists, manifests, Parquet record datasets | Medium | High: Spark, Trino, DuckDB and PyIceberg can query mudcat's elements and statistics by name |
| **C. Policy-enforcing proxy** | mudcat sits in front of an external Iceberg catalog. It passes requests through, but makes access decisions, issues credentials under mudcat policy, and records reads as provenance | Medium–high | High for governed table data, but every spec change touches the proxy |
| **D. Full writable catalog** | mudcat becomes the authoritative Iceberg catalog: commits, views, multi-table transactions, remote signing and maintenance | High, and ongoing | Only if being *the* table catalog is a goal |

#### Why "could" is yes

The design already mirrors the hard parts:

- commits made of requirements plus updates;
- server-side resolution plans, much like Iceberg's scan planning;
- credential vending;
- refs (branches and tags), and snapshots with parents;
- stable field IDs and partition specs that can change over time.

#### Why "should" is mostly no for level D

- **Two snapshot systems.** A writable Iceberg catalog stores Iceberg metadata files and snapshots alongside mudcat's own. Every table commit would have to map, one to one and in the same transaction, onto a mudcat snapshot. It's doable, but there would be two sources of truth for the same history.
- **Only some data can be Iceberg data.** Iceberg data files must be Parquet, ORC or Avro. Video, HDF5, Zarr, Lance and tar shards can't be exposed as Iceberg tables. Only mudcat's tabular parts can: element lists, manifests, statistics and Parquet records. That covers much of the useful metadata, but not the data itself.
- **A moving, crowded target.** The REST spec keeps growing: version 3 types, views, scan planning, remote signing, events. Polaris, Lakekeeper, Gravitino and Unity already compete for exactly this role. Matching them is a product of its own.
- **Maintenance becomes ours.** A writable catalog has to compact, expire snapshots and remove orphan files for the tables it owns. That's work we deliberately delegated to table engines.

#### What I'd do

1. **Level A first.** Register external Iceberg tables: track their snapshots, attach sources and duties, and link them into lineage. This is cheap, and it reflects how labs already work.
2. **Level B next, read-only,** limited to mudcat's own Parquet artefacts: element lists, manifests and statistics. This is the valuable part. It lets "query 10^11 elements with Spark or DuckDB" work through standard engines, with mudcat issuing scoped credentials.
3. **Level C only if users ask for mudcat's policy on table reads.** Examples are takedowns that must cover tabular data, or residency rules on table access.
4. **Level D only as an explicit product decision.** If chosen, build on an existing implementation, such as embedding Lakekeeper or Polaris behind mudcat's authorization port, rather than writing one from scratch. Run the Iceberg project's REST specification test suites against it.

#### Change to the current design

The architecture lists an "Iceberg REST catalog for records-structured datasets" as a gateway, and puts it in build step 6. I'd reword that as two items:

- registration of tables held in external Iceberg catalogs, in build step 3;
- a read-only Iceberg REST gateway over mudcat's own Parquet artefacts, in build step 6.

A full writable catalog would then be stated as a non-goal unless a product decision changes that. I haven't changed the documents.

## 51. Writable catalog with basic features would be good though?

**Question**

> Writable catalog with basic features would be good though?

**Answer**

Yes. A writable catalog with a deliberately small feature set is reasonable, and it fits the design better than I suggested last time, provided an Iceberg table maps onto mudcat's model in a single way. Here's what "basic" would mean, and how it stays a single source of truth.

#### Basic scope

| In | Out at first |
|---|---|
| `GET /v1/config`; namespace create, list, load, drop (namespaces are collections) | Views |
| Table create, load, rename, drop (drop is a soft delete, with purge as a separate step) | Server-side scan planning |
| Commits with requirements and updates:<br>• **requirements:** `assert-table-uuid`, `assert-ref-snapshot-id`, `assert-current-schema-id`, `assert-default-spec-id`<br>• **updates:** `add-snapshot`, `set-snapshot-ref`, `remove-snapshots`, `add-schema`, `set-current-schema`, `add-spec`, `set-default-spec`, `set-properties` | Remote signing, the events endpoint |
| Vended credentials for reads and writes, scoped to the table location | Staged create (atomic create-table-as-select), to add when Spark users need it |
| Bearer tokens from the customer's identity provider | Iceberg's OAuth token endpoint |
| Format version 2 | Version 3 features, later |

Multi-table commits are cheap for mudcat, because they're one PostgreSQL transaction, so they could come soon after. These endpoint and update names are from my knowledge of the spec, so check them against the current version before building.

#### How a table fits the model

- **Table and dataset.** An Iceberg table is a mudcat dataset with records structure and an Iceberg representation. Namespaces are collections, so ACLs, tags and usage policy apply as usual.
- **Snapshots.** Each Iceberg snapshot is a mudcat snapshot with one member: the table's metadata at that snapshot. mudcat does **not** register each data file as a version. Iceberg's manifests already list the files, and duplicating them would create two indexes to keep in step. An augmenter can index files later, for statistics, lineage or takedowns.
- **One set of refs and specs.** Iceberg branches and tags *are* mudcat refs, stored once, with Iceberg's `main` being mudcat's `main`. The table's partition specs and schemas *are* the dataset's specs and fields. There is no second copy that can drift.
- **Semantic versus physical commits.** Iceberg's `replace` operations (compaction, rewrites) change files but not content. mudcat records them as physical-only snapshots, which keeps the semantic/physical distinction from the constitution.
- **Storage.** Table state is kept in PostgreSQL, as Polaris and Lakekeeper do. mudcat still writes each new `metadata.json` to the table location, so tools that read files directly keep working.

#### The commit path

1. **The client writes its files.** The engine writes data files, manifests and the manifest list to storage, using credentials mudcat issued.
2. **The client sends the commit.** It posts its requirements and updates.
3. **mudcat writes the metadata file.** It writes a new `metadata.json` under a unique name. If anything later fails, the file is an orphan that maintenance removes.
4. **mudcat applies the commit.** In one transaction it checks the requirements, moves the ref and records the snapshot, the activity and the audit entry. A failed requirement returns `409 Conflict` and the client retries, as Iceberg clients already expect.

#### What mudcat adds over a plain Iceberg catalog

- **Policy at load.** Usage policy and the invalidation set are checked before credentials are issued.
- **Provenance.** Lineage comes from issued credentials and from OpenLineage events emitted by Spark.
- **Tracked takedowns.** A takedown on table data becomes a tracked task. mudcat asks an engine job to delete the rows, and records the resulting commit as the receipt.
- **Scheduled maintenance.** Snapshot expiry and orphan removal run as scheduled engine jobs, through the same runtime used for external-compute augmenters. mudcat never rewrites table files itself.

#### Risks and how to keep them bounded

- **Spec drift.** Implement against Apache iceberg-rust's metadata types if the server is in Rust, so they aren't written by hand. Test with Spark, Trino, PyIceberg and DuckDB, and against the Iceberg project's REST test fixtures, before claiming compatibility. I haven't checked those fixtures' current form.
- **Scope creep.** Write the out-of-scope list into the requirements, so "basic" stays basic.
- **Only tabular data.** Only Parquet, ORC and Avro data qualifies. Video, HDF5 and Zarr data stays in mudcat's own model, so the catalog has two kinds of dataset behind one API.

#### Changes to the documents

None are made yet. They would be:

- **Architecture:**
  - **Gateway entry:** a writable Iceberg REST catalog with this scope, keeping the read-only projection of mudcat's own Parquet artefacts.
  - **Build order:** registering external Iceberg catalogs moves to step 3; the writable catalog goes in step 5 or 6.
  - **Out-of-scope list:** added, as above.
- **Model:** a section on Iceberg-backed datasets.
- **Requirements:** about five new ones, covering commit semantics, refs shared with Iceberg, no registration of each data file, policy checked at table load, and the out-of-scope list.

## 52. what do you think about this project now? how would you describe it?

**Question**

> what do you think about this project now? how would you describe it?

**Answer**

#### How I'd describe it

**One line:** mudcat is a catalog for multimodal and scientific data that tracks what each piece of data is, where every copy lives, where it came from, what it may be used for, and which models were built from it, without ever holding the data itself.

**One paragraph:** mudcat descends from SLAC's Datacat. It keeps Datacat's core idea, logical datasets with versions and replicas at many sites, and extends it for AI. Data of any shape (files, tables, arrays, video, sensor logs) stays where it is, in buckets, HPC file systems or enclaves. Above that, mudcat keeps one transactional record of identity, immutable snapshots, provenance, licences and consent, and lineage from data to model to evaluation. Engines such as Ray, Spark and PyTorch read the data directly, using plans and credentials that mudcat issues. Policy is enforced when a training set is pinned, and takedowns and licence expiries spread through everything derived from the data.

#### What I think of it

**What's strong:**
- **A real gap.** The research held up. Science groups run two catalogs synchronised over a message bus, AI labs are hiring to build exactly this in-house, and nothing open combines replicas, typed metadata, lineage and usage policy in one store.
- **One central bet, consistently applied.** Identity, metadata, replica state and lineage live in one transactional store. Most later decisions follow from that, rather than being bolted on: pinning as the policy gate, the invalidation set, completeness per site, and observed lineage.
- **Lessons from other systems are taken seriously.** The design takes:
  - from Iceberg: commits that state their expected starting point, partition evolution, and branches and tags as refs;
  - from Rucio: replication rules;
  - from iRODS: per-replica state;
  - from the Butler: provenance kept in files;
  - from DataHub: that derived indexes must be rebuildable.

  Several of those systems' known failures are designed out explicitly.
- **Good discipline.** The documents have simplified technical English, NASA-style requirements, a constitution draft, and a clear line between semantic dimensions (version, snapshot) and physical ones (partition, replica). That last distinction is the most useful idea we arrived at; it resolved several later questions cleanly.

**What worries me:**
- **Breadth versus depth.** The design now covers catalog, provenance, policy, authorization, placement, federation, a hosted service, augmenters, statistics-driven planning and an Iceberg catalog. Each is a product somewhere else. Nothing is built, and the riskiest assumptions are untested:
  - whether PostgreSQL holds 10^9 versions and their snapshot membership;
  - whether the content-identity index works at 10^11 elements;
  - whether pipelines will actually carry element and source identifiers.
- **No anchor user yet.** The research says the systems that survived each grew around one committed experiment, and Amundsen died without a sponsor. The design is aimed at AI-for-science startups and multimodal labs, but no specific team has been named.
- **Uneven evidence.** Much of the external research came from search extracts, and some current facts are unverified, such as Iceberg version 3 details and the managed-service names. That's acceptable for a design, but not for specifications.
- **Drift from the documents.** Several decisions from the last few turns exist only in conversation: layouts, manifests versus element lists, partition specs, the content-hash correction, and the scope of a writable Iceberg catalog.

#### What I'd do next

1. **Record the uncommitted decisions,** so the documents match what we now think.
2. **Choose a minimal first release.** I'd take collections, data objects, versions, replicas, snapshots with refs, metadata documents, one partition spec, bulk registration, a Ray read connector, and registration of external Iceberg tables. Defer the hosted service, federation, placement and the writable catalog.
3. **Build the schema and benchmark the hard query early.** Measure snapshot membership (layout plus manifests plus unsealed rows) and resolution at 10^8 versions, before writing much else.
4. **Find an anchor user,** and let their real data and queries settle the open questions.
5. **Start the spec-driven process.** Turn the README principles into the constitution, and the first release into the first feature specification.

Overall, it's a coherent, well-motivated design aimed at a gap that is real. Its main risk is not a wrong idea but trying to build too much before the core bet is proven on real data.

## 53. > * whether PostgreSQL holds 10^9 versions and their snapshot membership; • whether the co

**Question**

> > * whether PostgreSQL holds 10^9 versions and their snapshot membership;
> 	•	whether the content-identity index works at 10^11 elements;
> 	•	whether pipelines will actually carry element and source identifiers.
>
> Surely Iceberg can cover some of that, and existing systems and their design and scale descriptions as well

**Answer**

You're right, and I overstated those three risks. Existing systems already show most of the scale. What's left is narrower: the parts of mudcat that are new combinations. Taking them one at a time, with the evidence from our research notes:

#### 1. PostgreSQL with 10^9 versions and snapshot membership

**Already demonstrated:**
- **Rubin's Butler** runs on PostgreSQL with about 1.5 billion datastore records in one repository, and about 250 million datasets in an earlier data preview. So 10^9 rows in one PostgreSQL database is proven, not hypothetical.
- **ATLAS Rucio** holds about a billion files, though on Oracle.
- **pgstac** reports hundreds of millions of items, and **DUNE MetaCat** targets 10^8 files and more.
- **Iceberg** shows that snapshot membership needn't be database rows at all: manifests plus a layout keep it in files, with pruning, for tables of millions of files. mudcat adopted exactly that structure.

**What's actually left:**
- **Concurrent writes** at that scale. The Butler measured inserts taking up to ten minutes; our batching ingest writer copies its fix.
- **Bulk deletes.** Emptying the Butler's trash took over an hour per chunk at 1.5 billion records, until the query was rewritten.
- **Online schema migrations.** One of the Butler's was pending for nearly a year.
- **Our hybrid of sealed manifests plus unsealed delta rows.** This is the one genuinely new piece. Neither Iceberg nor the Butler does it.

So the benchmark is now specific: resolution and commit latency for that hybrid, and throughput for sealing and deletion. Raw row count is no longer the question.

#### 2. Content-identity index at 10^11 elements

**Already demonstrated:**
- **Exact-hash lookup at this scale is a solved storage pattern.** A table keyed on the fingerprint, bucketed and sorted by it, with Parquet Bloom filters, handles it. The identity index can simply *be* an Iceberg or Lance table, built and maintained by Spark or Ray. LanceDB claims tables of about 100 billion rows. Hugging Face's Xet runs content-addressed deduplication over well over ten petabytes.
- **Near-duplicate detection at web scale runs routinely as a batch job.** Every large text-corpus pipeline runs MinHash LSH deduplication.

**What's left:**
- **Interactive near-duplicate lookup,** meaning "is this item in any training set?" answered in seconds rather than by a batch job.
- **Cross-modal similarity.**

Exact matching is low risk. Interactive near-duplicate lookup is moderate. Cross-modal similarity is research.

#### 3. Whether pipelines carry element and source IDs

This is an adoption problem, not a scale problem, and it's partly solved too:

- **Iceberg version 3 adds row lineage**: a stable row ID and the sequence number of the row's last update, preserved through rewrites. Tabular pipelines through version 3 tables get element identity for free. I'm stating this from memory and haven't checked the spec text.
- **Other formats already carry keys.** WebDataset has `__key__`, LeRobot's episode index is another example, and NVIDIA's Cosmos curator has identity utilities. The Butler carries dataset UUIDs through its pipeline graphs.
- **OpenLineage is already emitted by Spark and Airflow** for lineage at the dataset level.

**What's left:** non-tabular transforms in custom code, such as a Python script that crops videos. Making mudcat's Ray and data-loader connectors carry IDs and source references by default, instead of on request, covers the common cases.

#### Revised risk list

| Risk | Before | Now |
|---|---|---|
| PostgreSQL scale | Unproven | Proven by the Butler; benchmark only our sealed-plus-delta membership hybrid, sealing and deletion |
| Exact identity index | Unproven | Proven pattern; build it as an Iceberg or Lance table |
| Near-duplicate identity, interactive | Unproven | Moderate; batch versions exist |
| Pipelines carrying IDs | Unproven | Partly solved by Iceberg version 3 row lineage and format keys; connectors should carry IDs by default |

What I'd now call the main risks are the new combinations: the hybrid membership model, policy evaluation per manifest, and lineage stitched across tools. The base capabilities are proven elsewhere. The documents' risk sections and rationales should cite this evidence, Butler numbers included; I haven't updated them yet.

## 54. let's go back to the defintion of a dataset, what is it now

**Question**

> let's go back to the defintion of a dataset, what is it now

**Answer**

The documents currently say this (model, section 4):

> **Dataset**: a named, versioned set of data objects that mudcat treats as a unit (Datacat group; Rucio dataset). A dataset that grows over time with appended data is also a `dcat:DatasetSeries`.
> **Snapshot**: an immutable version of a dataset. Members are versions and annotation sets.

That's close, but after everything we've changed, the definition has three inconsistencies, and the documents still leave a few things unsaid.

#### How the meaning moved

1. **Datacat:** a "dataset" was one file with versions and locations. We renamed that to **data object**, and Datacat's *group* became the dataset.
2. **The first multimodal report:** "a dataset is a table of samples whose cells are scalars or pointers into files". We dropped that when you pointed out that not all data is tabular. Samples are now defined by a training-set specification's sample rule, not by the dataset.
3. **Now:** a versioned set of members of any structure, with immutable snapshots, refs, and a physical side (partition spec, layout, manifests) that never affects identity.

#### Three inconsistencies in the documents

- **Data objects or versions?** The dataset is called a set of *data objects*, but snapshots contain *versions*. These differ. When a file gets a new version, is the dataset's content different, or only its next snapshot?
- **Can datasets contain datasets?** The model doesn't allow it, but the shared-custody section in the architecture describes a release snapshot whose members are other zones' snapshots. That's nesting, the equivalent of Rucio containers.
- **Iceberg-backed datasets.** In our last discussion, an Iceberg table became a dataset whose snapshot has one member, the table's metadata at that snapshot. The model doesn't cover that kind of member.

#### The definition I'd propose

> A **dataset** is a named, owned set of members that changes only through commits. Each commit makes an immutable **snapshot**. Refs (branches and tags) name snapshots, and the content hash of a snapshot is its identity.
>
> **Members** are:
> - **versions** of data objects of any structure;
> - **annotation sets**: derived attributes keyed to elements;
> - **snapshots of other datasets**, pinned by content hash, which makes a dataset of datasets;
> - for a table-backed dataset, **table snapshots** (for example, Iceberg metadata at one snapshot).
>
> A dataset's **physical form** (partition spec, layout, manifests, representations, replicas) can change without changing any snapshot's identity.

Four rules follow from it:

- **Membership is by version.** A new version of a file doesn't enter a dataset until someone commits it. A dataset can have a **follow rule**, such as "track the latest version of everything under `/raw/2026/**`", which makes those commits automatically. Pinned and published snapshots always name exact versions, which fits "floating in requests, explicit in records".
- **Nesting is by snapshot, so it can't loop.** A dataset can include another dataset only as a pinned snapshot. Snapshots are immutable, so cycles are impossible, and a release made from parts in several zones is ordinary nesting.
- **A single file needs no wrapper.** Under the explicit-defaults principle, a data object can be used anywhere a dataset can, as an implied one-member dataset with one snapshot per version. Simple cases stay simple.
- **A data object belongs to one collection but any number of datasets.** The collection governs custody; the datasets govern sharing.

#### What a dataset is not

| Not | Because | That is a |
|---|---|---|
| A folder | Folders own files and govern custody | **Collection** |
| A table | It can hold tables, but also video, arrays, archives and models | **Structure descriptor** of its members |
| A training set | Weights, sample rules and policy decisions are applied *over* datasets | **Specification** and **pinned specification** |
| A subset | A predicate over snapshots that copies nothing | **Selection** |
| A physical copy | Formats and sites are physical | **Representation** and **replica** |

This keeps "dataset" meaning what DCAT, Croissant and Hugging Face users expect: the thing you name, version, cite and train from. The new parts are that it isn't tied to one structure, and that its storage can change without changing its identity.

I haven't changed the model. These would go into section 4: the member types, the follow rule, nesting by snapshot, and the implied one-member dataset.

## 55. so, for a collection, a dataset in a collection, if it was an iceberg table, that dataset 

**Question**

> so, for a collection, a dataset in a collection, if it was an iceberg table, that dataset would correspond to potentially a very large piece of data. If it's actually backed by a file, it's just one file?

**Answer**

Not quite. A dataset is never "backed by one file". The asymmetry you're noticing is real, but it comes from who keeps the list of members, not from size.

**What sits in a collection.** A collection can hold three kinds of entry:

| Entry | What it is | Who keeps the list of what's inside | Size |
|---|---|---|---|
| **Data object** | One logical file or object, with versions and replicas (Datacat's old "dataset") | Nobody needs to; it's one thing | Anything from a 1 KB JSON file to a multi-petabyte Zarr store treated as one object |
| **Managed dataset** | A named set of members (versions, annotation sets, other snapshots) | **mudcat**, through snapshots, layout and manifests | One member to about 10^9 members |
| **Table-backed dataset** | A dataset whose content is an Iceberg, Delta or Lance table | **The table format.** mudcat tracks table snapshots and delegates the file list | One member per snapshot (the table snapshot), standing for possibly billions of rows |

So, in your terms:
- **A single file** is a **data object**, not a dataset. Under the explicit-defaults principle it can be used wherever a dataset is expected, as an implied one-member dataset, but it doesn't need to be wrapped in one.
- **An Iceberg table** is one dataset with one member per snapshot, and that member can stand for a vast amount of data.
- **A thousand files grouped together** is a managed dataset with a thousand members.

**Member count and data volume are independent.**

- A table-backed dataset with one member can hold 500 TB.
- A managed dataset with 10^8 members can hold small files.
- One data object can be a petabyte array store.

The difference between the kinds is only where membership is recorded. It answers "should mudcat know every file?":

- **Managed:** yes. mudcat tracks each version, its replicas and its checksums.
- **Table-backed:** no. The table already tracks its files. mudcat would only duplicate that, and keeping two lists in step is the failure we want to avoid. An augmenter can still index the table's files lazily, when mudcat needs them for statistics, lineage or a takedown.

**What keeps the kinds from leaking into user code.** They all resolve the same way. A specification can mix them, and when it's pinned and resolved:

- managed members expand through mudcat's own layout and manifests;
- table members expand through the table format's scan planning;
- data objects resolve to a version and its replicas.

The plan the engine receives looks the same in every case: tasks of files, byte ranges or rows, with credentials and statistics. Ray never needs to know which kind it's reading.

I'd make this explicit in the model as a **membership mode** on each dataset: managed, table-backed, or (later) federated, meaning tables in an external database that mudcat only describes. That's the same three-way split I suggested early on, for cloud and data-lake use. It belongs next to the dataset definition we just tightened. I haven't changed the documents yet.

## 56. so a dataset is what Datacat may have called a dataset group.

**Question**

> so a dataset is what Datacat may have called a dataset group.

**Answer**

Yes, broadly. mudcat's **dataset** is the descendant of Datacat's **group**, the "macro-dataset" container that could hold only datasets, while Datacat's **dataset** became mudcat's **data object**. Four things differ:

| | Datacat group | mudcat dataset |
|---|---|---|
| Relation to files | **Containment.** A Datacat dataset lived in exactly one group or folder, so a group was a place in the path hierarchy | **Membership.** A data object lives in one collection but can belong to many datasets; the dataset has a name in a collection, but its members don't live inside it |
| Versioning | No history; the group reflected its current contents | Immutable snapshots, refs (branches and tags) and a content hash |
| What members are | Datacat datasets, resolved to their current versions through views | Exact versions, plus annotation sets, other datasets' snapshots, or table snapshots |
| Physical side | None | Partition spec, layout, manifests, representations |

So mudcat splits Datacat's group into two concepts:

- **Where files live**, with access control and inherited defaults, is the collection.
- **The named, versioned set you cite and train from** is the dataset.

Datacat merged the two because each file had one home.

That gives a clean import path for Datacat. Each Datacat group becomes:

1. a **collection**, the home of its files; and
2. a **managed dataset** in that collection, with a follow rule tracking the latest version of everything in it.

That keeps Datacat's behaviour, where the group shows the current files, and adds history. Datacat folders become plain collections. Datacat's version and location records map directly onto versions and replicas.

## 57. Did Butler use `datum`?

**Question**

> Did Butler use `datum`?

**Answer**

No. A code search of `lsst/daf_butler` finds no use of "datum". That search only covers the default branch, so it doesn't rule out the word in older Butler code or the documentation, but it isn't part of the current vocabulary. The Butler's terms are:

| Butler term | Meaning | Closest mudcat term |
|---|---|---|
| Dataset (a `DatasetRef`) | One stored thing, identified by dataset type, data ID and run, usually one file | A **version** of a data object |
| Dataset type | Name, dimensions and storage class shared by many datasets | Roughly a **metadata schema** plus a **structure descriptor** |
| Data ID / data coordinate | The values of its dimensions, such as instrument, visit and detector | Typed metadata joined to **reference data** |
| Dimension | A key with reference records (visit, detector, …) | **Reference data** |
| Component | A named part of a composite dataset, such as `calexp.wcs` | A **selector** or **element** |
| Collection (RUN, TAGGED, CHAINED, CALIBRATION) | A grouping of datasets; RUN collections also give versions by search order | Partly a **dataset** and its **snapshots**, partly a **view** |
| Datastore, storage class, formatter | Where and how bytes are stored and read | Replica and storage resource; structure descriptor and format |

The Butler's components are worth noting. They're the nearest Butler idea to mudcat's elements and selectors: addressable parts of one stored thing.

If you're considering "datum" as a name for something in mudcat, such as the element or the data object, I'd avoid it:

- **It collides with geodesy.** In geospatial and earth science a "datum" is a reference frame, as in "WGS84 datum" or "vertical datum". That's one of the fields we expect to serve.
- **It's vague.** "Datum" just means "a piece of data", so it doesn't say which level is meant.
- **It's taken.** Our notes record that INL's metadata extraction tool for HDF5, GeoTIFF, NetCDF, Parquet and Iceberg is called "Datum".

"Element" for the addressable parts, and "data object" for the logical file, are clearer and don't clash with any field.

## 58. Postgres actually uses datum internally though

**Question**

> Postgres actually uses datum internally though

**Answer**

Yes. In PostgreSQL's C internals, `Datum` is the generic container for a single value of any type, about the size of a pointer. It holds either the value itself or a pointer to it, and it's what server functions take and return. It isn't user-facing: SQL never mentions it.

That's one more argument against using the word in mudcat:

- **Different granularity.** A PostgreSQL datum is one value, such as one integer or one JSON document. In mudcat's vocabulary it would fit a single metadata value best, not an element or a file.
- **Collision in our own code.** mudcat sits on PostgreSQL and may well need server-side code: custom functions for blind indexes, partition transforms, or selector checks. Rust extensions written with `pgrx` handle `pg_sys::Datum` directly. If "datum" also meant a mudcat concept, the extension code would be using the same word for two things.

So PostgreSQL's usage confirms that the word already has established meanings in two places that matter to us, PostgreSQL internals and geodesy. I'd keep it out of mudcat's model.

## 59. Okay, so let's say a dataset was a table. a data object may actually be a partition? or ar

**Question**

> Okay, so let's say a dataset was a table. a data object may actually be a partition? or are they always separate

**Answer**

They're always separate concepts, because they live on opposite sides of the semantic/physical split. Your question also exposes a simpler way to model tables than the one I gave you.

**Why a partition isn't a data object**

- **A data object is semantic.** It has an identity and versions, it can be cited and pinned, and lineage points at it.
- **A partition is physical.** It's a grouping computed from member attributes by a spec, and it can change when the spec evolves. If a partition were a data object, repartitioning from `day` to `month` would destroy and create identities. That's exactly what the explicit-defaults principle forbids for physical dimensions.

They often *coincide*: one Parquet file per day, partitioned by day. Even then, the file is the data object, and "partitioned by day" is just how the layout groups it. Change the spec to months and the files keep their identities.

**When you want to address a partition as a unit,** for example to cite, grant, replicate or reprocess "2026-11-24", the right concept is a **selection**: the predicate `day = 2026-11-24`. It's defined by meaning, so it survives spec changes, and when it lines up with the current partitions the planner resolves it cheaply.

**The simpler model for tables.** An Iceberg table is best modelled as a **data object**, not as a dataset:

| Iceberg | mudcat |
|---|---|
| Namespace | Collection |
| Table | Data object, with records structure and format `iceberg` |
| Table snapshot | Version (identified by its metadata file's digest) |
| Branches and tags | Refs on the data object (refs already allow this) |
| Data files and manifests | The object's internal structure, managed by the table format, like members of an archive |
| Partition | Physical grouping inside the object; addressed through selections |

A data object was always meant to be "one logical thing with its own internal structure", and an Iceberg table is just a large case of that. The full range:

- a single file;
- a tar archive;
- a Zarr store;
- a Lance dataset;
- an Iceberg table.

Each has versions and replicas, and each keeps its inside to itself, which is the delegation we already wanted.

**What this changes from last time:**

- **No table-backed dataset kind.** The three "membership modes" collapse to one: datasets are always managed sets of members. A table is just a member that happens to be large.
- **A table on its own needs no dataset.** By the implied one-member rule it can be used as a dataset directly. A dataset that combines several tables, or tables with video files, is an ordinary dataset.
- **The writable Iceberg catalog maps more directly.** A commit makes a new version of the data object, and Iceberg refs are mudcat refs on that object.
- **Scale and lineage.** "Member count is independent of data volume" still holds. One member, the table, can hold 500 TB. Element lists for a table come from its own row lineage or file index, through an augmenter, when mudcat needs them.

I haven't changed the documents. This would replace the "table-backed dataset" idea from the last few messages with "tables are data objects". The architecture's Iceberg catalog section would then describe commits as new versions of the table's data object.

## 60. Is there no better naem for "data object"?

**Question**

> Is there no better naem for "data object"?

**Answer**

There are some, though none is perfect. "Data object" is precise, comes from iRODS and lines up with the FAIR Digital Object world, but it is clunky, and "object" also suggests an S3 object, which in mudcat is a replica's bytes. The name has to cover a single file, a tar archive, a Zarr store, a Lance dataset and an Iceberg table: one logical thing with versions, replicas and its own internal structure.

| Candidate | Where it's used | For | Against |
|---|---|---|---|
| **Asset** | DataHub and OpenMetadata ("data asset"); Dagster (an asset has materialisations over time and partitions) | Short; works for any size, from a file to a table; familiar to data and ML engineers. Dagster's asset is close to ours, with versions and partitions | STAC uses "asset" for one file inside an Item, which is lower in the hierarchy. Some catalogs use "asset" for anything, dashboards included |
| **Artifact** | W&B, MLflow, OCI | Versioned and content-addressed; natural for models | Suggests "output of a process"; a W&B artifact is closer to our dataset version; scientists rarely use it |
| **Granule** | NASA CMR and earth science: a collection holds granules | Pairs neatly with "collection"; means "the smallest unit managed on its own" | Unknown outside earth science; sounds small, but ours can be 500 TB |
| **Digital object** | DOI and FAIR Digital Object work, preservation (OAIS) | Standard in research-data infrastructure | Even longer than what we have; abstract |
| **Item** | STAC Item, catalog entries | Short | Generic; a STAC Item groups files, which is more like a small dataset |
| Data object (current) | iRODS | Precise, unambiguous | Clunky; the "object" clash |
| File, table, blob, record, entity, resource | — | — | Each already means something narrower or broader in mudcat or its standards |

**My pick is asset.**

- **It fits the whole range.** "This Iceberg table is an asset" reads as naturally as "this MP4 is an asset" or "this Zarr store is an asset".
- **It's what our likely users already say.** Data platform, ML and orchestration people use the word this way, and Dagster's version of it already has versions and partitions.
- **It doesn't collide with any core mudcat term:** dataset, collection, version, replica, representation and element all stay distinct.
- **The STAC clash is contained.** It belongs in the earth-science field profile, which would map a STAC Item to a mudcat asset or small dataset, and a STAC asset to a representation or replica.

The hierarchy then reads: a collection holds assets; an asset has versions; a version has replicas and possibly an element list; a dataset is a versioned set of asset versions.

If you'd rather stay close to Datacat and iRODS users, keep "data object" in the formal model and use "object" in the API and UI. I'd still choose asset for an AI-facing system.

The rename would touch every document, the schema sketch and the requirements, so I haven't made it. If you choose a name, I'll rename it consistently in one pass, including updating the Datacat, iRODS and Rucio equivalents in the comparison table.

## 61. Let's say we want to bias towards ML workflows, Ray, similar

**Question**

> Let's say we want to bias towards ML workflows, Ray, similar

**Answer**

If the bias is towards ML workflows, I'd choose **artifact** over asset. It's the term the ML tools already use for this concept, with versions, lineage and types:

| Tool | Its term | How it maps to mudcat |
|---|---|---|
| **Kubeflow / ML Metadata (MLMD)** | Artifact, execution, event, context | Almost one to one: artifact = data object; execution = activity; event = a used or generated relation; context = dataset or run grouping |
| **W&B** | Artifact (typed: dataset, model), artifact version, alias (`latest`, `production`), collection | Artifact = data object, artifact version = version, alias = our tag. W&B's registry even calls a group of artifacts a "collection" |
| **MLflow** | Artifacts logged to runs; logged models; datasets with a digest and source | Artifact = version of a data object; run = activity |
| **OCI ModelPack, KitOps** | Artifact | Models and datasets packaged as OCI artifacts |
| **DVC** | Artifacts (in its model registry), outputs | Same idea |
| **Ray** | Dataset, block, datasource, checkpoint | No term for this unit, so nothing conflicts. A Ray checkpoint becomes an artifact version when it's published |
| **Hugging Face** | Repository, revision, files | A repository revision ≈ artifact version, or a dataset snapshot |

**Why artifact over asset for this audience:**

- **It's the word ML engineers already use** for "a versioned thing with lineage, of any shape". A W&B dataset artifact can be thousands of files and a model artifact a single file, which matches our range from a single file up to an Iceberg table.
- **MLMD's model is nearly ours.** Its artifacts, executions, events and contexts line up with our data objects, activities, used/generated relations and datasets. That makes import, export and explanation easy.
- **"Asset" leans towards orchestration** (Dagster) and generic data catalogs, where it also covers dashboards.

**The cost:**

- **It suggests "output of a process".** Raw instrument data is an artifact only in MLMD's broad sense. Scientists may find it odd at first, but MLMD treats raw inputs as artifacts too, and it's a one-line definition.
- **Two small collisions:**
  - **Alias.** In W&B an *alias* is a movable pointer such as `latest`, which is our **tag** or **ref**. Our "alias" means an alternative identifier (path, DOI, Rucio name). With an ML bias I'd rename ours to **identifier** or **name**, so "alias" can't be misread.
  - **Run.** ML users say *run* for what PROV calls an activity. The model can keep "activity", but the API and UI should say "run" for training, evaluation and processing activities.

**With these choices the user-facing vocabulary is:**

- a **collection** holds **artifacts**;
- an **artifact** has **versions**, and refs such as `latest`, `main` or `release-3`;
- a version has **replicas**, and possibly an **element list**;
- a **dataset** is a versioned set of artifact versions, with **snapshots**;
- **runs** use and produce artifact versions;
- a **specification** is pinned into a training set.

Datacat, iRODS and Rucio users get a mapping table: data object, Datacat dataset or Rucio file DID all become artifact.

I haven't renamed anything. If you confirm artifact, plus "identifier" in place of "alias" and "run" in the API, I'll apply all three consistently across the model, architecture, requirements, field guide, README and schema sketch in one commit.

## 62. are there any other names, especially in HEP?

**Question**

> are there any other names, especially in HEP?

**Answer**

HEP mostly has file-centred names for this level. None is size-neutral the way "artifact" is, but several HEP terms are useful precedents elsewhere in the model. This is from my own knowledge, not checked in this session.

**Names for the unit itself** (one logical thing with versions and copies):

| Term | Where | Fit |
|---|---|---|
| **Logical file** (identified by an LFN, its logical file name) | Grid computing generally: ATLAS, CMS, LHCb (DIRAC), Belle II, iRODS-era systems | The classic HEP answer, separating the logical file from its physical copies (PFN, SURL, TURL). But it means *file*, so a Zarr store or an Iceberg table doesn't fit |
| **DID** (data identifier, `scope:name`) | Rucio | Size-neutral, but it names an *identifier*, and Rucio applies it at three levels (file, dataset, container), so it isn't one level |
| **GUID** | ATLAS files (from POOL) | An identifier, not a concept name |
| **File** | DBS (CMS), SAM and MetaCat (Fermilab and DUNE), the DIRAC File Catalog | Plain, but too narrow |
| **Dataset** | Fermi Datacat | The source of our confusion; it now means something else |

So HEP's honest answer is **logical file**. It's precise for HEP and iRODS users and fails exactly where mudcat goes further, with stores and tables. I'd keep it as the HEP alias for the concept, while the model uses **artifact**.

**HEP precedents for other parts of the model,** which may be more valuable than a name:

| HEP term | Where | mudcat equivalent |
|---|---|---|
| **Event** | Everywhere in HEP | **Element** |
| **Run, lumi section** | Experiments | **Reference data** (Butler dimensions) |
| **Block** | CMS (DBS/PhEDEx): a group of files that is the unit of transfer and placement | **Manifest** or partition as a placement unit. Rucio rules on blocks are close to our replication rules on snapshots |
| **Dataset definition** | Fermilab SAM: a saved *query* | **Specification** or selection |
| **Snapshot** | SAM: the frozen file list of a dataset definition at one moment | Exactly our **pinned specification**, and also the name we already use |
| **Data tier** (RAW, RECO, AOD, MINIAOD, NANOAOD) | CMS, ATLAS | **Representations** and derived datasets |
| **Skim, derivation** | CMS skims; ATLAS DAODs | **Selection**, or a derived dataset with provenance |
| **Friend tree** | ROOT: extra branches added beside a TTree without rewriting it | **Annotation set** |
| **Conditions, calibration** | Experiments | The **ubiquitous** usage role |
| **Container** | Rucio: a set of datasets | A dataset nested by snapshot |
| **Storage element** | WLCG, Rucio | **Storage resource** |
| **Production, campaign** | ATLAS, CMS, LHCb | Activity grouping, or a run context |

SAM's "snapshot" is worth noting because Fermilab used it, decades ago, for the same idea as pinning: freeze the result of a query, so that a processing job is reproducible.

**My recommendation:** use **artifact** as the concept, since it fits ML and is size-neutral. The HEP field profile in `fields.md` then gives these synonyms:

- **artifact** = logical file (LFN) or file DID;
- **element** = event;
- **representation** = data tier;
- **annotation set** = friend tree;
- **manifest** = block;
- **pinned specification** = SAM snapshot.

HEP users then read the model in their own words without the core taking file-only names. Nothing is renamed yet; this would be part of the same single rename pass as before.

## 63. what about in genomics or other computational biology disciplines

**Question**

> what about in genomics or other computational biology disciplines

**Answer**

Computational biology mostly names this level "file" or "object" too. But one tool, LaminDB, already uses almost exactly the vocabulary I proposed, and that exposes a naming collision we should settle. This is from my own knowledge, not checked in this session.

#### Names for the unit itself

| Term | Where | Fit |
|---|---|---|
| **DRS object** | GA4GH Data Repository Service | Very close to ours. A DRS object has an ID, a list of (algorithm, value) checksums, and *access methods* (s3, gs, https, Globus), which play the role of our replicas. It can be one blob or a bundle. The name is just "object" |
| **Artifact** | **LaminDB** (`ln.Artifact`): a file, folder, array or data frame, with versions and hashes | The strongest support for "artifact": the closest open-source neighbour in biology already uses it, for the same size-neutral range |
| **File entity** | Synapse | Files with versions inside folders and projects |
| **Dataset** | Galaxy (one file with a datatype); CZ CELLxGENE (one h5ad) | The same reversal as Datacat: "dataset" means one file |
| **Run** | NCBI SRA and ENA (SRR or ERR: the sequence data files of one run) | A strong collision with ML's "run". To a genomics user a run *is* data, not an activity |
| **File** | CWL, WDL/Cromwell, Seven Bridges, Terra | Plain and too narrow |

#### The collision this exposes: "collection"

In much of biology and ML, a **collection is a set of items**, which is our *dataset*:

- **LaminDB Collection:** a versioned set of artifacts, which is precisely our dataset.
- **CZ CELLxGENE collection:** a group of datasets, usually one publication.
- **Galaxy dataset collection:** a list or pair of datasets.
- **W&B registry collection:** groups artifact versions.
- **STAC and NASA CMR collections:** groups of items or granules.

Only iRODS (and the Butler, loosely) uses "collection" for a **folder**, which is how mudcat uses it now. If we adopt "artifact" for ML and biology users, those same users will read mudcat's "collection" as "dataset".

Two ways out:

1. **Rename our folder level to `namespace`.** This matches Iceberg and Polaris namespaces, Rucio scopes and MetaCat namespaces. Our attribute namespaces would then become **vocabularies**. Iceberg namespace = mudcat namespace, with no translation.
2. **Keep "collection"** for iRODS and Datacat continuity, and accept that biology and ML users will need the definition.

With the ML and biology bias, I'd choose option 1: namespace, artifact, version, replica, dataset, snapshot.

#### Precedents for other parts of the model

| Biology term | Where | mudcat equivalent |
|---|---|---|
| **Bundle** | Human Cell Atlas: a versioned set of files plus metadata | A dataset **snapshot** |
| **Dataset** (versioned, of specific file versions) | Synapse | Our **dataset**, membership by version |
| Read, cell, variant, spectrum | FASTQ/BAM, AnnData, VCF, mzML | **Element** |
| **Genomic interval** (`chr:start-end` on a named assembly) | htsget, BED, tabix | A **selector**, and a natural partition (`identity(chrom)`, `truncate(pos)`). It needs its reference assembly (GRCh38 or others) stated, as a geospatial region needs its coordinate reference system |
| FASTQ → BAM/CRAM → VCF; h5ad ↔ TileDB-SOMA | Pipelines | **Representations** and derived datasets |
| **Reference genome** | CRAM decoding, alignment | A **ubiquitous** input role, like calibrations in HEP |
| obs/var annotations, GFF/GTF | AnnData, genome annotation | **Annotation sets** |
| **Investigation, study, assay; sample, experiment, run** | ISA-Tab; SRA/ENA | **Reference data** and **sources** |
| **Data Use Ontology (DUO)** codes (general research use, health/medical/biomedical, disease-specific, not-for-profit) | GA4GH, dbGaP consent groups | Our **usage purposes and grants**, ready-made |
| **Passports and visas** | GA4GH | Externally issued access relationships through the authorization port |
| Splits by donor or patient, sequence-identity clusters | Machine learning on biological data | **Similarity criteria** for leakage checks |

Two of these should go straight into the design, not only the biology profile:

- **DUO as the purpose vocabulary** for controlled-access human data, instead of inventing our own.
- **DRS as an access interface:** mudcat could *serve* a GA4GH DRS endpoint, because artifact versions with replicas and checksums are already DRS objects. Genomics tools such as Terra and Seven Bridges would then fetch from mudcat directly.

#### Where the naming now stands

- **Artifact** for the unit. It is supported by ML tools (W&B, MLflow, Kubeflow's ML Metadata) and by LaminDB in biology.
- **Namespace** for the folder level, since "collection" means "set of items" to these users.
- **Vocabulary** for what we called attribute namespaces.
- **Field-profile synonyms:**
  - HEP: logical file, event, data tier, friend tree, block;
  - genomics: DRS object, read/cell/variant, bundle, DUO.

Nothing is renamed yet. This rename, plus the earlier "identifier" in place of "alias" and "run" in the API, would go in one consistent pass once you decide.

## 64. I'm happy to stick to artifact then

**Question**

> I'm happy to stick to artifact then

**Answer**

No separate answer. The work for this message is in the answer to a later message.

## 65. is that what Ray would use in particular?

**Question**

> is that what Ray would use in particular?

**Answer**

I renamed "data object" to "artifact" across the model, architecture and requirements (commit ad6444e, pushed). The model's class is now `mc:Artifact`. Its definition says an artifact can be a single file, an archive, an array store, a Lance dataset or an Iceberg table, and names the ML tools that use the term. The comparison table still lists the equivalents: iRODS data object, Datacat dataset, Rucio file DID and GA4GH DRS object. I also added synonyms to two field profiles:

- **HEP:** logical file, event, data tier, friend tree, block and SAM snapshot.
- **Genomics:** DRS object, reads/cells/variants, HCA bundle, a note that "run" means data in SRA and ENA, and DUO purpose codes.

I left "collection", "alias" and "activity" as they are, since you only confirmed "artifact".

**Does Ray use "artifact"?** No. Ray has no name for this unit, so there's nothing to conflict with:

- **Ray Data** uses `Dataset`, blocks, rows, `Datasource` and `Datasink`.
- **Ray Train and Tune** use `Checkpoint`, `Result` and trials, plus a storage path for outputs.
- **Artifacts in Ray come from integrations.** Its MLflow and W&B logger callbacks log checkpoints and files as *those* tools' artifacts.

So "artifact" is what Ray users meet in the trackers they pair with Ray, not in Ray itself. It is also the natural word for what a Ray job reads and writes through mudcat: the read connector resolves artifact versions, and the write connector publishes checkpoints and outputs as new artifact versions.

The one Ray term to watch is **`ray.data.Dataset`**. It's an in-memory, streaming distributed collection, not a catalogued, versioned set like a mudcat dataset. The connector reads a mudcat dataset, or a pinned specification, *into* a `ray.data.Dataset`. That's natural, but the documentation and API should name them clearly, for example `mudcat.ray.read(spec)` returning a Ray Dataset, so users don't conflate the two.

## 66. okay let's recap

**Question**

> okay let's recap

**Answer**

Here's where things stand.

#### What exists

**`brianv0/datacat`, branch `claude/cool-keller-domhwr`:**
- 9 research notes (Datacat code, iRODS, Rucio, Amundsen/DataHub, landscape, AI data, latest Butler, frontier-lab practice, AI-for-science practice).
- 2 reports.

**`brianv0/mudcat`, same branch name.** All pushed, written in simplified technical English:
- **`README.md`:** purpose, design principles (the draft constitution, now including "Optional structure has explicit defaults"), and the development process.
- **`docs/model.md`:** the conceptual model.
- **`docs/architecture.md`:** components, flows, D2 diagrams and the build order.
- **`docs/requirements.md`:** about 90 NASA-style "shall" requirements.
- **`docs/hosted.md`:** running mudcat as a hosted service.
- **`docs/fields.md`:** guidelines and synonyms for each scientific field.

#### The model in one breath

- **Collections** hold **artifacts**: a file, archive, array store, Lance dataset or Iceberg table. Artifacts have immutable **versions** (UUIDv7), and versions have **replicas** on **storage resources**.
- A version carries a **structure descriptor**, and **selectors** address parts of it. Where the data lists its own parts, an **element list** holds them.
- Metadata is held per namespace, with **tags**, **augmenters**, **statistics** and **hints**.
- A **dataset** is a versioned set of artifact versions (Datacat's group, extended). Its immutable **snapshots** carry a content hash, and **refs** (branches and tags) point at them.
- Usage rights attach to **sources** as grants, constraints and duties; the original terms are kept in ODRL. Access control is Zanzibar-style relationships behind a pluggable port.
- **Provenance** follows PROV, with roles on every use.
- On top sit **specifications**, which are pinned (policy enforced, decision recorded), then trained on and evaluated, with leakage checks.
- **Semantic dimensions** (version, snapshot, ref) are pinned and recorded. **Physical dimensions** (partition, layout, manifest, representation, replica) never affect identity.

#### The architecture in one breath

- **One PostgreSQL database per zone.** A transactional outbox feeds rebuildable indexes.
- **Commits** state their requirements, Iceberg style, and change only rows. A compactor seals manifests in the background.
- **Site agents** next to the data register, verify, extract and delete.
- **The catalog is never in the read path.** Engines such as Ray get plans and credentials and read storage directly.
- **Federate, delegate or replicate**, by rule, including shared custody between sovereign institutions.
- **Hosted service:** the vendor runs the control plane; customers keep the data plane, and can keep their own metadata.

#### Decided in conversation but not yet in the documents

| Decision | Status |
|---|---|
| **Partition specs** with Iceberg-style evolution: specs, stable field IDs, transforms with projections, spec per manifest | Agreed |
| **Layout**: the per-snapshot map from partition to manifests, replacing a separate manifest list | Agreed |
| **Manifest** means only the member kind (Iceberg sense); the element kind becomes **element list** | Agreed |
| **Content hash** is independent of layout, separate from manifest digests (a correction) | Agreed |
| **Tables are artifacts**: an Iceberg table is an artifact whose versions are table snapshots, with partitions addressed through selections. No table-backed dataset kind | Agreed |
| **Dataset definition**: membership by version; follow rules; nesting by snapshot; a single artifact usable as an implied dataset | Proposed |
| **Writable Iceberg REST catalog** with basic scope: namespaces, tables, commits with requirements, vended credentials, format version 2. Out of scope at first: views, scan planning, signing, events | Proposed |
| **Partition transforms** stay built-in and projectable; domain logic goes in augmenters; WASM only for augmenters | Proposed |
| **Schema sketch**: about 48 tables; loads in PostgreSQL 16 (scratchpad only) | Not committed, as you asked |
| **Risk list revised**: Butler, Iceberg, Lance and Xet show the base scale. What remains is our new combinations: sealed-plus-delta membership, policy per manifest, lineage across tools | Agreed |

#### Naming still open

- **Folder level:** "collection" (iRODS and Datacat sense) or "namespace" (Iceberg, Polaris, MetaCat sense). In ML and biology, "collection" usually means a set of items, which is our dataset.
- **Attribute namespaces:** become "vocabularies" if the folder level takes "namespace".
- **Alias:** "identifier", since in W&B an alias is a movable tag.
- **Activity:** shown as "run" in the API and UI.
- **Ray connectors:** name them so `ray.data.Dataset` and a mudcat dataset aren't confused.

#### Suggested next steps

1. **Write the agreed items** in the second table into the model, architecture, requirements and schema.
2. **Settle the open names**, then do one rename pass.
3. **Define a minimal first release.**
   - **In:** namespaces or collections, artifacts, versions, replicas, snapshots with refs, metadata, one partition spec, bulk registration, the Ray read connector, and registration of external Iceberg tables.
   - **Deferred:** the hosted service, federation, placement and the writable catalog.
4. **Benchmark the one new mechanism early:** membership as sealed layouts plus delta rows, at 10^8 versions.
5. **Find an anchor user.**
6. **Start the spec-driven process:** the README principles become the constitution, and the first release becomes the first specification.

## 67. is a dataset always a collection?

**Question**

> is a dataset always a collection?

**Answer**

No. In the current model they're separate things that relate to each other in two ways:

- **A dataset has a name in a collection.** It sits at a path such as `/projects/vision/curated-v3`, and that collection's ACLs and tags apply to the dataset entry itself.
- **Its members don't live in that collection.** Members are artifact versions whose artifacts can live in any collections. The dataset only *refers* to them.

| | Collection | Dataset |
|---|---|---|
| What it is | A place: a folder in the namespace | A set: named, versioned members |
| Its contents | Artifacts and sub-collections, each in exactly one collection | Artifact versions, annotation sets, other snapshots; an artifact can be in many datasets |
| History | Recorded changes (as-of listing), no snapshots | Immutable snapshots, refs and a content hash |
| Governs | Custody: who can read, write and administer the data | Sharing, citation, training input |

There are three places where they meet:

1. **A dataset can follow a collection.** A follow rule ("track the latest version of everything under `/raw/2026/**`") keeps a dataset's membership in step with a folder through automatic commits. The Datacat import uses this: each group becomes a collection plus a dataset that follows it, which is why the two can *look* like one thing.
2. **A collection can be snapshotted on request.** That operation creates a dataset holding the collection's contents at a given time, for citing or pinning a folder.
3. **A single artifact is an implied dataset.** It can be used where a dataset is expected, without any collection being a dataset.

I'd keep them separate, not make a dataset a special kind of collection. Datacat merged them (a group was a place, and a file lived in exactly one group), and that is what stopped one file from belonging to two groupings.

The naming question from the recap matters here. In ML and biology tools, "collection" usually *means* "a set of items": a W&B collection, a LaminDB Collection, a CZ CELLxGENE collection. Those users would expect a mudcat dataset to be a collection, and a mudcat collection to be something else. That's the strongest argument for renaming our folder level to **namespace**. The answer then reads more naturally: a dataset is never a namespace; it just has a name in one.

## 68. Recap terminologies then. show me mappings between mudcat, datacat, iceberg, felis, and ru

**Question**

> Recap terminologies then. show me mappings between mudcat, datacat, iceberg, felis, and rucio

**Answer**

These are the mapping tables. Marks used throughout:

- **†** a mudcat name with a rename still open: collection → namespace, attribute namespace → vocabulary, alias → identifier, activity → run.
- **≈** a design analogue (mudcat borrowed the idea), not an equivalence for integration.
- **—** no counterpart.

I've written them from my own knowledge of each system, not re-checked against their specifications in this session.

Iceberg needs care, because it relates to mudcat in two ways:

- **Integration:** an Iceberg table *is* a mudcat artifact, whose versions are its snapshots.
- **Design analogue:** inside a managed dataset, mudcat reuses Iceberg's *structure* (snapshot → layout → manifest → file).

#### Namespace and storage

| mudcat | Datacat | Iceberg | Felis | Rucio |
|---|---|---|---|---|
| Zone | One Datacat instance | Catalog | — | Instance, or VO (multi-VO) |
| Collection† | Folder (and the "home" role of a group) | Namespace | — | Scope (partly) |
| **Artifact** | Dataset | **Table** (integration) | — | File DID |
| Version | Dataset version | **Table snapshot** (integration) | — | — (files are immutable; no versions) |
| Replica | Location | — (≈ the files at the table location) | — | Replica |
| Storage resource | Site | Table location / storage (partly) | — | RSE (Rucio storage element) |
| Storage resource attributes, expressions | — | — | — | RSE attributes, RSE expressions |
| Alias† (path, DOI, Rucio name…) | Path | Table identifier (namespace.name) | — | DID `scope:name` |
| Replica state (registered, verified, suspect, lost, retired) | — (scan status) | — | — | Available, bad, suspicious, unavailable, quarantined (dark data) |

#### Content, structure and metadata

| mudcat | Datacat | Iceberg | Felis | Rucio |
|---|---|---|---|---|
| Structure descriptor | — (file type) | Table schema | Schema | — |
| Records structure (CSVW table group and tables) | — | Schema | Schema, table | — |
| Field | — | Field (column) | Column | — |
| Field ID (stable across renames) | — | Field ID | `@id` | — |
| Physical type | — | Primitive and nested types | `datatype` (+ per-database types) | — |
| Semantic tag (`csvw:propertyUrl`) | — | — | `ivoa:ucd` | — |
| Unit | — | — | `ivoa:unit`, `fits:tunit` | — |
| Keys, constraints, indexes | — | Identifier fields (partly) | `primaryKey`, constraints, indexes | — |
| Schema profile | — | — | Felis itself is a profile | — |
| Element | — | Row (row lineage `_row_id` in format version 3) | Row | Archive constituent (file inside a zip) |
| Element list | — | ≈ data file contents | — | Archive contents |
| Selector | — | Row filter (partly) | — | — |
| Metadata document per namespace† | User metadata (string, number, timestamp) | Table properties (partly) | Annotations | DID metadata (columns, JSON) |
| Attribute definition | — (implicit types) | — | Column definition | — |
| Tag | — | — | — | — |
| Reference data | Foreign metadata | — | Tables joined by foreign keys | — (kept in AMI or DBS outside Rucio) |
| Statistics | — | Column bounds in manifests; Puffin sketches | — | — |
| Hint | — | Sort order, write properties (partly) | — | — |

#### Datasets and versioning

| mudcat | Datacat | Iceberg | Felis | Rucio |
|---|---|---|---|---|
| **Dataset** | Group | — (≈ a whole table; a commit across tables in Nessie is closest) | — | Dataset DID |
| Dataset of datasets (nesting by snapshot) | — | — | — | Container DID |
| Snapshot | — | ≈ Snapshot | — | — (open, closed and monotonic states only) |
| Ref (branch, tag) | "Latest version" pointer | Refs: branch, tag | — | — |
| Content hash | — | — (snapshot IDs aren't content hashes) | — | — |
| Commit with requirements | — | Commit: requirements and updates | — | — |
| Snapshot summary (operation, added, removed) | — | Snapshot summary | — | — |
| Layout | — | ≈ Manifest list | — | — |
| Manifest | — | ≈ Manifest | — | — |
| Partition spec, transforms | — | Partition spec, transforms | — | — |
| Partition evolution | — | Spec evolution (spec IDs) | — | — |
| Annotation set | — | ≈ Added columns (schema evolution) | — | — |
| Representation | — | — | — | — |
| Selection | — | ≈ A filtered scan | — | — |
| View (version, location) | View (version, site) | — (Iceberg views are saved queries) | — | — |
| Snapshot expiry | — | Expire snapshots | — | DID lifetime |
| Catalog maintenance | — | Rewrite manifests, remove orphan files | — | Reaper, consistency checks |

#### Policy, access and lifecycle

| mudcat | Datacat | Iceberg | Felis | Rucio |
|---|---|---|---|---|
| Access control (Zanzibar-style relationships) | ACL (group:permissions, inherited, AFS-like) | — (left to the catalog implementation) | — | Accounts, permissions per scope |
| ACL root | Nearest container with an ACL | — | — | — |
| Source, usage grant, duty | — | — | — | — |
| Terms (ODRL) | — | — | — | — |
| Forward exclusion list | — | ≈ Delete files, deletion vectors | — | — |
| Tombstone / invalidation | — | — | — | — (Rucio "tombstone" means *eligible for deletion*, which is different) |
| Purge, deletion receipt | — | Physical deletion after expiry | — | Reaper deletion |
| Legal hold | — | — | — | Locks under a rule (partly) |
| Replication rule | — | — | — | Replication rule |
| Lock | — | — | — | Lock |
| Completeness | — | — | — | Rule state, satisfied or stuck (partly) |
| Subscription / follow rule | — | — | — | Subscription |
| Transfer (delegated to a mover) | — | — | — | Conveyor → FTS |
| Change log (CloudEvents) | — | Events endpoint (REST, newer) | — | Hermes messages |
| Quota | — | — | — | Account quota |

#### Provenance and ML

| mudcat | Datacat | Iceberg | Felis | Rucio |
|---|---|---|---|---|
| Activity† (run in the API) | — | — | — | — |
| Usage with role | — | — | — | — |
| Run record (PROV bundle) | — | — | — | — |
| Augmenter | Crawler (scan status) | — | — | — |
| Specification, pinned specification | — (search query) | — | — | — |
| Model, evaluation | — | — | — | — |
| Similarity criterion, leakage assertion | — | — | — | — |

#### How to read it

- **Datacat** maps cleanly onto mudcat's physical core and almost nothing above it. Its dataset, version, location and site are our artifact, version, replica and storage resource; its group is our dataset.
- **Iceberg** provides two things:
  - as an integration, a table is an artifact and its snapshots are versions;
  - as a design source, snapshots, refs, commits with requirements, partition specs and evolution, manifest lists, field IDs and statistics.
- **Felis** maps only to mudcat's record structure (fields, units, keys, semantic tags), which is why it's a schema profile in `fields.md`, not a core concept.
- **Rucio** maps onto placement and lifecycle: replicas, storage elements, rules, locks, subscriptions and the reaper. It has nothing for typed metadata, versions or lineage, which is why experiments pair it with a second catalog.

There are two false friends to watch for when working with these systems:

- **"Tombstone":** in Rucio it means a replica may be deleted; in mudcat it means content is invalidated.
- **"Dataset":** in Datacat it means a single file.

## 69. What is a dataset in mudcat then?

**Question**

> What is a dataset in mudcat then?

**Answer**

A **dataset** is a named set of artifact versions, together with its history. It's what you cite, share, pin into a training set, or publish. It changes only through commits, and every commit makes an immutable **snapshot**.

**What it contains (its members):**
- **Artifact versions:** specific versions of files, archives, array stores or tables, not "whatever is latest".
- **Annotation sets:** derived attributes layered on top, such as captions, labels or scores.
- **Snapshots of other datasets:** pinned by content hash, which makes a dataset of datasets.

**What it has:**
- **A name in a collection.** It lives at a path, so the collection's ACLs and tags apply to it, but its members can live anywhere.
- **Snapshots.** Each is immutable, with a parent, a change summary and a **content hash** over its members. The content hash is the snapshot's identity.
- **Refs.** Branches (such as `main`) move with commits; tags (such as `release-3`) don't. Publishing a snapshot means tagging it.
- **Optionally, a follow rule**, such as "track the latest version of everything under `/raw/2026/**`". It commits automatically, so the dataset keeps up with a folder.
- **A physical side you can ignore:** partition spec, layout, manifests, representations, replicas. These can change without changing any snapshot's identity.

**What it isn't:**

| It isn't | Because that's a |
|---|---|
| A folder; members don't live inside it | **Collection** |
| One file or one table; those are single things with versions | **Artifact** (one artifact on its own can stand in for a one-member dataset) |
| A training set with weights and sample rules | **Specification**, frozen as a **pinned specification** |
| A filtered subset | **Selection** |
| A copy in some format or place | **Representation** or **replica** |

**Examples:**
- **Robotics:** 40,000 episode videos plus a Parquet table of actions, with a captions annotation set. Tagged `v1` for a paper; `main` keeps moving as episodes arrive.
- **Science release:** a release built from three institutions' processed outputs, each included as a pinned snapshot from its own zone.
- **Benchmark:** a single Iceberg table of evaluation prompts. No wrapper is needed, because the table is an artifact and can be used as a dataset directly. Wrap it only when combining it with other things.

Compared with the earlier systems:
- In Datacat, this was a **group**, but without history, and members had to live inside it.
- In Rucio, it's closest to a **dataset DID**, plus versioned snapshots and typed members.
- In ML tools, it's what LaminDB calls a **Collection** and what Hugging Face calls a dataset repository at a revision.

The documents still say "a named, versioned set of artifacts". Membership by version, nesting by snapshot, follow rules and the implied one-member dataset are agreed here but not yet written in.

## 70. I'd like you to re-review the mappings deeply. Think about how consistent they are in this

**Question**

> I'd like you to re-review the mappings deeply. Think about how consistent they are in this broad field, and be critical and feel free to make suggestions based on other best-of-class tools for dataset management

**Answer**

The physical core holds up well, but the review found six mistakes in my tables, seven places where mudcat's vocabulary collides with itself or with the field, two structural weaknesses, and four concepts the best tools have that mudcat lacks. I checked the claims against the current Iceberg spec and REST API, Rucio's constants, the Felis data model, LaminDB, ML Metadata, OpenLineage and Croissant, all read from their GitHub repositories. Nothing in the documents has changed.

#### Corrections to the tables I gave you

| Claim | What the source says |
|---|---|
| Iceberg REST has an events endpoint | It doesn't. The spec has config, namespaces, tables, commit, multi-table commit, scan planning (`plan`, `tasks`), `credentials`, `sign`, `metrics`, `register`, views and now `functions` |
| Iceberg groups manifests by partition | No: "Manifests … are not associated with partitions." Each manifest records its spec ID, and the manifest list carries partition *summaries* per manifest |
| Rucio replica states include suspicious and quarantined | The states are available, unavailable, copying, being deleted, bad and temporarily unavailable. "Suspicious" is a bad-replica status |
| Rucio subscription ≈ mudcat follow rule | Wrong. A subscription creates replication *rules* for future data; a follow rule adds *members* |
| Rucio lock ≈ legal hold | Too weak to keep. A lock protects replicas for placement, not for legal reasons |
| Felis `@id` ≈ Iceberg field ID | Only loosely. `@id` is a cross-reference inside a schema, not an ID guaranteed stable across renames |

The second row matters for the design. Iceberg lets one manifest span many partitions and prunes on summary ranges. Our "layout: one entry per partition" is stricter and would recreate the small-manifest problem. I'd copy Iceberg: a layout is a list of manifests, each with its spec ID and partition summaries.

Other facts the sources confirmed:

- **Iceberg version 3:** it has row lineage (`_row_id`), geometry and geography types, and deletion vectors. It has no spatial partition transforms: the list is still identity, bucket, truncate, year, month, day, hour and void.
- **Iceberg version 4:** it is in development and adds relative paths, which would make replicating a table artifact across storage much easier.

#### Vocabulary collisions

| Term now | Problem | Suggestion |
|---|---|---|
| **Tag** | Used twice inside mudcat: a fixed ref *and* a governed classification | Keep **tag** for refs (git, Iceberg, OCI). Call classifications **labels** (as in Purview sensitivity labels, Kubernetes) |
| **Collection** | Means "set of items" in LaminDB (verified), W&B, STAC and PROV. Our own model makes a *snapshot* a `prov:Collection` while calling the *folder* `mc:Collection` | **Namespace** |
| **Zone** | Only iRODS uses it; in cloud a zone sits *inside* a region | **Catalog**, giving `catalog.namespace.name`, the same shape as Iceberg REST and Unity |
| **View** | Ours is Datacat's (version, site) rule; everywhere else a view is a saved query | Drop ours as an entity (it's just resolve options). Rename **selection** to **view** |
| **Specification** | Three meanings in the repo: training set, partition spec, and spec-driven development | **Recipe** and **pinned recipe**, by analogy with a lockfile |
| **Augmenter** | To ML people, augmentation means random crops and flips | **Enricher** |
| **Tombstone** | Delete marker in databases; "may be deleted" in Rucio; ours is a fourth meaning | **Invalidation** for the act. Keep "tombstone" for the stub left after erasure, as data repositories use it |

Smaller renames I'd make, grouped by confidence:

- **Already raised with you:** alias → **identifier**; activity → **run**.
- **New:** storage resource → **storage location** ("resource" is also our authorization term). The per-namespace metadata document → **aspect**, DataHub's word; OpenLineage says "facet", but that collides with search facets.

#### Structural weaknesses

1. **Artifact versus dataset is leaky.** We patched it twice: "a single artifact is an implied dataset" and "a table is an artifact", although a table is internally a set of files. Rucio (one DID, three types), git and OCI all treat "leaf" and "set" as one family. I'd keep both nouns, since LaminDB does too, but give them identical mechanics and one word for a revision: **version**. Today an Iceberg snapshot maps to a mudcat *version* while a dataset has *snapshots*, which reads backwards.
2. **Representation has no counterpart in any system.** That suggests it's a relation, not an entity. A representation is a set of artifact versions, which is just a dataset version. Model it as a dataset version linked by "representation of". HEP data tiers and OCI platform variants work this way.

#### Missing concepts

- **Split.** Croissant (`cr:Split`), Hugging Face and TFDS all have named train/validation/test splits. mudcat only has roles in a recipe. A split should be a named view stored with a dataset version.
- **Job versus run.** OpenLineage and LaminDB separate the definition from its execution. mudcat has only the execution. An enricher would then be one kind of job.
- **Run grouping.** ML Metadata has contexts, MLflow has experiments and W&B has projects. mudcat has nothing equivalent.
- **Documentation.** Dataset and model cards, as a first-class aspect.

#### Consistency by layer

- **Physical core** (artifact, version, replica, storage): consistent with Datacat, iRODS, Rucio and GA4GH DRS.
- **Versioning:** consistent with Iceberg and git once "version" is unified.
- **Governance:** no peers, so the names are ours to choose carefully.
- **ML layer:** the least aligned with ML tools.

Recommended vocabulary: catalog → namespace → artifact or dataset → version → replica at a storage location, with refs, labels, aspects, views and splits, enrichers, jobs and runs, and recipes that are pinned. Tell me which of these you accept and I'll apply them in one rename pass.

## 71. 1. keep tag, call the ref tag just that... a Tag Ref. 2. Namespaces aren't always hierarch

**Question**

> 1. keep tag, call the ref tag just that... a Tag Ref.
> 2. Namespaces aren't always hierarchical. I think ours should be? Not totally sure. Are we allowing collections of collections in datacat? how does that map to Iceberg Rest Catalog? Actually in theory I guess it's fine maybe even preferred, but I'd have to think about possible issues I guess
> 3. Drop Zone. I think Instance is better if that's what we are talking about
> 4. Drop view. Datacat always tried to have some concept of canonical, but maybe that's not necessary with lineage. it's probalby not useful I guess.
> 5. Drop Specification for now, I'm not sure it's needed yet, unless it's common in other ML tracking systems
> 6. Sure, Enricher sounds good. 
> 7. How about just Delete Marker?

**Answer**

No separate answer. The work for this message is in the answer to a later message.

## 72. If most the Data is in S3 (which it itself supports versions, delete markers), how well do

**Question**

> If most the Data is in S3 (which it itself supports versions, delete markers), how well do we map with those semantics in particular for non-tabular data

**Answer**

Your decisions are applied and pushed (commit 02f7e89). I made two judgement calls you should check, on items 5 and 7. Item 2 is still open, so I left "collection" alone.

#### What I changed

1. **Tag.** "Tag" alone now always means a classification. Refs are **branch refs** and **tag refs**.
3. **Instance** replaces zone everywhere. In the hosted document I added that an instance is a logical catalog, not a deployment: one control plane serves many instances.
4. **View is gone,** and so is the idea of a canonical replica. A request states a version and a place, defaulting to `latest` and the nearest verified replica. I kept "selection" under its own name.
5. **Specification is gone.** It isn't common in ML trackers: MLflow, W&B and ML Metadata record the *inputs a run used*, not a stored description of a training set. The nearest things are SeqIO mixtures (code), DVC lock files and the Databricks feature store's training set. mudcat now does what the trackers do.
   - **What I kept:** a small **resolution** record that the resolver writes: exact input versions, roles, action and purpose, and the policy decision. Without it the policy check has nowhere to live. Usage policy is now enforced at resolution.
   - **What is lost:** stored weights, sample rules and nested mixtures. They stay in job code and are recorded as run parameters.
6. **Enricher** replaces augmenter.
7. **"Tombstone" is gone,** but I didn't use "delete marker" for everything. In S3 a delete marker means "hidden, older versions still retrievable", which is exactly mudcat's forward delete, so it keeps that name. A takedown is the opposite: old versions must stop resolving. That is now an **invalidation record**, and the stub left after erasure is an **erasure record**. Say if you'd rather merge them.

#### Item 2: hierarchical namespaces

- **Datacat:** folders nest freely; groups hold only datasets.
- **Iceberg REST:** namespaces are multi-level. The spec has a `parent` parameter and a configurable separator, which I checked.
- **Other catalogs:** from memory, Glue and Hive allow one level, Unity fixes three, and engine support for nesting is uneven.

I'd go hierarchical. The issues to settle:

- **Engine reach:** tables that single-level engines must reach need to sit one level deep, or the gateway offers flattened names.
- **Segment names:** `/`, `.` and the Iceberg separator can't be allowed in a segment.
- **Sibling names:** a namespace and an artifact shouldn't share a name under one parent.
- **Depth and moves:** set a depth limit, and accept that moving a subtree rewrites its paths. IDs don't change.

#### S3 versions and delete markers

This is from my knowledge of S3, not checked this session. The mapping is close at the replica level:

| S3 | mudcat |
|---|---|
| Key | Path of a replica at a storage resource |
| Object version (VersionId) | One replica of one mudcat version |
| Overwrite of a key | New mudcat version of the same artifact; the old replica stays valid as a noncurrent version |
| Delete marker | Delete marker, when the bucket is the artifact's home; otherwise only that replica is hidden |
| Delete of a specific VersionId | Purge of that replica |
| Object Lock legal hold, retention | Legal hold, retention |
| Events and Inventory | Site agent registration and consistency checks |

The gaps, which need design changes not yet in the documents:

- **Replicas must record the provider's version ID.** A key alone doesn't identify the bytes.
- **Unversioned buckets.** An overwrite destroys the old bytes, so mudcat needs write-once keys there, and each storage resource should state whether its keys are mutable.
- **Who leads.** Each storage resource should say whether the bucket leads (mudcat observes) or mudcat leads (drift is an error).
- **Multi-object artifacts.** A Zarr store or image folder is thousands of keys, and S3 has no atomic snapshot of a prefix. Each such artifact version needs a list of (key, VersionId, checksum). That gives cheap time travel, but a capture taken mid-write is torn unless writers signal when they finish.
- **Purge.** S3 replication doesn't copy version-specific deletes, so mudcat must delete in each bucket. Compliance-mode Object Lock can block a purge outright.
- **Checksums.** Multipart ETags and composite checksums aren't full-object hashes, so the checksum type has to be recorded.
- **Undelete.** It should move `latest` back, not re-register the same content, which the idempotency key would reject.
