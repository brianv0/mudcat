# mudcat conceptual model

Status: draft. This document defines what mudcat records. [architecture.md](architecture.md) tells how mudcat stores and serves it.

## Conventions

The model uses RDF, with W3C and community vocabularies that already exist. JSON-LD is the exchange format. mudcat defines its own terms (prefix `mc:`) only where no standard term fits. RDF is the conceptual and interchange layer. The catalog stores its state relationally and publishes it through a fixed JSON-LD context.

Field-specific semantics, profiles and examples are in [fields.md](fields.md).

| Prefix | Vocabulary | Used for |
|---|---|---|
| `prov:` | W3C PROV-O | Entities, activities, agents, usage, derivation, invalidation |
| `dcat:` | W3C DCAT 3 | Datasets, dataset series, versions, distributions, data services |
| `csvw:` | W3C CSV on the Web | Record (tabular) structure: tables, columns, datatypes, keys |
| `odrl:` | W3C ODRL 2.2 | Terms of use as received and published (licences, contracts, opt-out policies) |
| `oa:` | W3C Web Annotation | Selectors into parts of a resource |
| `dcterms:` | Dublin Core terms | Titles, descriptions, licences, dates, creators |
| `qudt:` | QUDT | Units of measure (with UCUM codes) |
| `dpv:` | W3C Data Privacy Vocabulary | Purposes and legal bases, where personal data is involved |
| `mc:` | mudcat (placeholder namespace, not registered) | Everything else |

An entity is a node of a `mc:` class. "Subclass of" means `rdfs:subClassOf`.

### Identity

Every entity has an immutable identifier. The catalog publishes this identifier as an IRI. Names, paths and external identifiers are **aliases**. They are unique within their scheme, and point to an identifier. Most aliases are mutable. Content-hash aliases are the exception: they never change.

For every entity, versions and snapshots included, this identifier is a time-ordered UUID (UUIDv7). The zone that minted the UUID qualifies it. That zone is the entity's authority. The embedded timestamp is not a privacy risk, because the access model controls who can see an identifier. Also, UUIDv7 keeps index locality for inserts in time order.

**Content hashes.** The content checksum of a version and the Merkle hash of a snapshot are content hashes. They are immutable, unique and verifiable attributes, not identifiers. The catalog also publishes each content hash as an alias.

**Agreement across zones.** A zone can register a replica of a version that another zone produced. In this case, it uses the identifier from the zone that produced the version. Registration is idempotent on (data object, content checksum). Thus, independent zones that hold copies of the same content agree on its identity. This is important under shared custody (see [architecture.md](architecture.md)).

External alias schemes include:

- the path in the collection namespace;
- Rucio `scope:name`;
- DOI;
- Hugging Face repository and revision;
- OCI digest;
- Iceberg table identifier.

This reverses two earlier designs. Datacat used the name plus the parent folder as the key. DataHub builds URNs from platform and name strings. Renames break neither lineage nor policy.

### Immutability

Versions, snapshots, pinned specifications, policy decisions and provenance records are immutable after the catalog publishes them. Only aliases, tags, lifecycle status, replica state and access-control lists are mutable.

## Entity overview

```
                      Zone
                        │
                   Collection ──── contains ───┐
                        │                      │
                    DataObject ◄─ specializationOf ─ Version ── structure ──► StructureDescriptor
                                                   │   │                         (records│array│stream│…)
                                         replicaOf │   │ hasSource
                                                   ▼   ▼
             StorageResource ◄── storedOn ── Replica   Source ── terms ───► odrl:Policy
                                                   ▲
                      Dataset ── hasVersion ──► Snapshot ── hasMember ──► Version / AnnotationSet
                                                   │
                                 Representation ◄──┘ (materialised forms, each with replicas)

  Specification (prov:Plan) ── pinned as ──► PinnedSpecification ── materialised as ──► Representation
                                                   │ used (role: training)
                                                   ▼
                                               Training run ── generated ──► Model ── used by ──► Evaluation ──► EvaluationResult
```

## 1. Namespace and storage

These concepts come from iRODS and Datacat. The placement concepts come from Rucio.

**Zone** (`mc:Zone`). An independently administered mudcat instance that mints identifiers and can federate with other zones (from iRODS).

**Collection** (`mc:Collection`). A node in the logical namespace (iRODS collection; Datacat folder). It contains data objects and other collections. Its path is an alias. A collection can carry inherited defaults: access-control lists, metadata and provenance (for example, "instrument X produced everything under `/raw/campaign42`").

**DataObject** (`mc:DataObject`, subclass of `prov:Entity`). A logical unit of content, independent of how many revisions or copies exist (iRODS data object; Datacat dataset; Rucio file DID). Its mutable pointers are refs (section 4), for example `latest`.

**Version** (`mc:Version`, subclass of `prov:Entity`).

- An immutable revision of a data object (Datacat dataset version). It is `prov:specializationOf` its data object. It has `mc:versionNumber` as an integer and `dcat:version` as a label.
- It carries a set of content checksums, each an (algorithm, value) pair. The catalog permits several algorithms because different storage resources compute different checksums: adler32, MD5, SHA-256, CRC32C, and the chunk hashes of content-defined storage.
- It also carries its size, structure descriptor, source and format.
- It records the reader or formatter that is necessary to open it, so old data stays readable without configuration (a lesson from the Butler).

**Replica** (`mc:Replica`). One physical copy of a version on a storage resource (iRODS replica; Datacat location; Rucio replica).

- Attributes: URL or path, storage class, observed checksums, last verification time, and `mc:replicaState`.
- States: `registered` → `verified` → `suspect` → `lost` | `retired`.
- Replicas are byte-identical copies. Thus, they are physical records and not separate `prov:Entity` nodes.

**StorageResource** (`mc:StorageResource`). A place where replicas live (iRODS resource; Rucio storage element; Datacat site).

- Resources form a hierarchy, as iRODS composable resources do: provider → account → region → bucket or file system → prefix.
- Attributes: jurisdiction, storage class (with tape and cache tiers), protocols, egress cost, and if data can leave the resource at all. The catalog also permits arbitrary attributes, so that Rucio-style expressions can address them.
- An optional deterministic path function maps a version to its path on the resource.
- Access endpoints are `dcat:DataService` nodes.

**View** (`mc:View`). A rule that resolves a logical name to one version and one replica. It is a general form of Datacat's (version, site) view.

- Version selector: `latest`, a number, a ref, a snapshot, or a time ("as of T").
- Location preference: a named resource, "nearest to compute location L", "cheapest verified" or "canonical".

## 2. Structure

Every version has a **structure descriptor** (`mc:StructureDescriptor`). A format plugin extracts it at registration, or the producer declares it. Each descriptor has one kind:

| Kind | Examples | Addressable element | Selector |
|---|---|---|---|
| `mc:Opaque` | Model weights, a PDF, a proprietary instrument file | The whole object | None |
| `mc:Container` | tar, zip, HDF5 groups | Member | Member path, byte range |
| `mc:Records` | Parquet, Lance, CSV, JSON Lines, TFRecord, database tables | Record | Key set, row range |
| `mc:Array` | Zarr, netCDF | Chunk or region | Hyperslab over named dimensions |
| `mc:Stream` | MCAP and ROS bags, video, audio, waveforms | Message or frame | Channel and time span |
| `mc:Graph` | Knowledge graphs, meshes, interaction networks | Node, edge or subgraph | Key set |
| `mc:Composite` | A robot episode; a set of images with a design table | A tuple of components | Component selectors joined on an alignment key |

### Fields: one concept for columns, variables and channels

The common unit of description is the **field** (`mc:Field`): a named, typed, documented, semantically annotated quantity. It covers:

- a column of a table;
- a variable of an array dataset;
- a channel of a stream;
- a metadata attribute definition (section 3).

Every field has:

| Property | Term | Notes |
|---|---|---|
| Identifier | `mc:fieldId` | A UUIDv7. It does not change when the field gets a new name. Schema evolution and annotation sets refer to fields by this identifier, as Iceberg tables refer to columns by field ID |
| Name | `csvw:name` | Unique within its parent |
| Description | `dcterms:description` | |
| Logical datatype | `csvw:datatype` | XSD-based, with CSVW's derived types and formats |
| Physical datatype | `mc:physicalType` | Arrow type string. Necessary for nested lists, structs, tensors and fixed-width binary, which CSVW datatypes do not express well |
| Required | `csvw:required` | |
| Unit | `qudt:unit` | With UCUM code. The catalog keeps a raw unit string from the source format as `mc:unitString` |
| Semantic annotation | `csvw:propertyUrl` | The property the values denote, as an IRI. In CSVW this is the RDF property a column's values are asserted with, so it is the standard's own semantic hook |
| Value identifiers | `csvw:valueUrl` | URI template for identifier-valued fields: a canonical identifier of a domain entity, a key in a reference table |
| Shape | `mc:shape` | For array variables and tensor-valued cells: named dimensions |

### Record structures follow CSVW

A records descriptor is a CSVW table group:

- `csvw:TableGroup` contains `csvw:Table`s.
- Each table has a `csvw:tableSchema` that contains `csvw:Column`s, which are fields.
- Keys are `csvw:primaryKey`, and `csvw:foreignKey` with a `csvw:reference` to another table and its columns.

Element metadata uses foreign keys to declare its joins to **reference data** (section 3).

### Schema profiles

Community schema languages map onto fields and CSVW tables as import and export **profiles**, not as separate models. The field is the core concept, and CSVW gives the record structure. A profile carries extra annotations where it needs them. Croissant record sets are an example. The field guidelines list the profiles.

Most schema languages have the same parts, and each profile maps them in the same way:

| Part of a schema language | mudcat |
|---|---|
| Schema | `csvw:TableGroup` |
| Table | `csvw:Table` |
| Column | `csvw:Column` (a field) |
| Datatype | `csvw:datatype`, plus `mc:physicalType` |
| Nullable | The inverse of `csvw:required` |
| Description | `dcterms:description` |
| Semantic tag | `csvw:propertyUrl` |
| Unit | `qudt:unit`. The original unit string stays in `mc:unitString` |
| Primary key | `csvw:primaryKey` |
| Foreign key | `csvw:foreignKey` |
| Unique and check constraints, indexes | `mc:constraint`, `mc:index` |
| Annotations that only the profile uses | Profile annotations. mudcat keeps them, so that the profile can generate its own schemas and service metadata again |

### Arrays, streams and containers

- **Array descriptors** list:
  - dimensions (name, size, coordinate variable);
  - variables, which are fields with shape;
  - the chunk grid and codecs.

  Variable attributes map onto field properties. For example, a semantic name maps to `csvw:propertyUrl`, and units map to `qudt:unit`.
- **Stream descriptors** list:
  - channels, which are fields with a message schema;
  - the time base and clock;
  - nominal rates;
  - the index that is necessary to seek by time.
- **Container descriptors** list members, or a member pattern with a count if a full list is too expensive.
- **Composite descriptors** list components and the alignment key.

### Elements and selectors

An **element** is an addressable part of a version. A **selector** (`mc:Selector`) names the element. A selector on a version is itself a resource with an IRI: the version IRI plus a fragment. Thus, metadata, provenance and policy can attach to it. Selectors reuse W3C forms where they exist:

| Selector | Standard basis |
|---|---|
| Byte range | `oa:DataPositionSelector` |
| Member path | `oa:FragmentSelector` with a member-path fragment |
| Time span on a stream or media file | Media Fragments `#t=start,end`, plus channel |
| Spatial region of an image | Media Fragments `#xywh=` |
| Key set or row range | `mc:KeySelector`, `mc:RowRangeSelector` |
| Hyperslab | `mc:HyperslabSelector`: per-dimension index or coordinate ranges |

**The catalog enumerates elements only where the data enumerates them.** A **manifest** (`mc:Manifest`) lists the elements of one or more versions. It is a records-structured version with a fixed schema: element identifier, source, selector, fingerprints, and lineage fields. Arrays and streams usually have no manifest. Their coordinate system is the description, and selectors define their elements.

## 3. Metadata

Metadata follows the iRODS attribute–value–unit (AVU) form, with Datacat's typed values. Each attribute is a field.

- **Attribute definitions** (`mc:AttributeDefinition`, subclass of `mc:Field`) are registered in a namespace. Each definition gives its attribute a name, datatype, unit, semantic annotation, description and optional permitted values. An AVU becomes the triple (attribute definition IRI, typed literal, unit IRI).
- **Undeclared attributes:** the catalog accepts them, stores them and can search them. But it marks them as undeclared, and it does not use them for range queries or sorts until someone declares them.
- **Metadata schemas** (`mc:MetadataSchema`) group attribute definitions. They tell which entity classes they apply to, and which attributes are necessary.
  - mudcat generates validation shapes from them: SHACL for RDF consumers and JSON Schema for clients.
  - Schema authors can write them in LinkML.
- **Attachment levels:**
  - collection, as inherited defaults;
  - data object, which applies to all of its versions;
  - version;
  - replica (physical facts only);
  - element, through annotation sets (section 4).
- **Assertion origin.** Each value records if a person asserted it, a plugin extracted it, or a model inferred it (`mc:assertedBy`). Where relevant, it also records a confidence. Agent-extracted metadata is now common, and consumers need to tell it apart.
- **Reference data.** Reference tables, for example of instruments, campaigns or partners, are ordinary record datasets. Attributes link to them through `csvw:valueUrl` and foreign keys, and queries can join through them. This is a general form of Datacat's foreign metadata, the Butler's dimension records and MetaCat's external filters.

## 4. Datasets, snapshots and representations

**Dataset** (`mc:Dataset`, subclass of `dcat:Dataset`). A named, versioned set of data objects that mudcat treats as a unit (Datacat group; Rucio dataset). A dataset that grows over time with appended data is also a `dcat:DatasetSeries`.

**Snapshot** (`mc:Snapshot`, subclass of `prov:Entity` and `prov:Collection`). An immutable version of a dataset. `dcat:hasVersion` and `dcat:previousVersion` link it. Each change to a dataset makes a new snapshot, as each commit to an Iceberg table does. mudcat does not change a snapshot after it makes it.

- **Members** (`prov:hadMember`) are versions and annotation sets.
- **Content hash.** The snapshot has a Merkle hash (`mc:contentHash`) over the hashes of its members. The Butler's collection-snapshot proposal does the same. Snapshots share their unchanged parts. It is possible to verify a snapshot from its hash alone.
- **Parent and summary.** Each snapshot records its parent snapshot and a summary of the change. The summary gives the operation (add, remove, replace or annotate) and the number of members added and removed. The summary lets clients follow the history of a dataset without a comparison of member lists.
- **Structural sharing.** A snapshot refers to its members through manifests. A new snapshot refers again to each manifest that did not change, and writes new manifests only for the changes. Iceberg shares manifests between table snapshots in the same way.
- **Status:** `published`, `deprecated` or `withdrawn`, or no status for a snapshot that is not published.
  - Only published snapshots can be pinned into training-set specifications or cited.
  - mudcat can mint a DOI for a published snapshot.
- **Retention.** mudcat keeps each published snapshot. A snapshot that is not published expires after a retention period that the dataset sets. It expires only if no ref and no pinned specification refers to it. Then it removes the manifests that only that snapshot used.

**Ref** (`mc:Ref`). A named, mutable pointer from a dataset or a data object to one snapshot or version, as in Iceberg's branches and tags.

- A **branch** moves when a writer commits a new snapshot to it, for example `main` or `curation`.
- A **tag** does not move after mudcat creates it, for example `release-2026-11`.
- **Publication** is a tag on a snapshot, and gives the snapshot the `published` status. This replaces a mutable "working" snapshot: work happens on a branch of immutable snapshots, and publication marks one of them. This is the write-audit-publish pattern of Iceberg users.
- **Conditional updates.** A writer moves a branch only with a condition on the snapshot that the branch points to now. If another writer moved the branch first, mudcat rejects the update, and the writer tries again on the new snapshot. This is the optimistic concurrency of Iceberg commits.
- mudcat records the history of each branch, so a view can resolve a branch "as of" a time.

**Snapshots and collections.** Snapshots belong to datasets, not to collections. This follows Iceberg, where each table has its own snapshots and a namespace has none. mudcat gives three things for collections instead:

- **History of a collection.** mudcat keeps the transaction-time history of collection membership, aliases and refs (section 5, time travel). Thus a reader can list a collection subtree "as of" a time without a snapshot.
- **Atomic changes.** One conditional commit can add or change many data objects in one or more collections, in one transaction. All of them appear at the same time.
- **Snapshot on request.** A user can make a snapshot of a collection subtree as of a time. mudcat then makes a dataset whose snapshot contains the versions that the subtree held at that time, with a Merkle hash. The Butler's collection-snapshot proposal does the same. Such a snapshot can be published and cited like any other.

mudcat does not make a snapshot for each change to a collection, for two reasons:

- **Hot spots.** A hash over a subtree changes with each write under it. Every writer would then need a conditional update of the same ref at the root, and concurrent writers would conflict all the time.
- **Size.** A collection that receives data all the time would make one snapshot for each change. The Butler team names one large, growing collection of raw data as a failure case. For such a collection, a dataset series with one snapshot for each period (for example one each day) is better.

**AnnotationSet** (`mc:AnnotationSet`). An independently versioned set of derived attributes: captions, labels, quality scores, embeddings, safety flags, new measurements. Its key is an element identifier or a selector. Each has the activity that produced it. The addition of an annotation set creates a new snapshot that shares every other member with its parent. Exclusion lists for one dataset (for example opt-outs) are annotation sets. Takedowns use the zone-wide invalidation set instead (section 5).

**Representation** (`mc:Representation`, analogous to `dcat:Distribution`). One physical form of a snapshot, such as native HDF5, analysis-ready Zarr, Parquet, WebDataset shards or tokenized shards.

- Derivation: `mc:materializedFrom` the snapshot, with the transform parameters. `mc:materializedFrom` is a subproperty of `prov:wasDerivedFrom`.
- Representations of one snapshot are `prov:alternateOf` each other.
- Replicas are byte-identical only within a representation.

**Selection** (`mc:Selection`). A no-copy subset of one or more snapshots. A predicate or an explicit list of elements defines it. It also has the hash of the resolved element list.

## 5. Sources, access control and usage policy

### Sources

A **source** (`mc:Source`, subclass of `prov:Entity`) records where data originated and holds its usage terms. Every version, and every element that a manifest lists, references a source. Thus, mudcat evaluates policy over millions of sources and never over billions of elements. Each source has the activity that acquired it (`prov:wasGeneratedBy`).

| Source type | Records |
|---|---|
| `mc:CrawlSource` | Domain, fetch time, crawler identity, robots and opt-out state at fetch |
| `mc:LicensedSource` | Contract, licensor, permitted uses, term and expiry, deletion obligations |
| `mc:HumanDataSource` | Campaign, vendor, pseudonymous contributor, rubric version, quality-assurance outcome |
| `mc:UserDataSource` | Consent scope, legal basis (`dpv:`), retention class |
| `mc:GeneratedSource` | Model that generated it, prompt version, seed, parent elements |
| `mc:InstrumentSource` | Instrument and its calibration, protocol version, batch, planned versus executed procedure |
| `mc:ComputedSource` | Method record (section 6), code version |
| `mc:ExternalSource` | Enclave or federated partner, access conditions; possibly no reachable replica |

### Access control

Access control decides who may read, change and administer catalog entries and the data they describe. mudcat models it as relationships, in the style of Google's Zanzibar. This lets either mudcat itself or an external Zanzibar-style service serve it (see the authorization section of [architecture.md](architecture.md)).

- **Object types:** zone, collection, data object, dataset, snapshot, specification, model, storage resource, group, compartment, ticket.
- **Relations** connect objects to subjects or to other objects, for example `collection#owner`, `collection#writer`, `collection#reader`, `collection#acl_root`, `dataset#collection`, `group#member`, `compartment#member`. A subject is a user, a service account, or a set of subjects such as `group:team#member`.
- **Permissions:** mudcat computes them from relations by union, intersection and exclusion, and by a step along a relation to another object. For example, the readers, writers and owners of the collection's ACL root get read permission. If the object has a compartment, they must also be members of that compartment.
- **Datacat's permission set** (read, insert, write, delete, admin) maps onto permissions in this schema. iRODS read, write and own map onto the same set.

Three rules keep the schema portable and the graph shallow:

- **ACL roots.** mudcat keeps Datacat's rule: a collection without its own ACL inherits from the nearest ancestor that has one. mudcat maintains an `acl_root` relation from every collection to that ancestor. Thus, a check follows one hop, independent of the depth of the folder tree. External services cap the resolution depth, usually at a few dozen hops.
- **Tickets.** An iRODS-style ticket becomes a relationship whose condition limits it by expiry time or use count.
- **Compartments.** A compartment is an intersection. A subject needs membership of the compartment's group in addition to the ordinary grant. Compartments implement partner segregation and restricted evaluation sets.

**Correspondence with Datacat.** Of the reviewed systems, Datacat's ACL model is the closest to this one. It maps directly:

- **Entries become relationships.** A Datacat entry such as `alice@team:g:rw` on collection X becomes the relationships `collection:X#reader@group:team/alice#member` and `collection:X#writer@group:team/alice#member`.
- **Group lookup.** The group-manager lookup becomes the `group#member` relation.
- **Effective permissions.** The effective-permissions call becomes a check, or a lookup of subjects.

One difference is deliberate:

- **Datacat inherits by override.** A node with its own ACL replaces the inherited ACL, as in AFS.
- **Zanzibar inherits by union.** When a check follows a parent relation, it adds the parent's grants to the node's own grants.

The `acl_root` relation preserves Datacat's override semantics. Explicit additive grants are still possible where users want them.

For contrast, iRODS copies a collection's ACL onto new objects at creation time. Thus, later changes to the collection do not apply to objects that already exist. Rucio and the Butler grant permissions per account or per repository.

#### Granularity: containers by default, objects by exception

By default, grants go on containers. mudcat permits grants on individual objects as exceptions. It discourages grants on individual files and elements.

There are two containment structures, and they have different roles:

| Structure | Contains | Role in access control |
|---|---|---|
| Collection (Datacat folder) | Data objects and collections; each data object is in exactly one collection | **Custody.** Who may read, write and administer the data. ACLs usually sit here |
| Dataset and snapshot (Datacat group) | Versions, possibly from many collections; a version can be in many datasets | **Sharing.** A read grant on a published snapshot gives access to its members, with no change to their collections' ACLs |

The rules by tier:

| Tier | Entities | Grants |
|---|---|---|
| Containers | Collections, datasets | The normal place for grants, through ACL roots |
| Rich entities | Snapshots, specifications, models, evaluation suites | Individual grants permitted, because there are at most millions of them |
| Cheap entities | Data-object versions and replicas | Individual grants only as exceptions: one shared file, or one file withdrawn from an otherwise readable collection (an exclusion relation). The catalog reports how many exist |
| Elements | Parts of a version | No access grants. Element-level control is usage policy (exclusion lists) or redaction tiers (separate annotation sets with their own compartment) |

**Access through a dataset is a capability, not a set of per-file tuples.**

- A snapshot can have millions of members. Relationships from each member to each snapshot that contains it would multiply the relationship count by the number of snapshots.
- Instead, mudcat checks read permission on a snapshot once, at the snapshot. The resolver then lists the snapshot's members and issues credentials for them.
- Thus, a caller can read a member through a snapshot only when it resolves that snapshot, never when it browses the member's own collection.

**The catalog evaluates per-object conditions.** Some conditions are different for each object, such as an embargo date or a consent status. mudcat does not write these as one conditional relationship per object.

- The authorization service decides the container-level grant.
- The catalog adds a predicate over the object's own metadata in SQL, for example `release_date <= now()` unless the caller is in the project's group.
- Effective access is the conjunction of the two.

**Usage policy follows sources, not containers.** Sources cross the boundaries of collections and datasets. This gives three independent axes:

- custody follows collections;
- sharing follows datasets;
- usage rights follow sources.

Because these axes are separate, the folder layout does not have to mirror licence boundaries.

**Storage layout should follow access boundaries.**

- On object storage, the credentials that the resolver issues ultimately do the enforcement.
- A credential for a prefix is safe only if the prefix contains nothing that the caller may not read.
- Thus, collections that are ACL roots should map to their own storage prefixes, through the deterministic path function of the storage resource. Where they cannot, the resolver must use signed URLs per object instead.

**Ownership of relationships:**

- **Catalog-owned.** mudcat owns the relationships that follow from catalog structure (parent collection, ACL root, dataset membership). It synchronises them to any external service.
- **Written through mudcat's API.** Clients write grants (who is a reader or writer) through mudcat's API.
- **Externally owned.** The identity provider or the authorization service can own group membership.

### Usage policy

Usage policy decides the permitted uses of data. It is separate from access control because permission to read is not permission to train. It has two parts. The catalog database holds both parts, and mudcat evaluates them, independent of the authorization service that a deployment uses.

**Usage grants** (`mc:UsageGrant`). A relation between a source and a permitted (action, purpose) pair:

- **Actions:** `mc:use`, `mc:train`, `mc:evaluate`, `mc:fineTune`, `mc:distribute`, `mc:publishDerivative`.
- **Purposes:** from DPV where possible, for example research, commercial research or product development.

Grants have the same shape as access relationships, for example `source:X#train@purpose:research`. Thus, mudcat can mirror them to an external Zanzibar-style service for online checks. Enforcement does not depend on that mirror (see pinning, below).

**Constraints and duties** are structured records attached to sources:

- **Constraints:** expiry date, permitted jurisdictions, permitted compute locations, embargo end.
- **Duties** (`mc:Duty`): attribute, delete at close-out, honour opt-outs, notify the licensor, do not redistribute derivatives.

Duties **propagate** along `prov:wasDerivedFrom` and training usage to all derived entities, models included. They accumulate. mudcat flags conflicts and never resolves them silently.

**Evaluation at pinning.** A training-set specification is permitted for an (action, purpose) only if every source that contributes to it grants it and no constraint excludes it. The pinning service computes this with set operations in SQL over the sources of all components. Usually there are thousands to millions of these sources. The pinning service does not do one check per source. It also records the accumulated duties in the policy decision.

### Terms of use

mudcat keeps the terms themselves (what a licence or contract says) separate from the compiled grants, constraints and duties. Where possible, it records them in **ODRL 2.2** (`odrl:Policy`):

| Terms arrive as | Handling |
|---|---|
| A standard licence identifier (SPDX: `CC-BY-4.0`, `CC-BY-NC-4.0`, `Apache-2.0`, …) | Mapped to a pre-compiled set of grants, constraints and duties |
| An ODRL document: a data-space contract, a TDM Reservation Protocol opt-out policy, a RightsML licence | Compiled automatically; clauses that cannot be translated are flagged for review |
| A contract in plain text | A person records the grants and duties, and the document is attached |

- **Traceability.** Each compiled grant, constraint and duty records the clause it came from.
- **Partial compilation.** If mudcat compiled only part of the terms of a source, it marks the source as partly compiled. Pinning blocks any purpose that depends on the untranslated part until a reviewer approves it.
- **Export.** mudcat exports terms with published snapshots (`odrl:hasPolicy` in DCAT, and in Croissant and RO-Crate exports). The federation feed between zones also carries them.
- **Negotiation.** A zone can publish an ODRL offer and record the accepted agreement before the zones exchange data.

### Removal, tombstones and holds

"Delete" can mean five different operations. They differ in two ways:

- **Direction in time.** A forward operation changes only what comes after it, as an Iceberg delete file or an S3 delete marker does. A retroactive operation also changes what a reader can do with history.
- **What remains.** Some operations keep the bytes, some keep only the catalog record, and some keep only a minimal tombstone.

| Operation | Effect | Direction | Bytes | Catalog record |
|---|---|---|---|---|
| **Remove from a dataset** | A new snapshot without the member | Forward | Kept | Kept. Older snapshots still contain the member |
| **Delete a data object** | A **delete marker**: a new version with no content becomes `latest` | Forward | Kept until the retention of the data object ends | Kept. Older versions still resolve |
| **Invalidate** | A **tombstone** (`prov:wasInvalidatedBy` an `mc:Takedown` activity) on an element, version, source or snapshot, with the reason and the party that made the request | Retroactive | Kept until purge | Kept, with the tombstone |
| **Purge** | Removal of the bytes of each replica, with a **deletion receipt** (`mc:DeletionReceipt`) per replica: the content, the location, the time and the agent | Retroactive | Removed | Kept. Replicas get the state `retired` |
| **Erase** | Removal of the catalog record. Only a minimal tombstone stays: the identifier, the date, the reason and the stored content hash | Retroactive | Removed | Removed, except the minimal tombstone |

The rules:

- **Tombstones apply to all snapshots.** A tombstone does not create a new snapshot of each dataset that contains the content. mudcat keeps one zone-wide **invalidation set**. The resolver and the pinning service apply it to every snapshot, old or new. Thus a takedown takes effect at once, also for snapshots that a pinned specification already uses.
- **Exclusion lists in a dataset are forward.** An exclusion list that is an annotation set of one dataset (for example the opt-outs of one source) is a forward operation. It affects only the snapshots that contain it. This is the equivalent of an Iceberg deletion vector.
- **Snapshots keep their hashes.** A snapshot that contains invalidated, purged or erased content keeps its Merkle hash. The hash still proves what the snapshot contained. mudcat can make a redacted successor snapshot, for continued use of the rest of the content.
- **Erasure is for data that must not stay in the catalog,** for example personal data in metadata. The minimal tombstone does not contain the erased metadata. Sometimes the content hash can itself identify a person. Then mudcat also removes the hash. As a result, nobody can verify that member of the snapshots that contained it.
- **Legal holds** (`mc:LegalHold`) block purge and erasure for the entities in scope, and move them to a retention compartment, with the reason recorded. Where a storage resource has native retention locks, for example object lock on a bucket, mudcat applies the hold through them.
- **Models** that used invalidated content get a flag with the number of affected elements. A person decides what to do, and mudcat records the decision.

### Time travel

A reader can resolve a dataset, a data object or an alias "as of" a time T. Four rules apply:

1. **History, not past permissions.** As-of resolution gives the membership and metadata as of T. But mudcat applies the access control, usage policy and invalidation set of now. A reader cannot use time travel to get content that mudcat invalidated after T.
2. **Audit is separate.** An auditor who must know what mudcat permitted at T reads the recorded decisions. These are the decision in each pinned specification, with its authorization consistency token, and the audit log. Time travel does not compute past decisions again.
3. **Retention limits time travel.** mudcat keeps published snapshots, so time travel to them always works. Unpublished snapshots expire. As-of resolution to a time whose snapshot expired fails with an error that states the expiry. It does not silently resolve to another snapshot.
4. **Mutable state has a history.** mudcat keeps the transaction-time history of refs, aliases, ACLs, grants, replica states and lifecycle status. Thus, for example, an old path in a script resolves to the data object that the path named at T. Some periods of validity are part of the data, for example the term of a licence or an embargo date. These are ordinary attributes, not a second time dimension.

## 6. Provenance

Provenance uses PROV-O directly.

**Entities** are versions, snapshots, annotation sets, representations, pinned specifications, models and evaluation results.

**Activities** (subclasses of `prov:Activity`):

- `mc:Ingest`, `mc:Transform`, `mc:Training`, `mc:Evaluation`, `mc:Simulation`;
- `mc:Experiment` (physical or laboratory);
- `mc:Takedown`.

Each activity records:

- code version;
- container image digest;
- parameters;
- start and end time;
- the storage resource or cluster where it ran. The location of a run can be a legal fact.

**Agents** are people (`prov:Person`), services (`prov:SoftwareAgent`), instruments (`mc:Instrument`) and models. A model is an entity, and it also acts as an agent when it generates data.

**Usage roles.** mudcat qualifies `prov:used` with `prov:qualifiedUsage` and `prov:hadRole`. Roles form a small vocabulary:

- `mc:training`, `mc:validation`, `mc:test`;
- `mc:reference`, `mc:calibration`;
- `mc:baseModel`, `mc:tokenizer`, `mc:configuration`.

A role can carry the mark **ubiquitous** (calibration, tokenizer, base model), and default ancestry queries exclude these roles. Without this mark, one shared calibration links every raw file to every output, as the Butler team observes.

**Derivation subproperties:** `mc:fineTunedFrom`, `mc:quantizedFrom`, `mc:distilledFrom`, `mc:materializedFrom`, all subproperties of `prov:wasDerivedFrom`.

**Plans.** Training-set specifications, protocols and pipelines are `prov:Plan`s. `prov:qualifiedAssociation` and `prov:hadPlan` link them.

**Method records** (`mc:Method`) describe, as structured attributes, how someone measured or computed a value:

- for a computation: the measurement or computation method with its parameters, convergence criteria, code;
- for a physical experiment: instrument settings, protocol version.

A combination of values from different methods needs a declared equivalence (`mc:equivalentMethod`).

**Run records.** The full detail of a run (configuration, logs, per-task status, element-level usage) is a `prov:Bundle`. mudcat stores this bundle as a catalogued version. The catalog keeps only the activity, its qualified usages and generations, and a searchable summary. This follows the Butler's decision to move provenance into files.

**Capture mode.** Each relation records if it was:

- `observed`: inferred from credentials mudcat issued during the run;
- `declared`: reported by the client SDK;
- `imported`: received as OpenLineage events or from an experiment tracker.

Manifest columns carry **element-level lineage**: parent element identifiers, or a parent selector for arrays and streams.

**The experimental loop.** A model generates a **candidate** (`mc:Candidate`): a domain entity with a canonical identity. An `mc:Experiment` uses the candidate and generates results. These results go into a new snapshot. Because every version is immutable, the lineage graph stays acyclic across model generations.

## 7. Training and evaluation

**Specification** (`mc:Specification`, subclass of `prov:Plan`). A declarative training or evaluation set. It contains:

- **Components:** snapshot references, or queries that resolve to snapshots or selections.
- **Filters** over metadata and annotation sets.
- **Weights.**
- **Sample rule** (`mc:SampleRule`). For records, one record. For arrays, a window over named dimensions with size, stride and offset. For images, a crop. For streams, a clip or episode boundary.
- **Order and curriculum.**
- **Transforms,** such as tokenizer version and normalisation.
- **Seed.**

Specifications can nest. This is the object that SeqIO calls a mixture.

**PinnedSpecification** (`mc:PinnedSpecification`, subclass of `prov:Entity`). A specification frozen at a point in time:

- every component is fixed to a snapshot hash;
- every weight is a number;
- the pinning service attaches a **policy decision** (`mc:PolicyDecision`): the policies that it evaluated, the evaluator version, the outcome and what it excluded.

mudcat enforces usage policy at pinning.

**Materialisation.** A representation generated from a pinned specification. A training activity uses a pinned specification, in the `mc:training` role, through one of its materialisations.

**Model** (`mc:Model`, subclass of `mc:DataObject`). Its versions are checkpoints and exported weights, usually with opaque or container structures. The derivation subproperties above link them.

**EvaluationSuite** (`mc:EvaluationSuite`, subclass of `mc:PinnedSpecification`). A pinned specification with a protection class: public, restricted or held by a third party. It also carries registered canary strings and an access audit requirement.

**EvaluationResult** (`mc:EvaluationResult`). An `mc:Evaluation` activity that used a model and a suite generates it. It records metrics and their definitions.

**SimilarityCriterion** (`mc:SimilarityCriterion`). A registered, versioned rule that decides when two elements count as the same for leakage purposes:

- kinds: exact checksum, near-duplicate fingerprint, a domain-specific similarity measure, coordinate or time-range overlap, event date before a cutoff;
- plus its parameters, threshold and implementation version.

**LeakageAssertion** (`mc:LeakageAssertion`). A statement that two pinned specifications satisfy, or violate, a criterion. It carries the evidence (a similarity table or an identity-index query) and the time of the check.

## 8. Placement

**ReplicationRule** (`mc:ReplicationRule`), from Rucio. Fields:

- a subject: a snapshot, representation, pinned specification, or a query over them;
- a storage-resource expression;
- a number of copies;
- a lifetime;
- an owner.

Rules create locks on the replicas that satisfy them. Cache eviction can remove unlocked replicas.

**Completeness** (`mc:CompletenessRecord`). The statement that one representation is complete and verified on one storage resource at a given time. mudcat reports placement at this level, not file by file.

External movers do the transfers. mudcat records the requests and their outcomes as activities.

## 9. Correspondence with earlier systems

| mudcat | Standard terms | Datacat | iRODS | Rucio | Rubin Butler |
|---|---|---|---|---|---|
| Zone | — | — | Zone | VO (partly) | Repository |
| Collection | — | Folder | Collection | Container DID (partly) | — |
| DataObject | `prov:Entity` | Dataset | Data object | File DID | Dataset (by type and data ID) |
| Version | `prov:specializationOf`, `dcat:version` | Dataset version | — | — | Dataset in a RUN collection |
| Replica | — | Location | Replica | Replica | Datastore record (one site) |
| StorageResource | `dcat:DataService` (endpoints) | Site | Resource | RSE | Datastore |
| Dataset / Snapshot | `dcat:Dataset`, `prov:Collection` | Group | — | Dataset DID | TAGGED or CHAINED collection; proposed collection snapshot |
| Representation | `dcat:Distribution` | — | — | — | Storage class (partly) |
| Field / structure descriptor | `csvw:Column`, `csvw:Table` | — | — | — | Dataset type with its schema |
| Metadata attribute | `csvw:Column`, `csvw:propertyUrl` | User metadata | AVU | DID metadata | Dimension record fields |
| Reference data | `csvw:foreignKey` | Foreign metadata | — | — | Dimension records |
| View | — | View (version, site) | — | — | Collection search path |
| Access control | Zanzibar-style relationships | ACL | ACL, ticket | Account permissions | Repository groups |
| Usage policy | `mc:UsageGrant`, `mc:Duty`; terms as `odrl:Policy` | — | Rules (partly) | — | — |
| Provenance | PROV-O | — | Audit (partly) | — | Provenance quantum graph |
| Specification | `prov:Plan` | — | — | — | Pipeline and quantum graph (partly) |
| Replication rule | — | — | Resource hierarchy | Replication rule | — |

## 10. Open questions

- **Element identifiers.** Should element identifiers be IRIs in all exports? This is cheap for manifests, but at 10^11 elements the RDF serialisation must stay optional.
- **Grant vocabulary.** Which actions and purposes the first release enumerates, and how much of ODRL the compiler translates automatically.
- **Snapshot hashing.** How mudcat canonicalises the snapshot hash: member order, and if the hash includes annotation sets.
- **Sample identity across rules.** If sample rules for arrays should produce stable sample identifiers, or if selectors are enough.
- **Model representation.** If a model is a data object, a dataset of checkpoints, or both.
