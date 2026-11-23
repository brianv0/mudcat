# mudcat conceptual model

Status: draft. This document defines what mudcat records. How it is stored and served is in [architecture.md](architecture.md).

## Conventions

The model is expressed in RDF using existing W3C and community vocabularies, and JSON-LD is the exchange format. mudcat defines its own terms (prefix `mc:`) only where no standard term fits. RDF is the conceptual and interchange layer: the catalog stores its state relationally and publishes it through a fixed JSON-LD context.

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

Every entity has an opaque, immutable identifier: a time-ordered UUID (UUIDv7) qualified by the zone that minted it, published as an IRI. Names, paths and external identifiers are **aliases**. They are mutable, unique within their scheme, and point at an identifier.

External alias schemes include:

- the path in the collection namespace;
- Rucio `scope:name`;
- DOI;
- Hugging Face repository and revision;
- OCI digest;
- Iceberg table identifier;
- IVOA identifier.

This inverts two earlier designs: Datacat used name plus parent folder as the key, and DataHub builds URNs from platform and name strings. Renames break neither lineage nor policy.

### Immutability

Versions, snapshots, pinned specifications, policy decisions and provenance records are immutable once published. Mutable state is limited to aliases, tags, lifecycle status, replica state and access-control lists.

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

These concepts come from iRODS and Datacat; the placement concepts come from Rucio.

**Zone** (`mc:Zone`). An independently administered mudcat instance that mints identifiers and can federate with other zones (from iRODS).

**Collection** (`mc:Collection`). A node in the logical namespace. It contains data objects and other collections, and its path is an alias (iRODS collection; Datacat folder). A collection can carry inherited defaults: access-control lists, metadata and provenance (for example, "everything under `/raw/run42` was produced by instrument X").

**DataObject** (`mc:DataObject`, subclass of `prov:Entity`). A logical unit of content, independent of how many revisions or copies exist (iRODS data object; Datacat dataset; Rucio file DID). Pointers: `mc:latestVersion`, and named tags.

**Version** (`mc:Version`, subclass of `prov:Entity`).

- An immutable revision of a data object: `prov:specializationOf` its data object, with `mc:versionNumber` as an integer and `dcat:version` as a label (Datacat dataset version).
- It carries a set of content checksums, each an (algorithm, value) pair. Several algorithms are allowed because storage resources differ in what they compute: adler32, MD5, SHA-256, CRC32C, and the chunk hashes of content-defined storage.
- It also carries its size, structure descriptor, source and format.
- It records the reader or formatter needed to open it, so old data stays readable without configuration (a lesson from the Butler).

**Replica** (`mc:Replica`). One physical copy of a version on a storage resource (iRODS replica; Datacat location; Rucio replica).

- Attributes: URL or path, storage class, observed checksums, last verification time, and `mc:replicaState`.
- States: `registered` → `verified` → `suspect` → `lost` | `retired`.
- Replicas are byte-identical copies, so they are physical records rather than separate `prov:Entity` nodes.

**StorageResource** (`mc:StorageResource`). A place replicas live (iRODS resource; Rucio storage element; Datacat site).

- Resources form a hierarchy, as iRODS composable resources do: provider → account → region → bucket or file system → prefix.
- Attributes: jurisdiction, storage class (including tape and cache tiers), protocols, egress cost, and whether data may leave the resource at all. Arbitrary attributes are allowed so that Rucio-style expressions can address them.
- An optional deterministic path function maps a version to its path on the resource.
- Access endpoints are `dcat:DataService` nodes.

**View** (`mc:View`). A rule that resolves a logical name to one version and one replica, generalising Datacat's (version, site) view.

- Version selector: `latest`, a number, a tag or a snapshot.
- Location preference: a named resource, "nearest to compute location L", "cheapest verified" or "canonical".

## 2. Structure

Every version has a **structure descriptor** (`mc:StructureDescriptor`). It is extracted by a format plugin at registration, or declared by the producer. Each descriptor has one kind:

| Kind | Examples | Addressable element | Selector |
|---|---|---|---|
| `mc:Opaque` | Model weights, a PDF, a proprietary instrument file | The whole object | None |
| `mc:Container` | tar, zip, HDF5 groups, ROOT files, mmCIF | Member | Member path, byte range |
| `mc:Records` | Parquet, Lance, CSV, JSON Lines, TFRecord, database tables | Record | Key set, row range |
| `mc:Array` | Zarr, netCDF, OME-Zarr, FITS images and cubes | Chunk or region | Hyperslab over named dimensions |
| `mc:Stream` | MCAP and ROS bags, video, audio, waveforms | Message or frame | Channel and time span |
| `mc:Graph` | Knowledge graphs, meshes, interaction networks | Node, edge or subgraph | Key set |
| `mc:Composite` | A robot episode; a microscopy plate with images and a design table | A tuple of components | Component selectors joined on an alignment key |

### Fields: one concept for columns, variables and channels

The common unit of description is the **field** (`mc:Field`): a named, typed, documented, semantically annotated quantity. It covers:

- a column of a table;
- a variable of an array dataset;
- a channel of a stream;
- a metadata attribute definition (section 3).

Every field has:

| Property | Term | Notes |
|---|---|---|
| Name | `csvw:name` | Unique within its parent |
| Description | `dcterms:description` | |
| Logical datatype | `csvw:datatype` | XSD-based, with CSVW's derived types and formats |
| Physical datatype | `mc:physicalType` | Arrow type string. Needed for nested lists, structs, tensors and fixed-width binary, which CSVW datatypes do not express well |
| Required | `csvw:required` | |
| Unit | `qudt:unit` | With UCUM code. A raw unit string from the source format is kept as `mc:unitString` |
| Semantic annotation | `csvw:propertyUrl` | The property the values denote, as an IRI. In CSVW this is the RDF property a column's values are asserted with, so it is the standard's own semantic hook |
| Value identifiers | `csvw:valueUrl` | URI template for identifier-valued fields: a UniProt accession, an InChIKey, a run number in a reference table |
| Shape | `mc:shape` | For array variables and tensor-valued cells: named dimensions |

### Record structures follow CSVW

A records descriptor is a CSVW table group:

- `csvw:TableGroup` contains `csvw:Table`s.
- Each table has a `csvw:tableSchema` containing `csvw:Column`s, which are fields.
- Keys are `csvw:primaryKey`, and `csvw:foreignKey` with a `csvw:reference` to another table and its columns.

Foreign keys are how element metadata declares its joins to **reference data** (section 3).

### Felis as a profile

Rubin's Felis describes catalog schemas in YAML. It already uses JSON-LD-style `@id` keys and maps closely onto CSVW. mudcat treats Felis as an import and export **profile**, not as a separate model:

| Felis | mudcat |
|---|---|
| schema | `csvw:TableGroup` |
| table | `csvw:Table` |
| column | `csvw:Column` (a field) |
| `datatype` | `csvw:datatype`, plus `mc:physicalType` |
| `nullable` | inverse of `csvw:required` |
| `description` | `dcterms:description` |
| `ivoa:ucd` | `csvw:propertyUrl`, pointing at the UCD as an IRI. Where the IVOA publishes no IRI form, the profile defines one |
| `ivoa:unit`, `fits:tunit` | `qudt:unit`, with the original string in `mc:unitString` |
| `primaryKey` | `csvw:primaryKey` |
| ForeignKey constraint | `csvw:foreignKey` |
| Unique and check constraints, indexes | `mc:constraint`, `mc:index` (profile extensions) |
| `tap:*`, `votable:*` and per-database type overrides | Retained as profile annotations, so that TAP_SCHEMA and database DDL can be regenerated |

The same pattern applies to other community schema languages: CF conventions, Croissant record sets, AnnData obs/var schemas. Each is a profile that maps onto fields and CSVW tables, carrying extra annotations where it needs them.

### Arrays, streams and containers

- **Array descriptors** list:
  - dimensions (name, size, coordinate variable);
  - variables, which are fields with shape;
  - the chunk grid and codecs.

  Attributes follow the CF conventions: `standard_name` maps to `csvw:propertyUrl`, and `units` maps to `qudt:unit`.
- **Stream descriptors** list:
  - channels, which are fields with a message schema;
  - the time base and clock;
  - nominal rates;
  - the index needed to seek by time.
- **Container descriptors** list members, or a member pattern with a count when listing is too expensive.
- **Composite descriptors** list components and the alignment key.

### Elements and selectors

An **element** is an addressable part of a version, named by a **selector** (`mc:Selector`). A selector applied to a version is itself a resource with an IRI, formed as the version IRI plus a fragment, so metadata, provenance and policy can attach to it. Selectors reuse W3C forms where they exist:

| Selector | Standard basis |
|---|---|
| Byte range | `oa:DataPositionSelector` |
| Member path | `oa:FragmentSelector` with a member-path fragment |
| Time span on a stream or media file | Media Fragments `#t=start,end`, plus channel |
| Spatial region of an image | Media Fragments `#xywh=` |
| Key set or row range | `mc:KeySelector`, `mc:RowRangeSelector` |
| Hyperslab | `mc:HyperslabSelector`: per-dimension index or coordinate ranges |

**Elements are enumerated only where the data enumerates them.** A **manifest** (`mc:Manifest`) lists the elements of one or more versions. It is a records-structured version with a fixed schema: element identifier, source, selector, fingerprints, and lineage fields. Arrays and streams normally have no manifest: their coordinate system is the description, and elements are defined by selectors.

## 3. Metadata

Metadata follows the iRODS attribute–value–unit (AVU) form, with Datacat's typed values. Each attribute is a field.

- **Attribute definitions** (`mc:AttributeDefinition`, subclass of `mc:Field`) are registered in a namespace, giving each attribute a name, datatype, unit, semantic annotation, description and optional allowed values. An AVU becomes the triple (attribute definition IRI, typed literal, unit IRI).
- **Undeclared attributes** are accepted, stored and searchable, but are marked undeclared and are not used for range queries or sorting until declared.
- **Metadata schemas** (`mc:MetadataSchema`) group attribute definitions and say which entity classes they apply to, and which attributes are required.
  - Validation shapes are generated from them: SHACL for RDF consumers and JSON Schema for clients.
  - They can be authored in LinkML.
- **Attachment levels:**
  - collection, as inherited defaults;
  - data object, which applies to all of its versions;
  - version;
  - replica (physical facts only);
  - element, through annotation sets (section 4).
- **Assertion origin.** Each value records whether it was asserted by a person, extracted by a plugin, or inferred by a model, with a confidence where relevant (`mc:assertedBy`). Agent-extracted metadata is now common, and consumers need to tell it apart.
- **Reference data.** Reference tables of runs, exposures, instruments, calibrations, cell lines, compounds or partners are ordinary record datasets. Attributes link to them through `csvw:valueUrl` and foreign keys, and queries can join through them. This generalises Datacat's foreign metadata, the Butler's dimension records and MetaCat's external filters.

## 4. Datasets, snapshots and representations

**Dataset** (`mc:Dataset`, subclass of `dcat:Dataset`). A named, versioned set of data objects treated as a unit (Datacat group; Rucio dataset). A dataset that grows by appending over time is also a `dcat:DatasetSeries`.

**Snapshot** (`mc:Snapshot`, subclass of `prov:Entity` and `prov:Collection`). An immutable version of a dataset, linked by `dcat:hasVersion` and `dcat:previousVersion`.

- **Members** (`prov:hadMember`) are versions and annotation sets.
- **Identity.** The snapshot is identified by a Merkle hash (`mc:contentHash`) over the hashes of its members, as the Butler's collection-snapshot proposal does. Unchanged parts are shared between snapshots, and a snapshot can be verified from its identifier alone.
- **Lifecycle:** `working` (mutable, unhashed) → `published` (frozen) → `deprecated` → `withdrawn`.
  - Only published snapshots can be pinned into training-set specifications or cited.
  - A DOI can be minted for a published snapshot.

**AnnotationSet** (`mc:AnnotationSet`). An independently versioned set of derived attributes keyed by element identifier or selector: captions, labels, quality scores, embeddings, safety flags, new assay readouts. Each has its producing activity. Adding one creates a new snapshot that shares every other member with its parent. Exclusion lists (takedowns, opt-outs) are annotation sets.

**Representation** (`mc:Representation`, analogous to `dcat:Distribution`). One physical form of a snapshot, such as native HDF5, analysis-ready Zarr, Parquet, WebDataset shards or tokenized shards.

- Derivation: `mc:materializedFrom` the snapshot, with the transform parameters, which is a subproperty of `prov:wasDerivedFrom`.
- Representations of one snapshot are `prov:alternateOf` each other.
- Replicas are byte-identical only within a representation.

**Selection** (`mc:Selection`). A no-copy subset of one or more snapshots, defined by a predicate or an explicit list of elements, together with the hash of the resolved element list.

## 5. Sources, access control and usage policy

### Sources

A **source** (`mc:Source`, subclass of `prov:Entity`) records where data originated and holds its usage terms. Every version, and every element listed in a manifest, references a source. Policy is therefore evaluated over millions of sources and never over billions of elements. Each source has an acquiring activity (`prov:wasGeneratedBy`).

| Source type | Records |
|---|---|
| `mc:CrawlSource` | Domain, fetch time, crawler identity, robots and opt-out state at fetch |
| `mc:LicensedSource` | Contract, licensor, permitted uses, term and expiry, deletion obligations |
| `mc:HumanDataSource` | Campaign, vendor, pseudonymous contributor, rubric version, quality-assurance outcome |
| `mc:UserDataSource` | Consent scope, legal basis (`dpv:`), retention class |
| `mc:GeneratedSource` | Generating model, prompt version, seed, parent elements |
| `mc:InstrumentSource` | Instrument, calibration, protocol version, reagent lot, batch, planned versus executed procedure |
| `mc:ComputedSource` | Method record (section 6), code version |
| `mc:ExternalSource` | Enclave or federated partner, access conditions; possibly no reachable replica |

### Access control

Access control decides who may read, change and administer catalog entries and the data they describe. It is modelled as relationships, in the style of Google's Zanzibar, so that it can be served either by mudcat itself or by an external Zanzibar-style service (see the authorization section of [architecture.md](architecture.md)).

- **Object types:** zone, collection, data object, dataset, snapshot, specification, model, storage resource, group, compartment, ticket.
- **Relations** connect objects to subjects or to other objects, for example `collection#owner`, `collection#writer`, `collection#reader`, `collection#acl_root`, `dataset#collection`, `group#member`, `compartment#member`. A subject is a user, a service account, or a set of subjects such as `group:astro#member`.
- **Permissions** are computed from relations by union, intersection and exclusion, and by following a relation to another object. For example, read is granted to the readers, writers and owners of the collection's ACL root, intersected with membership of the object's compartment, if it has one.
- **Datacat's permission set** (read, insert, write, delete, admin) maps onto permissions in this schema. iRODS read, write and own map onto the same set.

Three rules keep the schema portable and the graph shallow:

- **ACL roots.** Datacat's rule is kept: a collection without its own ACL inherits from the nearest ancestor that has one. mudcat maintains an `acl_root` relation from every collection to that ancestor, so a check follows one hop however deep the folder tree is. External services cap resolution depth, typically at a few dozen hops.
- **Tickets.** An iRODS-style ticket becomes a relationship whose condition limits it by expiry time or use count.
- **Compartments.** A compartment is an intersection: membership of the compartment's group is required in addition to the ordinary grant. Compartments implement partner segregation and restricted evaluation sets.

**Correspondence with Datacat.** Of the systems reviewed, Datacat's ACL model is the closest to this one, and it maps directly:

- **Entries become relationships.** A Datacat entry such as `alice@astro:g:rw` on collection X becomes the relationships `collection:X#reader@group:astro/alice#member` and `collection:X#writer@group:astro/alice#member`.
- **Group lookup.** The group-manager lookup becomes the `group#member` relation.
- **Effective permissions.** The effective-permissions call becomes a check, or a lookup of subjects.

One difference is deliberate:

- **Datacat inherits by override.** A node with its own ACL replaces what it would have inherited, as in AFS.
- **Zanzibar inherits by union.** Following a parent relation adds the parent's grants to the node's own.

The `acl_root` relation preserves Datacat's override semantics. Additive grants can still be expressed explicitly where wanted.

For contrast, iRODS copies a collection's ACL onto new objects at creation time, so later changes to the collection do not apply to existing objects. Rucio and the Butler grant permissions per account or per repository.

**Ownership of relationships:**

- **Catalog-owned.** Relationships that follow from catalog structure (parent collection, ACL root, dataset membership) are owned by mudcat and synchronised to any external service.
- **Written through mudcat's API.** Grants (who is a reader or writer) are written through mudcat's API.
- **Externally owned.** Group membership may be owned by the identity provider or by the authorization service.

### Usage policy

Usage policy decides what data may be used for. It is separate from access control because permission to read is not permission to train. It has two parts, both held in the catalog database and evaluated by mudcat whichever authorization service is used.

**Usage grants** (`mc:UsageGrant`). A relation between a source and a permitted (action, purpose) pair:

- **Actions:** `mc:use`, `mc:train`, `mc:evaluate`, `mc:fineTune`, `mc:distribute`, `mc:publishDerivative`.
- **Purposes:** taken from DPV where possible, for example research, commercial research or product development.

Grants have the same shape as access relationships, for example `source:X#train@purpose:research`, so they can be mirrored to an external Zanzibar-style service for online checks. Enforcement does not depend on that mirror (see pinning, below).

**Constraints and duties** are structured records attached to sources:

- **Constraints:** expiry date, permitted jurisdictions, permitted compute locations, embargo end.
- **Duties** (`mc:Duty`): attribute, delete at close-out, honour opt-outs, notify the licensor, do not redistribute derivatives.

Duties **propagate** along `prov:wasDerivedFrom` and training usage to everything derived, models included. They accumulate. Conflicts are flagged, never resolved silently.

**Evaluation at pinning.** A training-set specification is permitted for an (action, purpose) only if every contributing source grants it and no constraint excludes it. The pinning service computes this as set operations in SQL over the sources of all components, typically thousands to millions of them, not as one check per source. It also records the accumulated duties in the policy decision.

### Terms of use

The terms themselves (what a licence or contract says) are kept separately from the compiled grants, constraints and duties. They are recorded in **ODRL 2.2** (`odrl:Policy`) where possible:

| Terms arrive as | Handling |
|---|---|
| A standard licence identifier (SPDX: `CC-BY-4.0`, `CC-BY-NC-4.0`, `Apache-2.0`, …) | Mapped to a pre-compiled set of grants, constraints and duties |
| An ODRL document: a data-space contract, a TDM Reservation Protocol opt-out policy, a RightsML licence | Compiled automatically; clauses that cannot be translated are flagged for review |
| A contract in plain text | A person records the grants and duties, and the document is attached |

- **Traceability.** Each compiled grant, constraint and duty records the clause it came from.
- **Partial compilation.** A source whose terms were only partly compiled is marked as such. Pinning blocks any purpose that depends on the untranslated part until a reviewer signs off.
- **Export.** Terms are exported with published snapshots (`odrl:hasPolicy` in DCAT, and in Croissant and RO-Crate exports) and carried by the federation feed between zones.
- **Negotiation.** A zone can publish an ODRL offer and record the accepted agreement before data is exchanged.

### Deletion and holds

- **Tombstone** (`prov:wasInvalidatedBy` an `mc:Takedown` activity). Applied to an element, version, source or snapshot, with the reason and the requesting party.
- **LegalHold** (`mc:LegalHold`). Overrides deletion for the entities in scope and moves them to a retention compartment, with the reason recorded.
- **DeletionReceipt** (`mc:DeletionReceipt`). Per replica: what was deleted, where, when and by which agent.

## 6. Provenance

Provenance uses PROV-O directly.

**Entities** are versions, snapshots, annotation sets, representations, pinned specifications, models and evaluation results.

**Activities** (subclasses of `prov:Activity`):

- `mc:Ingest`, `mc:Transform`, `mc:Training`, `mc:Evaluation`, `mc:Simulation`;
- `mc:Experiment` (physical or wet-lab);
- `mc:Takedown`.

Each activity records:

- code version;
- container image digest;
- parameters;
- start and end time;
- the storage resource or cluster where it ran. Where a run executed can be a legal fact.

**Agents** are people (`prov:Person`), services (`prov:SoftwareAgent`), instruments (`mc:Instrument`) and models. A model is an entity, and it also acts as an agent when it generates data.

**Usage roles.** `prov:used` is qualified with `prov:qualifiedUsage` and `prov:hadRole`. Roles form a small vocabulary:

- `mc:training`, `mc:validation`, `mc:test`;
- `mc:reference`, `mc:calibration`;
- `mc:baseModel`, `mc:tokenizer`, `mc:configuration`.

Roles can be marked **ubiquitous** (calibration, tokenizer, base model), and default ancestry queries exclude them. Without this, one shared calibration links every raw file to every output, as the Butler team observes.

**Derivation subproperties:** `mc:fineTunedFrom`, `mc:quantizedFrom`, `mc:distilledFrom`, `mc:materializedFrom`, all subproperties of `prov:wasDerivedFrom`.

**Plans.** Training-set specifications, protocols and pipelines are `prov:Plan`s, linked through `prov:qualifiedAssociation` and `prov:hadPlan`.

**Method records** (`mc:Method`) describe how a value was measured or computed, as structured attributes:

- for a computation: level of theory, functional, basis set, convergence criteria, code;
- for a physical experiment: instrument settings, assay protocol version.

Values from different methods are combined only under a declared equivalence (`mc:equivalentMethod`).

**Run records.** The full detail of a run (configuration, logs, per-task status, element-level usage) is a `prov:Bundle` stored as a catalogued version. The catalog keeps only the activity, its qualified usages and generations, and a searchable summary. This follows the Butler's decision to move provenance into files.

**Capture mode.** Each relation records whether it was:

- `observed`: inferred from credentials mudcat issued during the run;
- `declared`: reported by the client SDK;
- `imported`: received as OpenLineage events or from an experiment tracker.

**Element-level lineage** is carried in manifest columns: parent element identifiers, or a parent selector for arrays and streams.

**The experimental loop.** A model generates a **candidate** (`mc:Candidate`) with a canonical identity, such as a molecule, a material or a sequence. An `mc:Experiment` uses the candidate and generates results, which enter a new snapshot. Because every version is immutable, the lineage graph stays acyclic across model generations.

## 7. Training and evaluation

**Specification** (`mc:Specification`, subclass of `prov:Plan`). A declarative training or evaluation set. It contains:

- **Components:** snapshot references, or queries that resolve to snapshots or selections.
- **Filters** over metadata and annotation sets.
- **Weights.**
- **Sample rule** (`mc:SampleRule`). For records, one record. For arrays, a window over named dimensions with size, stride and offset. For images, a crop. For streams, a clip or episode boundary.
- **Ordering and curriculum.**
- **Transforms,** such as tokenizer version and normalisation.
- **Seed.**

Specifications can nest. This is the object SeqIO calls a mixture.

**PinnedSpecification** (`mc:PinnedSpecification`, subclass of `prov:Entity`). A specification frozen at a point in time:

- every component is fixed to a snapshot hash;
- every weight is a number;
- a **policy decision** (`mc:PolicyDecision`) is attached: the policies evaluated, the evaluator version, the outcome and what was excluded.

Pinning is where usage policy is enforced.

**Materialisation.** A representation generated from a pinned specification. A training activity uses a pinned specification, in the `mc:training` role, through one of its materialisations.

**Model** (`mc:Model`, subclass of `mc:DataObject`). Checkpoints and exported weights are its versions, usually opaque or container structures, linked by the derivation subproperties above.

**EvaluationSuite** (`mc:EvaluationSuite`, subclass of `mc:PinnedSpecification`). A pinned specification with a protection class: public, restricted or held by a third party. It also carries registered canary strings and an access audit requirement.

**EvaluationResult** (`mc:EvaluationResult`). Generated by an `mc:Evaluation` activity that used a model and a suite. It records metrics and their definitions.

**SimilarityCriterion** (`mc:SimilarityCriterion`). A registered, versioned rule deciding when two elements count as the same for leakage purposes:

- kinds: exact checksum, near-duplicate fingerprint, sequence identity, chemical scaffold, structure prototype, coordinate or time-range overlap, event date before a cutoff;
- plus its parameters, threshold and implementation version.

**LeakageAssertion** (`mc:LeakageAssertion`). A statement that two pinned specifications satisfy, or violate, a criterion. It carries the evidence (a similarity table or an identity-index query) and the time it was checked.

## 8. Placement

**ReplicationRule** (`mc:ReplicationRule`), from Rucio. Fields:

- a subject: a snapshot, representation, pinned specification, or a query over them;
- a storage-resource expression;
- a number of copies;
- a lifetime;
- an owner.

Rules create locks on the replicas that satisfy them. Unlocked replicas can be removed by cache eviction.

**Completeness** (`mc:CompletenessRecord`). The statement that one representation is complete and verified on one storage resource as of a given time. Placement is reported at this level, not file by file.

Transfers are carried out by external movers; mudcat records the requests and their outcomes as activities.

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
| Field / structure descriptor | `csvw:Column`, `csvw:Table` | — | — | — | Dataset type with Felis schemas |
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
- **IVOA UCD IRIs.** Which UCD IRI form should be used, and should mudcat publish one where the IVOA does not?
- **Grant vocabulary.** Which actions and purposes the first release enumerates, and how much of ODRL the compiler translates automatically.
- **Snapshot hashing.** How the snapshot hash is canonicalised: member ordering, and whether annotation sets are included.
- **Sample identity across rules.** Whether sample rules for arrays should produce stable sample identifiers, or whether selectors are enough.
- **Representing a model.** Whether a model is a data object, a dataset of checkpoints, or both.
