# mudcat requirements

Status: draft. This document states the requirements that the design in [model.md](model.md) and [architecture.md](architecture.md) depends on. The requirements are inputs for the feature specifications of the spec-driven process.

## Conventions

The requirements follow the guidance for writing requirements in the NASA Systems Engineering Handbook (NASA/SP-2016-6105 Rev 2, Appendix C):

- **"Shall"** marks a requirement. **"Will"** marks a statement of fact or intent. **"Should"** is not used.
- Each requirement states one need, in one sentence, with the system ("mudcat") as the subject.
- Each requirement is verifiable. It has one primary verification method:
  - **T**: test;
  - **A**: analysis;
  - **I**: inspection;
  - **D**: demonstration.
- Each requirement has a rationale. The rationale names the evidence or the design decision it comes from.
- A value that is not known yet is marked **TBD** (to be determined). A value that is an estimate and needs confirmation is marked **TBR** (to be resolved).
- Requirements do not use these words: "support", "user-friendly", "flexible", "minimize", "maximize", "as appropriate", "and/or", "etc.".
- Each requirement has a permanent identifier. An identifier is never reused.

The prose follows ASD Simplified Technical English, with the technical names of [model.md](model.md). Requirements that apply only to one scientific or technical field are not in this document; [fields.md](fields.md) contains field guidelines.

Identifier prefixes:

| Prefix | Area |
|---|---|
| ID | Identity and immutability |
| ST | Structure and metadata |
| PR | Provenance |
| AC | Access control |
| UP | Usage policy and deletion |
| RD | Data access and resolution |
| PL | Placement and federation |
| IN | Interfaces and standards |
| OP | Operation and deployment |
| PF | Performance and scale |
| QY | Queries the system must answer |

## Identity and immutability

| ID | Requirement | Rationale | Ver. |
|---|---|---|---|
| ID-1 | mudcat shall give each entity an immutable identifier that is a UUIDv7 qualified by the zone that minted it. | UUIDv7 keeps index locality for inserts in time order; the Butler changed to UUIDv7 for this reason. The access model controls who can see an identifier, so the embedded timestamp is not a privacy risk. | I |
| ID-2 | mudcat shall resolve each alias to exactly one identifier within the scheme of the alias. | Names, paths and external identifiers change. Lineage and policy must not break when they change. | T |
| ID-3 | mudcat shall reject a change to the content, the members or the metadata of a published version, a published snapshot, a pinned specification or a provenance record. | Immutable entities make reproduction, verification and replication across zones possible. | T |
| ID-4 | mudcat shall record a content checksum for each version, with the checksum algorithm. | Storage resources compute different algorithms; integrity checks need the algorithm with the value. | T |
| ID-5 | mudcat shall compute a Merkle hash for each published snapshot from the hashes of its members. | A party that holds the hashes can verify a snapshot without trust in the database of another zone. | T |
| ID-6 | When a zone registers a replica of a version that another zone produced, mudcat shall use the identifier that the producing zone minted. | Zones under shared custody must agree on identity without coordination at write time. | T |

## Structure and metadata

| ID | Requirement | Rationale | Ver. |
|---|---|---|---|
| ST-1 | mudcat shall record a structure descriptor of one of the kinds opaque, container, records, array, stream, graph or composite for each version. | Much scientific and multimodal data is not tabular. | I |
| ST-2 | mudcat shall describe record structures as CSVW table groups, tables and columns. | CSVW is a W3C standard; community schema languages map onto it as profiles. | I |
| ST-3 | mudcat shall import and export each registered schema profile without loss of the annotations that the profile defines. | A profile must be able to regenerate the schemas and service metadata of its community from mudcat. | T |
| ST-4 | mudcat shall address parts of a version with selectors for byte ranges, member paths, key sets, row ranges, hyperslabs and time spans. | Arrays and streams have no rows; samples are windows, crops or clips. | T |
| ST-5 | mudcat shall not need a list of elements to describe an array or a stream. | A list of every element of a 2 PB array store has no use and a high cost. | D |
| ST-6 | mudcat shall validate metadata against the metadata schemas that apply to the entity. | iRODS attribute–value–unit metadata has no types and no schemas; sites built their own conventions to compensate. | T |
| ST-7 | mudcat shall record for each metadata value if a person, a format plugin or a model asserted it. | Metadata that agents extract is now common, and users must be able to tell it apart. | T |

## Provenance

| ID | Requirement | Rationale | Ver. |
|---|---|---|---|
| PR-1 | mudcat shall record provenance with the PROV-O classes Entity, Activity and Agent and the relations used, wasGeneratedBy and wasDerivedFrom. | PROV-O is the W3C standard, and OpenLineage events map onto it. | I |
| PR-2 | mudcat shall record a role for each usage relation. | Contamination checks and ancestry queries need the role: training, validation, test, reference, calibration, base model, tokenizer or configuration. | T |
| PR-3 | mudcat shall exclude usage relations with a ubiquitous role from ancestry queries, unless the query includes them explicitly. | A shared calibration links every raw file to every output, which makes ancestry without filters useless. | T |
| PR-4 | mudcat shall record usage relations for reads that a job makes with credentials that mudcat issued under a run token. | Provenance that users must declare by hand is usually incomplete. | T |
| PR-5 | mudcat shall store the full record of a run as a catalogued version and keep a summary of its relations in the catalog database. | The Butler moved provenance into files to make schema changes easier. | I |
| PR-6 | mudcat shall import OpenLineage events as activities and relations. | Spark, Airflow, dbt and Flink emit OpenLineage events. | T |

## Access control

| ID | Requirement | Rationale | Ver. |
|---|---|---|---|
| AC-1 | mudcat shall make each access decision through the authorization port. | One interface lets mudcat or an external Zanzibar-style service serve access decisions. | I |
| AC-2 | mudcat shall provide a built-in adapter for the authorization port that needs no service other than PostgreSQL. | Small teams must be able to operate mudcat. | D |
| AC-3 | mudcat shall provide adapters for the authorization port for SpiceDB and for OpenFGA. | These are the two main open-source Zanzibar-style services, and both are available as managed services. | T |
| AC-4 | Each adapter for the authorization port shall pass the authorization conformance suite. | Adapters must give the same decisions for the same relationships. | T |
| AC-5 | mudcat shall resolve the access of a collection without its own ACL from the nearest ancestor collection that has an ACL. | This keeps the Datacat semantics and keeps each check to one hop. | T |
| AC-6 | mudcat shall deny access when the authorization port cannot give a decision. | Security functions must fail closed. | T |
| AC-7 | After mudcat removes a grant, mudcat shall deny the access that the grant gave in each check that starts after the removal. | Takedowns and revocations must take effect at once. | T |
| AC-8 | mudcat shall write each access decision at an enforcement point to an append-only audit log. | Audits and regulated users need a record of decisions. | I |

## Usage policy and deletion

| ID | Requirement | Rationale | Ver. |
|---|---|---|---|
| UP-1 | mudcat shall record a source for each version and for each element that a manifest lists. | Usage rights attach to the origin of data, not to its location in the namespace. | T |
| UP-2 | mudcat shall pin a specification for an action and a purpose only if each contributing source grants that action and purpose. | A training set is permitted only if all of its inputs permit it. | T |
| UP-3 | mudcat shall record with each pinned specification the policies it evaluated, the version of the evaluator and the result. | A user must be able to find out later why data was in a training set. | T |
| UP-4 | mudcat shall propagate the duties of a source to each entity derived from it, models included. | Licences and consent terms apply to derivatives; some data providers treat model weights as controlled derivatives. | T |
| UP-5 | mudcat shall keep the original terms document of each source and link each compiled grant, constraint and duty to the clause it came from. | Audits and disputes must trace a decision to the contract. | I |
| UP-6 | mudcat shall block the pinning of a purpose that depends on terms that the terms compiler could not translate, until a reviewer approves it. | Partial compilation must not grant more than the contract gives. | T |
| UP-7 | When mudcat receives a takedown, mudcat shall find each occurrence of the affected content in the zone and record a deletion receipt for each replica that it deletes. | A deletion must be complete and provable. | T |
| UP-8 | mudcat shall not delete data that a legal hold covers. | Preservation orders override deletion. | T |

## Data access and resolution

| ID | Requirement | Rationale | Ver. |
|---|---|---|---|
| RD-1 | mudcat shall not transfer data bytes through its own services when a client reads data. | A proxy in the read path causes GPU stalls at training scale, as Meta reported. | I |
| RD-2 | mudcat shall resolve a pinned specification to a manifest of elements, selectors and replica URLs in one request. | Batch jobs must not query the catalog once per file. | T |
| RD-3 | mudcat shall issue credentials scoped to a prefix only if the prefix is completely inside one ACL root that the caller can read. | Credentials scoped to a prefix give access to everything under that prefix. | T |
| RD-4 | mudcat shall select the replica for a read from the verified replicas that the jurisdiction and egress rules of their storage resources permit. | Data must not leave a place where law or contract keeps it. | T |

## Placement and federation

| ID | Requirement | Rationale | Ver. |
|---|---|---|---|
| PL-1 | mudcat shall record, for each representation and storage resource, if the representation is complete and verified on that storage resource. | The Butler team found no per-site completeness check in their multi-site system. | T |
| PL-2 | mudcat shall create transfer requests that satisfy the replication rules and send them to an external mover. | Movers such as FTS3 and Globus already do transfers well. | D |
| PL-3 | mudcat shall exchange only published, immutable descriptions with other zones. | Zones must not share mutable state. | I |
| PL-4 | mudcat shall make access decisions only for data on storage resources that its own zone owns. | Under federation the zone that owns the data decides. | T |
| PL-5 | mudcat shall sign each event that it sends on the federation feed. | Receiving zones must verify where an event came from. | T |

## Interfaces and standards

| ID | Requirement | Rationale | Ver. |
|---|---|---|---|
| IN-1 | mudcat shall export each entity as JSON-LD with the published mudcat context. | RDF vocabularies are the exchange model. | T |
| IN-2 | mudcat shall describe its REST interface in OpenAPI. | Clients in any language need a machine-readable contract. | I |
| IN-3 | mudcat shall accept bulk registration and return bulk resolution results in Apache Arrow format. | Registration at 10^5 versions per minute or more needs a columnar wire format. | T |
| IN-4 | mudcat shall export published snapshots as DCAT and Croissant 1.1 descriptions. | Repositories and AI tools read these formats. | T |
| IN-5 | mudcat shall serve record datasets through an Iceberg REST catalog interface. | Spark, Trino, DuckDB and PyIceberg read tables through Iceberg REST. | T |
| IN-6 | mudcat shall provide an MCP server that applies the permissions of the caller. | Agents are now common clients of catalogs. | T |
| IN-7 | mudcat shall import an existing Datacat database. | Datacat users need a migration path. | D |

## Operation and deployment

| ID | Requirement | Rationale | Ver. |
|---|---|---|---|
| OP-1 | mudcat shall operate with PostgreSQL as the only service that it needs. | Small teams must be able to operate mudcat. | D |
| OP-2 | mudcat shall rebuild each derived index from the catalog database and the registered manifests. | No state may exist only in a derived index; DataHub's recovery problems show the risk. | D |
| OP-3 | mudcat shall rebuild a derived index while the old index continues to serve queries. | Offline reindexing was the main cause of DataHub upgrade failures. | D |
| OP-4 | mudcat shall commit catalog rows for a registration before it reports the registration as complete. | Producers must not delete a staging copy that the catalog does not record. | T |
| OP-5 | mudcat shall accept a repeated registration of the same data object and checksum without a duplicate entry. | Batch jobs retry. | T |

## Performance and scale

| ID | Requirement | Rationale | Ver. |
|---|---|---|---|
| PF-1 | mudcat shall hold at least 10^9 (TBR) versions and replicas in one zone. | Frontier labs and large science collaborations reach this scale; Postgres-first catalogs report 10^8. | A |
| PF-2 | mudcat shall index at least 10^11 (TBR) elements in one zone through manifests. | Web corpora and packed multimodal datasets reach this scale. | A |
| PF-3 | mudcat shall register at least TBD versions per minute through bulk registration. | The Butler measured registration delays of up to ten minutes with hundreds of concurrent writers. | T |
| PF-4 | mudcat shall return the first page of a metadata query over 10^8 (TBR) versions in less than TBD seconds. | Interactive search must stay usable at science scale. | T |

## Queries the system must answer

Each of these queries shall be possible as one request to mudcat. The verification method for all of them is test (T), with a reference data set. The rationale for each is in the design report `reports/Multimodal data catalog design.md` in the research repository.

| ID | Requirement |
|---|---|
| QY-1 | mudcat shall list the models that used data from a given source in training, with the number of elements for each model. |
| QY-2 | mudcat shall produce the training-content summary for a given model. |
| QY-3 | mudcat shall report if an evaluation suite has elements that are related, under a given similarity criterion, to the elements of the pinned training specification of a given model. |
| QY-4 | mudcat shall list the storage resources on which a given pinned specification is complete and verified. |
| QY-5 | mudcat shall list each occurrence of given content, in each dataset, representation and storage resource of the zone. |
| QY-6 | mudcat shall list the specifications, models and caches that use data whose licence expires before a given date. |
| QY-7 | mudcat shall list the candidates that a given model generated, the experiments that used them, and the models that used the results of those experiments in training. |
| QY-8 | mudcat shall resolve the pinned specification of a given model to its manifest and report the elements that usage policy now excludes. |
