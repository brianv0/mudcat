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
| ID-1 | mudcat shall give each entity an immutable identifier that is a UUIDv7 qualified by the instance that minted it. | UUIDv7 keeps index locality for inserts in time order; the Butler changed to UUIDv7 for this reason. The access model controls who can see an identifier, so the embedded timestamp is not a privacy risk. | I |
| ID-2 | mudcat shall resolve each alias to exactly one identifier within the scheme of the alias. | Names, paths and external identifiers change. Lineage and policy must not break when they change. | T |
| ID-3 | mudcat shall reject a change to the content, the members or the metadata of a published version, a published snapshot, a resolution or a provenance record. | Immutable entities make reproduction, verification and replication across instances possible. | T |
| ID-4 | mudcat shall record a content checksum for each version, with the checksum algorithm. | Storage resources compute different algorithms; integrity checks need the algorithm with the value. | T |
| ID-5 | mudcat shall compute a Merkle hash for each published snapshot from the hashes of its members. | A party that holds the hashes can verify a snapshot without trust in the database of another instance. | T |
| ID-6 | When an instance registers a replica of a version that another instance produced, mudcat shall use the identifier that the producing instance minted. | Instances under shared custody must agree on identity without coordination at write time. | T |
| ID-7 | mudcat shall move a branch only if the branch points to the snapshot that the request states. | Concurrent writers must not lose each other's changes; Iceberg commits use the same optimistic concurrency. | T |
| ID-8 | mudcat shall keep each published snapshot until a person withdraws it. | Citations refer to published snapshots. | T |
| ID-9 | mudcat shall expire a snapshot that is not published only if no ref and no resolution refers to it. | Snapshots that no process uses must not use storage for ever, but expiry must not break a reference. | T |
| ID-10 | mudcat shall make, on request, a snapshot of the versions that a collection subtree held at a given time. | Users who work by folder need citable, verifiable states of a folder; the Butler proposes the same operation. | T |

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
| ST-8 | mudcat shall give each field an identifier that does not change when the field gets a new name. | Schema evolution and annotation sets must not break when a column gets a new name; Iceberg uses field IDs for the same reason. | T |
| ST-9 | mudcat shall reject a write to an attribute namespace from a principal that the namespace owner did not name. | Customer systems such as laboratory systems must stay the only writers of their metadata. | T |
| ST-10 | mudcat shall store values of a customer-held attribute only in the customer's environment. | Some metadata, such as consent status, must not leave the customer's environment. | I |
| ST-11 | mudcat shall evaluate query predicates on customer-held attributes only through the customer gateway. | The control plane must give correct results without access to the values. | T |
| ST-12 | mudcat shall record a policy decision that depends on customer-held attributes without the values of those attributes. | Audit needs the decision, but the values must stay with the customer. | T |
| ST-13 | mudcat shall let an enricher write only attributes, tags and annotation sets in namespaces that the enricher owns. | Two plugins must not overwrite each other's metadata. | T |
| ST-14 | mudcat shall record the enricher and its version with each value that an enricher writes. | Users must tell computed metadata from asserted metadata, and find values that an old plugin version computed. | T |
| ST-15 | mudcat shall hide an enricher output that needs review until a person accepts it. | AI-generated descriptions and classifications can be wrong. | T |
| ST-16 | mudcat shall run enrichers from third parties only in site agents or in the customer gateway. | Third-party code must not run in the control plane, and must run under the customer's grants. | I |
| ST-17 | mudcat shall apply a tag on a collection to each entity under it that does not set another value of that tag. | Classification of a whole collection must not need a write to each object. | T |

## Provenance

| ID | Requirement | Rationale | Ver. |
|---|---|---|---|
| PR-1 | mudcat shall record provenance with the PROV-O classes Entity, Activity and Agent and the relations used, wasGeneratedBy and wasDerivedFrom. | PROV-O is the W3C standard, and OpenLineage events map onto it. | I |
| PR-2 | mudcat shall record a role for each usage relation. | Contamination checks and ancestry queries need the role: training, validation, test, reference, calibration, base model, tokenizer or configuration. | T |
| PR-3 | mudcat shall exclude usage relations with a ubiquitous role from ancestry queries, unless the query includes them explicitly. | A shared calibration links every raw file to every output, which makes ancestry without filters useless. | T |
| PR-4 | mudcat shall record usage relations for reads that a job makes with credentials that mudcat issued under a run token. | Provenance that users must declare by hand is usually incomplete. | T |
| PR-5 | mudcat shall store the full record of a run as a catalogued version and keep a summary of its relations in the catalog database. | The Butler moved provenance into files to make schema changes easier. | I |
| PR-6 | mudcat shall import OpenLineage events as activities and relations. | Spark, Airflow, dbt and Flink emit OpenLineage events. | T |
| PR-7 | mudcat shall record each compaction or rewrite of data files as an activity that generates a new representation or a redacted successor snapshot. | Files are immutable versions with lineage, so a silent rewrite would break provenance and verification. | T |

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
| UP-2 | mudcat shall resolve data for an action and a purpose only if each contributing source grants that action and purpose. | A training set is permitted only if all of its inputs permit it. | T |
| UP-3 | mudcat shall record with each resolution the exact versions of its inputs, the policies it evaluated, the version of the evaluator and the result. | A user must be able to find out later why data was in a training set. | T |
| UP-4 | mudcat shall propagate the duties of a source to each entity derived from it, models included. | Licences and consent terms apply to derivatives; some data providers treat model weights as controlled derivatives. | T |
| UP-5 | mudcat shall keep the original terms document of each source and link each compiled grant, constraint and duty to the clause it came from. | Audits and disputes must trace a decision to the contract. | I |
| UP-6 | mudcat shall block a resolution for a purpose that depends on terms that the terms compiler could not translate, until a reviewer approves it. | Partial compilation must not grant more than the contract gives. | T |
| UP-7 | When mudcat receives a takedown, mudcat shall find each occurrence of the affected content in the instance and record a deletion receipt for each replica that it deletes. | A deletion must be complete and provable. | T |
| UP-8 | mudcat shall not purge or erase data that a legal hold covers. | Preservation orders override deletion. | T |
| UP-9 | mudcat shall exclude invalidated content from each resolution, for snapshots made before and after the invalidation. | A takedown must take effect at once, also for training sets that exist. | T |
| UP-10 | mudcat shall keep the Merkle hash of a snapshot after it purges content of that snapshot. | The hash proves what the snapshot contained. | T |
| UP-11 | When mudcat erases an entity, mudcat shall keep only its identifier, the date, the reason and, unless the hash can identify a person, its content hash. | Erasure requests can cover metadata; the erasure record must not keep what the request removes. | T |
| UP-12 | mudcat shall apply the access control, usage policy and invalidation set that are in effect now to each as-of resolution. | Time travel must not give back access that a takedown or revocation removed. | T |
| UP-13 | mudcat shall keep the transaction-time history of refs, aliases, ACLs, grants, replica states and lifecycle status. | Old scripts and audits must resolve names and decisions as they were. | T |
| UP-14 | mudcat shall return an error that states the expiry when an as-of resolution needs an expired snapshot. | A silent resolution to another snapshot would break reproducibility. | T |
| UP-15 | mudcat shall accept a manifest at resolution without a check of each element only if every source that the manifest uses permits the requested action and purpose, and no entry is in the invalidation set. | A check of each element is too slow at 10^11 elements, but a shortcut must not let excluded data through. | T |

## Data access and resolution

| ID | Requirement | Rationale | Ver. |
|---|---|---|---|
| RD-1 | mudcat shall not transfer data bytes through its own services when a client reads data. | A proxy in the read path causes GPU stalls at training scale, as Meta reported. | I |
| RD-2 | mudcat shall resolve the inputs of a job to a manifest of elements, selectors and replica URLs in one request. | Batch jobs must not query the catalog once per file. | T |
| RD-3 | mudcat shall issue credentials scoped to a prefix only if the prefix is completely inside one ACL root that the caller can read. | Credentials scoped to a prefix give access to everything under that prefix. | T |
| RD-4 | mudcat shall select the replica for a read from the verified replicas that the jurisdiction and egress rules of their storage resources permit. | Data must not leave a place where law or contract keeps it. | T |
| RD-5 | When an instance uses the credential-broker mode, mudcat shall issue storage credentials only through a broker that runs in the customer's environment. | Regulated customers of a hosted service must keep the vendor without access to their bytes. | T |
| RD-6 | mudcat shall skip a manifest or a file during planning only if its statistics prove that no element inside it matches the request. | Statistics must make plans faster without a change to their results. | T |
| RD-7 | mudcat shall return, with each task of a resolution plan, the expected bytes, the expected number of elements and the replica location. | Compute engines need these values to size and place their tasks. | T |
| RD-8 | mudcat shall provide read and write connectors for Ray Data that pass the run token of the job. | Lineage must come from the jobs that use the data, without extra code in each job. | D |

## Placement and federation

| ID | Requirement | Rationale | Ver. |
|---|---|---|---|
| PL-1 | mudcat shall record, for each representation and storage resource, if the representation is complete and verified on that storage resource. | The Butler team found no per-site completeness check in their multi-site system. | T |
| PL-2 | mudcat shall create transfer requests that satisfy the replication rules and send them to an external mover. | Movers such as FTS3 and Globus already do transfers well. | D |
| PL-3 | mudcat shall exchange only published, immutable descriptions with other instances. | Instances must not share mutable state. | I |
| PL-4 | mudcat shall make access decisions only for data on storage resources that its own instance owns. | Under federation the instance that owns the data decides. | T |
| PL-5 | mudcat shall sign each event that it sends on the federation feed. | Receiving instances must verify where an event came from. | T |
| PL-6 | mudcat shall report each file on a storage resource that no replica record describes, and each replica record whose file is missing. | Dark data and lost replicas make the catalog wrong; Rucio runs the same consistency checks. | T |

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
| OP-5 | mudcat shall accept a repeated registration of the same artifact and checksum without a duplicate entry. | Batch jobs retry. | T |
| OP-6 | mudcat shall seal unsealed member rows into manifest files without a change to the members or the Merkle hash of any snapshot. | Maintenance must not change what a snapshot contains. | T |
| OP-7 | mudcat shall run the same release as a hosted service and as a self-hosted installation. | Customers must be able to move between hosted and self-hosted operation. | I |
| OP-8 | mudcat shall keep the catalog of each instance in a database or schema that holds no other tenant's data. | Tenant isolation, recovery and deletion are simpler and safer per instance. | I |
| OP-9 | mudcat shall connect site agents to the control plane only through connections that the agents open. | Customers must not need to open inbound ports. | D |
| OP-10 | mudcat shall export a complete instance in a form that the self-hosted release can import. | Customers must be able to leave the hosted service without loss. | T |
| OP-11 | mudcat shall keep the metadata of each instance in the region that the instance selects. | Metadata can be sensitive and subject to residency rules. | I |

## Performance and scale

| ID | Requirement | Rationale | Ver. |
|---|---|---|---|
| PF-1 | mudcat shall hold at least 10^9 (TBR) versions and replicas in one instance. | Frontier labs and large science collaborations reach this scale; Postgres-first catalogs report 10^8. | A |
| PF-2 | mudcat shall index at least 10^11 (TBR) elements in one instance through manifests. | Web corpora and packed multimodal datasets reach this scale. | A |
| PF-3 | mudcat shall register at least TBD versions per minute through bulk registration. | The Butler measured registration delays of up to ten minutes with hundreds of concurrent writers. | T |
| PF-4 | mudcat shall return the first page of a metadata query over 10^8 (TBR) versions in less than TBD seconds. | Interactive search must stay usable at science scale. | T |
| PF-5 | mudcat shall commit a change to a dataset in less than TBD milliseconds, with TBD concurrent writers on the same branch. | Iceberg commits take seconds because they write metadata files to object storage; mudcat commits only change database rows. | T |

## Queries the system must answer

Each of these queries shall be possible as one request to mudcat. The verification method for all of them is test (T), with a reference data set. The rationale for each is in the design report `reports/Multimodal data catalog design.md` in the research repository.

| ID | Requirement |
|---|---|
| QY-1 | mudcat shall list the models that used data from a given source in training, with the number of elements for each model. |
| QY-2 | mudcat shall produce the training-content summary for a given model. |
| QY-3 | mudcat shall report if an evaluation dataset has elements that are related, under a given similarity criterion, to the elements of the training inputs of a given model. |
| QY-4 | mudcat shall list the storage resources on which a given snapshot or representation is complete and verified. |
| QY-5 | mudcat shall list each occurrence of given content, in each dataset, representation and storage resource of the instance. |
| QY-6 | mudcat shall list the runs, models and caches that use data whose licence expires before a given date. |
| QY-7 | mudcat shall list the candidates that a given model generated, the experiments that used them, and the models that used the results of those experiments in training. |
| QY-8 | mudcat shall resolve again the training inputs of a given model and report the elements that usage policy now excludes. |
