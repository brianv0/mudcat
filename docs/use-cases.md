# mudcat grounding use cases

Status: selected. These eight use cases ground the design. Each feature specification must serve at least one of them. A feature that serves none of them needs a new reason.

## How they were selected

Fourteen candidate use cases came from the research on AI laboratories and AI-for-science organisations. Web searches then tested each one. The first five below have the best result on three criteria:

| Criterion | Question |
|---|---|
| Evidence | Do public sources show that the problem exists, in the words of the people who have it? |
| Difference | Does the use case need something that current tools do not give in one place? |
| Reach | Can a team adopt mudcat for this without a large organisation behind the decision? |

Together, these five must also cover the model: data that is not tabular, replicas, lineage, usage policy, tenants, and the change stream with search.

**A second round** added use cases 6, 7 and 8. They come from published systems, not from searches: the Goods catalog and the study of production ML pipelines at Google, Google Dataset Search, and Unity Catalog at Databricks. These papers describe what large organisations found necessary after years of operation.

**Use case, not scale.** mudcat takes the use cases of these systems and not their scale. Goods held 26 billion entries. mudcat has a target of 10^9 versions for one tenant (see the scale envelope in [architecture.md](architecture.md)). But a laboratory with 10^7 files has the same problems as Google had: nobody knows what exists, most trained models are not used, and intermediate data stays for ever. Each use case below must work at the scale of one team.

| Use case | Evidence | Difference | Reach |
|---|---|---|---|
| 1. Curation of video and sensor data | Strong | High | Medium |
| 2. Training data for scientific models | Strong | High | High |
| 3. Rights and removal for training content | Strong | Highest | Medium |
| 4. Governed retrieval for assistants | Strong | Medium | High |
| 5. A shared facility with many small groups | Strong | Medium | Medium |
| 6. A catalog of data that nobody registered | Strong | Medium | Highest |
| 7. Continuous training pipelines | Strong | Medium | Medium |
| 8. Datasets in the public ecosystem | Strong | Medium | High |

The evidence for use cases 1 to 5 is from search results and from the research notes. Each link is a lead to read, not a proof. The evidence for use cases 6 to 8 is from papers and specifications that the project read in full.

## 1. Curation of video and sensor data for model training

**Who.** A laboratory that trains video, robotics or driving models. It has petabytes of video and sensor logs in object storage, and GPUs that are often in another cloud.

**Situation.**

- Pipelines split raw video into clips, then filter, caption and embed them. NVIDIA documents such pipelines on Ray for "100+ PB" of video, with clip embeddings for search and for the removal of duplicates.
- Robotics data packs many episodes into one file. Teams mine fleet logs for rare scenarios with embedding search.
- Captions, scores and embeddings change each week. The raw video does not change.

**What the laboratory must be able to do.**

1. Register raw video and logs where they are, with no copy.
2. Address a clip or an episode inside a file, by time span.
3. Add captions, scores and embeddings as annotation sets, with no new copy of the video.
4. Find clips by metadata, by text and by similarity.
5. Resolve a set of clips for a training job, and read them directly from storage with Ray.
6. Know which version of each annotation set a model used.
7. Have a complete, verified copy near the GPUs before the job starts.
8. Remove training shards and other caches when no job needs them, and keep the record of what they contained.

**Parts of mudcat that this use case tests.** Stream and composite structure, selectors (ST-1, ST-4, ST-5). Annotation sets and enrichers (ST-13, ST-14). Search (EV-9, EV-10). Resolution and the Ray connectors (RD-1, RD-2, RD-7, RD-8). Completeness and replication rules (PL-1, PL-2). Lifetime classes (PL-7, PR-8).

**What current tools do not give.** Table formats such as Lance hold the clips and the annotations well. They do not hold replicas on other sites, usage terms, or lineage to models.

**Evidence.** [NVIDIA: petabyte-scale video processing](https://developer.nvidia.com/blog/petabyte-scale-video-processing-with-nvidia-nemo-curator-on-nvidia-dgx-cloud/). [NVIDIA cosmos-curator](https://github.com/NVIDIA/cosmos-curator). [LeRobot dataset v3](https://huggingface.co/docs/lerobot/lerobot-dataset-v3). [The Robot Data Factory](https://arxiv.org/pdf/2609.16705). [GM: mining fleet data](https://news.gm.com/home.detail.html/Pages/news/us/en/engineering/2026/jun/0625-scaling-autonomous-systems.html).

**Consequence for the design.** Rules about jurisdiction apply to access, not only to the place of storage. The 2026 rules of China for automotive data cover data that stays in China but that a party in another country can read. Thus the resolver must also use the location of the caller (RD-9).

## 2. Training data for scientific models, from several sources and sites

**Who.** A small team that trains a model on scientific data, for example an interatomic potential. The team has a few data engineers at most. Raw results are at an HPC centre, and curated data is in a cloud.

**Situation.**

- The team combines datasets that different groups computed with different settings. A 2026 paper says that three large datasets carry settings that are not consistent, and that these give "two incompatible energy surfaces".
- A benchmark is only correct when its labels come from the same method as the training data.
- The definition of a training set often says what it must not contain, for example every structure that is also in a benchmark.
- Raw results are too large to copy. Curated data has copies in more than one place.

**What the team must be able to do.**

1. Record the method that made each value, in a form that a program can compare.
2. Combine two datasets only when a person declared that their methods are equivalent.
3. Make a training set that excludes all data that is similar to a benchmark, and prove it.
4. Keep raw data at the HPC centre and curated data in the cloud, as replicas or as representations of the same snapshot.
5. Tell, a year later, which data and which code made a model.
6. Sign in and manage the members of the project without an identity system of their own.
7. Use a public dataset from a repository together with their own data, under the licence of that dataset (see use case 8).

**Parts of mudcat that this use case tests.** Sources and method records (UP-1). Similarity criteria and leakage assertions (QY-3). Representations, replicas and storage that mudcat cannot control (PL-1, RD-4). Provenance with roles (PR-1 to PR-5, QY-1, QY-8). Snapshots and refs (ID-5, ID-7). One instance with one tenant (OP-1, OP-13).

**What current tools do not give.** Dataset hubs give versions and downloads. Workflow tools give provenance for one workflow. No tool joins the method, the copies on several sites, the benchmark exclusion and the model.

**Evidence.** [Better without U](https://arxiv.org/html/2601.21056). The research notes on AI-for-science practice, for the datasets and for the raw results that stay at an HPC centre.

**Second domain.** Weather and climate models have the same shape, with arrays in the place of structures. The reanalysis archive exists as a raw copy and as an analysis-ready copy, and one edition gets new data each month ([ARCO-ERA5](https://github.com/google-research/arco-era5), [Icechunk ERA5](https://registry.opendata.aws/earthmover-era5/)). This domain tests array structure and dataset series.

## 3. Rights and removal for training content

**Who.** An organisation that trains models on content with terms: licensed content, crawled content, the data of users, and the voices or images of people.

**Situation.**

- The EU AI Act obliges providers of general-purpose models to publish a summary of their training content. Enforcement can start in August 2026.
- Licences end. Legal commentary says that a trained model usually continues to work after the licence for its training data ends, and that practice moves to the deletion of the source data.
- People demand removal. In 2026, 25 French voice actors got 47 voice models removed from two platforms. One complaint in the United States asks for the destruction or the retraining of models.
- NIH treats the parameters of a model trained on controlled-access genomic data as a derivative of that data. The holder cannot share the model or keep it after the project ends.

**What the organisation must be able to do.**

1. Record, for each source, the permitted actions and purposes, the end date and the duties.
2. Stop a training job that would use content without permission, before the job reads data.
3. Find each copy of removed content: in derived shards, embeddings, caches and replicas, and in training sets that nobody uses now.
4. Delete those copies and keep a receipt for each one.
5. Keep content that a legal hold covers, and record why.
6. List the models that used the content. A person then decides what to do, and mudcat records the decision.
7. Make the training-content summary for a model from the records.

**Parts of mudcat that this use case tests.** Sources, usage grants, duties and terms (UP-1 to UP-6). Evaluation at resolution (UP-2, UP-3, UP-15). Invalidation, purge, erasure and legal holds (UP-7 to UP-11). Propagation of duties to models (UP-4). The content-identity index (QY-5). Reports from provenance (QY-1, QY-2, QY-6). Removal that follows derivation into intermediates and caches (UP-17), and the list of abandoned data (QY-9).

**What current tools do not give.** Catalogs have tags and access control. Experiment trackers know the inputs of a run. No open tool holds usage terms, enforces them before training, and follows a removal through derived data to models.

**Evidence.** [EU: template for the training-content summary](https://digital-strategy.ec.europa.eu/en/faqs/template-general-purpose-ai-model-providers-summarise-their-training-content). [Sidley: training data and grounding data](https://www.sidley.com/en/insights/newsupdates/2026/07/legal-implications-in-ai-development-and-deployment-training-data-and-grounding-data). [IAPP: erasure requests and model retraining](https://iapp.org/news/a/why-erasure-requests-don-t-require-model-retraining-governance-that-is-defensible). [OECD: French voice actors](https://oecd.ai/en/incidents/2026-04-02-eec9). [NIH NOT-OD-25-081](https://grants.nih.gov/grants/guide/notice-files/NOT-OD-25-081.html). The research notes on frontier-laboratory practice, for the court filings and settlements.

**Consequence for the design.** mudcat flags the models that used removed content and does not act on them. This agrees with the practice that the sources describe.

## 4. Governed retrieval for assistants

**Who.** An organisation that gives an assistant access to its documents, recordings and tables through a retrieval index.

**Situation.**

- The retrieval index is a copy. Permissions and deletions that the copy got at ingestion become stale.
- One 2026 report says that "permission-aware" retrieval is correct only "as of the last time you ran the embedding pipeline".
- When a source document is deleted, its chunks, embeddings, summaries and cached answers can stay in the index.
- The recommended remedies are propagation by events, and a check of each result against the source at the time of the query.

**What the organisation must be able to do.**

1. Describe each document once, with its permissions, its terms and its origin.
2. Feed retrieval indexes from a stream of changes, not from batch jobs.
3. Remove a document from every index when it is deleted or invalidated.
4. Check the permission of the caller again before a result is returned.
5. Refer from an answer to the exact versions of the documents that the assistant used.
6. Permit retrieval for some content and training for other content.

**Parts of mudcat that this use case tests.** The change stream, state documents and removal events (EV-1 to EV-8). Search and the check of results (EV-9 to EV-11). Access control (AC-1, AC-7). Annotation sets for chunks and embeddings. Versions and identifiers (ID-1 to ID-3).

**What current tools do not give.** Vector stores hold the embeddings. Search products hold their own copy of permissions. No tool is the source of truth that tells every index what changed and what to remove.

**Evidence.** [Your embeddings don't know the contractor was off-boarded](https://tianpan.co/blog/2026-05-22-embeddings-dont-know-contractor-was-offboarded). [WorkOS: RAG access control](https://workos.com/blog/rag-access-control-document-permissions). Most of these sources are reports from practitioners and vendors.

**Not yet in the model.** Retrieval as an action in usage grants, and a retrieval role in provenance.

## 5. A shared facility with many small groups

**Who.** A facility that serves hundreds of small groups from other organisations, for example a synchrotron, a cryo-EM centre or a sequencing centre. It can also be a service that hosts mudcat for many small laboratories.

**Situation.**

- Each group owns its data for a period. The data policy of the ESRF gives the team sole access for three years, and the period can be extended.
- A catalogue holds the metadata. Each session gets a DOI. A team can mint a DOI for the data of a publication, and that makes the data public at once.
- Some facilities keep data for a short time only. Two cryo-EM centres keep data on their servers for 7 days and for 30 days after a session.
- The groups are small and have no identity system that the facility can use.

**What the facility must be able to do.**

1. Give each group its own space, with its own members, and with no access between groups.
2. Let a group manage its members with what it has: a GitHub organisation or a similar source.
3. Apply an embargo date to each dataset, and lift it early at publication.
4. Mint a DOI for a published snapshot, and export a standard description that dataset search engines read (see use case 8).
5. Record that a copy at the facility ends, and where the group keeps its own copy.
6. Operate many groups in one installation, with a small staff.

**Parts of mudcat that this use case tests.** Tenants, the tenant resolver and shared tables (ID-11, ID-12, OP-8, OP-12, OP-13). Access control with ACL roots and conditions for each object (AC-5, AC-7). Publication, DOIs and export (ID-8, IN-4, IN-8 to IN-11). Replica states and retention. The sign-in and membership design for small groups, which is not yet in the documents.

**What current tools do not give.** Facility catalogues exist and work. They do not hold lineage to models, usage terms, or copies outside the facility, and each one serves one kind of facility.

**Evidence.** [ESRF data policy](https://www.esrf.fr/datapolicy). [ESRF DOI concepts](https://www.esrf.fr/home/UsersAndScience/UserGuide/esrf-data-policy/doi/doi---concepts.html). [UCSD cryo-EM data retention policy](https://cryoem-facility.ucsd.edu/policies/Data-Retention-Policy.pdf). [Harvard cryo-EM data policies](https://cryoem.hms.harvard.edu/sbgrid-hc2em-data-policies).

## 6. A catalog of data that nobody registered

**Who.** An organisation whose teams write data to shared storage with their own tools. No rule tells them to register it, and no team will change its pipelines for a catalog.

**Situation.**

- Google built Goods for this situation. It collects metadata "after the datasets were created, accessed, or updated", and does not interfere with owners or users.
- Most entries are not independent. Goods groups dated files, versions, shards and copies at other sites into one logical dataset, from the parts of their paths.
- More than 5% of the entries were removed each day, and as many were added. Many of them are intermediate results that connect durable datasets in lineage.
- Inference is not certain. Goods keeps each candidate schema with a score, and separates a value that it computed from a value that it propagated.
- People used the catalog in ways that its designers did not plan: to find all data with a sensitive schema, to find their own data again, and to understand old code from the record of its inputs and outputs.
- A different team built monitoring on the catalog: an alert when a dataset changes in a way that nobody expected.

**What the organisation must be able to do.**

1. Find a dataset on any storage system with one search, with no action from its owner.
2. See, for each dataset, its owner, structure, size, last change, and the jobs that read and write it.
3. See one logical dataset for many dated files, versions, shards and copies.
4. Find again a dataset whose path the person forgot.
5. List each dataset that contains a given field or schema, for an audit.
6. Follow lineage across teams: find the cause of a problem upstream, and find the users downstream.
7. Add a description or a tag that other teams see.
8. Get an alert when the size, the parts or the values of a dataset change in a way that nobody expected.
9. Claim a dataset, and thus make it subject to usage policy.

**Parts of mudcat that this use case tests.** Discovery and grouping rules (PL-8, ST-18). Inferred and propagated metadata, and enricher status (ST-19, ST-20, ST-21). Search (EV-9 to EV-11). Provenance from logs (PR-12) and through removed data (PR-8). Tags (ST-17). Expectations (ST-22, ST-23, EV-15). The unknown source (UP-16). Queries for an audit and for dependents (QY-11, QY-12).

**What current tools do not give.** Catalog products read the metadata of systems that already have a catalog, such as databases and table formats. Files in a bucket or on a file system have no such metadata. Goods solves this, but it is internal to Google, and a survey calls it "heavily tailored" to the practices there.

**Evidence.** [Goods: Organizing Google's Datasets](https://research.google/pubs/goods-organizing-googles-datasets/) (SIGMOD 2016). [Data Lakes: A Survey of Functions and Systems](https://arxiv.org/pdf/2106.09592). [Unity Catalog](https://www.databricks.com/sites/default/files/2025-06/unity-catalog-open-universal-governance-lakehouse-beyond.pdf) (SIGMOD 2025), which reports that about 16% of its tables are in a catalog of another system.

**Consequence for the design.** This is the easiest way to adopt mudcat: a team points a site agent at its storage and changes nothing else. Thus discovery must give value before a person writes any metadata.

## 7. Continuous training pipelines

**Who.** A team that trains models again and again on new data, each hour or each day, and puts some of them into service.

**Situation.**

- A study of 3000 production pipelines at Google found about 450,000 trained models in four months. About 80% of them were never deployed.
- The record of a pipeline that runs for months is one connected graph. Runs share their inputs, and one model is the start point for the next. The largest record had about 6900 nodes.
- Training used less than one third of the compute. Analysis and validation of data and models used about 35%. Ingestion used about 22%, in many cases for a copy of the input that makes the run reproducible.
- Consecutive runs shared about two thirds of their input.
- The study could not tell if a model that was not deployed was useful in another way, because nothing recorded it.

**What the team must be able to do.**

1. Record each run with no extra code in the job.
2. Get, for the model in service, exactly the data, the checks and the code that made it, and not the full history of the pipeline.
3. Train each day on the last days of data, with no copy of that data.
4. Compare the input of a run with the input of the run before, from statistics, with no read of the data.
5. Stop a run before training when its input fails a check.
6. Know which models went into service and which did not.
7. Find the runs and the data that led to nothing, and remove the bytes that can go.
8. Keep the lineage when intermediate data is removed.
9. Find each run, model and deployment that used an input that was later found to be bad.

**Parts of mudcat that this use case tests.** Run tokens (PR-4). Pipelines, runs and the run view (PR-10, PR-11). Deployments (PR-9). Dataset series and snapshots that share their manifests (ID-13). Statistics and expectations (ST-22, ST-23). Lifetime classes and stubs (PL-7, PR-8, UP-17). Use records and abandoned data (QY-9, QY-10). Dependents of a version (QY-11).

**What current tools do not give.** Experiment trackers and ML Metadata record runs and their inputs. They do not hold usage terms, the lifetime of the data that a run leaves, or the copies of that data on storage.

**Evidence.** [Production Machine Learning Pipelines: Empirical Analysis and Optimization Opportunities](https://arxiv.org/abs/2103.16007) (SIGMOD 2021). [Data Lifecycle Challenges in Production Machine Learning](https://sigmodrecord.org/2018/10/19/data-lifecycle-challenges-in-production-machine-learning-a-survey/) (SIGMOD Record 2018).

**Consequence for the design.** mudcat must record that a model went into service. Without this record, it cannot separate the data that matters from the data that nobody used.

## 8. Datasets in the public ecosystem

**Who.** A research group or a company that publishes datasets for others, and that trains on public datasets from repositories.

**Situation.**

- Dataset search engines read a Schema.org description on the landing page of each dataset. Google needs only a name and a description of 50 to 5000 characters, and recommends about fifteen more properties.
- Descriptions on the web are thin. A study of the Google Dataset Search corpus found a licence for about 34% of the datasets, a download link for 44% and a DOI for about 11%.
- Google's guidelines separate a copy of a dataset (`sameAs`) from a dataset that is based on another (`isBasedOn`). They call their advice on versions and provenance an initial outline.
- Croissant is the description that ML tools load. Version 1.1 adds provenance with PROV-O and conditions of use with ODRL.
- A public dataset can change with no new version, and it can go away.

**What the group must be able to do.**

1. Publish a snapshot with a description that mudcat generates from the catalog, with no second copy to maintain.
2. Know before publication which properties the description lacks.
3. Publish a correction, and keep each citation of the old version correct.
4. State if a dataset is a copy of another dataset or is based on it.
5. Bring a public dataset into the catalog from its description, with its licence as terms and with no copy of the bytes.
6. Train on public data and own data together, with the policy checked for both.
7. Know when a public dataset changed or went away after the group used it.

**Parts of mudcat that this use case tests.** The public description and its conformance levels (IN-4, IN-8 to IN-11). Published snapshots and identifiers (ID-2, ID-3, ID-8). Import and reference storage (IN-12). External sources and terms (UP-1, UP-2, UP-5). Checksums (ID-4).

**What current tools do not give.** Repositories publish descriptions for the data that they host. Hubs for ML data serve Croissant for their own datasets. No tool generates the description from the same record that holds the lineage and the terms, and none brings public datasets under the same policy as private data.

**Evidence.** [Google: Dataset structured data](https://developers.google.com/search/docs/appearance/structured-data/dataset). [Google Dataset Search](https://research.google/pubs/google-dataset-search-building-a-search-engine-for-datasets-in-an-open-web-ecosystem/) (WWW 2019). [Google Dataset Search by the Numbers](https://arxiv.org/pdf/2006.06894v1). [Croissant 1.1](https://docs.mlcommons.org/croissant/docs/croissant-spec-1.1.html). [DCAT 3](https://www.w3.org/TR/vocab-dcat-3/).

**Not yet in the model.** The training, validation and test splits that Croissant describes.

## What the eight use cases cover

| Part of the design | 1 | 2 | 3 | 4 | 5 | 6 | 7 | 8 |
|---|---|---|---|---|---|---|---|---|
| Data that is not tabular | Yes | Yes | | Yes | Yes | Yes | | Yes |
| Replicas and placement | Yes | Yes | Yes | | Yes | Yes | | |
| Lineage to models and evaluations | Yes | Yes | Yes | Yes | | Yes | Yes | |
| Usage policy and removal | | | Yes | Yes | Yes | Yes | Yes | Yes |
| Tenants and small groups | | Yes | | | Yes | | | |
| Change stream and search | Yes | | Yes | Yes | | Yes | | |
| Enrichers | Yes | Yes | | Yes | | Yes | Yes | |
| Discovery of data that nobody registered | | | | | | Yes | | |
| Lifetime of intermediates and caches | Yes | | Yes | | Yes | Yes | Yes | |
| Expectations on new data | | | | | | Yes | Yes | |
| Public description and import | | Yes | | | Yes | | | Yes |

## Search use cases

Search is a part of each grounding use case. [search.md](search.md) lists 17 search use cases, from an exact name to a question from an agent, with the grounding use case that each one comes from. It also gives the measures and the targets for the quality and the speed of search.

## Use cases in reserve

These use cases are real, but they are not grounding use cases at this time:

| Use case | Why it is in reserve |
|---|---|
| Clinical images across hospitals | Strong evidence, and the best test of federation and of metadata that the customer holds. But adoption needs a regulated organisation and a long approval |
| Data for single-cell models | The policy part is confirmed and is in use case 3. The search did not confirm the parts about formats and leakage |
| Laboratory work in a loop with models | The loop is well described. The search found no source for the link from a prediction to its experiment. Test it in talks with a laboratory |
| Trajectories of agents, and data from people and from models | Only commentary. No primary source tells how laboratories record this provenance |
| Custody of benchmarks | Confirmed in general. It is part of use case 2 (exclusion of a benchmark) and use case 3 (content that must not be in training) |
| Controlled research data in an enclave: CUI and export-controlled data | Strong evidence: universities operate enclaves for such research, some with high-performance computers, and contractors must meet NIST SP 800-171. [controlled-data.md](controlled-data.md) gives the design, and the requirements AC-9 to AC-11, UP-18 to UP-20, OP-23 and OP-24 come from it. It is in reserve because adoption needs the security office of an organisation, and because compliance is a property of a deployment |
| One governance layer for many engines, with filters for rows and columns | Unity Catalog shows the need. But a filter below the level of a file needs an engine that the catalog can trust, and no open standard for such an engine exists |
| Query plans from the statistics of each block, for tables of petabytes | The BigQuery paper on metadata shows the method. It is a property of a query engine at a scale that mudcat does not target. mudcat keeps statistics for plans (RD-6), but it is not the engine |
