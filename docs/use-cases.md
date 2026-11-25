# mudcat grounding use cases

Status: selected. These five use cases ground the design. Each feature specification must serve at least one of them. A feature that serves none of them needs a new reason.

## How they were selected

Fourteen candidate use cases came from the research on AI laboratories and AI-for-science organisations. Web searches then tested each one. The five below have the best result on three criteria:

| Criterion | Question |
|---|---|
| Evidence | Do public sources show that the problem exists, in the words of the people who have it? |
| Difference | Does the use case need something that current tools do not give in one place? |
| Reach | Can a team adopt mudcat for this without a large organisation behind the decision? |

Together, the five must also cover the model: data that is not tabular, replicas, lineage, usage policy, tenants, and the change stream with search.

| Use case | Evidence | Difference | Reach |
|---|---|---|---|
| 1. Curation of video and sensor data | Strong | High | Medium |
| 2. Training data for scientific models | Strong | High | High |
| 3. Rights and removal for training content | Strong | Highest | Medium |
| 4. Governed retrieval for assistants | Strong | Medium | High |
| 5. A shared facility with many small groups | Strong | Medium | Medium |

The evidence is from search results and from the research notes. Each link is a lead to read, not a proof.

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

**Parts of mudcat that this use case tests.** Stream and composite structure, selectors (ST-1, ST-4, ST-5). Annotation sets and enrichers (ST-13, ST-14). Search (EV-9, EV-10). Resolution and the Ray connectors (RD-1, RD-2, RD-7, RD-8). Completeness and replication rules (PL-1, PL-2).

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
3. Find each copy of removed content: in derived shards, embeddings, caches and replicas.
4. Delete those copies and keep a receipt for each one.
5. Keep content that a legal hold covers, and record why.
6. List the models that used the content. A person then decides what to do, and mudcat records the decision.
7. Make the training-content summary for a model from the records.

**Parts of mudcat that this use case tests.** Sources, usage grants, duties and terms (UP-1 to UP-6). Evaluation at resolution (UP-2, UP-3, UP-15). Invalidation, purge, erasure and legal holds (UP-7 to UP-11). Propagation of duties to models (UP-4). The content-identity index (QY-5). Reports from provenance (QY-1, QY-2, QY-6).

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
4. Mint a DOI for a published snapshot, and export a standard description.
5. Record that a copy at the facility ends, and where the group keeps its own copy.
6. Operate many groups in one installation, with a small staff.

**Parts of mudcat that this use case tests.** Tenants, the tenant resolver and shared tables (ID-11, ID-12, OP-8, OP-12, OP-13). Access control with ACL roots and conditions for each object (AC-5, AC-7). Publication, DOIs and export (ID-8, IN-4). Replica states and retention. The sign-in and membership design for small groups, which is not yet in the documents.

**What current tools do not give.** Facility catalogues exist and work. They do not hold lineage to models, usage terms, or copies outside the facility, and each one serves one kind of facility.

**Evidence.** [ESRF data policy](https://www.esrf.fr/datapolicy). [ESRF DOI concepts](https://www.esrf.fr/home/UsersAndScience/UserGuide/esrf-data-policy/doi/doi---concepts.html). [UCSD cryo-EM data retention policy](https://cryoem-facility.ucsd.edu/policies/Data-Retention-Policy.pdf). [Harvard cryo-EM data policies](https://cryoem.hms.harvard.edu/sbgrid-hc2em-data-policies).

## What the five use cases cover

| Part of the design | 1 | 2 | 3 | 4 | 5 |
|---|---|---|---|---|---|
| Data that is not tabular | Yes | Yes | | Yes | Yes |
| Replicas and placement | Yes | Yes | Yes | | Yes |
| Lineage to models and evaluations | Yes | Yes | Yes | Yes | |
| Usage policy and removal | | | Yes | Yes | Yes |
| Tenants and small groups | | Yes | | | Yes |
| Change stream and search | Yes | | Yes | Yes | |
| Enrichers | Yes | Yes | | Yes | |

## Use cases in reserve

These use cases are real, but they are not grounding use cases at this time:

| Use case | Why it is in reserve |
|---|---|
| Clinical images across hospitals | Strong evidence, and the best test of federation and of metadata that the customer holds. But adoption needs a regulated organisation and a long approval |
| Data for single-cell models | The policy part is confirmed and is in use case 3. The search did not confirm the parts about formats and leakage |
| Laboratory work in a loop with models | The loop is well described. The search found no source for the link from a prediction to its experiment. Test it in talks with a laboratory |
| Trajectories of agents, and data from people and from models | Only commentary. No primary source tells how laboratories record this provenance |
| Custody of benchmarks | Confirmed in general. It is part of use case 2 (exclusion of a benchmark) and use case 3 (content that must not be in training) |
