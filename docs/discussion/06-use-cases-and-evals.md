# Design discussion, part 06: Use cases and evals

This file is a record of the design discussion: each question, and the answer as it was given at that time. Later decisions can replace what an answer says. The documents in `docs/` hold the present design, and `docs/open-items.md` holds what is not yet in them. Paths under `/tmp` and commit IDs in the answers refer to the state at that time.

## 101. Brainstorm and/ir research at least 10 use cases for this system. ignore anything from dat

**Question**

> Brainstorm and/ir research at least 10 use cases for this system. ignore anything from datacat or Fermi telescope in particular, as well as LSST

**Answer**

Here are 14 use cases, drawn from the two research passes on frontier-lab and AI-for-science practice plus my own brainstorming. Each is marked **(evidence)** where our notes have public evidence that the problem exists, or **(brainstorm)** where it's my inference. None is tied to Datacat, Fermi or LSST.

#### AI industry

1. **Video model data curation (evidence).** A video lab holds petabytes of clips with captions, quality scores and embeddings that change weekly.
   - *Needs:* clip-level selectors, annotation layers that don't rewrite video, licence terms per partner, staging to GPUs in another cloud.
2. **Robotics episode data (evidence).** Fleets add thousands of hours a week, packed many episodes per file, mixed with simulation data.
   - *Needs:* composite structure (video, sensors, actions aligned by time), dataset series with daily snapshots, lineage from episode to policy to evaluation.
3. **Licensed-content compliance (evidence).** A model developer must honour licences that expire, opt-outs, takedowns and legal holds, and report training content to regulators.
   - *Needs:* usage rights on sources, invalidation that reaches derived shards and models, deletion receipts, "find every copy".
4. **Driving and sensor log mining (evidence).** A team mines rare scenarios from hundreds of petabytes of logs, under data-localisation rules.
   - *Needs:* embedding search over clips, replicas pinned to jurisdictions, purge of personal data with proof.
5. **Agent and RL trajectories, human and synthetic data (evidence).** Labs want canonical trajectory datasets and to know which model or vendor produced each sample.
   - *Needs:* typed sources (generator model, vendor campaign), stream and episode structure, contamination tracking.
6. **Voice and likeness consent (brainstorm).** A speech company must withdraw a speaker's data, and know which models heard it, when consent is revoked.
   - *Needs:* consent as a tag with propagation, retroactive invalidation, model flags.
7. **Governed retrieval for assistants (brainstorm).** A company feeds documents, recordings and tables to a RAG system.
   - *Needs:* chunks as elements, embeddings as annotation sets, permission-trimmed search, removal events that clear external vector stores, citations back to exact versions.

#### AI for science

8. **Lab-in-the-loop drug discovery (evidence).** Microscopy plates and assays feed models whose candidates go back to the wet lab.
   - *Needs:* assay and protocol versions, batch and control links, partner compartments, the model → candidate → experiment → dataset loop.
9. **Single-cell foundation models (evidence).** Atlases of 10^8 cells arrive as dated releases in several formats, some under controlled access.
   - *Needs:* linked representations, donor-level leakage checks, consent duties that follow into model weights.
10. **Materials and chemistry potentials (evidence).** Training mixes simulation datasets computed with different settings; raw outputs stay at an HPC centre.
    - *Needs:* method records, declared equivalence before merging, exclusion against a benchmark, replicas across HPC and cloud.
11. **Weather and climate models (evidence).** A multi-petabyte archive exists as raw, cloud-optimised and analysis-ready arrays, appended monthly.
    - *Needs:* array structure without element lists, window sample rules, time-based test splits, representations of one snapshot.
12. **Clinical imaging across hospitals (evidence for the rules; brainstorm for the fit).** Images can't leave each hospital, and metadata contains patient information.
    - *Needs:* federation between tenants, customer-held metadata, reference storage, aggregate-only release rules, audit trails.
13. **Shared instrument facility (brainstorm).** A cryo-EM or light-source facility serves hundreds of small outside groups with embargo periods.
    - *Needs:* GitHub or ORCID onboarding, a tenant or project per group, embargo dates as per-object conditions, DOIs at publication.

#### Cross-cutting

14. **Benchmark custody (evidence).** Someone holds evaluation sets that must never enter training, and must prove it.
    - *Needs:* protection tags, canaries, similarity criteria and leakage assertions, audited access, a gate before results are accepted.

#### What they stress in the design

| Capability | Use cases |
|---|---|
| Usage rights, invalidation, receipts | 3, 4, 6, 7, 9, 12 |
| Non-tabular structure and selectors | 1, 2, 4, 5, 11 |
| Lineage through models and evaluations | 2, 5, 8, 10, 14 |
| Replicas and placement across sites | 1, 4, 10, 11, 12 |
| Tenancy, federation, onboarding | 8, 12, 13 |
| Search and enrichment | 1, 4, 7 |

#### Where I'd anchor

- **Use case 8 or 10 as the first real user.** AI-for-science startups have the clearest gap and small teams that adopt tools.
- **Use case 2 for the multimodal side.** Robotics labs already use open components and lack this layer.
- **Use case 3 as the wedge that sells it.** Compliance is what makes a catalog mandatory instead of nice to have, and nothing open covers it.

These aren't in the repository. They would be useful as a `docs/use-cases.md`, since each one can become the scenario for a feature specification.

## 102. when finished, conduct searches to help validate those use cases

**Question**

> when finished, conduct searches to help validate those use cases

**Answer**

I ran 17 searches across the 14 use cases. Eleven are supported by public evidence, though three of those only in part, and two are weak on the specific claim I made. One needs reframing, and two findings change the design. The searches return summaries of pages, so treat each as a lead to read, not a proof.

#### Verdict by use case

| # | Use case | Verdict | What the search found |
|---|---|---|---|
| 1 | Video curation | Supported | NVIDIA documents Ray-based pipelines for "100+ PB" of video, with clip embeddings for search and deduplication and captioning stages. Its guidance flags licensing and redistribution |
| 2 | Robotics episodes | Partly | LeRobot v3 packs many episodes per file. A 2026 paper argues for persistent "data factory" infrastructure with reproducibility and governance. No fleet volumes found |
| 3 | Licensed-content compliance | Partly | The EU training-content summary is real, with enforcement possible from August 2026. I found no public deal terms on deletion |
| 4 | Driving log mining | Supported, with a correction | GM, Applied Intuition and AWS describe embedding search over fleet logs. China's 2026 automotive rules exist, but see below |
| 5 | Agent trajectories and data provenance | Weak | Only commentary and vendor material. No primary source on how labs record provenance |
| 6 | Voice consent | Reframe | The evidence is lawsuits and takedowns over *unconsented* use, not consent withdrawal. One complaint seeks destruction or retraining of models, and 25 French voice actors got 47 cloned voice models removed |
| 7 | Governed retrieval | Supported | Several 2026 write-ups describe the exact failure: permissions and deletions copied into a vector index go stale |
| 8 | Lab-in-the-loop | Weak on my claim | The predict–test–feed-back loop and fragmented assay data are well described. Nothing addressed lineage from prediction to experiment |
| 9 | Single-cell models | Partly | NIH confirms model weights from controlled-access data are "Data Derivatives" that can't be shared or kept after close-out. Formats and donor leakage weren't covered |
| 10 | Materials potentials | Supported | A 2026 paper says MPtrj, Alexandria and OMat24 carry inconsistent settings giving "two incompatible energy surfaces". Another says benchmarks need labels made with the same method as the training data |
| 11 | Weather models | Partly | ARCO-ERA5 exists as raw-converted and analysis-ready Zarr, and an Icechunk edition is updated monthly. Volumes and test-year splits weren't covered |
| 12 | Clinical imaging | Supported | A 2026 radiology project linked imaging metadata, outcomes and AI measurements "without centralizing the underlying imaging studies". A federated network trains models inside each hospital |
| 13 | Shared instrument facility | Supported | ESRF's policy gives the team sole access for three years, keeps metadata in a catalogue, and mints a DOI per session. A team-minted DOI makes those datasets public at once |
| 14 | Benchmark custody | Partly | Canaries and private held-out sets are standard, and private sets "rely on trust in a single organization". Nothing on specific incidents |

#### Two findings that change the design

- **Jurisdiction is about access, not only location (use case 4).** China's rules govern export, and they cover data stored in China but made accessible to parties abroad. mudcat's jurisdiction attribute on storage isn't enough: the resolver also has to consider where the *caller* is.
- **Licence expiry doesn't reach the model (use case 3).** Legal commentary says a trained model generally keeps working after a training-data licence lapses, and that practice is moving toward deleting source data, not retraining. That supports the current design, which flags affected models for a human decision and doesn't act on them automatically.

#### What the searches confirm about the design

- **Use case 7** matches the change stream and search design closely. The recommended fixes are event-driven propagation and re-checking candidates against the source at query time, which is what we specified.
- **Use case 13** gives a concrete model for embargo: a per-object date, renewable, lifted early by publication.
- **Use case 10** confirms method records and "declared equivalence before merging" as real needs.

#### What to do with the weak ones

- **5 and 8:** keep them as hypotheses, and test them in conversations with a lab, not with more searching.
- **6:** restate it as "remove a person's voice or likeness and identify affected models", which is what the evidence shows.

Sources:
- [NVIDIA: petabyte-scale video processing](https://developer.nvidia.com/blog/petabyte-scale-video-processing-with-nvidia-nemo-curator-on-nvidia-dgx-cloud/)
- [NVIDIA cosmos-curator](https://github.com/NVIDIA/cosmos-curator)
- [NVIDIA Cosmos cookbook: data curation](https://nvidia-cosmos.github.io/cosmos-cookbook/core_concepts/data_curation/overview.html)
- [LeRobot dataset v3 documentation](https://huggingface.co/docs/lerobot/lerobot-dataset-v3)
- [The Robot Data Factory (arXiv)](https://arxiv.org/pdf/2609.16705)
- [EU: template for the training-content summary, FAQ](https://digital-strategy.ec.europa.eu/en/faqs/template-general-purpose-ai-model-providers-summarise-their-training-content)
- [Sidley: training data and grounding data](https://www.sidley.com/en/insights/newsupdates/2026/07/legal-implications-in-ai-development-and-deployment-training-data-and-grounding-data)
- [IAPP: erasure requests and model retraining](https://iapp.org/news/a/why-erasure-requests-don-t-require-model-retraining-governance-that-is-defensible)
- [GM: scaling world understanding for autonomous systems](https://news.gm.com/home.detail.html/Pages/news/us/en/engineering/2026/jun/0625-scaling-autonomous-systems.html)
- [Applied Intuition: mining massive autonomy datasets](https://www.appliedintuition.com/blog/ai-for-mining-massive-autonomy-datasets)
- [DLA Piper: China automotive data transfer guidance](https://privacymatters.dlapiper.com/2026/02/china-new-guidance-on-data-transfer-and-identification-of-important-data-in-the-automotive-sector/)
- [Mondaq: China's 2026 automotive data guidelines](https://www.mondaq.com/china/data-protection/1803032/chinas-2026-automotive-data-cross-border-transfer-guidelines)
- [OECD AI incidents: French voice actors](https://oecd.ai/en/incidents/2026-04-02-eec9)
- [Bloomberg Law: lawsuits over AI voice training](https://news.bloomberglaw.com/ip-law/adobe-latest-target-in-wave-of-lawsuits-over-ai-voice-training)
- [Your embeddings don't know the contractor was off-boarded](https://tianpan.co/blog/2026-05-22-embeddings-dont-know-contractor-was-offboarded)
- [WorkOS: RAG access control](https://workos.com/blog/rag-access-control-document-permissions)
- [GEN: data infrastructure in drug discovery](https://www.genengnews.com/topics/artificial-intelligence/why-data-infrastructure-determines-ai-success-in-drug-discovery/)
- [MIT Technology Review: closing the data loop](https://www.technologyreview.com/2026/07/27/1139667/closing-the-data-loop-in-ai-driven-drug-discovery/amp/)
- [NIH NOT-OD-25-081](https://grants.nih.gov/grants/guide/notice-files/NOT-OD-25-081.html)
- [Better without U (arXiv)](https://arxiv.org/html/2601.21056)
- [ARCO-ERA5](https://github.com/google-research/arco-era5)
- [Icechunk ERA5 on AWS Open Data](https://registry.opendata.aws/earthmover-era5/)
- [ACR: advancing imaging research without moving data](https://www.acr.org/News-and-Publications/2026/advancing-imaging-research-without-moving-data)
- [Apheris and T-Systems](https://www.telekom.com/en/newsroom/latest-updates/media-information/2026/10/apheris-and-t-systems-protect-data)
- [ESRF data policy](https://www.esrf.fr/datapolicy)
- [ESRF DOI concepts](https://www.esrf.fr/home/UsersAndScience/UserGuide/esrf-data-policy/doi/doi---concepts.html)
- [How can I publish my LLM benchmark (arXiv)](https://arxiv.org/html/2505.18102v4)

## 103. Find the strongest ones. This will be our grounding use cases

**Question**

> Find the strongest ones. This will be our grounding use cases

**Answer**

I picked five grounding use cases and wrote them up as `docs/use-cases.md` in the mudcat repo (commit b704339, pushed). The README now links the document and says each feature specification must serve at least one of them.

I scored the fourteen candidates on three things: public evidence that the problem exists, whether it needs something current tools don't give in one place, and whether a team could adopt mudcat for it without a large organisation behind the decision. The five also had to cover the whole model between them.

| # | Grounding use case | Why it made the cut | Main parts of mudcat it tests |
|---|---|---|---|
| 1 | Curation of video and sensor data for model training | NVIDIA documents pipelines for "100+ PB" of video, and the robotics and driving evidence points the same way | Stream structure and selectors, annotation sets, enrichers, search, Ray, placement near GPUs |
| 2 | Training data for scientific models, across HPC and cloud | A 2026 paper shows major datasets carry inconsistent settings, and small teams are our most reachable users | Method records, declared equivalence, benchmark exclusion, replicas and representations, lineage to models |
| 3 | Rights and removal for training content | Regulation, legal commentary, takedowns and the NIH derivative rule all confirm it, and no open tool covers it | Usage grants and duties, enforcement before training, invalidation with receipts, legal holds, regulator reports |
| 4 | Governed retrieval for assistants | Several 2026 reports describe stale permissions and deletions in vector indexes | Change stream, removal events, search with re-checked permissions, citations to exact versions |
| 5 | A shared facility with many small groups | ESRF's policy shows the three-year embargo, the catalogue and the DOIs | Tenants, onboarding without an identity system, embargo per dataset, publication |

I merged some of the fourteen:

- **Use case 1** absorbs robotics episodes and driving log mining, which have the same shape.
- **Use case 2** names weather and climate models as its second domain, so arrays and monthly-appended series are still tested.
- **Use case 3** absorbs the voice and likeness case, reframed as "remove a person's content and identify affected models", which is what the evidence shows.

Five stay in reserve, each with its reason in the document:

- **Clinical imaging across hospitals:** strong evidence, but adoption is slow.
- **Single-cell models:** only the policy half was confirmed, and that part is already in use case 3.
- **Lab-in-the-loop:** no source for the link from prediction to experiment.
- **Agent trajectories:** commentary only.
- **Benchmark custody:** folded into use cases 2 and 3.

Things to know before relying on it:

- **The evidence is leads, not proof.** It comes from search summaries and our research notes. Use case 4 rests mostly on practitioner and vendor write-ups.
- **Two use cases point at design that isn't written yet.** Use case 4 needs retrieval as a governed action. Use case 5 needs the sign-in and membership design for small groups.
- **I added one requirement, RD-9.** Jurisdiction rules apply to where the caller is, not only where the data is stored. It comes from the China automotive data rules found during validation.

## 104. For each one of those use cases, we're going to built at least 5 simulated variants of sys

**Question**

> For each one of those use cases, we're going to built at least 5 simulated variants of system interaction - basically evals - for them.

**Answer**

The evals are built: 31 scenarios in a new `evals/` directory in the mudcat repo (commit 51d0bcf, pushed). There is no mudcat implementation and no runner yet, so these are specifications of behaviour, not tests that run. A small checker confirms all five files are structurally valid.

**What each eval is.** A YAML scenario with a start state, a sequence of steps, the results mudcat must give after each step, and the requirement IDs it verifies. Steps use a vocabulary of 46 actions (`register`, `commit`, `resolve`, `invalidate`, `poll` and so on), which is a first sketch of the system's operations.

**What was built:**

| Use case | Evals | Examples |
|---|---|---|
| 1. Video and sensor curation | 6 | Register in place and train; two enrichers committing at once; GPUs in another cloud with corrupted transfers; a caller in the wrong country |
| 2. Scientific training data | 6 | Reproduce a model a year later; combining datasets with different methods; excluding a benchmark; an HPC job with no network; monthly-appended arrays |
| 3. Rights and removal | 7 | A source that doesn't permit training; a licence ending; a takedown across three datasets and two sites; a legal hold against a takedown; the regulator summary |
| 4. Governed retrieval | 6 | Building an index from the stream; revoked access while the index is stale; a consumer that fell behind; 20 concurrent writers and a consumer that must miss nothing |
| 5. Shared facility | 6 | Two tenants that can't see each other; an embargo ending by date or by DOI; 30-day retention; GitHub team membership with GitHub unreachable; moving a tenant |

Every use case has at least five different kinds of variant, so the evals test more than the normal path. The kinds are concurrency, policy refusals, removal, time, placement, isolation and failure.

**What the evals found.** Writing them exposed 40 places where a scenario needs something the documents don't yet say. The checker prints the full list. The main groups:

- **Not in the model at all:**
  - storage management modes and mounts;
  - follow rules;
  - snapshots as members of other datasets;
  - retrieval as a governed action;
  - linked identities, membership sources and pipeline sign-in.
- **Behaviour described but with no requirement:**
  - delete markers;
  - method records and declared equivalence;
  - similarity criteria and leakage assertions;
  - DOIs;
  - per-object access conditions such as embargo;
  - the flag on a model after a removal.
- **Decisions nobody has made:**
  - Does mudcat refuse an evaluation result with a leakage violation, or only mark it?
  - What can a user without access see of an embargoed dataset?
  - How does a takedown in one tenant reach a copy in another?
  - What happens to stream positions when a tenant moves?

**Coverage.** 84 of the 128 requirements have at least one eval. The 44 without are mostly performance targets, the export and import interfaces, federation, and the SpiceDB and OpenFGA adapters.

One choice I made without asking: where a rule had no stated enforcement point, I picked one and recorded it as a gap. The clearest case is UC2-02, where combining different methods is refused at publication, not at commit.

## 105. so you will need to vary things, and work to design tasks effecirvely

**Question**

> so you will need to vary things, and work to design tasks effecirvely

**Answer**

The evals are now 15 task templates that expand into 280 variants, plus 16 invariants; the 31 fixed scenarios stay as reference examples. This is pushed as `4542cb7`, and `python3 evals/check.py` validates all three layers. Nothing is executed yet: there is no runner and no mudcat to run against.

I read "evals" as outcome-based tasks: a goal, a parameterised start state, and a grader on the end state. The actor can be a person, a pipeline or an AI agent with the mudcat tools.

#### How a task is designed

- **Goal, not steps.** The actor gets only the goal. Checks are statements about the end state, so any sequence of calls that reaches the right state passes.
- **Oracle.** Each template has ordered rules that compute the expected outcome from the parameters.
- **Two kinds of variation.** Decision axes change the expected outcome or the checks and get every combination. Cover axes (number of members, number of writers, interface) must not change the outcome; each value is paired with each outcome at least once.
- **Minimal pairs.** Every outcome has two variants that differ in one value and flip the result, so a system that always permits or always refuses fails.
- **Negative checks and distractors.** For example, revoking one user must not hide the document from a second user, and an artifact with the same identifier in another tenant must not appear.
- **Invariants.** The 16 global statements (for example, a published snapshot never changes its Merkle hash) are to be checked after every step of every variant.

`generate.py` refuses a template with an axis that has no effect, an oracle rule that decides nothing, or an outcome with no minimal pair. That check rejected 13 axes in my first draft, which I reclassified as cover axes or removed.

#### Variants per use case

| Use case | Tasks | Variants |
|---|---|---|
| 1. Curation of video and sensor data | resolve-and-read, concurrent-commits, stage-near-compute | 49 |
| 2. Training data for scientific models | combine-methods, leakage-check, reproduce-later | 27 |
| 3. Rights and removal | policy-gate, removal, content-summary | 120 |
| 4. Governed retrieval | stream-consumer, retrieval-after-change | 27 |
| 5. Shared facility | tenant-isolation, embargo, membership-source, tenant-move | 57 |

`python3 evals/generate.py --show T3.1` prints the variants of one task, and `--pairs T3.1` prints its minimal pairs.

#### Decisions the oracles made that the documents do not state

Writing the oracles forced four choices that the model and requirements do not state yet. Tell me if any is wrong:

- An add-only commit succeeds with no client retry, even with 32 concurrent writers.
- A legal hold blocks purge and erasure, but not invalidation.
- A resolution with a strict place option fails when the copy near the cluster is not complete, rather than returning a partial plan.
- A summary of training content states which runs have no recorded resolution, rather than omitting them.

The tasks and invariants cover 63 of the 128 requirements; performance and federation have no task yet. Also not done: random action sequences checked only by invariants, and tasks with measured results such as time or cost.

## 106. push all changes

**Question**

> push all changes

**Answer**

Everything is already pushed; there was nothing left to commit or push in either repository.

- **`brianv0/mudcat`**: the working tree is clean and the remote branch `claude/cool-keller-domhwr` is at `4542cb7`, the eval redesign commit.
- **`brianv0/datacat`**: the working tree is clean and the remote branch `claude/cool-keller-domhwr` is at `e82819e`, matching local.

The database schema sketch is still only in the scratchpad, uncommitted, as you asked earlier.
