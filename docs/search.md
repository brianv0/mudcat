# mudcat search: use cases and measures

Status: draft. This document lists what people and programs search for in mudcat, and how the project will measure if search is good and fast. [architecture.md](architecture.md) tells how search works. [requirements.md](requirements.md) has the requirements, with the prefixes EV and PF. The values in this document are estimates. Each one has the mark TBR until a measurement confirms it.

## What is known about search for datasets

- **It is mostly exploratory.** An analysis of the query logs of four national data portals concludes that people use portal search "in an exploratory manner, rather than to retrieve a specific resource". A survey of the field says that such search involves many queries and refinements.
- **Time and place are in most requests.** In written requests for data, the place, the period and the granularity of the data are the most common features.
- **Inside an organisation, people also search for things that they know.** Google's Goods catalog reports search for one's own data whose path the person forgot, search by the name of a schema, and search by the job that reads or writes a dataset.
- **Rank is a different problem than on the web.** Datasets have no links between them except provenance. Goods ranks by the kind of dataset, by the number of readers and dependents, and by the presence of a description from the owner. It reports that ranking was still not good enough.
- **Filters need counts.** A study of users of two dataset search products recommends filters that show the number of results that remain, so that a user does not filter too far.
- **The field has few benchmarks.** The survey calls the lack of benchmarks "one of the most widely recognized problems of dataset search". Two test collections now exist for search by topic: ACORDAR and the NTCIR Data Search task. Both use nDCG@10 as a measure of quality.
- **A filter changes the cost of a vector search.** A benchmark of filtered vector search finds that no method is best at all selectivities. A search that filters after the vector search does not find enough results when few items pass the filter.

## Two kinds of answer

Each search request asks for one of two kinds of answer. The two kinds have different measures, so the request must state which one it wants.

| Kind | Meaning | Source | Can it be behind the catalog? |
|---|---|---|---|
| **Ranked** | The best results first. A page of results is enough | The search index | Yes, by the index lag. But mudcat checks access and the invalidation set again for each result |
| **Complete** | All matches, with none absent and none wrong | The catalog database | No |

An audit needs a complete answer. A person who looks for a dataset needs a ranked answer. A ranked answer never contains an entity that the complete answer to the same request does not contain.

## Search use cases

| ID | Use case | Example of a request | Kind | Grounding use case |
|---|---|---|---|---|
| S1 | Find a thing by its exact name, path or identifier | `raw/2026/run_0412` or a DOI | Ranked | All |
| S2 | Find a thing whose name the person does not remember exactly | `rawframes 2026`, with one spelling error | Ranked | 6 |
| S3 | Find a thing by a name that it had before | A path from an old script, after a rename | Ranked | 2, 6 |
| S4 | Find one's own recent data again | "my datasets of last week" | Ranked | 6 |
| S5 | Find data by a part of its structure | Each dataset with a field `exposure_time` | Ranked | 2, 6 |
| S6 | Find data by the jobs and the data around it | Read by the job `daily_report`; downstream of dataset X | Ranked | 6, 7 |
| S7 | Find data on a topic | "sea surface temperature reanalysis" | Ranked | 2, 8 |
| S8 | Find data on a topic in other words than its description uses | "ocean heat" for the dataset above | Ranked | 2, 8 |
| S9 | Narrow by typed metadata, with counts for each value | Temperature between 250 K and 300 K; format; owner | Ranked or complete | 2, 5 |
| S10 | Find data that covers a period and a place | 2019 to 2023, North Atlantic | Ranked or complete | 2, 8 |
| S11 | Find data that the caller may use for a purpose | Datasets that permit training for commercial use | Ranked or complete | 3, 8 |
| S12 | Find more like this one | Datasets with the same fields, tags or neighbours in lineage | Ranked | 1, 6 |
| S13 | Find elements by text or by example | Clips of a left turn in rain; clips like this clip | Ranked | 1, 4 |
| S14 | Complete a name while the person types | `raw/20` | Ranked | All |
| S15 | Answer a question from an agent | "Which datasets have patient ages, and may I train on them?" | Ranked | 4 |
| S16 | List all matches for an audit | Each dataset that contains the field `user_email` | Complete | 3, 6 |
| S17 | Find data that can be combined with one's own | Tables with the same key; tables with the same fields | Ranked | Later |

S17 is the subject of most research on data lakes after Goods: search for tables that a given table can join or extend. mudcat keeps sketches of distinct values that such a search can use. The project does not plan this search for the first release.

## Query classes for measurement

A benchmark needs classes of queries whose correct answers are known. Each class below can be generated from a catalog, with no person to judge the results, except Q5 and Q6.

| Class | Queries | How the correct answer is known | Use cases |
|---|---|---|---|
| Q1 exact name | The name, a complete path or an identifier of one entity | It is that entity | S1 |
| Q2 changed name | A name with one change: one spelling error, joined or split words, a part of the path, an absent word | It is the entity that the name came from | S2, S14 |
| Q3 old name | An identifier that an entity had before a rename | It is that entity | S3 |
| Q4 field name | The name of a field, exact or with one spelling error | The entities whose structure has the field | S5 |
| Q5 topic | Words from a description | Judged collections: ACORDAR, NTCIR Data Search | S7 |
| Q6 other words | A question or a paraphrase of a description | Paraphrases that a model writes and a person checks in a sample | S8, S15 |
| Q7 filter | Conditions on typed attributes, tags, coverage and policy, with facets | The complete answer from the catalog database | S9, S10, S11 |
| Q8 text with filter | Q2 or Q5 together with Q7 | As Q2 or Q5, inside the complete answer of the filter | S9 |
| Q9 neighbours | A condition on lineage or on use | The complete answer from the catalog database | S4, S6 |
| Q10 similar entity | One entity as the query | Entities that share a planted set of fields and tags | S12 |
| Q11 prefix | The first 2 to 10 characters of a name | The entities whose name starts with them | S14 |
| Q12 element vector | An embedding, with a filter of a known selectivity | The exact nearest neighbours that pass the filter | S13 |
| Q13 all matches | A request for a complete answer | The complete answer from the catalog database | S16 |

## Measures

### Quality

| Measure | Definition | Query classes |
|---|---|---|
| Success@1 | The part of the queries whose correct entity is the first result | Q1 |
| MRR@10 | The mean of 1 divided by the position of the correct entity, where 0 counts for a position after 10 | Q2, Q3, Q4, Q11 |
| nDCG@10 | Normalised discounted cumulative gain of the first ten results, with graded judgments | Q5, Q6, Q8 |
| Recall@100 | The part of the relevant entities that are in the first 100 results. It shows if the first stage of search finds the candidates | Q5, Q6 |
| Recall@10 against exact | The part of the 10 exact nearest neighbours that the search returns | Q10, Q12 |
| Exactness | Precision and recall against the complete answer. Both must be 1 | Q7, Q9, Q13 |
| Facet count error | The difference between the count that the answer gives for a facet value and the correct count | Q7, Q8 |
| Zero-result rate | The part of the queries that return no result although a correct result exists | Q1 to Q6 |

Quality in search by topic depends on the collection. BM25 gets an nDCG@10 of about 0.59 on ACORDAR, and the best runs on NTCIR Data Search get about 0.15. Thus the targets for Q5 and Q6 compare mudcat with a baseline on the same collection. They are not absolute values.

### Speed

| Measure | Definition |
|---|---|
| Latency | The time from the arrival of a request at the service to the last byte of the first page, at percentiles 50, 95 and 99 |
| Latency of completion | The same, for Q11 |
| Throughput | The number of queries each second at which the latency at percentile 95 is still below its target |

Each result for speed states the corpus, the query class, the number of concurrent callers, and the readable share of the caller (see below).

### Freshness

| Measure | Definition |
|---|---|
| Index lag | The time from the commit of a change to the first ranked answer that shows the change, at percentiles 50 and 99 |
| Removal lag in answers | The time from the commit of a removal of access, an invalidation or a delete marker to the last answer that still shows the entity. The target is zero, because mudcat checks each result again |
| Stated position | Each ranked answer gives the position in the change stream that the index has processed. A client that wrote a change can wait for that position |

### Permissions

Search with permissions has a hard case: a caller who can read a small part of the catalog. The **readable share** of a caller is the part of the search documents that the caller can read.

| Measure | Definition |
|---|---|
| Leaks | The number of results, counts or completions that show an entity that the caller cannot read. The target is zero |
| Short pages | The part of the queries that return fewer results than the page size, although enough readable matches exist. This occurs when search filters after it ranks |
| Latency ratio | The latency at percentile 95 for a caller with a small readable share, divided by the latency for a caller who can read all |
| Examined results | The number of candidates that the catalog checked for each result that it returned |

### Cost

| Measure | Definition |
|---|---|
| Index size | Bytes of the search index for each search document, with and without vectors |
| Index rate | Search documents that the index builder processes each second |
| Rebuild time | The time to build a new index from the catalog while the old index serves queries |
| Memory | The memory that the search engine needs to hold the latency targets |

### Measures in operation

A benchmark shows if search can be good. Logs show if it is. mudcat records these values for each tenant, with no query text unless the tenant permits it:

- the zero-result rate;
- the part of the sessions that end with no selected result;
- the part of the queries that the person changes and sends again;
- the mean position of the result that the person selects;
- the part of the searches that lead to a description or a resolution of a result in the same session.

## Targets

Two deployment profiles have targets. The lab profile uses PostgreSQL for all search. The large profile uses an external search engine.

| Measure | Query classes | Lab profile, 10^7 search documents | Large profile, 10^8 search documents | Requirement |
|---|---|---|---|---|
| Success@1 | Q1 | 0.99 or higher | 0.99 or higher | PF-10 |
| MRR@10 | Q2, Q3, Q4 | 0.85 or higher (TBR) | 0.85 or higher (TBR) | PF-18 |
| nDCG@10 | Q5 | Not lower than BM25 on the same collection | The same | PF-11 |
| nDCG@10 | Q6 | Higher than lexical search alone (TBR by how much) | The same | PF-11 |
| Exactness | Q7, Q9, Q13 | 1 | 1 | EV-21 |
| Recall@10 against exact | Q12, at readable shares and filters from 1 to 0.001 | 0.95 or higher (TBR) | 0.95 or higher (TBR) | PF-17 |
| Latency, percentile 95 | Q1 to Q4, Q7 to Q9, with 20 concurrent callers | 200 ms or lower (TBR) | 200 ms or lower (TBR) | PF-12 |
| Latency, percentile 95 | Q5, Q6, Q10 | 500 ms or lower (TBR) | 500 ms or lower (TBR) | PF-19 |
| Latency of completion, percentile 95 | Q11 | 50 ms or lower (TBR) | 50 ms or lower (TBR) | PF-13 |
| Index lag, percentile 99 | All ranked classes | 30 s or lower (TBR) | 30 s or lower (TBR) | PF-14 |
| Leaks | All | 0 | 0 | EV-11, EV-19 |
| Short pages | All ranked classes, readable share 0.001 or higher | 0 | 0 | EV-20 |
| Latency ratio | Readable share 0.01 against 1 | 2 or lower (TBR) | 2 or lower (TBR) | PF-15 |
| Rebuild time | — | 1 hour or lower (TBR) | 12 hours or lower (TBR) | PF-16 |

The targets for latency follow a common rule for interactive systems: an answer in about 0.1 s seems immediate, and an answer in about 1 s keeps the attention of the person. A target of 200 ms at percentile 95 leaves time for the network and for the display.

## The benchmark

The benchmark is in [evals/benchmarks/search.yaml](../evals/benchmarks/search.yaml), as a specification that a runner will use. Its parts:

**Corpora.** A generator makes a catalog of a given size. It does not make data files. The corpus must have the properties that make search hard:

- names and paths with the patterns that Goods describes: dates, versions, shards, sites and UUIDs;
- many names that differ in one part only, for example 1,000 datasets with the name `events` under different teams;
- a long tail of attribute names, with 10^4 different attributes in one tenant;
- descriptions from a person for a small part of the entities, and inferred metadata for the others;
- ACL roots of very different sizes, so that readable shares from 1 to 0.001 exist;
- a rate of change of 5% of the entries each day during the measurement.

| Corpus | Search documents | Purpose |
|---|---|---|
| Small | 10^5 | Development, and quality measures |
| Lab | 10^7 | The targets of the lab profile |
| Large | 10^8 | The targets of the large profile |
| Judged | The size of each public collection | Quality for Q5 |

**Queries.** Each query class has 1,000 queries or more for each corpus. The generator derives the queries of Q1 to Q4 and Q7 to Q13 from the corpus, together with their correct answers.

**Procedure.**

1. Load the corpus through the normal registration interface, so that the index is built from the change stream.
2. Wait until the index lag is zero. Then run each query class one time to fill caches.
3. Measure quality with one caller.
4. Measure speed with 1, 20 and 100 concurrent callers, at readable shares of 1, 0.1, 0.01 and 0.001.
5. Measure freshness: write changes at a fixed rate, and send queries for the changed entities.
6. Report each result with the corpus, the hardware, the versions of the engines and the version of the embedding model.

**Baselines.** Each run reports the same measures for PostgreSQL full-text search alone and for lexical search alone in the selected engine. Thus a change to ranking shows its effect.

## Open questions

- **Judgments for Q6.** If paraphrases that a model writes are a fair test, and how large the sample is that a person checks.
- **Rank signals.** Which signals from the catalog (use, dependents, publication, description from a person) improve MRR@10 and nDCG@10. The benchmark can measure each one alone.
- **Licences of the judged collections.** If the project can use ACORDAR and the NTCIR collections in its tests.
- **Counts for facets.** If counts must be exact for a caller with a small readable share, or if a stated estimate is permitted.
- **Search between tenants.** How a search that covers federated tenants reports its measures.

## Sources

- Kacprzak and others, "Characterising dataset search: an analysis of search logs and data requests", Journal of Web Semantics, 2019. [Record](https://eprints.soton.ac.uk/426734).
- Chapman and others, "Dataset search: a survey", VLDB Journal, 2020. [arXiv 1901.00735](https://arxiv.org/abs/1901.00735).
- Halevy and others, "Goods: Organizing Google's Datasets", SIGMOD 2016. [Paper](https://research.google/pubs/goods-organizing-googles-datasets/).
- "User Experience in Dataset Search Platform Interfaces", 2024. [arXiv 2403.15861](https://arxiv.org/abs/2403.15861).
- ACORDAR test collection, SIGIR 2022. [Repository](https://www.github.com/nju-websoft/ACORDAR).
- NTCIR-15 Data Search task. [Overview](https://research.nii.ac.jp/ntcir/workshop/OnlineProceedings15/pdf/ntcir/01-NTCIR15-OV-DATA-KatoM.pdf).
- "Benchmarking Filtered Approximate Nearest Neighbor Search Algorithms on Transformer-based Embedding Vectors", 2025. [arXiv 2507.21989](https://arxiv.org/abs/2507.21989).
- LakeBench, PVLDB 2024. [Paper](https://vldb.org/pvldb/vol17/p1925-chai.pdf).
