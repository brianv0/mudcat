# Design discussion, part 02: Access control, usage policy, federation, and the language of the documents

This file is a record of the design discussion: each question, and the answer as it was given at that time. Later decisions can replace what an answer says. The documents in `docs/` hold the present design, and `docs/open-items.md` holds what is not yet in them. Paths under `/tmp` and commit IDs in the answers refer to the state at that time.

## 13. what about zanzibar style policy instead of odrl?

**Question**

> what about zanzibar style policy instead of odrl?

**Answer**

Zanzibar is the better choice for **access control**, but it can't replace the **usage-policy** half of what I gave ODRL. I'd split the job: Zanzibar-style relationships for who may do what, and a small policy model for what data may be used for. This is from my own knowledge; I didn't research either system in this session.

#### Where Zanzibar fits better than what I wrote

The access model in `docs/model.md` is already relationship-shaped: ACLs inherited down collections, groups, compartments, tickets. Zanzibar expresses all of that directly as relation tuples and rewrite rules, and does it better than ACL lists in three ways:

- **Inheritance through relations:** inheritance follows any relation, not just the folder tree. Datacat checked only the target and its parent, never the whole ancestor chain.
- **Reverse lookups:** "list everything Alice can read" and "who can read this" are built-in queries, and the catalog's search filter needs both.
- **Consistency tokens:** Zanzibar's tokens (zookies) say which version of the permissions a decision used. A revocation is guaranteed to apply to every check after it, which matters for takedowns. A pinned training-set specification can record the token, which is exactly what its policy-decision record is meant to capture.

So Zanzibar should replace the ACL and compartment model outright.

#### What Zanzibar cannot express

| Need | Zanzibar | Why it matters |
|---|---|---|
| **Purpose of use** (train, evaluate only, non-commercial) | Only by encoding purposes as fake subjects | Permission to read is not permission to train |
| **Expiry and time windows** | Not in the original paper; SpiceDB adds conditions and expiring relationships | Licences expire |
| **Duties** (attribute, delete at close-out, honour opt-outs) | None. It returns allow or deny, not "allow, provided you do X" | Duties must travel to derived data and models |
| **"Every input must permit"** | Inheritance through a relation means "any parent grants it" | A training set is permitted only if all of its sources permit it. I believe SpiceDB has added an all-parents form, but I haven't verified it |
| **Checks across very wide fan-in** | Designed for shallow graphs with depth limits | One pinned specification can draw on millions of sources |

#### Recommended split

1. **Access control: Zanzibar.** Collections, data objects, groups, compartments, tickets and the inheritance rules become a schema of relations and permissions.
2. **Usage grants: also Zanzibar tuples, but evaluated in bulk.** A per-source grant becomes a tuple such as `source:X#train@purpose:research`. When a specification is pinned, the pinning service resolves its sources in bulk and computes "every source permits this purpose" with set operations in SQL, instead of running millions of individual checks.
3. **Constraints and duties: a small policy model of mudcat's own.** Expiry, jurisdiction and duties stay as data attached to sources, and propagate along lineage as the model already describes. Licence terms that arrive as ODRL are imported into that model; ODRL is no longer the language the system evaluates.
4. **Implementation: start with a Zanzibar subset built into PostgreSQL.** That is a tuples table plus recursive queries, which keeps PostgreSQL as the only required dependency. Larger deployments can move to SpiceDB or OpenFGA, both of which can run on PostgreSQL.

Cedar is the alternative if you'd rather have one language for both halves, since it combines relationships with conditions. I'd still keep duties outside any authorization engine, because duties are data that must travel with derived objects, not access decisions.

If you agree, this replaces the access-control and usage-policy parts of section 5 in `docs/model.md`, and the policy-engine section of `docs/architecture.md`. I haven't changed either yet.

## 14. what would odrl be covering then?

**Question**

> what would odrl be covering then?

**Answer**

ODRL would cover the **terms themselves**: what was agreed, in a standard machine-readable form. It would stop being the thing mudcat evaluates. There would be three layers:

| Layer | Answers | Form |
|---|---|---|
| Terms | What did the licence or contract say? | ODRL document, kept unchanged as the record of the agreement |
| Compiled policy | What does mudcat enforce? | Usage grants as relationship tuples, plus constraints and duties attached to the source |
| Access control | Who may do it? | Zanzibar relationships |

ODRL's jobs:

1. **Importing terms.** Where terms already arrive as ODRL, mudcat compiles them into grants and duties:
   - **Crawl opt-outs:** the W3C community group's TDM Reservation Protocol, the text-and-data-mining opt-out mechanism that crawled-data sources publish, writes its policies in ODRL.
   - **Media licensing:** the news industry's RightsML is an ODRL profile.
   - **European data spaces:** their contract offers and agreements are ODRL.
2. **Keeping evidence.** The original document stays attached to the source. Each compiled grant points back to the clause it came from, so an audit or dispute can trace an enforcement decision to the contract.
3. **Exporting terms.** Published snapshots carry their terms in a form others can read. DCAT attaches policies with `odrl:hasPolicy`, so Croissant and DCAT exports, and the federation feed between zones, pass terms on in a standard form.
4. **Negotiating with outside parties.** A zone, or a partner that runs a data-space connector, can publish an ODRL offer and record the accepted agreement before data flows.

Most data never needs ODRL at all, so intake has three tiers:

| Terms arrive as | Handling |
|---|---|
| A standard licence identifier (SPDX: `CC-BY-4.0`, `CC-BY-NC-4.0`, `Apache-2.0`, …) | Mapped to a pre-compiled set of grants and duties |
| An ODRL document | Compiled automatically; any clause that cannot be translated goes to review |
| A contract in plain text | A person records the grants and duties; the contract is attached |

A source whose terms were only partly compiled is marked as such. Pinning blocks any purpose that depends on the untranslated part until a reviewer signs off. That keeps the evaluated policy model small without losing what the contract actually says.

I haven't changed the design documents yet; this, with the Zanzibar split, would replace section 5 of `docs/model.md`.

## 15. could we make sure we could integrate with zanzibar-like cloud offerings?

**Question**

> could we make sure we could integrate with zanzibar-like cloud offerings?

**Answer**

Yes. The design now sends every access check through one internal interface, so it can be served by mudcat itself or by an external Zanzibar-style service, self-hosted or managed. This also brings in the Zanzibar split and the narrower role for ODRL that I proposed earlier. It's committed and pushed to `brianv0/mudcat` on `claude/cool-keller-domhwr`.

The new "Authorization port" section in `docs/architecture.md` covers:

- **Adapters:**
  - built-in: relationship tuples in the catalog's PostgreSQL, so there is still no extra dependency;
  - SpiceDB, self-hosted or AuthZed's managed service;
  - OpenFGA, self-hosted or Okta FGA;
  - others such as Ory Keto, through the same contract;
  - Cedar and Amazon Verified Permissions as partial support: checks only, no reverse lookups.
- **Common operations:** the interface uses only operations both SpiceDB and OpenFGA offer, with a table mapping each one to its API call.
- **A neutral schema:** written once and compiled to each provider's schema language, using only features both main implementations support. Provider-specific features can appear only in optional extensions.
- **Consistency:**
  - relationship changes go through the catalog's change outbox in the same transaction as the catalog change;
  - revocations and takedowns use the provider's strongest consistency mode;
  - a reconciler repairs drift between the two stores;
  - pinned training-set specifications record the provider's consistency token where it issues one.
- **Large result sets:** search asks the provider only for the small set of access roots and compartments a user can read, then filters in SQL. Provider list operations have result limits, OpenFGA's especially.
- **Failure behaviour:** requests fail closed, cached decisions are short-lived, and every decision is audited.
- **Conformance suite:** one set of authorization scenarios runs against every adapter, and an adapter is supported only if it passes.

Two changes went into `docs/model.md` to make this work:

- **Shallow graph:** Datacat's "inherit from the nearest ancestor with an ACL" rule is kept as an explicit link from each collection to that ancestor. A check then follows one hop however deep the folder tree is, which keeps it within the providers' resolution-depth limits.
- **Usage policy stays in mudcat:** usage grants, constraints and duties are evaluated by mudcat at pinning, whichever provider is configured. "Every source must permit this" is computed in SQL over the sources, not as millions of individual provider checks. Grants can still be mirrored to the provider for ordinary online checks.

The managed-service names and API call names come from my own knowledge, not checked against current documentation, and the architecture doc says to confirm them before choosing a provider. I added one open question: whether grants should be owned by the external service when one is configured, instead of by mudcat with a synchronised copy.

## 16. what such systems exist

**Question**

> what such systems exist

**Answer**

Several exist. For mudcat, the realistic adapter targets are **SpiceDB** and **OpenFGA**: both are open source under Apache-2.0, actively developed, and offered as managed services. Only a few points below were checked with a quick search this session; the rest is from my own knowledge and is marked as such.

#### Relationship-based (Zanzibar-style) engines

| System | Origin and backing | Managed offering | Notes for mudcat |
|---|---|---|---|
| **SpiceDB** | AuthZed; Apache-2.0 | AuthZed's hosted service | The closest to the Zanzibar paper, including its consistency tokens. Runs on PostgreSQL, CockroachDB, Spanner or MySQL (from memory). Conditions are written in CEL. |
| **OpenFGA** | Built at Auth0/Okta, donated to the CNCF; Apache-2.0. Became a CNCF incubating project in late 2025 ([CNCF](https://www.cncf.io/blog/2025/11/11/openfga-becomes-a-cncf-incubating-project/)) | Okta FGA / Auth0 FGA, built on OpenFGA | Vendor-neutral governance. Runs on PostgreSQL or MySQL (from memory). It has CEL conditions, but a consistency *mode* instead of SpiceDB-style tokens, and result limits on list operations. |
| **Permify** | Acquired by FusionAuth in November 2025 and since sold as "FusionAuth FGA by Permify"; FusionAuth says it stays open source ([Built In](https://builtin.com/articles/fusionauth-acquires-permify-20251125)) | Through FusionAuth | A competitor's comparison flags its licence ([AuthZed](https://authzed.com/learn/openfga-alternatives)), so check the licence directly before adopting it. |
| **Ory Keto** | Ory; Apache-2.0 (from memory) | Ory Network | The natural choice if a site already runs Ory for identity. Historically less complete than SpiceDB on Zanzibar rewrite rules (from memory). |
| **WorkOS FGA** | From Warrant, which WorkOS acquired in 2024 (from memory; not verified) | WorkOS | Mainly a managed product; current open-source status unverified. |
| **Topaz** | Aserto; combines a relationship directory with OPA policies (from memory) | Aserto | I could not confirm Aserto's current status. |

#### Policy-based alternatives

These are not tuple stores, but are often evaluated alongside the engines above:

- **Cedar and Amazon Verified Permissions:** policies over entities and attributes. The caller supplies relationship data with each request, so reverse lookups such as "everything Alice can read" are not built in.
- **Oso Cloud:** the Polar policy language, with relationship support.
- **Cerbos:** attribute-based policies, open source, with a hosted control plane.
- **Open Policy Agent:** general-purpose policy (Rego). Permit.io builds on it with relationship features.

#### Internal systems, as design references only

- **Google Zanzibar** itself.
- **Airbnb's Himeji** and **Carta's** authorization service. Both have public write-ups (from memory).

#### What this means for the design

- **Build these first:** the built-in PostgreSQL adapter, SpiceDB and OpenFGA. Together they cover self-hosted use and the two largest managed offerings.
- **Permify and Ory Keto:** treat them as candidates for the same adapter contract, once their licence and feature coverage are checked against the conformance suite.
- **Cedar:** stays a partial adapter, because it can't answer reverse lookups.

Most comparison pages in the search results are vendor content and disagree on which engine to choose ([pkgpulse](https://www.pkgpulse.com/guides/openfga-vs-permify-vs-spicedb-zanzibar-authorization-2026), [WorkOS](https://workos.com/blog/top-5-google-zanzibar-open-source-implementations-in-2024)). None had benchmark data, so the choice between SpiceDB and OpenFGA should be settled by running the conformance suite and a load test against each.

Sources:
- [CNCF: OpenFGA becomes a CNCF incubating project](https://www.cncf.io/blog/2025/11/11/openfga-becomes-a-cncf-incubating-project/)
- [Built In: FusionAuth acquires Permify](https://builtin.com/articles/fusionauth-acquires-permify-20251125)
- [AuthZed: OpenFGA alternatives](https://authzed.com/learn/openfga-alternatives)
- [pkgpulse: OpenFGA vs Permify vs SpiceDB (2026)](https://www.pkgpulse.com/guides/openfga-vs-permify-vs-spicedb-zanzibar-authorization-2026)
- [WorkOS: Top Zanzibar open-source implementations](https://workos.com/blog/top-5-google-zanzibar-open-source-implementations-in-2024)

## 17. seems like datacat model is closest to zanzibar model.

**Question**

> seems like datacat model is closest to zanzibar model.

**Answer**

Mostly, yes. Of the systems we looked at, Datacat's access model is the closest to Zanzibar, but it is a simpler relative of it.

**What already matches:**
- **Groups as subjects.** An entry such as `alice@astro:g:rw` grants rights to a group, which is a Zanzibar subject set (`group#member`).
- **External membership.** Membership comes from a separate group-manager service, much as Zanzibar resolves group membership through relations.
- **Inheritance.** Permissions flow down the folder hierarchy, as Zanzibar does by following a parent relation.
- **Effective permissions.** The effective-permissions call corresponds to a Zanzibar check.

**What Datacat lacks:**
- **Storage:** ACLs are strings stored on each node, at most 32 entries, not tuples.
- **Computed permissions:** none. The five rights (r, i, d, w, a) are independent letters; none implies another.
- **Intersection and exclusion:** none, so compartments can't be expressed.
- **Reverse lookups and consistency:** no index for "everything Alice can read", and a 60-second per-server cache instead of consistency tokens.
- **Ancestor checks:** a check looked at the target or its parent, never the whole ancestor chain.

**One difference that matters:** Datacat inherits by override, like AFS: a folder with its own ACL replaces what it would have inherited. Zanzibar inherits by union: the parent's grants are added to the node's own. The `acl_root` relation in the design (each collection points to its nearest ancestor that has an ACL) keeps Datacat's override behaviour, and also keeps checks to one hop.

**The other systems are further away:**
- **iRODS** copies a collection's ACL onto new objects when they are created, so later changes to the collection don't reach existing objects.
- **Rucio and the Butler** grant permissions per account or per repository.

So existing Datacat ACLs can be migrated mechanically. I added this mapping to the access-control section of `docs/model.md` and pushed it.

## 18. what about dataset vs individual file/dataset strategy

**Question**

> what about dataset vs individual file/dataset strategy

**Answer**

No separate answer. The work for this message is in the answer to a later message.

## 19. what about group/folder/collection vs individual file/dataset strategy

**Question**

> what about group/folder/collection vs individual file/dataset strategy

**Answer**

No separate answer. The work for this message is in the answer to a later message.

## 20. when do you federate, when do you delegate, when do you replicate?

**Question**

> when do you federate, when do you delegate, when do you replicate?

**Answer**

I've answered both questions in the design documents and pushed them to `brianv0/mudcat` on `claude/cool-keller-domhwr`.

#### Collections versus individual files and datasets

Grants go on containers by default, and on individual objects only as exceptions. This is now a "Granularity" subsection under access control in `docs/model.md`.

- **Two containment structures, two roles.** A file sits in exactly one collection (a Datacat folder), and that collection's ACL governs custody: who may read, write and administer it. A file can be in many datasets (Datacat groups), and datasets govern sharing.
- **Sharing through a dataset is checked once, at the snapshot.** The resolver then issues credentials for its members. Writing a relationship from every member to every snapshot containing it would multiply relationship counts by the number of snapshots.
- **Grants by tier:**
  - collections and datasets: the normal place for grants;
  - snapshots, specifications and models: individual grants allowed, since there are at most millions of them;
  - individual file versions: exceptions only (one shared file, or one file withdrawn from a readable folder), and the catalog reports how many exist;
  - parts of files: no access grants at all; these are handled by usage exclusion lists or redaction tiers.
- **Conditions that differ per object are checked by the catalog in SQL.** Examples are an observation's proprietary period or a patient's consent status. They are combined with the container-level grant, not written as one conditional relationship per object.
- **Three separate axes:** custody follows collections, sharing follows datasets, and usage rights follow sources. This keeps folder layout from having to mirror licence boundaries.
- **Storage prefixes should follow ACL roots.** Credentials scoped to a prefix are only safe when the prefix holds nothing the caller can't read. Otherwise the resolver falls back to signed URLs per object.

#### Federate, delegate or replicate

Two tests decide most cases; they are in a new section of `docs/architecture.md`.

1. **Who can say no?** If one authority controls the state, replicate it within that authority, or delegate it to a specialist system. If several independent authorities each control part of it, federate.
2. **Is it immutable?** Versions, published snapshots, pinned specifications and provenance records can be copied anywhere. Mutable state (ACLs, grants, replica state) has exactly one writer, and is never replicated with several.

How that plays out:

- **Replicate, within one zone:**
  - file bytes, placed by replication rules;
  - catalog read replicas;
  - search and identity indexes;
  - the structural relationships copied into an external authorization service.

  Rubin running an independent Butler at each site, for one organisation's data, is the counter-example: federation used where replication was the right answer. Rucio's model, one catalog with many sites, is the one to follow.
- **Delegate, with a contract and a way to verify the answer:**
  - identity and groups to the identity provider;
  - access checks to SpiceDB or OpenFGA;
  - byte movement to FTS3 or Globus;
  - table file lists to Iceberg or Lance;
  - query execution to DuckDB or Trino;
  - training inside an enclave to the enclave itself.

  mudcat never delegates usage-policy decisions, because they depend on lineage only the catalog holds.
- **Federate, across organisations, jurisdictions, enclaves and partners:**
  - zones exchange immutable descriptions only, never mutable state;
  - references cross zones by IRI, and the zone that owns the data makes every access decision;
  - identity federates through OIDC, but authorization does not;
  - data or descriptions move only under a recorded agreement, and a zone that accepts copies takes on their obligations, including honouring tombstones;
  - an enclave zone can publish descriptions and lineage while never letting its data leave.

The section ends with a table giving the answer for each kind of state: bytes, catalog records, indexes, provenance, identity, access control, usage policy, tombstones and transfers.

## 21. in the rubin model, there are multiple institutions which could "own" the data and would w

**Question**

> in the rubin model, there are multiple institutions which could "own" the data and would want to for sovereignty reasons, for example

**Answer**

You're right, and I've corrected it. I had called Rubin's per-site Butlers "federation used where replication was the right answer", which assumes one owner. With several institutions each wanting sovereignty over their copies, Rubin is a third case that neither pure replication nor loose federation fits. I've added it to `docs/architecture.md` as "Shared custody" and pushed.

**The case:** one logical dataset, such as a data release, held by several sovereign custodians.
- A single catalog would take sovereignty away from the custodians.
- Independent catalogs lose the fact that it is one dataset. That is what the Butler team's draft diagnosis describes: corrections don't propagate, and there is no completeness check per site.

**The answer:** federation with a shared namespace, splitting authority by kind of state, not by site.

| State | Authority |
|---|---|
| Definition of the dataset: data objects, published snapshots, corrections | The producing zone for each part; a part processed at a site belongs to that site |
| A release made of parts from several sites | A release snapshot built from the member snapshots' hashes, signed by each producer and co-signed |
| Replicas, replica state and completeness on a site's storage | The custodian that owns the storage |
| Access decisions for a site's users and copies | The custodian |
| Minimum terms, such as data rights and proprietary periods | The publisher, through the agreement. Custodians may add stricter local terms but not looser ones |

Three mechanisms make this work without shared mutable state:

- **Content-derived identifiers.** A version's ID is now computed from its data object's ID and its content checksum (UUIDv5), so every zone that registers a copy gets the same ID. The Butler does the same with deterministic UUIDs where sites must agree. I changed the identity section of `docs/model.md` to match: other entities keep minted UUIDv7 IDs owned by their zone.
- **Hash-identified snapshots.** A release made of parts owned by different zones can be verified from the hashes alone, so no zone has to trust another zone's database.
- **The federation feed.** Corrections and tombstones flow from producers, and completeness records flow from custodians. Each custodian publishes which snapshot hashes it holds completely, so a site that failed to apply an update is visible instead of silent.

What I haven't worked out is how a co-signed release is governed: who may publish one, and what happens if a custodian refuses a correction or deletion that its national law conflicts with. The agreement between zones would have to settle both.

## 22. verify the language used so far largely maps to STE with domain-specific language

**Question**

> verify the language used so far largely maps to STE with domain-specific language

**Answer**

Partly. On the rules I can measure, the mudcat documents come close to STE. Voice, *-ing* forms and some vocabulary don't, and the two research reports in the Datacat fork are not STE at all.

I can't check full compliance. STE (ASD-STE100) also limits each approved word to one meaning, and I don't have the dictionary here. My list of words STE doesn't approve is from memory and only indicative.

| Document | Sentences | Mean length | Over 25 words | Passive voice | *-ing* words |
|---|---|---|---|---|---|
| `README.md` | 44 | 11 words | 7% | 18% | 6 |
| `docs/model.md` | 307 | 12 words | 2% | 21% | 31 |
| `docs/architecture.md` | 264 | 13 words | 5% | 20% | 48 |
| Datacat report | 252 | 17 words | 19% | 14% | 70 |
| Multimodal design report | 188 | 16 words | 12% | 27% | 64 |

The checker sometimes runs two sentences together where one begins with the lowercase "mudcat", so the long-sentence counts are slightly high. The passive-voice count is a pattern match, so treat it as approximate.

**What already conforms in the mudcat documents:**
- **Sentence length:** almost all sentences are within STE's 25-word limit for descriptive text.
- **One idea per sentence:** mostly yes, helped by the bullets and tables.
- **Domain terms:** technical names (replica, snapshot, ACL root, pinned specification) and terms from the systems we studied are used consistently, which STE allows.

**Where they depart from STE:**
- **Passive voice, about one sentence in five.** STE wants active voice where possible. For example, "Grant removals … are checked with the provider's strongest consistency mode" would become "The authorization port checks grant removals with …".
- ***-ing* forms.** "Training", "pinning" and "listing" are acceptable as technical names. "Existing", "using", "including", "containing" and "matching" should be rewritten as clauses.
- **Vocabulary:**

  | Word | Times used | STE replacement |
  |---|---|---|
  | "whether" | 14 | "if" |
  | "therefore" | 8 | "thus" or "as a result" |
  | "require", "required" | 13 | "need", "necessary" |
  | "obtains" | 2 | "gets" |
  | "typically" | 2 | "usually" |
  | "however" | 1 | "but" |
  | "prior" | 1 | "before" |
  | "performs" | 1 | "does" |

- **Long sentences:** a few run to 33–41 words, mostly ones with a parenthetical or a semicolon.
- **Noun clusters:** STE limits these to three words. Some domain terms are longer ("content-identity index", "authorization consistency token"); they would need defining once as technical names.

If you want STE-conformant text, an editing pass on the three mudcat documents would fix the vocabulary and *-ing* forms, split the long sentences, and make most passives active. I haven't changed anything yet. I'd leave the two research reports as they are, since they are narrative research, not specifications.

## 23. do it

**Question**

> do it

**Answer**

No separate answer. The work for this message is in the answer to a later message.

## 24. btw uuidv7 is almost always the way to go for identifiers in a system like this. There's g

**Question**

> btw uuidv7 is almost always the way to go for identifiers in a system like this. There's generally no privacy concerns with leaked timestamps precisely because because there's a strong model to accessing the data

**Answer**

No separate answer. The work for this message is in the answer to a later message.

## 25. also if any requirements are stated for the system, try to map to NASA guidelines for auth

**Question**

> also if any requirements are stated for the system, try to map to NASA guidelines for authoring requirements

**Answer**

No separate answer. The work for this message is in the answer to a later message.

## 26. remove IVOA references and other field-specific references from the model and arch too. Th

**Question**

> remove IVOA references and other field-specific references from the model and arch too. They were used only for discussion purposes, they are not related to any real requirement. You could extract them out to a field-specific guidelines guide, and then we could dive deeper into wet-lab, HEP, solid state physics, genomics, geospatial, or other data-intensive fields semantics

**Answer**

The requirements draft is done: `docs/requirements.md` has 69 "shall" requirements in NASA format, each with an ID, rationale and verification method, and the queries the system must answer are now requirements QY-1 to QY-8. The editing passes on `model.md` and `architecture.md` are still running. When they finish, I'll make three changes to those files:

- **Identifiers:** use UUIDv7 for versions too, dropping the content-derived IDs. Cross-zone agreement then comes from copies keeping the ID the producing zone minted, with the checksum as a unique key.
- **Field-specific material:** move IVOA, astronomy, wet-lab, materials and genomics material out of the model, the architecture and the requirements, into a new field guidelines document. References to other systems (Datacat, iRODS, Rucio, the Butler) stay, as design precedents.
- **Requirements links:** point the architecture's requirements table at the requirement IDs.

## 27. Some inspiration from the system could be iceberg along with it's REST based catalog and t

**Question**

> Some inspiration from the system could be iceberg along with it's REST based catalog and things it's snapshot mechanism as well. We don't need to go that deep, but if there's any inspiration you can gleem from that, feel free. Be criticial, don't necessarily add it if it's not strictly necessary, but I saw you mentioning snapshots and I thought maybe it was worht a discussion

**Answer**

Everything you asked for since the STE question is done, committed and pushed to `brianv0/mudcat` on `claude/cool-keller-domhwr`.

**STE pass.** Edited `README.md`, `docs/model.md` and `docs/architecture.md`:

| | Before | After |
|---|---|---|
| Passive sentences | about 20% | about 1% (the rest have no useful agent, or the checker misread an adjective) |
| Non-STE words (whether, therefore, require…) | 15 in `model.md`, 8 in `architecture.md` | none |
| *-ing* words left | — | only technical names such as pinning and training |

Real sentences are now 25 words or fewer; the checker still flags a few that it merges at "mudcat". No facts or design decisions changed, but some sentences now name an agent the original left unstated, for example "clients write grants through mudcat's API". The agents that made the edits listed each such place, and I accepted them.

**UUIDv7 everywhere.** Every entity, versions and snapshots included, now has a UUIDv7, with your rationale stated. Checksums and snapshot Merkle hashes remain as verifiable attributes and aliases, not identifiers. For agreement across zones, a zone registering a copy uses the producing zone's ID, and registration is idempotent on (data object, content checksum).

**NASA-style requirements.** The new `docs/requirements.md` has 73 "shall" requirements. Each has an ID, one need per statement, a rationale and a verification method (test, analysis, inspection or demonstration). Unknown values are marked TBD or TBR, and the eight "queries the system must answer" are now requirements QY-1 to QY-8. The architecture's requirements table and scale envelope cite the IDs.

**Field-specific material moved out.** IVOA, Felis by name, FITS/ROOT, chemistry, wet-lab, genomics, medical and grid-credential details are gone from the model, architecture and requirements, replaced by generic wording. The new `docs/fields.md` holds them:

- **What a field profile contains:** schema profile, format plugins, vocabularies, identifiers, method records, similarity criteria, gateways and policy conventions.
- **A worked example:** the Felis mapping, as the template for a schema profile.
- **Starting sections** for astronomy, HEP and grid computing, materials and chemistry, biology and the wet lab, genomics and medicine, earth and geospatial, and robotics.

References to other systems (Datacat, iRODS, Rucio, the Butler, DataHub) stayed in the core documents, as design precedents. One loss to note: the model has no concrete example of a field gateway or of a plugin that needs field semantics. I added a generic profile-mapping table to the model so `mc:constraint` and `mc:index` are still defined there.

**Iceberg, critically.** I adopted only what solves a problem the design already had:

| Adopted | Why |
|---|---|
| Every change makes a new immutable snapshot; publishing is a tag on a branch | Removes the mutable "working" snapshot. This is Iceberg's write-audit-publish pattern |
| Conditional commits: the client states which snapshot it expects the branch to point to | Concurrent writers can't lose each other's changes |
| A parent and a change summary on each snapshot | History and the change feed without comparing member lists |
| Snapshots share unchanged manifests | Iceberg's version of the Merkle sharing the design already had |
| Expiry of unpublished snapshots that nothing refers to | Needed once every change makes a snapshot |
| Stable field IDs that survive renames | CSVW columns are keyed by name, which breaks on a rename |
| "As of time T" resolution | Cheap once branch history is recorded |
| Server-side planning for resolutions too big for one response | Same need as Iceberg's server-side scan planning |
| Optionally showing any manifest as an Iceberg table | Spark, Trino and DuckDB can then query elements directly |

Rejected:

- **Catalog state in metadata files with a pointer swap:** PostgreSQL is the source of truth, and keeping both would duplicate it.
- **Partition specs, sort orders and Avro manifests:** these are concerns of a table format, not a catalog.
- **Branch merging and cherry-picking:** not needed.
- **Iceberg's snapshot IDs:** they are random and carry no content hash, so mudcat's Merkle hashes stay.
