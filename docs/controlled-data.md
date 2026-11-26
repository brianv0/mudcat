# mudcat and controlled data: CUI, export control and NIST requirements

Status: draft profile. This document tells how mudcat applies to data that United States rules control: Controlled Unclassified Information (CUI), technical data under the export regulations ITAR and EAR, and the NIST requirements for the systems that hold such data. It is a profile, as a field profile is: it adds to the core documents and does not change them. Other rule sets, for example for health data or for the dual-use rules of the EU, can have their own profiles.

This document is not legal advice. The export control office or the security office of each organisation decides what is controlled and what a system must do. Several statements below come from summaries and not from the text of the rules. The research notes name them.

## The rules, in short

| Rule | What it covers | What matters for a catalog |
|---|---|---|
| CUI program (32 CFR 2002, the CUI Registry of NARA) | Unclassified information that a law or a policy says must be protected | Each item has a banner marking: `CUI`, a category, and optionally a limited dissemination control such as `NOFORN` or `FED ONLY`. Only the markings of the Registry are permitted. Export-controlled information is one category (`CUI//SP-EXPT`) |
| NIST SP 800-171 | The security requirements for CUI in systems outside the federal government | Revision 2 has 110 requirements in 14 families. Revision 3, of May 2024, has 97 requirements in 17 families. Assessments for CMMC Level 2 use Revision 2 at this time |
| DFARS 252.204-7012 | Contractors of the Department of Defense | They must meet NIST SP 800-171. A cloud service that holds the data must meet the FedRAMP Moderate baseline or an equivalent. They must report a cyber incident in 72 hours |
| ITAR (22 CFR 120 to 130) | Technical data about defence articles | Access by a person who is not a US person is an export, also when it occurs inside the United States. Encrypted data is a special case (see below) |
| EAR (15 CFR 730 to 774) | Dual-use items and technology | The same ideas as ITAR, with different lists and licences. Rules for the weights of large AI models changed in 2025, and their present state needs a check |
| FIPS 140 | Cryptographic modules | NIST SP 800-171 needs validated cryptography where cryptography protects CUI. The use of a strong algorithm is not enough. The module must be on the list of validated modules |

**The encryption rule of ITAR.** 22 CFR 120.54(a)(5) says that it is not an export to send or to store unclassified technical data that is encrypted from end to end. The conditions are:

- the cryptographic modules comply with FIPS 140-2 or its successors;
- the strength is that of AES-128 or more;
- the data is not sent to a proscribed country or to Russia, and is not stored there;
- no third party gets the means to decrypt.

Access to the data in its encrypted form is then not a release. A service that holds the keys does not meet these conditions.

## What is different for a catalog

1. **The catalog itself holds controlled information.** Names, paths, descriptions, field names, statistics, embeddings, previews, the search index, the change stream, logs and backups can each show what the data is. A system that protects the files and publishes their metadata does not protect the data.
2. **Access is the event that the rules control.** The place of storage is only a part. The status of the person and the place from which the person reads are the other parts.
3. **Derived data keeps the control.** A subset, an embedding index, a cache, and in most views a model that was trained on the data, are controlled as their inputs are. The decision for a model is a decision of a person, case by case.
4. **Operators are persons with access.** The staff of a hosted service who can read the database have access to all that it holds.
5. **Each service outside the boundary is a possible release:** an enricher that calls a model service, a search engine in another network, a public description, a feed to another tenant, an agent.
6. **The organisation must show what happened.** It needs a record of each access, and after an incident it must tell in a short time what a person or a credential could reach.

## How mudcat handles each need

| Need | Mechanism in mudcat | State |
|---|---|---|
| Mark what is controlled | **Control marks**: tags with a closed list of values from the rule set, for example the CUI category and the dissemination control (model, section 3) | Added with this profile |
| Marks on a complete project | A tag on a collection applies to all that is under it (ST-17) | In the model |
| Derived data keeps the control | A control mark always propagates along derivation, with the most restrictive value of the inputs (UP-18) | Added with this profile |
| Only an authority lowers a mark | A recorded decision of a principal with the role for that mark (UP-19) | Added with this profile |
| Only permitted persons | Compartments, with conditions on **attributes of the principal** such as the status as a US person, a signed agreement or a completed course. Each attribute has a source and an expiry (AC-9) | Compartments are in the model. Attributes are added with this profile |
| Only from permitted places | The resolver uses the location of the caller (RD-9) | In the requirements |
| Only on permitted storage | Each storage resource has a jurisdiction, and the replica for a read and the target of a replication rule must be permitted (RD-4) | In the requirements |
| No way around the catalog | The enforcement level `catalog` for the storage resource: all credentials come from mudcat, for a short time and for one scope | The level is an open item. Credentials are in the architecture |
| Metadata is protected as the data is | The same access checks for metadata and search results (EV-11, EV-19). The metadata of a tenant stays in its region (OP-11) | In the requirements |
| Nothing controlled leaves through a side door | The **boundary rule**: mudcat sends marked data or metadata to a derived index, an enricher, a subscriber or another tenant only if the configuration permits that receiver for that mark (OP-23) | Added with this profile |
| Operators | Operators are principals. The same checks and the same audit apply to them (AC-10) | Added with this profile |
| Validated cryptography | mudcat uses only the cryptographic modules that the configuration of the deployment lists (OP-24) | Added with this profile |
| A record of each access | The audit log has each decision, with the attributes of the principal and the location of the caller (AC-8, AC-11) | AC-11 is added with this profile |
| Removal that can be shown | Purge with a deletion receipt for each replica (UP-7 to UP-11) | In the model |
| The scope of an incident | Queries for what a principal or a credential could reach and what it resolved, and for each place that holds marked data (QY-13, QY-14) | Added with this profile |
| Markings on what leaves | Each description and each export carries the marks of its entity (UP-20). A public description does not exist for a marked entity | Added with this profile |
| Encrypted data that a service cannot read | The custody level "customer-encrypted" for metadata, and storage that mudcat does not hold keys for ([hosted.md](hosted.md)) | In the hosted design |

## Deployment

**The enclave profile.** For controlled data, the normal deployment is one instance inside the boundary of the organisation. Each component is inside: the database, the search engine, the site agents, the enrichers and the change stream. Nothing connects out. Sign-in uses the identity provider of the organisation, with more than one factor. The sign-in with GitHub or a similar service, which the project proposes for small groups, is not for this profile. Universities operate such enclaves for research under these rules, some with high-performance computers.

**A hosted service.** A hosted mudcat for controlled data must be a separate instance, in a region and with staff that the rules permit, and with the necessary authorization of the service itself. This is a property of the operator and not of the software. The same release runs in both forms (OP-7), so an organisation can start in its enclave.

**Encrypted custody.** The encryption rule of ITAR permits storage with a provider that cannot decrypt. In mudcat this is the custody level "customer-encrypted" for marked attributes, and storage whose keys the customer holds. The price is function: the service cannot search or compute on what it cannot read.

**Open and controlled data together.** An organisation with both kinds uses two tenants: one in the enclave and one outside it. Only descriptions that an authority released go from the first to the second.

## What mudcat does not do

- **mudcat does not make an organisation compliant.** NIST SP 800-171 is for the complete system of an organisation. Many of its families are about people, buildings, training, configuration and response to incidents. mudcat gives functions and evidence for some families: access control, audit, media protection, the protection of communications, and integrity.
- **mudcat does not decide what is controlled.** A person with the authority sets the marks. An enricher can propose a mark, and the proposal needs a review.
- **mudcat cannot stop a read that does not go through it.** On storage with the enforcement level `site` or `none`, a person with direct access can read the data. For such storage, policy is a record and an audit.
- **mudcat does not decide if a model is controlled.** It gives the model the marks of its inputs, and it records the decision of the authority that lowers them.

## Open questions

- Which attributes of a principal the first release has, and who can state them: the identity provider, an administrator of the tenant, or a signed record.
- If embeddings and statistics always get the mark of their inputs, or if a rule can release some kinds.
- How a control mark of one rule set relates to a mark of another, for one entity.
- If the gateway for agents is off by default for marked data.
- Which release of FIPS 140 the lists of modules must follow, because the validations for FIPS 140-2 end.
