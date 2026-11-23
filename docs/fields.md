# mudcat field guidelines

Status: starting point. The core documents ([model.md](model.md), [architecture.md](architecture.md) and [requirements.md](requirements.md)) contain no field-specific semantics. This document collects field-specific material, so that each field can be studied in more depth later. The examples here come from the design discussion and the research notes. They are not requirements.

## What a field profile contains

A **field profile** adapts mudcat to one scientific or technical field. It adds to mudcat, but it does not change the core model. A profile can contain these parts:

| Part | Purpose | Core concept it extends |
|---|---|---|
| Schema profile | Maps the field's schema language onto fields and CSVW tables | Schema profiles (model, section 2) |
| Format plugins | Read the field's file formats and produce structure descriptors, metadata and fingerprints | Format plugins (architecture) |
| Semantic vocabularies | Give IRIs for `csvw:propertyUrl`, and units | Fields |
| Identifier schemes | Canonical identifiers of domain entities, for `csvw:valueUrl` and for the content-identity index | Fields, content identity |
| Reference data | Standard reference tables, for example instruments, runs or organisms | Reference data (model, section 3) |
| Source types | Typical sources and their usual terms of use | Sources (model, section 5) |
| Method records | The parameters that describe how the field measures or computes a value | Method records (model, section 6) |
| Similarity criteria | The field's rules for leakage between training and test data | Similarity criteria (model, section 7) |
| Gateways | The field's standard query and access services | Field gateways (architecture) |
| Policy conventions | The field's usual embargo, consent and access rules | Usage policy |

A profile is a plugin with a version. mudcat records the version of the profile with each structure descriptor and each piece of metadata that the profile produced.

## A schema profile, worked example: Felis

Rubin's Felis describes catalog schemas in YAML. It uses JSON-LD-style `@id` keys and maps closely onto CSVW. It is the worked example of a schema profile:

| Felis | mudcat |
|---|---|
| schema | `csvw:TableGroup` |
| table | `csvw:Table` |
| column | `csvw:Column` (a field) |
| `datatype` | `csvw:datatype`, plus `mc:physicalType` |
| `nullable` | The inverse of `csvw:required` |
| `description` | `dcterms:description` |
| `ivoa:ucd` | `csvw:propertyUrl`, which points to the UCD as an IRI. Where the IVOA publishes no IRI form, the profile defines one |
| `ivoa:unit`, `fits:tunit` | `qudt:unit`, with the original string in `mc:unitString` |
| `primaryKey` | `csvw:primaryKey` |
| Foreign-key constraint | `csvw:foreignKey` |
| Unique and check constraints, indexes | `mc:constraint`, `mc:index` |
| `tap:*`, `votable:*` and type overrides for each database | Profile annotations. mudcat keeps them, so that the profile can generate TAP_SCHEMA and database DDL again |

Other schema languages follow the same pattern: the CF conventions, AnnData obs and var schemas, and others below.

## Astronomy

- **Schema profile:** Felis (above).
- **Formats:** FITS images, cubes and tables; HDF5.
  - Format plugin library: astropy.
  - Viewers for the web UI: images and spectra.
- **Semantic vocabularies:** IVOA UCDs as semantic tags; IVOA units.
- **Identifiers:** IVOA identifiers as an external alias scheme.
- **Reference data:** observations, exposures, instruments, filters, calibrations.
- **Gateways:** IVOA TAP. The gateway generates TAP_SCHEMA from the catalog's CSVW descriptions through the Felis profile. ObsCore and DataLink are possible later.
- **Policy conventions:** proprietary periods for each observation, for example `release_date <= now()` unless the caller is in the group of the observing proposal. Data-rights holders as a group in the access model.
- **Shared custody:** Rubin data releases. Several institutions in different countries each hold, process and serve copies, and each wants sovereignty over its copies. This is the motivating case for the shared-custody pattern in the architecture.
- **Spatial selectors and similarity:** sky regions and coordinate overlap.

**Open question:** which UCD IRI form to use, and if mudcat must publish one where the IVOA does not.

## High-energy physics and grid computing

- **Formats:** ROOT files and trees (format plugin library: uproot); HDF5.
- **Reference data:** runs, run ranges, data-quality flags, calibrations, detector conditions.
- **Query examples:** Datacat's query language came from this field, for example `run > 6200 and quality =~ "GO*"` over path globs such as `/raw/**`.
- **Ubiquitous inputs:** detector calibrations and conditions data link every raw file to every output. Their usage role must be marked ubiquitous.
- **Grid credentials:** storage that needs X.509 certificates, or WLCG and SciTokens token profiles.
  - The resolver issues credentials of both kinds.
  - The WLCG delayed its move from X.509 to tokens until about 2028, so sites need both kinds for some years.
- **Movers:** FTS3 and Rucio. Rucio can stay the placement engine at sites that already use it. mudcat then registers the replicas that Rucio creates.

## Materials and chemistry

- **Formats:** CIF and mmCIF; ASE databases; the output of simulation codes.
- **Identifiers:** InChIKey for molecules; identifiers from structure databases; OPTIMADE identifiers.
- **Method records (computation):** level of theory, functional, basis set, pseudopotentials, convergence criteria, code and code version.
  - Values from different methods can be combined only under a declared equivalence.
  - Example: a dataset that mixes two DFT settings is not one consistent training set.
- **Similarity criteria:** chemical scaffold; structure prototype; composition. Training sets are sometimes defined by exclusion against a benchmark set.
- **Provenance:** workflow engines of the field, for example AiiDA, as lineage sources.
- **The experimental loop:** a model proposes a candidate molecule or material, and an experiment or a simulation tests it.

## Biology and the wet lab

- **Formats:**
  - OME-Zarr and microscopy formats;
  - AnnData (h5ad) and TileDB-SOMA for single-cell data;
  - mmCIF for macromolecular structures;
  - instrument output, normalised through the Allotrope Simple Model where possible.
- **Identifiers:** UniProt accessions, PDB identifiers, Cellosaurus cell-line identifiers, ChEMBL compound identifiers.
- **Reference data:** cell lines, compounds, perturbations, plates, wells.
- **Sources:** instrument runs with reagent lots, batches, protocol versions, and the planned procedure compared with the executed one.
- **Method records (measurement):** instrument settings; assay protocol and its version.
  - Assays have versions.
  - Values from different assay versions can be combined only under a declared equivalence.
  - Replicates, censored values, failed runs and negative results are data. mudcat keeps them.
- **Structure:** a microscopy plate is a composite of images and a design table. The design table includes the control wells.
- **Annotation sets:** new assay readouts on existing samples.
- **Similarity criteria:** sequence identity at a stated threshold; structure similarity; splits by time.
- **The experimental loop:** a model proposes a sequence or a compound, and a wet-lab experiment tests it.
- **Lineage sources:** Nextflow and Snakemake are common here, but they are also general workflow engines.

## Genomics and medicine

- **Formats:** FASTQ, BAM, CRAM, VCF; AnnData for single-cell data; DICOM for medical images.
- **Access:** GA4GH DRS for object access; GA4GH Passports and Visas for controlled access; htsget for streaming reads.
- **Policy conventions:**
  - **Consent status per record.** This is a per-object condition that the catalog evaluates in SQL.
  - **Controlled-access enclaves.** These forbid the export of data at the level of individuals. mudcat holds descriptions and lineage for such data, with no replica outside the enclave.
  - **Models as controlled derivatives.** Some providers treat model weights trained on controlled data as controlled derivatives. The duties of the source propagate to the model.
- **Clinical data models:** OMOP as reference data.

## Earth and geospatial

- **Schema profile:** the CF conventions. `standard_name` maps to `csvw:propertyUrl`, and `units` maps to `qudt:unit`.
- **Formats:** netCDF, Zarr, GRIB, GeoTIFF, GeoZarr.
- **Catalog standard:** STAC, as a gateway and as an import format.
- **Representations:** raw archive, cloud-optimised and analysis-ready forms of one dataset are representations of one snapshot.
- **Similarity criteria:** time ranges (test periods that no model saw in training); spatial regions.
- **Time append:** datasets that grow each month are dataset series. Each update makes a new snapshot.

## Robotics and multimodal AI data

- **Formats:** MCAP and ROS bags; episode datasets that pack many episodes into shared Parquet and video files.
- **Structure:** an episode is a composite of video streams, sensor streams and action records, aligned by time.
- **Selectors:** clips and episode boundaries as time spans.

## Further work

Each field section can grow into a full profile. The likely order follows the first users of mudcat:

1. biology and the wet lab;
2. materials and chemistry;
3. earth and geospatial;
4. astronomy;
5. high-energy physics;
6. genomics and medicine.
