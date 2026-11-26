# mudcat tenant encryption

Status: exploration. Nothing in this document is decided, and it adds no requirements and no evals. It examines how the Salesforce Database encrypts the data of each tenant with a key of that tenant, and how much of that design mudcat can use. The reading notes, with sources, are in the research repository (`tenant_encryption.md`).

## What the Salesforce Database does

The Salesforce Database keeps many tenants in the same tables. It encrypts the data of each tenant with a key of that tenant. Three properties make this possible:

1. **Tenant clustering.** The tenant ID is the first part of each key. Thus the records of one tenant are together in storage.
2. **Immutable blocks.** Storage is a log-structured merge tree. An *extent* is like a file, and a *fragment* is like a block in a file. A fragment never changes after the database writes it.
3. **One tenant in each block.** A fragment holds the data of one tenant only. The database encrypts each fragment with the key of that tenant, and puts the ID of the key in the fragment.

The results:

- **Encryption occurs one time for each block,** when a flush or a compaction writes it. A database with mutable pages must encrypt a page again after each change. The paper gives this as the reason that immutable storage is necessary for the design.
- **Rotation uses compaction.** Compaction writes blocks again in all cases, and it uses the newest key. Old blocks stay readable, because each block names its key.
- **The cost is small.** The paper reports about 14 µs for the encryption of a fragment, in a write of 2.7 ms. More than 7,000 tenants have a key of their own, and the largest has more than 80 TB.

The paper does not say how the transaction log and the data in memory are encrypted. Log records of many tenants are mixed, so this is the difficult part. The paper also does not say which key a cloned tenant uses for the blocks that it shares with its source.

## What mudcat has of the three properties

mudcat stores its state in two different ways, and they give different answers.

| Part of mudcat | Tenant clustering | Immutable | One tenant in each unit | Result |
|---|---|---|---|---|
| Manifests, element lists, annotation sets, statistics | Yes: a prefix for each tenant | Yes | Yes | The Salesforce design applies |
| Data files on managed storage | Yes | Yes: a version never changes | Yes | It applies |
| Segments of the change stream, exports, backups of one tenant | Yes | Yes | Yes | It applies |
| Segments of a search or vector index | Only if each tenant has its own index | Yes, in the common engines | Only if each tenant has its own index | It applies if the engine can use a key for each index (TBR) |
| Rows in PostgreSQL | In indexes only | No | No: a page has rows of many tenants | It does not apply |

mudcat already has a flush and a compaction. The architecture says that catalog maintenance **seals** recent member rows into manifest files and **merges** small manifests. A seal is a flush. A merge is a compaction. PostgreSQL has the role of the memory store and the log: it holds the newest rows and the pointers. So mudcat has the same structure as the Salesforce Database for dataset members, and the same gap at the same place.

## A design for the immutable parts

### The unit of encryption is the manifest and the files that it lists

A manifest holds the members of one partition, or of a small range of partition values. This is the mudcat equivalent of a block. Each manifest has a data key of its own. Each data file on managed storage has a data key of its own. Because a manifest belongs to one dataset of one tenant, no unit holds data of two tenants.

### The key tree follows the reference tree

Iceberg table encryption puts the key of each file in the **reference** to that file, and not in the file. mudcat can do the same:

- An entry in a manifest holds the wrapped key of the data file that it lists (Iceberg calls this field `key_metadata`).
- A manifest reference holds the wrapped key of its manifest.
- A small number of **key encryption keys** wrap the keys in the references. The **tenant key** wraps the key encryption keys. The tenant key stays in a key management service, and mudcat never stores it.

One call to the key management service opens a tree of references. The service does not get one call for each file.

| Key | Number | Where it is |
|---|---|---|
| Tenant key | One for each tenant, with versions | In the key management service of the instance, or of the customer |
| Key encryption key | A small number for each tenant, replaced after a period | In the catalog database, wrapped by the tenant key |
| Manifest key | One for each manifest | In the manifest reference, wrapped |
| File key | One for each data file | In the manifest entry of the file, wrapped |

### Encryption occurs at a seal and at a merge

Catalog maintenance encrypts a manifest when it writes the manifest. It does not encrypt at each commit. This is the "one time for each block" property.

### Rotation has three levels

- **A new version of the tenant key:** mudcat wraps the key encryption keys again. This changes a small number of rows and no file.
- **A new key encryption key:** new references use it. Old references keep the old one, which stays as long as a snapshot needs it.
- **New data keys for old data:** a merge writes manifests again with new keys. A rewrite of data files is work of a site agent, and it is an option. Snowflake makes the same separation between rotation and rekeying.

### Cheap copies stay cheap

A clone, or a share with another tenant, makes a new reference to a manifest that exists. With the key in the reference, the new reference holds the same manifest key, wrapped for the tenant that receives it. No file changes.

One decision is open: does a clone stay readable when the source tenant removes its key? With a second wrap, it does. If the source must keep that control, the clone must ask for the key through the source each time.

### Three modes for the data files of a storage resource

mudcat is not in the read path, so the question is who encrypts and who gets the key.

| Mode | Who encrypts | What mudcat gives to a reader | Works with tools that do not know mudcat |
|---|---|---|---|
| None | Nobody, or the storage with a key that mudcat does not know | A location and a credential | Yes |
| Storage | The object store, with a key of the tenant in its key management service | A credential that also permits the use of that key | Yes |
| Client | The writer, with a file key from mudcat. Parquet has its own encryption format. Other files need a format for streams with independent segments, so that range reads work | A location, a credential and the file key | No. The reader needs the mudcat client or an engine that knows the format |

The client mode has an effect outside tenancy. On a file system where mudcat cannot issue credentials, the design says that policy is an audit and not a prevention. If the files are ciphertext, the catalog decides who gets the key. Thus the catalog becomes a point of prevention on storage that it does not control. The limit: a reader that has a key can keep it.

Data on external storage, and data that mudcat refers to only, keep the protection that their owner gave them. mudcat records it and does not change it.

### Hashes and checksums

- The content hash of a version, and the Merkle hash of a snapshot, are hashes of the plaintext. A new key does not change the identity of data. This agrees with the rule that a hash does not depend on the layout.
- A replica in client mode also has a checksum of its ciphertext. A site agent can then verify a replica, and a transfer tool can copy it, with no key.
- A plaintext hash shows that two tenants have the same file. mudcat must not use content hashes to join data across tenants, and must not show a hash to a caller that cannot read the version.

### Removal with keys

The removal of a key removes all that the key protects. This gives a fast purge for a manifest and its files, which is otherwise a rewrite. The provenance record stays, as the design principle "the record outlives the bytes" demands. A legal hold must block the removal of a key in the same way that it blocks a purge.

## The catalog database

The rows in PostgreSQL are the difficult part, for the reason that the paper gives: pages change, and a page holds rows of several tenants. PostgreSQL has no encryption with a key for each tenant inside shared tables. The options:

| Option | What it gives | What it costs |
|---|---|---|
| **A. A database for each tenant.** The dedicated tier, or a database of its own in a shared PostgreSQL cluster with the `pg_tde` extension | A key for each tenant for tables and indexes | No shared tables. `pg_tde` needs the PostgreSQL distribution of Percona, and managed PostgreSQL services do not have it. The write-ahead log has one key for the cluster. The planner statistics, which hold sample values, and the temporary files of queries are not encrypted. Not practical for a very large number of small tenants |
| **B. Encrypted columns.** The service encrypts values with a key of the tenant before it writes a row | A key for each tenant for the values. Shared tables stay | PostgreSQL cannot sort, compare or search the values. Equality needs a blind index, as the customer-encrypted custody level does now |
| **C. A smaller database.** Seal more kinds of records into tenant blocks: versions, provenance records, metadata payloads, audit records. PostgreSQL keeps pointers, recent rows and indexes | Most of the content gets the block design | Indexes in PostgreSQL still hold names and indexed values in the clear. Queries must read blocks or per-tenant indexes |
| **D. A different engine.** A store with tenant-clustered immutable blocks | The complete Salesforce design | The reading notes found no available engine with this function. FoundationDB had a design for per-tenant keys, but its tenant function was experimental, and it is absent from the current main branch |

**Accepted limits.** The owner of the project accepted three limits of PostgreSQL for a key of a tenant. They apply to each form of option A:

- The write-ahead log, and thus its archive, has one key for the cluster.
- The planner statistics in the system catalogs hold sample values, and they do not have the key of the tenant.
- The temporary files of queries do not have the key of the tenant.

With these limits accepted, option A has three forms. All three need a PostgreSQL that the operator runs, and not a managed service.

| Form | Key of the tenant | Shared tables | Limit on the number of tenants |
|---|---|---|---|
| **A1. A cluster for each tenant** (the dedicated tier) | The key of the volume or of the managed service | No | Cost of a cluster for each tenant |
| **A2. A database for each tenant in one cluster,** with `pg_tde` | The principal key of the database, in a key management service | No: the same schema in each database | Thousands (TBR). Each database needs its own connections and its own schema changes |
| **A3. A partition for each tenant in shared tables.** Each partition is in a tablespace of its own, on a volume with its own key | The key of the volume | Yes: one schema, one database, row-level security as now | Hundreds (TBR). Each table needs a list partition for each tenant |

`pg_tde` cannot do A3 alone. In `pg_tde`, each partition gets a different internal key, but one principal key wraps all keys of a database, and no key belongs to a tablespace. A3 thus needs encryption of the volume below each tablespace. That key is open while the volume is in use.

Option B is good for columns that the database never reads: the payload of an outbox row, the details of an audit record, a long description. Such a row is written one time, so the service encrypts it one time. Option B is poor for names and for attributes that queries filter on.

**A recommendation, for discussion:**

1. Give each tenant a tenant key in all tiers. Use it for the immutable parts as described above. This is the largest part of the metadata by volume, and all of the data.
2. In the shared tier, use option B for the columns that the database never reads. State clearly that names, paths and indexed attributes in the shared database have the key of the instance, and not a key of the tenant.
3. Keep the period short in which a row is in PostgreSQL only and not in a sealed block.
4. A tenant that needs a key of its own for the catalog rows gets form A2, a database of its own in a shared cluster. It has a key in a key management service that the tenant can remove, and it needs no volume for each tenant. A move between tiers does not change the keys of records. Form A1 stays for tenants that also need capacity or recovery of their own.
5. Keep the catalog store behind a narrow interface, so that an engine of kind D can replace PostgreSQL later. Do not select such an engine now.

## What a tenant key gives, and what it does not

- **It gives:** separation of tenants at rest. The loss of one key or one file affects one tenant. A tenant can hold its key in its own key management service and remove the access of the service. Removal by key.
- **It does not give:** protection from the running service. In all modes except the client mode, the service gets the key and reads the plaintext. An operator of the service can do the same. Data in memory and in caches is plaintext.
- **The client mode is different for data files.** If only the clients of the tenant can use the tenant key, the service holds ciphertext and wrapped keys for data files. It still reads manifests, unless the customer gateway does that work.

## Other effects

- **Availability.** The service keeps unwrapped keys in memory for a short period. If the key management service of a tenant does not answer, the requests of that tenant fail after that period. Other tenants continue. The same period is the delay before a removal of access has an effect.
- **Statistics, filters and embeddings** show the content of data. They need the same key as the data that they describe. Iceberg uses the same line: it encrypts manifests, which hold statistics, and not the table metadata file, which holds none.
- **Partition summaries in a manifest reference** are values of the data. If they stay in a PostgreSQL row, they are in the clear in the shared tier. They can go in an encrypted list, as in Iceberg, at the cost of one more read.
- **The change stream.** An event holds the values before and after a change. The service encrypts the payload of each event with a key of the tenant, and a subscriber of that tenant gets the key.
- **Search.** A key for each tenant needs an index for each tenant, and an engine that can use a different key for each index.

## Questions for the owner of the project

1. **What must the key protect against?** Separation at rest, where the service can read, is the Salesforce design. A service that cannot read needs the client mode and the customer gateway, and it costs search and statistics in the control plane.
2. **Which form of option A for a tenant with a key: A2 or A3?** A2 has a key that the tenant can remove in a key management service. A3 keeps shared tables.
3. **Is the manifest the correct unit for a data key?** It makes a partition the unit of sharing and of removal by key.
4. **Does a clone depend on the key of its source?**
5. **Is option C wanted for its own sake?** A smaller database also helps the move of a tenant, which is then mostly a change of pointers.
