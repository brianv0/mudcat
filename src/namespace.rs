//! Namespace and storage: collections, artifacts, versions, replicas and storage resources
//! (model.md, section 1). Registration is idempotent on (artifact, content checksum) (OP-5).

use crate::authz;
use crate::error::{Error, Result};
use crate::instance::{new_id, Tx};
use crate::storage::{url_of, BucketConfig};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use uuid::Uuid;

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct ResourceSpec {
    pub name: String,
    pub location: String,
    pub jurisdiction: String,
    /// managed | external
    pub mode: String,
    /// s3 | posix
    #[serde(default = "s3")]
    pub protocol: String,
    #[serde(default)]
    pub versioned: bool,
    #[serde(default = "yes")]
    pub egress_allowed: bool,
    /// The bucket in the store. The default is the name of the resource, qualified by the tenant.
    pub bucket: Option<String>,
    #[serde(default)]
    pub attrs: Value,
}
fn s3() -> String {
    "s3".into()
}
fn yes() -> bool {
    true
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Resource {
    pub id: Uuid,
    pub name: String,
    pub location: String,
    pub jurisdiction: String,
    pub mode: String,
    pub protocol: String,
    pub versioned: bool,
    pub egress_allowed: bool,
    pub bucket: String,
    pub attrs: Value,
}

impl Resource {
    pub fn url(&self, key: &str) -> String {
        url_of(&self.bucket, key, self.protocol == "posix")
    }
}

fn resource_from_row(r: &tokio_postgres::Row) -> Resource {
    Resource {
        id: r.get("id"),
        name: r.get("name"),
        location: r.get("location"),
        jurisdiction: r.get("jurisdiction"),
        mode: r.get("mode"),
        protocol: r.get("protocol"),
        versioned: r.get("versioned"),
        egress_allowed: r.get("egress_allowed"),
        bucket: r.get("bucket"),
        attrs: r.get("attrs"),
    }
}

pub async fn create_resource(tx: &mut Tx<'_>, spec: ResourceSpec) -> Result<Resource> {
    let id = new_id();
    let tenant = tx.sess.tenant_id()?;
    let bucket = spec.bucket.clone().unwrap_or_else(|| format!("{}-{}", &tenant.to_string()[..8], spec.name));
    let attrs = if spec.attrs.is_null() { json!({}) } else { spec.attrs.clone() };
    tx.exec(
        "insert into storage_resource (id, name, location, jurisdiction, mode, protocol, versioned, egress_allowed, bucket, attrs)
         values ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)",
        &[&id, &spec.name, &spec.location, &spec.jurisdiction, &spec.mode, &spec.protocol, &spec.versioned, &spec.egress_allowed, &bucket, &attrs],
    )
    .await?;
    tx.inst().store.create_bucket(&bucket, BucketConfig { versioned: spec.versioned, posix: spec.protocol == "posix" })?;
    tx.emit("storage.resource.changed", id, "storage-resource", None, json!({"name": spec.name, "location": spec.location})).await?;
    resource(tx, &spec.name).await
}

pub async fn resource(tx: &Tx<'_>, name: &str) -> Result<Resource> {
    let r = tx.qopt("select * from storage_resource where name = $1", &[&name]).await?.ok_or(Error::NotFound)?;
    Ok(resource_from_row(&r))
}

pub async fn resource_by_id(tx: &Tx<'_>, id: Uuid) -> Result<Resource> {
    let r = tx.qopt("select * from storage_resource where id = $1", &[&id]).await?.ok_or(Error::NotFound)?;
    Ok(resource_from_row(&r))
}

pub async fn resources(tx: &Tx<'_>) -> Result<Vec<Resource>> {
    Ok(tx.q("select * from storage_resource order by name", &[]).await?.iter().map(resource_from_row).collect())
}

// ---------------------------------------------------------------------------- identifiers

/// Point an identifier to an entity. The old binding stays in the history (UP-13).
pub async fn set_identifier(tx: &mut Tx<'_>, scheme: &str, value: &str, entity: Uuid) -> Result<()> {
    let commit = tx.commit_id().await?;
    tx.exec(
        "update identifier set valid_to = $3 where scheme = $1 and value = $2 and valid_to is null",
        &[&scheme, &value, &commit],
    )
    .await?;
    tx.exec(
        "insert into identifier (scheme, value, entity_id, valid_from) values ($1,$2,$3,$4)
         on conflict (tenant_id, scheme, value, valid_from) do update set entity_id = excluded.entity_id",
        &[&scheme, &value, &entity, &commit],
    )
    .await?;
    Ok(())
}

pub async fn remove_identifier(tx: &mut Tx<'_>, scheme: &str, value: &str) -> Result<()> {
    let commit = tx.commit_id().await?;
    tx.exec("update identifier set valid_to = $3 where scheme = $1 and value = $2 and valid_to is null", &[&scheme, &value, &commit])
        .await?;
    Ok(())
}

/// Resolve an identifier to exactly one ID (ID-2), now or as of a commit.
pub async fn resolve_identifier(tx: &Tx<'_>, scheme: &str, value: &str, as_of_commit: Option<i64>) -> Result<Option<Uuid>> {
    let row = match as_of_commit {
        None => tx.qopt("select entity_id from identifier where scheme=$1 and value=$2 and valid_to is null", &[&scheme, &value]).await?,
        Some(c) => {
            tx.qopt(
                "select entity_id from identifier where scheme=$1 and value=$2 and valid_from <= $3 and (valid_to is null or valid_to > $3)",
                &[&scheme, &value, &c],
            )
            .await?
        }
    };
    Ok(row.map(|r| r.get(0)))
}

pub async fn identifiers_of(tx: &Tx<'_>, entity: Uuid) -> Result<Vec<(String, String)>> {
    Ok(tx
        .q("select scheme, value from identifier where entity_id = $1 and valid_to is null order by scheme, value", &[&entity])
        .await?
        .iter()
        .map(|r| (r.get(0), r.get(1)))
        .collect())
}

/// The last commit at or before a time (EV-14).
pub async fn commit_at(tx: &Tx<'_>, t: chrono::DateTime<chrono::Utc>) -> Result<i64> {
    let r = tx.q1("select coalesce(max(commit_id), 0) from commit_log where at <= $1", &[&t]).await?;
    Ok(r.get(0))
}

// ---------------------------------------------------------------------------- collections

pub fn parent_path(path: &str) -> Option<&str> {
    if path == "/" {
        return None;
    }
    match path.rfind('/') {
        Some(0) => Some("/"),
        Some(i) => Some(&path[..i]),
        None => None,
    }
}

pub fn base_name(path: &str) -> &str {
    path.rsplit('/').next().unwrap_or(path)
}

#[derive(Clone, Debug)]
pub struct Collection {
    pub id: Uuid,
    pub path: String,
    pub acl_root: Uuid,
    pub has_acl: bool,
    pub resource_id: Option<Uuid>,
    pub prefix: Option<String>,
}

fn collection_from_row(r: &tokio_postgres::Row) -> Collection {
    Collection {
        id: r.get("id"),
        path: r.get("path"),
        acl_root: r.get("acl_root_id"),
        has_acl: r.get("has_acl"),
        resource_id: r.get("resource_id"),
        prefix: r.get("prefix"),
    }
}

pub async fn collection(tx: &Tx<'_>, path: &str) -> Result<Collection> {
    let r = tx.qopt("select * from collection where path = $1", &[&path]).await?.ok_or(Error::NotFound)?;
    Ok(collection_from_row(&r))
}

pub async fn collection_by_id(tx: &Tx<'_>, id: Uuid) -> Result<Collection> {
    let r = tx.qopt("select * from collection where id = $1", &[&id]).await?.ok_or(Error::NotFound)?;
    Ok(collection_from_row(&r))
}

/// The collection at a path, made with its ancestors if it does not exist.
pub async fn ensure_collection(tx: &mut Tx<'_>, path: &str) -> Result<Collection> {
    if let Some(r) = tx.qopt("select * from collection where path = $1", &[&path]).await? {
        return Ok(collection_from_row(&r));
    }
    let commit = tx.commit_id().await?;
    let id = new_id();
    let (parent_id, acl_root, has_acl, resource, prefix) = match parent_path(path) {
        None => (None, id, true, None, None),
        Some(pp) => {
            let parent = Box::pin(ensure_collection(tx, pp)).await?;
            // a mount on an ancestor gives this collection a prefix under the same resource
            let prefix = parent.prefix.as_ref().map(|p| format!("{}{}/", p, base_name(path)));
            (Some(parent.id), parent.acl_root, false, parent.resource_id, prefix)
        }
    };
    tx.exec(
        "insert into collection (id, path, parent_id, has_acl, acl_root_id, resource_id, prefix, created_commit)
         values ($1,$2,$3,$4,$5,$6,$7,$8) on conflict do nothing",
        &[&id, &path, &parent_id, &has_acl, &acl_root, &resource, &prefix, &commit],
    )
    .await?;
    let c = collection(tx, path).await?;
    if c.id == id {
        set_identifier(tx, "path", path, id).await?;
        if let Some(p) = parent_id {
            authz::write_structural(tx, "collection", &id.to_string(), "parent", "collection", &p.to_string()).await?;
        }
        tx.emit("collection.created", id, "collection", Some(c.acl_root), json!({"path": path})).await?;
    }
    Ok(c)
}

/// Give a collection its own ACL. It becomes the ACL root of each collection under it that has
/// no ACL of its own (AC-5).
pub async fn set_acl_root(tx: &mut Tx<'_>, path: &str) -> Result<Collection> {
    let c = ensure_collection(tx, path).await?;
    if c.has_acl {
        return Ok(c);
    }
    let old_root = c.acl_root;
    tx.exec("update collection set has_acl = true, acl_root_id = id where id = $1", &[&c.id]).await?;
    let like = format!("{}/%", path.trim_end_matches('/'));
    tx.exec("update collection set acl_root_id = $1 where path like $2 and acl_root_id = $3", &[&c.id, &like, &old_root]).await?;
    authz::write_structural(tx, "collection", &c.id.to_string(), "acl_root", "collection", &c.id.to_string()).await?;
    tx.emit("collection.acl-changed", c.id, "collection", Some(c.id), json!({"path": path})).await?;
    collection(tx, path).await
}

/// Mount a storage resource on a collection: the subtree maps to a prefix on the resource, so
/// that an ACL root can have its own storage prefix (model.md, "Storage layout should follow
/// access boundaries").
pub async fn mount(tx: &mut Tx<'_>, path: &str, resource: &str, prefix: &str) -> Result<()> {
    let c = ensure_collection(tx, path).await?;
    let r = self::resource(tx, resource).await?;
    tx.exec("update collection set resource_id = $2, prefix = $3 where id = $1", &[&c.id, &r.id, &prefix]).await?;
    let like = format!("{}/%", path.trim_end_matches('/'));
    let rows = tx.q("select id, path from collection where path like $1", &[&like]).await?;
    for row in rows {
        let p: String = row.get(1);
        let rel = &p[path.trim_end_matches('/').len() + 1..];
        let id: Uuid = row.get(0);
        tx.exec("update collection set resource_id = $2, prefix = $3 where id = $1", &[&id, &r.id, &format!("{prefix}{rel}/")]).await?;
    }
    Ok(())
}

// ---------------------------------------------------------------------------- registration

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Checksum {
    pub algorithm: String,
    pub value: String,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct ReplicaSpec {
    pub resource: String,
    /// The key on the resource. The default comes from the mount of the collection.
    pub key: Option<String>,
    #[serde(default)]
    pub representation: Option<Uuid>,
    /// The state of the new replica: registered by default.
    #[serde(default)]
    pub state: Option<String>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Register {
    pub path: String,
    #[serde(default)]
    pub kind: Option<String>,
    pub content_hash: Option<String>,
    #[serde(default)]
    pub checksums: Vec<Checksum>,
    #[serde(default)]
    pub size: i64,
    #[serde(default)]
    pub structure: Option<Value>,
    #[serde(default)]
    pub format: Option<String>,
    /// The name of the source of the version (UP-1).
    #[serde(default)]
    pub source: Option<String>,
    #[serde(default)]
    pub replica: Option<ReplicaSpec>,
    /// The element list, for structures that enumerate elements.
    #[serde(default)]
    pub elements: Option<Vec<Value>>,
    #[serde(default)]
    pub label: Option<String>,
    /// The ID that the producing tenant minted (ID-6).
    #[serde(default)]
    pub id: Option<Uuid>,
    /// A near-duplicate fingerprint of the content.
    #[serde(default)]
    pub fingerprint: Option<String>,
    /// A domain key, for example the prototype of a structure.
    #[serde(default)]
    pub domain_key: Option<String>,
    /// Statistics of the version: time span, extent, counts.
    #[serde(default)]
    pub stats: Option<Value>,
    /// A delete marker (model.md, removal table).
    #[serde(default)]
    pub delete_marker: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Registered {
    pub artifact_id: Uuid,
    pub version_id: Uuid,
    pub number: i32,
    pub created: bool,
    pub replica_id: Option<Uuid>,
}

/// The structure kinds of ST-1.
pub const STRUCTURE_KINDS: &[&str] = &["opaque", "container", "records", "array", "stream", "graph", "composite"];

pub async fn register(tx: &mut Tx<'_>, req: &Register) -> Result<Registered> {
    let parent = parent_path(&req.path).ok_or_else(|| Error::invalid("an artifact needs a collection"))?.to_string();
    let coll = ensure_collection(tx, &parent).await?;
    register_in(tx, &coll, req).await
}

/// Register a batch in one transaction. The write permission is checked once for each collection.
pub async fn register_batch(tx: &mut Tx<'_>, reqs: &[Register]) -> Result<Vec<Registered>> {
    let mut colls: std::collections::HashMap<String, Collection> = Default::default();
    let mut out = Vec::with_capacity(reqs.len());
    for r in reqs {
        let parent = parent_path(&r.path).ok_or_else(|| Error::invalid("an artifact needs a collection"))?.to_string();
        if !colls.contains_key(&parent) {
            let c = ensure_collection(tx, &parent).await?;
            authz::require(tx, "registration", "write", "collection", c.id).await?;
            colls.insert(parent.clone(), c);
        }
    }
    let mut paths = std::collections::HashSet::new();
    let bulk = reqs.len() >= 8 && reqs.iter().all(|r| !r.delete_marker && r.content_hash.is_some() && paths.insert(r.path.as_str()));
    if bulk {
        return register_bulk(tx, &colls, reqs).await;
    }
    for r in reqs {
        let parent = parent_path(&r.path).unwrap().to_string();
        let c = colls[&parent].clone();
        out.push(register_in(tx, &c, r).await?);
    }
    Ok(out)
}

/// Registration of a batch with set-based statements: a few statements for the whole batch,
/// whatever its size (architecture.md, "Ingest writer"). Idempotent on (artifact, checksum).
async fn register_bulk(tx: &mut Tx<'_>, colls: &std::collections::HashMap<String, Collection>, reqs: &[Register]) -> Result<Vec<Registered>> {
    use std::collections::HashMap;
    let commit = tx.commit_id().await?;
    let tenant = tx.sess.tenant_id()?;
    let mut sources: HashMap<String, Uuid> = HashMap::new();
    for r in reqs {
        if let Some(n) = &r.source {
            if !sources.contains_key(n) {
                let id = source_id(tx, &r.source).await?.unwrap();
                sources.insert(n.clone(), id);
            }
        }
        let k = r.structure.as_ref().and_then(|s| s["kind"].as_str()).unwrap_or("opaque");
        if !STRUCTURE_KINDS.contains(&k) {
            return Err(Error::invalid(format!("unknown structure kind {k}")));
        }
    }
    let coll_of = |r: &Register| colls[parent_path(&r.path).unwrap()].clone();
    let cids: Vec<Uuid> = reqs.iter().map(|r| coll_of(r).id).collect();
    let names: Vec<String> = reqs.iter().map(|r| base_name(&r.path).to_string()).collect();
    let mut artifacts: HashMap<(Uuid, String), Uuid> = HashMap::new();
    for row in tx
        .q("select a.id, a.collection_id, a.name from artifact a join unnest($1::uuid[], $2::text[]) as x(c, n) on a.collection_id = x.c and a.name = x.n", &[&cids, &names])
        .await?
    {
        artifacts.insert((row.get(1), row.get(2)), row.get(0));
    }
    let existing_aids: Vec<Uuid> = artifacts.values().copied().collect();
    let mut known: HashMap<(Uuid, String), (Uuid, i32)> = HashMap::new();
    let mut max_number: HashMap<Uuid, i32> = HashMap::new();
    if !existing_aids.is_empty() {
        for row in tx.q("select artifact_id, content_hash, id, number from version where artifact_id = any($1) and content_hash is not null", &[&existing_aids]).await? {
            let a: Uuid = row.get(0);
            let n: i32 = row.get(3);
            known.insert((a, row.get(1)), (row.get(2), n));
            let m = max_number.entry(a).or_insert(0);
            *m = (*m).max(n);
        }
    }
    let mut out = Vec::with_capacity(reqs.len());
    let mut events = Vec::new();
    let (mut a_ids, mut a_colls, mut a_names, mut a_kinds) = (vec![], vec![], vec![], vec![]);
    let (mut id_values, mut id_entities) = (vec![], vec![]);
    let (mut v_ids, mut v_aids, mut v_nums, mut v_labels, mut v_hashes, mut v_checksums, mut v_sizes, mut v_counts) = (vec![], vec![], vec![], vec![], vec![], vec![], vec![], vec![]);
    let (mut v_structs, mut v_formats, mut v_sources, mut v_manifests, mut v_stats) = (vec![], vec![], vec![], vec![], vec![]);
    let (mut ix_kind, mut ix_key, mut ix_v, mut ix_e) = (vec![], vec![], vec![], vec![]);
    let mut changed_artifacts = Vec::new();
    let mut replica_jobs: Vec<(Uuid, usize)> = Vec::new();
    for (i, r) in reqs.iter().enumerate() {
        let c = coll_of(r);
        let name = names[i].clone();
        let hash = r.content_hash.clone().unwrap();
        let (aid, new_artifact) = match artifacts.get(&(c.id, name.clone())) {
            Some(a) => (*a, false),
            None => {
                let id = new_id();
                a_ids.push(id);
                a_colls.push(c.id);
                a_names.push(name.clone());
                a_kinds.push(r.kind.clone().unwrap_or_else(|| "data".into()));
                id_values.push(r.path.clone());
                id_entities.push(id);
                events.push(tx.event_json(commit, "artifact.registered", id, "artifact", 1, Some(c.acl_root), false, json!({"path": r.path})));
                (id, true)
            }
        };
        if !new_artifact {
            if let Some((vid, n)) = known.get(&(aid, hash.clone())) {
                out.push(Registered { artifact_id: aid, version_id: *vid, number: *n, created: false, replica_id: None });
                if r.replica.is_some() {
                    replica_jobs.push((*vid, i));
                }
                continue;
            }
            changed_artifacts.push(aid);
        }
        let vid = r.id.unwrap_or_else(new_id);
        let number = max_number.get(&aid).copied().unwrap_or(0) + 1;
        let structure = r.structure.clone().unwrap_or_else(|| json!({"kind": "opaque"}));
        let source = r.source.as_ref().map(|n| sources[n]);
        let mut checksums = r.checksums.clone();
        if !checksums.iter().any(|c| c.algorithm == "sha256") {
            checksums.push(Checksum { algorithm: "sha256".into(), value: hash.clone() });
        }
        let element_count = match &r.elements {
            Some(e) => e.len() as i64,
            None => structure["element_count"].as_i64().unwrap_or(1),
        };
        let manifest = match &r.elements {
            Some(elements) if !elements.is_empty() => {
                let mid = new_id();
                let rows: Vec<Value> = elements
                    .iter()
                    .map(|e| {
                        let mut e = e.clone();
                        if e.get("source").is_none() {
                            if let Some(s) = source {
                                e["source"] = json!(s);
                            }
                        }
                        e
                    })
                    .collect();
                tx.inst().manifests.write(tenant, mid, &rows)?;
                for e in elements {
                    let eid = e["id"].as_str().map(|s| s.to_string()).unwrap_or_else(|| e["id"].to_string());
                    for (k, f) in [("checksum", "checksum"), ("fingerprint", "fingerprint"), ("domain-key", "domain_key")] {
                        if let Some(v) = e[f].as_str() {
                            ix_kind.push(k.to_string());
                            ix_key.push(v.to_string());
                            ix_v.push(vid);
                            ix_e.push(eid.clone());
                        }
                    }
                }
                Some(mid)
            }
            _ => None,
        };
        for (k, v) in [("checksum", Some(hash.clone())), ("fingerprint", r.fingerprint.clone()), ("domain-key", r.domain_key.clone())] {
            if let Some(v) = v {
                ix_kind.push(k.to_string());
                ix_key.push(v);
                ix_v.push(vid);
                ix_e.push(String::new());
            }
        }
        let skind = structure["kind"].as_str().unwrap_or("opaque").to_string();
        events.push(tx.event_json(commit, "version.registered", vid, "version", 1, Some(c.acl_root), false,
            json!({"artifact": aid, "path": r.path, "number": number, "content_hash": hash, "size": r.size, "structure": skind})));
        v_ids.push(vid);
        v_aids.push(aid);
        v_nums.push(number);
        v_labels.push(r.label.clone());
        v_hashes.push(hash);
        v_checksums.push(serde_json::to_value(&checksums)?);
        v_sizes.push(r.size);
        v_counts.push(element_count);
        v_structs.push(structure);
        v_formats.push(r.format.clone());
        v_sources.push(source);
        v_manifests.push(manifest);
        v_stats.push(r.stats.clone().unwrap_or_else(|| json!({})));
        out.push(Registered { artifact_id: aid, version_id: vid, number, created: true, replica_id: None });
        if r.replica.is_some() {
            replica_jobs.push((vid, i));
        }
    }
    if !a_ids.is_empty() {
        tx.exec(
            "insert into artifact (id, collection_id, name, kind, created_commit) select *, $5 from unnest($1::uuid[], $2::uuid[], $3::text[], $4::text[])",
            &[&a_ids, &a_colls, &a_names, &a_kinds, &commit],
        )
        .await?;
        tx.exec(
            "insert into identifier (scheme, value, entity_id, valid_from) select 'path', v, e, $3 from unnest($1::text[], $2::uuid[]) as x(v, e)",
            &[&id_values, &id_entities, &commit],
        )
        .await?;
        tx.exec("insert into entity (id, kind, revision, created_commit) select x, 'artifact', 1, $2 from unnest($1::uuid[]) x", &[&a_ids, &commit]).await?;
    }
    if !v_ids.is_empty() {
        tx.exec(
            "insert into version (id, artifact_id, number, label, content_hash, checksums, size, element_count, structure, format, source_id, element_manifest, stats, created_commit)
             select *, $14 from unnest($1::uuid[], $2::uuid[], $3::int[], $4::text[], $5::text[], $6::jsonb[], $7::bigint[], $8::bigint[], $9::jsonb[], $10::text[], $11::uuid[], $12::uuid[], $13::jsonb[])",
            &[&v_ids, &v_aids, &v_nums, &v_labels, &v_hashes, &v_checksums, &v_sizes, &v_counts, &v_structs, &v_formats, &v_sources, &v_manifests, &v_stats, &commit],
        )
        .await?;
        tx.exec("update artifact a set latest_version_id = x.v from unnest($1::uuid[], $2::uuid[]) as x(a, v) where a.id = x.a", &[&v_aids, &v_ids]).await?;
        tx.exec("insert into entity (id, kind, revision, created_commit) select x, 'version', 1, $2 from unnest($1::uuid[]) x", &[&v_ids, &commit]).await?;
    }
    if !ix_kind.is_empty() {
        tx.exec(
            "insert into identity_index (key_kind, key, version_id, element_id) select * from unnest($1::text[], $2::text[], $3::uuid[], $4::text[]) on conflict do nothing",
            &[&ix_kind, &ix_key, &ix_v, &ix_e],
        )
        .await?;
    }
    if !changed_artifacts.is_empty() {
        for row in tx.q("update entity set revision = revision + 1 where id = any($1) returning id, revision", &[&changed_artifacts]).await? {
            let a: Uuid = row.get(0);
            events.push(tx.event_json(commit, "artifact.changed", a, "artifact", row.get(1), None, false, json!({"new_version": true})));
        }
    }
    // replicas
    let mut resources: HashMap<String, Resource> = HashMap::new();
    let (mut r_ids, mut r_vs, mut r_res, mut r_reps, mut r_urls, mut r_states) = (vec![], vec![], vec![], vec![], vec![], vec![]);
    let existing_replicas: std::collections::HashSet<(Uuid, String)> = {
        let vs: Vec<Uuid> = replica_jobs.iter().map(|j| j.0).collect();
        tx.q("select version_id, url from replica where version_id = any($1)", &[&vs]).await?.iter().map(|r| (r.get(0), r.get(1))).collect()
    };
    for (vid, i) in &replica_jobs {
        let r = &reqs[*i];
        let spec = r.replica.as_ref().unwrap();
        if !resources.contains_key(&spec.resource) {
            resources.insert(spec.resource.clone(), resource(tx, &spec.resource).await?);
        }
        let res = &resources[&spec.resource];
        let c = coll_of(r);
        let key = spec.key.clone().unwrap_or_else(|| format!("{}{}", c.prefix.clone().unwrap_or_default(), names[*i]));
        let url = res.url(&key);
        if existing_replicas.contains(&(*vid, url.clone())) {
            continue;
        }
        let rid = new_id();
        let state = spec.state.clone().unwrap_or_else(|| "registered".into());
        events.push(tx.event_json(commit, "replica.registered", rid, "replica", 1, Some(c.acl_root), false, json!({"version": vid, "resource": res.name, "url": url, "state": state})));
        r_ids.push(rid);
        r_vs.push(*vid);
        r_res.push(res.id);
        r_reps.push(spec.representation);
        r_urls.push(url);
        r_states.push(state);
        if let Some(o) = out.iter_mut().find(|o| o.version_id == *vid) {
            o.replica_id = Some(rid);
        }
    }
    if !r_ids.is_empty() {
        tx.exec(
            "insert into replica (id, version_id, resource_id, representation_id, url, state, created_commit) select *, $7 from unnest($1::uuid[], $2::uuid[], $3::uuid[], $4::uuid[], $5::text[], $6::text[])",
            &[&r_ids, &r_vs, &r_res, &r_reps, &r_urls, &r_states, &commit],
        )
        .await?;
        tx.exec("insert into replica_history (replica_id, commit_id, state) select x, $3, s from unnest($1::uuid[], $2::text[]) as y(x, s)", &[&r_ids, &r_states, &commit]).await?;
        tx.exec("insert into entity (id, kind, revision, created_commit) select x, 'replica', 1, $2 from unnest($1::uuid[]) x", &[&r_ids, &commit]).await?;
    }
    tx.emit_many(events).await?;
    Ok(out)
}

async fn source_id(tx: &Tx<'_>, name: &Option<String>) -> Result<Option<Uuid>> {
    match name {
        None => Ok(None),
        Some(n) => {
            let r = tx.qopt("select id from source where name = $1", &[n]).await?;
            r.map(|r| Some(r.get(0))).ok_or_else(|| Error::invalid(format!("unknown source {n}")))
        }
    }
}

async fn register_in(tx: &mut Tx<'_>, coll: &Collection, req: &Register) -> Result<Registered> {
    let name = base_name(&req.path).to_string();
    let kind = req.kind.clone().unwrap_or_else(|| "data".into());
    let source = source_id(tx, &req.source).await?;
    if !req.delete_marker && req.content_hash.is_none() {
        return Err(Error::invalid("a version needs a content checksum (ID-4)"));
    }
    let structure = req.structure.clone().unwrap_or_else(|| json!({"kind": "opaque"}));
    let skind = structure["kind"].as_str().unwrap_or("opaque");
    if !STRUCTURE_KINDS.contains(&skind) {
        return Err(Error::invalid(format!("unknown structure kind {skind}")));
    }

    // the artifact, by its path
    let existing = tx.qopt("select id from artifact where collection_id = $1 and name = $2", &[&coll.id, &name]).await?;
    let (artifact_id, new_artifact) = match existing {
        Some(r) => (r.get::<_, Uuid>(0), false),
        None => {
            let id = new_id();
            let commit = tx.commit_id().await?;
            tx.exec(
                "insert into artifact (id, collection_id, name, kind, created_commit) values ($1,$2,$3,$4,$5)",
                &[&id, &coll.id, &name, &kind, &commit],
            )
            .await?;
            set_identifier(tx, "path", &req.path, id).await?;
            tx.emit("artifact.registered", id, "artifact", Some(coll.acl_root), json!({"path": req.path, "kind": kind})).await?;
            (id, true)
        }
    };

    // idempotency: the same artifact and checksum is the same version (OP-5, INV-13)
    if let Some(h) = &req.content_hash {
        if !new_artifact {
            if let Some(r) = tx.qopt("select id, number from version where artifact_id = $1 and content_hash = $2", &[&artifact_id, h]).await? {
                let vid: Uuid = r.get(0);
                let replica_id = match &req.replica {
                    Some(spec) => add_replica_quiet(tx, vid, coll, &name, spec).await?,
                    None => None,
                };
                return Ok(Registered { artifact_id, version_id: vid, number: r.get(1), created: false, replica_id });
            }
        }
    }

    let commit = tx.commit_id().await?;
    let vid = req.id.unwrap_or_else(new_id);
    let number: i32 = tx
        .q1("select coalesce(max(number), 0) + 1 from version where artifact_id = $1", &[&artifact_id])
        .await?
        .get(0);
    let mut checksums = req.checksums.clone();
    if let Some(h) = &req.content_hash {
        if !checksums.iter().any(|c| c.algorithm == "sha256") {
            checksums.push(Checksum { algorithm: "sha256".into(), value: h.clone() });
        }
    }
    let element_count: i64 = match (&req.elements, skind) {
        (Some(e), _) => e.len() as i64,
        (None, _) => structure["element_count"].as_i64().unwrap_or(1),
    };
    // the element list goes to the manifest store, not to the catalog database (PF-2)
    let element_manifest = match &req.elements {
        Some(elements) if !elements.is_empty() => {
            let mid = new_id();
            let tenant = tx.sess.tenant_id()?;
            let rows: Vec<Value> = elements
                .iter()
                .map(|e| {
                    let mut e = e.clone();
                    if e.get("source").is_none() {
                        if let Some(s) = source {
                            e["source"] = json!(s);
                        }
                    }
                    e
                })
                .collect();
            tx.inst().manifests.write(tenant, mid, &rows)?;
            Some(mid)
        }
        _ => None,
    };
    let stats = req.stats.clone().unwrap_or_else(|| json!({}));
    let checksums_v = serde_json::to_value(&checksums)?;
    tx.exec(
        "insert into version (id, artifact_id, number, label, content_hash, checksums, size, element_count, structure, format,
                              source_id, is_delete_marker, element_manifest, stats, created_commit)
         values ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)",
        &[
            &vid, &artifact_id, &number, &req.label, &req.content_hash, &checksums_v, &req.size, &element_count, &structure,
            &req.format, &source, &req.delete_marker, &element_manifest, &stats, &commit,
        ],
    )
    .await?;
    tx.exec("update artifact set latest_version_id = $2 where id = $1", &[&artifact_id, &vid]).await?;
    if let Some(h) = &req.content_hash {
        tx.exec(
            "insert into identity_index (key_kind, key, version_id) values ('checksum', $1, $2) on conflict do nothing",
            &[h, &vid],
        )
        .await?;
    }
    if let Some(f) = &req.fingerprint {
        tx.exec("insert into identity_index (key_kind, key, version_id) values ('fingerprint', $1, $2) on conflict do nothing", &[f, &vid])
            .await?;
    }
    if let Some(d) = &req.domain_key {
        tx.exec("insert into identity_index (key_kind, key, version_id) values ('domain-key', $1, $2) on conflict do nothing", &[d, &vid])
            .await?;
    }
    if let Some(elements) = &req.elements {
        index_elements(tx, vid, elements).await?;
    }
    let typ = if req.delete_marker { "artifact.delete-marker-written" } else { "version.registered" };
    tx.emit(
        typ,
        vid,
        "version",
        Some(coll.acl_root),
        json!({"artifact": artifact_id, "path": req.path, "number": number, "content_hash": req.content_hash, "size": req.size, "structure": skind}),
    )
    .await?;
    // the artifact changed: its latest version is new
    if !new_artifact {
        tx.emit("artifact.changed", artifact_id, "artifact", Some(coll.acl_root), json!({"latest": vid, "path": req.path})).await?;
    }
    let replica_id = match &req.replica {
        Some(spec) => add_replica_quiet(tx, vid, coll, &name, spec).await?,
        None => None,
    };
    Ok(Registered { artifact_id, version_id: vid, number, created: true, replica_id })
}

/// Index the checksums, fingerprints and domain keys of elements (the content-identity index).
async fn index_elements(tx: &Tx<'_>, vid: Uuid, elements: &[Value]) -> Result<()> {
    let mut kinds = Vec::new();
    let mut keys = Vec::new();
    let mut eids = Vec::new();
    for e in elements {
        let eid = e["id"].as_str().map(|s| s.to_string()).unwrap_or_else(|| e["id"].to_string());
        for (k, field) in [("checksum", "checksum"), ("fingerprint", "fingerprint"), ("domain-key", "domain_key")] {
            if let Some(v) = e[field].as_str() {
                kinds.push(k.to_string());
                keys.push(v.to_string());
                eids.push(eid.clone());
            }
        }
    }
    if !kinds.is_empty() {
        tx.exec(
            "insert into identity_index (key_kind, key, version_id, element_id)
             select k, v, $1, e from unnest($2::text[], $3::text[], $4::text[]) as t(k, v, e) on conflict do nothing",
            &[&vid, &kinds, &keys, &eids],
        )
        .await?;
    }
    Ok(())
}

async fn add_replica_quiet(tx: &mut Tx<'_>, vid: Uuid, coll: &Collection, name: &str, spec: &ReplicaSpec) -> Result<Option<Uuid>> {
    let res = resource(tx, &spec.resource).await?;
    let key = match &spec.key {
        Some(k) => k.clone(),
        None => format!("{}{}", coll.prefix.clone().unwrap_or_default(), name),
    };
    let url = res.url(&key);
    if let Some(r) = tx.qopt("select id from replica where version_id = $1 and resource_id = $2 and url = $3", &[&vid, &res.id, &url]).await? {
        return Ok(Some(r.get(0)));
    }
    let state = spec.state.clone().unwrap_or_else(|| "registered".into());
    Ok(Some(add_replica(tx, vid, &res, &url, &state, spec.representation, Some(coll.acl_root)).await?))
}

pub async fn add_replica(
    tx: &mut Tx<'_>,
    version: Uuid,
    res: &Resource,
    url: &str,
    state: &str,
    representation: Option<Uuid>,
    acl_root: Option<Uuid>,
) -> Result<Uuid> {
    let commit = tx.commit_id().await?;
    let id = new_id();
    tx.exec(
        "insert into replica (id, version_id, resource_id, representation_id, url, state, created_commit) values ($1,$2,$3,$4,$5,$6,$7)",
        &[&id, &version, &res.id, &representation, &url, &state, &commit],
    )
    .await?;
    tx.exec("insert into replica_history (replica_id, commit_id, state) values ($1,$2,$3)", &[&id, &commit, &state]).await?;
    tx.emit("replica.registered", id, "replica", acl_root, json!({"version": version, "resource": res.name, "url": url, "state": state}))
        .await?;
    Ok(id)
}

/// Change the state of a replica, with history and an event.
pub async fn set_replica_state(tx: &mut Tx<'_>, replica: Uuid, state: &str, extra: Value) -> Result<()> {
    let commit = tx.commit_id().await?;
    tx.exec("update replica set state = $2 where id = $1", &[&replica, &state]).await?;
    tx.exec(
        "insert into replica_history (replica_id, commit_id, state) values ($1,$2,$3)
         on conflict (tenant_id, replica_id, commit_id) do update set state = excluded.state",
        &[&replica, &commit, &state],
    )
    .await?;
    tx.emit(&format!("replica.{state}"), replica, "replica", None, extra).await?;
    Ok(())
}

/// Write a delete marker: a new version with no content becomes the latest (model.md, removal table).
pub async fn delete_artifact(tx: &mut Tx<'_>, path: &str) -> Result<Registered> {
    let parent = parent_path(path).ok_or(Error::NotFound)?;
    let coll = collection(tx, parent).await?;
    authz::require(tx, "registration", "write", "collection", coll.id).await?;
    register_in(tx, &coll, &Register { path: path.into(), delete_marker: true, ..Default::default() }).await
}

/// Give an artifact a new path. Its ID does not change (ID-2).
pub async fn rename(tx: &mut Tx<'_>, old: &str, new: &str) -> Result<Uuid> {
    let id = resolve_identifier(tx, "path", old, None).await?.ok_or(Error::NotFound)?;
    let parent = parent_path(new).ok_or_else(|| Error::invalid("no collection"))?;
    let coll = ensure_collection(tx, parent).await?;
    tx.exec("update artifact set collection_id = $2, name = $3 where id = $1", &[&id, &coll.id, &base_name(new)]).await?;
    remove_identifier(tx, "path", old).await?;
    set_identifier(tx, "path", new, id).await?;
    tx.emit("identifier.changed", id, "artifact", Some(coll.acl_root), json!({"scheme": "path", "old": old, "new": new})).await?;
    Ok(id)
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct VersionInfo {
    pub id: Uuid,
    pub artifact_id: Uuid,
    pub number: i32,
    pub content_hash: Option<String>,
    pub size: i64,
    pub element_count: i64,
    pub structure: Value,
    pub source_id: Option<Uuid>,
    pub is_delete_marker: bool,
    pub element_manifest: Option<Uuid>,
    pub stats: Value,
    pub collection_id: Uuid,
    pub acl_root: Uuid,
    pub path: String,
}

const VERSION_SELECT: &str = "select v.id, v.artifact_id, v.number, v.content_hash, v.size, v.element_count, v.structure, v.source_id,
       v.is_delete_marker, v.element_manifest, v.stats, a.collection_id, c.acl_root_id, c.path || '/' || a.name as path
  from version v join artifact a on a.id = v.artifact_id and a.tenant_id = v.tenant_id
  join collection c on c.id = a.collection_id and c.tenant_id = a.tenant_id";

fn version_from_row(r: &tokio_postgres::Row) -> VersionInfo {
    let mut path: String = r.get(13);
    if path.starts_with("//") {
        path.remove(0);
    }
    VersionInfo {
        id: r.get(0),
        artifact_id: r.get(1),
        number: r.get(2),
        content_hash: r.get(3),
        size: r.get(4),
        element_count: r.get(5),
        structure: r.get(6),
        source_id: r.get(7),
        is_delete_marker: r.get(8),
        element_manifest: r.get(9),
        stats: r.get(10),
        collection_id: r.get(11),
        acl_root: r.get(12),
        path,
    }
}

pub async fn version(tx: &Tx<'_>, id: Uuid) -> Result<VersionInfo> {
    let r = tx.qopt(&format!("{VERSION_SELECT} where v.id = $1"), &[&id]).await?.ok_or(Error::NotFound)?;
    Ok(version_from_row(&r))
}

pub async fn versions(tx: &Tx<'_>, ids: &[Uuid]) -> Result<Vec<VersionInfo>> {
    let rows = tx.q(&format!("{VERSION_SELECT} where v.id = any($1)"), &[&ids]).await?;
    Ok(rows.iter().map(version_from_row).collect())
}

/// The versions of an artifact, newest first.
pub async fn artifact_versions(tx: &Tx<'_>, artifact: Uuid) -> Result<Vec<VersionInfo>> {
    let rows = tx.q(&format!("{VERSION_SELECT} where v.artifact_id = $1 order by v.number desc"), &[&artifact]).await?;
    Ok(rows.iter().map(version_from_row).collect())
}

/// The latest version of the artifact at a path, now or as of a commit.
pub async fn latest_version(tx: &Tx<'_>, path: &str, as_of: Option<i64>) -> Result<VersionInfo> {
    let aid = resolve_identifier(tx, "path", path, as_of).await?.ok_or(Error::NotFound)?;
    let row = match as_of {
        None => tx.qopt(&format!("{VERSION_SELECT} where v.artifact_id = $1 order by v.number desc limit 1"), &[&aid]).await?,
        Some(c) => {
            tx.qopt(&format!("{VERSION_SELECT} where v.artifact_id = $1 and v.created_commit <= $2 order by v.number desc limit 1"), &[&aid, &c])
                .await?
        }
    };
    Ok(version_from_row(&row.ok_or(Error::NotFound)?))
}

/// The versions that a collection subtree held as of a commit (the latest of each artifact, no
/// delete markers).
pub async fn subtree_versions(tx: &Tx<'_>, path: &str, as_of: Option<i64>) -> Result<Vec<VersionInfo>> {
    let like = format!("{}/%", path.trim_end_matches('/'));
    let c = as_of.unwrap_or(i64::MAX);
    let rows = tx
        .q(
            &format!(
                "select * from ({VERSION_SELECT} where (c.path = $1 or c.path like $2) and v.created_commit <= $3) x
                  where x.number = (select max(number) from version v2 where v2.artifact_id = x.artifact_id and v2.created_commit <= $3)
                    and not x.is_delete_marker order by x.path"
            ),
            &[&path, &like, &c],
        )
        .await?;
    Ok(rows.iter().map(version_from_row).collect())
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ReplicaInfo {
    pub id: Uuid,
    pub version_id: Uuid,
    pub resource_id: Uuid,
    pub resource: String,
    pub location: String,
    pub jurisdiction: String,
    pub egress_allowed: bool,
    pub protocol: String,
    pub mode: String,
    pub representation_id: Option<Uuid>,
    pub url: String,
    pub state: String,
}

pub async fn replicas_of(tx: &Tx<'_>, versions: &[Uuid]) -> Result<Vec<ReplicaInfo>> {
    let rows = tx
        .q(
            "select r.id, r.version_id, r.resource_id, s.name, s.location, s.jurisdiction, s.egress_allowed, s.protocol, s.mode,
                    r.representation_id, r.url, r.state
               from replica r join storage_resource s on s.id = r.resource_id and s.tenant_id = r.tenant_id
              where r.version_id = any($1) order by r.version_id, s.name",
            &[&versions],
        )
        .await?;
    Ok(rows
        .iter()
        .map(|r| ReplicaInfo {
            id: r.get(0),
            version_id: r.get(1),
            resource_id: r.get(2),
            resource: r.get(3),
            location: r.get(4),
            jurisdiction: r.get(5),
            egress_allowed: r.get(6),
            protocol: r.get(7),
            mode: r.get(8),
            representation_id: r.get(9),
            url: r.get(10),
            state: r.get(11),
        })
        .collect())
}

/// The kind of an entity, or not found. An ID of another tenant is not found (INV-7).
pub async fn entity_kind(tx: &Tx<'_>, id: Uuid) -> Result<String> {
    let r = tx.qopt("select kind, erased from entity where id = $1", &[&id]).await?.ok_or(Error::NotFound)?;
    if r.get::<_, bool>(1) {
        return Err(Error::NotFound);
    }
    Ok(r.get(0))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parent_path_and_base_name() {
        assert_eq!(parent_path("/"), None);
        assert_eq!(parent_path("/a"), Some("/"));
        assert_eq!(parent_path("/a/b"), Some("/a"));
        assert_eq!(parent_path("/a/b/c.csv"), Some("/a/b"));
        assert_eq!(parent_path("a"), None);
        assert_eq!(base_name("/a/b/c.csv"), "c.csv");
        assert_eq!(base_name("/a"), "a");
        assert_eq!(base_name("name"), "name");
        assert_eq!(base_name("/"), "");
    }
}
