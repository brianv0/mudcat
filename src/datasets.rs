//! Datasets, snapshots, refs, manifests and annotation sets (model.md, section 4).
//!
//! A commit is a database transaction. It writes no files (architecture.md, "Commit cost"). A
//! snapshot holds manifest references; a new snapshot refers again to each manifest that did not
//! change, and restricts a reference to leave out removed members.

use crate::error::{Error, Result};
use crate::instance::{new_id, sha256_hex, Session, Tx};
use crate::namespace;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use uuid::Uuid;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Member {
    pub id: Uuid,
    /// version | annotation-set | snapshot
    #[serde(default = "version_kind")]
    pub kind: String,
}
fn version_kind() -> String {
    "version".into()
}

impl Member {
    pub fn version(id: Uuid) -> Self {
        Member { id, kind: "version".into() }
    }
    pub fn annotation_set(id: Uuid) -> Self {
        Member { id, kind: "annotation-set".into() }
    }
    pub fn snapshot(id: Uuid) -> Self {
        Member { id, kind: "snapshot".into() }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DatasetChange {
    pub dataset: String,
    #[serde(default = "main")]
    pub branch: String,
    /// A requirement on the snapshot that the branch points to now (assert-ref-snapshot-id). With
    /// none, the requirement names only the branch, which is enough for updates that only add.
    #[serde(default)]
    pub requirement: Option<Uuid>,
    #[serde(default)]
    pub add: Vec<Member>,
    #[serde(default)]
    pub remove: Vec<Uuid>,
    #[serde(default)]
    pub replace: Vec<(Uuid, Member)>,
}
fn main() -> String {
    "main".into()
}

impl Default for DatasetChange {
    fn default() -> Self {
        DatasetChange { dataset: String::new(), branch: main(), requirement: None, add: vec![], remove: vec![], replace: vec![] }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Committed {
    pub dataset: Uuid,
    pub snapshot: Uuid,
    pub parent: Option<Uuid>,
    pub summary: Value,
    pub warnings: Value,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Entry {
    pub member_id: Uuid,
    pub member_kind: String,
    pub member_hash: String,
    pub source_id: Option<Uuid>,
    pub size: i64,
    pub element_count: i64,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct DatasetOptions {
    #[serde(default)]
    pub retention_days: Option<i32>,
    #[serde(default)]
    pub is_series: bool,
    #[serde(default)]
    pub follow: Option<String>,
}

pub async fn create_dataset(tx: &mut Tx<'_>, path: &str, opts: &DatasetOptions) -> Result<Uuid> {
    if let Some(id) = namespace::resolve_identifier(tx, "path", path, None).await? {
        if tx.qopt("select 1 from dataset where id = $1", &[&id]).await?.is_some() {
            return Ok(id);
        }
        return Err(Error::invalid(format!("{path} names an entity that is not a dataset")));
    }
    let parent = namespace::parent_path(path).ok_or_else(|| Error::invalid("a dataset needs a collection"))?;
    let coll = namespace::ensure_collection(tx, parent).await?;
    let commit = tx.commit_id().await?;
    let id = new_id();
    let retention = opts.retention_days.unwrap_or(30);
    let follow = opts.follow.as_ref().map(|f| json!({"collection": f}));
    tx.exec(
        "insert into dataset (id, path, collection_id, retention_days, is_series, follow, created_commit) values ($1,$2,$3,$4,$5,$6,$7)",
        &[&id, &path, &coll.id, &retention, &opts.is_series, &follow, &commit],
    )
    .await?;
    namespace::set_identifier(tx, "path", path, id).await?;
    crate::authz::write_structural(tx, "dataset", &id.to_string(), "collection", "collection", &coll.id.to_string()).await?;
    tx.emit("dataset.created", id, "dataset", Some(coll.acl_root), json!({"path": path})).await?;
    // the default branch, at an empty snapshot (explicit defaults)
    let snap = new_id();
    let summary = json!({"operation": "create", "added": 0, "added_bytes": 0, "removed": 0, "removed_bytes": 0, "members": 0});
    tx.exec(
        "insert into snapshot (id, dataset_id, parent_id, created_commit, created_at, summary) values ($1,$2,null,$3,$4,$5)",
        &[&snap, &id, &commit, &tx.now, &summary],
    )
    .await?;
    tx.register_entity(snap, "snapshot").await?;
    set_ref(tx, id, "main", "branch", snap).await?;
    Ok(id)
}

pub async fn dataset_id(tx: &Tx<'_>, path_or_id: &str) -> Result<Uuid> {
    if let Ok(id) = path_or_id.parse::<Uuid>() {
        if tx.qopt("select 1 from dataset where id = $1", &[&id]).await?.is_some() {
            return Ok(id);
        }
        return Err(Error::NotFound);
    }
    let id = namespace::resolve_identifier(tx, "path", path_or_id, None).await?.ok_or(Error::NotFound)?;
    tx.qopt("select 1 from dataset where id = $1", &[&id]).await?.ok_or(Error::NotFound)?;
    Ok(id)
}

pub async fn dataset_path(tx: &Tx<'_>, id: Uuid) -> Result<String> {
    Ok(tx.qopt("select path from dataset where id = $1", &[&id]).await?.ok_or(Error::NotFound)?.get(0))
}

async fn set_ref(tx: &mut Tx<'_>, dataset: Uuid, name: &str, kind: &str, snapshot: Uuid) -> Result<()> {
    let commit = tx.commit_id().await?;
    tx.exec(
        "insert into ref (dataset_id, name, kind, snapshot_id) values ($1,$2,$3,$4)
         on conflict (tenant_id, dataset_id, name) do update set snapshot_id = excluded.snapshot_id",
        &[&dataset, &name, &kind, &snapshot],
    )
    .await?;
    tx.exec(
        "insert into ref_history (dataset_id, name, commit_id, snapshot_id) values ($1,$2,$3,$4)
         on conflict (tenant_id, dataset_id, name, commit_id) do update set snapshot_id = excluded.snapshot_id",
        &[&dataset, &name, &commit, &snapshot],
    )
    .await?;
    Ok(())
}

/// Create a ref. A tag ref does not move after it is created.
pub async fn create_ref(tx: &mut Tx<'_>, dataset: &str, name: &str, kind: &str, snapshot: Uuid) -> Result<()> {
    let did = dataset_id(tx, dataset).await?;
    if let Some(r) = tx.qopt("select kind, snapshot_id from ref where dataset_id = $1 and name = $2", &[&did, &name]).await? {
        let k: String = r.get(0);
        let s: Uuid = r.get(1);
        if k == "tag" && s != snapshot {
            return Err(Error::refused("tag-ref-immutable", format!("the tag ref {name} exists and does not move"), json!({"ref": name})));
        }
    }
    set_ref(tx, did, name, kind, snapshot).await?;
    let root = crate::metadata::acl_root_of(tx, did).await?;
    tx.emit("ref.moved", did, "dataset", root, json!({"ref": name, "kind": kind, "snapshot": snapshot})).await?;
    Ok(())
}

/// The snapshot that a ref points to, now or as of a time (time travel, model.md section 5).
pub async fn resolve_ref(tx: &Tx<'_>, dataset: Uuid, name: &str, as_of: Option<chrono::DateTime<chrono::Utc>>) -> Result<Uuid> {
    let snap: Uuid = match as_of {
        None => tx
            .qopt("select snapshot_id from ref where dataset_id = $1 and name = $2", &[&dataset, &name])
            .await?
            .ok_or_else(|| Error::refused("no-ref", format!("the dataset has no ref {name}"), Value::Null))?
            .get(0),
        Some(t) => {
            let c = namespace::commit_at(tx, t).await?;
            tx.qopt(
                "select snapshot_id from ref_history where dataset_id = $1 and name = $2 and commit_id <= $3 order by commit_id desc limit 1",
                &[&dataset, &name, &c],
            )
            .await?
            .ok_or_else(|| Error::refused("no-ref", format!("the ref {name} did not exist at {t}"), Value::Null))?
            .get(0)
        }
    };
    check_not_expired(tx, snap).await?;
    Ok(snap)
}

/// An expired snapshot fails with an error that states the expiry (UP-14).
pub async fn check_not_expired(tx: &Tx<'_>, snap: Uuid) -> Result<()> {
    let r = tx.qopt("select expired_at from snapshot where id = $1", &[&snap]).await?.ok_or(Error::NotFound)?;
    if let Some(e) = r.get::<_, Option<chrono::DateTime<chrono::Utc>>>(0) {
        return Err(Error::refused(
            "snapshot-expired",
            format!("the snapshot {snap} expired at {e}, because it was not published and nothing referred to it; mudcat does not resolve to another snapshot"),
            json!({"snapshot": snap, "expired_at": e}),
        ));
    }
    Ok(())
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SnapshotInfo {
    pub id: Uuid,
    pub dataset_id: Uuid,
    pub parent_id: Option<Uuid>,
    pub created_commit: i64,
    pub summary: Value,
    pub status: Option<String>,
    pub merkle_hash: Option<String>,
    pub member_count: i64,
    pub warnings: Value,
    pub equivalences: Vec<Uuid>,
    pub expired: bool,
}

pub async fn snapshot(tx: &Tx<'_>, id: Uuid) -> Result<SnapshotInfo> {
    let r = tx
        .qopt(
            "select id, dataset_id, parent_id, created_commit, summary, status, merkle_hash, member_count, warnings, equivalences, expired_at is not null
               from snapshot where id = $1",
            &[&id],
        )
        .await?
        .ok_or(Error::NotFound)?;
    Ok(SnapshotInfo {
        id: r.get(0),
        dataset_id: r.get(1),
        parent_id: r.get(2),
        created_commit: r.get(3),
        summary: r.get(4),
        status: r.get(5),
        merkle_hash: r.get(6),
        member_count: r.get(7),
        warnings: r.get(8),
        equivalences: r.get(9),
        expired: r.get(10),
    })
}

/// The members of a snapshot, through its manifest references. A reference to a manifest of
/// another tenant (a tenant mapping) is read through the explicit path between tenants.
pub async fn members(tx: &Tx<'_>, snap: Uuid) -> Result<Vec<Entry>> {
    let refs = tx.q("select manifest_tenant, manifest_id, excluded from manifest_ref where snapshot_id = $1 order by ordinal", &[&snap]).await?;
    let me = tx.sess.tenant_id()?;
    let mut out = Vec::new();
    let mut local = Vec::new();
    for r in &refs {
        let mt: Uuid = r.get(0);
        let mid: Uuid = r.get(1);
        let ex: Vec<Uuid> = r.get(2);
        if mt == me {
            local.push((mid, ex));
        } else {
            out.extend(foreign_entries(tx, mt, mid, &ex).await?);
        }
    }
    if !local.is_empty() {
        let mids: Vec<Uuid> = local.iter().map(|l| l.0).collect();
        let rows = tx
            .q(
                "select manifest_id, member_id, member_kind, member_hash, source_id, size, element_count from manifest_entry where manifest_id = any($1)",
                &[&mids],
            )
            .await?;
        let excl: HashMap<Uuid, HashSet<Uuid>> = local.into_iter().map(|(m, e)| (m, e.into_iter().collect())).collect();
        for r in rows {
            let mid: Uuid = r.get(0);
            let member: Uuid = r.get(1);
            if excl.get(&mid).map(|e| e.contains(&member)).unwrap_or(false) {
                continue;
            }
            out.push(Entry { member_id: member, member_kind: r.get(2), member_hash: r.get(3), source_id: r.get(4), size: r.get(5), element_count: r.get(6) });
        }
    }
    Ok(out)
}

async fn foreign_entries(tx: &Tx<'_>, owner: Uuid, manifest: Uuid, excluded: &[Uuid]) -> Result<Vec<Entry>> {
    let me = tx.sess.tenant_id()?;
    let ok = tx
        .qopt("select 1 from cross_tenant_ref where owner_tenant = $1 and target_id = $2 and holder_tenant = $3", &[&owner, &manifest, &me])
        .await?;
    if ok.is_none() {
        return Err(Error::NotFound);
    }
    tx.exec("select set_config('mudcat.tenant', $1, true)", &[&owner.to_string()]).await?;
    let rows = tx
        .q("select member_id, member_kind, member_hash, source_id, size, element_count from manifest_entry where manifest_id = $1", &[&manifest])
        .await;
    tx.exec("select set_config('mudcat.tenant', $1, true)", &[&me.to_string()]).await?;
    Ok(rows?
        .iter()
        .filter(|r| !excluded.contains(&r.get::<_, Uuid>(0)))
        .map(|r| Entry { member_id: r.get(0), member_kind: r.get(1), member_hash: r.get(2), source_id: r.get(3), size: r.get(4), element_count: r.get(5) })
        .collect())
}

/// The member entry of a version, annotation set or snapshot.
async fn entry_of(tx: &Tx<'_>, m: &Member) -> Result<Entry> {
    match m.kind.as_str() {
        "version" => {
            let r = tx
                .qopt("select content_hash, source_id, size, element_count, is_delete_marker from version where id = $1", &[&m.id])
                .await?
                .ok_or_else(|| Error::refused("member-not-found", format!("the version {} does not exist", m.id), json!({"member": m.id})))?;
            if r.get::<_, bool>(4) {
                return Err(Error::invalid("a delete marker cannot be a member"));
            }
            Ok(Entry { member_id: m.id, member_kind: m.kind.clone(), member_hash: r.get::<_, Option<String>>(0).unwrap_or_default(), source_id: r.get(1), size: r.get(2), element_count: r.get(3) })
        }
        "annotation-set" => {
            let r = tx
                .qopt("select content_hash, entry_count from annotation_set where id = $1", &[&m.id])
                .await?
                .ok_or_else(|| Error::refused("member-not-found", format!("the annotation set {} does not exist", m.id), json!({"member": m.id})))?;
            Ok(Entry { member_id: m.id, member_kind: m.kind.clone(), member_hash: r.get(0), source_id: None, size: 0, element_count: r.get(1) })
        }
        "snapshot" => {
            let r = tx
                .qopt("select merkle_hash, member_count, status from snapshot where id = $1", &[&m.id])
                .await?
                .ok_or_else(|| Error::refused("member-not-found", format!("the snapshot {} does not exist", m.id), json!({"member": m.id})))?;
            let hash: Option<String> = r.get(0);
            let hash = hash.ok_or_else(|| Error::refused("not-published", "a snapshot that is a member of a dataset must be published, so that it has a Merkle hash", json!({"member": m.id})))?;
            let size: i64 = tx
                .q1("select coalesce(sum((summary->>'total_bytes')::bigint), 0)::bigint from snapshot where id = $1", &[&m.id])
                .await?
                .get(0);
            Ok(Entry { member_id: m.id, member_kind: m.kind.clone(), member_hash: hash, source_id: None, size, element_count: r.get(1) })
        }
        k => Err(Error::invalid(format!("unknown member kind {k}"))),
    }
}

/// Apply commits to one or more datasets, in one transaction (architecture.md, "Commits with requirements").
pub async fn commit(tx: &mut Tx<'_>, changes: &[DatasetChange]) -> Result<Vec<Committed>> {
    let mut out = Vec::new();
    for ch in changes {
        out.push(commit_one(tx, ch).await?);
    }
    Ok(out)
}

async fn commit_one(tx: &mut Tx<'_>, ch: &DatasetChange) -> Result<Committed> {
    let did = dataset_id(tx, &ch.dataset).await?;
    crate::authz::require(tx, "registration", "write", "dataset", did).await?;
    // the row lock on the ref serialises the writers of one branch
    let r = tx
        .qopt(if crate::mutant("no-branch-lock") { "select snapshot_id, kind from ref where dataset_id = $1 and name = $2" } else { "select snapshot_id, kind from ref where dataset_id = $1 and name = $2 for update" }, &[&did, &ch.branch])
        .await?
        .ok_or_else(|| Error::refused("no-ref", format!("the dataset has no branch {}", ch.branch), Value::Null))?;
    let head: Uuid = r.get(0);
    let kind: String = r.get(1);
    if kind == "tag" && !crate::mutant("tag-ref-movable") {
        return Err(Error::refused(
            "tag-ref-immutable",
            format!("{} is a tag ref; its snapshot is fixed and a commit cannot move it", ch.branch),
            json!({"ref": ch.branch, "snapshot": head}),
        ));
    }
    let strict = !ch.remove.is_empty() || !ch.replace.is_empty();
    match ch.requirement {
        Some(req) if req != head => {
            return Err(Error::conflict(
                format!("the branch {} points to {head}, not to {req}", ch.branch),
                json!({"branch": ch.branch, "required": req, "current": head}),
            ))
        }
        None if strict => return Err(Error::invalid("a commit that removes or replaces members needs a requirement on the current snapshot")),
        _ => {}
    }
    // only the members that the commit names are read from the parent, not all of its members
    let mut named: Vec<Uuid> = ch.remove.clone();
    named.extend(ch.replace.iter().map(|(old, _)| *old));
    named.extend(ch.add.iter().map(|m| m.id));
    named.extend(ch.replace.iter().map(|(_, new)| new.id));
    let present_owned = lookup_members(tx, head, &named).await?;
    let present: HashMap<Uuid, &Entry> = present_owned.iter().map(|(k, v)| (*k, v)).collect();
    let parent = snapshot(tx, head).await?;

    let mut removed: Vec<Uuid> = ch.remove.clone();
    removed.extend(ch.replace.iter().map(|(old, _)| *old));
    for m in &removed {
        if !present.contains_key(m) {
            return Err(Error::refused(
                "member-not-found",
                format!("the member {m} is not in the snapshot {head}; another commit removed it, or it was never a member"),
                json!({"member": m, "snapshot": head}),
            ));
        }
    }
    let removed_set: HashSet<Uuid> = removed.iter().copied().collect();
    let mut adds: Vec<Member> = ch.add.clone();
    adds.extend(ch.replace.iter().map(|(_, new)| new.clone()));
    let mut seen = HashSet::new();
    let mut wanted = Vec::new();
    for m in adds {
        if (present.contains_key(&m.id) && !removed_set.contains(&m.id)) || !seen.insert(m.id) {
            continue; // a member that is already there is there one time
        }
        wanted.push(m);
    }
    // the entries of version members with one query; other kinds one by one
    let vids: Vec<Uuid> = wanted.iter().filter(|m| m.kind == "version").map(|m| m.id).collect();
    let mut ventries: HashMap<Uuid, Entry> = HashMap::new();
    if !vids.is_empty() {
        for r in tx.q("select id, content_hash, source_id, size, element_count, is_delete_marker from version where id = any($1)", &[&vids]).await? {
            if r.get::<_, bool>(5) {
                return Err(Error::invalid("a delete marker cannot be a member"));
            }
            let id: Uuid = r.get(0);
            ventries.insert(id, Entry { member_id: id, member_kind: "version".into(), member_hash: r.get::<_, Option<String>>(1).unwrap_or_default(), source_id: r.get(2), size: r.get(3), element_count: r.get(4) });
        }
    }
    let mut added = Vec::with_capacity(wanted.len());
    for m in &wanted {
        if m.kind == "version" {
            match ventries.remove(&m.id) {
                Some(e) => added.push(e),
                None => return Err(Error::refused("member-not-found", format!("the version {} does not exist", m.id), json!({"member": m.id}))),
            }
        } else {
            added.push(entry_of(tx, m).await?);
        }
    }

    let commit = tx.commit_id().await?;
    let snap = new_id();
    // manifest references: the parent's, restricted for removed members, plus one new manifest
    // the parent's manifest references, with one statement; a removed member goes to the
    // exclusions of the reference whose manifest holds it
    tx.exec(
        "insert into manifest_ref (snapshot_id, ordinal, manifest_tenant, manifest_id, excluded)
         select $2, r.ordinal, r.manifest_tenant, r.manifest_id,
                r.excluded || coalesce((select array_agg(e.member_id) from manifest_entry e
                                         where e.manifest_id = r.manifest_id and e.tenant_id = r.manifest_tenant and e.member_id = any($3)), '{}')
           from manifest_ref r where r.snapshot_id = $1",
        &[&head, &snap, &removed],
    )
    .await?;
    let ordinal: i32 = tx.q1("select coalesce(max(ordinal) + 1, 0) from manifest_ref where snapshot_id = $1", &[&snap]).await?.get(0);
    if !added.is_empty() {
        let mid = new_id();
        let sources: Vec<Uuid> = added.iter().filter_map(|e| e.source_id).collect::<BTreeSet<_>>().into_iter().collect();
        let total: i64 = added.iter().map(|e| e.size).sum();
        tx.exec(
            "insert into manifest (id, sealed, entry_count, total_size, sources, created_commit) values ($1,false,$2,$3,$4,$5)",
            &[&mid, &(added.len() as i64), &total, &sources, &commit],
        )
        .await?;
        insert_entries(tx, mid, &added).await?;
        let me = tx.sess.tenant_id()?;
        tx.exec(
            "insert into manifest_ref (snapshot_id, ordinal, manifest_tenant, manifest_id, excluded) values ($1,$2,$3,$4,'{}')",
            &[&snap, &ordinal, &me, &mid],
        )
        .await?;
    }
    let removed_bytes: i64 = removed.iter().map(|m| present[m].size).sum();
    let added_bytes: i64 = added.iter().map(|e| e.size).sum();
    let count = parent.member_count - removed.len() as i64 + added.len() as i64;
    let parent_bytes = match parent.summary["total_bytes"].as_i64() {
        Some(b) => b,
        None => members(tx, head).await?.iter().map(|e| e.size).sum(),
    };
    let total_bytes: i64 = parent_bytes - removed_bytes + added_bytes;
    let operation = match (added.is_empty(), removed.is_empty()) {
        (false, true) if added.iter().all(|e| e.member_kind == "annotation-set") => "annotate",
        (false, true) => "add",
        (true, false) => "remove",
        (false, false) => "replace",
        (true, true) => "none",
    };
    let summary = json!({"operation": operation, "added": added.len(), "added_bytes": added_bytes, "removed": removed.len(),
                         "removed_bytes": removed_bytes, "members": count, "total_bytes": total_bytes});
    tx.exec(
        "insert into snapshot (id, dataset_id, parent_id, created_commit, created_at, summary, member_count) values ($1,$2,$3,$4,$5,$6,$7)",
        &[&snap, &did, &head, &commit, &tx.now, &summary, &count],
    )
    .await?;
    // the method rule: a commit that only adds versions of sources that the parent already has
    // keeps the warnings and equivalences of the parent; other commits check all sources
    let parent_sources: BTreeSet<Uuid> = tx
        .q("select distinct unnest(m.sources) from manifest_ref r join manifest m on m.id = r.manifest_id and m.tenant_id = r.manifest_tenant where r.snapshot_id = $1", &[&head])
        .await?
        .iter()
        .map(|r| r.get(0))
        .collect();
    let same_sources = removed.is_empty() && added.iter().all(|e| e.member_kind != "snapshot" && e.source_id.map(|s| parent_sources.contains(&s)).unwrap_or(true));
    let (warnings_v, equivalences) = if same_sources {
        (parent.warnings.clone(), parent.equivalences.clone())
    } else {
        let (warnings, equivalences) = method_check(tx, snap).await?;
        (json!(warnings.iter().map(|w| w.to_json()).collect::<Vec<_>>()), equivalences)
    };
    tx.exec("update snapshot set warnings = $2, equivalences = $3 where id = $1", &[&snap, &warnings_v, &equivalences]).await?;
    set_ref(tx, did, &ch.branch, "branch", snap).await?;
    let root = crate::metadata::acl_root_of(tx, did).await?;
    let limit = tx.inst().config.event_member_limit;
    let mut data = json!({"dataset": did, "path": ch.dataset, "branch": ch.branch, "parent": head, "summary": summary});
    if added.len() + removed.len() <= limit {
        data["added_members"] = json!(added.iter().map(|e| e.member_id).collect::<Vec<_>>());
        data["removed_members"] = json!(removed);
    } else {
        data["changes"] = json!(format!("/datasets/{did}/changes?from={head}&to={snap}"));
    }
    tx.emit("snapshot.committed", snap, "snapshot", root, data).await?;
    tx.emit("ref.moved", did, "dataset", root, json!({"ref": ch.branch, "snapshot": snap, "previous": head})).await?;
    Ok(Committed { dataset: did, snapshot: snap, parent: Some(head), summary, warnings: warnings_v })
}

/// The entries of a snapshot among some member IDs, through its manifest references.
async fn lookup_members(tx: &Tx<'_>, snap: Uuid, ids: &[Uuid]) -> Result<HashMap<Uuid, Entry>> {
    let me = tx.sess.tenant_id()?;
    let foreign: i64 = tx.q1("select count(*) from manifest_ref where snapshot_id = $1 and manifest_tenant <> $2", &[&snap, &me]).await?.get(0);
    if foreign > 0 {
        let wanted: HashSet<Uuid> = ids.iter().copied().collect();
        return Ok(members(tx, snap).await?.into_iter().filter(|e| wanted.contains(&e.member_id)).map(|e| (e.member_id, e)).collect());
    }
    let rows = tx
        .q(
            "select e.member_id, e.member_kind, e.member_hash, e.source_id, e.size, e.element_count
               from manifest_ref r join manifest_entry e on e.manifest_id = r.manifest_id and e.tenant_id = r.manifest_tenant
              where r.snapshot_id = $1 and e.member_id = any($2) and not (e.member_id = any(r.excluded))",
            &[&snap, &ids],
        )
        .await?;
    Ok(rows
        .iter()
        .map(|r| (r.get(0), Entry { member_id: r.get(0), member_kind: r.get(1), member_hash: r.get(2), source_id: r.get(3), size: r.get(4), element_count: r.get(5) }))
        .collect())
}

async fn insert_entries(tx: &Tx<'_>, mid: Uuid, entries: &[Entry]) -> Result<()> {
    for chunk in entries.chunks(5000) {
        let ids: Vec<Uuid> = chunk.iter().map(|e| e.member_id).collect();
        let kinds: Vec<String> = chunk.iter().map(|e| e.member_kind.clone()).collect();
        let hashes: Vec<String> = chunk.iter().map(|e| e.member_hash.clone()).collect();
        let sources: Vec<Option<Uuid>> = chunk.iter().map(|e| e.source_id).collect();
        let sizes: Vec<i64> = chunk.iter().map(|e| e.size).collect();
        let counts: Vec<i64> = chunk.iter().map(|e| e.element_count).collect();
        tx.exec(
            "insert into manifest_entry (manifest_id, member_id, member_kind, member_hash, source_id, size, element_count)
             select $1, * from unnest($2::uuid[], $3::text[], $4::text[], $5::uuid[], $6::bigint[], $7::bigint[])",
            &[&mid, &ids, &kinds, &hashes, &sources, &sizes, &counts],
        )
        .await?;
    }
    Ok(())
}

// ---------------------------------------------------------------------------- methods

#[derive(Clone, Debug)]
pub struct MethodWarning {
    pub method_a: Value,
    pub method_b: Value,
    pub hash_a: String,
    pub hash_b: String,
    pub attributes: Vec<String>,
}

impl MethodWarning {
    fn to_json(&self) -> Value {
        json!({"kind": "methods-differ", "methods": [self.method_a, self.method_b], "method_hashes": [self.hash_a, self.hash_b],
               "attributes": self.attributes,
               "message": format!("the members have two methods with no declared equivalence; they differ in {}", self.attributes.join(", "))})
    }
}

pub fn method_hash(m: &Value) -> String {
    // canonical JSON: keys sorted
    let canon: BTreeMap<String, Value> = m.as_object().map(|o| o.iter().map(|(k, v)| (k.clone(), v.clone())).collect()).unwrap_or_default();
    sha256_hex(serde_json::to_string(&canon).unwrap().as_bytes())
}

/// The sources of every version in a snapshot, nested snapshots included.
pub async fn snapshot_sources(tx: &Tx<'_>, snap: Uuid) -> Result<BTreeSet<Uuid>> {
    let mut out = BTreeSet::new();
    let mut stack = vec![snap];
    let mut seen = HashSet::new();
    while let Some(s) = stack.pop() {
        if !seen.insert(s) {
            continue;
        }
        for e in members(tx, s).await? {
            if let Some(src) = e.source_id {
                out.insert(src);
            }
            if e.member_kind == "snapshot" {
                stack.push(e.member_id);
            }
        }
    }
    Ok(out)
}

pub async fn method_check(tx: &Tx<'_>, snap: Uuid) -> Result<(Vec<MethodWarning>, Vec<Uuid>)> {
    let sources: Vec<Uuid> = snapshot_sources(tx, snap).await?.into_iter().collect();
    let rows = tx.q("select distinct method_hash, method from source where id = any($1) and method is not null", &[&sources]).await?;
    let methods: Vec<(String, Value)> = rows.iter().map(|r| (r.get(0), r.get(1))).collect();
    let mut warnings = Vec::new();
    let mut equivalences = Vec::new();
    for i in 0..methods.len() {
        for j in i + 1..methods.len() {
            let (ha, ma) = &methods[i];
            let (hb, mb) = &methods[j];
            let eq = tx
                .qopt("select id from equivalence where (method_a = $1 and method_b = $2) or (method_a = $2 and method_b = $1)", &[ha, hb])
                .await?;
            match eq {
                Some(r) => equivalences.push(r.get(0)),
                None => {
                    let mut attrs = BTreeSet::new();
                    for (k, v) in ma.as_object().into_iter().flatten() {
                        if mb.get(k) != Some(v) {
                            attrs.insert(k.clone());
                        }
                    }
                    for (k, _) in mb.as_object().into_iter().flatten() {
                        if ma.get(k).is_none() {
                            attrs.insert(k.clone());
                        }
                    }
                    warnings.push(MethodWarning { method_a: ma.clone(), method_b: mb.clone(), hash_a: ha.clone(), hash_b: hb.clone(), attributes: attrs.into_iter().collect() });
                }
            }
        }
    }
    Ok((warnings, equivalences))
}

pub async fn declare_equivalence(tx: &mut Tx<'_>, a: &Value, b: &Value, after: Option<&str>, reason: &str) -> Result<Uuid> {
    let id = new_id();
    let (ha, hb) = (method_hash(a), method_hash(b));
    tx.exec(
        "insert into equivalence (id, method_a, method_b, declared_by, at, reason, after) values ($1,$2,$3,$4,$5,$6,$7)",
        &[&id, &ha, &hb, &tx.principal().0, &tx.now, &reason, &after],
    )
    .await?;
    tx.emit("method.equivalence-declared", id, "equivalence", None, json!({"methods": [a, b], "reason": reason, "after": after})).await?;
    Ok(id)
}

// ---------------------------------------------------------------------------- publication

/// The Merkle hash of a set of member entries (ID-5). Leaves are sorted by member ID.
pub fn merkle(entries: &[Entry]) -> String {
    if crate::mutant("merkle-constant") {
        return sha256_hex(b"constant");
    }
    let mut leaves: Vec<(Uuid, String)> = entries.iter().map(|e| (e.member_id, sha256_hex(format!("{}:{}:{}", e.member_kind, e.member_id, e.member_hash).as_bytes()))).collect();
    leaves.sort();
    let mut level: Vec<String> = leaves.into_iter().map(|l| l.1).collect();
    if level.is_empty() {
        return sha256_hex(b"");
    }
    while level.len() > 1 {
        level = level
            .chunks(2)
            .map(|c| if c.len() == 2 { sha256_hex(format!("{}{}", c[0], c[1]).as_bytes()) } else { c[0].clone() })
            .collect();
    }
    level.pop().unwrap()
}

/// Publish the snapshot that a branch points to, with a tag ref (write-audit-publish).
pub async fn publish(tx: &mut Tx<'_>, dataset: &str, branch: &str, tag_ref: &str) -> Result<SnapshotInfo> {
    let did = dataset_id(tx, dataset).await?;
    crate::authz::require(tx, "export", "write", "dataset", did).await?;
    let snap = resolve_ref(tx, did, branch, None).await?;
    publish_snapshot(tx, did, snap, tag_ref).await
}

pub async fn publish_snapshot(tx: &mut Tx<'_>, did: Uuid, snap: Uuid, tag_ref: &str) -> Result<SnapshotInfo> {
    let (warnings, equivalences) = method_check(tx, snap).await?;
    if let Some(w) = warnings.first() {
        return Err(Error::refused(
            "methods-differ",
            format!(
                "mudcat refuses the publication: the members have the methods {} and {}, which differ in {}, and no equivalence is declared",
                w.method_a, w.method_b, w.attributes.join(", ")
            ),
            json!({"methods": [w.method_a, w.method_b], "attributes": w.attributes}),
        ));
    }
    let entries = members(tx, snap).await?;
    let hash = merkle(&entries);
    let commit = tx.commit_id().await?;
    tx.exec(
        "update snapshot set status = 'published', merkle_hash = coalesce(merkle_hash, $2), member_count = $3, equivalences = $4,
                published_at = coalesce(published_at, $5), published_by = coalesce(published_by, $6) where id = $1",
        &[&snap, &hash, &(entries.len() as i64), &equivalences, &tx.now, &tx.principal().0],
    )
    .await?;
    let _ = commit;
    namespace::set_identifier(tx, "content-hash", &hash, snap).await?;
    create_ref(tx, &did.to_string(), tag_ref, "tag", snap).await?;
    let root = crate::metadata::acl_root_of(tx, did).await?;
    tx.emit("snapshot.published", snap, "snapshot", root, json!({"dataset": did, "tag_ref": tag_ref, "merkle_hash": hash, "equivalences": equivalences}))
        .await?;
    snapshot(tx, snap).await
}

/// Mint a DOI for a published snapshot. The DOI makes the snapshot public at once (use case 5).
pub async fn mint_doi(tx: &mut Tx<'_>, snap: Uuid) -> Result<String> {
    let s = snapshot(tx, snap).await?;
    if s.status.as_deref() != Some("published") {
        return Err(Error::refused("not-published", "mudcat mints a DOI only for a published snapshot", Value::Null));
    }
    crate::authz::require(tx, "export", "write", "dataset", s.dataset_id).await?;
    if let Some(r) = tx.qopt("select value from identifier where entity_id = $1 and scheme = 'doi' and valid_to is null", &[&snap]).await? {
        return Ok(r.get(0));
    }
    let doi = format!("{}/mudcat.{}", tx.inst().config.doi_prefix, &snap.simple().to_string()[..16]);
    namespace::set_identifier(tx, "doi", &doi, snap).await?;
    crate::authz::grant(tx, "snapshot", &snap.to_string(), "reader", "user:*", None).await?;
    let root = crate::metadata::acl_root_of(tx, s.dataset_id).await?;
    tx.emit("snapshot.doi-minted", snap, "snapshot", root, json!({"doi": doi})).await?;
    Ok(doi)
}

/// Change the status of a published snapshot (deprecated or withdrawn). Only a person does this (ID-8).
pub async fn set_status(tx: &mut Tx<'_>, snap: Uuid, status: &str) -> Result<()> {
    if !["deprecated", "withdrawn"].contains(&status) {
        return Err(Error::invalid("status is deprecated or withdrawn"));
    }
    tx.exec("update snapshot set status = $2 where id = $1 and status is not null", &[&snap, &status]).await?;
    tx.emit(&format!("snapshot.{status}"), snap, "snapshot", None, json!({})).await?;
    Ok(())
}

// ---------------------------------------------------------------------------- changes between snapshots

/// The members that `to` added and removed in comparison with `from`, in pages (EV-12).
pub async fn changes(tx: &Tx<'_>, from: Uuid, to: Uuid, after: Option<Uuid>, page: usize) -> Result<Value> {
    let a: BTreeMap<Uuid, Entry> = members(tx, from).await?.into_iter().map(|e| (e.member_id, e)).collect();
    let b: BTreeMap<Uuid, Entry> = members(tx, to).await?.into_iter().map(|e| (e.member_id, e)).collect();
    let mut items: Vec<(Uuid, &str)> = Vec::new();
    for k in b.keys() {
        if !a.contains_key(k) {
            items.push((*k, "added"));
        }
    }
    for k in a.keys() {
        if !b.contains_key(k) {
            items.push((*k, "removed"));
        }
    }
    items.sort();
    let start = after.map(|c| items.iter().position(|i| i.0 > c).unwrap_or(items.len())).unwrap_or(0);
    let pg: Vec<_> = items.iter().skip(start).take(page).collect();
    let next = if start + pg.len() < items.len() { pg.last().map(|l| l.0) } else { None };
    Ok(json!({
        "from": from, "to": to,
        "added": pg.iter().filter(|i| i.1 == "added").map(|i| json!({"member": i.0, "kind": b[&i.0].member_kind})).collect::<Vec<_>>(),
        "removed": pg.iter().filter(|i| i.1 == "removed").map(|i| json!({"member": i.0, "kind": a[&i.0].member_kind})).collect::<Vec<_>>(),
        "next": next,
    }))
}

// ---------------------------------------------------------------------------- cheap copies

/// Clone a snapshot into a new dataset of the same tenant. The clone refers to the same
/// manifests; nothing is copied (ID-13).
pub async fn clone_snapshot(tx: &mut Tx<'_>, snap: Uuid, to_path: &str) -> Result<Uuid> {
    let src = snapshot(tx, snap).await?;
    crate::authz::require(tx, "export", "read", "snapshot", snap).await?;
    let did = create_dataset(tx, to_path, &DatasetOptions::default()).await?;
    let head = resolve_ref(tx, did, "main", None).await?;
    let commit = tx.commit_id().await?;
    let new_snap = new_id();
    tx.exec(
        "insert into snapshot (id, dataset_id, parent_id, created_commit, created_at, summary, member_count) values ($1,$2,$3,$4,$5,$6,$7)",
        &[&new_snap, &did, &head, &commit, &tx.now, &json!({"operation": "clone", "from": snap, "members": src.member_count}), &src.member_count],
    )
    .await?;
    tx.exec(
        "insert into manifest_ref (snapshot_id, ordinal, manifest_tenant, manifest_id, excluded)
         select $2, ordinal, manifest_tenant, manifest_id, excluded from manifest_ref where snapshot_id = $1",
        &[&snap, &new_snap],
    )
    .await?;
    set_ref(tx, did, "main", "branch", new_snap).await?;
    let root = crate::metadata::acl_root_of(tx, did).await?;
    tx.emit("snapshot.committed", new_snap, "snapshot", root, json!({"dataset": did, "summary": {"operation": "clone", "from": snap}})).await?;
    Ok(did)
}

/// Give a copy of a published snapshot to another tenant of the same instance. The new dataset
/// refers to the manifests of the source tenant through a tenant mapping; mudcat copies no
/// manifest and no data. The copy is an explicit path between tenants, with an audit record.
pub async fn clone_to_tenant(src: &Session, snap: Uuid, dst: &Session, to_path: &str, agreement: &str) -> Result<Uuid> {
    let src_tenant = src.tenant_id()?;
    let dst_tenant = dst.tenant_id()?;
    let (refs, versions, member_count, hash) = {
        let mut c = src.client().await?;
        let tx = src.begin(&mut c).await?;
        let s = snapshot(&tx, snap).await?;
        if s.status.as_deref() != Some("published") {
            return Err(Error::refused("not-published", "only a published snapshot can be given to another tenant", Value::Null));
        }
        crate::authz::require(&tx, "export", "read", "snapshot", snap).await?;
        let refs: Vec<(i32, Uuid, Uuid, Vec<Uuid>)> = tx
            .q("select ordinal, manifest_tenant, manifest_id, excluded from manifest_ref where snapshot_id = $1 order by ordinal", &[&snap])
            .await?
            .iter()
            .map(|r| (r.get(0), r.get(1), r.get(2), r.get(3)))
            .collect();
        let versions: Vec<Uuid> = members(&tx, snap).await?.iter().map(|e| e.member_id).collect();
        tx.audit("export", "share-with-tenant", &format!("snapshot:{snap}"), true, Some(agreement), json!({"to_tenant": dst_tenant})).await?;
        tx.commit().await?;
        (refs, versions, s.member_count, s.merkle_hash)
    };
    let mut c = dst.client().await?;
    let mut tx = dst.begin(&mut c).await?;
    let did = create_dataset(&mut tx, to_path, &DatasetOptions::default()).await?;
    let head = resolve_ref(&tx, did, "main", None).await?;
    let commit = tx.commit_id().await?;
    let new_snap = new_id();
    tx.exec(
        "insert into snapshot (id, dataset_id, parent_id, created_commit, created_at, summary, member_count, merkle_hash, status, published_at)
         values ($1,$2,$3,$4,$5,$6,$7,$8,'published',$5)",
        &[&new_snap, &did, &head, &commit, &tx.now, &json!({"operation": "clone", "from": snap, "from_tenant": src_tenant, "agreement": agreement, "members": member_count}), &member_count, &hash],
    )
    .await?;
    for (ord, mt, mid, ex) in &refs {
        tx.exec(
            "insert into manifest_ref (snapshot_id, ordinal, manifest_tenant, manifest_id, excluded) values ($1,$2,$3,$4,$5)",
            &[&new_snap, ord, mt, mid, ex],
        )
        .await?;
        tx.exec(
            "insert into cross_tenant_ref (owner_tenant, target_id, holder_tenant, holder_entity) values ($1,$2,$3,$4) on conflict do nothing",
            &[mt, mid, &dst_tenant, &new_snap],
        )
        .await?;
    }
    for v in &versions {
        tx.exec(
            "insert into cross_tenant_ref (owner_tenant, target_id, holder_tenant, holder_entity) values ($1,$2,$3,$4) on conflict do nothing",
            &[&src_tenant, v, &dst_tenant, &new_snap],
        )
        .await?;
    }
    set_ref(&mut tx, did, "main", "branch", new_snap).await?;
    tx.audit("export", "receive-from-tenant", &format!("snapshot:{new_snap}"), true, Some(agreement), json!({"from_tenant": src_tenant, "from_snapshot": snap})).await?;
    let root = crate::metadata::acl_root_of(&tx, did).await?;
    tx.emit("snapshot.committed", new_snap, "snapshot", root, json!({"dataset": did, "summary": {"operation": "clone", "from_tenant": src_tenant}})).await?;
    tx.commit().await?;
    Ok(did)
}

// ---------------------------------------------------------------------------- annotation sets, representations, selections

/// Write an annotation set: derived attributes keyed by element (version ID and selector). The
/// values go to the manifest store. The caller then commits the set to a branch.
pub async fn write_annotation_set(
    tx: &mut Tx<'_>,
    dataset: &str,
    name: &str,
    enricher: Option<(&str, &str)>,
    key_kind: &str,
    rows: &[Value],
) -> Result<(Uuid, i32)> {
    let did = dataset_id(tx, dataset).await?;
    let commit = tx.commit_id().await?;
    let id = new_id();
    let version_no: i32 = tx
        .q1("select coalesce(max(version_no), 0) + 1 from annotation_set where dataset_id = $1 and name = $2", &[&did, &name])
        .await?
        .get(0);
    let tenant = tx.sess.tenant_id()?;
    let enriched: Vec<Value> = rows
        .iter()
        .map(|r| {
            let mut r = r.clone();
            if let Some((e, v)) = enricher {
                r["asserted_by"] = json!({"enricher": e, "version": v});
            }
            r
        })
        .collect();
    let file = tx.inst().manifests.write(tenant, id, &enriched)?;
    let mut h = String::new();
    for r in &enriched {
        h.push_str(&r.to_string());
    }
    let hash = sha256_hex(h.as_bytes());
    let (en, ev) = enricher.map(|(a, b)| (Some(a.to_string()), Some(b.to_string()))).unwrap_or((None, None));
    tx.exec(
        "insert into annotation_set (id, dataset_id, name, version_no, enricher, enricher_version, key_kind, manifest_file, entry_count, content_hash, created_commit)
         values ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)",
        &[&id, &did, &name, &version_no, &en, &ev, &key_kind, &file, &(rows.len() as i64), &hash, &commit],
    )
    .await?;
    let root = crate::metadata::acl_root_of(tx, did).await?;
    tx.emit("annotation-set.written", id, "annotation-set", root, json!({"dataset": did, "name": name, "version": version_no, "enricher": en, "enricher_version": ev, "entries": rows.len()}))
        .await?;
    Ok((id, version_no))
}

pub async fn annotation_set_info(tx: &Tx<'_>, id: Uuid) -> Result<Value> {
    let r = tx
        .qopt("select name, version_no, enricher, enricher_version, key_kind, entry_count, content_hash, dataset_id from annotation_set where id = $1", &[&id])
        .await?
        .ok_or(Error::NotFound)?;
    Ok(json!({"id": id, "name": r.get::<_, String>(0), "version": r.get::<_, i32>(1), "enricher": r.get::<_, Option<String>>(2),
              "enricher_version": r.get::<_, Option<String>>(3), "key_kind": r.get::<_, String>(4), "entries": r.get::<_, i64>(5),
              "content_hash": r.get::<_, String>(6), "dataset": r.get::<_, Uuid>(7)}))
}

pub async fn annotation_rows(tx: &Tx<'_>, id: Uuid) -> Result<Vec<Value>> {
    tx.inst().manifests.read(tx.sess.tenant_id()?, id)
}

/// Add an annotation set to a branch; it replaces the earlier version of the set with the same name.
pub async fn commit_annotation_set(tx: &mut Tx<'_>, dataset: &str, branch: &str, set: Uuid) -> Result<Committed> {
    let did = dataset_id(tx, dataset).await?;
    let name: String = tx.q1("select name from annotation_set where id = $1", &[&set]).await?.get(0);
    let head = resolve_ref(tx, did, branch, None).await?;
    let current = members(tx, head).await?;
    let ids: Vec<Uuid> = current.iter().filter(|e| e.member_kind == "annotation-set").map(|e| e.member_id).collect();
    let old: Vec<Uuid> = tx.q("select id from annotation_set where id = any($1) and name = $2", &[&ids, &name]).await?.iter().map(|r| r.get(0)).collect();
    let ch = if old.is_empty() {
        DatasetChange { dataset: dataset.into(), branch: branch.into(), requirement: None, add: vec![Member::annotation_set(set)], ..Default::default() }
    } else {
        DatasetChange {
            dataset: dataset.into(),
            branch: branch.into(),
            requirement: Some(head),
            replace: vec![(old[0], Member::annotation_set(set))],
            ..Default::default()
        }
    };
    Ok(commit(tx, &[ch]).await?.remove(0))
}

/// A representation: one physical form of a snapshot, made of its own versions.
pub async fn add_representation(tx: &mut Tx<'_>, snap: Uuid, name: &str, versions: &[Uuid], transform: Value, activity: Option<Uuid>) -> Result<Uuid> {
    let commit = tx.commit_id().await?;
    let id = new_id();
    let versions = versions.to_vec();
    tx.exec(
        "insert into representation (id, snapshot_id, name, transform, activity_id, members, created_commit) values ($1,$2,$3,$4,$5,$6,$7)",
        &[&id, &snap, &name, &transform, &activity, &versions, &commit],
    )
    .await?;
    for v in &versions {
        tx.exec("insert into derivation (entity_id, from_id, kind) values ($1,$2,'materializedFrom') on conflict do nothing", &[v, &snap]).await?;
    }
    tx.emit("representation.created", id, "representation", None, json!({"snapshot": snap, "name": name, "members": versions.len()})).await?;
    Ok(id)
}

/// A selection: a no-copy subset of a snapshot, defined by a predicate. mudcat evaluates the
/// predicate again at each resolution.
pub async fn create_selection(tx: &mut Tx<'_>, dataset: &str, name: &str, predicate: &Value) -> Result<Value> {
    let did = dataset_id(tx, dataset).await?;
    let snap = resolve_ref(tx, did, "main", None).await?;
    let entries = members(tx, snap).await?;
    let selected = crate::resolver::apply_selection(tx, &entries, predicate).await?;
    let mut ids: Vec<Uuid> = selected.iter().map(|e| e.member_id).collect();
    ids.sort();
    let hash = sha256_hex(ids.iter().map(|i| i.to_string()).collect::<Vec<_>>().join(",").as_bytes());
    let elements: i64 = selected.iter().map(|e| e.element_count).sum();
    let commit = tx.commit_id().await?;
    let id = new_id();
    tx.exec(
        "insert into selection (id, name, dataset_id, snapshot_id, predicate, element_hash, count, created_commit) values ($1,$2,$3,$4,$5,$6,$7,$8)",
        &[&id, &name, &did, &snap, predicate, &hash, &elements, &commit],
    )
    .await?;
    namespace::set_identifier(tx, "selection", &format!("{dataset}#{name}"), id).await?;
    tx.emit("selection.created", id, "selection", None, json!({"dataset": did, "name": name, "elements": elements, "hash": hash})).await?;
    Ok(json!({"id": id, "name": name, "snapshot": snap, "members": ids.len(), "elements": elements, "element_hash": hash}))
}

pub async fn selection(tx: &Tx<'_>, id_or_name: &str) -> Result<(Uuid, Value)> {
    let id = match id_or_name.parse::<Uuid>() {
        Ok(id) => id,
        Err(_) => namespace::resolve_identifier(tx, "selection", id_or_name, None).await?.ok_or(Error::NotFound)?,
    };
    let r = tx.qopt("select predicate from selection where id = $1", &[&id]).await?.ok_or(Error::NotFound)?;
    Ok((id, r.get(0)))
}

/// Make a dataset whose snapshot holds the versions that a collection subtree held at a time (ID-10).
pub async fn snapshot_of_collection(tx: &mut Tx<'_>, path: &str, at: chrono::DateTime<chrono::Utc>, dataset: &str) -> Result<Committed> {
    let c = namespace::commit_at(tx, at).await?;
    let versions = namespace::subtree_versions(tx, path, Some(c)).await?;
    create_dataset(tx, dataset, &DatasetOptions::default()).await?;
    let ch = DatasetChange { dataset: dataset.into(), add: versions.iter().map(|v| Member::version(v.id)).collect(), ..Default::default() };
    Ok(commit(tx, &[ch]).await?.remove(0))
}

// ---------------------------------------------------------------------------- maintenance

/// Seal the unsealed manifests of the head snapshot of a branch into one sealed manifest file.
/// Only where the members are stored changes, not what they are (OP-6).
pub async fn seal(tx: &mut Tx<'_>, dataset: &str, branch: &str) -> Result<Option<Uuid>> {
    let did = dataset_id(tx, dataset).await?;
    let snap = resolve_ref(tx, did, branch, None).await?;
    let me = tx.sess.tenant_id()?;
    let refs = tx
        .q(
            "select r.ordinal, r.manifest_id from manifest_ref r join manifest m on m.id = r.manifest_id and m.tenant_id = r.tenant_id
              where r.snapshot_id = $1 and r.manifest_tenant = $2 and not m.sealed order by r.ordinal",
            &[&snap, &me],
        )
        .await?;
    if refs.len() < 2 {
        return Ok(None);
    }
    let ords: Vec<i32> = refs.iter().map(|r| r.get(0)).collect();
    // the entries of these references, without their exclusions
    let all = members(tx, snap).await?;
    let mids: Vec<Uuid> = refs.iter().map(|r| r.get(1)).collect();
    let in_unsealed: HashSet<Uuid> = tx
        .q("select member_id from manifest_entry where manifest_id = any($1)", &[&mids])
        .await?
        .iter()
        .map(|r| r.get(0))
        .collect();
    let excluded_elsewhere: HashSet<Uuid> = tx
        .q("select unnest(excluded) from manifest_ref where snapshot_id = $1 and not (ordinal = any($2))", &[&snap, &ords])
        .await?
        .iter()
        .map(|r| r.get(0))
        .collect();
    let entries: Vec<Entry> = all.into_iter().filter(|e| in_unsealed.contains(&e.member_id) && !excluded_elsewhere.contains(&e.member_id)).collect();
    let commit = tx.commit_id().await?;
    let mid = new_id();
    let sources: Vec<Uuid> = entries.iter().filter_map(|e| e.source_id).collect::<BTreeSet<_>>().into_iter().collect();
    let rows: Vec<Value> = entries.iter().map(|e| serde_json::to_value(e).unwrap()).collect();
    let file = tx.inst().manifests.write(me, mid, &rows)?;
    let total: i64 = entries.iter().map(|e| e.size).sum();
    tx.exec(
        "insert into manifest (id, sealed, entry_count, total_size, sources, file, created_commit) values ($1,true,$2,$3,$4,$5,$6)",
        &[&mid, &(entries.len() as i64), &total, &sources, &file, &commit],
    )
    .await?;
    insert_entries(tx, mid, &entries).await?;
    tx.exec("delete from manifest_ref where snapshot_id = $1 and ordinal = any($2)", &[&snap, &ords]).await?;
    tx.exec(
        "insert into manifest_ref (snapshot_id, ordinal, manifest_tenant, manifest_id, excluded) values ($1,$2,$3,$4,'{}')",
        &[&snap, &ords[0], &me, &mid],
    )
    .await?;
    tx.emit("manifest.sealed", mid, "manifest", None, json!({"snapshot": snap, "entries": entries.len(), "merged": mids.len()})).await?;
    Ok(Some(mid))
}

/// Expire unpublished snapshots that are older than the retention of their dataset and that no
/// ref and no resolution refers to (ID-9). Remove the manifests that only they used.
pub async fn expire(tx: &mut Tx<'_>) -> Result<Vec<Uuid>> {
    let rows = tx
        .q(
            "select s.id from snapshot s join dataset d on d.id = s.dataset_id and d.tenant_id = s.tenant_id
              where s.status is null and s.expired_at is null
                and s.created_at < $1::timestamptz - make_interval(days => d.retention_days)
                and not exists (select 1 from ref r where r.snapshot_id = s.id)
                and not exists (select 1 from resolution x where x.inputs::text like '%' || s.id::text || '%')
                and not exists (select 1 from manifest_entry e where e.member_id = s.id)",
            &[&tx.now],
        )
        .await?;
    let ids: Vec<Uuid> = rows.iter().map(|r| r.get(0)).collect();
    if ids.is_empty() {
        return Ok(ids);
    }
    tx.exec("update snapshot set expired_at = $2 where id = any($1)", &[&ids, &tx.now]).await?;
    let mids: Vec<Uuid> = tx.q("select distinct manifest_id from manifest_ref where snapshot_id = any($1)", &[&ids]).await?.iter().map(|r| r.get(0)).collect();
    tx.exec("delete from manifest_ref where snapshot_id = any($1)", &[&ids]).await?;
    // remove a manifest only when no reference in any tenant names it (OP-15)
    for m in mids {
        let used = tx.qopt("select 1 from manifest_ref where manifest_id = $1 limit 1", &[&m]).await?;
        let foreign = tx.qopt("select 1 from cross_tenant_ref where target_id = $1 limit 1", &[&m]).await?;
        if used.is_none() && foreign.is_none() {
            tx.exec("delete from manifest_entry where manifest_id = $1", &[&m]).await?;
            tx.exec("delete from manifest where id = $1", &[&m]).await?;
        }
    }
    for id in &ids {
        tx.emit("snapshot.expired", *id, "snapshot", None, json!({})).await?;
    }
    Ok(ids)
}

/// Apply the follow rule of a dataset: a new snapshot on main with the latest versions of the
/// collection that it follows (use case 2, UC2-06).
pub async fn apply_follow_rule(tx: &mut Tx<'_>, dataset: &str) -> Result<Option<Committed>> {
    let did = dataset_id(tx, dataset).await?;
    let follow: Option<Value> = tx.q1("select follow from dataset where id = $1", &[&did]).await?.get(0);
    let Some(coll) = follow.and_then(|f| f["collection"].as_str().map(|s| s.to_string())) else { return Ok(None) };
    let head = resolve_ref(tx, did, "main", None).await?;
    let current = members(tx, head).await?;
    let latest = namespace::subtree_versions(tx, &coll, None).await?;
    let current_versions = namespace::versions(tx, &current.iter().map(|e| e.member_id).collect::<Vec<_>>()).await?;
    let mut replace = Vec::new();
    let mut add = Vec::new();
    for l in &latest {
        match current_versions.iter().find(|c| c.artifact_id == l.artifact_id) {
            Some(c) if c.id != l.id => replace.push((c.id, Member::version(l.id))),
            Some(_) => {}
            None => add.push(Member::version(l.id)),
        }
    }
    if replace.is_empty() && add.is_empty() {
        return Ok(None);
    }
    let ch = DatasetChange { dataset: dataset.into(), branch: "main".into(), requirement: Some(head), add, replace, remove: vec![] };
    Ok(Some(commit(tx, &[ch]).await?.remove(0)))
}


/// What the data policy shows of a dataset to a principal that cannot read its data: for a
/// signed-in user, that the dataset exists, its description, its embargo date and its published
/// snapshots with DOIs; for an anonymous user, nothing (use case 5).
pub async fn restricted_view(tx: &Tx<'_>, dataset: Uuid) -> Result<Value> {
    if tx.principal().is_anonymous() {
        return Err(Error::NotFound);
    }
    let path = dataset_path(tx, dataset).await?;
    let desc: Option<String> = tx
        .qopt("select payload->>'description' from metadata_payload where entity_id = $1 and payload ? 'description' and status = 'visible' limit 1", &[&dataset])
        .await?
        .and_then(|r| r.get(0));
    let embargo = crate::authz::embargo_until(tx, "dataset", dataset).await?;
    let dois: Vec<Value> = tx
        .q(
            "select i.value, s.id from snapshot s join identifier i on i.entity_id = s.id and i.tenant_id = s.tenant_id and i.scheme = 'doi' and i.valid_to is null where s.dataset_id = $1",
            &[&dataset],
        )
        .await?
        .iter()
        .map(|r| json!({"doi": r.get::<_, String>(0), "snapshot": r.get::<_, Uuid>(1)}))
        .collect();
    Ok(json!({"dataset": dataset, "path": path, "description": desc, "embargo_until": embargo, "public_snapshots": dois}))
}
