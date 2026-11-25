//! Provenance with PROV-O: activities, qualified usage with roles, generation and derivation
//! (model.md, section 6), and the reports that come from provenance (QY-1, QY-2, QY-6).

use crate::datasets;
use crate::error::{Error, Result};
use crate::instance::{new_id, sha256_hex, Tx};
use crate::namespace::{self, Register};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use uuid::Uuid;

/// Roles that are marked ubiquitous. Default ancestry queries exclude them (PR-3).
pub const UBIQUITOUS: &[&str] = &["calibration", "tokenizer", "baseModel", "configuration"];

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct RunSpec {
    /// training | evaluation | ingest | transform | answer | simulation | experiment
    pub kind: String,
    #[serde(default)]
    pub code: Option<String>,
    #[serde(default)]
    pub image: Option<String>,
    #[serde(default)]
    pub params: Value,
    #[serde(default)]
    pub location: Option<String>,
    #[serde(default)]
    pub resolution: Option<Uuid>,
    /// Entities that the run used, with their roles: [{"entity", "role", "selector"}].
    #[serde(default)]
    pub used: Vec<Value>,
    /// declared (the default), observed or imported.
    #[serde(default)]
    pub capture: Option<String>,
    /// The full run record (a prov:Bundle): configuration, logs, task status.
    #[serde(default)]
    pub record: Value,
}

/// Record a run: an activity, its qualified usages, and its full record as a catalogued version (PR-5).
pub async fn record_run(tx: &mut Tx<'_>, run: &RunSpec) -> Result<Uuid> {
    let id = new_id();
    let capture = run.capture.clone().unwrap_or_else(|| "declared".into());
    let params = if run.params.is_null() { json!({}) } else { run.params.clone() };
    let run_token: Option<String> = match run.resolution {
        Some(r) => tx.qopt("select run_token from resolution where id = $1", &[&r]).await?.and_then(|r| r.get(0)),
        None => None,
    };
    tx.exec(
        "insert into activity (id, kind, code, image, params, started_at, ended_at, location, resolution_id, run_token, capture)
         values ($1,$2,$3,$4,$5,$6,$6,$7,$8,$9,$10)",
        &[&id, &run.kind, &run.code, &run.image, &params, &tx.now, &run.location, &run.resolution, &run_token, &capture],
    )
    .await?;
    tx.emit("activity.started", id, "activity", None, json!({"kind": run.kind, "code": run.code, "image": run.image, "location": run.location})).await?;
    // usages from the resolution, in their roles
    if let Some(r) = run.resolution {
        let inputs: Value = tx.qopt("select inputs from resolution where id = $1", &[&r]).await?.ok_or(Error::NotFound)?.get(0);
        for i in inputs.as_array().cloned().unwrap_or_default() {
            let role = i["role"].as_str().unwrap_or("training").to_string();
            for key in ["snapshot", "version"] {
                if let Some(e) = i[key].as_str().and_then(|s| s.parse::<Uuid>().ok()) {
                    add_usage(tx, id, e, &role, &capture, i.get("selector").cloned()).await?;
                }
            }
            for set in i["annotation_sets"].as_array().cloned().unwrap_or_default() {
                if let Some(e) = set["id"].as_str().and_then(|s| s.parse::<Uuid>().ok()) {
                    add_usage(tx, id, e, &role, &capture, None).await?;
                }
            }
        }
        tx.exec("insert into usage (activity_id, entity_id, role, capture) values ($1,$2,'resolution',$3) on conflict do nothing", &[&id, &r, &capture]).await?;
    }
    for u in &run.used {
        let e: Uuid = u["entity"].as_str().and_then(|s| s.parse().ok()).ok_or_else(|| Error::invalid("a usage names an entity"))?;
        add_usage(tx, id, e, u["role"].as_str().unwrap_or("reference"), &capture, u.get("selector").cloned()).await?;
    }
    // the full record is a catalogued version, not rows in the catalog (PR-5)
    let bundle = json!({"@type": "prov:Bundle", "activity": id, "kind": run.kind, "code": run.code, "image": run.image, "params": params,
                        "location": run.location, "resolution": run.resolution, "record": run.record});
    let bytes = serde_json::to_vec(&bundle)?;
    let hash = sha256_hex(&bytes);
    let tenant = tx.sess.tenant_id()?;
    let mid = new_id();
    tx.inst().manifests.write(tenant, mid, &[bundle.clone()])?;
    let reg = namespace::register(
        tx,
        &Register {
            path: format!("/runs/{id}"),
            kind: Some("run-record".into()),
            content_hash: Some(hash),
            size: bytes.len() as i64,
            structure: Some(json!({"kind": "records", "format": "prov-bundle+json", "manifest": mid})),
            format: Some("application/ld+json".into()),
            ..Default::default()
        },
    )
    .await?;
    tx.exec("update activity set run_record_version = $2 where id = $1", &[&id, &reg.version_id]).await?;
    tx.exec("insert into generation (activity_id, entity_id) values ($1,$2) on conflict do nothing", &[&id, &reg.version_id]).await?;
    tx.emit("activity.ended", id, "activity", None, json!({"run_record": reg.version_id})).await?;
    Ok(id)
}

pub async fn add_usage(tx: &mut Tx<'_>, activity: Uuid, entity: Uuid, role: &str, capture: &str, selector: Option<Value>) -> Result<()> {
    tx.exec(
        "insert into usage (activity_id, entity_id, role, capture, selector) values ($1,$2,$3,$4,$5)
         on conflict (tenant_id, activity_id, entity_id, role) do update set capture = case when usage.capture = 'observed' then 'observed' else excluded.capture end",
        &[&activity, &entity, &role, &capture, &selector],
    )
    .await?;
    tx.emit("usage.recorded", activity, "activity", None, json!({"entity": entity, "role": role, "capture": capture})).await?;
    Ok(())
}

/// A read connector reports the reads that a job made with credentials under a run token. mudcat
/// records them as observed usage (PR-4).
pub async fn report_reads(tx: &mut Tx<'_>, run_token: &str, reads: usize) -> Result<Uuid> {
    let r = tx.qopt("select id, inputs from resolution where run_token = $1", &[&run_token]).await?.ok_or(Error::NotFound)?;
    let rid: Uuid = r.get(0);
    let inputs: Value = r.get(1);
    let act = match tx.qopt("select id from activity where run_token = $1 order by started_at limit 1", &[&run_token]).await? {
        Some(a) => a.get(0),
        None => {
            let id = new_id();
            tx.exec(
                "insert into activity (id, kind, started_at, resolution_id, run_token, capture, params) values ($1,'training',$2,$3,$4,'observed','{}')",
                &[&id, &tx.now, &rid, &run_token],
            )
            .await?;
            tx.emit("activity.started", id, "activity", None, json!({"kind": "training", "run_token": run_token})).await?;
            id
        }
    };
    for i in inputs.as_array().cloned().unwrap_or_default() {
        let role = i["role"].as_str().unwrap_or("training").to_string();
        for key in ["snapshot", "version"] {
            if let Some(e) = i[key].as_str().and_then(|s| s.parse::<Uuid>().ok()) {
                add_usage(tx, act, e, &role, "observed", i.get("selector").cloned()).await?;
            }
        }
        for set in i["annotation_sets"].as_array().cloned().unwrap_or_default() {
            if let Some(e) = set["id"].as_str().and_then(|s| s.parse::<Uuid>().ok()) {
                add_usage(tx, act, e, &role, "observed", None).await?;
            }
        }
    }
    tx.exec("update activity set params = params || $2 where id = $1", &[&act, &json!({"observed_reads": reads})]).await?;
    Ok(act)
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct ModelSpec {
    pub path: String,
    pub content_hash: String,
    #[serde(default)]
    pub size: i64,
    /// The activity that generated the model, usually a training run.
    #[serde(default)]
    pub activity: Option<Uuid>,
    /// Base models: [(model version, derivation kind)], for example fineTunedFrom.
    #[serde(default)]
    pub derived_from: Vec<(Uuid, String)>,
    #[serde(default)]
    pub source: Option<String>,
}

/// Publish a model version. Its lineage leads to the run, the resolution and the inputs. The
/// duties of the sources propagate to it (UP-4).
pub async fn publish_model(tx: &mut Tx<'_>, m: &ModelSpec) -> Result<Uuid> {
    let reg = namespace::register(
        tx,
        &Register {
            path: m.path.clone(),
            kind: Some("model".into()),
            content_hash: Some(m.content_hash.clone()),
            size: m.size,
            structure: Some(json!({"kind": "opaque", "format": "safetensors"})),
            source: m.source.clone(),
            ..Default::default()
        },
    )
    .await?;
    let v = reg.version_id;
    if let Some(a) = m.activity {
        tx.exec("insert into generation (activity_id, entity_id) values ($1,$2) on conflict do nothing", &[&a, &v]).await?;
        let rows = tx.q("select entity_id, role from usage where activity_id = $1", &[&a]).await?;
        for r in rows {
            let e: Uuid = r.get(0);
            let role: String = r.get(1);
            if role != "resolution" && !UBIQUITOUS.contains(&role.as_str()) {
                tx.exec("insert into derivation (entity_id, from_id, kind) values ($1,$2,'wasDerivedFrom') on conflict do nothing", &[&v, &e]).await?;
            }
        }
    }
    for (base, kind) in &m.derived_from {
        tx.exec("insert into derivation (entity_id, from_id, kind) values ($1,$2,$3) on conflict do nothing", &[&v, base, kind]).await?;
    }
    let duties = model_duties(tx, v).await?;
    let root = crate::metadata::acl_root_of(tx, v).await?;
    tx.emit("model.published", v, "version", root, json!({"path": m.path, "activity": m.activity, "duties": duties})).await?;
    Ok(v)
}

/// The training inputs of a model version: for each training run, its resolution and the
/// member versions that it resolved.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TrainingInput {
    pub activity: Uuid,
    pub resolution: Option<Uuid>,
    pub role: String,
    pub snapshot: Option<Uuid>,
    pub versions: Vec<Uuid>,
    pub elements: i64,
    pub selector: Option<Value>,
    pub sources: BTreeMap<Uuid, i64>,
}

pub async fn training_runs(tx: &Tx<'_>, model_version: Uuid) -> Result<Vec<(Uuid, Option<Uuid>)>> {
    let rows = tx
        .q(
            "select a.id, a.resolution_id from generation g join activity a on a.id = g.activity_id and a.tenant_id = g.tenant_id
              where g.entity_id = $1 and a.kind = 'training' order by a.started_at",
            &[&model_version],
        )
        .await?;
    Ok(rows.iter().map(|r| (r.get(0), r.get(1))).collect())
}

/// The members of one resolution, as recorded: the exact inputs, less what it excluded.
pub async fn resolution_members(tx: &Tx<'_>, resolution: Uuid, roles: Option<&[&str]>) -> Result<Vec<(String, Option<Uuid>, Vec<datasets::Entry>, Option<Value>)>> {
    let r = tx.qopt("select inputs, decision from resolution where id = $1", &[&resolution]).await?.ok_or(Error::NotFound)?;
    let inputs: Value = r.get(0);
    let decision: Value = r.get(1);
    let excluded: HashSet<String> = decision["excluded"]["members"].as_array().cloned().unwrap_or_default().iter().filter_map(|v| v.as_str().map(|s| s.to_string())).collect();
    let mut out = Vec::new();
    for i in inputs.as_array().cloned().unwrap_or_default() {
        let role = i["role"].as_str().unwrap_or("training").to_string();
        if let Some(rs) = roles {
            if !rs.contains(&role.as_str()) {
                continue;
            }
        }
        let mut entries = Vec::new();
        let mut snap = None;
        if let Some(s) = i["snapshot"].as_str().and_then(|s| s.parse::<Uuid>().ok()) {
            snap = Some(s);
            let mut stack = vec![s];
            let mut seen = HashSet::new();
            while let Some(x) = stack.pop() {
                if !seen.insert(x) {
                    continue;
                }
                for e in datasets::members(tx, x).await? {
                    match e.member_kind.as_str() {
                        "snapshot" => stack.push(e.member_id),
                        "version" => entries.push(e),
                        _ => {}
                    }
                }
            }
            if let Some(sel) = i.get("selection").filter(|v| !v.is_null()) {
                entries = crate::resolver::apply_selection(tx, &entries, sel).await?;
            }
        } else {
            let ids: Vec<Uuid> = if let Some(v) = i["version"].as_str().and_then(|s| s.parse::<Uuid>().ok()) {
                vec![v]
            } else {
                i["versions"].as_array().cloned().unwrap_or_default().iter().filter_map(|v| v.as_str().and_then(|s| s.parse().ok())).collect()
            };
            let rows = tx.q("select id, coalesce(content_hash, ''), source_id, size, element_count from version where id = any($1)", &[&ids]).await?;
            for r in rows {
                entries.push(datasets::Entry { member_id: r.get(0), member_kind: "version".into(), member_hash: r.get(1), source_id: r.get(2), size: r.get(3), element_count: r.get(4) });
            }
        }
        entries.retain(|e| !excluded.contains(&e.member_id.to_string()));
        out.push((role, snap, entries, i.get("selector").cloned().filter(|v| !v.is_null())));
    }
    Ok(out)
}

pub async fn training_inputs(tx: &Tx<'_>, model_version: Uuid) -> Result<Vec<TrainingInput>> {
    let mut out = Vec::new();
    for (act, res) in training_runs(tx, model_version).await? {
        let Some(res) = res else {
            out.push(TrainingInput { activity: act, resolution: None, role: "training".into(), snapshot: None, versions: vec![], elements: 0, selector: None, sources: BTreeMap::new() });
            continue;
        };
        for (role, snap, entries, selector) in resolution_members(tx, res, Some(&["training"])).await? {
            let mut sources = BTreeMap::new();
            for e in &entries {
                if let Some(s) = e.source_id {
                    *sources.entry(s).or_insert(0) += e.element_count;
                }
            }
            out.push(TrainingInput {
                activity: act,
                resolution: Some(res),
                role,
                snapshot: snap,
                elements: entries.iter().map(|e| e.element_count).sum(),
                versions: entries.iter().map(|e| e.member_id).collect(),
                selector,
                sources,
            });
        }
    }
    Ok(out)
}

/// The base models of a model version, nearest first (fineTunedFrom, distilledFrom, ...).
pub async fn base_models(tx: &Tx<'_>, model_version: Uuid) -> Result<Vec<Uuid>> {
    let mut out = Vec::new();
    let mut cur = vec![model_version];
    for _ in 0..32 {
        let rows = tx
            .q(
                "select d.from_id from derivation d join version v on v.id = d.from_id and v.tenant_id = d.tenant_id
                   join artifact a on a.id = v.artifact_id and a.tenant_id = v.tenant_id
                  where d.entity_id = any($1) and a.kind = 'model'",
                &[&cur],
            )
            .await?;
        cur = rows.iter().map(|r| r.get(0)).filter(|m| !out.contains(m)).collect();
        if cur.is_empty() {
            break;
        }
        out.extend(cur.iter().copied());
    }
    Ok(out)
}

/// For affected versions: each model whose training resolution contains some of them, with
/// the affected versions it contains. Models fine-tuned from such a model are included.
pub async fn models_with_inputs(tx: &Tx<'_>, versions: &[Uuid]) -> Result<Vec<(Uuid, Vec<Uuid>)>> {
    // snapshots that contain the versions, also through nested snapshots
    let mut containers: BTreeSet<Uuid> = BTreeSet::new();
    let mut frontier: Vec<Uuid> = versions.to_vec();
    for _ in 0..8 {
        let rows = tx
            .q(
                "select distinct r.snapshot_id from manifest_entry e join manifest_ref r on r.manifest_id = e.manifest_id and r.manifest_tenant = e.tenant_id
                  where e.member_id = any($1) and not (e.member_id = any(r.excluded))",
                &[&frontier],
            )
            .await?;
        frontier = rows.iter().map(|r| r.get(0)).filter(|s| !containers.contains(s)).collect();
        if frontier.is_empty() {
            break;
        }
        containers.extend(frontier.iter().copied());
    }
    let mut keys: Vec<String> = containers.iter().map(|s| s.to_string()).collect();
    keys.extend(versions.iter().map(|v| v.to_string()));
    let rows = tx
        .q(
            "select x.id from resolution x where exists (select 1 from unnest($1::text[]) k where x.inputs::text like '%' || k || '%')",
            &[&keys],
        )
        .await?;
    let affected: HashSet<Uuid> = versions.iter().copied().collect();
    let mut out: BTreeMap<Uuid, BTreeSet<Uuid>> = BTreeMap::new();
    for r in rows {
        let res: Uuid = r.get(0);
        let members = resolution_members(tx, res, Some(&["training", "validation"])).await?;
        let hit: BTreeSet<Uuid> = members.iter().flat_map(|m| m.2.iter().map(|e| e.member_id)).filter(|v| affected.contains(v)).collect();
        if hit.is_empty() {
            continue;
        }
        let models = tx
            .q("select g.entity_id from activity a join generation g on g.activity_id = a.id and g.tenant_id = a.tenant_id
                 join version v on v.id = g.entity_id and v.tenant_id = g.tenant_id join artifact ar on ar.id = v.artifact_id and ar.tenant_id = v.tenant_id
                 where a.resolution_id = $1 and ar.kind = 'model'", &[&res])
            .await?;
        for m in models {
            let mv: Uuid = m.get(0);
            out.entry(mv).or_default().extend(hit.iter().copied());
            // models derived from this model used the content too
            for d in derived_models(tx, mv).await? {
                out.entry(d).or_default().extend(hit.iter().copied());
            }
        }
    }
    Ok(out.into_iter().map(|(k, v)| (k, v.into_iter().collect())).collect())
}

async fn derived_models(tx: &Tx<'_>, model: Uuid) -> Result<Vec<Uuid>> {
    let mut out = Vec::new();
    let mut cur = vec![model];
    for _ in 0..16 {
        let rows = tx
            .q("select d.entity_id from derivation d join version v on v.id = d.entity_id and v.tenant_id = d.tenant_id
                 join artifact a on a.id = v.artifact_id and a.tenant_id = v.tenant_id where d.from_id = any($1) and a.kind = 'model'", &[&cur])
            .await?;
        cur = rows.iter().map(|r| r.get(0)).filter(|m| !out.contains(m)).collect();
        if cur.is_empty() {
            break;
        }
        out.extend(cur.iter().copied());
    }
    Ok(out)
}

/// Everything derived from entities: versions of a source, snapshots that contain a version, and
/// entities derived from those (for tag and duty propagation).
pub async fn downstream(tx: &Tx<'_>, ids: &[Uuid], limit: usize) -> Result<Vec<Uuid>> {
    let mut seen: BTreeSet<Uuid> = ids.iter().copied().collect();
    let mut frontier: Vec<Uuid> = ids.to_vec();
    while !frontier.is_empty() && seen.len() < limit {
        let rows = tx
            .q(
                "select id from version where source_id = any($1)
                 union select entity_id from derivation where from_id = any($1)
                 union select r.snapshot_id from manifest_entry e join manifest_ref r on r.manifest_id = e.manifest_id and r.manifest_tenant = e.tenant_id where e.member_id = any($1)
                 union select g.entity_id from usage u join generation g on g.activity_id = u.activity_id and g.tenant_id = u.tenant_id
                        where u.entity_id = any($1) and u.role not in ('calibration','tokenizer','configuration')",
                &[&frontier],
            )
            .await?;
        frontier = rows.iter().map(|r| r.get(0)).filter(|x| seen.insert(*x)).collect();
    }
    Ok(seen.into_iter().collect())
}

/// The duties that a model carries from the sources of its training data and of its base models.
pub async fn model_duties(tx: &Tx<'_>, model_version: Uuid) -> Result<Vec<Value>> {
    let mut chain = vec![model_version];
    chain.extend(base_models(tx, model_version).await?);
    let mut sources = BTreeSet::new();
    for m in &chain {
        for t in training_inputs(tx, *m).await? {
            sources.extend(t.sources.keys().copied());
        }
        if let Some(s) = tx.qopt("select source_id from version where id = $1", &[m]).await?.and_then(|r| r.get::<_, Option<Uuid>>(0)) {
            sources.insert(s);
        }
    }
    let sources: Vec<Uuid> = sources.into_iter().collect();
    crate::policy::duties_of_sources(tx, &sources).await
}

/// The state of a model: its flags (invalidated inputs, and licences that ended), its duties.
/// Flags for ended licences are computed at the time of the query.
pub async fn model_state(tx: &Tx<'_>, model_version: Uuid) -> Result<Value> {
    let mut flags = crate::policy::flags_of(tx, model_version).await?;
    let mut ended = Vec::new();
    for t in training_inputs(tx, model_version).await? {
        let Some(res) = t.resolution else { continue };
        let decision: Value = tx.q1("select decision from resolution where id = $1", &[&res]).await?.get(0);
        for p in decision["policies"].as_array().cloned().unwrap_or_default() {
            if let Some(na) = p["not_after"].as_str().and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok()) {
                if na.with_timezone(&chrono::Utc) < tx.now {
                    let src: Uuid = p["source"].as_str().and_then(|s| s.parse().ok()).unwrap_or_default();
                    let n = t.sources.get(&src).copied().unwrap_or(0);
                    ended.push(json!({"kind": "licence-ended", "source": p["name"], "ended_at": na, "affected_elements": n}));
                }
            }
        }
    }
    flags.extend(ended);
    let v = namespace::version(tx, model_version).await?;
    Ok(json!({"model_version": model_version, "path": v.path, "content_hash": v.content_hash, "flags": flags, "duties": model_duties(tx, model_version).await?}))
}

// ---------------------------------------------------------------------------- lineage

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct LineageQuery {
    /// ancestors | descendants
    pub direction: String,
    #[serde(default)]
    pub depth: Option<usize>,
    #[serde(default)]
    pub max_nodes: Option<usize>,
    #[serde(default)]
    pub include_ubiquitous: bool,
}

/// A traversal at the level of snapshots, versions, activities and resolutions. A traversal that
/// passes its limit of depth or nodes fails with an error that states the limit (PF-7).
pub async fn lineage(tx: &Tx<'_>, start: Uuid, q: &LineageQuery) -> Result<Value> {
    let depth = q.depth.unwrap_or(8);
    let max_nodes = q.max_nodes.unwrap_or(10_000);
    let mut nodes: BTreeMap<Uuid, Value> = BTreeMap::new();
    let mut edges = Vec::new();
    let mut frontier = vec![start];
    nodes.insert(start, node(tx, start).await?);
    let mut level = 0;
    while !frontier.is_empty() {
        if level >= depth {
            // is there more beyond the limit?
            let more = next_level(tx, &frontier, &q.direction, q.include_ubiquitous).await?;
            if more.iter().any(|(_, to, _)| !nodes.contains_key(to)) {
                return Err(Error::refused("lineage-limit", format!("the traversal passes its depth limit of {depth}; mudcat returns no partial result"), json!({"limit": "depth", "value": depth})));
            }
            break;
        }
        let next = next_level(tx, &frontier, &q.direction, q.include_ubiquitous).await?;
        frontier = Vec::new();
        for (from, to, rel) in next {
            edges.push(json!({"from": from, "to": to, "relation": rel}));
            if !nodes.contains_key(&to) {
                if nodes.len() >= max_nodes {
                    return Err(Error::refused("lineage-limit", format!("the traversal passes its limit of {max_nodes} nodes; mudcat returns no partial result"), json!({"limit": "nodes", "value": max_nodes})));
                }
                nodes.insert(to, node(tx, to).await?);
                frontier.push(to);
            }
        }
        level += 1;
    }
    Ok(json!({"start": start, "direction": q.direction, "nodes": nodes.into_values().collect::<Vec<_>>(), "edges": edges}))
}

async fn next_level(tx: &Tx<'_>, frontier: &[Uuid], direction: &str, ubiquitous: bool) -> Result<Vec<(Uuid, Uuid, String)>> {
    let ubi: Vec<String> = if ubiquitous { vec![] } else { UBIQUITOUS.iter().map(|s| s.to_string()).collect() };
    let rows = if direction == "descendants" {
        tx.q(
            "select from_id, entity_id, kind from derivation where from_id = any($1)
             union all select u.entity_id, u.activity_id, 'used:' || u.role from usage u where u.entity_id = any($1) and not (u.role = any($2))
             union all select activity_id, entity_id, 'generated' from generation where activity_id = any($1)
             union all select e.member_id, r.snapshot_id, 'memberOf' from manifest_entry e
                 join manifest_ref r on r.manifest_id = e.manifest_id and r.manifest_tenant = e.tenant_id
                where e.member_id = any($1) and e.member_kind = 'snapshot' and not (e.member_id = any(r.excluded))",
            &[&frontier, &ubi],
        )
        .await?
    } else {
        tx.q(
            "select g.entity_id, g.activity_id, 'wasGeneratedBy' from generation g where g.entity_id = any($1)
             union all select u.activity_id, u.entity_id, 'used:' || u.role from usage u where u.activity_id = any($1) and not (u.role = any($2))
             union all select d.entity_id, d.from_id, d.kind from derivation d where d.entity_id = any($1)
               and not exists (select 1 from generation g where g.entity_id = d.entity_id)
             union all select r.snapshot_id, e.member_id, 'hadMember' from manifest_ref r
                 join manifest_entry e on e.manifest_id = r.manifest_id and e.tenant_id = r.manifest_tenant
                where r.snapshot_id = any($1) and e.member_kind = 'snapshot' and not (e.member_id = any(r.excluded))",
            &[&frontier, &ubi],
        )
        .await?
    };
    Ok(rows.iter().map(|r| (r.get(0), r.get(1), r.get(2))).collect())
}

async fn node(tx: &Tx<'_>, id: Uuid) -> Result<Value> {
    if let Some(r) = tx.qopt("select kind, code, image, params, location, resolution_id, run_record_version from activity where id = $1", &[&id]).await? {
        return Ok(json!({"id": id, "type": "activity", "kind": r.get::<_, String>(0), "code": r.get::<_, Option<String>>(1), "image": r.get::<_, Option<String>>(2),
                         "params": r.get::<_, Value>(3), "location": r.get::<_, Option<String>>(4), "resolution": r.get::<_, Option<Uuid>>(5), "run_record": r.get::<_, Option<Uuid>>(6)}));
    }
    if let Some(r) = tx.qopt("select s.merkle_hash, s.status, d.path from snapshot s join dataset d on d.id = s.dataset_id and d.tenant_id = s.tenant_id where s.id = $1", &[&id]).await? {
        return Ok(json!({"id": id, "type": "snapshot", "merkle_hash": r.get::<_, Option<String>>(0), "status": r.get::<_, Option<String>>(1), "dataset": r.get::<_, String>(2)}));
    }
    if let Some(r) = tx.qopt("select action, purpose from resolution where id = $1", &[&id]).await? {
        return Ok(json!({"id": id, "type": "resolution", "action": r.get::<_, Option<String>>(0), "purpose": r.get::<_, Option<String>>(1)}));
    }
    if let Ok(v) = namespace::version(tx, id).await {
        return Ok(json!({"id": id, "type": "version", "path": v.path, "content_hash": v.content_hash}));
    }
    if let Some(r) = tx.qopt("select name, version_no from annotation_set where id = $1", &[&id]).await? {
        return Ok(json!({"id": id, "type": "annotation-set", "name": r.get::<_, String>(0), "version": r.get::<_, i32>(1)}));
    }
    Ok(json!({"id": id, "type": "entity"}))
}

// ---------------------------------------------------------------------------- reports

/// QY-1: the models that used data from a source in training, with the number of elements.
pub async fn models_using_source(tx: &Tx<'_>, source: Uuid) -> Result<Vec<Value>> {
    let versions: Vec<Uuid> = tx.q("select id from version where source_id = $1", &[&source]).await?.iter().map(|r| r.get(0)).collect();
    let counts: HashMap<Uuid, i64> = tx
        .q("select id, element_count from version where id = any($1)", &[&versions])
        .await?
        .iter()
        .map(|r| (r.get(0), r.get(1)))
        .collect();
    let mut out = Vec::new();
    for (m, inputs) in models_with_inputs(tx, &versions).await? {
        let n: i64 = inputs.iter().map(|v| counts.get(v).copied().unwrap_or(0)).sum();
        let v = namespace::version(tx, m).await?;
        out.push(json!({"model_version": m, "path": v.path, "elements": n, "flags": crate::policy::flags_of(tx, m).await?}));
    }
    Ok(out)
}

/// QY-2: the training-content summary of a model, from the catalog only. It reads no training data.
pub async fn content_summary(tx: &Tx<'_>, model_version: Uuid) -> Result<Value> {
    let mut chain = vec![model_version];
    chain.extend(base_models(tx, model_version).await?);
    let mut per_source: BTreeMap<Uuid, i64> = BTreeMap::new();
    let mut bytes_per_source: BTreeMap<Uuid, i64> = BTreeMap::new();
    let mut runs = Vec::new();
    let mut gaps = Vec::new();
    for m in &chain {
        let tr = training_runs(tx, *m).await?;
        for (act, res) in &tr {
            match res {
                None => gaps.push(json!({"run": act, "model_version": m, "reason": "the run has no resolution"})),
                Some(r) => {
                    runs.push(json!({"run": act, "model_version": m, "resolution": r}));
                    for (_, _, entries, _) in resolution_members(tx, *r, Some(&["training"])).await? {
                        for e in entries {
                            if let Some(s) = e.source_id {
                                *per_source.entry(s).or_default() += e.element_count;
                                *bytes_per_source.entry(s).or_default() += e.size;
                            }
                        }
                    }
                }
            }
        }
    }
    let ids: Vec<Uuid> = per_source.keys().copied().collect();
    let rows = tx.q("select id, name, kind, attrs, terms_status from source where id = any($1) order by name", &[&ids]).await?;
    let mut sections: BTreeMap<String, Vec<Value>> = BTreeMap::new();
    for r in &rows {
        let id: Uuid = r.get(0);
        let kind: String = r.get(2);
        let attrs: Value = r.get(3);
        let terms = tx.q("select format, content from terms_document where source_id = $1", &[&id]).await?;
        sections.entry(kind).or_default().push(json!({
            "source": id, "name": r.get::<_, String>(1), "elements": per_source[&id], "bytes": bytes_per_source[&id], "attrs": attrs,
            "terms": terms.iter().map(|t| json!({"format": t.get::<_, String>(0), "content": t.get::<_, Value>(1)})).collect::<Vec<_>>(),
        }));
    }
    let mut report = serde_json::Map::new();
    for (kind, items) in &sections {
        let section = match kind.as_str() {
            "crawl" => {
                let mut domains: BTreeMap<String, i64> = BTreeMap::new();
                let mut crawlers = BTreeSet::new();
                let mut periods = Vec::new();
                for i in items {
                    *domains.entry(i["attrs"]["domain"].as_str().unwrap_or("?").to_string()).or_default() += i["bytes"].as_i64().unwrap_or(0);
                    if let Some(c) = i["attrs"]["crawler"].as_str() {
                        crawlers.insert(c.to_string());
                    }
                    periods.push(json!({"source": i["name"], "from": i["attrs"]["fetch_from"], "to": i["attrs"]["fetch_to"]}));
                }
                let mut d: Vec<(String, i64)> = domains.into_iter().collect();
                d.sort_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(&b.0)));
                json!({"sources": items.len(), "elements": items.iter().map(|i| i["elements"].as_i64().unwrap_or(0)).sum::<i64>(),
                       "domains_by_size": d.iter().map(|(k, v)| json!({"domain": k, "bytes": v})).collect::<Vec<_>>(),
                       "crawlers": crawlers, "fetch_periods": periods})
            }
            "licensed" => json!({"sources": items.len(), "elements": items.iter().map(|i| i["elements"].as_i64().unwrap_or(0)).sum::<i64>(),
                                 "licensors": items.iter().map(|i| json!({"licensor": i["attrs"]["licensor"], "source": i["name"], "terms": i["terms"], "elements": i["elements"]})).collect::<Vec<_>>()}),
            _ => json!({"sources": items.len(), "elements": items.iter().map(|i| i["elements"].as_i64().unwrap_or(0)).sum::<i64>(),
                        "items": items.iter().map(|i| json!({"source": i["name"], "elements": i["elements"], "attrs": i["attrs"]})).collect::<Vec<_>>()}),
        };
        report.insert(kind.clone(), section);
    }
    let complete = gaps.is_empty();
    Ok(json!({
        "model_version": model_version, "models_in_chain": chain, "runs": runs, "sections": Value::Object(report),
        "elements_by_source": per_source.iter().map(|(k, v)| json!({"source": k, "elements": v})).collect::<Vec<_>>(),
        "complete": complete, "gaps": gaps,
        "statement": if complete { "The report covers all recorded runs.".to_string() } else { format!("The report is not complete: {} runs have no resolution.", gaps.len()) },
        "read_training_data": false,
    }))
}

/// QY-6: the runs, models, representations and caches that use data whose licence ends before a date.
pub async fn uses_of_expiring(tx: &Tx<'_>, before: chrono::DateTime<chrono::Utc>) -> Result<Value> {
    let srcs = tx
        .q("select distinct s.id, s.name, g.not_after from usage_grant g join source s on s.id = g.source_id and s.tenant_id = g.tenant_id where g.not_after is not null and g.not_after < $1", &[&before])
        .await?;
    let mut out = Vec::new();
    for s in srcs {
        let sid: Uuid = s.get(0);
        let versions: Vec<Uuid> = tx.q("select id from version where source_id = $1", &[&sid]).await?.iter().map(|r| r.get(0)).collect();
        let models = models_with_inputs(tx, &versions).await?;
        let mut run_ids = BTreeSet::new();
        for (m, _) in &models {
            for (a, _) in training_runs(tx, *m).await? {
                run_ids.insert(a);
            }
        }
        // representations and caches made from snapshots that contain the data
        let snaps: Vec<Uuid> = tx
            .q("select distinct r.snapshot_id from manifest_entry e join manifest_ref r on r.manifest_id = e.manifest_id and r.manifest_tenant = e.tenant_id where e.member_id = any($1)", &[&versions])
            .await?
            .iter()
            .map(|r| r.get(0))
            .collect();
        let reps = tx.q("select id, name, snapshot_id from representation where snapshot_id = any($1)", &[&snaps]).await?;
        let caches = tx.q("select snapshot_id from policy_cache where snapshot_id = any($1)", &[&snaps]).await?;
        out.push(json!({
            "source": sid, "name": s.get::<_, String>(1), "licence_ends": s.get::<_, chrono::DateTime<chrono::Utc>>(2),
            "runs": run_ids, "models": models.iter().map(|m| m.0).collect::<Vec<_>>(),
            "representations": reps.iter().map(|r| json!({"id": r.get::<_, Uuid>(0), "name": r.get::<_, String>(1), "snapshot": r.get::<_, Uuid>(2)})).collect::<Vec<_>>(),
            "caches": caches.iter().map(|r| json!({"policy_cache_of_snapshot": r.get::<_, Uuid>(0)})).collect::<Vec<_>>(),
        }));
    }
    Ok(json!({"before": before, "sources": out}))
}

/// QY-5: each occurrence of content, by checksum or fingerprint: versions and elements, the
/// datasets and snapshots that contain them, representations, annotation sets and replicas.
pub async fn occurrences(tx: &Tx<'_>, key: &str) -> Result<Value> {
    let hits = tx.q("select distinct version_id, element_id from identity_index where key = $1", &[&key]).await?;
    let mut versions = BTreeSet::new();
    let mut elements = Vec::new();
    for h in &hits {
        let v: Uuid = h.get(0);
        let e: String = h.get(1);
        versions.insert(v);
        if !e.is_empty() {
            elements.push(json!({"version": v, "element": e}));
        }
    }
    let vids: Vec<Uuid> = versions.iter().copied().collect();
    let snaps = tx
        .q(
            "select distinct r.snapshot_id, d.path, s.status from manifest_entry e join manifest_ref r on r.manifest_id = e.manifest_id and r.manifest_tenant = e.tenant_id
               join snapshot s on s.id = r.snapshot_id and s.tenant_id = r.tenant_id join dataset d on d.id = s.dataset_id and d.tenant_id = s.tenant_id
              where e.member_id = any($1) and not (e.member_id = any(r.excluded))",
            &[&vids],
        )
        .await?;
    let datasets: BTreeSet<String> = snaps.iter().map(|r| r.get::<_, String>(1)).collect();
    let reps = tx.q("select id, name, snapshot_id from representation where members && $1", &[&vids]).await?;
    let replicas = namespace::replicas_of(tx, &vids).await?;
    // annotation sets keyed by these versions
    let tenant = tx.sess.tenant_id()?;
    let mut sets = Vec::new();
    for r in tx.q("select id, name, version_no from annotation_set", &[]).await? {
        let sid: Uuid = r.get(0);
        if let Ok(rows) = tx.inst().manifests.read(tenant, sid) {
            if rows.iter().any(|x| x["version"].as_str().and_then(|s| s.parse::<Uuid>().ok()).map(|v| versions.contains(&v)).unwrap_or(false)) {
                sets.push(json!({"id": sid, "name": r.get::<_, String>(1), "version": r.get::<_, i32>(2)}));
            }
        }
    }
    Ok(json!({
        "key": key, "versions": vids, "elements": elements, "datasets": datasets,
        "snapshots": snaps.iter().map(|r| json!({"snapshot": r.get::<_, Uuid>(0), "dataset": r.get::<_, String>(1), "status": r.get::<_, Option<String>>(2)})).collect::<Vec<_>>(),
        "representations": reps.iter().map(|r| json!({"id": r.get::<_, Uuid>(0), "name": r.get::<_, String>(1), "snapshot": r.get::<_, Uuid>(2)})).collect::<Vec<_>>(),
        "annotation_sets": sets,
        "replicas": replicas.iter().map(|r| json!({"replica": r.id, "version": r.version_id, "resource": r.resource, "url": r.url, "state": r.state})).collect::<Vec<_>>(),
    }))
}

/// The published datasets that depend on a declared equivalence.
pub async fn datasets_with_equivalences(tx: &Tx<'_>) -> Result<Vec<Value>> {
    let rows = tx
        .q("select d.path, s.id, s.equivalences from snapshot s join dataset d on d.id = s.dataset_id and d.tenant_id = s.tenant_id where s.status = 'published' and cardinality(s.equivalences) > 0", &[])
        .await?;
    Ok(rows.iter().map(|r| json!({"dataset": r.get::<_, String>(0), "snapshot": r.get::<_, Uuid>(1), "equivalences": r.get::<_, Vec<Uuid>>(2)})).collect())
}

/// Models that used an input that now has a newer version (use case 2, UC2-06).
pub async fn models_with_newer_inputs(tx: &Tx<'_>) -> Result<Vec<Value>> {
    let models = tx
        .q("select v.id from version v join artifact a on a.id = v.artifact_id and a.tenant_id = v.tenant_id where a.kind = 'model'", &[])
        .await?;
    let mut out = Vec::new();
    for m in models {
        let mv: Uuid = m.get(0);
        let mut changed = Vec::new();
        for t in training_inputs(tx, mv).await? {
            let infos = namespace::versions(tx, &t.versions).await?;
            for v in infos {
                let latest: Option<Uuid> = tx.q1("select latest_version_id from artifact where id = $1", &[&v.artifact_id]).await?.get(0);
                if latest.map(|l| l != v.id).unwrap_or(false) {
                    changed.push(json!({"input": v.path, "used_version": v.id, "newer_version": latest}));
                }
            }
        }
        if !changed.is_empty() {
            out.push(json!({"model_version": mv, "changed_inputs": changed}));
        }
    }
    Ok(out)
}
