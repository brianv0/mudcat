//! The resolver (architecture.md, "Resolver"). It fixes each input of a job to an exact snapshot
//! or version, applies the invalidation set and the usage policy, picks replicas, plans read
//! tasks, issues credentials, and records the resolution. It is not in the data path: it never
//! reads data bytes.

use crate::authz;
use crate::datasets::{self, Entry};
use crate::error::{Error, Result};
use crate::instance::{new_id, Session, Tx, EVALUATOR_VERSION};
use crate::namespace::{self, ReplicaInfo, VersionInfo};
use crate::policy;
use crate::storage::{parse_url, Credential};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use uuid::Uuid;

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Input {
    #[serde(default)]
    pub dataset: Option<String>,
    #[serde(default, rename = "ref")]
    pub reference: Option<String>,
    #[serde(default)]
    pub snapshot: Option<Uuid>,
    #[serde(default)]
    pub artifact: Option<String>,
    #[serde(default)]
    pub version: Option<Uuid>,
    #[serde(default)]
    pub collection: Option<String>,
    #[serde(default)]
    pub as_of: Option<chrono::DateTime<chrono::Utc>>,
    /// A selection predicate, or {"name": "<dataset>#<selection>"}.
    #[serde(default)]
    pub selection: Option<Value>,
    /// An element-level selector, for example {"time": [1979, 2018]}.
    #[serde(default)]
    pub selector: Option<Value>,
    #[serde(default)]
    pub representation: Option<String>,
    #[serde(default = "training")]
    pub role: String,
}
fn training() -> String {
    "training".into()
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Request {
    pub inputs: Vec<Input>,
    #[serde(default)]
    pub action: Option<String>,
    #[serde(default)]
    pub purpose: Option<String>,
    /// The compute location of the job. The default is the place of the session.
    #[serde(default)]
    pub place: Option<String>,
    /// strict: use only replicas at the place. prefer (the default): prefer them.
    #[serde(default)]
    pub place_mode: Option<String>,
    /// whole (the default): every source must permit. permitted-sources-only: leave out the rest.
    #[serde(default)]
    pub scope: Option<String>,
    /// online (the default), or before-submission for a job that cannot reach mudcat.
    #[serde(default)]
    pub mode: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Task {
    pub version: Uuid,
    pub path: String,
    pub url: String,
    pub replica: Uuid,
    pub resource: String,
    pub location: String,
    /// object | element-list | selector
    pub addressing: String,
    pub selector: Option<Value>,
    pub elements: Option<Vec<String>>,
    pub expected_bytes: i64,
    pub expected_elements: i64,
    pub source: Option<Uuid>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct IssuedCredential {
    pub resource: String,
    pub credential: Credential,
    pub covers: usize,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Resolution {
    pub id: Uuid,
    pub inputs: Vec<Value>,
    pub decision: Value,
    pub tasks: Vec<Task>,
    pub credentials: Vec<IssuedCredential>,
    pub run_token: String,
    pub notices: Vec<Value>,
    /// For before-submission mode: the manifest file that the job reads with no connection to mudcat.
    pub manifest_file: Option<String>,
}

impl Resolution {
    pub fn total_elements(&self) -> i64 {
        self.tasks.iter().map(|t| t.expected_elements).sum()
    }
    pub fn versions(&self) -> BTreeSet<Uuid> {
        self.tasks.iter().map(|t| t.version).collect()
    }
}

/// One input after it is fixed to exact entities.
struct Fixed {
    record: Value,
    /// member versions, after the selection
    versions: Vec<Entry>,
    selector: Option<Value>,
    representation: Option<String>,
    snapshot: Option<Uuid>,
}

pub async fn resolve(sess: &Session, req: &Request) -> Result<Resolution> {
    resolve_inner(sess, req, None).await
}

/// Resolve again the exact inputs of an earlier resolution, with the access control, usage
/// policy and invalidation set of now (QY-8, UP-12).
pub async fn resolve_again(sess: &Session, id: Uuid) -> Result<Resolution> {
    let (req, orig) = {
        let mut c = sess.client().await?;
        let tx = sess.begin(&mut c).await?;
        let r = tx.qopt("select inputs, action, purpose, place, decision, request from resolution where id = $1", &[&id]).await?.ok_or(Error::NotFound)?;
        let inputs: Value = r.get(0);
        let mut req = Request { action: r.get(1), purpose: r.get(2), place: r.get(3), ..Default::default() };
        let orig_req: Value = r.get(5);
        req.place_mode = orig_req["place_mode"].as_str().map(|s| s.to_string());
        req.scope = orig_req["scope"].as_str().map(|s| s.to_string());
        for i in inputs.as_array().cloned().unwrap_or_default() {
            let mut inp = Input { role: i["role"].as_str().unwrap_or("training").into(), ..Default::default() };
            inp.selection = i.get("selection").cloned().filter(|v| !v.is_null());
            inp.selector = i.get("selector").cloned().filter(|v| !v.is_null());
            inp.representation = i["representation"].as_str().map(|s| s.to_string());
            if let Some(s) = i["snapshot"].as_str() {
                inp.snapshot = s.parse().ok();
            } else if let Some(v) = i["version"].as_str() {
                inp.version = v.parse().ok();
            } else if i["versions"].is_array() {
                inp.selection = Some(json!({"members": i["versions"]}));
                inp.collection = i["collection"].as_str().map(|s| s.to_string());
                inp.as_of = i["as_of"].as_str().and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok()).map(|t| t.with_timezone(&chrono::Utc));
            }
            req.inputs.push(inp);
        }
        (req, (inputs, r.get::<_, Value>(4)))
    };
    let mut res = resolve_inner(sess, &req, Some(id)).await?;
    // what changed since the first resolution
    let orig_excluded: BTreeSet<String> = orig.1["excluded"]["members"].as_array().cloned().unwrap_or_default().iter().filter_map(|v| v.as_str().map(|s| s.to_string())).collect();
    let now_excluded: Vec<Value> = res.decision["excluded"]["members"].as_array().cloned().unwrap_or_default();
    let newly: Vec<Value> = now_excluded.into_iter().filter(|v| !orig_excluded.contains(v.as_str().unwrap_or(""))).collect();
    res.decision["again_of"] = json!(id);
    res.decision["newly_excluded"] = json!(newly);
    res.decision["original_inputs"] = orig.0;
    Ok(res)
}

async fn fix_input(tx: &Tx<'_>, inp: &Input, notices: &mut Vec<Value>, decisions: &mut Vec<Value>) -> Result<Fixed> {
    let mut record = json!({"role": inp.role});
    let mut snapshot = None;
    let mut versions: Vec<Entry> = Vec::new();
    if let Some(v) = inp.version {
        let d = authz::require(tx, "resolution", "read", "version", v).await?;
        decisions.push(json!({"input": v, "via": d.via, "membership_age_secs": d.membership_age_secs}));
        let info = namespace::version(tx, v).await?;
        versions.push(version_entry(&info));
        record["version"] = json!(v);
    } else if let Some(path) = &inp.artifact {
        let c = match inp.as_of {
            Some(t) => Some(namespace::commit_at(tx, t).await?),
            None => None,
        };
        let info = namespace::latest_version(tx, path, c).await?;
        let d = authz::require(tx, "resolution", "read", "version", info.id).await?;
        decisions.push(json!({"input": info.id, "via": d.via, "membership_age_secs": d.membership_age_secs}));
        if info.is_delete_marker {
            return Err(Error::refused("deleted", format!("the latest version of {path} is a delete marker; older versions resolve by their version IDs"), json!({"artifact": info.artifact_id})));
        }
        versions.push(version_entry(&info));
        record["version"] = json!(info.id);
        record["artifact"] = json!(info.artifact_id);
    } else if let Some(path) = &inp.collection {
        let coll = namespace::collection(tx, path).await?;
        let d = authz::require(tx, "resolution", "read", "collection", coll.id).await?;
        decisions.push(json!({"input": coll.id, "via": d.via}));
        let c = match inp.as_of {
            Some(t) => Some(namespace::commit_at(tx, t).await?),
            None => None,
        };
        let vs = namespace::subtree_versions(tx, path, c).await?;
        record["collection"] = json!(path);
        record["as_of"] = json!(inp.as_of);
        record["versions"] = json!(vs.iter().map(|v| v.id).collect::<Vec<_>>());
        versions = vs.iter().map(version_entry).collect();
    } else {
        let snap = match (inp.snapshot, &inp.dataset) {
            (Some(s), _) => {
                datasets::check_not_expired(tx, s).await?;
                s
            }
            (None, Some(d)) => {
                let did = datasets::dataset_id(tx, d).await?;
                let r = inp.reference.clone().unwrap_or_else(|| "main".into());
                record["ref"] = json!(r);
                datasets::resolve_ref(tx, did, &r, inp.as_of).await?
            }
            _ => return Err(Error::invalid("an input names a dataset, a snapshot, an artifact, a version or a collection")),
        };
        let d = authz::require(tx, "resolution", "read", "snapshot", snap).await?;
        decisions.push(json!({"input": snap, "via": d.via, "membership_age_secs": d.membership_age_secs}));
        let info = datasets::snapshot(tx, snap).await?;
        record["snapshot"] = json!(snap);
        record["dataset"] = json!(info.dataset_id);
        record["merkle_hash"] = json!(info.merkle_hash);
        snapshot = Some(snap);
        // expand nested snapshots; annotation sets are named exactly
        let mut stack = vec![snap];
        let mut seen = HashSet::new();
        let mut sets = Vec::new();
        let mut nested = Vec::new();
        while let Some(s) = stack.pop() {
            if !seen.insert(s) {
                continue;
            }
            for e in datasets::members(tx, s).await? {
                match e.member_kind.as_str() {
                    "snapshot" => {
                        nested.push(json!({"snapshot": e.member_id, "merkle_hash": e.member_hash}));
                        stack.push(e.member_id);
                    }
                    "annotation-set" => sets.push(datasets::annotation_set_info(tx, e.member_id).await?),
                    _ => versions.push(e),
                }
            }
        }
        if !nested.is_empty() {
            record["member_snapshots"] = json!(nested);
        }
        record["annotation_sets"] = json!(sets);
        // does a newer snapshot exist on the ref?
        if let (Some(d), None) = (&inp.dataset, inp.as_of) {
            let _ = d;
        }
        let head: Option<Uuid> = tx
            .qopt("select snapshot_id from ref where dataset_id = $1 and kind = 'branch' and name = 'main'", &[&info.dataset_id])
            .await?
            .map(|r| r.get(0));
        if let Some(h) = head {
            if h != snap && inp.snapshot.is_some() {
                let hinfo = datasets::snapshot(tx, h).await?;
                if hinfo.created_commit > info.created_commit {
                    notices.push(json!({"kind": "newer-version", "message": "an input has a newer version", "input": snap, "newer": h}));
                }
            }
        }
    }
    if let Some(sel) = &inp.selection {
        record["selection"] = sel.clone();
        versions = apply_selection(tx, &versions, sel).await?;
    }
    if let Some(s) = &inp.selector {
        record["selector"] = s.clone();
    }
    if let Some(r) = &inp.representation {
        record["representation"] = json!(r);
    }
    // a member version with a newer version of its artifact
    let infos = namespace::versions(tx, &versions.iter().map(|e| e.member_id).collect::<Vec<_>>()).await?;
    let newer: Vec<Value> = {
        let aids: Vec<Uuid> = infos.iter().map(|v| v.artifact_id).collect();
        let rows = tx.q("select id, latest_version_id from artifact where id = any($1)", &[&aids]).await?;
        let latest: HashMap<Uuid, Option<Uuid>> = rows.iter().map(|r| (r.get(0), r.get(1))).collect();
        infos
            .iter()
            .filter(|v| latest.get(&v.artifact_id).copied().flatten().map(|l| l != v.id).unwrap_or(false))
            .map(|v| json!({"version": v.id, "path": v.path, "latest": latest[&v.artifact_id]}))
            .collect()
    };
    if !newer.is_empty() {
        notices.push(json!({"kind": "newer-member-versions", "message": "an input has a newer version", "members": newer}));
    }
    Ok(Fixed { record, versions, selector: inp.selector.clone(), representation: inp.representation.clone(), snapshot })
}

fn version_entry(v: &VersionInfo) -> Entry {
    Entry { member_id: v.id, member_kind: "version".into(), member_hash: v.content_hash.clone().unwrap_or_default(), source_id: v.source_id, size: v.size, element_count: v.element_count }
}

/// Apply a selection predicate to member entries (model.md, "Selection").
pub async fn apply_selection(tx: &Tx<'_>, entries: &[Entry], pred: &Value) -> Result<Vec<Entry>> {
    if let Some(name) = pred["name"].as_str() {
        let (_, p) = datasets::selection(tx, name).await?;
        return Box::pin(apply_selection(tx, entries, &p)).await;
    }
    if let Some(srcs) = pred["sources"].as_array() {
        let mut ids = HashSet::new();
        for s in srcs {
            ids.insert(policy::source_id(tx, s.as_str().unwrap_or("")).await?);
        }
        return Ok(entries.iter().filter(|e| e.source_id.map(|s| ids.contains(&s)).unwrap_or(false)).cloned().collect());
    }
    if let Some(ms) = pred["members"].as_array() {
        let ids: HashSet<String> = ms.iter().filter_map(|m| m.as_str().map(|s| s.to_string())).collect();
        return Ok(entries.iter().filter(|e| ids.contains(&e.member_id.to_string())).cloned().collect());
    }
    if let Some(ns) = pred.get("not_similar") {
        let to = ns["to"].as_str().ok_or_else(|| Error::invalid("not_similar needs 'to'"))?;
        let crit = ns["criterion"].as_str().ok_or_else(|| Error::invalid("not_similar needs 'criterion'"))?;
        let (kind, _) = crate::leakage::criterion(tx, crit, None).await?;
        let test = crate::leakage::dataset_versions(tx, to).await?;
        let ids: Vec<Uuid> = entries.iter().map(|e| e.member_id).collect();
        let related = crate::leakage::related_versions(tx, &kind, &ids, &test).await?;
        return Ok(entries.iter().filter(|e| !related.contains(&e.member_id)).cloned().collect());
    }
    if let Some(t) = pred.get("tag") {
        let (n, v) = (t["name"].as_str().unwrap_or(""), t["value"].as_str().unwrap_or(""));
        let mut out = Vec::new();
        for e in entries {
            let tags = crate::metadata::effective_tags(tx, e.member_id).await?;
            if tags.iter().any(|(a, b)| a == n && b == v) {
                out.push(e.clone());
            }
        }
        return Ok(out);
    }
    if let Some(p) = pred.get("permitted") {
        let sources: Vec<Uuid> = entries.iter().filter_map(|e| e.source_id).collect::<BTreeSet<_>>().into_iter().collect();
        let v = policy::evaluate(tx, &sources, p["action"].as_str().unwrap_or(""), p["purpose"].as_str().unwrap_or("")).await?;
        let ok: HashSet<Uuid> = v.iter().filter(|x| x.outcome == "permit").map(|x| x.source).collect();
        return Ok(entries.iter().filter(|e| e.source_id.map(|s| ok.contains(&s)).unwrap_or(false)).cloned().collect());
    }
    Err(Error::invalid(format!("unknown selection predicate {pred}")))
}

async fn resolve_inner(sess: &Session, req: &Request, again_of: Option<Uuid>) -> Result<Resolution> {
    let place = req.place.clone().or_else(|| sess.place.clone());
    let strict = req.place_mode.as_deref() == Some("strict");
    let permitted_only = req.scope.as_deref() == Some("permitted-sources-only");
    let mut c = sess.client().await?;
    let mut tx = sess.begin(&mut c).await?;
    let mut notices = Vec::new();
    let mut access = Vec::new();
    let mut fixed = Vec::new();
    for inp in &req.inputs {
        fixed.push(fix_input(&tx, inp, &mut notices, &mut access).await?);
    }

    // the invalidation set applies to every snapshot, old or new (UP-9)
    let all_ids: Vec<Uuid> = fixed.iter().flat_map(|f| f.versions.iter().map(|e| e.member_id)).collect();
    let invalid = policy::invalidated_versions(&tx, &all_ids).await?;
    let inv_set = policy::invalidation_set(&tx).await?;
    let mut excluded_members: Vec<String> = Vec::new();
    let mut excluded_invalidated: i64 = 0;
    for f in fixed.iter_mut() {
        f.versions.retain(|e| {
            if invalid.contains(&e.member_id) && !crate::mutant("no-invalidation") {
                excluded_members.push(e.member_id.to_string());
                excluded_invalidated += e.element_count;
                false
            } else {
                true
            }
        });
    }

    // usage policy over the sources of all the requested data (UP-2)
    let mut decision = json!({
        "evaluator": EVALUATOR_VERSION, "action": req.action, "purpose": req.purpose, "place": place,
        "access": access, "consistency_token": format!("builtin:{}", tx.now.timestamp_micros()),
    });
    let mut excluded_policy: i64 = 0;
    let mut by_source: BTreeMap<String, i64> = BTreeMap::new();
    let mut duties = Vec::new();
    if let (Some(action), Some(purpose)) = (&req.action, &req.purpose) {
        let mut elements_by_source: HashMap<Option<Uuid>, i64> = HashMap::new();
        for f in &fixed {
            for e in &f.versions {
                *elements_by_source.entry(e.source_id).or_default() += e.element_count;
            }
        }
        let sources: Vec<Uuid> = elements_by_source.keys().filter_map(|s| *s).collect();
        let verdicts = policy::evaluate(&tx, &sources, action, purpose).await?;
        let ok: HashSet<Uuid> = verdicts.iter().filter(|v| v.outcome == "permit").map(|v| v.source).collect();
        decision["policies"] = serde_json::to_value(&verdicts)?;
        let no_source = elements_by_source.get(&None).copied().unwrap_or(0);
        if permitted_only {
            for f in fixed.iter_mut() {
                f.versions.retain(|e| {
                    let keep = e.source_id.map(|s| ok.contains(&s)).unwrap_or(false);
                    if !keep {
                        excluded_policy += e.element_count;
                        *by_source.entry(e.source_id.map(|s| s.to_string()).unwrap_or_else(|| "none".into())).or_default() += e.element_count;
                    }
                    keep
                });
            }
        } else {
            let refused: Vec<Value> = verdicts
                .iter()
                .filter(|v| v.outcome == "refuse")
                .map(|v| json!({"source": v.source, "name": v.name, "reason": v.reason, "elements": elements_by_source.get(&Some(v.source)).copied().unwrap_or(0), "clause": v.clause, "not_after": v.not_after}))
                .collect();
            let review: Vec<Value> = verdicts
                .iter()
                .filter(|v| v.outcome == "review")
                .map(|v| json!({"source": v.source, "name": v.name, "clause": v.clause, "reason": v.reason, "elements": elements_by_source.get(&Some(v.source)).copied().unwrap_or(0)}))
                .collect();
            if !refused.is_empty() || no_source > 0 {
                let mut names: Vec<String> = refused.iter().map(|r| format!("{} ({} elements: {})", r["name"].as_str().unwrap_or(""), r["elements"], r["reason"].as_str().unwrap_or(""))).collect();
                if no_source > 0 {
                    names.push(format!("{no_source} elements with no source"));
                }
                decision["outcome"] = json!("refused");
                decision["refused_sources"] = json!(refused);
                sess.audit("resolution", &format!("{action}:{purpose}"), "resolution", false, Some("usage policy"), decision.clone()).await?;
                return Err(Error::refused("policy-refused", format!("usage policy does not permit {action} for {purpose}: {}", names.join("; ")), decision));
            }
            if !review.is_empty() {
                decision["outcome"] = json!("blocked-for-review");
                decision["review"] = json!(review);
                sess.audit("resolution", &format!("{action}:{purpose}"), "resolution", false, Some("clause needs review"), decision.clone()).await?;
                let clauses: Vec<String> = review.iter().map(|r| format!("{} of {}", r["clause"].as_str().unwrap_or(""), r["name"].as_str().unwrap_or(""))).collect();
                return Err(Error::refused("blocked-for-review", format!("the resolution needs a review of the clause {}", clauses.join(", ")), decision));
            }
        }
        let used: Vec<Uuid> = sources.iter().filter(|s| ok.contains(s)).copied().collect();
        duties = policy::duties_of_sources(&tx, &used).await?;
    }

    // jurisdiction rules apply to the location of the caller (RD-9)
    let vinfos = namespace::versions(&tx, &fixed.iter().flat_map(|f| f.versions.iter().map(|e| e.member_id)).collect::<Vec<_>>()).await?;
    let vmap: HashMap<Uuid, VersionInfo> = vinfos.into_iter().map(|v| (v.id, v)).collect();
    let colls: BTreeSet<Uuid> = vmap.values().map(|v| v.collection_id).collect();
    for c in &colls {
        for (name, value) in crate::metadata::effective_tags(&tx, *c).await? {
            let rows = tx.q("select config from tag_policy where name = $1 and value = $2 and kind = 'jurisdiction'", &[&name, &value]).await?;
            for r in rows {
                let cfg: Value = r.get(0);
                let allowed: Vec<String> = cfg["allowed_locations"].as_array().cloned().unwrap_or_default().iter().filter_map(|v| v.as_str().map(|s| s.to_string())).collect();
                let here = place.clone().unwrap_or_default();
                if !allowed.contains(&here) {
                    let d = json!({"rule": format!("{name} = {value}"), "allowed_locations": allowed, "caller_location": here});
                    sess.audit("resolution", "read", &format!("collection:{c}"), false, Some("jurisdiction"), d.clone()).await?;
                    return Err(Error::refused(
                        "jurisdiction",
                        format!("the jurisdiction rule {name} = {value} does not permit access from {here}; it permits {}", allowed.join(", ")),
                        d,
                    ));
                }
            }
        }
    }

    // replicas, and the plan
    let ids: Vec<Uuid> = vmap.keys().copied().collect();
    let replicas = namespace::replicas_of(&tx, &ids).await?;
    let mut by_version: HashMap<Uuid, Vec<ReplicaInfo>> = HashMap::new();
    for r in replicas {
        by_version.entry(r.version_id).or_default().push(r);
    }
    let reps = representation_versions(&tx, &fixed).await?;
    let rep_ids: Vec<Uuid> = reps.values().flatten().copied().collect();
    let mut rep_vinfo: HashMap<Uuid, VersionInfo> = HashMap::new();
    if !rep_ids.is_empty() {
        for v in namespace::versions(&tx, &rep_ids).await? {
            rep_vinfo.insert(v.id, v);
        }
        for r in namespace::replicas_of(&tx, &rep_ids).await? {
            by_version.entry(r.version_id).or_default().push(r);
        }
    }
    let element_keys = &inv_set.fingerprints;
    let tenant = sess.tenant_id()?;
    let mut tasks = Vec::new();
    let mut missing_at_place = 0usize;
    let mut no_replica = 0usize;
    let at_place_ok = |vid: &Uuid| -> bool {
        by_version.get(vid).map(|rs| rs.iter().any(|r| r.state == "verified" && place.as_deref() == Some(r.location.as_str()))).unwrap_or(false)
    };
    for f in &fixed {
        // a representation of the snapshot can stand for its members
        let chosen_rep: Option<Vec<Uuid>> = match (&f.representation, f.snapshot) {
            (Some(name), Some(s)) => Some(reps.get(&(s, name.clone())).cloned().ok_or_else(|| Error::refused("no-representation", format!("the snapshot has no representation {name}"), Value::Null))?),
            (None, Some(s)) if place.is_some() && !f.versions.iter().all(|e| at_place_ok(&e.member_id)) => {
                reps.iter().filter(|((sn, _), _)| *sn == s).find(|(_, vs)| vs.iter().all(at_place_ok)).map(|(_, vs)| vs.clone())
            }
            _ => None,
        };
        let members: Vec<(Uuid, i64)> = match &chosen_rep {
            Some(vs) => vs.iter().map(|v| (*v, 0)).collect(),
            None => f.versions.iter().map(|e| (e.member_id, e.element_count)).collect(),
        };
        let rep_infos = &rep_vinfo;
        for (vid, _) in members {
            let v = vmap.get(&vid).or_else(|| rep_infos.get(&vid)).ok_or(Error::NotFound)?;
            let candidates: Vec<&ReplicaInfo> = by_version
                .get(&vid)
                .map(|rs| {
                    rs.iter()
                        .filter(|r| r.state == "verified")
                        .filter(|r| r.egress_allowed || place.as_deref() == Some(r.location.as_str()))
                        .collect()
                })
                .unwrap_or_default();
            let at_place: Vec<&&ReplicaInfo> = candidates.iter().filter(|r| place.as_deref() == Some(r.location.as_str())).collect();
            let best: Option<&ReplicaInfo> = if strict {
                at_place.first().map(|r| **r)
            } else {
                at_place.first().map(|r| **r).or_else(|| candidates.first().copied())
            };
            let Some(rep) = best else {
                if strict && !candidates.is_empty() {
                    missing_at_place += 1;
                } else {
                    no_replica += 1;
                }
                continue;
            };
            tasks.extend(plan_version(&tx, tenant, v, rep, f.selector.as_ref(), element_keys)?);
        }
    }
    if missing_at_place > 0 {
        let d = json!({"not_available": missing_at_place, "place": place});
        sess.audit("resolution", "read", "resolution", false, Some("not available at the place"), d.clone()).await?;
        return Err(Error::refused(
            "not-available-there",
            format!("{missing_at_place} members have no verified replica at {}; mudcat issues no plan", place.clone().unwrap_or_default()),
            d,
        ));
    }
    if no_replica > 0 {
        let d = json!({"no_replica": no_replica});
        sess.audit("resolution", "read", "resolution", false, Some("no replica"), d.clone()).await?;
        return Err(Error::refused(
            "no-replica",
            format!("no replica is available for {no_replica} members; the catalog records still exist"),
            d,
        ));
    }

    let total_selected: usize = fixed.iter().map(|f| f.versions.len()).sum();
    decision["outcome"] = json!(if total_selected == 0 && !excluded_members.is_empty() { "nothing-to-read" } else { "permitted" });
    decision["excluded"] = json!({
        "invalidated_elements": excluded_invalidated, "members": excluded_members,
        "policy_elements": excluded_policy, "policy_by_source": by_source,
        "total_elements": excluded_invalidated + excluded_policy,
    });
    decision["duties"] = json!(duties);

    // credentials (RD-3)
    let mode_before = req.mode.as_deref() == Some("before-submission");
    let credentials = if tasks.is_empty() { vec![] } else { issue_credentials(&tx, &tasks).await? };
    let run_token = format!("rt-{}", hex::encode(rand::random::<[u8; 12]>()));
    let commit = tx.commit_id().await?;
    let id = new_id();
    let inputs: Vec<Value> = fixed.iter().map(|f| f.record.clone()).collect();
    let req_v = serde_json::to_value(req)?;
    tx.exec(
        "insert into resolution (id, principal, request, inputs, action, purpose, place, decision, run_token, created_commit, at) values ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)",
        &[&id, &sess.principal.0, &req_v, &json!(inputs), &req.action, &req.purpose, &place, &decision, &run_token, &commit, &tx.now],
    )
    .await?;
    for cred in &credentials {
        let kind = match cred.credential {
            Credential::Prefix { .. } => "prefix",
            Credential::SignedUrl { .. } => "signed-url",
            Credential::Site => "none",
        };
        tx.exec(
            "insert into credential_log (id, resolution_id, run_token, principal, kind, scope, issued_at) values ($1,$2,$3,$4,$5,$6,$7)",
            &[&new_id(), &id, &run_token, &sess.principal.0, &kind, &serde_json::to_value(&cred.credential)?, &tx.now],
        )
        .await?;
    }
    tx.emit("resolution.created", id, "resolution", None, json!({"inputs": inputs, "action": req.action, "purpose": req.purpose, "again_of": again_of})).await?;
    let manifest_file = if mode_before {
        let rows: Vec<Value> = tasks.iter().map(|t| json!({"path": t.url.trim_start_matches("file://"), "version": t.version, "bytes": t.expected_bytes, "elements": t.expected_elements})).collect();
        Some(tx.inst().manifests.write(tenant, id, &rows)?)
    } else {
        None
    };
    tx.commit().await?;
    sess.audit("resolution", req.action.as_deref().unwrap_or("read"), &format!("resolution:{id}"), true, Some("permitted"), json!({"tasks": tasks.len()})).await?;
    if let Some(m) = &manifest_file {
        notices.push(json!({"kind": "site-enforces-access", "message": "the plan is a manifest file with POSIX paths; mudcat issues no storage credentials, and the site enforces access", "manifest": m}));
    }
    Ok(Resolution { id, inputs, decision, tasks, credentials, run_token, notices, manifest_file })
}

/// The versions of each representation of the snapshots of the inputs: (snapshot, name) -> versions.
async fn representation_versions(tx: &Tx<'_>, fixed: &[Fixed]) -> Result<HashMap<(Uuid, String), Vec<Uuid>>> {
    let snaps: Vec<Uuid> = fixed.iter().filter_map(|f| f.snapshot).collect();
    let rows = tx.q("select snapshot_id, name, members from representation where snapshot_id = any($1)", &[&snaps]).await?;
    Ok(rows.iter().map(|r| ((r.get(0), r.get(1)), r.get(2))).collect())
}

/// Plan the read tasks of one version, by its structure kind. mudcat lists elements only where the
/// data lists them; arrays and streams are addressed by selectors (ST-5).
fn plan_version(tx: &Tx<'_>, tenant: Uuid, v: &VersionInfo, rep: &ReplicaInfo, selector: Option<&Value>, invalid_keys: &BTreeSet<String>) -> Result<Vec<Task>> {
    let kind = v.structure["kind"].as_str().unwrap_or("opaque");
    let base = |addressing: &str, selector: Option<Value>, elements: Option<Vec<String>>, bytes: i64, n: i64| Task {
        version: v.id,
        path: v.path.clone(),
        url: rep.url.clone(),
        replica: rep.id,
        resource: rep.resource.clone(),
        location: rep.location.clone(),
        addressing: addressing.into(),
        selector,
        elements,
        expected_bytes: bytes,
        expected_elements: n,
        source: v.source_id,
    };
    match kind {
        "records" | "container" | "graph" => {
            let mut ids = Vec::new();
            let mut bytes = 0i64;
            if let Some(m) = v.element_manifest {
                for e in tx.inst().manifests.read(tenant, m)? {
                    let bad = ["checksum", "fingerprint"].iter().any(|k| e[*k].as_str().map(|s| invalid_keys.contains(s)).unwrap_or(false));
                    if bad {
                        continue;
                    }
                    bytes += e["size"].as_i64().unwrap_or(0);
                    ids.push(e["id"].as_str().map(|s| s.to_string()).unwrap_or_else(|| e["id"].to_string()));
                }
            }
            let n = ids.len() as i64;
            let bytes = if bytes == 0 { v.size } else { bytes };
            Ok(vec![base("element-list", None, Some(ids), bytes, n)])
        }
        "stream" => {
            let duration = v.structure["duration_s"].as_f64().or_else(|| v.stats["duration_s"].as_f64()).unwrap_or(0.0);
            let rate = v.structure["frame_rate"].as_f64().unwrap_or(1.0);
            let (a, b) = match selector.and_then(|s| s["t"].as_array().cloned()) {
                Some(t) => (t[0].as_f64().unwrap_or(0.0).max(0.0), t[1].as_f64().unwrap_or(duration).min(duration)),
                None => (0.0, duration),
            };
            if b <= a && duration > 0.0 {
                return Ok(vec![]);
            }
            let frac = if duration > 0.0 { (b - a) / duration } else { 1.0 };
            let channel = v.structure["channels"].as_array().and_then(|c| c.first().cloned()).and_then(|c| c["name"].as_str().map(|s| s.to_string()));
            let sel = json!({"type": "media-fragment", "t": [a, b], "fragment": format!("#t={a},{b}"), "channel": channel});
            Ok(vec![base("selector", Some(sel), None, (v.size as f64 * frac).round() as i64, ((b - a) * rate).round() as i64)])
        }
        "array" => {
            // chunks along the first dimension that overlap the selector
            let dims = v.structure["dimensions"].as_array().cloned().unwrap_or_default();
            let Some(d0) = dims.first() else { return Ok(vec![base("selector", Some(json!({"type": "hyperslab"})), None, v.size, v.element_count)]) };
            let name = d0["name"].as_str().unwrap_or("dim0").to_string();
            let start = d0["start"].as_f64().unwrap_or(0.0);
            let size = d0["size"].as_f64().unwrap_or(1.0);
            let chunk = v.structure["chunk"].as_array().and_then(|c| c.first()).and_then(|c| c.as_f64()).unwrap_or(size);
            let (lo, hi) = match selector.and_then(|s| s[&name].as_array().cloned()) {
                Some(r) => (r[0].as_f64().unwrap_or(start), r[1].as_f64().unwrap_or(start + size - 1.0)),
                None => (start, start + size - 1.0),
            };
            let per_index = v.element_count as f64 / size;
            let bytes_per_index = v.size as f64 / size;
            let mut out = Vec::new();
            let mut c0 = start;
            while c0 < start + size {
                let c1 = (c0 + chunk - 1.0).min(start + size - 1.0);
                let (a, b) = (c0.max(lo), c1.min(hi));
                if a <= b {
                    let n = b - a + 1.0;
                    let sel = json!({"type": "hyperslab", name.clone(): [a, b], "chunk": [c0, c1]});
                    out.push(base("selector", Some(sel), None, (bytes_per_index * n).round() as i64, (per_index * n).round() as i64));
                }
                c0 += chunk;
            }
            Ok(out)
        }
        "composite" => Ok(vec![base("selector", Some(json!({"type": "component", "components": v.structure["components"]})), None, v.size, v.element_count)]),
        _ => Ok(vec![base("object", None, None, v.size, 1)]),
    }
}

/// Issue credentials for the tasks of a plan. A prefix credential only if the prefix is entirely
/// inside one ACL root that the caller can read; otherwise signed URLs per object (RD-3).
async fn issue_credentials(tx: &Tx<'_>, tasks: &[Task]) -> Result<Vec<IssuedCredential>> {
    let expires = (tx.now + chrono::Duration::hours(12)).timestamp();
    let store = &tx.inst().store;
    let mut by_res: BTreeMap<String, Vec<&Task>> = BTreeMap::new();
    for t in tasks {
        by_res.entry(t.resource.clone()).or_default().push(t);
    }
    let mut out = Vec::new();
    for (res_name, ts) in by_res {
        let res = namespace::resource(tx, &res_name).await?;
        if res.protocol == "posix" && res.mode == "external" {
            out.push(IssuedCredential { resource: res_name, credential: Credential::Site, covers: ts.len() });
            continue;
        }
        // the ACL roots of the versions in this resource
        let vids: Vec<Uuid> = ts.iter().map(|t| t.version).collect();
        let roots: BTreeSet<Uuid> = namespace::versions(tx, &vids).await?.iter().map(|v| v.acl_root).collect();
        let mut prefix = None;
        if roots.len() == 1 {
            let root = *roots.iter().next().unwrap();
            let rc = namespace::collection_by_id(tx, root).await?;
            if let (Some(rid), Some(p)) = (rc.resource_id, rc.prefix.clone()) {
                let all_in = ts.iter().all(|t| parse_url(&t.url).map(|(b, k)| b == res.bucket && k.starts_with(&p)).unwrap_or(false));
                // no collection with another ACL root may have its data under the same prefix
                let like = format!("{}%", p);
                let other = tx
                    .qopt("select 1 from collection where resource_id = $1 and prefix like $2 and acl_root_id <> $3 limit 1", &[&rid, &like, &root])
                    .await?;
                if rid == res.id && all_in && other.is_none() && !p.is_empty() {
                    prefix = Some(p);
                }
            }
        }
        match prefix {
            Some(p) => out.push(IssuedCredential { resource: res_name, credential: store.prefix_credential(&res.bucket, if crate::mutant("wide-prefix") { "" } else { &p }, expires), covers: ts.len() }),
            None => {
                for t in ts {
                    out.push(IssuedCredential { resource: res_name.clone(), credential: store.signed_url(&t.url, expires), covers: 1 });
                }
            }
        }
    }
    Ok(out)
}

/// The credential of a resolution that covers a URL.
pub fn credential_for<'a>(res: &'a Resolution, url: &str) -> Option<&'a Credential> {
    let (b, k) = parse_url(url).ok()?;
    res.credentials.iter().map(|c| &c.credential).find(|c| match c {
        Credential::Prefix { bucket, prefix, .. } => *bucket == b && k.starts_with(prefix.as_str()),
        Credential::SignedUrl { url: u, .. } => u == url,
        Credential::Site => url.starts_with("file:///"),
    })
}
