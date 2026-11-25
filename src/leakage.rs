//! Similarity criteria and leakage assertions (model.md, section 7; QY-3).
//!
//! mudcat computes each criterion from records, not from data: exact checksums and fingerprints
//! from the content-identity index, domain keys from the same index, and time ranges from
//! selectors and statistics. A check reads no data bytes.

use crate::error::{Error, Result};
use crate::instance::{new_id, Tx};
use serde_json::{json, Value};
use std::collections::{BTreeSet, HashSet};
use uuid::Uuid;

pub const KINDS: &[&str] = &["exact-checksum", "near-duplicate", "domain-similarity", "time-range-overlap"];

pub async fn register_criterion(tx: &mut Tx<'_>, name: &str, kind: &str, params: Value) -> Result<i32> {
    if !KINDS.contains(&kind) {
        return Err(Error::invalid(format!("unknown criterion kind {kind}")));
    }
    let v: i32 = tx.q1("select coalesce(max(version), 0) + 1 from similarity_criterion where name = $1", &[&name]).await?.get(0);
    tx.exec("insert into similarity_criterion (name, version, kind, params) values ($1,$2,$3,$4)", &[&name, &v, &kind, &params]).await?;
    tx.emit("similarity-criterion.registered", crate::authz::uuid_for(&format!("criterion:{name}:{v}")), "similarity-criterion", None, json!({"name": name, "version": v, "kind": kind}))
        .await?;
    Ok(v)
}

/// The kind and version of a criterion (the newest version by default).
pub async fn criterion(tx: &Tx<'_>, name: &str, version: Option<i32>) -> Result<(String, i32)> {
    let r = match version {
        Some(v) => tx.qopt("select kind, version from similarity_criterion where name = $1 and version = $2", &[&name, &v]).await?,
        None => tx.qopt("select kind, version from similarity_criterion where name = $1 order by version desc limit 1", &[&name]).await?,
    };
    let r = r.ok_or_else(|| Error::invalid(format!("unknown similarity criterion {name}")))?;
    Ok((r.get(0), r.get(1)))
}

/// The member versions of the head of the main branch of a dataset.
pub async fn dataset_versions(tx: &Tx<'_>, dataset: &str) -> Result<Vec<Uuid>> {
    let did = crate::datasets::dataset_id(tx, dataset).await?;
    let snap = crate::datasets::resolve_ref(tx, did, "main", None).await?;
    Ok(crate::datasets::members(tx, snap).await?.iter().filter(|e| e.member_kind == "version").map(|e| e.member_id).collect())
}

fn key_kind(kind: &str) -> &'static str {
    match kind {
        "exact-checksum" => "checksum",
        "near-duplicate" => "fingerprint",
        _ => "domain-key",
    }
}

/// The versions among `candidates` that are related to some version of `other` (version level).
pub async fn related_versions(tx: &Tx<'_>, kind: &str, candidates: &[Uuid], other: &[Uuid]) -> Result<HashSet<Uuid>> {
    if kind == "time-range-overlap" {
        let a = ranges(tx, candidates).await?;
        let b = ranges(tx, other).await?;
        return Ok(a.iter().filter(|(_, r)| b.iter().any(|(_, s)| overlap(*r, *s))).map(|(v, _)| *v).collect());
    }
    let rows = tx
        .q(
            "select distinct a.version_id from identity_index a join identity_index b on b.key_kind = a.key_kind and b.key = a.key and b.tenant_id = a.tenant_id
              where a.key_kind = $1 and a.element_id = '' and b.element_id = '' and a.version_id = any($2) and b.version_id = any($3) and a.version_id <> b.version_id",
            &[&key_kind(kind), &candidates, &other],
        )
        .await?;
    Ok(rows.iter().map(|r| r.get(0)).collect())
}

async fn ranges(tx: &Tx<'_>, ids: &[Uuid]) -> Result<Vec<(Uuid, (f64, f64))>> {
    let rows = tx.q("select id, stats from version where id = any($1)", &[&ids]).await?;
    Ok(rows
        .iter()
        .filter_map(|r| {
            let s: Value = r.get(1);
            let t = s["time"].as_array()?;
            Some((r.get(0), (t[0].as_f64()?, t[1].as_f64()?)))
        })
        .collect())
}

fn overlap(a: (f64, f64), b: (f64, f64)) -> bool {
    a.0 <= b.1 && b.0 <= a.1
}

fn range_of(sel: &Value) -> Option<(f64, f64)> {
    let t = sel["time"].as_array().or_else(|| sel["t"].as_array())?;
    Some((t[0].as_f64()?, t[1].as_f64()?))
}

/// The training side of a check: the versions and the selectors of the training inputs.
pub struct TrainSide {
    pub versions: Vec<Uuid>,
    pub selectors: Vec<(f64, f64)>,
    pub description: Value,
}

pub async fn train_side(tx: &Tx<'_>, spec: &Value) -> Result<TrainSide> {
    if let Some(m) = spec["model"].as_str().and_then(|s| s.parse::<Uuid>().ok()) {
        let inputs = crate::provenance::training_inputs(tx, m).await?;
        let mut versions = BTreeSet::new();
        let mut selectors = Vec::new();
        for i in &inputs {
            versions.extend(i.versions.iter().copied());
            if let Some(r) = i.selector.as_ref().and_then(range_of) {
                selectors.push(r);
            }
        }
        return Ok(TrainSide { versions: versions.into_iter().collect(), selectors, description: json!({"model": m, "resolutions": inputs.iter().filter_map(|i| i.resolution).collect::<BTreeSet<_>>()}) });
    }
    if let Some(sel) = spec["selection"].as_str() {
        let (id, pred) = crate::datasets::selection(tx, sel).await?;
        let did: Uuid = tx.q1("select dataset_id from selection where id = $1", &[&id]).await?.get(0);
        let snap = crate::datasets::resolve_ref(tx, did, "main", None).await?;
        let entries = crate::datasets::members(tx, snap).await?;
        let chosen = crate::resolver::apply_selection(tx, &entries, &pred).await?;
        return Ok(TrainSide { versions: chosen.iter().map(|e| e.member_id).collect(), selectors: vec![], description: json!({"selection": sel, "snapshot": snap}) });
    }
    if let Some(d) = spec["dataset"].as_str() {
        let versions = dataset_versions(tx, d).await?;
        let selectors = spec.get("selector").and_then(range_of).into_iter().collect();
        return Ok(TrainSide { versions, selectors, description: json!({"dataset": d, "selector": spec.get("selector")}) });
    }
    Err(Error::invalid("the training side names a model, a selection or a dataset"))
}

/// Check if test data is related to training data under a criterion, and record a leakage
/// assertion with the criterion, its version and the evidence (QY-3).
pub async fn check(tx: &mut Tx<'_>, train: &Value, test: &Value, criterion_name: &str) -> Result<Value> {
    let (kind, version) = criterion(tx, criterion_name, None).await?;
    let before = tx.inst().metrics.bytes_read_for_catalog.load(std::sync::atomic::Ordering::SeqCst);
    let t = train_side(tx, train).await?;
    let test_dataset = test["dataset"].as_str().ok_or_else(|| Error::invalid("the test side names a dataset"))?;
    let mut evidence = json!({"criterion": criterion_name, "version": version, "kind": kind, "train": t.description, "test": test});
    let related: i64;
    let mut sample = Vec::new();
    if kind == "time-range-overlap" && test.get("selector").is_some() {
        // from the two selectors alone
        let r = range_of(&test["selector"]).ok_or_else(|| Error::invalid("the test selector has no time range"))?;
        let overlaps: Vec<(f64, f64)> = t.selectors.iter().filter(|s| overlap(**s, r)).map(|s| (s.0.max(r.0), s.1.min(r.1))).collect();
        related = overlaps.len() as i64;
        evidence["method"] = json!("selectors");
        evidence["overlap"] = json!(overlaps.iter().map(|o| json!([o.0, o.1])).collect::<Vec<_>>());
    } else {
        let test_versions = dataset_versions(tx, test_dataset).await?;
        let rel = if kind == "time-range-overlap" {
            let tr = ranges(tx, &test_versions).await?;
            let mut train_ranges = ranges(tx, &t.versions).await?.into_iter().map(|x| x.1).collect::<Vec<_>>();
            train_ranges.extend(t.selectors.iter().copied());
            evidence["method"] = json!("selectors and statistics");
            tr.iter().filter(|(_, r)| train_ranges.iter().any(|s| overlap(*r, *s))).map(|(v, _)| *v).collect::<HashSet<_>>()
        } else {
            evidence["method"] = json!(format!("content-identity index ({})", key_kind(&kind)));
            related_versions(tx, &kind, &test_versions, &t.versions).await?
        };
        related = rel.len() as i64;
        let mut s: Vec<Uuid> = rel.into_iter().collect();
        s.sort();
        sample = s.into_iter().take(20).collect();
        evidence["related_sample"] = json!(sample);
    }
    let after = tx.inst().metrics.bytes_read_for_catalog.load(std::sync::atomic::Ordering::SeqCst);
    evidence["bytes_scanned"] = json!(after - before);
    let result = if related == 0 { "satisfied" } else { "violated" };
    let id = new_id();
    tx.exec(
        "insert into leakage_assertion (id, criterion, criterion_version, train, test, result, related_count, evidence, at) values ($1,$2,$3,$4,$5,$6,$7,$8,$9)",
        &[&id, &criterion_name, &version, train, test, &result, &related, &evidence, &tx.now],
    )
    .await?;
    tx.emit("leakage-assertion.recorded", id, "leakage-assertion", None, json!({"result": result, "related": related, "criterion": criterion_name, "version": version})).await?;
    let _ = sample;
    Ok(json!({"assertion": id, "result": result, "related": related, "criterion": criterion_name, "criterion_version": version, "evidence": evidence}))
}

/// Evaluate a model on an evaluation dataset. Before mudcat accepts the result, it checks the
/// similarity criteria of the evaluation dataset against the training inputs of the model.
pub async fn evaluate(tx: &mut Tx<'_>, model: Uuid, dataset: &str, metrics: Value) -> Result<Value> {
    let did = crate::datasets::dataset_id(tx, dataset).await?;
    let crits: Vec<String> = tx
        .q("select payload -> 'leakage_criteria' from metadata_payload where entity_id = $1 and payload ? 'leakage_criteria'", &[&did])
        .await?
        .iter()
        .flat_map(|r| r.get::<_, Value>(0).as_array().cloned().unwrap_or_default())
        .filter_map(|v| v.as_str().map(|s| s.to_string()))
        .collect();
    let mut assertions = Vec::new();
    for c in &crits {
        assertions.push(check(tx, &json!({"model": model}), &json!({"dataset": dataset}), c).await?);
    }
    let act = new_id();
    tx.exec("insert into activity (id, kind, started_at, ended_at, params) values ($1,'evaluation',$2,$2,$3)", &[&act, &tx.now, &json!({"metrics": metrics})]).await?;
    tx.exec("insert into usage (activity_id, entity_id, role, capture) values ($1,$2,'test','declared'), ($1,$3,'reference','declared')", &[&act, &did, &model]).await?;
    let result = new_id();
    tx.exec("insert into generation (activity_id, entity_id) values ($1,$2)", &[&act, &result]).await?;
    let violated: Vec<&Value> = assertions.iter().filter(|a| a["result"] == "violated").collect();
    let record = json!({"evaluation_result": result, "activity": act, "model": model, "dataset": dataset, "metrics": metrics,
                        "assertions": assertions, "leakage": if violated.is_empty() { "satisfied" } else { "violated" }});
    tx.emit("evaluation-result.recorded", result, "evaluation-result", None, record.clone()).await?;
    let commit = tx.commit_id().await?;
    let who = tx.principal().0.clone();
    tx.exec(
        "insert into metadata_payload (entity_id, namespace, payload, origin, asserted_by, commit_id) values ($1, 'evaluation', $2, 'person', $3, $4)",
        &[&result, &record, &who, &commit],
    )
    .await?;
    Ok(record)
}
