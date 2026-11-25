//! Usage policy: sources, usage grants, constraints and duties, terms of use, invalidation,
//! purge, erasure and legal holds (model.md, section 5). mudcat always evaluates usage policy
//! itself, with set operations over the sources of the requested data.

use crate::error::{Error, Result};
use crate::instance::{new_id, Tx};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use uuid::Uuid;

pub const ACTIONS: &[&str] = &["use", "train", "evaluate", "fineTune", "distribute", "publishDerivative", "retrieve"];

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct SourceSpec {
    pub name: String,
    /// crawl | licensed | human | user | generated | instrument | computed | external
    pub kind: String,
    #[serde(default)]
    pub attrs: Value,
    #[serde(default)]
    pub method: Option<Value>,
}

pub async fn create_source(tx: &mut Tx<'_>, s: &SourceSpec) -> Result<Uuid> {
    if let Some(r) = tx.qopt("select id from source where name = $1", &[&s.name]).await? {
        return Ok(r.get(0));
    }
    let commit = tx.commit_id().await?;
    let id = new_id();
    let attrs = if s.attrs.is_null() { json!({}) } else { s.attrs.clone() };
    let mh = s.method.as_ref().map(crate::datasets::method_hash);
    tx.exec(
        "insert into source (id, name, kind, attrs, method, method_hash, created_commit) values ($1,$2,$3,$4,$5,$6,$7)",
        &[&id, &s.name, &s.kind, &attrs, &s.method, &mh, &commit],
    )
    .await?;
    crate::namespace::set_identifier(tx, "source", &s.name, id).await?;
    tx.emit("source.changed", id, "source", None, json!({"name": s.name, "kind": s.kind})).await?;
    Ok(id)
}

pub async fn source_id(tx: &Tx<'_>, name_or_id: &str) -> Result<Uuid> {
    if let Ok(id) = name_or_id.parse::<Uuid>() {
        return Ok(id);
    }
    Ok(tx.qopt("select id from source where name = $1", &[&name_or_id]).await?.ok_or(Error::NotFound)?.get(0))
}

pub async fn grant_usage(tx: &mut Tx<'_>, source: Uuid, action: &str, purpose: &str, not_after: Option<chrono::DateTime<chrono::Utc>>, clause: Option<&str>, terms: Option<Uuid>) -> Result<Uuid> {
    let commit = tx.commit_id().await?;
    let id = new_id();
    tx.exec(
        "insert into usage_grant (id, source_id, action, purpose, not_after, clause, terms_id, granted_by, created_commit) values ($1,$2,$3,$4,$5,$6,$7,$8,$9)",
        &[&id, &source, &action, &purpose, &not_after, &clause, &terms, &tx.principal().0, &commit],
    )
    .await?;
    tx.emit("usage-grant.changed", source, "source", None, json!({"grant": id, "action": action, "purpose": purpose, "not_after": not_after, "clause": clause}))
        .await?;
    Ok(id)
}

pub async fn add_duty(tx: &mut Tx<'_>, source: Uuid, kind: &str, clause: Option<&str>, terms: Option<Uuid>, params: Value) -> Result<Uuid> {
    let id = new_id();
    tx.exec("insert into duty (id, source_id, kind, clause, terms_id, params) values ($1,$2,$3,$4,$5,$6)", &[&id, &source, &kind, &clause, &terms, &params])
        .await?;
    tx.emit("duty.changed", source, "source", None, json!({"duty": kind, "clause": clause})).await?;
    Ok(id)
}

// ---------------------------------------------------------------------------- terms compiler

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "format", rename_all = "lowercase")]
pub enum Terms {
    /// A standard licence identifier.
    Spdx { id: String },
    /// An ODRL 2.2 policy document.
    Odrl { document: Value },
    /// A contract in plain text; a person records the grants and duties.
    Text { text: String, #[serde(default)] grants: Vec<Value>, #[serde(default)] duties: Vec<String> },
}

/// The pre-compiled grants and duties of standard licences: (actions, purposes, duties).
fn spdx_map(id: &str) -> Option<(Vec<&'static str>, Vec<&'static str>, Vec<&'static str>)> {
    let all_actions = vec!["use", "train", "evaluate", "fineTune", "distribute", "publishDerivative", "retrieve"];
    match id {
        "CC-BY-4.0" => Some((all_actions, vec!["any"], vec!["attribute"])),
        "CC-BY-SA-4.0" => Some((all_actions, vec!["any"], vec!["attribute", "share-alike"])),
        "CC0-1.0" | "Apache-2.0" | "MIT" => Some((all_actions, vec!["any"], if id == "CC0-1.0" { vec![] } else { vec!["attribute"] })),
        "CC-BY-NC-4.0" => Some((all_actions, vec!["research", "education"], vec!["attribute"])),
        "CC-BY-ND-4.0" => Some((vec!["use", "evaluate", "distribute", "retrieve"], vec!["any"], vec!["attribute", "no-derivatives"])),
        _ => None,
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Compiled {
    pub terms_id: Uuid,
    pub status: String,
    pub grants: Vec<Uuid>,
    pub duties: Vec<String>,
    pub review: Vec<Value>,
}

/// Keep the terms document of a source and compile it into grants, constraints and duties. Each
/// compiled record links to its clause (UP-5). Clauses that the compiler cannot translate go to
/// the review queue, and the source becomes partly compiled (UP-6).
pub async fn set_terms(tx: &mut Tx<'_>, source: Uuid, terms: &Terms) -> Result<Compiled> {
    let commit = tx.commit_id().await?;
    let tid = new_id();
    let (format, content) = match terms {
        Terms::Spdx { id } => ("spdx", json!({"id": id})),
        Terms::Odrl { document } => ("odrl", document.clone()),
        Terms::Text { text, grants, duties } => ("text", json!({"text": text, "grants": grants, "duties": duties})),
    };
    tx.exec("insert into terms_document (id, source_id, format, content, commit_id) values ($1,$2,$3,$4,$5)", &[&tid, &source, &format, &content, &commit])
        .await?;
    let mut grants = Vec::new();
    let mut duties = Vec::new();
    let mut review = Vec::new();
    match terms {
        Terms::Spdx { id } => {
            let (actions, purposes, ds) = spdx_map(id).ok_or_else(|| Error::invalid(format!("no stored mapping for the licence {id}")))?;
            let clause = format!("spdx:{id}");
            for a in &actions {
                for p in &purposes {
                    grants.push(grant_usage(tx, source, a, p, None, Some(&clause), Some(tid)).await?);
                }
            }
            for d in ds {
                add_duty(tx, source, d, Some(&clause), Some(tid), json!({})).await?;
                duties.push(d.to_string());
            }
        }
        Terms::Odrl { document } => {
            for (i, perm) in document["permission"].as_array().cloned().unwrap_or_default().iter().enumerate() {
                let clause = perm["uid"].as_str().map(|s| s.to_string()).unwrap_or_else(|| format!("permission[{i}]"));
                let action = perm["action"].as_str().unwrap_or("use").trim_start_matches("odrl:").trim_start_matches("mc:").to_string();
                let mut purposes = Vec::new();
                let mut not_after = None;
                let mut untranslated = Vec::new();
                for c in perm["constraint"].as_array().cloned().unwrap_or_default() {
                    let left = c["leftOperand"].as_str().unwrap_or("").trim_start_matches("odrl:");
                    let op = c["operator"].as_str().unwrap_or("").trim_start_matches("odrl:");
                    match (left, op) {
                        ("purpose", "eq") => purposes.push(c["rightOperand"].as_str().unwrap_or("").to_string()),
                        ("purpose", "isAnyOf") => {
                            for p in c["rightOperand"].as_array().cloned().unwrap_or_default() {
                                purposes.push(p.as_str().unwrap_or("").to_string());
                            }
                        }
                        ("dateTime", "lt") | ("dateTime", "lteq") => {
                            not_after = c["rightOperand"].as_str().and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok()).map(|t| t.with_timezone(&chrono::Utc));
                        }
                        _ => untranslated.push(c.clone()),
                    }
                }
                if !ACTIONS.contains(&action.as_str()) {
                    untranslated.push(json!({"action": action}));
                }
                if !untranslated.is_empty() {
                    let id = new_id();
                    let details = json!({"permission": perm, "untranslated": untranslated});
                    tx.exec(
                        "insert into review_item (id, kind, target_id, clause, details, created_commit) values ($1,'clause',$2,$3,$4,$5)",
                        &[&id, &source, &clause, &details, &commit],
                    )
                    .await?;
                    review.push(json!({"review_item": id, "clause": clause, "untranslated": untranslated}));
                    continue;
                }
                if purposes.is_empty() {
                    purposes.push("any".into());
                }
                for p in &purposes {
                    grants.push(grant_usage(tx, source, &action, p, not_after, Some(&clause), Some(tid)).await?);
                }
                for d in perm["duty"].as_array().cloned().unwrap_or_default() {
                    let kind = d["action"].as_str().unwrap_or("duty").trim_start_matches("odrl:").to_string();
                    add_duty(tx, source, &kind, Some(&clause), Some(tid), d.clone()).await?;
                    duties.push(kind);
                }
            }
            for (i, d) in document["obligation"].as_array().cloned().unwrap_or_default().iter().enumerate() {
                let kind = d["action"].as_str().unwrap_or("duty").trim_start_matches("odrl:").to_string();
                add_duty(tx, source, &kind, Some(&format!("obligation[{i}]")), Some(tid), d.clone()).await?;
                duties.push(kind);
            }
        }
        Terms::Text { grants: gs, duties: ds, .. } => {
            for g in gs {
                let not_after = g["not_after"].as_str().and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok()).map(|t| t.with_timezone(&chrono::Utc));
                let clause = g["clause"].as_str().unwrap_or("recorded by a person");
                grants.push(grant_usage(tx, source, g["action"].as_str().unwrap_or("use"), g["purpose"].as_str().unwrap_or("any"), not_after, Some(clause), Some(tid)).await?);
            }
            for d in ds {
                add_duty(tx, source, d, Some("recorded by a person"), Some(tid), json!({})).await?;
                duties.push(d.clone());
            }
        }
    }
    let status = if review.is_empty() { "compiled" } else { "partly-compiled" };
    tx.exec("update source set terms_status = $2 where id = $1", &[&source, &status]).await?;
    tx.emit("source.terms-changed", source, "source", None, json!({"terms": tid, "format": format, "status": status, "review": review})).await?;
    Ok(Compiled { terms_id: tid, status: status.into(), grants, duties, review })
}

/// Decide a clause in the review queue. A decision that permits gives a grant that links to the clause.
pub async fn review_clause(tx: &mut Tx<'_>, item: Uuid, decision: &Value) -> Result<Vec<Uuid>> {
    let r = tx
        .qopt("select target_id, clause, status from review_item where id = $1 and kind = 'clause'", &[&item])
        .await?
        .ok_or(Error::NotFound)?;
    let source: Uuid = r.get(0);
    let clause: Option<String> = r.get(1);
    let mut grants = Vec::new();
    for g in decision["grants"].as_array().cloned().unwrap_or_default() {
        let not_after = g["not_after"].as_str().and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok()).map(|t| t.with_timezone(&chrono::Utc));
        let c = format!("{} (reviewed by {})", clause.clone().unwrap_or_default(), tx.principal());
        grants.push(grant_usage(tx, source, g["action"].as_str().unwrap_or("use"), g["purpose"].as_str().unwrap_or("any"), not_after, Some(&c), None).await?);
    }
    tx.exec(
        "update review_item set status = 'decided', decision = $2, reviewer = $3, decided_at = $4 where id = $1",
        &[&item, decision, &tx.principal().0, &tx.now],
    )
    .await?;
    let open = tx.qopt("select 1 from review_item where target_id = $1 and kind = 'clause' and status = 'open'", &[&source]).await?;
    if open.is_none() {
        tx.exec("update source set terms_status = 'compiled' where id = $1", &[&source]).await?;
    }
    tx.emit("source.terms-reviewed", source, "source", None, json!({"clause": clause, "decision": decision, "reviewer": tx.principal().0})).await?;
    Ok(grants)
}

// ---------------------------------------------------------------------------- evaluation

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SourceVerdict {
    pub source: Uuid,
    pub name: String,
    pub kind: String,
    /// permit | refuse | review
    pub outcome: String,
    pub reason: String,
    pub grant: Option<Uuid>,
    pub clause: Option<String>,
    pub not_after: Option<chrono::DateTime<chrono::Utc>>,
}

/// Evaluate usage policy for an action and a purpose over a set of sources, with one query.
pub async fn evaluate(tx: &Tx<'_>, sources: &[Uuid], action: &str, purpose: &str) -> Result<Vec<SourceVerdict>> {
    let rows = tx
        .q(
            "select s.id, s.name, s.kind, s.terms_status,
                    (select json_agg(json_build_object('id', g.id, 'not_after', g.not_after, 'clause', g.clause) order by g.not_after desc nulls first)
                       from usage_grant g where g.source_id = s.id and g.action = $2 and (g.purpose = $3 or g.purpose = 'any') and g.revoked_commit is null) as grants,
                    (select string_agg(r.clause, ', ') from review_item r where r.target_id = s.id and r.kind = 'clause' and r.status = 'open') as open_clauses
               from source s where s.id = any($1)",
            &[&sources, &action, &purpose],
        )
        .await?;
    let mut out = Vec::new();
    for r in rows {
        let grants: Option<Value> = r.get(4);
        let open: Option<String> = r.get(5);
        let status: String = r.get(3);
        let mut v = SourceVerdict { source: r.get(0), name: r.get(1), kind: r.get(2), outcome: "refuse".into(), reason: String::new(), grant: None, clause: None, not_after: None };
        let grants = grants.and_then(|g| g.as_array().cloned()).unwrap_or_default();
        let live = grants.iter().find(|g| {
            g["not_after"].as_str().and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok()).map(|t| t.with_timezone(&chrono::Utc) >= tx.now).unwrap_or(true)
        });
        if let Some(g) = live {
            v.outcome = "permit".into();
            v.reason = "granted".into();
            v.grant = g["id"].as_str().and_then(|s| s.parse().ok());
            v.clause = g["clause"].as_str().map(|s| s.to_string());
            v.not_after = g["not_after"].as_str().and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok()).map(|t| t.with_timezone(&chrono::Utc));
        } else if let Some(g) = grants.first() {
            v.reason = format!("the grant of {action} for {purpose} ended at {}", g["not_after"].as_str().unwrap_or("?"));
            v.clause = g["clause"].as_str().map(|s| s.to_string());
            v.not_after = g["not_after"].as_str().and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok()).map(|t| t.with_timezone(&chrono::Utc));
        } else if status == "partly-compiled" && open.is_some() {
            v.outcome = "review".into();
            v.clause = open.clone();
            v.reason = format!("the terms are partly compiled; the request depends on the clause {} that needs a review", open.unwrap());
        } else {
            v.reason = format!("the source grants no {action} for the purpose {purpose}");
        }
        out.push(v);
    }
    Ok(out)
}

/// The duties of sources, for the policy decision and for propagation (UP-4).
pub async fn duties_of_sources(tx: &Tx<'_>, sources: &[Uuid]) -> Result<Vec<Value>> {
    let rows = tx
        .q("select d.kind, d.clause, s.name, d.params from duty d join source s on s.id = d.source_id and s.tenant_id = d.tenant_id where d.source_id = any($1) order by s.name, d.kind", &[&sources])
        .await?;
    Ok(rows.iter().map(|r| json!({"duty": r.get::<_, String>(0), "clause": r.get::<_, Option<String>>(1), "source": r.get::<_, String>(2), "params": r.get::<_, Value>(3)})).collect())
}

/// Compute the effective-policy cache of a snapshot: the pairs that every source permits, and the
/// earliest expiry of the grants that it used (architecture.md, "Performance of policy predicates").
pub async fn refresh_policy_cache(tx: &Tx<'_>, snap: Uuid) -> Result<()> {
    let sources: Vec<Uuid> = crate::datasets::snapshot_sources(tx, snap).await?.into_iter().collect();
    let mut permitted = Vec::new();
    let mut earliest: Option<chrono::DateTime<chrono::Utc>> = None;
    let pairs = tx.q("select distinct action, purpose from usage_grant where source_id = any($1) and revoked_commit is null", &[&sources]).await?;
    for p in pairs {
        let (a, pu): (String, String) = (p.get(0), p.get(1));
        let v = evaluate(tx, &sources, &a, &pu).await?;
        if !sources.is_empty() && v.iter().all(|x| x.outcome == "permit") {
            permitted.push(format!("{a}:{pu}"));
            for x in &v {
                if let Some(t) = x.not_after {
                    earliest = Some(earliest.map(|e| e.min(t)).unwrap_or(t));
                }
            }
        }
    }
    tx.exec(
        "insert into policy_cache (snapshot_id, permitted, earliest_expiry, computed_at) values ($1,$2,$3,$4)
         on conflict (tenant_id, snapshot_id) do update set permitted = excluded.permitted, earliest_expiry = excluded.earliest_expiry, computed_at = excluded.computed_at",
        &[&snap, &permitted, &earliest, &tx.now],
    )
    .await?;
    Ok(())
}

/// The predicate `permitted(action, purpose)` on a snapshot, from the cache, with no read of
/// its members (PF-8). The expiry is compared with the time of the query, so an expired licence
/// takes effect at once without an update of the cache.
pub async fn permitted_predicate(tx: &Tx<'_>, snap: Uuid, action: &str, purpose: &str) -> Result<Option<bool>> {
    let r = tx.qopt("select permitted, earliest_expiry from policy_cache where snapshot_id = $1", &[&snap]).await?;
    Ok(r.map(|r| {
        let p: Vec<String> = r.get(0);
        let e: Option<chrono::DateTime<chrono::Utc>> = r.get(1);
        let pair = p.iter().any(|x| x == &format!("{action}:{purpose}") || x == &format!("{action}:any"));
        pair && e.map(|e| e >= tx.now).unwrap_or(true)
    }))
}

// ---------------------------------------------------------------------------- invalidation

#[derive(Clone, Debug, Default)]
pub struct InvalidationSet {
    pub versions: BTreeSet<String>,
    pub artifacts: BTreeSet<String>,
    pub sources: BTreeSet<String>,
    pub snapshots: BTreeSet<String>,
    pub fingerprints: BTreeSet<String>,
}

impl InvalidationSet {
    pub fn is_empty(&self) -> bool {
        self.versions.is_empty() && self.artifacts.is_empty() && self.sources.is_empty() && self.snapshots.is_empty() && self.fingerprints.is_empty()
    }
}

/// The tenant-wide invalidation set. The resolver applies it to every snapshot, old or new.
pub async fn invalidation_set(tx: &Tx<'_>) -> Result<InvalidationSet> {
    let mut s = InvalidationSet::default();
    for r in tx.q("select target_kind, target from invalidation", &[]).await? {
        let (k, t): (String, String) = (r.get(0), r.get(1));
        match k.as_str() {
            "version" => s.versions.insert(t),
            "artifact" => s.artifacts.insert(t),
            "source" => s.sources.insert(t),
            "snapshot" => s.snapshots.insert(t),
            _ => s.fingerprints.insert(t),
        };
    }
    Ok(s)
}

/// The versions among `ids` that the invalidation set excludes as a whole.
pub async fn invalidated_versions(tx: &Tx<'_>, ids: &[Uuid]) -> Result<BTreeSet<Uuid>> {
    let rows = tx
        .q(
            "select v.id from version v where v.id = any($1) and exists (select 1 from invalidation i where
                (i.target_kind = 'version' and i.target = v.id::text) or (i.target_kind = 'artifact' and i.target = v.artifact_id::text)
                or (i.target_kind = 'source' and i.target = v.source_id::text)
                or (i.target_kind = 'fingerprint' and (i.target = v.content_hash
                    or exists (select 1 from identity_index x where x.version_id = v.id and x.element_id = '' and x.key = i.target))))
             union select x from unnest($1::uuid[]) x where not exists (select 1 from version v where v.id = x)",
            &[&ids],
        )
        .await?;
    Ok(rows.iter().map(|r| r.get(0)).collect())
}

pub async fn is_invalidated(tx: &Tx<'_>, id: Uuid) -> Result<bool> {
    Ok(!invalidated_versions(tx, &[id]).await?.is_empty())
}

/// Invalidate content: an element fingerprint, a version, an artifact, a source or a snapshot.
/// The record is retroactive: from now, no resolution and no search result contains the content
/// (UP-9). mudcat makes no new snapshot. Models that used the content get a flag.
pub async fn invalidate(tx: &mut Tx<'_>, target_kind: &str, target: &str, reason: &str, requested_by: &str) -> Result<Value> {
    if !["version", "artifact", "source", "snapshot", "fingerprint"].contains(&target_kind) {
        return Err(Error::invalid(format!("cannot invalidate a {target_kind}")));
    }
    let commit = tx.commit_id().await?;
    let id = new_id();
    tx.exec(
        "insert into invalidation (id, target_kind, target, reason, requested_by, commit_id, at) values ($1,$2,$3,$4,$5,$6,$7)",
        &[&id, &target_kind, &target, &reason, &requested_by, &commit, &tx.now],
    )
    .await?;
    tx.audit("deletion", "invalidate", &format!("{target_kind}:{target}"), true, Some(reason), json!({"invalidation": id})).await?;
    let versions = affected_versions(tx, target_kind, target).await?;
    let elements = affected_elements(tx, target_kind, target).await?;
    // removal events, so that indexes outside mudcat delete their copies (EV-7)
    let vinfo = crate::namespace::versions(tx, &versions).await?;
    let mut artifacts = BTreeSet::new();
    for v in &vinfo {
        tx.emit_removal("removal.invalidated", v.id, "version", Some(v.acl_root), json!({"invalidation": id, "reason": reason, "artifact": v.artifact_id}))
            .await?;
        artifacts.insert((v.artifact_id, v.acl_root));
    }
    for (a, root) in &artifacts {
        let all: i64 = tx.q1("select count(*) from version where artifact_id = $1 and not is_delete_marker", &[a]).await?.get(0);
        let bad = invalidated_versions(tx, &tx.q("select id from version where artifact_id = $1 and not is_delete_marker", &[a]).await?.iter().map(|r| r.get(0)).collect::<Vec<Uuid>>())
            .await?
            .len() as i64;
        if bad >= all {
            tx.emit_removal("removal.invalidated", *a, "artifact", Some(*root), json!({"invalidation": id, "reason": reason})).await?;
        }
    }
    // chunks and other annotations keyed by the removed versions
    let chunk_events = annotation_entities_of(tx, &versions).await?;
    for (cid, root) in &chunk_events {
        tx.emit_removal("removal.invalidated", *cid, "chunk", *root, json!({"invalidation": id, "reason": reason})).await?;
    }
    let mut affected: Vec<(Uuid, i64)> = vinfo.iter().map(|v| (v.id, v.element_count)).collect();
    for (vid, n) in &elements {
        if !versions.contains(vid) {
            affected.push((*vid, *n));
        }
    }
    let flags = flag_models(tx, &affected, "invalidated-input", &format!("invalidation {id}: {reason}")).await?;
    tx.emit("invalidation.added", id, "invalidation", None, json!({"target_kind": target_kind, "target": target, "reason": reason, "requested_by": requested_by, "versions": versions.len()}))
        .await?;
    Ok(json!({"invalidation": id, "target_kind": target_kind, "target": target, "reason": reason, "requested_by": requested_by,
              "versions": versions, "elements": elements.iter().map(|e| json!({"version": e.0, "elements": e.1})).collect::<Vec<_>>(),
              "flagged_models": flags, "new_snapshots": 0}))
}

/// The versions that an invalidation target excludes as a whole.
pub async fn affected_versions(tx: &Tx<'_>, kind: &str, target: &str) -> Result<Vec<Uuid>> {
    let rows = match kind {
        "version" => tx.q("select id from version where id::text = $1", &[&target]).await?,
        "artifact" => tx.q("select id from version where artifact_id::text = $1", &[&target]).await?,
        "source" => tx.q("select id from version where source_id::text = $1", &[&target]).await?,
        "snapshot" => {
            let id: Uuid = target.parse().map_err(|_| Error::invalid("not a snapshot ID"))?;
            let m = crate::datasets::members(tx, id).await?;
            return Ok(m.iter().filter(|e| e.member_kind == "version").map(|e| e.member_id).collect());
        }
        _ => {
            tx.q(
                "select id from version where content_hash = $1 union select version_id from identity_index where key = $1 and element_id = ''",
                &[&target],
            )
            .await?
        }
    };
    Ok(rows.iter().map(|r| r.get(0)).collect())
}

/// Elements inside versions (for example an image inside a training shard) that a fingerprint names.
pub async fn affected_elements(tx: &Tx<'_>, kind: &str, target: &str) -> Result<Vec<(Uuid, i64)>> {
    if kind != "fingerprint" {
        return Ok(vec![]);
    }
    let rows = tx
        .q("select version_id, count(*) from identity_index where key = $1 and element_id <> '' group by version_id", &[&target])
        .await?;
    Ok(rows.iter().map(|r| (r.get(0), r.get(1))).collect())
}

async fn annotation_entities_of(tx: &Tx<'_>, versions: &[Uuid]) -> Result<Vec<(Uuid, Option<Uuid>)>> {
    if versions.is_empty() {
        return Ok(vec![]);
    }
    let rows = tx
        .q("select entity_id, acl_root from search_doc where parent_id = any($1) and kind = 'chunk' and index_version = (select coalesce(max(active_version),1) from index_state where name='search')", &[&versions])
        .await?;
    let mut out: Vec<(Uuid, Option<Uuid>)> = rows.iter().map(|r| (r.get(0), r.get(1))).collect();
    // chunks that are not in the search index yet: from the annotation sets that key them
    let sets = tx.q("select id from annotation_set where key_kind = 'byte-range'", &[]).await?;
    let tenant = tx.sess.tenant_id()?;
    let vs: BTreeSet<String> = versions.iter().map(|v| v.to_string()).collect();
    for s in sets {
        let sid: Uuid = s.get(0);
        if let Ok(rows) = tx.inst().manifests.read(tenant, sid) {
            for r in rows {
                if vs.contains(r["version"].as_str().unwrap_or("")) {
                    if let Some(cid) = r["id"].as_str().and_then(|c| c.parse::<Uuid>().ok()) {
                        if !out.iter().any(|o| o.0 == cid) {
                            out.push((cid, None));
                        }
                    }
                }
            }
        }
    }
    Ok(out)
}

/// Flag each model that a training run made from affected content, with the number of affected
/// elements. mudcat does not change the model; a person decides (model.md, removal rules).
pub async fn flag_models(tx: &mut Tx<'_>, affected: &[(Uuid, i64)], kind: &str, cause: &str) -> Result<Vec<Value>> {
    if affected.is_empty() {
        return Ok(vec![]);
    }
    let counts: HashMap<Uuid, i64> = affected.iter().copied().collect();
    let ids: Vec<Uuid> = counts.keys().copied().collect();
    let mut per_model: BTreeMap<Uuid, i64> = BTreeMap::new();
    for (model, inputs) in crate::provenance::models_with_inputs(tx, &ids).await? {
        let n: i64 = inputs.iter().map(|v| counts.get(v).copied().unwrap_or(0)).sum();
        *per_model.entry(model).or_default() += n;
    }
    let commit = tx.commit_id().await?;
    let mut out = Vec::new();
    for (m, n) in per_model {
        let id = new_id();
        tx.exec(
            "insert into model_flag (id, model_version_id, kind, cause, affected_elements, created_commit) values ($1,$2,$3,$4,$5,$6)",
            &[&id, &m, &kind, &cause, &n, &commit],
        )
        .await?;
        tx.emit("model.flagged", m, "version", None, json!({"flag": id, "kind": kind, "cause": cause, "affected_elements": n})).await?;
        out.push(json!({"flag": id, "model_version": m, "affected_elements": n}));
    }
    Ok(out)
}

/// Record the decision of a person about a flag on a model.
pub async fn decide_flag(tx: &mut Tx<'_>, flag: Uuid, decision: &str) -> Result<()> {
    let n = tx
        .exec("update model_flag set decision = $2, decided_by = $3, decided_at = $4 where id = $1", &[&flag, &decision, &tx.principal().0, &tx.now])
        .await?;
    if n == 0 {
        return Err(Error::NotFound);
    }
    let m: Uuid = tx.q1("select model_version_id from model_flag where id = $1", &[&flag]).await?.get(0);
    tx.emit("model.flag-decided", m, "version", None, json!({"flag": flag, "decision": decision})).await?;
    Ok(())
}

pub async fn flags_of(tx: &Tx<'_>, model_version: Uuid) -> Result<Vec<Value>> {
    let rows = tx
        .q("select id, kind, cause, affected_elements, decision, decided_by, decided_at from model_flag where model_version_id = $1 order by created_commit", &[&model_version])
        .await?;
    Ok(rows
        .iter()
        .map(|r| json!({"flag": r.get::<_, Uuid>(0), "kind": r.get::<_, String>(1), "cause": r.get::<_, String>(2), "affected_elements": r.get::<_, i64>(3),
                       "decision": r.get::<_, Option<String>>(4), "decided_by": r.get::<_, Option<String>>(5), "decided_at": r.get::<_, Option<chrono::DateTime<chrono::Utc>>>(6)}))
        .collect())
}

// ---------------------------------------------------------------------------- legal holds

pub async fn place_hold(tx: &mut Tx<'_>, name: &str, reason: &str, scope: &[Uuid]) -> Result<Uuid> {
    let id = new_id();
    tx.exec("insert into legal_hold (id, name, reason, placed_by, placed_at) values ($1,$2,$3,$4,$5)", &[&id, &name, &reason, &tx.principal().0, &tx.now])
        .await?;
    for e in scope {
        tx.exec("insert into hold_scope (hold_id, entity_id) values ($1,$2) on conflict do nothing", &[&id, e]).await?;
    }
    // the retention compartment of the hold: the person who placed it is a member
    let comp = format!("retention-{id}");
    crate::authz::grant(tx, "compartment", &comp, "member", &tx.principal().0.clone(), None).await?;
    tx.emit("legal-hold.placed", id, "legal-hold", None, json!({"name": name, "reason": reason, "scope": scope})).await?;
    Ok(id)
}

pub async fn release_hold(tx: &mut Tx<'_>, name: &str, reason: &str) -> Result<Value> {
    let r = tx.qopt("select id from legal_hold where name = $1 and released_at is null", &[&name]).await?.ok_or(Error::NotFound)?;
    let id: Uuid = r.get(0);
    tx.exec("update legal_hold set released_by = $2, released_reason = $3, released_at = $4 where id = $1", &[&id, &tx.principal().0, &reason, &tx.now])
        .await?;
    tx.emit("legal-hold.released", id, "legal-hold", None, json!({"name": name, "reason": reason, "released_by": tx.principal().0})).await?;
    Ok(json!({"hold": id, "released_by": tx.principal().0, "released_at": tx.now, "reason": reason}))
}

/// The active hold that covers a version: on the version, its artifact, or a dataset that holds it.
pub async fn hold_on(tx: &Tx<'_>, version: Uuid) -> Result<Option<(Uuid, String, String)>> {
    let r = tx
        .qopt(
            "select h.id, h.name, h.reason from legal_hold h join hold_scope s on s.hold_id = h.id and s.tenant_id = h.tenant_id
              where h.released_at is null and (
                    s.entity_id = $1
                 or s.entity_id = (select artifact_id from version where id = $1)
                 or s.entity_id in (select sn.dataset_id from manifest_entry e
                                      join manifest_ref r on r.manifest_id = e.manifest_id and r.manifest_tenant = e.tenant_id and not (e.member_id = any(r.excluded))
                                      join snapshot sn on sn.id = r.snapshot_id and sn.tenant_id = r.tenant_id
                                     where e.member_id = $1))
              limit 1",
            &[&version],
        )
        .await?;
    Ok(r.map(|r| (r.get(0), r.get(1), r.get(2))))
}

/// Other tenants that refer to a version (OP-15).
async fn foreign_holders(tx: &Tx<'_>, version: Uuid) -> Result<Vec<Value>> {
    let me = tx.sess.tenant_id()?;
    let rows = tx
        .q("select holder_tenant, holder_entity from cross_tenant_ref where owner_tenant = $1 and target_id = $2", &[&me, &version])
        .await?;
    Ok(rows
        .iter()
        .map(|r| {
            let t: Uuid = r.get(0);
            let name = tx.inst().tenant(t).map(|t| t.name).unwrap_or_default();
            json!({"tenant": t, "tenant_name": name, "snapshot": r.get::<_, Uuid>(1)})
        })
        .collect())
}

#[derive(Clone, Debug, Serialize, Deserialize, Default)]
pub struct RemovalOutcome {
    pub invalidation: Option<Uuid>,
    pub blocked_by_hold: Option<Value>,
    pub referenced_by: Vec<Value>,
    pub deletion_tasks: Vec<i64>,
    pub rewrite_tasks: Vec<i64>,
    pub erased: Vec<Uuid>,
}

async fn ensure_invalidated(tx: &mut Tx<'_>, versions: &[Uuid], reason: &str) -> Result<Option<Uuid>> {
    let already = invalidated_versions(tx, versions).await?;
    let mut last = None;
    for v in versions {
        if !already.contains(v) {
            let r = invalidate(tx, "version", &v.to_string(), reason, &tx.principal().0.clone()).await?;
            last = r["invalidation"].as_str().and_then(|s| s.parse().ok());
        }
    }
    Ok(last)
}

/// Purge content: invalidate it, then delete the bytes of each replica through the site agents,
/// with one deletion receipt for each (UP-7). A legal hold blocks the purge, but not the
/// invalidation (UP-8). A reference from another tenant blocks the removal (OP-15).
pub async fn purge(tx: &mut Tx<'_>, versions: &[Uuid], reason: &str) -> Result<RemovalOutcome> {
    let mut out = RemovalOutcome::default();
    for v in versions {
        let holders = foreign_holders(tx, *v).await?;
        if !holders.is_empty() {
            out.referenced_by = holders.clone();
            tx.sess.audit("deletion", "purge", &format!("version:{v}"), false, Some("referenced by another tenant"), json!({"holders": holders})).await?;
            return Err(Error::refused(
                "referenced-by-tenant",
                format!("mudcat refuses to remove the version {v}: a dataset of the tenant {} still refers to it", holders[0]["tenant_name"].as_str().unwrap_or("?")),
                json!({"version": v, "referenced_by": holders}),
            ));
        }
    }
    out.invalidation = ensure_invalidated(tx, versions, reason).await?;
    for v in versions {
        if let Some((hid, name, hreason)) = hold_on(tx, *v).await? {
            tx.sess.audit("deletion", "purge", &format!("version:{v}"), false, Some(&format!("legal hold {name}: {hreason}")), json!({"hold": hid})).await?;
            for x in versions {
                tx.exec("insert into hold_scope (hold_id, entity_id) values ($1,$2) on conflict do nothing", &[&hid, x]).await?;
            }
            tx.emit("removal.blocked-by-hold", *v, "version", None, json!({"hold": name, "reason": hreason, "operation": "purge"})).await?;
            out.blocked_by_hold = Some(json!({"hold": hid, "name": name, "reason": hreason, "compartment": format!("retention-{hid}")}));
            return Ok(out);
        }
    }
    tx.audit("deletion", "purge", &format!("versions:{}", versions.len()), true, Some(reason), json!({"versions": versions})).await?;
    out.deletion_tasks = enqueue_deletions(tx, versions, reason).await?;
    // derived files that contain the content as an element must be written again without it
    for v in versions {
        let hash: Option<String> = tx.qopt("select content_hash from version where id = $1", &[v]).await?.and_then(|r| r.get(0));
        if let Some(h) = hash {
            let shards = tx.q("select distinct version_id from identity_index where key = $1 and element_id <> ''", &[&h]).await?;
            for s in shards {
                let sid: Uuid = s.get(0);
                let res: Option<Uuid> = tx.qopt("select resource_id from replica where version_id = $1 limit 1", &[&sid]).await?.map(|r| r.get(0));
                if let Some(res) = res {
                    let id: i64 = tx
                        .q1("insert into agent_task (kind, resource_id, payload) values ('rewrite', $1, $2) returning id", &[&res, &json!({"version": sid, "drop_key": h, "reason": reason})])
                        .await?
                        .get(0);
                    out.rewrite_tasks.push(id);
                }
            }
        }
    }
    Ok(out)
}

async fn enqueue_deletions(tx: &mut Tx<'_>, versions: &[Uuid], reason: &str) -> Result<Vec<i64>> {
    let mut ids = Vec::new();
    let reps = crate::namespace::replicas_of(tx, versions).await?;
    for r in reps.iter().filter(|r| r.state != "retired") {
        let id: i64 = tx
            .q1(
                "insert into agent_task (kind, resource_id, payload) values ('delete', $1, $2) returning id",
                &[&r.resource_id, &json!({"replica": r.id, "version": r.version_id, "url": r.url, "all_versions": true, "reason": reason})],
            )
            .await?
            .get(0);
        ids.push(id);
    }
    Ok(ids)
}

/// Erase versions: remove their catalog records, and keep only erasure records with the ID, the
/// date, the reason and the content hash (UP-11). The bytes go too.
pub async fn erase(tx: &mut Tx<'_>, versions: &[Uuid], reason: &str, hash_identifies_person: bool) -> Result<RemovalOutcome> {
    let mut out = purge(tx, versions, reason).await?;
    if out.blocked_by_hold.is_some() {
        if let Some(h) = &out.blocked_by_hold {
            tx.emit("removal.blocked-by-hold", versions[0], "version", None, json!({"hold": h["name"], "operation": "erase"})).await?;
        }
        return Ok(out);
    }
    let commit = tx.commit_id().await?;
    for v in versions {
        let info = crate::namespace::version(tx, *v).await?;
        let hash = if hash_identifies_person { None } else { info.content_hash.clone() };
        tx.exec(
            "insert into erasure_record (id, kind, erased_at, reason, content_hash, commit_id) values ($1,'version',$2,$3,$4,$5) on conflict do nothing",
            &[v, &tx.now, &reason, &hash, &commit],
        )
        .await?;
        tx.exec("delete from metadata_payload where entity_id = $1", &[v]).await?;
        tx.exec("delete from metadata_history where entity_id = $1", &[v]).await?;
        tx.exec("delete from attr_index where entity_id = $1", &[v]).await?;
        tx.exec("delete from tag where entity_id = $1", &[v]).await?;
        tx.exec("delete from identity_index where version_id = $1", &[v]).await?;
        tx.exec("delete from identifier where entity_id = $1", &[v]).await?;
        tx.exec("delete from replica_history where replica_id in (select id from replica where version_id = $1)", &[v]).await?;
        tx.exec("delete from verification where replica_id in (select id from replica where version_id = $1)", &[v]).await?;
        tx.exec("delete from replica where version_id = $1", &[v]).await?;
        tx.exec("update artifact set latest_version_id = (select id from version where artifact_id = $1 and id <> $2 order by number desc limit 1) where id = $1", &[&info.artifact_id, v])
            .await?;
        tx.exec("delete from version where id = $1", &[v]).await?;
        if hash_identifies_person {
            tx.exec("update manifest_entry set member_hash = '' where member_id = $1", &[v]).await?;
        }
        tx.emit_removal("removal.erased", *v, "version", Some(info.acl_root), json!({"reason": reason})).await?;
        tx.exec("update entity set erased = true where id = $1", &[v]).await?;
        let left: i64 = tx.q1("select count(*) from version where artifact_id = $1", &[&info.artifact_id]).await?.get(0);
        if left == 0 {
            tx.exec(
                "insert into erasure_record (id, kind, erased_at, reason, commit_id) values ($1,'artifact',$2,$3,$4) on conflict do nothing",
                &[&info.artifact_id, &tx.now, &reason, &commit],
            )
            .await?;
            tx.exec("delete from metadata_payload where entity_id = $1", &[&info.artifact_id]).await?;
            tx.exec("delete from identifier where entity_id = $1", &[&info.artifact_id]).await?;
            tx.exec("delete from artifact where id = $1", &[&info.artifact_id]).await?;
            tx.emit_removal("removal.erased", info.artifact_id, "artifact", Some(info.acl_root), json!({"reason": reason})).await?;
            tx.exec("update entity set erased = true where id = $1", &[&info.artifact_id]).await?;
        }
        out.erased.push(*v);
    }
    Ok(out)
}

/// Erase one metadata payload, for example one that holds the name of a person. The erasure
/// record does not hold the erased values.
pub async fn erase_payload(tx: &mut Tx<'_>, entity: Uuid, namespace: &str, reason: &str) -> Result<Uuid> {
    let n = tx.exec("delete from metadata_payload where entity_id = $1 and namespace = $2", &[&entity, &namespace]).await?;
    if n == 0 {
        return Err(Error::NotFound);
    }
    tx.exec("delete from metadata_history where entity_id = $1 and namespace = $2", &[&entity, &namespace]).await?;
    tx.exec("delete from attr_index where entity_id = $1", &[&entity]).await?;
    let id = crate::authz::uuid_for(&format!("payload:{entity}/{namespace}"));
    let commit = tx.commit_id().await?;
    tx.exec(
        "insert into erasure_record (id, kind, erased_at, reason, commit_id) values ($1,'metadata-payload',$2,$3,$4) on conflict do nothing",
        &[&id, &tx.now, &reason, &commit],
    )
    .await?;
    let root = crate::metadata::acl_root_of(tx, entity).await?;
    tx.emit_removal("removal.erased", id, "metadata-payload", root, json!({"entity": entity, "namespace": namespace, "reason": reason})).await?;
    // the entity changed: consumers get its new state document
    let kind = crate::namespace::entity_kind(tx, entity).await.unwrap_or_else(|_| "entity".into());
    tx.emit("metadata.removed", entity, &kind, root, json!({"namespace": namespace})).await?;
    Ok(id)
}
