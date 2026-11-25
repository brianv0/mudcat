//! The invariants of evals/invariants.yaml, as checks that a runner makes after each step.

use super::Ctx;
use crate::error::Result;
use crate::instance::Principal;
use crate::in_tx;
use crate::storage::Credential;
use std::collections::{BTreeMap, BTreeSet};
use uuid::Uuid;

pub async fn check(ctx: &mut Ctx, id: &str) -> Result<(bool, String)> {
    let sys = ctx.admin.as_principal(Principal::system());
    match id {
        // mudcat transfers no data bytes when a client reads data
        "INV-1" => {
            let n = ctx.inst.metrics.bytes_served_by_catalog.load(std::sync::atomic::Ordering::SeqCst);
            let catalog_reads = ctx.inst.store.accesses().iter().filter(|a| a.client == "mudcat-api").count();
            Ok((n == 0 && catalog_reads == 0, format!("bytes served by the catalog: {n}")))
        }
        // a published snapshot never changes its members or its Merkle hash
        "INV-2" => {
            let published = ctx.record.published.clone();
            for (snap, (members, hash)) in published {
                let (now_members, now_hash): (BTreeSet<Uuid>, String) = in_tx!(sys, |tx| {
                    let m = crate::datasets::members(&tx, snap).await?;
                    let i = crate::datasets::snapshot(&tx, snap).await?;
                    (m.iter().map(|e| e.member_id).collect(), i.merkle_hash.unwrap_or_default())
                });
                if now_members != members || now_hash != hash {
                    return Ok((false, format!("snapshot {snap} changed: members {} -> {}, hash {} -> {}", members.len(), now_members.len(), hash, now_hash)));
                }
            }
            Ok((true, String::new()))
        }
        // invalidated content is never in a resolution, a plan or a search result
        "INV-3" => {
            let res = ctx.record.resolutions.clone();
            let searches = ctx.record.search_results.clone();
            let bad = in_tx!(sys, |tx| {
                let invs = tx.q("select target_kind, target, commit_id, at from invalidation order by commit_id", &[]).await?;
                // the versions and the elements that each invalidation names, computed once
                let mut named: Vec<(i64, chrono::DateTime<chrono::Utc>, BTreeSet<Uuid>, BTreeSet<(Uuid, String)>)> = Vec::new();
                for i in &invs {
                    let kind: String = i.get(0);
                    let target: String = i.get(1);
                    let vs: BTreeSet<Uuid> = crate::policy::affected_versions(&tx, &kind, &target).await?.into_iter().collect();
                    let els: BTreeSet<(Uuid, String)> = if kind == "fingerprint" {
                        tx.q("select version_id, element_id from identity_index where key = $1 and element_id <> ''", &[&target]).await?.iter().map(|r| (r.get(0), r.get(1))).collect()
                    } else {
                        BTreeSet::new()
                    };
                    named.push((i.get(2), i.get(3), vs, els));
                }
                let mut bad = Vec::new();
                for r in &res {
                    let commit: Option<i64> = tx.qopt("select created_commit from resolution where id = $1", &[&r.id]).await?.map(|x| x.get(0));
                    let Some(commit) = commit else { continue };
                    for (c, _, vs, els) in &named {
                        if *c >= commit {
                            continue;
                        }
                        for t in &r.tasks {
                            if vs.contains(&t.version) {
                                bad.push(format!("resolution {} has invalidated {}", r.id, t.version));
                            }
                            if let Some(list) = &t.elements {
                                if els.iter().any(|(v, e)| *v == t.version && list.contains(e)) {
                                    bad.push(format!("plan of {} lists an invalidated element", r.id));
                                }
                            }
                        }
                    }
                }
                for (at, ids) in &searches {
                    for (_, t, vs, _) in &named {
                        if t > at {
                            continue;
                        }
                        if ids.iter().any(|i| vs.contains(i)) {
                            bad.push("a search result has invalidated content".to_string());
                        }
                    }
                }
                bad
            });
            Ok((bad.is_empty(), bad.into_iter().take(3).collect::<Vec<_>>().join("; ")))
        }
        // a resolution names the exact version or snapshot of each input
        "INV-4" => {
            let bad: Vec<String> = in_tx!(sys, |tx| {
                let rows = tx.q("select id, inputs from resolution", &[]).await?;
                let mut bad = Vec::new();
                for r in rows {
                    let inputs: serde_json::Value = r.get(1);
                    for i in inputs.as_array().cloned().unwrap_or_default() {
                        let exact = i["snapshot"].as_str().and_then(|s| s.parse::<Uuid>().ok()).is_some()
                            || i["version"].as_str().and_then(|s| s.parse::<Uuid>().ok()).is_some()
                            || i["versions"].as_array().map(|a| a.iter().all(|v| v.as_str().and_then(|s| s.parse::<Uuid>().ok()).is_some())).unwrap_or(false);
                        if !exact {
                            bad.push(format!("resolution {} has an input with no exact ID: {i}", r.get::<_, Uuid>(0)));
                        }
                    }
                }
                bad
            });
            Ok((bad.is_empty(), bad.join("; ")))
        }
        // one event for each committed change, positions in commit order
        "INV-5" => {
            crate::stream::flush(&sys).await?;
            let bad: Vec<String> = in_tx!(sys, |tx| {
                let rows = tx.q("select position, subject, revision, commit_id from event order by position", &[]).await?;
                let commits: BTreeSet<i64> = tx.q("select commit_id from commit_log", &[]).await?.iter().map(|r| r.get(0)).collect();
                let floor: i64 = tx.qopt("select floor from stream_state", &[]).await?.map(|r| r.get(0)).unwrap_or(0);
                let mut bad = Vec::new();
                let mut last = floor;
                let mut revs: BTreeMap<Uuid, i32> = BTreeMap::new();
                for r in rows {
                    let p: i64 = r.get(0);
                    if p != last + 1 {
                        bad.push(format!("position {p} after {last}"));
                    }
                    last = p;
                    let s: Uuid = r.get(1);
                    let rev: i32 = r.get(2);
                    if let Some(prev) = revs.get(&s) {
                        if rev <= *prev {
                            bad.push(format!("entity {s}: revision {rev} after {prev}"));
                        }
                    }
                    revs.insert(s, rev);
                    if !commits.contains(&r.get::<_, i64>(3)) {
                        bad.push(format!("event at {p} has no commit"));
                    }
                }
                bad
            });
            Ok((bad.is_empty(), bad.into_iter().take(5).collect::<Vec<_>>().join("; ")))
        }
        // a request returns only the data of its own tenant
        "INV-6" => {
            let answers = ctx.record.answers.clone();
            for (tenant, ids) in answers {
                let s = ctx.inst.session(tenant, Principal::system());
                let found: i64 = in_tx!(s, |tx| tx.q1("select count(*) from entity where id = any($1)", &[&ids]).await?.get(0));
                if found as usize != ids.iter().collect::<BTreeSet<_>>().len() {
                    return Ok((false, format!("an answer of tenant {tenant} has {} IDs, {found} of the tenant", ids.len())));
                }
            }
            Ok((true, String::new()))
        }
        // the answer for an ID of another tenant is the same as for an ID that does not exist
        "INV-7" => {
            let others: Vec<Uuid> = ctx.inst.tenants.read().keys().copied().filter(|t| *t != ctx.tenant.id).collect();
            let Some(other) = others.first() else { return Ok((true, "one tenant only".into())) };
            let os = ctx.inst.session(*other, Principal::system());
            let foreign: Option<Uuid> = in_tx!(os, |tx| tx.qopt("select id from entity limit 1", &[]).await?.map(|r| r.get(0)));
            let Some(foreign) = foreign else { return Ok((true, String::new())) };
            let mine = ctx.admin.clone();
            let a = in_tx!(mine, |tx| crate::namespace::entity_kind(&tx, foreign).await.err().map(|e| e.to_json()));
            let b = in_tx!(mine, |tx| crate::namespace::entity_kind(&tx, Uuid::new_v4()).await.err().map(|e| e.to_json()));
            Ok((a.is_some() && a == b, format!("{a:?} vs {b:?}")))
        }
        // a replica is verified only after a successful comparison of checksums
        "INV-8" => {
            let n: i64 = in_tx!(sys, |tx| tx
                .q1(
                    "select count(*) from replica r join version v on v.id = r.version_id and v.tenant_id = r.tenant_id where r.state = 'verified'
                       and not exists (select 1 from verification x where x.replica_id = r.id and x.ok and x.observed = v.content_hash)",
                    &[],
                )
                .await?
                .get(0));
            Ok((n == 0, format!("{n} verified replicas without a verification")))
        }
        // each access decision at an enforcement point is in the audit log
        "INV-9" => {
            let n: i64 = in_tx!(sys, |tx| tx
                .q1("select count(*) from resolution r where not exists (select 1 from audit a where a.point = 'resolution' and a.object = 'resolution:' || r.id::text)", &[])
                .await?
                .get(0));
            Ok((n == 0, format!("{n} resolutions without an audit entry")))
        }
        // no removal of a version or manifest that a reference names
        "INV-10" => {
            let rows: i64 = in_tx!(sys, |tx| tx
                .q1(
                    "select (select count(*) from manifest_ref r where r.manifest_tenant = r.tenant_id and not exists (select 1 from manifest m where m.id = r.manifest_id))
                          + (select count(*) from manifest_entry e where e.member_kind = 'version' and not exists (select 1 from version v where v.id = e.member_id)
                               and not exists (select 1 from erasure_record x where x.id = e.member_id))",
                    &[],
                )
                .await?
                .get(0));
            Ok((rows == 0, format!("{rows} references with no target")))
        }
        // after a grant is removed, a later check is denied the access that the grant gave
        "INV-11" => {
            let rows: Vec<String> = in_tx!(sys, |tx| tx.q("select tuple from relation_history where op = 'delete'", &[]).await?.iter().map(|r| r.get(0)).collect());
            for t in rows {
                // object:id#relation@subject
                let Some((obj, subject)) = t.split_once('@') else { continue };
                let Some((otype_id, rel)) = obj.split_once('#') else { continue };
                let Some((otype, oid)) = otype_id.split_once(':') else { continue };
                if !subject.starts_with("user:") {
                    continue;
                }
                let us = ctx.inst.session(ctx.tenant.id, Principal(subject.to_string()));
                if otype == "group" && rel == "member" {
                    let groups = in_tx!(us, |tx| crate::authz::groups_of(&tx).await?);
                    let still: bool = in_tx!(sys, |tx| tx
                        .qopt("select 1 from relation where object_type='group' and object_id=$1 and relation='member' and subject_type='user' and subject_id=$2", &[&oid, &subject.trim_start_matches("user:")])
                        .await?
                        .is_some());
                    if groups.contains(&oid.to_string()) && !still {
                        return Ok((false, format!("{subject} is still in {oid} after the revocation")));
                    }
                } else if otype == "collection" {
                    let Ok(id) = oid.parse::<Uuid>() else { continue };
                    let d = in_tx!(us, |tx| crate::authz::check(&tx, "read", "collection", id).await?);
                    if d.allow && d.via.as_deref() == Some(t.as_str()) {
                        return Ok((false, format!("the removed grant {t} still gives access")));
                    }
                }
            }
            Ok((true, String::new()))
        }
        // mudcat does not purge or erase content that a legal hold covers
        "INV-12" => {
            let holds = ctx.record.holds.clone();
            for v in holds {
                let reps = in_tx!(sys, |tx| {
                    if tx.qopt("select 1 from version where id = $1", &[&v]).await?.is_none() {
                        return Ok((false, format!("held version {v} has no record")));
                    }
                    crate::namespace::replicas_of(&tx, &[v]).await?
                });
                for r in reps {
                    let (b, k) = crate::storage::parse_url(&r.url)?;
                    if !ctx.inst.store.exists(&b, &k) || r.state == "retired" {
                        return Ok((false, format!("the bytes of held version {v} are gone ({})", r.url)));
                    }
                }
            }
            Ok((true, String::new()))
        }
        // a second registration of the same artifact and checksum makes no new version and no new event
        "INV-13" => {
            crate::stream::flush(&sys).await?;
            let n: i64 = in_tx!(sys, |tx| tx
                .q1("select count(*) from (select subject from event where type = 'mudcat.version.registered.v1' group by subject having count(*) > 1) x", &[])
                .await?
                .get(0));
            let d: i64 = in_tx!(sys, |tx| tx
                .q1("select count(*) from (select artifact_id, content_hash from version where content_hash is not null group by 1, 2 having count(*) > 1) x", &[])
                .await?
                .get(0));
            Ok((n == 0 && d == 0, format!("{n} versions with two registration events, {d} duplicate versions")))
        }
        // a permitted resolution has no element whose source does not grant the request
        "INV-14" => {
            for r in &ctx.record.resolutions {
                if r.decision["action"].is_null() {
                    continue;
                }
                let permitted: BTreeSet<String> = r.decision["policies"]
                    .as_array()
                    .cloned()
                    .unwrap_or_default()
                    .iter()
                    .filter(|p| p["outcome"] == "permit")
                    .filter_map(|p| p["source"].as_str().map(|s| s.to_string()))
                    .collect();
                for t in &r.tasks {
                    let ok = t.source.map(|s| permitted.contains(&s.to_string())).unwrap_or(false);
                    if !ok {
                        return Ok((false, format!("resolution {} has {} from a source that does not permit", r.id, t.version)));
                    }
                }
            }
            Ok((true, String::new()))
        }
        // prefix credentials cover no object outside one ACL root that the caller can read
        "INV-15" => {
            let res = ctx.record.resolutions.clone();
            for r in &res {
                let caller = ctx.record.principals.get(&r.id).cloned().unwrap_or_else(|| ctx.admin.clone());
                for c in &r.credentials {
                    let Credential::Prefix { bucket, prefix, .. } = &c.credential else { continue };
                    let objects = ctx.inst.store.list(bucket)?;
                    let like = format!("s3://{bucket}/{prefix}%");
                    let by_url: BTreeMap<String, Uuid> = in_tx!(sys, |tx| tx
                        .q(
                            "select r.url, c.acl_root_id from replica r join version v on v.id = r.version_id and v.tenant_id = r.tenant_id
                               join artifact a on a.id = v.artifact_id and a.tenant_id = v.tenant_id join collection c on c.id = a.collection_id and c.tenant_id = a.tenant_id
                              where r.url like $1",
                            &[&like],
                        )
                        .await?
                        .iter()
                        .map(|x| (x.get(0), x.get(1)))
                        .collect());
                    let mut roots = BTreeSet::new();
                    for (k, _) in objects.iter().filter(|(k, _)| k.starts_with(prefix.as_str())) {
                        match by_url.get(&format!("s3://{bucket}/{k}")) {
                            Some(x) => {
                                roots.insert(*x);
                            }
                            None => return Ok((false, format!("the prefix {prefix} covers {k}, which no replica describes"))),
                        }
                    }
                    if roots.len() > 1 {
                        return Ok((false, format!("the prefix {prefix} covers {} ACL roots", roots.len())));
                    }
                    if let Some(root) = roots.iter().next() {
                        let d = in_tx!(caller, |tx| crate::authz::check(&tx, "read", "collection", *root).await?);
                        if !d.allow {
                            return Ok((false, format!("the caller cannot read the ACL root of the prefix {prefix}")));
                        }
                    }
                }
            }
            Ok((true, String::new()))
        }
        // a derived index that is dropped and built again has the same content as before
        "INV-16" => {
            crate::search::build(&sys).await?;
            let before = crate::search::snapshot_index(&sys).await?;
            crate::search::rebuild(&sys).await?;
            let after = crate::search::snapshot_index(&sys).await?;
            Ok((before == after, format!("{} documents before, {} after", before.len(), after.len())))
        }
        other => Ok((false, format!("unknown invariant {other}"))),
    }
}
