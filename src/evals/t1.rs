//! Drivers for the task templates of use case 1: curation of video and sensor data.

use super::fixtures::*;
use super::Ctx;
use crate::datasets::{DatasetChange, Member};
use crate::error::{Error, Result};
use crate::in_tx;
use crate::placement::{self, AgentFault, MoverFault, SiteAgent};
use crate::resolver;
use crate::{datasets, namespace, policy, Principal};
use serde_json::json;
use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::sync::atomic::Ordering;
use std::sync::Arc;
use uuid::Uuid;

/// T1.1: get the data of a dataset for a training job, and read it directly from storage.
pub async fn resolve_and_read(ctx: &mut Ctx) -> Result<()> {
    let structure = ctx.p("structure");
    let external = ctx.is("storage", "external");
    let n = ctx.n("members") as usize;
    let admin = ctx.admin.clone();
    let res = resource(&admin, "store", "cloud-a", if external { "external" } else { "managed" }, false).await?;
    acl_root(&admin, "/data", Some(("store", "data/"))).await?;
    source(&admin, "src", "instrument", json!({}), &[("train", "research")]).await?;
    grant_collection(&admin, "/data", "reader", "user:trainer").await?;
    // a distractor: a second ACL root on another prefix of the same bucket, with two registered and
    // verified files that the trainer cannot read. A prefix credential for more than /data covers them (INV-15).
    acl_root(&admin, "/secret", Some(("store", "secret/"))).await?;
    let secret_files: Vec<(String, Vec<u8>)> = (0..2).map(|i| (format!("hidden-{i}.bin"), file_bytes("opaque", i, "secret"))).collect();
    let files: Vec<(String, Vec<u8>)> = (0..n).map(|i| (format!("item-{i:06}.{}", ext(&structure)), file_bytes(&structure, i, "a"))).collect();
    let copied_before = ctx.inst.metrics.bytes_copied_for_catalog.load(Ordering::SeqCst);
    let puts_before = ctx.inst.store.accesses().iter().filter(|a| a.op == "put" && a.client != "lab").count();
    let regs = if external {
        // the data is already in a bucket that mudcat does not manage; a site agent registers it in place
        for (name, data) in &files {
            ctx.inst.store.put("lab", &res.bucket, &format!("data/{name}"), data)?;
        }
        in_tx!(admin, |tx| {
            let c = tx.commit_id().await?;
            tx.exec("insert into metadata_payload (entity_id, namespace, payload, origin, asserted_by, commit_id) select id, 'defaults', '{\"source\":\"src\"}', 'person', 'user:admin', $1 from collection where path = '/data'", &[&c]).await?;
        });
        for (name, data) in &secret_files {
            ctx.inst.store.put("lab", &res.bucket, &format!("secret/{name}"), data)?;
        }
        placement::report_inventory(&admin, "store", "agent-store").await?;
        // only the versions under /data; the distractor has two more
        let ids: Vec<Uuid> = in_tx!(admin, |tx| tx
            .q("select v.id from version v join artifact a on a.id = v.artifact_id join collection c on c.id = a.collection_id where c.path = '/data'", &[])
            .await?
            .iter()
            .map(|r| r.get(0))
            .collect());
        ids
    } else {
        register_files(&admin, &res, "/secret", "secret/", &secret_files, None, None).await?;
        register_files(&admin, &res, "/data", "data/", &files, Some("src"), None).await?.iter().map(|r| r.version_id).collect()
    };
    let copied = ctx.inst.metrics.bytes_copied_for_catalog.load(Ordering::SeqCst) - copied_before;
    let mudcat_puts = ctx.inst.store.accesses().iter().filter(|a| a.op == "put" && a.client != "lab" && a.client != "producer").count() - puts_before.min(0);
    ctx.check("copied no bytes at registration", copied == 0 && mudcat_puts == 0, format!("{copied} bytes copied, {mudcat_puts} writes by mudcat"));
    verify_all(&admin, "store").await?;
    let members: Vec<Member> = regs.iter().map(|v| Member::version(*v)).collect();
    dataset(&admin, "/data/clips", members).await?;
    let snap = publish(ctx, &admin, "/data/clips", "v1").await?;
    ctx.step().await?;

    // invalidation
    let invalidated: BTreeSet<Uuid> = match ctx.p("invalidated").as_str() {
        "some" => regs.iter().take((n / 5).max(1)).copied().collect(),
        "all" => regs.iter().copied().collect(),
        _ => BTreeSet::new(),
    };
    if ctx.is("invalidated", "all") {
        let sid: Uuid = in_tx!(admin, |tx| policy::source_id(&tx, "src").await?);
        in_tx!(admin, |tx| policy::invalidate(&mut tx, "source", &sid.to_string(), "takedown of the source", "counsel").await?);
    } else {
        for v in &invalidated {
            in_tx!(admin, |tx| policy::invalidate(&mut tx, "version", &v.to_string(), "takedown", "counsel").await?);
        }
    }
    ctx.step().await?;

    let trainer = ctx.session("user:trainer");
    let req = train_request(vec![dataset_input("/data/clips", Some("v1"), "training")], "train", "research");
    let r = resolve(ctx, &trainer, &req).await?;
    ctx.step().await?;
    let outcome = if r.tasks.is_empty() && r.decision["outcome"] == "nothing-to-read" { "nothing-to-read" } else { "resolved" };
    ctx.outcome(outcome);
    ctx.check("names the exact version of each input", r.inputs[0]["snapshot"] == json!(snap.id), format!("input {}", r.inputs[0]["snapshot"]));
    let task_versions: BTreeSet<Uuid> = r.tasks.iter().map(|t| t.version).collect();
    let expected: BTreeSet<Uuid> = regs.iter().copied().filter(|v| !invalidated.contains(v)).collect();
    ctx.check("covers each member that is not invalidated", task_versions == expected, format!("{} in plan, {} expected", task_versions.len(), expected.len()));
    ctx.check("has a replica URL, the expected bytes", r.tasks.iter().all(|t| !t.url.is_empty() && t.expected_bytes > 0 && t.expected_elements > 0), "");
    let excluded = r.decision["excluded"]["members"].as_array().map(|a| a.len()).unwrap_or(0);
    ctx.check("the number of members that policy excluded", excluded == invalidated.len(), format!("{excluded}"));
    ctx.check("The plan has no task, and mudcat issued no credentials", r.tasks.is_empty() && r.credentials.is_empty(), "");
    ctx.check("policy excluded all", excluded == n, format!("{excluded} of {n}"));
    let manifests: i64 = in_tx!(admin, |tx| tx.q1("select count(*) from version where element_manifest is not null", &[]).await?.get(0));
    ctx.check("made no element list", manifests == 0 && r.tasks.iter().all(|t| t.addressing == "selector" && t.selector.is_some()), format!("{manifests} element lists"));
    ctx.check("through an element list", r.tasks.iter().all(|t| t.addressing == "element-list" && t.elements.as_ref().map(|e| !e.is_empty()).unwrap_or(false)) && manifests == n as i64, "");
    ctx.check("each member as one object", r.tasks.iter().all(|t| t.addressing == "object" && t.expected_elements == 1), "");
    if outcome == "resolved" {
        let rep = placement::read_plan(&trainer, "job", &r).await?;
        let sizes: i64 = in_tx!(admin, |tx| tx.q1("select coalesce(sum(size),0)::bigint from version where id = any($1)", &[&expected.iter().copied().collect::<Vec<_>>()]).await?.get(0));
        let served = ctx.inst.metrics.bytes_served_by_catalog.load(Ordering::SeqCst);
        ctx.check("read all bytes directly from storage", rep.failures.is_empty() && rep.bytes as i64 == sizes && served == 0, format!("{} bytes read of {sizes}, failures {:?}", rep.bytes, rep.failures.first()));
    }
    ctx.step().await?;
    Ok(())
}

fn ext(structure: &str) -> &'static str {
    match structure {
        "records" => "jsonl",
        "container" => "tar",
        "stream" => "mp4",
        "array" => "zarr",
        _ => "bin",
    }
}

/// Register `n` opaque versions with no replicas, quickly.
async fn quick_versions(s: &crate::Session, coll: &str, prefix: &str, n: usize) -> Result<Vec<Uuid>> {
    let regs: Vec<namespace::Register> = (0..n)
        .map(|i| namespace::Register {
            path: format!("{coll}/{prefix}{i:06}"),
            content_hash: Some(crate::instance::sha256_hex(format!("{coll}/{prefix}{i}").as_bytes())),
            size: 100 + i as i64,
            source: Some("src".into()),
            ..Default::default()
        })
        .collect();
    let mut out = Vec::new();
    for c in regs.chunks(2000) {
        out.extend(in_tx!(s, |tx| namespace::register_batch(&mut tx, c).await?).into_iter().map(|r| r.version_id));
    }
    Ok(out)
}

/// T1.2: several writers change the same branch at the same time; no change is lost.
pub async fn concurrent_commits(ctx: &mut Ctx) -> Result<()> {
    let writers = ctx.n("writers") as usize;
    let change = ctx.p("change");
    let overlapping = ctx.is("members", "overlapping");
    let admin = ctx.admin.clone();
    source(&admin, "src", "instrument", json!({}), &[("train", "research")]).await?;
    let base = quick_versions(&admin, "/d", "base-", 1000).await?;
    let extra = quick_versions(&admin, "/d", "new-", writers * 10 + 10).await?;
    let first = dataset(&admin, "/d/set", base.iter().map(|v| Member::version(*v)).collect()).await?;
    acl_root(&admin, "/d", None).await?;
    grant_collection(&admin, "/d", "writer", "group:writers#member").await?;
    for w in 0..writers {
        grant(&admin, "group", "writers", "member", &format!("user:writer{w}")).await?;
    }
    publish(ctx, &admin, "/d/set", "start").await?;
    let s0 = first.snapshot;
    let start_pos = { crate::stream::flush(&admin).await?; in_tx!(admin, |tx| tx.q1("select last_position from stream_state", &[]).await?.get::<_, i64>(0)) };
    ctx.step().await?;

    // the members that each writer changes
    let plan: Vec<(Vec<Uuid>, Vec<Uuid>)> = (0..writers)
        .map(|w| {
            let (lo, hi) = if overlapping { (w * 5, w * 5 + 10) } else { (w * 10, w * 10 + 10) };
            let olds: Vec<Uuid> = base[lo..hi].to_vec();
            let news: Vec<Uuid> = extra[lo..hi].to_vec();
            (olds, news)
        })
        .collect();
    let barrier = Arc::new(tokio::sync::Barrier::new(writers));
    let mut handles = Vec::new();
    for (w, (olds, news)) in plan.clone().into_iter().enumerate() {
        let s = ctx.session(&format!("user:writer{w}"));
        let b = barrier.clone();
        let change = change.clone();
        handles.push(tokio::spawn(async move {
            let mut attempts = 0;
            let mut conflicts = 0;
            let mut gone_errors: Vec<String> = Vec::new();
            let mut olds = olds;
            let mut requirement = if change == "add-only" { None } else { Some(s0) };
            b.wait().await;
            loop {
                attempts += 1;
                let ch = match change.as_str() {
                    "add-only" => DatasetChange { dataset: "/d/set".into(), add: news.iter().map(|v| Member::version(*v)).collect(), requirement, ..Default::default() },
                    "remove" => DatasetChange { dataset: "/d/set".into(), remove: olds.clone(), requirement, ..Default::default() },
                    _ => DatasetChange {
                        dataset: "/d/set".into(),
                        replace: olds.iter().zip(news.iter()).map(|(o, n)| (*o, Member::version(*n))).collect(),
                        requirement,
                        ..Default::default()
                    },
                };
                match commit(&s, ch).await {
                    Ok(c) => return Ok::<_, Error>((attempts, conflicts, gone_errors, Some(c), olds)),
                    Err(Error::Conflict { details, .. }) => {
                        conflicts += 1;
                        requirement = details["current"].as_str().and_then(|x| x.parse().ok());
                    }
                    Err(Error::Refused { code: "member-not-found", details, message }) => {
                        gone_errors.push(message);
                        let m: Uuid = details["member"].as_str().and_then(|x| x.parse().ok()).unwrap_or_default();
                        olds.retain(|x| *x != m);
                        if olds.is_empty() {
                            return Ok((attempts, conflicts, gone_errors, None, olds));
                        }
                        // the requirement stays: the client tries again on the same snapshot
                        let r: Uuid = details["snapshot"].as_str().and_then(|x| x.parse().ok()).unwrap_or(s0);
                        requirement = Some(r);
                    }
                    Err(e) => return Err(e),
                }
                if attempts > 200 {
                    return Err(Error::invalid("too many retries"));
                }
            }
        }));
    }
    let mut results = Vec::new();
    for h in handles {
        results.push(h.await.map_err(|e| Error::invalid(e.to_string()))??);
    }
    ctx.step().await?;
    let successes: Vec<_> = results.iter().filter_map(|r| r.3.clone()).collect();
    let all_first = results.iter().all(|r| r.0 == 1 && r.1 == 0);
    ctx.outcome(if all_first { "all-succeed-first-time" } else { "conflicts-then-retries" });

    // the change stream
    crate::stream::flush(&admin).await?;
    let poll = crate::stream::poll(&admin, start_pos, 100_000, None, false).await?;
    let commits: Vec<&serde_json::Value> = poll.events.iter().filter(|e| e["type"] == "mudcat.snapshot.committed.v1" && e["data"]["path"] == "/d/set").collect();
    let mut chain_ok = true;
    let mut prev = s0.to_string();
    for e in &commits {
        if e["data"]["parent"].as_str() != Some(prev.as_str()) {
            chain_ok = false;
        }
        prev = e["subject"].as_str().unwrap_or("").to_string();
    }
    ctx.check("one snapshot-committed event for each successful commit", commits.len() == successes.len() && chain_ok, format!("{} events, {} commits, chain {chain_ok}", commits.len(), successes.len()));
    let last = prev.parse::<Uuid>().unwrap_or(s0);
    let (first_m, last_m, snaps): (BTreeSet<Uuid>, Vec<Uuid>, i64) = in_tx!(admin, |tx| {
        let a = datasets::members(&tx, s0).await?.iter().map(|e| e.member_id).collect();
        let b = datasets::members(&tx, last).await?.iter().map(|e| e.member_id).collect();
        let n: i64 = tx.q1("select count(*) from snapshot where created_commit > (select created_commit from snapshot where id = $1)", &[&s0]).await?.get(0);
        (a, b, n)
    });
    let last_set: BTreeSet<Uuid> = last_m.iter().copied().collect();
    let added: i64 = commits.iter().map(|e| e["data"]["summary"]["added"].as_i64().unwrap_or(0)).sum();
    let removed: i64 = commits.iter().map(|e| e["data"]["summary"]["removed"].as_i64().unwrap_or(0)).sum();
    let diff_add = last_set.difference(&first_m).count() as i64;
    let diff_rem = first_m.difference(&last_set).count() as i64;
    ctx.check("sum of the change summaries", added == diff_add && removed == diff_rem, format!("summaries +{added} -{removed}, difference +{diff_add} -{diff_rem}"));
    ctx.check("succeeds with no retry by the client", all_first && successes.len() == writers, format!("{} first-time successes of {writers}", results.iter().filter(|r| r.0 == 1).count()));
    let union: BTreeSet<Uuid> = plan.iter().flat_map(|p| p.1.iter().copied()).collect();
    ctx.check("union of all added members", union.is_subset(&last_set), "");
    ctx.check("is in the last snapshot one time", last_m.len() == last_set.len(), format!("{} members, {} distinct", last_m.len(), last_set.len()));
    let conflicts: usize = results.iter().filter(|r| r.1 > 0).count();
    ctx.check("gets a conflict, and no partial change", conflicts == writers - 1 && snaps == successes.len() as i64, format!("{conflicts} writers had a conflict; {snaps} new snapshots for {} commits", successes.len()));
    // sequential application in commit order
    let mut expected = first_m.clone();
    for c in &commits {
        for m in c["data"]["removed_members"].as_array().cloned().unwrap_or_default() {
            expected.remove(&m.as_str().unwrap_or("").parse::<Uuid>().unwrap_or_default());
        }
        for m in c["data"]["added_members"].as_array().cloned().unwrap_or_default() {
            expected.insert(m.as_str().unwrap_or("").parse::<Uuid>().unwrap_or_default());
        }
    }
    let intended_removed: BTreeSet<Uuid> = plan.iter().flat_map(|p| p.0.iter().copied()).collect();
    ctx.check(
        "the result of the commits applied one after the other",
        expected == last_set && intended_removed.is_disjoint(&last_set),
        format!("{} members; all removed members gone: {}", last_set.len(), intended_removed.is_disjoint(&last_set)),
    );
    let gone = results.iter().filter(|r| !r.2.is_empty()).count();
    let names_member = results.iter().flat_map(|r| r.2.iter()).all(|m| m.contains("member") && m.contains('-'));
    ctx.check("already gone gets an error that names the member", gone > 0 && names_member, format!("{gone} writers got such an error"));
    ctx.step().await?;
    Ok(())
}

/// T1.3: a complete and verified copy near the compute cluster, then a resolution there.
pub async fn stage_near_compute(ctx: &mut Ctx) -> Result<()> {
    let n = ctx.n("members") as usize;
    let fault = ctx.p("fault");
    let strict = ctx.is("resolve", "only-at-cluster");
    let admin = ctx.admin.clone();
    let far = resource(&admin, "far", "cloud-a", "managed", false).await?;
    let target_mode = if ctx.is("target", "managed-cache") { "managed" } else { "external" };
    resource(&admin, "near", "cloud-b", target_mode, false).await?;
    acl_root(&admin, "/v", Some(("far", "v/"))).await?;
    grant_collection(&admin, "/v", "reader", "user:trainer").await?;
    source(&admin, "src", "instrument", json!({}), &[("train", "research")]).await?;
    let files: Vec<(String, Vec<u8>)> = (0..n).map(|i| (format!("c{i:06}.bin"), file_bytes("opaque", i, "far"))).collect();
    let regs = register_files(&admin, &far, "/v", "v/", &files, Some("src"), None).await?;
    verify_all(&admin, "far").await?;
    dataset(&admin, "/v/clips", regs.iter().map(|r| Member::version(r.version_id)).collect()).await?;
    let snap = publish(ctx, &admin, "/v/clips", "v1").await?;
    ctx.step().await?;
    in_tx!(admin, |tx| placement::add_rule(&mut tx, snap.id, &["near".to_string()], 1, Some(chrono::Duration::days(14))).await?);
    let mf = match fault.as_str() {
        "corrupt-some" => MoverFault { corrupt: 3, stop_after: None },
        "mover-stops" => MoverFault { corrupt: 0, stop_after: Some(n / 2) },
        _ => MoverFault::default(),
    };
    placement::run_mover(&admin, &mf).await?;
    ctx.step().await?;
    let a = SiteAgent::new("agent-a", "near");
    let b = SiteAgent::new("agent-b", "near");
    if fault == "agent-stops" {
        a.run(&admin, 1_000_000, AgentFault::StopsAfterLease).await?;
        // before the lease ends, nobody else takes the tasks
        let early = b.run(&admin, 1_000_000, AgentFault::None).await?;
        ctx.inst.clock.advance(ctx.inst.config.lease + chrono::Duration::minutes(1));
        let mut done = 0;
        loop {
            let k = b.run(&admin, 5000, AgentFault::None).await?;
            done += k;
            if k == 0 {
                break;
            }
        }
        let (moved, total): (i64, i64) = in_tx!(admin, |tx| {
            let r = tx.q1("select count(*) filter (where lease_owner = 'agent-b' and attempts >= 2 and state = 'done'), count(*) from agent_task where kind = 'verify' and resource_id = (select id from storage_resource where name = 'near')", &[]).await?;
            (r.get(0), r.get(1))
        });
        ctx.check("gave the task of the stopped agent to another agent", early == 0 && moved == total && done as i64 == total, format!("{moved} of {total} tasks done by agent-b after the lease; {early} before it ended"));
    } else {
        loop {
            if a.run(&admin, 5000, AgentFault::None).await? == 0 {
                break;
            }
        }
    }
    ctx.step().await?;

    // the completeness record against the replica states
    let (complete, verified, total, actual): (bool, i64, i64, i64) = in_tx!(admin, |tx| {
        let near = namespace::resource(&tx, "near").await?.id;
        let c = placement::refresh_completeness(&mut tx, snap.id, near).await?;
        let actual: i64 = tx
            .q1("select count(distinct version_id) from replica where resource_id = (select id from storage_resource where name='near') and state = 'verified'", &[])
            .await?
            .get(0);
        (c["complete"].as_bool().unwrap_or(false), c["verified"].as_i64().unwrap_or(0), c["total"].as_i64().unwrap_or(0), actual)
    });
    ctx.check("agrees with the states of the replicas", verified == actual && complete == (verified == total), format!("record {verified}/{total} complete={complete}; replicas {actual}"));
    let bad_verified: i64 = in_tx!(admin, |tx| tx
        .q1("select count(*) from replica r join version v on v.id = r.version_id and v.tenant_id = r.tenant_id where r.state = 'verified' and not exists (select 1 from verification x where x.replica_id = r.id and x.ok and x.observed = v.content_hash)", &[])
        .await?
        .get(0));
    ctx.check("without a successful comparison of checksums", bad_verified == 0, format!("{bad_verified}"));
    ctx.check("the rule stays not satisfied", !complete, format!("complete={complete}"));

    let trainer = ctx.session("user:trainer");
    let mut req = train_request(vec![resolver::Input { snapshot: Some(snap.id), role: "training".into(), ..Default::default() }], "train", "research");
    req.place = Some("cloud-b".into());
    req.place_mode = Some(if strict { "strict" } else { "prefer" }.into());
    let r = resolve(ctx, &trainer, &req).await;
    let outcome = match &r {
        Ok(res) => {
            let near_only = res.tasks.iter().all(|t| t.resource == "near");
            if near_only && complete {
                if fault == "agent-stops" { "complete-after-lease-ends" } else { "complete" }
            } else {
                "served-from-two-places"
            }
        }
        Err(e) if e.code() == "not-available-there" => "not-available-there",
        Err(_) => "error",
    };
    ctx.outcome(outcome);
    let missing = total - verified;
    match &r {
        Ok(res) => {
            ctx.check("The plan uses only replicas on the target", res.tasks.iter().all(|t| t.resource == "near"), "");
            let verified_near: HashSet<Uuid> = in_tx!(admin, |tx| tx
                .q("select version_id from replica where resource_id = (select id from storage_resource where name='near') and state = 'verified'", &[])
                .await?
                .iter()
                .map(|r| r.get(0))
                .collect());
            let ok = res.tasks.iter().all(|t| if verified_near.contains(&t.version) { t.resource == "near" } else { t.resource == "far" });
            let far_tasks = res.tasks.iter().filter(|t| t.resource == "far").count() as i64;
            ctx.check("uses the far replica for each member", ok && far_tasks == missing, format!("{far_tasks} far tasks for {missing} missing"));
            let states: BTreeMap<Uuid, String> = in_tx!(admin, |tx| tx
                .q("select id, state from replica", &[])
                .await?
                .iter()
                .map(|r| (r.get(0), r.get(1)))
                .collect());
            ctx.check("no replica with the state suspect or copying", res.tasks.iter().all(|t| states.get(&t.replica).map(|s| s == "verified").unwrap_or(false)), "");
        }
        Err(e) => {
            let n_na = e.details()["not_available"].as_i64().unwrap_or(-1);
            ctx.check("mudcat issues no plan", e.code() == "not-available-there" && n_na == missing, format!("{e}"));
        }
    }
    let _ = Principal::system();
    ctx.step().await?;
    Ok(())
}
