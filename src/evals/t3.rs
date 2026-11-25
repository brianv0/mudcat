//! Drivers for the task templates of use case 3: rights and removal for training content.

use super::fixtures::*;
use super::Ctx;
use crate::datasets::{self, DatasetChange, Member};
use crate::error::Result;
use crate::in_tx;
use crate::instance::EVALUATOR_VERSION;
use crate::namespace;
use crate::policy::{self, SourceSpec, Terms};
use crate::provenance::{self, ModelSpec, RunSpec};
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet};
use uuid::Uuid;

/// T3.1: get data for an action and a purpose; permitted only when every source permits it.
pub async fn policy_gate(ctx: &mut Ctx) -> Result<()> {
    let action = ctx.p("action");
    let purpose = ctx.p("purpose");
    let state = ctx.p("sources");
    let permitted_only = ctx.is("scope", "permitted-sources-only");
    let admin = ctx.admin.clone();
    let store = resource(&admin, "store", "cloud-a", "managed", false).await?;
    acl_root(&admin, "/corpus", Some(("store", "corpus/"))).await?;
    grant_collection(&admin, "/corpus", "reader", "user:trainer").await?;
    let all = [("train", "research"), ("train", "commercial"), ("evaluate", "research"), ("evaluate", "commercial")];
    source(&admin, "news-licence", "licensed", json!({"licensor": "News Co"}), &all).await?;
    source(&admin, "books", "licensed", json!({"licensor": "Books Ltd"}), &all).await?;
    let third = "partner-x";
    match state.as_str() {
        "all-permit" => {
            source(&admin, third, "licensed", json!({}), &all).await?;
        }
        "one-lacks-purpose" => {
            source(&admin, third, "crawl", json!({}), &[("train", "research"), ("evaluate", "research")]).await?;
        }
        "one-lacks-action" => {
            source(&admin, third, "user", json!({}), &[("evaluate", "research"), ("evaluate", "commercial")]).await?;
        }
        "one-expired" => {
            let id = source(&admin, third, "licensed", json!({}), &[]).await?;
            let ended = ctx.inst.now() - chrono::Duration::days(1);
            in_tx!(admin, |tx| for (a, p) in all {
                policy::grant_usage(&mut tx, id, a, p, Some(ended), Some("contract clause 4"), None).await?;
            });
        }
        _ => {
            let id = source(&admin, third, "licensed", json!({}), &[]).await?;
            let doc = json!({"uid": "urn:policy:partner-x", "permission": [
                {"uid": "clause-7", "action": "train", "constraint": [{"leftOperand": "spatial", "operator": "eq", "rightOperand": "approved-region"}]},
                {"uid": "clause-8", "action": "evaluate", "constraint": [{"leftOperand": "industryContext", "operator": "eq", "rightOperand": "research-only-partner"}]}]});
            in_tx!(admin, |tx| policy::set_terms(&mut tx, id, &Terms::Odrl { document: doc }).await?);
        }
    }
    let mut members = Vec::new();
    let mut elements_third = 0i64;
    for (si, src) in ["news-licence", "books", third].iter().enumerate() {
        let files: Vec<(String, Vec<u8>)> = (0..8).map(|i| (format!("{src}-{i}.jsonl"), file_bytes("records", si * 100 + i, src))).collect();
        let regs = register_files(&admin, &store, "/corpus", "corpus/", &files, Some(src), None).await?;
        if *src == third {
            elements_third = 8 * 3;
        }
        members.extend(regs.iter().map(|r| Member::version(r.version_id)));
    }
    verify_all(&admin, "store").await?;
    dataset(&admin, "/corpus/set", members).await?;
    publish(ctx, &admin, "/corpus/set", "v1").await?;
    ctx.step().await?;
    let creds_before: i64 = in_tx!(admin, |tx| tx.q1("select count(*) from credential_log", &[]).await?.get(0));
    let res_before: i64 = in_tx!(admin, |tx| tx.q1("select count(*) from resolution", &[]).await?.get(0));
    let deny_before = audit_count(&admin, "resolution", "deny").await?;
    let trainer = ctx.session("user:trainer");
    let mut req = train_request(vec![dataset_input("/corpus/set", Some("v1"), "training")], &action, &purpose);
    if permitted_only {
        req.scope = Some("permitted-sources-only".into());
    }
    let r = resolve(ctx, &trainer, &req).await;
    ctx.step().await?;
    let creds_after: i64 = in_tx!(admin, |tx| tx.q1("select count(*) from credential_log", &[]).await?.get(0));
    let res_after: i64 = in_tx!(admin, |tx| tx.q1("select count(*) from resolution", &[]).await?.get(0));
    let deny_after = audit_count(&admin, "resolution", "deny").await?;
    let third_permits = match state.as_str() {
        "all-permit" => true,
        "one-lacks-purpose" => purpose == "research",
        "one-lacks-action" => action == "evaluate",
        _ => false,
    };
    match &r {
        Ok(res) => {
            ctx.outcome("permitted");
            let d = &res.decision;
            ctx.check("a policy decision with the policies", d["policies"].as_array().map(|p| p.len() == 3).unwrap_or(false) && d["evaluator"] == EVALUATOR_VERSION && d["outcome"] == "permitted", "");
            ctx.check("issued a plan and credentials", !res.tasks.is_empty() && !res.credentials.is_empty(), "");
            let left_out = d["excluded"]["policy_elements"].as_i64().unwrap_or(-1);
            let expected = if third_permits { 0 } else { elements_third };
            let third_id = in_tx!(admin, |tx| policy::source_id(&tx, third).await?);
            let no_third = third_permits || res.tasks.iter().all(|t| t.source != Some(third_id));
            ctx.check("no element of a source that does not permit", no_third && left_out == expected, format!("left out {left_out}, expected {expected}"));
        }
        Err(e) => {
            ctx.outcome(match e.code() {
                "policy-refused" => "refused",
                "blocked-for-review" => "blocked-for-review",
                _ => "error",
            });
            let m = e.to_string();
            ctx.check("names the source that does not permit", m.contains(third) && m.contains(&format!("{elements_third} elements")), m.clone());
            ctx.check("names the clause that needs a review", m.contains("clause-7") || m.contains("clause-8"), m.clone());
        }
    }
    ctx.check("issued no plan and no credentials", creds_after == creds_before && res_after == res_before, format!("credentials {creds_before}->{creds_after}"));
    ctx.check("The audit log has the decision", deny_after == deny_before + 1, format!("{deny_before}->{deny_after}"));
    Ok(())
}

/// T3.2: remove content at one of five levels, with the effect of that level and no other.
pub async fn removal(ctx: &mut Ctx) -> Result<()> {
    let level = ctx.p("level");
    let hold = ctx.is("hold", "active");
    let point = ctx.p("point");
    let versioned = ctx.is("storage", "external-with-versions");
    let admin = ctx.admin.clone();
    let mode = if versioned { "external" } else { "managed" };
    let r1 = resource(&admin, "store-1", "cloud-a", mode, versioned).await?;
    let r2 = resource(&admin, "store-2", "cloud-a", mode, versioned).await?;
    acl_root(&admin, "/r", Some(("store-1", "r/"))).await?;
    grant_collection(&admin, "/r", "reader", "user:trainer").await?;
    source(&admin, "src", "licensed", json!({}), &[("train", "research")]).await?;
    // the content C, the distractor D from the same source, and other members
    let mut files: Vec<(String, Vec<u8>)> = vec![("C.bin".into(), b"the content to remove".to_vec()), ("D.bin".into(), b"other content of the same source".to_vec())];
    files.extend((0..6).map(|i| (format!("o{i}.bin"), file_bytes("opaque", i, "r"))));
    if versioned {
        // the storage keeps old object versions of the content
        admin.inst.store.put("producer", &r1.bucket, "r/C.bin", b"an older object version")?;
        admin.inst.store.put("producer", &r2.bucket, "r/C.bin", b"an older object version")?;
    }
    let regs = register_files(&admin, &r1, "/r", "r/", &files, Some("src"), None).await?;
    let c = regs[0].version_id;
    let d = regs[1].version_id;
    // with a hold: a version of the same source that is in no dataset, so that no hold covers it. The
    // request names it before C (a request for more than one version: the hold of the second counts)
    let free = if hold { Some(register_files(&admin, &r1, "/r", "r/", &[("F.bin".to_string(), b"a version that no hold covers".to_vec())], Some("src"), None).await?[0].version_id) } else { None };
    // a second replica of C
    admin.inst.store.put("producer", &r2.bucket, "r/C.bin", &files[0].1)?;
    in_tx!(admin, |tx| namespace::add_replica(&mut tx, c, &r2, &r2.url("r/C.bin"), "registered", None, None).await?);
    verify_all(&admin, "store-1").await?;
    verify_all(&admin, "store-2").await?;
    let members: Vec<Member> = regs.iter().map(|r| Member::version(r.version_id)).collect();
    dataset(&admin, "/r/a", members.clone()).await?;
    dataset(&admin, "/r/b", members).await?;
    let pa = publish(ctx, &admin, "/r/a", "v1").await?;
    let pb = publish(ctx, &admin, "/r/b", "v1").await?;
    let hold_id = if hold {
        let did = in_tx!(admin, |tx| datasets::dataset_id(&tx, "/r/a").await?);
        ctx.record.holds.push(c);
        Some(in_tx!(admin, |tx| policy::place_hold(&mut tx, "litigation 22-cv-1", "preservation order in case 22-cv-1", &[did]).await?))
    } else {
        None
    };
    let trainer = ctx.session("user:trainer");
    let req = train_request(vec![dataset_input("/r/a", Some("v1"), "training")], "train", "research");
    let mut old_res = None;
    let mut model = None;
    if point != "before-any-use" {
        let r = resolve(ctx, &trainer, &req).await?;
        if point == "after-training" {
            let act = in_tx!(admin, |tx| provenance::record_run(&mut tx, &RunSpec { kind: "training".into(), resolution: Some(r.id), ..Default::default() }).await?);
            model = Some(in_tx!(admin, |tx| provenance::publish_model(&mut tx, &ModelSpec { path: "/models/m".into(), content_hash: "weights-1".into(), size: 1, activity: Some(act), ..Default::default() }).await?));
        }
        old_res = Some(r);
    }
    ctx.step().await?;
    let start_pos: i64 = { crate::stream::flush(&admin).await?; in_tx!(admin, |tx| tx.q1("select last_position from stream_state", &[]).await?.get(0)) };
    let counsel = ctx.admin.clone();
    let mut blocked: Option<Value> = None;
    let mut op_ok = true;
    match level.as_str() {
        "remove-from-dataset" => {
            let head = in_tx!(admin, |tx| datasets::resolve_ref(&tx, datasets::dataset_id(&tx, "/r/a").await?, "main", None).await?);
            commit(&counsel, DatasetChange { dataset: "/r/a".into(), requirement: Some(head), remove: vec![c], ..Default::default() }).await?;
        }
        "delete-marker" => {
            in_tx!(counsel, |tx| namespace::delete_artifact(&mut tx, "/r/C.bin").await?);
        }
        "invalidate" => {
            in_tx!(counsel, |tx| policy::invalidate(&mut tx, "version", &c.to_string(), "takedown request 4711", "rights holder").await?);
        }
        "purge" | "erase" => {
            let named: Vec<Uuid> = free.into_iter().chain([c]).collect();
            let out = if level == "purge" {
                in_tx!(counsel, |tx| policy::purge(&mut tx, &named, "takedown request 4711").await?)
            } else {
                in_tx!(counsel, |tx| policy::erase(&mut tx, &named, "erasure request", false).await?)
            };
            blocked = out.blocked_by_hold.clone();
            run_agent(&admin, "store-1").await?;
            run_agent(&admin, "store-2").await?;
        }
        _ => op_ok = false,
    }
    ctx.step().await?;

    // what remains
    let (version_exists, erasure, receipts, latest_is_marker): (bool, bool, i64, bool) = in_tx!(admin, |tx| {
        let v = tx.qopt("select 1 from version where id = $1", &[&c]).await?.is_some();
        let e = tx.qopt("select 1 from erasure_record where id = $1 and reason <> '' and erased_at is not null", &[&c]).await?.is_some();
        let r: i64 = tx.q1("select count(*) from deletion_receipt where version_id = $1", &[&c]).await?.get(0);
        let m: bool = tx.q1("select coalesce((select v.is_delete_marker from artifact a join version v on v.id = a.latest_version_id where a.name = 'C.bin'), false)", &[]).await?.get(0);
        (v, e, r, m)
    });
    let bytes_exist = admin.inst.store.exists(&r1.bucket, "r/C.bin") && admin.inst.store.exists(&r2.bucket, "r/C.bin");
    let old_versions = admin.inst.store.object_versions(&r1.bucket, "r/C.bin") + admin.inst.store.object_versions(&r2.bucket, "r/C.bin");

    // resolution of every snapshot, old and new, by a normal user
    let mut c_resolves = false;
    let mut d_resolves = true;
    for (ds, r) in [("/r/a", "v1"), ("/r/b", "v1"), ("/r/a", "main"), ("/r/b", "main")] {
        let q = train_request(vec![dataset_input(ds, Some(r), "training")], "train", "research");
        let res = resolve(ctx, &trainer, &q).await?;
        let vs: BTreeSet<Uuid> = res.tasks.iter().map(|t| t.version).collect();
        if vs.contains(&c) && r == "v1" {
            c_resolves = true;
        }
        if vs.contains(&c) && r == "main" && level == "remove-from-dataset" && ds == "/r/a" {
            c_resolves = true;
        }
        if r == "v1" && !vs.contains(&d) {
            d_resolves = false;
        }
    }
    let head_a = in_tx!(admin, |tx| {
        let h = datasets::resolve_ref(&tx, datasets::dataset_id(&tx, "/r/a").await?, "main", None).await?;
        datasets::members(&tx, h).await?.iter().map(|e| e.member_id).collect::<BTreeSet<Uuid>>()
    });
    let outcome = match level.as_str() {
        "remove-from-dataset" | "delete-marker" if c_resolves => "forward-only",
        "invalidate" if !c_resolves && bytes_exist => "excluded",
        "purge" | "erase" if blocked.is_some() => "blocked-by-hold",
        "purge" if receipts == 2 && version_exists => "bytes-gone",
        "erase" if !version_exists && erasure => "record-gone",
        _ => "unexpected",
    };
    ctx.outcome(outcome);

    let pa_now = in_tx!(admin, |tx| (datasets::members(&tx, pa.id).await?.len(), datasets::snapshot(&tx, pa.id).await?.merkle_hash));
    let pb_now = in_tx!(admin, |tx| (datasets::members(&tx, pb.id).await?.len(), datasets::snapshot(&tx, pb.id).await?.merkle_hash));
    ctx.check("keep their members list and their Merkle hash", pa_now == (8, pa.merkle_hash.clone()) && pb_now == (8, pb.merkle_hash.clone()), format!("{pa_now:?}"));
    ctx.check("other content from the same source resolves as before", d_resolves, "");
    let newest_lacks = match level.as_str() {
        "remove-from-dataset" => !head_a.contains(&c),
        _ => latest_is_marker,
    };
    ctx.check("The newest state does not contain the content", newest_lacks && c_resolves, format!("newest lacks: {newest_lacks}; old resolves: {c_resolves}"));
    let flags = match model {
        Some(m) => in_tx!(admin, |tx| policy::flags_of(&tx, m).await?),
        None => vec![],
    };
    ctx.check("The model has no flag", flags.is_empty(), format!("{flags:?}"));
    ctx.check("The hold does not block the request, and the bytes exist", op_ok && bytes_exist, "");
    ctx.check("does not resolve from any snapshot, old or new", !c_resolves, "");
    if let Some(r) = &old_res {
        let again = resolve_again(ctx, &trainer, r.id).await?;
        let newly: Vec<String> = again.decision["newly_excluded"].as_array().cloned().unwrap_or_default().iter().filter_map(|v| v.as_str().map(|s| s.to_string())).collect();
        ctx.check("A new run of the old resolution reports the content as excluded", newly.contains(&c.to_string()), format!("{newly:?}"));
    }
    if let Some(m) = model {
        let hash = in_tx!(admin, |tx| namespace::version(&tx, m).await?.content_hash);
        ctx.check("The model has a flag with the number of affected elements", flags.iter().any(|f| f["affected_elements"] == 1) && hash.as_deref() == Some("weights-1"), format!("{flags:?}"));
    }
    ctx.check("Each replica has a deletion receipt, and the catalog record", receipts == 2 && version_exists, format!("{receipts} receipts"));
    ctx.check("The old object versions on the storage are deleted also", old_versions == 0, format!("{old_versions} object versions left"));
    crate::stream::flush(&admin).await?;
    let poll = crate::stream::poll(&admin, start_pos, 10_000, None, true).await?;
    let removal_event = poll.events.iter().any(|d| d["id"] == json!(c) && d["removed"] == true);
    ctx.check("Only an erasure record stays, and the state stream has a removal event", !version_exists && erasure && removal_event, format!("removal event: {removal_event}"));
    if hold && matches!(level.as_str(), "purge" | "erase") {
        let b = blocked.clone().unwrap_or(Value::Null);
        ctx.check("The bytes exist, and the answer names the hold and its reason", bytes_exist && b["name"] == "litigation 22-cv-1" && b["reason"].as_str().map(|r| r.contains("22-cv-1")).unwrap_or(false), format!("bytes exist: {bytes_exist}; blocked by: {b}"));
    }
    if let Some(_b) = &blocked {
        let comps = in_tx!(admin, |tx| crate::authz::compartments_of(&tx, "version", c).await?);
        // and the access: a reader who is not a member of the compartment is denied the version, a member is allowed
        let outsider = in_tx!(trainer, |tx| crate::authz::check(&tx, "read", "version", c).await?);
        let member = in_tx!(admin, |tx| crate::authz::check(&tx, "read", "version", c).await?);
        ctx.check(
            "in the retention compartment",
            hold_id.map(|h| comps.contains(&format!("retention-{h}"))).unwrap_or(false) && !outsider.allow && member.allow,
            format!("{comps:?}; reader outside the compartment: {} ({}); member: {} ({})", outsider.allow, outsider.reason, member.allow, member.reason),
        );
    }
    let _ = BTreeMap::<u8, u8>::new();
    Ok(())
}

/// T3.3: make the summary of training content for a model, from the catalog only.
pub async fn content_summary(ctx: &mut Ctx) -> Result<()> {
    let kinds_p = ctx.p("kinds");
    let base_models = ctx.params["base_models"].as_i64().unwrap_or(0) as usize;
    let gap = ctx.is("records", "one-run-has-no-resolution");
    let n_sources = ctx.n("sources").max(4) as usize;
    let kinds: Vec<&str> = match kinds_p.as_str() {
        "crawl-only" => vec!["crawl"],
        "crawl-and-licensed" => vec!["crawl", "licensed"],
        _ => vec!["crawl", "licensed", "user", "generated"],
    };
    let admin = ctx.admin.clone();
    let store = resource(&admin, "store", "cloud-a", "managed", false).await?;
    acl_root(&admin, "/c", Some(("store", "c/"))).await?;
    grant_collection(&admin, "/c", "reader", "user:trainer").await?;
    // one dataset for the model and one for each base model; each with its own sources
    let mut all_resolutions = Vec::new();
    let mut models: Vec<Uuid> = Vec::new();
    let mut source_ids_by_model: Vec<BTreeSet<Uuid>> = Vec::new();
    for level in 0..=base_models {
        // level 0 is the deepest base model when base_models > 0; the last one is the model
        let n = if level == base_models { n_sources } else { 4 };
        let mut members = Vec::new();
        let mut ids = BTreeSet::new();
        let mut files_all = Vec::new();
        for s in 0..n {
            let kind = kinds[s % kinds.len()];
            let name = format!("m{level}-src{s:03}");
            let attrs = match kind {
                "crawl" => json!({"domain": format!("site{}.example", s % 7), "crawler": format!("mudbot/{}", 1 + s % 2), "fetch_from": "2025-01-01", "fetch_to": "2025-06-30"}),
                "licensed" => json!({"licensor": format!("Licensor {s}")}),
                "user" => json!({"consent_scope": "product improvement", "legal_basis": "dpv:Consent"}),
                _ => json!({"model": "gen-model-7", "prompt_version": "p3"}),
            };
            let id = source(&admin, &name, kind, attrs, &[("train", "research")]).await?;
            if kind == "licensed" {
                in_tx!(admin, |tx| policy::set_terms(&mut tx, id, &Terms::Spdx { id: "CC-BY-4.0".into() }).await?);
            }
            ids.insert(id);
            for f in 0..2 {
                files_all.push((format!("m{level}-s{s:03}-{f}.jsonl"), file_bytes("records", s * 10 + f, &name), name.clone()));
            }
        }
        for chunk in files_all.chunks(500) {
            for src in chunk.iter().map(|x| x.2.clone()).collect::<BTreeSet<_>>() {
                let fs: Vec<(String, Vec<u8>)> = chunk.iter().filter(|x| x.2 == src).map(|x| (x.0.clone(), x.1.clone())).collect();
                let regs = register_files(&admin, &store, "/c", "c/", &fs, Some(&src), None).await?;
                members.extend(regs.iter().map(|r| Member::version(r.version_id)));
            }
        }
        verify_all(&admin, "store").await?;
        let path = format!("/c/train-{level}");
        dataset(&admin, &path, members).await?;
        let trainer = ctx.session("user:trainer");
        let r = resolve(ctx, &trainer, &train_request(vec![dataset_input(&path, Some("main"), "training")], "train", "research")).await?;
        all_resolutions.push(r.clone());
        let act = in_tx!(admin, |tx| provenance::record_run(&mut tx, &RunSpec { kind: "training".into(), resolution: Some(r.id), ..Default::default() }).await?);
        let derived = models.last().map(|b| vec![(*b, "fineTunedFrom".to_string())]).unwrap_or_default();
        let m = in_tx!(admin, |tx| provenance::publish_model(&mut tx, &ModelSpec { path: format!("/models/m{level}"), content_hash: format!("w{level}"), size: 1, activity: Some(act), derived_from: derived, ..Default::default() }).await?);
        models.push(m);
        source_ids_by_model.push(ids);
    }
    let model = *models.last().unwrap();
    let mut gap_run = None;
    if gap {
        // one more training run of the model, recorded with no resolution
        let act = in_tx!(admin, |tx| provenance::record_run(&mut tx, &RunSpec { kind: "training".into(), code: Some("git:untracked".into()), ..Default::default() }).await?);
        in_tx!(admin, |tx| tx.exec("insert into generation (activity_id, entity_id) values ($1,$2)", &[&act, &model]).await?);
        gap_run = Some(act);
    }
    ctx.step().await?;
    let read_before = ctx.inst.metrics.bytes_read_for_catalog.load(std::sync::atomic::Ordering::SeqCst);
    let gets_before = ctx.inst.store.accesses().iter().filter(|a| a.op == "get").count();
    let counsel = ctx.admin.clone();
    let report = in_tx!(counsel, |tx| provenance::content_summary(&tx, model).await?);
    let read_after = ctx.inst.metrics.bytes_read_for_catalog.load(std::sync::atomic::Ordering::SeqCst);
    let gets_after = ctx.inst.store.accesses().iter().filter(|a| a.op == "get").count();
    ctx.outcome(if report["complete"] == true { "report" } else { "report-states-gap" });
    let sections: BTreeSet<String> = report["sections"].as_object().map(|o| o.keys().cloned().collect()).unwrap_or_default();
    let want: BTreeSet<String> = kinds.iter().map(|k| k.to_string()).collect();
    ctx.check("one section for each kind of source", sections == want, format!("{sections:?}"));
    let report_total: i64 = report["elements_by_source"].as_array().cloned().unwrap_or_default().iter().map(|e| e["elements"].as_i64().unwrap_or(0)).sum();
    let res_total: i64 = all_resolutions.iter().map(|r| r.total_elements()).sum();
    ctx.check("agree with the numbers in the resolutions", report_total == res_total, format!("report {report_total}, resolutions {res_total}"));
    ctx.check("read no training data", read_after == read_before && gets_after == gets_before && report["read_training_data"] == false, "");
    let crawl = &report["sections"]["crawl"];
    let doms: Vec<i64> = crawl["domains_by_size"].as_array().cloned().unwrap_or_default().iter().map(|d| d["bytes"].as_i64().unwrap_or(0)).collect();
    let sorted = doms.windows(2).all(|w| w[0] >= w[1]);
    let crawl_sources: usize = crawl["sources"].as_u64().unwrap_or(0) as usize;
    ctx.check("lists domains by size, crawler names and fetch periods", !doms.is_empty() && sorted && !crawl["crawlers"].as_array().map(|c| c.is_empty()).unwrap_or(true) && crawl["fetch_periods"].as_array().map(|p| p.len() == crawl_sources).unwrap_or(false), "");
    let lic = report["sections"]["licensed"]["licensors"].as_array().cloned().unwrap_or_default();
    ctx.check("names each licensor and its terms", !lic.is_empty() && lic.iter().all(|l| l["licensor"].is_string() && l["terms"].as_array().map(|t| !t.is_empty()).unwrap_or(false)), "");
    let in_report: BTreeSet<String> = report["elements_by_source"].as_array().cloned().unwrap_or_default().iter().filter_map(|e| e["source"].as_str().map(|s| s.to_string())).collect();
    let base_ok = source_ids_by_model[..base_models].iter().all(|ids| ids.iter().all(|i| in_report.contains(&i.to_string())));
    ctx.check("includes the sources of each of the", base_ok, "");
    if let Some(g) = gap_run {
        ctx.check("names the run that has no resolution", report["gaps"][0]["run"] == json!(g) && report["statement"].as_str().unwrap_or("").contains("not complete"), format!("{}", report["statement"]));
    }
    ctx.check("covers all recorded runs", report["statement"].as_str().unwrap_or("").contains("covers all recorded runs"), "");
    let _ = SourceSpec::default();
    Ok(())
}
