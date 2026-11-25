//! Drivers for the task templates of use case 2: training data for scientific models.

use super::fixtures::*;
use super::Ctx;
use crate::datasets::{self, DatasetChange, Member};
use crate::error::{Error, Result};
use crate::in_tx;
use crate::namespace::{self, Register};
use crate::policy::{self, SourceSpec};
use crate::provenance::{self, ModelSpec, RunSpec};
use crate::resolver;
use serde_json::{json, Value};
use std::collections::BTreeSet;
use uuid::Uuid;

/// T2.1: make and publish one training dataset from two datasets.
pub async fn combine_methods(ctx: &mut Ctx) -> Result<()> {
    let admin = ctx.admin.clone();
    let different = ctx.is("methods", "different");
    let ma = json!({"functional": "PBE", "hubbard_u": "no", "code": "vasp-6.4"});
    let mb = if different { json!({"functional": "PBE", "hubbard_u": "yes", "code": "vasp-6.4"}) } else { ma.clone() };
    in_tx!(admin, |tx| {
        policy::create_source(&mut tx, &SourceSpec { name: "group-a".into(), kind: "computed".into(), attrs: json!({}), method: Some(ma.clone()) }).await?;
        policy::create_source(&mut tx, &SourceSpec { name: "group-c".into(), kind: "computed".into(), attrs: json!({}), method: Some(mb.clone()) }).await?;
    });
    for (ds, src) in [("/p/a", "group-a"), ("/p/c", "group-c")] {
        let regs: Vec<Register> = (0..5)
            .map(|i| Register { path: format!("{ds}-files/s{i}"), content_hash: Some(crate::instance::sha256_hex(format!("{ds}{i}").as_bytes())), size: 10, source: Some(src.into()), ..Default::default() })
            .collect();
        let vs = in_tx!(admin, |tx| namespace::register_batch(&mut tx, &regs).await?);
        dataset(&admin, ds, vs.iter().map(|v| Member::version(v.version_id)).collect()).await?;
        publish(ctx, &admin, ds, "v1").await?;
    }
    let eq = if ctx.is("equivalence", "declared") {
        let lead = ctx.session("user:lead");
        Some(in_tx!(lead, |tx| datasets::declare_equivalence(&mut tx, &ma, &mb, Some("energy alignment, script git:def456"), "the energies were aligned").await?))
    } else {
        None
    };
    ctx.step().await?;
    let (sa, sc) = in_tx!(admin, |tx| {
        let a = datasets::resolve_ref(&tx, datasets::dataset_id(&tx, "/p/a").await?, "v1", None).await?;
        let c = datasets::resolve_ref(&tx, datasets::dataset_id(&tx, "/p/c").await?, "v1", None).await?;
        (a, c)
    });
    let researcher = admin.clone();
    let committed = in_tx!(researcher, |tx| {
        datasets::create_dataset(&mut tx, "/p/train", &Default::default()).await?;
        datasets::commit(&mut tx, &[DatasetChange { dataset: "/p/train".into(), add: vec![Member::snapshot(sa), Member::snapshot(sc)], ..Default::default() }]).await?.remove(0)
    });
    let warnings = committed.warnings.as_array().cloned().unwrap_or_default();
    let mut outcome = if warnings.is_empty() { "accepted" } else { "accepted-with-warning" };
    let mut snap_of_record = committed.snapshot;
    let mut refusal: Option<Error> = None;
    if ctx.is("step", "publish") {
        match in_tx!(researcher, |tx| datasets::publish(&mut tx, "/p/train", "main", "v1").await) {
            Ok(info) => {
                record_published(ctx, info.id).await?;
                snap_of_record = info.id;
                outcome = "accepted";
            }
            Err(e) => {
                outcome = if e.code() == "methods-differ" { "refused" } else { "error" };
                refusal = Some(e);
            }
        }
    }
    ctx.outcome(outcome);
    ctx.step().await?;
    if let Some(e) = &refusal {
        let m = e.to_string();
        ctx.check("names the two methods and the attribute", m.contains("hubbard_u") && m.contains("\"no\"") && m.contains("\"yes\""), m.clone());
    }
    let published: i64 = in_tx!(admin, |tx| tx.q1("select count(*) from snapshot s join dataset d on d.id = s.dataset_id where d.path = '/p/train' and s.status = 'published'", &[]).await?.get(0));
    ctx.check("no new published snapshot", published == 0, format!("{published}"));
    ctx.check("a warning that names the two methods", warnings.first().map(|w| w["methods"].as_array().map(|m| m.len() == 2).unwrap_or(false)).unwrap_or(false), format!("{warnings:?}"));
    let info = in_tx!(admin, |tx| datasets::snapshot(&tx, snap_of_record).await?);
    if let Some(eq) = eq {
        let row = in_tx!(admin, |tx| tx.q1("select declared_by, reason from equivalence where id = $1", &[&eq]).await?);
        let by: String = row.get(0);
        let reason: String = row.get(1);
        ctx.check("refers to the declared equivalence", info.equivalences.contains(&eq) && by == "user:lead" && !reason.is_empty(), format!("{:?}", info.equivalences));
    }
    ctx.check("The snapshot has no warning", info.warnings.as_array().map(|w| w.is_empty()).unwrap_or(true), "");
    Ok(())
}

async fn training_model(ctx: &mut Ctx, dataset_path: &str, model_path: &str) -> Result<(Uuid, resolver::Resolution, Uuid)> {
    let trainer = ctx.session("user:trainer");
    let req = train_request(vec![dataset_input(dataset_path, Some("main"), "training")], "train", "research");
    let r = resolve(ctx, &trainer, &req).await?;
    let admin = ctx.admin.clone();
    let act = in_tx!(admin, |tx| provenance::record_run(&mut tx, &RunSpec { kind: "training".into(), code: Some("git:abc123".into()), image: Some("sha256:feed".into()), params: json!({"seed": 7}), resolution: Some(r.id), ..Default::default() }).await?);
    let mv = in_tx!(admin, |tx| provenance::publish_model(&mut tx, &ModelSpec { path: model_path.into(), content_hash: crate::instance::sha256_hex(model_path.as_bytes()), size: 1000, activity: Some(act), ..Default::default() }).await?);
    Ok((mv, r, act))
}

/// T2.2: find out if test data is related to the training inputs of a model.
pub async fn leakage_check(ctx: &mut Ctx) -> Result<()> {
    let criterion = ctx.p("criterion");
    let related = ctx.params["related"].as_i64().unwrap_or(0) as usize;
    let admin = ctx.admin.clone();
    let store = resource(&admin, "store", "cloud-a", "managed", false).await?;
    acl_root(&admin, "/t", Some(("store", "t/"))).await?;
    grant_collection(&admin, "/t", "reader", "user:trainer").await?;
    source(&admin, "src", "computed", json!({}), &[("train", "research"), ("evaluate", "research")]).await?;
    in_tx!(admin, |tx| crate::leakage::register_criterion(&mut tx, "crit", &criterion, json!({"threshold": 0.9})).await?);
    // training data: 1000 structures; 40 pairs of them are related to each other (a distractor)
    let n_train = 1000;
    let key = |i: usize| format!("k-{i}");
    let train_key = |i: usize| if i >= 920 { key(i - 40) } else { key(i) };
    let mut files = Vec::new();
    let mut regs_meta = Vec::new();
    for i in 0..n_train {
        let k = train_key(i);
        let data = if criterion == "exact-checksum" && i >= 920 { format!("structure {}", i - 40) } else { format!("structure {i}") };
        files.push((format!("train/s{i:05}"), data.clone().into_bytes()));
        regs_meta.push((k, i));
    }
    let mut regs = Vec::new();
    for ((name, data), (k, i)) in files.iter().zip(regs_meta.iter()) {
        let skey = format!("t/{name}");
        admin.inst.store.put("producer", &store.bucket, &skey, data)?;
        let mut r = Register {
            path: format!("/t/{name}"),
            content_hash: Some(crate::instance::sha256_hex(data)),
            size: data.len() as i64,
            source: Some("src".into()),
            replica: Some(namespace::ReplicaSpec { resource: "store".into(), key: Some(skey), ..Default::default() }),
            stats: Some(json!({"time": [*i as f64 * 10.0, *i as f64 * 10.0 + 5.0]})),
            ..Default::default()
        };
        match criterion.as_str() {
            "near-duplicate" => r.fingerprint = Some(k.clone()),
            "domain-similarity" => r.domain_key = Some(k.clone()),
            _ => {}
        }
        regs.push(r);
    }
    let tv = in_tx!(admin, |tx| namespace::register_batch(&mut tx, &regs).await?);
    verify_all(&admin, "store").await?;
    dataset(&admin, "/t/training", tv.iter().map(|v| Member::version(v.version_id)).collect()).await?;
    let (model, _, _) = training_model(ctx, "/t/training", "/models/m1").await?;
    // evaluation datasets: the named one with {related} related elements, and a distractor with 900
    for (ds, n, rel, salt) in [("/t/eval", 10_000usize, related, "e"), ("/t/eval2", 1000usize, 900usize, "x")] {
        // the named dataset must hold the largest `related` value of the template (500)
        let n = ctx.scale.size(n, 2000, 600);
        let regs: Vec<Register> = (0..n)
            .map(|j| {
                let is_rel = j < rel;
                let content = if is_rel && criterion == "exact-checksum" { format!("structure {}", j % 900) } else { format!("{salt}-test {j}") };
                let mut r = Register { path: format!("{ds}-files/e{j:05}"), content_hash: Some(crate::instance::sha256_hex(content.as_bytes())), size: 10, source: Some("src".into()), ..Default::default() };
                let t0 = if is_rel { (j % 900) as f64 * 10.0 + 1.0 } else { 1.0e7 + j as f64 * 10.0 };
                r.stats = Some(json!({"time": [t0, t0 + 2.0]}));
                match criterion.as_str() {
                    "near-duplicate" => r.fingerprint = Some(if is_rel { key(j % 900) } else { format!("{salt}-{j}") }),
                    "domain-similarity" => r.domain_key = Some(if is_rel { key(j % 900) } else { format!("{salt}-{j}") }),
                    _ => {}
                }
                r
            })
            .collect();
        let mut vs = Vec::new();
        for c in regs.chunks(2000) {
            vs.extend(in_tx!(admin, |tx| namespace::register_batch(&mut tx, c).await?));
        }
        dataset(&admin, ds, vs.iter().map(|v| Member::version(v.version_id)).collect()).await?;
    }
    ctx.step().await?;
    let read_before = ctx.inst.metrics.bytes_read_for_catalog.load(std::sync::atomic::Ordering::SeqCst);
    let evaluator = ctx.session("user:admin");
    let a1 = in_tx!(evaluator, |tx| crate::leakage::check(&mut tx, &json!({"model": model}), &json!({"dataset": "/t/eval"}), "crit").await?);
    let a2 = in_tx!(evaluator, |tx| crate::leakage::check(&mut tx, &json!({"model": model}), &json!({"dataset": "/t/eval"}), "crit").await?);
    let read_after = ctx.inst.metrics.bytes_read_for_catalog.load(std::sync::atomic::Ordering::SeqCst);
    ctx.step().await?;
    let n_rel = a1["related"].as_i64().unwrap_or(-1);
    ctx.outcome(if a1["result"] == "satisfied" { "satisfied" } else { "violated" });
    let row = in_tx!(admin, |tx| tx.q1("select criterion, criterion_version, evidence from leakage_assertion where id = $1", &[&a1["assertion"].as_str().unwrap_or("").parse::<Uuid>().unwrap_or_default()]).await?);
    let ev: Value = row.get(2);
    ctx.check("A leakage assertion exists with the criterion", row.get::<_, String>(0) == "crit" && row.get::<_, i32>(1) == 1 && ev.is_object(), "");
    ctx.check("A second check with the same inputs", a1["result"] == a2["result"] && a1["related"] == a2["related"], "");
    ctx.check("counts only relations between the named evaluation dataset", n_rel == related as i64, format!("{n_rel} related, {related} expected"));
    ctx.check("the number of related test elements", n_rel == related as i64, format!("{n_rel}"));
    let scanned = ev["bytes_scanned"].as_u64().unwrap_or(1) + (read_after - read_before);
    ctx.check("computed the result from selectors", scanned == 0 && ev["method"].as_str().unwrap_or("").contains("selectors"), format!("{}", ev["method"]));
    ctx.check("computed the result from recorded checksums", scanned == 0 && ev["method"].as_str().unwrap_or("").contains("checksum"), format!("{}", ev["method"]));
    Ok(())
}

/// T2.3: get again, one year later, exactly the training inputs of a model.
pub async fn reproduce_later(ctx: &mut Ctx) -> Result<()> {
    let change = ctx.p("change");
    let admin = ctx.admin.clone();
    let store = resource(&admin, "store", "cloud-a", "managed", false).await?;
    resource(&admin, "store-2", "cloud-a", "managed", false).await?;
    acl_root(&admin, "/s", Some(("store", "s/"))).await?;
    grant_collection(&admin, "/s", "reader", "user:trainer").await?;
    source(&admin, "src", "computed", json!({}), &[("train", "research")]).await?;
    in_tx!(admin, |tx| crate::metadata::register_enricher(&mut tx, "labeler", "1.0", "labels", false, "site-agent").await?);
    let files: Vec<(String, Vec<u8>)> = (0..20).map(|i| (format!("x{i:02}.bin"), file_bytes("opaque", i, "s"))).collect();
    let regs = register_files(&admin, &store, "/s/data", "s/data/", &files, Some("src"), None).await?;
    verify_all(&admin, "store").await?;
    let versions: Vec<Uuid> = regs.iter().map(|r| r.version_id).collect();
    in_tx!(admin, |tx| datasets::create_dataset(&mut tx, "/s/train", &Default::default()).await?);
    let labels = |ver: &str| -> Vec<Value> { versions.iter().map(|v| json!({"version": v, "label": format!("label-{ver}")})).collect() };
    let rows = labels("1");
    let (set1, _) = in_tx!(admin, |tx| datasets::write_annotation_set(&mut tx, "/s/train", "labels", Some(("labeler", "1.0")), "version", &rows).await?);
    if change == "as-of-expired-snapshot" {
        // the run was recorded by an outside tracker, with the dataset "as of" a time and no resolution
        let s0 = commit(&admin, DatasetChange { dataset: "/s/train".into(), add: versions.iter().map(|v| Member::version(*v)).chain([Member::annotation_set(set1)]).collect(), ..Default::default() }).await?;
        let t0 = ctx.inst.now();
        let act = in_tx!(admin, |tx| provenance::record_run(&mut tx, &RunSpec { kind: "training".into(), capture: Some("imported".into()), params: json!({"input": "/s/train@main", "as_of": t0}), ..Default::default() }).await?);
        let _ = act;
        ctx.inst.clock.advance(chrono::Duration::seconds(5));
        // a later commit moves main; nothing refers to s0 any more
        commit(&admin, DatasetChange { dataset: "/s/train".into(), requirement: Some(s0.snapshot), remove: vec![versions[0]], ..Default::default() }).await?;
        ctx.inst.clock.advance(chrono::Duration::days(365));
        let expired = in_tx!(admin, |tx| datasets::expire(&mut tx).await?);
        let reviewer = ctx.session("user:trainer");
        let before: i64 = in_tx!(admin, |tx| tx.q1("select count(*) from resolution", &[]).await?.get(0));
        let mut req = train_request(vec![dataset_input("/s/train", Some("main"), "training")], "train", "research");
        req.inputs[0].as_of = Some(t0);
        let r = resolve(ctx, &reviewer, &req).await;
        let after: i64 = in_tx!(admin, |tx| tx.q1("select count(*) from resolution", &[]).await?.get(0));
        match r {
            Err(e) if e.code() == "snapshot-expired" => {
                ctx.outcome("error-states-expiry");
                ctx.check("the snapshot expired", e.to_string().contains("expired") && expired.contains(&s0.snapshot) && before == after, e.to_string());
            }
            Err(e) => {
                ctx.outcome("error");
                ctx.check("the snapshot expired", false, e.to_string());
            }
            Ok(_) => ctx.outcome("same"),
        }
        ctx.step().await?;
        return Ok(());
    }
    commit(&admin, DatasetChange { dataset: "/s/train".into(), add: versions.iter().map(|v| Member::version(*v)).chain([Member::annotation_set(set1)]).collect(), ..Default::default() }).await?;
    let snap = publish(ctx, &admin, "/s/train", "v1").await?;
    let trainer = ctx.session("user:trainer");
    let req = train_request(vec![dataset_input("/s/train", Some("v1"), "training")], "train", "research");
    let r0 = resolve(ctx, &trainer, &req).await?;
    let act = in_tx!(admin, |tx| provenance::record_run(&mut tx, &RunSpec { kind: "training".into(), code: Some("git:abc".into()), resolution: Some(r0.id), ..Default::default() }).await?);
    in_tx!(admin, |tx| provenance::publish_model(&mut tx, &ModelSpec { path: "/models/m".into(), content_hash: "m1".into(), size: 1, activity: Some(act), ..Default::default() }).await?);
    ctx.step().await?;
    ctx.inst.clock.advance(chrono::Duration::days(365));

    let mut renamed: Option<(Uuid, String)> = None;
    let mut moved: Option<(Uuid, String)> = None;
    let mut invalidated: BTreeSet<Uuid> = BTreeSet::new();
    let mut set2 = None;
    match change.as_str() {
        "input-has-new-version" => {
            let data = b"corrected bytes".to_vec();
            let f = vec![("x00.bin".to_string(), data)];
            let nv = register_files(&admin, &store, "/s/data", "s/data/v2-", &f, Some("src"), None).await?;
            verify_all(&admin, "store").await?;
            let head = in_tx!(admin, |tx| datasets::resolve_ref(&tx, datasets::dataset_id(&tx, "/s/train").await?, "main", None).await?);
            commit(&admin, DatasetChange { dataset: "/s/train".into(), requirement: Some(head), replace: vec![(versions[0], Member::version(nv[0].version_id))], ..Default::default() }).await?;
        }
        "member-invalidated" => {
            for v in &versions[..2] {
                in_tx!(admin, |tx| crate::policy::invalidate(&mut tx, "version", &v.to_string(), "consent withdrawn", "steward").await?);
                invalidated.insert(*v);
            }
        }
        "path-renamed" => {
            in_tx!(admin, |tx| namespace::rename(&mut tx, "/s/data/x03.bin", "/s/data/renamed-x03.bin").await?);
            renamed = Some((versions[3], "/s/data/renamed-x03.bin".into()));
        }
        "replica-moved" => {
            // copy one file to a new resource, verify it, and retire the old replica
            let v = versions[5];
            let (old_url, data) = {
                let reps = in_tx!(admin, |tx| namespace::replicas_of(&tx, &[v]).await?);
                let (b, k) = crate::storage::parse_url(&reps[0].url)?;
                (reps[0].url.clone(), admin.inst.store.read_local("mover", &b, &k)?)
            };
            let res2 = in_tx!(admin, |tx| namespace::resource(&tx, "store-2").await?);
            let key = "moved/x05.bin";
            admin.inst.store.put("mover", &res2.bucket, key, &data)?;
            let url = res2.url(key);
            in_tx!(admin, |tx| {
                let rid = namespace::add_replica(&mut tx, v, &res2, &url, "registered", None, None).await?;
                tx.exec("insert into agent_task (kind, resource_id, payload) values ('verify', $1, $2)", &[&res2.id, &json!({"replica": rid})]).await?;
                let old: Uuid = tx.q1("select id from replica where url = $1", &[&old_url]).await?.get(0);
                tx.exec("insert into agent_task (kind, resource_id, payload) values ('delete', $1, $2)", &[&store.id, &json!({"replica": old, "version": v, "url": old_url})]).await?;
            });
            run_agent(&admin, "store-2").await?;
            run_agent(&admin, "store").await?;
            moved = Some((v, url));
        }
        "enricher-has-new-version" => {
            in_tx!(admin, |tx| crate::metadata::register_enricher(&mut tx, "labeler", "2.0", "labels", false, "site-agent").await?);
            let rows = labels("2");
            let (s2, _) = in_tx!(admin, |tx| datasets::write_annotation_set(&mut tx, "/s/train", "labels", Some(("labeler", "2.0")), "version", &rows).await?);
            in_tx!(admin, |tx| datasets::commit_annotation_set(&mut tx, "/s/train", "main", s2).await?);
            set2 = Some(s2);
        }
        _ => {}
    }
    ctx.step().await?;
    let reviewer = ctx.session("user:trainer");
    let r = resolve_again(ctx, &reviewer, r0.id).await?;
    ctx.step().await?;
    let orig: BTreeSet<Uuid> = r0.tasks.iter().map(|t| t.version).collect();
    let now: BTreeSet<Uuid> = r.tasks.iter().map(|t| t.version).collect();
    let newly: Vec<String> = r.decision["newly_excluded"].as_array().cloned().unwrap_or_default().iter().filter_map(|v| v.as_str().map(|s| s.to_string())).collect();
    ctx.outcome(if !newly.is_empty() { "same-less-excluded" } else if now == orig { "same" } else { "different" });
    ctx.check("The members and their versions are the same", now == orig && r.inputs[0]["snapshot"] == json!(snap.id), "");
    ctx.check("also reports that an input has a newer version", r.notices.iter().any(|n| n["message"] == "an input has a newer version"), format!("{:?}", r.notices));
    if let Some((v, p)) = &renamed {
        let t = r.tasks.iter().find(|t| t.version == *v);
        ctx.check("shows the new path, and the same IDs", t.map(|t| &t.path == p).unwrap_or(false), format!("{:?}", t.map(|t| &t.path)));
    }
    if let Some((v, url)) = &moved {
        let t = r.tasks.iter().find(|t| t.version == *v);
        let h0 = in_tx!(admin, |tx| namespace::version(&tx, *v).await?.content_hash);
        let h_orig = r0.tasks.iter().find(|t| t.version == *v).map(|_| h0.clone());
        ctx.check("has the new replica URL", t.map(|t| &t.url == url).unwrap_or(false) && h_orig == Some(h0), format!("{:?}", t.map(|t| &t.url)));
    }
    if let Some(s2) = set2 {
        let sets: Vec<String> = r.inputs[0]["annotation_sets"].as_array().cloned().unwrap_or_default().iter().filter_map(|s| s["id"].as_str().map(|x| x.to_string())).collect();
        ctx.check("has the old version of the annotation set", sets.contains(&set1.to_string()) && !sets.contains(&s2.to_string()), format!("{sets:?}"));
    }
    let inv: BTreeSet<String> = invalidated.iter().map(|v| v.to_string()).collect();
    let rest_same = now == orig.difference(&invalidated).copied().collect();
    ctx.check("reports each element that policy now excludes", newly.iter().cloned().collect::<BTreeSet<_>>() == inv && rest_same, format!("{newly:?}"));
    let _ = (Error::NotFound, Value::Null);
    Ok(())
}
