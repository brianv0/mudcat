//! Reference scenarios of use case 3: rights and removal for training content
//! (evals/scenarios/uc3-rights-removal.yaml).

use mudcat::datasets::{self, Member};
use mudcat::evals::fixtures::*;
use mudcat::instance::sha256_hex;
use mudcat::namespace::{self, Register, ReplicaSpec, Resource};
use mudcat::policy::{self, Terms};
use mudcat::provenance::{self, ModelSpec, RunSpec};
use mudcat::resolver::{self, Input};
use mudcat::{authz, in_tx, metadata, placement, stream, Instance, Principal, Result, Session};
use serde_json::{json, Value};
use std::collections::BTreeSet;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use uuid::Uuid;

async fn setup(tag: &str) -> Result<(Arc<Instance>, Session)> {
    let url = std::env::var("MUDCAT_TEST_DB").unwrap_or_else(|_| "postgres://mudcat:mudcat@localhost:54329/mudcat_scenarios".into());
    let base = Instance::open(&url, "target/scenario-data").await?;
    let id = Uuid::new_v4().simple().to_string();
    let inst = base.fork(std::path::PathBuf::from("target/scenario-data/work").join(format!("{tag}-{id}")));
    let t = mudcat::tenants::create_tenant(&inst, &format!("{tag}-{}", &id[..10]), "user:admin").await?;
    let s = inst.session(t.id, Principal::user("admin"));
    Ok((inst, s))
}

async fn publish_tag(s: &Session, path: &str, tag: &str) -> Result<datasets::SnapshotInfo> {
    Ok(in_tx!(s, |tx| datasets::publish(&mut tx, path, "main", tag).await?))
}

async fn count(s: &Session, sql: &str) -> Result<i64> {
    Ok(in_tx!(s, |tx| tx.q1(sql, &[]).await?.get::<_, i64>(0)))
}

/// Files of one source in a collection; returns the versions.
async fn files_of(s: &Session, res: &Resource, coll: &str, prefix: &str, src: &str, n: usize) -> Result<Vec<Uuid>> {
    let files: Vec<(String, Vec<u8>)> = (0..n).map(|i| (format!("{src}-{i}.jsonl"), file_bytes("records", i, src))).collect();
    Ok(register_files(s, res, coll, prefix, &files, Some(src), None).await?.iter().map(|r| r.version_id).collect())
}

async fn train(s: &Session, inputs: Vec<Input>, model: &str, purpose: &str, base: Option<Uuid>) -> Result<(Uuid, resolver::Resolution, Uuid)> {
    let r = resolver::resolve(s, &train_request(inputs, "train", purpose)).await?;
    let act = in_tx!(s, |tx| provenance::record_run(&mut tx, &RunSpec { kind: "training".into(), code: Some("git:train".into()), resolution: Some(r.id), ..Default::default() }).await?);
    let derived = base.map(|b| vec![(b, "fineTunedFrom".to_string())]).unwrap_or_default();
    let mv = in_tx!(s, |tx| provenance::publish_model(&mut tx, &ModelSpec { path: model.into(), content_hash: sha256_hex(model.as_bytes()), size: 1, activity: Some(act), derived_from: derived, ..Default::default() }).await?);
    Ok((mv, r, act))
}

fn text_terms(grants: Vec<Value>) -> Terms {
    Terms::Text { text: "the contract".into(), grants, duties: vec![] }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn uc3_01() -> Result<()> {
    let (_inst, s) = setup("uc3-01").await?;
    let store = resource(&s, "store", "cloud", "managed", false).await?;
    acl_root(&s, "/c", Some(("store", "c/"))).await?;
    for (name, kind, grants) in [
        ("news-licence", "licensed", vec![json!({"action": "train", "purpose": "research", "clause": "licence §3.1"}), json!({"action": "train", "purpose": "commercial", "clause": "licence §3.2"})]),
        ("crawl-2025", "crawl", vec![json!({"action": "train", "purpose": "research", "clause": "crawl policy §2"})]),
        ("user-content", "user", vec![json!({"action": "evaluate", "purpose": "research", "clause": "user terms §5"})]),
    ] {
        let id = source(&s, name, kind, json!({}), &[]).await?;
        in_tx!(s, |tx| policy::set_terms(&mut tx, id, &text_terms(grants.clone())).await?);
    }
    let mut members = Vec::new();
    for src in ["news-licence", "crawl-2025", "user-content"] {
        members.extend(files_of(&s, &store, "/c/files", "c/files/", src, 4).await?);
    }
    verify_all(&s, "store").await?;
    dataset(&s, "/c/corpus", members.into_iter().map(Member::version).collect()).await?;
    let trainer = s.as_principal(Principal::user("admin"));

    let creds = count(&s, "select count(*) from credential_log").await?;
    let resolutions = count(&s, "select count(*) from resolution").await?;
    let r = resolver::resolve(&trainer, &train_request(vec![dataset_input("/c/corpus", Some("main"), "training")], "train", "commercial")).await;
    let e = r.err().expect("mudcat refuses the resolution.");
    let m = e.to_string();
    assert!(e.code() == "policy-refused", "mudcat refuses the resolution.");
    assert!(m.contains("crawl-2025 (12 elements") && m.contains("user-content (12 elements"), "The error names \"crawl-2025\" and \"user-content\", and the number of elements from each. ({m})");
    assert!(count(&s, "select count(*) from credential_log").await? == creds && count(&s, "select count(*) from resolution").await? == resolutions, "mudcat issues no credentials and no plan.");

    let news = in_tx!(s, |tx| policy::source_id(&tx, "news-licence").await?);
    let sel = Input { dataset: Some("/c/corpus".into()), selection: Some(json!({"sources": ["news-licence"]})), role: "training".into(), ..Default::default() };
    let r = resolver::resolve(&trainer, &train_request(vec![sel], "train", "commercial")).await?;
    assert!(r.tasks.len() == 4 && r.tasks.iter().all(|t| t.source == Some(news)), "The resolution succeeds, and contains only elements of \"news-licence\".");
    assert!(r.decision["policies"].as_array().unwrap().len() == 1 && r.decision["evaluator"] == mudcat::instance::EVALUATOR_VERSION && r.decision["outcome"] == "permitted", "The policy decision lists the policies that mudcat evaluated, the version of the evaluator, and the result.");

    let evaluator = s.as_principal(Principal::user("admin"));
    let sel = Input { dataset: Some("/c/corpus".into()), selection: Some(json!({"sources": ["user-content"]})), role: "test".into(), ..Default::default() };
    let ev = resolver::resolve(&evaluator, &train_request(vec![sel], "evaluate", "research")).await;
    assert!(ev.is_ok(), "The resolution succeeds.");

    // the auditor: why was each element in the training resolution permitted
    for p in r.decision["policies"].as_array().unwrap() {
        let g: Uuid = p["grant"].as_str().unwrap().parse().unwrap();
        let row = in_tx!(s, |tx| tx.q1("select g.clause, d.format, d.content from usage_grant g join terms_document d on d.id = g.terms_id where g.id = $1", &[&g]).await?);
        assert!(p["clause"] == "licence §3.2" && row.get::<_, Option<String>>(0).as_deref() == Some("licence §3.2") && row.get::<_, Value>(2)["text"] == "the contract", "The answer gives, for each source, the grant and the clause of the terms that the grant came from.");
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn uc3_02() -> Result<()> {
    let (inst, s) = setup("uc3-02").await?;
    let store = resource(&s, "store", "cloud", "managed", false).await?;
    acl_root(&s, "/v", Some(("store", "v/"))).await?;
    let t_end = inst.now() + chrono::Duration::days(10);
    let src = source(&s, "stock-video", "licensed", json!({"licensor": "Stock Co"}), &[]).await?;
    in_tx!(s, |tx| policy::grant_usage(&mut tx, src, "train", "commercial", Some(t_end), Some("licence term §1"), None).await?);
    let vs = files_of(&s, &store, "/v/files", "v/files/", "stock-video", 5).await?;
    verify_all(&s, "store").await?;
    let snap = dataset(&s, "/v/clips", vs.iter().map(|v| Member::version(*v)).collect()).await?.snapshot;
    in_tx!(s, |tx| {
        policy::refresh_policy_cache(&tx, snap).await?;
        datasets::add_representation(&mut tx, snap, "training-shards", &vs, json!({"format": "webdataset"}), None).await?;
    });
    let computed_at: chrono::DateTime<chrono::Utc> = in_tx!(s, |tx| tx.q1("select computed_at from policy_cache where snapshot_id = $1", &[&snap]).await?.get(0));
    // M1 is trained before T
    let (m1, _, act) = train(&s, vec![dataset_input("/v/clips", Some("main"), "training")], "/models/M1", "commercial", None).await?;
    assert!(true, "The resolution succeeds."); // the train() call above resolved before T
    inst.clock.advance(chrono::Duration::days(11));
    let r = resolver::resolve(&s, &train_request(vec![dataset_input("/v/clips", Some("main"), "training")], "train", "commercial")).await;
    let e = r.err().expect("mudcat refuses the resolution");
    assert!(e.code() == "policy-refused" && e.to_string().contains("stock-video") && e.to_string().contains("ended at"), "mudcat refuses the resolution and names the end of the grant of \"stock-video\". ({e})");
    let still: chrono::DateTime<chrono::Utc> = in_tx!(s, |tx| tx.q1("select computed_at from policy_cache where snapshot_id = $1", &[&snap]).await?.get(0));
    let pred = in_tx!(s, |tx| policy::permitted_predicate(&tx, snap, "train", "commercial").await?);
    assert!(still == computed_at && pred == Some(false), "This is true although the effective-policy cache was not computed again (the cache predicate also answers no).");
    let report = in_tx!(s, |tx| provenance::uses_of_expiring(&tx, t_end + chrono::Duration::days(30)).await?);
    let entry = report["sources"].as_array().unwrap().iter().find(|x| x["name"] == "stock-video").cloned().expect("stock-video in the report");
    assert!(
        entry["runs"].as_array().unwrap().contains(&json!(act)) && entry["models"].as_array().unwrap().contains(&json!(m1)) && !entry["caches"].as_array().unwrap().is_empty() && !entry["representations"].as_array().unwrap().is_empty(),
        "The report lists the runs that used \"stock-video\", M1, and the caches and representations that contain its data."
    );
    let state = in_tx!(s, |tx| provenance::model_state(&tx, m1).await?);
    let flag = state["flags"].as_array().unwrap().iter().find(|f| f["kind"] == "licence-ended").cloned().expect("a licence-ended flag");
    assert!(flag["affected_elements"] == 15, "M1 has a flag that an input licence ended, with the number of affected elements. ({flag})");
    assert!(state["content_hash"] == json!(sha256_hex(b"/models/M1")), "mudcat did not change, hide or delete M1.");
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn uc3_03() -> Result<()> {
    let (inst, s) = setup("uc3-03").await?;
    let r1 = resource(&s, "store-1", "cloud", "managed", false).await?;
    let r2 = resource(&s, "store-2", "cloud", "external", true).await?;
    acl_root(&s, "/img", Some(("store-1", "img/"))).await?;
    source(&s, "web-images", "crawl", json!({"domain": "images.example"}), &[("train", "research")]).await?;
    let image = b"the image of the work".to_vec();
    let h = sha256_hex(&image);
    let f = "fp:work-4711".to_string();
    // the image on two storage resources; the second keeps old object versions
    inst.store.put("producer", &r1.bucket, "img/work.png", &image)?;
    inst.store.put("producer", &r2.bucket, "img/work.png", b"an older object version")?;
    inst.store.put("producer", &r2.bucket, "img/work.png", &image)?;
    let mut regs = vec![Register { path: "/img/work.png".into(), content_hash: Some(h.clone()), size: image.len() as i64, source: Some("web-images".into()), fingerprint: Some(f.clone()), replica: Some(ReplicaSpec { resource: "store-1".into(), key: Some("img/work.png".into()), ..Default::default() }), ..Default::default() }];
    // two training shards that contain the image as an element
    for k in 0..2 {
        let data = format!("shard {k}").into_bytes();
        let key = format!("img/shard-{k}.tar");
        inst.store.put("producer", &r1.bucket, &key, &data)?;
        regs.push(Register {
            path: format!("/img/shard-{k}.tar"),
            content_hash: Some(sha256_hex(&data)),
            size: data.len() as i64,
            source: Some("web-images".into()),
            structure: Some(json!({"kind": "container"})),
            elements: Some(vec![json!({"id": "work.png", "fingerprint": f, "checksum": h}), json!({"id": format!("other-{k}.png"), "fingerprint": format!("fp:other-{k}")})]),
            replica: Some(ReplicaSpec { resource: "store-1".into(), key: Some(key), ..Default::default() }),
            ..Default::default()
        });
    }
    let others: Vec<Uuid> = files_of(&s, &r1, "/img/more", "img/more/", "web-images", 3).await?;
    let mut ids = Vec::new();
    for r in &regs {
        ids.push(in_tx!(s, |tx| namespace::register(&mut tx, r).await?).version_id);
    }
    let img = ids[0];
    in_tx!(s, |tx| namespace::add_replica(&mut tx, img, &r2, &r2.url("img/work.png"), "registered", None, None).await?);
    verify_all(&s, "store-1").await?;
    verify_all(&s, "store-2").await?;
    let mut published = Vec::new();
    for d in ["/img/set-1", "/img/set-2", "/img/set-3"] {
        let mut m = vec![Member::version(img)];
        m.extend(others.iter().map(|v| Member::version(*v)));
        dataset(&s, d, m).await?;
        published.push(publish_tag(&s, d, "v1").await?);
    }
    in_tx!(s, |tx| {
        datasets::add_representation(&mut tx, published[0].id, "shards-a", &[ids[1]], json!({}), None).await?;
        datasets::add_representation(&mut tx, published[0].id, "shards-b", &[ids[2]], json!({}), None).await?;
    });
    let (emb, _) = in_tx!(s, |tx| datasets::write_annotation_set(&mut tx, "/img/set-1", "embeddings", None, "version", &[json!({"version": img, "embedding": [0.1, 0.2, 0.3]})]).await?);
    in_tx!(s, |tx| datasets::commit_annotation_set(&mut tx, "/img/set-1", "main", emb).await?);
    let r9 = resolver::resolve(&s, &train_request(vec![dataset_input("/img/set-1", Some("v1"), "training")], "train", "research")).await?;
    assert!(r9.tasks.iter().any(|t| t.version == img));
    stream::flush(&s).await?;
    let pos: i64 = in_tx!(s, |tx| tx.q1("select last_position from stream_state", &[]).await?.get(0));
    let snaps_before = count(&s, "select count(*) from snapshot").await?;

    let counsel = s.clone();
    let inv = in_tx!(counsel, |tx| policy::invalidate(&mut tx, "fingerprint", &f, "takedown request 4711", "rights holder").await?);
    let row = in_tx!(s, |tx| tx.q1("select reason, requested_by from invalidation where id = $1", &[&inv["invalidation"].as_str().unwrap().parse::<Uuid>().unwrap()]).await?);
    assert!(row.get::<_, String>(0) == "takedown request 4711" && row.get::<_, String>(1) == "rights holder", "An invalidation record exists with the reason and the party.");
    assert_eq!(count(&s, "select count(*) from snapshot").await?, snaps_before, "mudcat created no new snapshot of the three datasets.");

    let r = resolver::resolve(&s, &train_request(vec![dataset_input("/img/set-2", Some("v1"), "training")], "train", "research")).await?;
    assert!(!r.tasks.iter().any(|t| t.version == img), "The resolved input does not contain the image.");

    let occ = in_tx!(s, |tx| provenance::occurrences(&tx, &f).await?);
    let ds: BTreeSet<String> = occ["datasets"].as_array().unwrap().iter().map(|d| d.as_str().unwrap().to_string()).collect();
    let img_replicas: Vec<&Value> = occ["replicas"].as_array().unwrap().iter().filter(|r| r["version"] == json!(img)).collect();
    let res_names: BTreeSet<String> = img_replicas.iter().map(|r| r["resource"].as_str().unwrap().to_string()).collect();
    assert!(
        ds.len() == 3 && occ["representations"].as_array().unwrap().len() == 2 && occ["annotation_sets"].as_array().unwrap().iter().any(|a| a["name"] == "embeddings") && res_names.len() == 2,
        "The answer lists the three datasets, the two representations, the annotation set, and each replica on the two storage resources. ({occ})"
    );

    let versions = in_tx!(s, |tx| policy::affected_versions(&tx, "fingerprint", &f).await?);
    let out = in_tx!(counsel, |tx| policy::purge(&mut tx, &versions, "takedown request 4711").await?);
    assert!(out.deletion_tasks.len() == 2 && out.rewrite_tasks.len() == 2, "For the shards that contain the image, mudcat makes a task to write them again without it.");
    run_agent(&s, "store-1").await?;
    run_agent(&s, "store-2").await?;
    let receipts: i64 = in_tx!(s, |tx| tx.q1("select count(*) from deletion_receipt where version_id = $1", &[&img]).await?.get(0));
    assert_eq!(receipts, 2, "Site agents delete each replica of the image. There is one deletion receipt for each replica.");
    assert!(inst.store.object_versions(&r2.bucket, "img/work.png") == 0 && !inst.store.exists(&r1.bucket, "img/work.png"), "On storage that keeps old object versions, the old versions are deleted also.");
    for p in &published {
        let now = in_tx!(s, |tx| datasets::snapshot(&tx, p.id).await?.merkle_hash);
        assert_eq!(now, p.merkle_hash, "Each snapshot has the same Merkle hash as before the purge.");
    }
    let again = resolver::resolve_again(&s, r9.id).await?;
    assert!(again.decision["newly_excluded"].as_array().unwrap().contains(&json!(img.to_string())), "mudcat reports the image as an element that policy now excludes.");
    stream::flush(&s).await?;
    let p = stream::poll(&s, pos, 10_000, None, true).await?;
    assert!(p.events.iter().any(|d| d["id"] == json!(img) && d["removed"] == true), "The stream has a removal event for the image.");
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn uc3_04() -> Result<()> {
    let (_inst, s) = setup("uc3-04").await?;
    let store = resource(&s, "store", "cloud", "managed", false).await?;
    acl_root(&s, "/books", Some(("store", "books/"))).await?;
    grant_collection(&s, "/books", "reader", "user:reader").await?;
    source(&s, "publisher", "licensed", json!({}), &[("train", "research"), ("evaluate", "research")]).await?;
    let vs = files_of(&s, &store, "/books/files", "books/files/", "publisher", 3).await?;
    verify_all(&s, "store").await?;
    let d = dataset(&s, "/books/books-1", vs.iter().map(|v| Member::version(*v)).collect()).await?;
    let counsel = s.clone();
    let hold = in_tx!(counsel, |tx| policy::place_hold(&mut tx, "litigation 22-cv-1", "litigation 22-cv-1", &[d.dataset]).await?);
    let book = vs[0];
    in_tx!(counsel, |tx| policy::invalidate(&mut tx, "version", &book.to_string(), "takedown", "rights holder").await?);
    for action in ["train", "evaluate"] {
        let r = resolver::resolve(&s, &train_request(vec![dataset_input("/books/books-1", Some("main"), "training")], action, "research")).await?;
        assert!(!r.tasks.iter().any(|t| t.version == book), "The book does not resolve for any action ({action}).");
    }
    let out = in_tx!(counsel, |tx| policy::purge(&mut tx, &[book], "takedown").await?);
    let b = out.blocked_by_hold.clone().expect("the hold blocks the purge");
    let url = in_tx!(s, |tx| namespace::replicas_of(&tx, &[book]).await?)[0].url.clone();
    let (bk, key) = mudcat::storage::parse_url(&url)?;
    assert!(s.inst.store.exists(&bk, &key) && b["name"] == "litigation 22-cv-1" && b["reason"] == "litigation 22-cv-1", "mudcat does not delete the bytes. The answer names the legal hold and its reason.");
    let comps = in_tx!(s, |tx| authz::compartments_of(&tx, "version", book).await?);
    let reader = s.as_principal(Principal::user("reader"));
    let denied = in_tx!(reader, |tx| authz::check(&tx, "read", "version", book).await?);
    let member = in_tx!(counsel, |tx| authz::check(&tx, "read", "version", book).await?);
    let other = in_tx!(reader, |tx| authz::check(&tx, "read", "version", vs[1]).await?);
    assert!(comps.contains(&format!("retention-{hold}")) && !denied.allow && member.allow && other.allow, "The book is in the retention compartment. Only the members of that compartment can read it.");
    let er = in_tx!(counsel, |tx| policy::erase(&mut tx, &[book], "erasure", false).await?);
    assert!(er.blocked_by_hold.map(|h| h["name"] == "litigation 22-cv-1").unwrap_or(false) && er.erased.is_empty(), "mudcat refuses the erasure and names the legal hold.");
    let rel = in_tx!(counsel, |tx| policy::release_hold(&mut tx, "litigation 22-cv-1", "case closed").await?);
    let row = in_tx!(s, |tx| tx.q1("select released_by, released_at, released_reason from legal_hold where id = $1", &[&hold]).await?);
    assert!(rel["released_by"] == "user:admin" && row.get::<_, Option<String>>(0).is_some() && row.get::<_, Option<chrono::DateTime<chrono::Utc>>>(1).is_some() && row.get::<_, Option<String>>(2).as_deref() == Some("case closed"), "The release records who released the hold, when, and the reason.");
    let out = in_tx!(counsel, |tx| policy::purge(&mut tx, &[book], "takedown").await?);
    assert!(out.blocked_by_hold.is_none());
    run_agent(&s, "store").await?;
    let receipts: i64 = in_tx!(s, |tx| tx.q1("select count(*) from deletion_receipt where version_id = $1", &[&book]).await?.get(0));
    assert_eq!(receipts, 1, "The purge completes, with one deletion receipt for each replica.");
    stream::flush(&s).await?;
    let types: Vec<String> = in_tx!(s, |tx| tx
        .q("select type from event where subject = $1 or data->'data'->>'version' = $2 or type like 'mudcat.legal-hold.%' order by position", &[&book, &book.to_string()])
        .await?
        .iter()
        .map(|r| r.get(0))
        .collect());
    let at = |t: &str| types.iter().position(|x| x == t).unwrap_or(usize::MAX);
    let (i, b, r, p) = (at("mudcat.removal.invalidated.v1"), at("mudcat.removal.blocked-by-hold.v1"), at("mudcat.legal-hold.released.v1"), at("mudcat.replica.purged.v1"));
    assert!(i < b && b < r && r < p && p != usize::MAX, "The history shows the invalidation, the blocked purge, the release and the purge, in the order of their commits. ({types:?})");
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn uc3_05() -> Result<()> {
    let (_inst, s) = setup("uc3-05").await?;
    let store = resource(&s, "store", "cloud", "managed", false).await?;
    acl_root(&s, "/p", Some(("store", "p/"))).await?;
    let open = source(&s, "open-set", "licensed", json!({}), &[]).await?;
    let partner = source(&s, "partner-A", "licensed", json!({"licensor": "Partner A"}), &[]).await?;
    let steward = s.as_principal(Principal::user("steward"));
    let c = in_tx!(steward, |tx| policy::set_terms(&mut tx, open, &Terms::Spdx { id: "CC-BY-4.0".into() }).await?);
    assert!(!c.grants.is_empty() && c.duties.contains(&"attribute".to_string()), "The source has grants and an attribution duty from the stored mapping of that licence.");
    let doc = json!({"uid": "urn:partner-a:2026", "permission": [
        {"uid": "clause-1", "action": "train", "constraint": [{"leftOperand": "purpose", "operator": "eq", "rightOperand": "research"}]},
        {"uid": "clause-2", "action": "train", "constraint": [{"leftOperand": "industryContext", "operator": "eq", "rightOperand": "media-partners"}]}]});
    let c = in_tx!(steward, |tx| policy::set_terms(&mut tx, partner, &Terms::Odrl { document: doc.clone() }).await?);
    let stored: Value = in_tx!(s, |tx| tx.q1("select content from terms_document where id = $1", &[&c.terms_id]).await?.get(0));
    assert_eq!(stored, doc, "mudcat keeps the original document.");
    let open_items = count(&s, "select count(*) from review_item where kind = 'clause' and status = 'open' and clause = 'clause-2'").await?;
    assert!(c.status == "partly-compiled" && open_items == 1, "The source has the status partly compiled. The clause that mudcat did not translate is in the review queue.");
    let mut members = files_of(&s, &store, "/p/files", "p/files/", "partner-A", 3).await?;
    members.extend(files_of(&s, &store, "/p/files", "p/files/", "open-set", 2).await?);
    verify_all(&s, "store").await?;
    dataset(&s, "/p/partner-data", members.into_iter().map(Member::version).collect()).await?;
    let req = train_request(vec![dataset_input("/p/partner-data", Some("main"), "training")], "train", "commercial");
    let r = resolver::resolve(&s, &req).await;
    let e = r.err().expect("mudcat blocks the resolution");
    assert!(e.code() == "blocked-for-review" && e.to_string().contains("clause-2"), "mudcat blocks the resolution and names the clause that needs review. ({e})");
    let item: Uuid = in_tx!(s, |tx| tx.q1("select id from review_item where clause = 'clause-2'", &[]).await?.get(0));
    let counsel = s.as_principal(Principal::user("counsel"));
    let grants = in_tx!(counsel, |tx| policy::review_clause(&mut tx, item, &json!({"grants": [{"action": "train", "purpose": "commercial", "not_after": "2027-12-31T00:00:00Z"}]})).await?);
    let row = in_tx!(s, |tx| tx.q1("select not_after, granted_by, clause from usage_grant where id = $1", &[&grants[0]]).await?);
    let na: Option<chrono::DateTime<chrono::Utc>> = row.get(0);
    let clause: Option<String> = row.get(2);
    assert!(na.map(|t| t.to_rfc3339().starts_with("2027-12-31")).unwrap_or(false) && row.get::<_, String>(1) == "user:counsel" && clause.unwrap_or_default().contains("clause-2"), "The grant exists with the end date, the reviewer, and a link to the clause.");
    let (m2, r, _) = train(&s, vec![dataset_input("/p/partner-data", Some("main"), "training")], "/models/m2", "commercial", None).await?;
    assert_eq!(r.decision["outcome"], "permitted", "The resolution succeeds.");
    let duties = in_tx!(s, |tx| provenance::model_duties(&tx, m2).await?);
    assert!(duties.iter().any(|d| d["duty"] == "attribute" && d["source"] == "open-set"), "The model carries the attribution duty of \"open-set\" if its inputs included that source.");
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn uc3_06() -> Result<()> {
    let (inst, s) = setup("uc3-06").await?;
    let store = resource(&s, "store", "cloud", "managed", false).await?;
    acl_root(&s, "/t", Some(("store", "t/"))).await?;
    let mut members = Vec::new();
    let mut licensed = Vec::new();
    for i in 0..46usize {
        let (kind, attrs) = match i {
            0..=39 => ("crawl", json!({"domain": format!("site{}.example", i % 7), "crawler": format!("mudbot/{}", 1 + i % 2), "fetch_from": "2025-01-01", "fetch_to": "2025-03-31"})),
            40..=42 => ("licensed", json!({"licensor": format!("Licensor {i}")})),
            43 => ("user", json!({"consent_scope": "model training", "legal_basis": "dpv:Consent"})),
            _ => ("generated", json!({"model": "gen-7", "prompt_version": "p3"})),
        };
        let name = format!("src-{i:02}");
        let id = source(&s, &name, kind, attrs, &[("train", "research")]).await?;
        if kind == "licensed" {
            in_tx!(s, |tx| policy::set_terms(&mut tx, id, &Terms::Spdx { id: "CC-BY-4.0".into() }).await?);
            licensed.push(id);
        }
        members.extend(files_of(&s, &store, "/t/files", "t/files/", &name, 1).await?);
    }
    verify_all(&s, "store").await?;
    dataset(&s, "/t/mix", members.into_iter().map(Member::version).collect()).await?;
    let (m3, r, _) = train(&s, vec![dataset_input("/t/mix", Some("main"), "training")], "/models/M3", "research", None).await?;
    let read_before = inst.metrics.bytes_read_for_catalog.load(Ordering::SeqCst);
    let gets_before = inst.store.accesses().iter().filter(|a| a.op == "get").count();
    let report = in_tx!(s, |tx| provenance::content_summary(&tx, m3).await?);
    let sections: BTreeSet<String> = report["sections"].as_object().unwrap().keys().cloned().collect();
    assert_eq!(sections, ["crawl", "generated", "licensed", "user"].iter().map(|x| x.to_string()).collect::<BTreeSet<_>>(), "The report has one section for each kind of source.");
    let crawl = &report["sections"]["crawl"];
    let sizes: Vec<i64> = crawl["domains_by_size"].as_array().unwrap().iter().map(|d| d["bytes"].as_i64().unwrap()).collect();
    assert!(sizes.len() == 7 && sizes.windows(2).all(|w| w[0] >= w[1]) && crawl["crawlers"].as_array().unwrap().len() == 2 && crawl["fetch_periods"].as_array().unwrap().len() == 40, "For crawled content, it lists the domains by size, the crawler names and the fetch periods.");
    let total: i64 = report["elements_by_source"].as_array().unwrap().iter().map(|e| e["elements"].as_i64().unwrap()).sum();
    assert_eq!(total, r.total_elements(), "The numbers in the report agree with the numbers in the resolution of M3.");
    assert!(inst.metrics.bytes_read_for_catalog.load(Ordering::SeqCst) == read_before && inst.store.accesses().iter().filter(|a| a.op == "get").count() == gets_before, "mudcat made the report from the catalog, with no read of training data.");
    let used = in_tx!(s, |tx| provenance::models_using_source(&tx, licensed[0]).await?);
    assert!(used.len() == 1 && used[0]["model_version"] == json!(m3) && used[0]["elements"] == 3, "The report lists M3 with the number of elements from that source.");
    // fine-tuning
    source(&s, "ft-data", "human", json!({"vendor": "labelers inc"}), &[("train", "research")]).await?;
    let ft = files_of(&s, &store, "/t/ft", "t/ft/", "ft-data", 2).await?;
    verify_all(&s, "store").await?;
    dataset(&s, "/t/ft-set", ft.into_iter().map(Member::version).collect()).await?;
    let (m3ft, _, _) = train(&s, vec![dataset_input("/t/ft-set", Some("main"), "training")], "/models/m3-finetuned", "research", Some(m3)).await?;
    let rep2 = in_tx!(s, |tx| provenance::content_summary(&tx, m3ft).await?);
    let src_ids: BTreeSet<String> = rep2["elements_by_source"].as_array().unwrap().iter().map(|e| e["source"].as_str().unwrap().to_string()).collect();
    let ftid = in_tx!(s, |tx| policy::source_id(&tx, "ft-data").await?);
    assert!(src_ids.len() == 47 && src_ids.contains(&ftid.to_string()) && src_ids.contains(&licensed[0].to_string()), "The summary for the new model includes the sources of M3 and the sources of the fine-tuning data.");
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn uc3_07() -> Result<()> {
    let (_inst, s) = setup("uc3-07").await?;
    let store = resource(&s, "store", "cloud", "managed", false).await?;
    acl_root(&s, "/s", Some(("store", "s/"))).await?;
    in_tx!(s, |tx| metadata::declare_attribute(&mut tx, &metadata::AttributeSpec { namespace: "tags".into(), name: "consent".into(), datatype: "string".into(), is_tag: true, propagate: true, permitted: Some(vec!["given".into(), "withdrawn".into()]), ..Default::default() }).await?);
    let sp = source(&s, "sp-12", "user", json!({"speaker": "sp-12"}), &[("train", "research")]).await?;
    in_tx!(s, |tx| metadata::tag(&mut tx, sp, "consent", "given").await?);
    let recs = files_of(&s, &store, "/s/speech", "s/speech/", "sp-12", 3).await?;
    let gen = source(&s, "genomes-src", "external", json!({}), &[("train", "research")]).await?;
    in_tx!(s, |tx| {
        policy::add_duty(&mut tx, gen, "share-only-with-approved-users", Some("data use agreement §4"), None, json!({"group": "approved-users"})).await?;
        policy::add_duty(&mut tx, gen, "delete-at-project-end", Some("data use agreement §7"), None, json!({})).await?;
    });
    let gfiles = files_of(&s, &store, "/s/genomes", "s/genomes/", "genomes-src", 2).await?;
    verify_all(&s, "store").await?;
    dataset(&s, "/s/speech-set", recs.iter().map(|v| Member::version(*v)).collect()).await?;
    dataset(&s, "/s/genomes-set", gfiles.into_iter().map(Member::version).collect()).await?;
    let (v1, _, _) = train(&s, vec![dataset_input("/s/speech-set", Some("main"), "training")], "/models/V1", "research", None).await?;
    let (g1, _, _) = train(&s, vec![dataset_input("/s/genomes-set", Some("main"), "training")], "/models/G1", "research", None).await?;
    in_tx!(s, |tx| metadata::write(&mut tx, &metadata::WriteMetadata { entity: recs[0], namespace: "speaker".into(), payload: json!({"name": "Jane Doe"}), ..Default::default() }).await?);
    stream::flush(&s).await?;
    let pos: i64 = in_tx!(s, |tx| tx.q1("select last_position from stream_state", &[]).await?.get(0));

    let officer = s.clone();
    let propagated = in_tx!(officer, |tx| metadata::tag(&mut tx, sp, "consent", "withdrawn").await?);
    let v1_tags = in_tx!(s, |tx| metadata::effective_tags(&tx, v1).await?);
    assert!(propagated.contains(&v1) && recs.iter().all(|r| propagated.contains(r)) && v1_tags.contains(&("consent".into(), "withdrawn".into())), "The tag propagates to the entities derived from the source.");
    in_tx!(officer, |tx| policy::invalidate(&mut tx, "source", &sp.to_string(), "consent withdrawn", "privacy officer").await?);
    for action in ["train", "evaluate"] {
        let r = resolver::resolve(&s, &train_request(vec![dataset_input("/s/speech-set", Some("main"), "training")], action, "research")).await;
        let none = match r {
            Ok(r) => !r.tasks.iter().any(|t| recs.contains(&t.version)),
            Err(e) => e.code() != "internal",
        };
        assert!(none, "No recording of sp-12 resolves for any action.");
    }
    let used = in_tx!(s, |tx| provenance::models_using_source(&tx, sp).await?);
    let entry = used.iter().find(|m| m["model_version"] == json!(v1)).cloned().expect("V1 in the report");
    assert!(entry["elements"] == 9, "The report lists V1 with the number of affected elements.");
    let flags = in_tx!(s, |tx| policy::flags_of(&tx, v1).await?);
    let hash = in_tx!(s, |tx| namespace::version(&tx, v1).await?.content_hash);
    assert!(!flags.is_empty() && hash == Some(sha256_hex(b"/models/V1")), "V1 has a flag. mudcat did not change V1.");
    let lead = s.as_principal(Principal::user("lead"));
    let flag: Uuid = flags[0]["flag"].as_str().unwrap().parse().unwrap();
    in_tx!(lead, |tx| policy::decide_flag(&mut tx, flag, "retrain without sp-12 before the next release").await?);
    let row = in_tx!(s, |tx| tx.q1("select decision, decided_by, decided_at from model_flag where id = $1", &[&flag]).await?);
    assert!(row.get::<_, Option<String>>(0).is_some() && row.get::<_, Option<String>>(1).as_deref() == Some("user:lead") && row.get::<_, Option<chrono::DateTime<chrono::Utc>>>(2).is_some(), "mudcat records the decision, the person and the time.");
    let er = in_tx!(officer, |tx| policy::erase_payload(&mut tx, recs[0], "speaker", "erasure request of the speaker").await?);
    let rec = in_tx!(s, |tx| tx.q1("select row_to_json(e)::text from erasure_record e where id = $1", &[&er]).await?).get::<_, String>(0);
    let left = count(&s, "select count(*) from metadata_history where payload::text like '%Jane Doe%'").await? + count(&s, "select count(*) from metadata_payload where payload::text like '%Jane Doe%'").await?;
    assert!(rec.contains("erasure request") && !rec.contains("Jane") && left == 0, "Only an erasure record stays, with the ID, the date and the reason. It does not contain the name.");
    stream::flush(&s).await?;
    let p = stream::poll(&s, pos, 10_000, None, true).await?;
    assert!(p.events.iter().any(|d| d["id"] == json!(er) && d["removed"] == true), "The state stream has a removal event for the payload.");
    let engineer = s.clone();
    let ex = in_tx!(engineer, |tx| mudcat::export::check_model_export(&tx, g1, "user:outsider").await);
    assert!(matches!(&ex, Err(e) if e.code() == "duty" && e.to_string().contains("share only with approved users") && e.to_string().contains("genomes-src")), "mudcat refuses the export and names the duty that G1 has from \"genomes\". ({ex:?})");
    grant(&s, "group", "approved-users", "member", "user:approved").await?;
    let ok = in_tx!(engineer, |tx| {
        mudcat::export::check_model_export(&tx, g1, "user:approved").await?;
        let a = namespace::version(&tx, g1).await?.artifact_id;
        authz::grant(&mut tx, "artifact", &a.to_string(), "reader", "user:approved", None).await
    });
    assert!(ok.is_ok(), "The grant succeeds.");
    let approved = s.as_principal(Principal::user("approved"));
    let d = in_tx!(approved, |tx| authz::check(&tx, "read", "version", g1).await?);
    assert!(d.allow, "the approved user can read G1");
    let _ = placement::AgentFault::None;
    Ok(())
}
