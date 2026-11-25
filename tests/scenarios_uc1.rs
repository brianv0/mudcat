//! Reference scenarios of use case 1: curation of video and sensor data
//! (evals/scenarios/uc1-curation.yaml). Each test asserts the `expect` statements of its steps.

use mudcat::datasets::{self, DatasetChange, Member};
use mudcat::evals::fixtures::*;
use mudcat::metadata::{self, AttributeSpec, WriteMetadata};
use mudcat::placement::{self, AgentFault, MoverFault, SiteAgent};
use mudcat::provenance::{self, LineageQuery, ModelSpec};
use mudcat::resolver::{self, Input};
use mudcat::search::{self, SearchRequest};
use mudcat::storage::Credential;
use mudcat::{in_tx, namespace, policy, stream, Instance, Principal, Result, Session};
use serde_json::json;
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

fn sys(s: &Session) -> Session {
    s.as_principal(Principal::system())
}

async fn publish_tag(s: &Session, path: &str, tag: &str) -> Result<datasets::SnapshotInfo> {
    Ok(in_tx!(s, |tx| datasets::publish(&mut tx, path, "main", tag).await?))
}

async fn head(s: &Session, path: &str) -> Result<Uuid> {
    Ok(in_tx!(s, |tx| datasets::resolve_ref(&tx, datasets::dataset_id(&tx, path).await?, "main", None).await?))
}

async fn member_ids(s: &Session, snap: Uuid) -> Result<BTreeSet<Uuid>> {
    Ok(in_tx!(s, |tx| datasets::members(&tx, snap).await?.iter().map(|e| e.member_id).collect()))
}

/// Video files in a bucket that mudcat does not manage, with a captions set; S1 and S2.
async fn video_fixture(s: &Session, n: usize) -> Result<(Vec<Uuid>, Uuid, Uuid, Uuid)> {
    let res = resource(s, "lab-video", "cloud-a", "external", false).await?;
    acl_root(s, "/video", None).await?;
    acl_root(s, "/video/raw", Some(("lab-video", "raw/"))).await?;
    grant_collection(s, "/video/raw", "reader", "user:trainer").await?;
    grant_collection(s, "/video", "reader", "user:trainer").await?;
    source(s, "fleet-2026", "instrument", json!({}), &[("train", "research")]).await?;
    for i in 0..n {
        s.inst.store.put("lab", &res.bucket, &format!("raw/clip-{i:05}.mp4"), &file_bytes("stream", i, "fleet"))?;
    }
    let coll = in_tx!(s, |tx| namespace::collection(&tx, "/video/raw").await?.id);
    in_tx!(s, |tx| metadata::write(&mut tx, &WriteMetadata { entity: coll, namespace: "defaults".into(), payload: json!({"source": "fleet-2026"}), ..Default::default() }).await?);
    placement::report_inventory(s, "lab-video", "agent-lab").await?;
    verify_all(s, "lab-video").await?;
    let versions: Vec<Uuid> = in_tx!(s, |tx| tx.q("select id from version order by id", &[]).await?.iter().map(|r| r.get(0)).collect());
    let s1 = dataset(s, "/video/drive-clips", versions.iter().map(|v| Member::version(*v)).collect()).await?.snapshot;
    in_tx!(s, |tx| metadata::register_enricher(&mut tx, "captioner", "1.0", "captions", false, "site-agent").await?);
    let rows: Vec<serde_json::Value> = versions
        .iter()
        .enumerate()
        .map(|(i, v)| json!({"version": v, "selector": {"type": "media-fragment", "t": [0, 10]}, "text": if i % 2 == 0 { "pedestrian crossing at night in rain" } else { "car on a highway at noon" }}))
        .collect();
    let (set, _) = in_tx!(s, |tx| datasets::write_annotation_set(&mut tx, "/video/drive-clips", "captions", Some(("captioner", "1.0")), "time-span", &rows).await?);
    let s2 = in_tx!(s, |tx| datasets::commit_annotation_set(&mut tx, "/video/drive-clips", "main", set).await?).snapshot;
    Ok((versions, s1, s2, set))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn uc1_01() -> Result<()> {
    let (inst, s) = setup("uc1-01").await?;
    let res = resource(&s, "lab-video", "cloud-a", "external", false).await?;
    acl_root(&s, "/video", None).await?;
    acl_root(&s, "/video/raw", Some(("lab-video", "raw/"))).await?;
    grant_collection(&s, "/video", "reader", "user:trainer").await?;
    grant_collection(&s, "/video/raw", "reader", "user:trainer").await?;
    source(&s, "fleet-2026", "instrument", json!({}), &[("train", "research")]).await?;
    for i in 0..1000 {
        inst.store.put("lab", &res.bucket, &format!("raw/clip-{i:05}.mp4"), &file_bytes("stream", i, "fleet"))?;
    }
    let coll = in_tx!(s, |tx| namespace::collection(&tx, "/video/raw").await?.id);
    in_tx!(s, |tx| metadata::write(&mut tx, &WriteMetadata { entity: coll, namespace: "defaults".into(), payload: json!({"source": "fleet-2026"}), ..Default::default() }).await?);

    // step 1: report_inventory
    let puts_before = inst.store.accesses().iter().filter(|a| a.op == "put").count();
    placement::report_inventory(&s, "lab-video", "agent-lab").await?;
    let (artifacts, versions, replicas): (i64, i64, i64) = in_tx!(s, |tx| {
        let r = tx.q1("select (select count(*) from artifact a join collection c on c.id = a.collection_id where c.path = '/video/raw'), (select count(*) from version), (select count(*) from replica)", &[]).await?;
        (r.get(0), r.get(1), r.get(2))
    });
    assert!(artifacts == 1000 && versions == 1000 && replicas == 1000, "1,000 artifacts exist in /video/raw. Each has one version and one replica ({artifacts}, {versions}, {replicas})");
    let bad: i64 = in_tx!(s, |tx| tx.q1("select count(*) from version where structure->>'kind' <> 'stream' or structure->'duration_s' is null or structure->'frame_rate' is null", &[]).await?.get(0));
    assert_eq!(bad, 0, "Each version has a stream structure descriptor with its duration and frame rate.");
    assert_eq!(inst.metrics.bytes_copied_for_catalog.load(Ordering::SeqCst), 0, "mudcat copied no bytes.");
    assert_eq!(inst.store.accesses().iter().filter(|a| a.op == "put").count(), puts_before, "mudcat copied no bytes (no writes to storage).");

    // step 2: verify
    verify_all(&s, "lab-video").await?;
    let unverified: i64 = in_tx!(s, |tx| tx.q1("select count(*) from replica where state <> 'verified' or observed_checksum is null", &[]).await?.get(0));
    assert_eq!(unverified, 0, "Each replica has the state verified and a recorded checksum.");

    // step 3: commit
    let vs: Vec<Uuid> = in_tx!(s, |tx| tx.q("select id from version", &[]).await?.iter().map(|r| r.get(0)).collect());
    let s1 = dataset(&s, "/video/drive-clips", vs.iter().map(|v| Member::version(*v)).collect()).await?.snapshot;
    assert_eq!(member_ids(&s, s1).await?.len(), 1000, "Snapshot S1 exists with 1,000 members.");
    assert_eq!(head(&s, "/video/drive-clips").await?, s1, "The branch ref main points to S1.");

    // step 4: enrich
    in_tx!(s, |tx| metadata::register_enricher(&mut tx, "captioner", "1.0", "captions", false, "site-agent").await?);
    let rows: Vec<serde_json::Value> = vs.iter().map(|v| json!({"version": v, "selector": {"type": "media-fragment", "t": [0, 10]}, "text": "a clip"})).collect();
    let (set, _) = in_tx!(s, |tx| datasets::write_annotation_set(&mut tx, "/video/drive-clips", "captions", Some(("captioner", "1.0")), "time-span", &rows).await?);
    let s2 = in_tx!(s, |tx| datasets::commit_annotation_set(&mut tx, "/video/drive-clips", "main", set).await?).snapshot;
    let stored = in_tx!(s, |tx| datasets::annotation_rows(&tx, set).await?);
    assert!(stored.iter().all(|r| r["version"].is_string() && r["selector"]["t"].is_array()), "An annotation set \"captions\" exists. Its key is a version ID and a time span.");
    let m1 = member_ids(&s, s1).await?;
    let m2 = member_ids(&s, s2).await?;
    assert!(m1.is_subset(&m2) && m2.len() == 1001, "Snapshot S2 exists. S2 shares all 1,000 video members with S1.");
    let shared: i64 = in_tx!(s, |tx| tx
        .q1("select count(*) from manifest_ref a join manifest_ref b on a.manifest_id = b.manifest_id where a.snapshot_id = $1 and b.snapshot_id = $2", &[&s1, &s2])
        .await?
        .get(0));
    assert!(shared >= 1, "S2 shares the manifests of S1 (structural sharing)");
    assert!(stored.iter().all(|r| r["asserted_by"]["enricher"] == "captioner" && r["asserted_by"]["version"] == "1.0"), "Each caption records the enricher and its version.");

    // step 5: resolve
    let trainer = s.as_principal(Principal::user("trainer"));
    let r = resolver::resolve(&trainer, &train_request(vec![dataset_input("/video/drive-clips", Some("main"), "training")], "train", "research")).await?;
    let sets: Vec<String> = r.inputs[0]["annotation_sets"].as_array().unwrap().iter().map(|x| x["id"].as_str().unwrap().to_string()).collect();
    assert!(r.inputs[0]["snapshot"] == json!(s2) && sets == vec![set.to_string()], "A resolution exists. It names S2 and the version of \"captions\" exactly.");
    assert!(
        r.tasks.len() == 1000 && r.tasks.iter().all(|t| !t.url.is_empty() && t.selector.as_ref().map(|x| x["t"].is_array()).unwrap_or(false) && t.expected_bytes > 0 && t.expected_elements > 0),
        "The plan lists tasks with a replica URL, a time span, expected bytes and expected elements."
    );
    assert_eq!(r.decision["outcome"], "permitted", "The resolution holds a policy decision with the outcome permitted.");

    // step 6: read with Ray
    let rep = placement::read_plan(&trainer, "ray-job", &r).await?;
    assert!(rep.failures.is_empty() && rep.bytes > 0, "The job reads the bytes directly from \"lab-video\".");
    let from_lab = inst.store.accesses().iter().filter(|a| a.client == "ray-job" && a.bucket == res.bucket).count();
    assert_eq!(from_lab, 1000, "The job reads the bytes directly from \"lab-video\" (access log of the storage).");
    assert_eq!(inst.metrics.bytes_served_by_catalog.load(Ordering::SeqCst), 0, "mudcat served no data bytes");
    let act = rep.observed_activity.unwrap();
    let observed: i64 = in_tx!(s, |tx| tx.q1("select count(*) from usage where activity_id = $1 and capture = 'observed'", &[&act]).await?.get(0));
    assert!(observed >= 2, "mudcat records the usage with the capture mode observed.");

    // step 7: publish_model
    let mv = in_tx!(s, |tx| provenance::publish_model(&mut tx, &ModelSpec { path: "/models/drive-vlm".into(), content_hash: "ckpt-1".into(), size: 10, activity: Some(act), ..Default::default() }).await?);
    let lin = in_tx!(s, |tx| provenance::lineage(&tx, mv, &LineageQuery { direction: "ancestors".into(), ..Default::default() }).await?);
    let ids: BTreeSet<String> = lin["nodes"].as_array().unwrap().iter().map(|n| n["id"].as_str().unwrap().to_string()).collect();
    let act_node = lin["nodes"].as_array().unwrap().iter().find(|n| n["type"] == "activity").cloned().unwrap();
    assert!(
        ids.contains(&s2.to_string()) && ids.contains(&set.to_string()) && act_node["resolution"] == json!(r.id),
        "A model version exists. Its lineage leads to the resolution, to S2 and to \"captions\"."
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn uc1_02() -> Result<()> {
    let (_inst, s) = setup("uc1-02").await?;
    let (_vs, _s1, s2, _) = video_fixture(&s, 20).await?;
    in_tx!(s, |tx| {
        metadata::register_enricher(&mut tx, "scorer", "1.0", "quality", false, "site-agent").await?;
        metadata::register_enricher(&mut tx, "embedder", "1.0", "embeddings", false, "site-agent").await?;
    });
    stream::flush(&s).await?;
    let start: i64 = in_tx!(s, |tx| tx.q1("select last_position from stream_state", &[]).await?.get(0));
    let (q, _) = in_tx!(s, |tx| datasets::write_annotation_set(&mut tx, "/video/drive-clips", "quality scores", Some(("scorer", "1.0")), "version", &[json!({"score": 0.9})]).await?);
    let (e, _) = in_tx!(s, |tx| datasets::write_annotation_set(&mut tx, "/video/drive-clips", "embeddings", Some(("embedder", "1.0")), "version", &[json!({"embedding": [0.1, 0.2]})]).await?);
    // two enrichers commit at the same time; the requirement names only the branch
    let a = commit(&s, DatasetChange { dataset: "/video/drive-clips".into(), add: vec![Member::annotation_set(q)], ..Default::default() });
    let b = commit(&s, DatasetChange { dataset: "/video/drive-clips".into(), add: vec![Member::annotation_set(e)], ..Default::default() });
    let (ra, rb) = tokio::join!(a, b);
    assert!(ra.is_ok(), "The commit succeeds.");
    assert!(rb.is_ok(), "The commit succeeds with no retry by the client.");
    let newest = member_ids(&s, head(&s, "/video/drive-clips").await?).await?;
    assert!(newest.contains(&q) && newest.contains(&e), "The newest snapshot contains the two annotation sets.");
    // a removal with a requirement on S2
    let ten: Vec<Uuid> = member_ids(&s, s2).await?.into_iter().take(10).collect();
    let before = newest.clone();
    let r = commit(&s, DatasetChange { dataset: "/video/drive-clips".into(), requirement: Some(s2), remove: ten.clone(), ..Default::default() }).await;
    assert!(matches!(r, Err(mudcat::Error::Conflict { .. })), "mudcat rejects the commit with a conflict, because main does not point to S2.");
    assert_eq!(member_ids(&s, head(&s, "/video/drive-clips").await?).await?, before, "No member was removed.");
    // the subscriber
    stream::flush(&s).await?;
    let p = stream::poll(&s, start, 10_000, None, false).await?;
    let commits: Vec<&serde_json::Value> = p.events.iter().filter(|e| e["type"] == "mudcat.snapshot.committed.v1").collect();
    assert_eq!(commits.len(), 2, "The subscriber gets one snapshot-committed event for each successful commit.");
    let pos: Vec<i64> = p.events.iter().map(|e| e["sequence"].as_i64().unwrap()).collect();
    assert!(pos.windows(2).all(|w| w[1] == w[0] + 1), "The positions increase with no event out of order.");
    assert!(commits.iter().all(|c| c["data"]["summary"]["added"].is_number() && c["data"]["summary"]["added_bytes"].is_number() && c["data"]["summary"]["removed"].is_number()), "Each event carries a change summary with numbers and sizes.");
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn uc1_03() -> Result<()> {
    let (inst, s) = setup("uc1-03").await?;
    let lab = resource(&s, "lab-video", "cloud-a", "managed", false).await?;
    resource(&s, "gpu-cache", "cloud-b", "managed", false).await?;
    acl_root(&s, "/video", Some(("lab-video", "video/"))).await?;
    grant_collection(&s, "/video", "reader", "user:trainer").await?;
    source(&s, "fleet-2026", "instrument", json!({}), &[("train", "research")]).await?;
    let files: Vec<(String, Vec<u8>)> = (0..40).map(|i| (format!("c{i:03}.mp4"), file_bytes("stream", i, "x"))).collect();
    let regs = register_files(&s, &lab, "/video/clips", "video/clips/", &files, Some("fleet-2026"), None).await?;
    verify_all(&s, "lab-video").await?;
    dataset(&s, "/video/drive-clips", regs.iter().map(|r| Member::version(r.version_id)).collect()).await?;
    let s2 = publish_tag(&s, "/video/drive-clips", "v1").await?.id;

    // step 1: where is S2 complete and verified
    let on = in_tx!(s, |tx| placement::complete_on(&mut tx, s2).await?);
    assert_eq!(on, vec!["lab-video".to_string()], "The answer contains \"lab-video\" only.");

    // step 2: add_rule
    let rule = in_tx!(s, |tx| placement::add_rule(&mut tx, s2, &["gpu-cache".to_string()], 1, Some(chrono::Duration::days(14))).await?);
    assert_eq!(rule["transfers"], 40, "mudcat sends transfer requests to a mover.");
    let gpu = in_tx!(s, |tx| namespace::resource(&tx, "gpu-cache").await?.id);
    let c = in_tx!(s, |tx| placement::refresh_completeness(&mut tx, s2, gpu).await?);
    assert_eq!(c["complete"], false, "The completeness record for S2 on \"gpu-cache\" says not complete.");

    // step 3: 3 files arrive with wrong bytes
    placement::run_mover(&s, &MoverFault { corrupt: 3, stop_after: None }).await?;
    SiteAgent::new("agent-gpu", "gpu-cache").run(&s, 1000, AgentFault::None).await?;
    let suspect: i64 = in_tx!(s, |tx| tx.q1("select count(*) from replica where resource_id = $1 and state = 'suspect'", &[&gpu]).await?.get(0));
    assert_eq!(suspect, 3, "After verification, the 3 replicas have the state suspect.");
    let c = in_tx!(s, |tx| placement::refresh_completeness(&mut tx, s2, gpu).await?);
    assert_eq!(c["complete"], false, "The completeness record for S2 on \"gpu-cache\" still says not complete.");

    // step 4: resolve in cloud B
    let trainer = s.as_principal(Principal::user("trainer"));
    let mut req = train_request(vec![Input { snapshot: Some(s2), role: "training".into(), ..Default::default() }], "train", "research");
    req.place = Some("cloud-b".into());
    let r = resolver::resolve(&trainer, &req).await?;
    let on_gpu = r.tasks.iter().filter(|t| t.resource == "gpu-cache").count();
    let on_lab = r.tasks.iter().filter(|t| t.resource == "lab-video").count();
    assert_eq!(on_gpu, 37, "The plan uses \"gpu-cache\" for the verified replicas.");
    let states: std::collections::HashMap<Uuid, String> = in_tx!(s, |tx| tx.q("select id, state from replica", &[]).await?.iter().map(|r| (r.get(0), r.get(1))).collect());
    assert!(on_lab == 3 && r.tasks.iter().all(|t| states[&t.replica] == "verified"), "The plan does not use a suspect replica. For the 3 files, it uses \"lab-video\".");

    // step 5: 15 days pass
    inst.clock.advance(chrono::Duration::days(15));
    let ended = in_tx!(s, |tx| placement::expire_rules(&mut tx).await?);
    let locked: i64 = in_tx!(s, |tx| tx.q1("select count(*) from replica where resource_id = $1 and lock_until >= $2", &[&gpu, &tx.now]).await?.get(0));
    assert!(ended.len() == 1 && locked == 0, "The replicas on \"gpu-cache\" have no lock. mudcat can remove them.");
    let lab_ok: i64 = in_tx!(s, |tx| tx.q1("select count(*) from replica where resource_id = (select id from storage_resource where name = 'lab-video') and state = 'verified'", &[]).await?.get(0));
    assert_eq!(lab_ok, 40, "The replicas on \"lab-video\" did not change.");
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn uc1_04() -> Result<()> {
    let (inst, s) = setup("uc1-04").await?;
    // NOT ASSERTED: the scale of 100,000 clips; this test uses 200 clips and 20 partner clips.
    let store = resource(&s, "store", "cloud-a", "managed", false).await?;
    acl_root(&s, "/video/drive-clips-data", Some(("store", "clips/"))).await?;
    acl_root(&s, "/video/partner", Some(("store", "partner/"))).await?;
    grant_collection(&s, "/video/drive-clips-data", "reader", "user:ana").await?;
    source(&s, "fleet", "instrument", json!({}), &[("train", "research")]).await?;
    in_tx!(s, |tx| metadata::declare_attribute(&mut tx, &AttributeSpec { namespace: "tags".into(), name: "weather".into(), datatype: "string".into(), is_tag: true, permitted: Some(vec!["rain".into(), "clear".into()]), ..Default::default() }).await?);
    let files: Vec<(String, Vec<u8>)> = (0..200).map(|i| (format!("clip{i:03}.mp4"), file_bytes("stream", i, "c"))).collect();
    let clips = register_files(&s, &store, "/video/drive-clips-data", "clips/", &files, Some("fleet"), None).await?;
    let pfiles: Vec<(String, Vec<u8>)> = (0..20).map(|i| (format!("p{i:03}.mp4"), file_bytes("stream", i, "p"))).collect();
    let partner = register_files(&s, &store, "/video/partner", "partner/", &pfiles, Some("fleet"), None).await?;
    verify_all(&s, "store").await?;
    for (i, c) in clips.iter().enumerate() {
        let w = if i % 4 == 0 { "rain" } else { "clear" };
        in_tx!(s, |tx| metadata::tag(&mut tx, c.version_id, "weather", w).await?);
    }
    let mut members: Vec<Member> = clips.iter().map(|c| Member::version(c.version_id)).collect();
    members.extend(partner.iter().map(|c| Member::version(c.version_id)));
    dataset(&s, "/video/drive-clips", members).await?;
    let caption = |i: usize, partner: bool| if partner || i % 4 == 0 { "pedestrian crossing at night in rain" } else { "truck on a highway at noon" };
    let mut rows: Vec<serde_json::Value> = clips.iter().enumerate().map(|(i, c)| json!({"version": c.version_id, "selector": {"t": [0, 10]}, "text": caption(i, false)})).collect();
    rows.extend(partner.iter().enumerate().map(|(i, c)| json!({"version": c.version_id, "selector": {"t": [0, 10]}, "text": caption(i, true)})));
    let (set, _) = in_tx!(s, |tx| datasets::write_annotation_set(&mut tx, "/video/drive-clips", "captions", Some(("captioner", "1.0")), "time-span", &rows).await?);
    in_tx!(s, |tx| datasets::commit_annotation_set(&mut tx, "/video/drive-clips", "main", set).await?);
    search::build(&s).await?;
    // C7 is invalidated after the index was built; the index has not processed it
    let c7 = clips[8].version_id;
    in_tx!(s, |tx| policy::invalidate(&mut tx, "version", &c7.to_string(), "takedown", "counsel").await?);

    let ana = s.as_principal(Principal::user("ana"));
    let r = search::search(&ana, &SearchRequest { text: Some("pedestrain at night in rain".into()), limit: Some(50), ..Default::default() }).await?;
    let hits: Vec<Uuid> = r.hits.iter().filter_map(|h| h.version).collect();
    let rainy: BTreeSet<Uuid> = clips.iter().enumerate().filter(|(i, _)| i % 4 == 0).map(|(_, c)| c.version_id).collect();
    assert!(!hits.is_empty() && hits.iter().take(10).all(|h| rainy.contains(h)), "The results contain clips with the caption word \"pedestrian\", although the query has a spelling error.");
    assert!(r.ranking.contains("lexical") && r.ranking.contains("vector"), "The rank uses lexical match and vector similarity.");
    let partner_ids: BTreeSet<Uuid> = partner.iter().map(|p| p.version_id).collect();
    assert!(hits.iter().all(|h| !partner_ids.contains(h)), "No result is from /video/partner.");
    assert!(!hits.contains(&c7), "Clip C7 is not in the results.");

    let r2 = search::search(&ana, &SearchRequest { similar_to: Some(hits[0]), filter: Some(json!({"tag": {"name": "weather", "value": "rain"}})), limit: Some(20), ..Default::default() }).await?;
    let similar: Vec<Uuid> = r2.hits.iter().filter_map(|h| h.version).collect();
    let mut all_rain = !similar.is_empty();
    for v in &similar {
        let tags = in_tx!(s, |tx| metadata::effective_tags(&tx, *v).await?);
        all_rain &= tags.contains(&("weather".to_string(), "rain".to_string()));
    }
    assert!(all_rain, "The results are clips with a similar embedding, and all have the tag value rain.");

    let manifests_before: i64 = in_tx!(s, |tx| tx.q1("select count(*) from manifest", &[]).await?.get(0));
    let puts_before = inst.store.accesses().iter().filter(|a| a.op == "put").count();
    let sel = in_tx!(s, |tx| datasets::create_selection(&mut tx, "/video/drive-clips", "night-rain", &json!({"members": hits})).await?);
    assert!(sel["element_hash"].as_str().map(|h| h.len() == 64).unwrap_or(false), "A selection exists with the hash of its resolved element list.");
    let manifests_after: i64 = in_tx!(s, |tx| tx.q1("select count(*) from manifest", &[]).await?.get(0));
    assert!(manifests_after == manifests_before && inst.store.accesses().iter().filter(|a| a.op == "put").count() == puts_before, "mudcat copied no clips and no manifests.");
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn uc1_05() -> Result<()> {
    let (_inst, s) = setup("uc1-05").await?;
    let cn = resource(&s, "cn-store", "cn", "managed", false).await?;
    resource(&s, "eu-store", "eu", "managed", false).await?;
    acl_root(&s, "/video/cn", Some(("cn-store", "cn/"))).await?;
    grant_collection(&s, "/video/cn", "reader", "group:trainers#member").await?;
    grant(&s, "group", "trainers", "member", "user:trainer-eu").await?;
    grant(&s, "group", "trainers", "member", "user:trainer-cn").await?;
    source(&s, "fleet-cn", "instrument", json!({}), &[("train", "research")]).await?;
    in_tx!(s, |tx| {
        metadata::declare_attribute(&mut tx, &AttributeSpec { namespace: "tags".into(), name: "jurisdiction".into(), datatype: "string".into(), is_tag: true, permitted: Some(vec!["CN-export-controlled".into()]), ..Default::default() }).await?;
        metadata::set_tag_policy(&mut tx, "jurisdiction", "CN-export-controlled", "jurisdiction", json!({"allowed_locations": ["cn"]})).await?;
        let c = namespace::collection(&tx, "/video/cn").await?.id;
        metadata::tag(&mut tx, c, "jurisdiction", "CN-export-controlled").await?;
    });
    let files: Vec<(String, Vec<u8>)> = (0..10).map(|i| (format!("c{i}.mp4"), file_bytes("stream", i, "cn"))).collect();
    let regs = register_files(&s, &cn, "/video/cn", "cn/", &files, Some("fleet-cn"), None).await?;
    verify_all(&s, "cn-store").await?;
    let ds = dataset(&s, "/video/cn/clips", regs.iter().map(|r| Member::version(r.version_id)).collect()).await?;

    let eu = s.as_principal(Principal::user("trainer-eu"));
    let creds_before: i64 = in_tx!(s, |tx| tx.q1("select count(*) from credential_log", &[]).await?.get(0));
    let deny_before = audit_count(&s, "resolution", "deny").await?;
    let mut req = train_request(vec![dataset_input("/video/cn/clips", Some("main"), "training")], "train", "research");
    req.place = Some("eu".into());
    let r = resolver::resolve(&eu, &req).await;
    let e = r.err().expect("mudcat refuses the resolution.");
    assert!(e.code() == "jurisdiction" && e.to_string().contains("CN-export-controlled"), "mudcat refuses the resolution. The error names the jurisdiction rule. ({e})");
    let creds_after: i64 = in_tx!(s, |tx| tx.q1("select count(*) from credential_log", &[]).await?.get(0));
    assert_eq!(creds_after, creds_before, "mudcat issues no credentials.");
    assert_eq!(audit_count(&s, "resolution", "deny").await?, deny_before + 1, "The audit log has the refusal.");

    let cn_user = s.as_principal(Principal::user("trainer-cn"));
    req.place = Some("cn".into());
    let r = resolver::resolve(&cn_user, &req).await?;
    assert!(!r.tasks.is_empty(), "The resolution succeeds.");
    assert!(
        r.credentials.len() == 1 && matches!(&r.credentials[0].credential, Credential::Prefix { prefix, .. } if prefix == "cn/"),
        "The credentials have the scope of one prefix that is completely inside the ACL root of /video/cn."
    );
    let rule = in_tx!(s, |tx| placement::add_rule(&mut tx, ds.dataset, &["eu-store".to_string()], 1, None).await);
    assert!(matches!(&rule, Err(e) if e.code() == "jurisdiction"), "mudcat rejects the rule, because the data cannot leave the jurisdiction.");
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn uc1_06() -> Result<()> {
    let (_inst, s) = setup("uc1-06").await?;
    let (_vs, _s1, s2, set1) = video_fixture(&s, 10).await?;
    let trainer = s.as_principal(Principal::user("trainer"));
    let r1 = resolver::resolve(&trainer, &train_request(vec![dataset_input("/video/drive-clips", Some("main"), "training")], "train", "research")).await?;
    assert_eq!(r1.inputs[0]["snapshot"], json!(s2));
    // captioner 2.0 runs; during the run readers still get version 1
    in_tx!(s, |tx| metadata::register_enricher(&mut tx, "captioner", "2.0", "captions", false, "site-agent").await?);
    let vs: Vec<Uuid> = in_tx!(s, |tx| tx.q("select id from version", &[]).await?.iter().map(|r| r.get(0)).collect());
    let rows: Vec<serde_json::Value> = vs.iter().map(|v| json!({"version": v, "selector": {"t": [0, 10]}, "text": "better caption"})).collect();
    let (set2, n2) = in_tx!(s, |tx| datasets::write_annotation_set(&mut tx, "/video/drive-clips", "captions", Some(("captioner", "2.0")), "time-span", &rows).await?);
    let during = resolver::resolve(&trainer, &train_request(vec![dataset_input("/video/drive-clips", Some("main"), "training")], "train", "research")).await?;
    assert_eq!(during.inputs[0]["annotation_sets"][0]["id"], json!(set1), "During the run, readers still get \"captions\" version 1.");
    let s3 = in_tx!(s, |tx| datasets::commit_annotation_set(&mut tx, "/video/drive-clips", "main", set2).await?).snapshot;
    let m3 = member_ids(&s, s3).await?;
    let stored = in_tx!(s, |tx| datasets::annotation_rows(&tx, set2).await?);
    assert!(n2 == 2 && m3.contains(&set2) && !m3.contains(&set1) && stored.iter().all(|r| r["asserted_by"]["version"] == "2.0"), "After the run, a new snapshot holds \"captions\" version 2. Each value records the version 2.0.");
    // the auditor resolves R1 again
    let again = resolver::resolve_again(&trainer, r1.id).await?;
    assert_eq!(again.inputs[0]["annotation_sets"][0]["id"], json!(set1), "The result names \"captions\" version 1, as R1 did.");
    // a describer whose output needs review
    in_tx!(s, |tx| metadata::register_enricher(&mut tx, "describer", "1.0", "descriptions", true, "site-agent").await?);
    let did = in_tx!(s, |tx| datasets::dataset_id(&tx, "/video/drive-clips").await?);
    let w = in_tx!(s, |tx| metadata::write(&mut tx, &WriteMetadata { entity: did, namespace: "descriptions".into(), payload: json!({"description": "zebracorn dashcam footage"}), origin: "model".into(), enricher: Some("describer".into()), enricher_version: Some("1.0".into()), ..Default::default() }).await?);
    assert_eq!(w.status, "proposed", "The description has the status proposed.");
    search::build(&s).await?;
    let r = search::search(&trainer, &SearchRequest { text: Some("zebracorn".into()), ..Default::default() }).await?;
    assert!(!r.hits.iter().any(|h| h.entity == did), "A search by a normal user does not match the proposed description.");
    let curator = s.as_principal(Principal::user("curator"));
    in_tx!(curator, |tx| metadata::review(&mut tx, did, "descriptions", true).await?);
    let p = in_tx!(s, |tx| metadata::payloads(&tx, did, false).await?);
    let d = &p["descriptions"];
    assert!(d["status"] == "visible" && d["origin"] == "model" && d["enricher"] == "describer" && d["reviewed_by"] == "user:curator", "The description is visible, and records that a model wrote it and that a person accepted it. ({d})");
    search::build(&s).await?;
    let r = search::search(&trainer, &SearchRequest { text: Some("zebracorn".into()), ..Default::default() }).await?;
    assert!(r.hits.iter().any(|h| h.entity == did), "after the review the description is searchable");
    let _ = sys(&s);
    Ok(())
}
