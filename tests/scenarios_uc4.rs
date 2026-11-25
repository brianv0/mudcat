//! Reference scenarios of use case 4: governed retrieval for assistants
//! (evals/scenarios/uc4-retrieval.yaml). Each test follows the steps of one scenario and
//! asserts each `expect` statement.

use http_body_util::BodyExt;
use mudcat::datasets::Member;
use mudcat::evals::fixtures::*;
use mudcat::in_tx;
use mudcat::instance::{Instance, Principal, Session, TenantInfo};
use mudcat::namespace::{self, Register};
use mudcat::provenance::{self, RunSpec};
use mudcat::search::{self, SearchRequest};
use mudcat::{metadata, policy, resolver, stream, Result};
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use tower::ServiceExt;
use uuid::Uuid;

// ---------------------------------------------------------------------------- helpers

fn db_url() -> String {
    std::env::var("MUDCAT_TEST_DB").unwrap_or_else(|_| "postgres://mudcat:mudcat@localhost:54329/mudcat_scenarios_uc45".into())
}

/// Apply the schema once for the process, on a thread of its own, before the tests use it.
fn schema_once() {
    static ONCE: std::sync::OnceLock<()> = std::sync::OnceLock::new();
    ONCE.get_or_init(|| {
        std::thread::spawn(|| {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async { Instance::open(&db_url(), "target/scenario-data").await.expect("apply the schema") });
        })
        .join()
        .unwrap();
    });
}

async fn setup(name: &str) -> (Arc<Instance>, TenantInfo, Session) {
    schema_once();
    let base = Instance::open_with(&db_url(), "target/scenario-data", false).await.expect("open the instance");
    let short = &Uuid::new_v4().simple().to_string()[..8];
    let inst = base.fork(format!("target/scenario-data/{name}-{short}").into());
    let t = mudcat::tenants::create_tenant(&inst, &format!("{name}-{short}"), "user:admin").await.expect("create a tenant");
    let admin = inst.session(t.id, Principal::user("admin"));
    (inst, t, admin)
}

async fn tenant(inst: &Arc<Instance>, name: &str) -> (TenantInfo, Session) {
    let short = &Uuid::new_v4().simple().to_string()[..8];
    let t = mudcat::tenants::create_tenant(inst, &format!("{name}-{short}"), "user:admin").await.expect("create a tenant");
    let s = inst.session(t.id, Principal::user("admin"));
    (t, s)
}

async fn http(router: &axum::Router, method: &str, uri: &str, host: &str, token: Option<&str>, body: Option<Value>) -> (u16, Value) {
    let mut b = axum::http::Request::builder().method(method).uri(uri).header("host", host);
    if let Some(t) = token {
        b = b.header("authorization", format!("Bearer {t}"));
    }
    let req = match body {
        Some(v) => b.header("content-type", "application/json").body(axum::body::Body::from(v.to_string())),
        None => b.body(axum::body::Body::empty()),
    }
    .unwrap();
    let resp = router.clone().oneshot(req).await.unwrap();
    let status = resp.status().as_u16();
    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    (status, serde_json::from_slice(&bytes).unwrap_or(Value::Null))
}

/// Bring the search index up to date with every committed change.
async fn build_index(s: &Session) -> Result<()> {
    stream::flush(s).await?;
    search::build(s).await?;
    Ok(())
}

async fn position(s: &Session) -> Result<i64> {
    stream::flush(s).await?;
    Ok(in_tx!(s, |tx| tx.q1("select last_position from stream_state", &[]).await?.get::<_, i64>(0)))
}

/// All the events after a position, as the identity of the session sees them.
async fn poll_all(s: &Session, from: i64, sub: Option<Uuid>, state: bool) -> Result<(Vec<Value>, i64)> {
    stream::flush(s).await?;
    let mut pos = from;
    let mut out = Vec::new();
    loop {
        let p = stream::poll(s, pos, 2000, sub, state).await?;
        out.extend(p.events);
        if p.next == pos {
            break;
        }
        pos = p.next;
    }
    Ok((out, pos))
}

fn doc_text(i: usize, hr: bool) -> String {
    if hr {
        format!("Document {i} of human resources. Holiday policy for contractors: contractors get {} days of holiday.", 10 + i % 5)
    } else {
        format!("Document {i} of engineering. Build notes for the bridge project, part {i}.")
    }
}

/// Chunk rows of an annotation set: each row has an ID, the document version, a byte-range
/// selector into it, the text and the version of the embedding model.
fn chunks(version: Uuid, text: &str, n: usize) -> Vec<Value> {
    let len = text.len();
    (0..n)
        .map(|k| {
            let (a, b) = (k * len / n, (k + 1) * len / n);
            json!({"id": Uuid::now_v7(), "version": version, "selector": {"type": "oa:DataPositionSelector", "start": a, "end": b},
                   "text": &text[a..b.max(a)], "embedding_model": search::EMBED_MODEL, "embedding": search::embed(&text[a..b])})
        })
        .collect()
}

// ---------------------------------------------------------------------------- UC4-01

/// Build a retrieval index from the change stream, and cite sources.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn uc4_01() -> Result<()> {
    let (inst, _t, admin) = setup("uc4-01").await;
    let store = resource(&admin, "docs", "eu", "managed", false).await?;
    acl_root(&admin, "/docs/hr", Some(("docs", "hr/"))).await?;
    acl_root(&admin, "/docs/eng", Some(("docs", "eng/"))).await?;
    grant_collection(&admin, "/docs/hr", "reader", "user:maria").await?;
    grant_collection(&admin, "/docs/eng", "reader", "user:otto").await?;
    grant_collection(&admin, "/docs/hr", "reader", "service:retrieval").await?;
    grant_collection(&admin, "/docs/eng", "reader", "service:retrieval").await?;
    // given: documents in two collections with different readers (500 here, 5,000 in the scenario)
    let n = 250;
    let mut versions = Vec::new();
    let mut texts = BTreeMap::new();
    for (coll, prefix, hr) in [("/docs/hr", "hr/", true), ("/docs/eng", "eng/", false)] {
        let files: Vec<(String, Vec<u8>)> = (0..n).map(|i| (format!("doc-{i:04}.txt"), doc_text(i, hr).into_bytes())).collect();
        for r in register_files(&admin, &store, coll, prefix, &files, None, None).await? {
            versions.push((r.version_id, coll));
        }
        for (i, (_, d)) in files.iter().enumerate() {
            texts.insert(versions[versions.len() - n + i].0, String::from_utf8(d.clone()).unwrap());
        }
    }
    in_tx!(admin, |tx| metadata::register_enricher(&mut tx, "chunker", "1.0", "chunks", false, "site-agent").await?);

    // step 1: the chunk enricher writes the annotation set "chunks", keyed by byte range
    let members: Vec<Member> = versions.iter().map(|v| Member::version(v.0)).collect();
    dataset(&admin, "/docs/all", members).await?;
    let rows: Vec<Value> = versions.iter().flat_map(|(v, _)| chunks(*v, &texts[v], 2)).collect();
    let enricher = inst.session(admin.tenant_id()?, Principal::service("chunker"));
    let (set, _) = in_tx!(enricher, |tx| mudcat::datasets::write_annotation_set(&mut tx, "/docs/all", "chunks", Some(("chunker", "1.0")), "byte-range", &rows).await?);
    in_tx!(admin, |tx| mudcat::datasets::commit_annotation_set(&mut tx, "/docs/all", "main", set).await?);
    let stored = in_tx!(admin, |tx| mudcat::datasets::annotation_rows(&tx, set).await?);
    let all_versions: BTreeSet<Uuid> = versions.iter().map(|v| v.0).collect();
    assert!(
        stored.iter().all(|r| r["selector"]["type"] == "oa:DataPositionSelector" && r["version"].as_str().and_then(|s| s.parse::<Uuid>().ok()).map(|v| all_versions.contains(&v)).unwrap_or(false)),
        "Each chunk has a selector into one document version."
    );
    assert!(
        stored.iter().all(|r| r["embedding_model"] == search::EMBED_MODEL && r["asserted_by"]["enricher"] == "chunker" && r["asserted_by"]["version"] == "1.0"),
        "Each embedding records the version of the embedding model."
    );

    // step 2: the retrieval service subscribes to the state documents of /docs
    let service = inst.session(admin.tenant_id()?, Principal::service("retrieval"));
    let sub = stream::subscribe(&service, "/docs", "state").await?;
    let owner: String = in_tx!(admin, |tx| tx.q1("select principal from subscription where id = $1", &[&sub]).await?.get(0));
    assert_eq!(owner, "service:retrieval", "The subscription has the identity of the service.");

    // step 3: backfill
    let (pos, docs) = stream::backfill(&service, Some(sub)).await?;
    let artifacts: BTreeSet<String> = docs.iter().filter(|d| d["kind"] == "artifact").filter_map(|d| d["id"].as_str().map(|s| s.to_string())).collect();
    let readable: i64 = in_tx!(admin, |tx| tx
        .q1("select count(*) from artifact a join collection c on c.id = a.collection_id where c.path in ('/docs/hr', '/docs/eng')", &[])
        .await?
        .get(0));
    assert_eq!(artifacts.len() as i64, readable, "The service gets one state document for each entity that its identity can read.");
    assert!(
        docs.iter().filter(|d| d["kind"] == "artifact").all(|d| d["access"]["acl_root"].is_string()),
        "Each state document has the access part, with the ACL root."
    );
    // the live stream continues from the position of the backfill
    let late = vec![("late.txt".to_string(), b"a document that comes after the backfill".to_vec())];
    let reg = register_files(&admin, &store, "/docs/eng", "eng/", &late, None, None).await?[0].clone();
    let (events, _) = poll_all(&service, pos, Some(sub), true).await?;
    assert!(events.iter().any(|d| d["id"] == json!(reg.artifact_id)), "After the backfill, the live stream continues with no gap.");

    // step 4: the assistant searches for maria
    build_index(&admin).await?;
    let maria = inst.session(admin.tenant_id()?, Principal::user("maria"));
    let r = search::search(&maria, &SearchRequest { text: Some("holiday policy for contractors".into()), ..Default::default() }).await?;
    assert!(!r.hits.is_empty(), "the search finds chunks");
    let hr: BTreeSet<Uuid> = versions.iter().filter(|v| v.1 == "/docs/hr").map(|v| v.0).collect();
    assert!(r.hits.iter().all(|h| h.version.map(|v| hr.contains(&v)).unwrap_or(false)), "The results contain only chunks of documents that maria can read.");

    // step 5: the assistant records its answer, with the chunks that it used
    let used: Vec<Value> = r.hits.iter().take(3).map(|h| json!({"entity": h.version, "role": "retrieval", "selector": h.matched["selector"]})).collect();
    let assistant = inst.session(admin.tenant_id()?, Principal::service("assistant"));
    let answer = in_tx!(assistant, |tx| provenance::record_run(&mut tx, &RunSpec { kind: "answer".into(), used: used.clone(), ..Default::default() }).await?);
    let cited = in_tx!(admin, |tx| provenance::cited_inputs(&tx, answer).await?);
    assert_eq!(cited.len(), used.len(), "The answer refers to the exact document versions.");
    assert!(
        cited.iter().all(|(v, sel, role)| role == "retrieval" && used.iter().any(|u| u["entity"] == json!(v) && Some(&u["selector"]) == sel.as_ref())),
        "The answer refers to the exact document versions and the selectors of the chunks."
    );
    Ok(())
}

// ---------------------------------------------------------------------------- UC4-02

/// A person loses access, and the index does not know it yet.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn uc4_02() -> Result<()> {
    let (inst, _t, admin) = setup("uc4-02").await;
    let store = resource(&admin, "docs", "eu", "managed", false).await?;
    acl_root(&admin, "/docs/projects", Some(("docs", "projects/"))).await?;
    grant_collection(&admin, "/docs/projects", "reader", "group:contractors#member").await?;
    grant(&admin, "group", "contractors", "member", "user:ken").await?;
    grant_collection(&admin, "/docs/projects", "reader", "service:retrieval").await?;
    let files = vec![("budget.txt".to_string(), b"project budget for the new bridge".to_vec())];
    let doc = register_files(&admin, &store, "/docs/projects", "projects/", &files, None, None).await?[0].clone();
    in_tx!(admin, |tx| metadata::write(&mut tx, &metadata::WriteMetadata { entity: doc.version_id, namespace: "dc".into(), payload: json!({"description": "project budget"}), ..Default::default() }).await?);
    verify_all(&admin, "docs").await?;
    // given: the index of the retrieval service has a copy from one hour ago; mudcat's index too
    build_index(&admin).await?;
    let service = inst.session(admin.tenant_id()?, Principal::service("retrieval"));
    let sub = stream::subscribe(&service, "/docs", "change").await?;
    let service_pos = position(&admin).await?;
    let before = service_pos;

    // step 1: the admin removes ken from the group
    revoke(&admin, "group", "contractors", "member", "user:ken").await?;
    let (events, _) = poll_all(&admin, before, None, false).await?;
    assert!(
        events.iter().any(|e| e["type"] == "mudcat.relationship.removed.v1" && e["data"]["tuple"] == "group:contractors#member@user:ken"),
        "The change stream has an event for the removed relationship."
    );

    // step 2: ken searches immediately; the search index has not processed the change
    let ken = inst.session(admin.tenant_id()?, Principal::user("ken"));
    let r = search::search(&ken, &SearchRequest { text: Some("project budget".into()), ..Default::default() }).await?;
    assert!(r.hits.iter().all(|h| h.version != Some(doc.version_id)), "mudcat returns no result from /docs/projects.");
    assert_eq!(inst.metrics.cached_decisions.load(std::sync::atomic::Ordering::SeqCst), 0, "mudcat did not use a cached decision for this check.");

    // step 3: ken resolves the document
    let creds_before: i64 = in_tx!(admin, |tx| tx.q1("select count(*) from credential_log", &[]).await?.get(0));
    let res = resolver::resolve(&ken, &resolver::Request { inputs: vec![resolver::Input { artifact: Some("/docs/projects/budget.txt".into()), role: "retrieval".into(), ..Default::default() }], ..Default::default() }).await;
    let creds_after: i64 = in_tx!(admin, |tx| tx.q1("select count(*) from credential_log", &[]).await?.get(0));
    assert!(matches!(&res, Err(e) if e.code() == "access-denied"), "mudcat denies the request: {res:?}");
    assert_eq!(creds_before, creds_after, "mudcat issues no credentials.");

    // step 4: the retrieval service polls from its position
    let (events, _) = poll_all(&service, service_pos, Some(sub), false).await?;
    assert!(
        events.iter().any(|e| e["type"] == "mudcat.relationship.removed.v1" && e["data"]["tuple"].as_str().map(|t| t.contains("user:ken")).unwrap_or(false)),
        "The service gets the event and can update its copy of the permissions."
    );

    // step 5: the auditor asks for the decisions about ken after the revocation
    let denials: Vec<(String, String)> = in_tx!(admin, |tx| tx
        .q("select point, object from audit where principal = 'user:ken' and decision = 'deny' order by id", &[])
        .await?
        .iter()
        .map(|r| (r.get(0), r.get(1)))
        .collect());
    assert!(denials.iter().any(|d| d.0 == "search") && denials.iter().any(|d| d.0 == "resolution"), "The audit log has each denial: {denials:?}");
    Ok(())
}

// ---------------------------------------------------------------------------- UC4-03

/// A document is removed, and one consumer is behind.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn uc4_03() -> Result<()> {
    let (inst, _t, admin) = setup("uc4-03").await;
    let store = resource(&admin, "docs", "eu", "managed", false).await?;
    resource(&admin, "docs-copy", "eu", "managed", false).await?;
    acl_root(&admin, "/docs", Some(("docs", "docs/"))).await?;
    let text = "Retention schedule of the archive: the unique phrase zebra-quartz-lantern appears only in document D. ".repeat(3);
    let files = vec![("D.txt".to_string(), text.clone().into_bytes()), ("E.txt".to_string(), b"another document about archives".to_vec())];
    let regs = register_files(&admin, &store, "/docs", "docs/", &files, None, None).await?;
    let d = regs[0].clone();
    // a second replica of D
    let copy = in_tx!(admin, |tx| namespace::resource(&tx, "docs-copy").await?);
    inst.store.put("mover", &copy.bucket, "docs/D.txt", text.as_bytes())?;
    in_tx!(admin, |tx| namespace::add_replica(&mut tx, d.version_id, &copy, &copy.url("docs/D.txt"), "registered", None, None).await?);
    verify_all(&admin, "docs").await?;
    verify_all(&admin, "docs-copy").await?;
    dataset(&admin, "/docs/set", regs.iter().map(|r| Member::version(r.version_id)).collect()).await?;
    let rows = chunks(d.version_id, &text, 12);
    let chunk_ids: BTreeSet<String> = rows.iter().map(|r| r["id"].as_str().unwrap().to_string()).collect();
    let (set, _) = in_tx!(admin, |tx| mudcat::datasets::write_annotation_set(&mut tx, "/docs/set", "chunks", None, "byte-range", &rows).await?);
    in_tx!(admin, |tx| mudcat::datasets::commit_annotation_set(&mut tx, "/docs/set", "main", set).await?);
    build_index(&admin).await?;
    // consumer B stopped two hours ago; consumer A is current
    let pos_b = position(&admin).await?;
    let mut index_a: BTreeSet<String> = BTreeSet::new();
    index_a.insert(d.artifact_id.to_string());
    index_a.insert(d.version_id.to_string());
    index_a.extend(chunk_ids.iter().cloned());
    inst.clock.advance(chrono::Duration::hours(2));
    let pos_a = position(&admin).await?;

    // step 1: the records officer invalidates D
    in_tx!(admin, |tx| policy::invalidate(&mut tx, "artifact", &d.artifact_id.to_string(), "retention period ended", "records-officer").await?);
    let (events, _) = poll_all(&admin, pos_a, None, false).await?;
    let removed: BTreeSet<String> = events.iter().filter(|e| e["removal"] == true).filter_map(|e| e["subject"].as_str().map(|s| s.to_string())).collect();
    assert!(removed.contains(&d.artifact_id.to_string()), "The stream has a removal event for D.");
    assert!(chunk_ids.is_subset(&removed), "The stream has a removal event for its 12 chunks ({} of 12).", chunk_ids.intersection(&removed).count());

    // step 2: consumer A polls and deletes D and its chunks from its index
    let (events_a, _) = poll_all(&admin, pos_a, None, false).await?;
    for e in &events_a {
        if e["removal"] == true {
            index_a.remove(e["subject"].as_str().unwrap_or(""));
        }
    }
    assert!(index_a.is_empty(), "Consumer A gets the removal events, and deletes D and its chunks from its index.");

    // step 3: a search for a phrase that only D contains; the index of mudcat is not rebuilt
    let r = search::search(&admin, &SearchRequest { text: Some("zebra-quartz-lantern".into()), ..Default::default() }).await?;
    assert!(r.hits.iter().all(|h| h.version != Some(d.version_id)), "mudcat returns no chunk of D, also when the search index of mudcat has not processed the removal.");

    // step 4: consumer B polls from its old position
    let (events_b, _) = poll_all(&admin, pos_b, None, false).await?;
    let positions: Vec<i64> = events_b.iter().map(|e| e["sequence"].as_i64().unwrap()).collect();
    assert!(positions.windows(2).all(|w| w[1] == w[0] + 1) && positions.first() == Some(&(pos_b + 1)), "Consumer B gets all the events that it missed, in order");
    let removed_b: BTreeSet<String> = events_b.iter().filter(|e| e["removal"] == true).filter_map(|e| e["subject"].as_str().map(|s| s.to_string())).collect();
    assert!(chunk_ids.is_subset(&removed_b) && removed_b.contains(&d.artifact_id.to_string()), "the removal events included.");

    // step 5: consumer B's position is older than the retention period of change events
    inst.clock.advance(inst.config.event_retention + chrono::Duration::days(1));
    stream::prune(&admin).await?;
    let err = stream::poll(&admin, pos_b, 100, None, false).await;
    assert!(matches!(&err, Err(e) if e.code() == "position-too-old"), "mudcat tells consumer B that its position is too old.");
    let (_, docs) = stream::backfill(&admin, None).await?;
    assert!(!docs.iter().any(|x| x["id"] == json!(d.artifact_id)), "After a backfill, consumer B has no state document for D.");

    // step 6: purge D
    let out = in_tx!(admin, |tx| policy::purge(&mut tx, &[d.version_id], "retention period ended").await?);
    assert!(out.blocked_by_hold.is_none());
    run_agent(&admin, "docs").await?;
    run_agent(&admin, "docs-copy").await?;
    let (receipts, replicas): (i64, i64) = in_tx!(admin, |tx| {
        let a: i64 = tx.q1("select count(*) from deletion_receipt where version_id = $1", &[&d.version_id]).await?.get(0);
        let b: i64 = tx.q1("select count(*) from replica where version_id = $1", &[&d.version_id]).await?.get(0);
        (a, b)
    });
    assert!(receipts == 2 && replicas == 2, "Each replica of D is deleted, with one receipt for each.");
    assert!(!inst.store.exists(&store.bucket, "docs/D.txt") && !inst.store.exists(&copy.bucket, "docs/D.txt"), "the bytes of each replica are gone");
    Ok(())
}

// ---------------------------------------------------------------------------- UC4-04

/// A consumer outside mudcat that applies an event only if its revision is newer.
#[derive(Default)]
struct Consumer {
    docs: BTreeMap<String, i64>,
    registrations: usize,
    paths: BTreeSet<String>,
    max_seen: i64,
    lower: bool,
}

impl Consumer {
    fn apply(&mut self, events: &[Value], replay: bool) -> usize {
        let mut dup = 0;
        for e in events {
            let p = e["sequence"].as_i64().unwrap();
            if p <= self.max_seen && !replay {
                self.lower = true;
            }
            self.max_seen = self.max_seen.max(p);
            if e["type"] != "mudcat.artifact.registered.v1" {
                continue;
            }
            let id = e["subject"].as_str().unwrap().to_string();
            let rev = e["revision"].as_i64().unwrap();
            match self.docs.get(&id) {
                Some(r) if *r >= rev => dup += 1,
                _ => {
                    self.docs.insert(id, rev);
                    self.registrations += 1;
                    self.paths.insert(e["data"]["path"].as_str().unwrap_or("").to_string());
                }
            }
        }
        dup
    }
}

/// Many writers, and a consumer that must not miss an event.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn uc4_04() -> Result<()> {
    let (inst, t, admin) = setup("uc4-04").await;
    in_tx!(admin, |tx| namespace::ensure_collection(&mut tx, "/docs").await?);
    let start = position(&admin).await?;
    let writers = 20usize;
    let per = 500usize;
    let rolled_back = Arc::new(parking_lot::Mutex::new(Vec::<String>::new()));
    let mut handles = Vec::new();
    for w in 0..writers {
        let s = inst.session(t.id, Principal::user("admin"));
        let rb = rolled_back.clone();
        handles.push(tokio::spawn(async move {
            let mut written = 0;
            let mut k = 0;
            while written < per {
                let n = (10 + (w * 7 + k * 11) % 30).min(per - written).max(8);
                let n = n.min(per - written);
                let rollback = k % 5 == 2;
                let regs: Vec<Register> = (0..n.max(1))
                    .map(|i| {
                        let path = if rollback { format!("/docs/rb-{w}-{k}-{i}") } else { format!("/docs/w{w:02}-{:04}", written + i) };
                        Register { path: path.clone(), content_hash: Some(mudcat::instance::sha256_hex(path.as_bytes())), size: 10, ..Default::default() }
                    })
                    .collect();
                let mut c = s.client().await?;
                let mut tx = s.begin(&mut c).await?;
                namespace::register_batch(&mut tx, &regs).await?;
                // long and short transactions overlap
                tokio::time::sleep(std::time::Duration::from_millis(((w + k) % 4 * 10) as u64)).await;
                if rollback {
                    rb.lock().extend(regs.iter().map(|r| r.path.clone()));
                    drop(tx);
                } else {
                    tx.commit().await?;
                    written += regs.len();
                }
                k += 1;
            }
            Ok::<_, mudcat::Error>(())
        }));
    }
    // the consumer polls each 100 ms, applies events by revision, and stores its position
    let consumer = inst.session(t.id, Principal::user("admin"));
    let mut pos = start;
    let mut stored;
    let mut c = Consumer::default();
    c.max_seen = start;
    loop {
        let finished = handles.iter().all(|h| h.is_finished());
        let p = stream::poll(&consumer, pos, 1000, None, false).await?;
        c.apply(&p.events, false);
        pos = p.next;
        stored = pos;
        if finished {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    for h in handles {
        h.await.unwrap()?;
    }
    // step 2: until the stream is empty
    stream::flush(&consumer).await?;
    loop {
        let p = stream::poll(&consumer, pos, 2000, None, false).await?;
        let n = p.events.len();
        c.apply(&p.events, false);
        pos = p.next;
        if n == 0 {
            break;
        }
    }
    let rb = rolled_back.lock().clone();
    assert!(!rb.is_empty() && rb.iter().all(|p| !c.paths.contains(p)), "Each registration that commits makes one event. A registration that rolls back makes no event.");
    assert!(!c.lower, "The consumer never gets an event with a position lower than a position that it got before.");
    assert_eq!(c.registrations, writers * per, "At the end, the consumer has exactly 10,000 registration events.");

    // step 3: the consumer stops, and starts again from a stored position that is behind
    let restart_from = stored.min(pos) - 50;
    let mut dups = 0;
    let mut p2 = restart_from;
    let mut positions = Vec::new();
    loop {
        let p = stream::poll(&consumer, p2, 2000, None, false).await?;
        let n = p.events.len();
        positions.extend(p.events.iter().map(|e| e["sequence"].as_i64().unwrap()));
        dups += c.apply(&p.events, true);
        p2 = p.next;
        if n == 0 {
            break;
        }
    }
    assert!(positions.first() == Some(&(restart_from + 1)) && positions.windows(2).all(|w| w[1] == w[0] + 1), "The consumer gets no gap. It can get some events a second time.");
    assert_eq!(c.registrations, writers * per, "With the revision of each event, the consumer applies each change one time only ({dups} came again).");

    // step 4: one document a second time, with the same content
    let versions_before: i64 = in_tx!(admin, |tx| tx.q1("select count(*) from version", &[]).await?.get(0));
    let path = "/docs/w00-0000".to_string();
    let again = Register { path: path.clone(), content_hash: Some(mudcat::instance::sha256_hex(path.as_bytes())), size: 10, ..Default::default() };
    let r = in_tx!(admin, |tx| namespace::register_batch(&mut tx, &[again]).await?);
    let versions_after: i64 = in_tx!(admin, |tx| tx.q1("select count(*) from version", &[]).await?.get(0));
    let (events, _) = poll_all(&admin, pos, None, false).await?;
    assert!(!r[0].created && versions_after == versions_before, "No new version exists");
    assert!(!events.iter().any(|e| e["type"] == "mudcat.version.registered.v1"), "and the stream has no second registration event.");
    Ok(())
}

// ---------------------------------------------------------------------------- UC4-05

/// A subscription must see only what its identity can read.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn uc4_05() -> Result<()> {
    let (inst, t1, admin) = setup("uc4-05").await;
    let (t2, admin2) = tenant(&inst, "uc4-05-t2").await;
    let store = resource(&admin, "docs", "eu", "managed", false).await?;
    acl_root(&admin, "/docs/hr", Some(("docs", "hr/"))).await?;
    acl_root(&admin, "/docs/eng", Some(("docs", "eng/"))).await?;
    grant_collection(&admin, "/docs/eng", "reader", "service:indexer").await?;
    in_tx!(admin, |tx| metadata::declare_attribute(&mut tx, &metadata::AttributeSpec { namespace: "hr".into(), name: "salary_band".into(), datatype: "string".into(), custody: Some("customer-held".into()), ..Default::default() }).await?);
    let s2 = resource(&admin2, "docs", "eu", "managed", false).await?;
    let other = register_files(&admin2, &s2, "/docs/eng", "eng/", &[("secret.txt".to_string(), b"tenant 2".to_vec())], None, None).await?[0].clone();
    let subscriber = inst.session(t1.id, Principal::service("indexer"));
    let sub = stream::subscribe(&subscriber, "/docs", "change").await?;

    // step 1: one document in each collection, each with a customer-held salary band
    let hr = register_files(&admin, &store, "/docs/hr", "hr/", &[("pay.txt".to_string(), b"pay scales".to_vec())], None, None).await?[0].clone();
    let eng = register_files(&admin, &store, "/docs/eng", "eng/", &[("design.txt".to_string(), b"design notes".to_vec())], None, None).await?[0].clone();
    for v in [hr.version_id, eng.version_id] {
        in_tx!(admin, |tx| metadata::write(&mut tx, &metadata::WriteMetadata { entity: v, namespace: "hr".into(), payload: json!({"salary_band": "band-7-confidential"}), ..Default::default() }).await?);
    }

    // step 2: the subscriber polls from the start
    let (events, _) = poll_all(&subscriber, 0, Some(sub), false).await?;
    let text = Value::Array(events.clone()).to_string();
    assert!(events.iter().any(|e| e["subject"] == json!(eng.artifact_id)), "The subscriber gets the events for /docs/eng");
    assert!(!text.contains(&hr.artifact_id.to_string()) && !text.contains(&hr.version_id.to_string()), "and no event for /docs/hr.");
    assert!(!text.contains("band-7-confidential"), "No event contains a value of \"salary band\".");

    // step 3: backfill
    let (_, docs) = stream::backfill(&subscriber, Some(sub)).await?;
    let dt = Value::Array(docs.clone()).to_string();
    assert!(docs.iter().any(|d| d["id"] == json!(eng.artifact_id)), "the backfill has the document of /docs/eng");
    assert!(!dt.contains(&hr.artifact_id.to_string()) && !dt.contains(&other.artifact_id.to_string()), "The backfill has no state document from /docs/hr and none from tenant T2.");
    assert!(!dt.contains("band-7-confidential"), "the backfill has no customer-held value");

    // step 4: the ID of the document of tenant T2, at the endpoint of T1
    let router = mudcat::api::router(inst.clone());
    let token = mudcat::auth::issue(&inst, t1.id, &Principal::service("indexer"), chrono::Duration::hours(1));
    let (st, body) = http(&router, "GET", &format!("/entities/{}", other.artifact_id), &t1.endpoint, Some(&token), None).await;
    let (st2, body2) = http(&router, "GET", &format!("/entities/{}", Uuid::now_v7()), &t1.endpoint, Some(&token), None).await;
    assert!(st == 404 && body["error"] == "not-found" && st == st2 && body == body2, "mudcat answers that the entity does not exist.");

    // step 5: poll at the endpoint of T2 with a token for T1
    let (st, body) = http(&router, "GET", "/stream?from=0", &t2.endpoint, Some(&token), None).await;
    assert!(st == 401 && body["message"].as_str().unwrap_or("").contains("audience"), "mudcat refuses the request, because the audience of the token is not T2.");
    assert!(body.get("events").is_none(), "the refusal has no data");
    Ok(())
}

// ---------------------------------------------------------------------------- UC4-06

/// A document changes after an answer cited it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn uc4_06() -> Result<()> {
    let (inst, _t, admin) = setup("uc4-06").await;
    let store = resource(&admin, "docs", "eu", "managed", false).await?;
    acl_root(&admin, "/handbook", Some(("docs", "handbook/"))).await?;
    grant_collection(&admin, "/handbook", "reader", "user:reviewer").await?;
    grant_collection(&admin, "/handbook", "reader", "user:trainer").await?;
    source(&admin, "handbook", "licensed", json!({}), &[("use", "retrieval"), ("retrieve", "any")]).await?;
    let files = vec![("P.txt".to_string(), b"employee handbook, travel rules, version one".to_vec()), ("Q.txt".to_string(), b"employee handbook, leave rules".to_vec())];
    let regs = register_files(&admin, &store, "/handbook", "handbook/", &files, Some("handbook"), None).await?;
    let p1 = regs[0].clone();
    for r in &regs {
        in_tx!(admin, |tx| metadata::write(&mut tx, &metadata::WriteMetadata { entity: r.version_id, namespace: "dc".into(), payload: json!({"description": "employee handbook"}), ..Default::default() }).await?);
    }
    verify_all(&admin, "docs").await?;
    // given: an answer A1 cites P version 1 with a selector
    let selector = json!({"type": "oa:DataPositionSelector", "start": 0, "end": 17});
    let a1 = in_tx!(admin, |tx| provenance::record_run(&mut tx, &RunSpec { kind: "answer".into(), used: vec![json!({"entity": p1.version_id, "role": "retrieval", "selector": selector})], ..Default::default() }).await?);
    let t_a1 = inst.now();
    inst.clock.advance(chrono::Duration::seconds(5));
    let pos = position(&admin).await?;

    // step 1: the author registers a revised P
    let f2 = vec![("P.txt".to_string(), b"employee handbook, travel rules, version two".to_vec())];
    let p2 = register_files(&admin, &store, "/handbook", "handbook/v2-", &f2, Some("handbook"), None).await?[0].clone();
    verify_all(&admin, "docs").await?;
    assert!(p2.artifact_id == p1.artifact_id && p2.number == 2, "P has version 2.");
    let (docs, _) = poll_all(&admin, pos, None, true).await?;
    assert!(docs.iter().any(|d| d["id"] == json!(p1.artifact_id) && d["latest_version"]["id"] == json!(p2.version_id)), "The state stream has the new state document of P.");

    // step 2: the reviewer resolves the citation A1
    let reviewer = inst.session(admin.tenant_id()?, Principal::user("reviewer"));
    let cited = in_tx!(reviewer, |tx| provenance::cited_inputs(&tx, a1).await?);
    let inputs: Vec<resolver::Input> = cited.iter().map(|(v, sel, role)| resolver::Input { version: Some(*v), selector: sel.clone(), role: role.clone(), ..Default::default() }).collect();
    let r = resolver::resolve(&reviewer, &resolver::Request { inputs, ..Default::default() }).await?;
    assert!(r.tasks.len() == 1 && r.tasks[0].version == p1.version_id && r.inputs[0]["selector"] == selector, "The citation resolves to P version 1 and the same selector.");

    // step 3: the author deletes P
    in_tx!(admin, |tx| namespace::delete_artifact(&mut tx, "/handbook/P.txt").await?);
    let latest: bool = in_tx!(admin, |tx| tx
        .q1("select v.is_delete_marker from artifact a join version v on v.id = a.latest_version_id where a.id = $1", &[&p1.artifact_id])
        .await?
        .get(0));
    assert!(latest, "P has a delete marker as its newest version.");
    let stale = search::search(&reviewer, &SearchRequest { text: Some("employee handbook travel".into()), ..Default::default() }).await?;
    build_index(&admin).await?;
    let current = search::search(&reviewer, &SearchRequest { text: Some("employee handbook travel".into()), ..Default::default() }).await?;
    assert!(stale.hits.iter().chain(current.hits.iter()).all(|h| h.entity != p1.artifact_id), "A search does not return P.");
    assert!(current.hits.iter().any(|h| h.version == Some(regs[1].version_id)), "the other document is still found");
    for v in [p1.version_id, p2.version_id] {
        let r = resolver::resolve(&reviewer, &resolver::Request { inputs: vec![resolver::Input { version: Some(v), role: "retrieval".into(), ..Default::default() }], ..Default::default() }).await?;
        assert!(r.tasks.iter().any(|t| t.version == v), "P version 1 and P version 2 still resolve by their version IDs for a user with access.");
    }

    // step 4: P as of the time of A1
    let r = resolver::resolve(&reviewer, &resolver::Request { inputs: vec![resolver::Input { artifact: Some("/handbook/P.txt".into()), as_of: Some(t_a1), role: "retrieval".into(), ..Default::default() }], ..Default::default() }).await?;
    assert!(r.tasks.len() == 1 && r.tasks[0].version == p1.version_id, "The result is P version 1.");

    // step 5: a training job on the collection of P
    let trainer = inst.session(admin.tenant_id()?, Principal::user("trainer"));
    let r = resolver::resolve(&trainer, &train_request(vec![resolver::Input { collection: Some("/handbook".into()), role: "training".into(), ..Default::default() }], "train", "research")).await;
    assert!(matches!(&r, Err(e) if e.code() == "policy-refused" && e.to_string().contains("handbook")), "mudcat refuses the resolution, because \"handbook\" does not grant train: {r:?}");
    Ok(())
}
