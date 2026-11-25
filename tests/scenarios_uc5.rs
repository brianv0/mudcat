//! Reference scenarios of use case 5: a shared facility with many small groups
//! (evals/scenarios/uc5-facility.yaml). Each test follows the steps of one scenario and asserts
//! each `expect` statement.

use http_body_util::BodyExt;
use mudcat::datasets::{self, DatasetChange, Member};
use mudcat::evals::fixtures::*;
use mudcat::in_tx;
use mudcat::instance::{Instance, Principal, Session, TenantInfo};
use mudcat::namespace::{self, Register, ResourceSpec};
use mudcat::placement::{self, MoverFault};
use mudcat::{auth, authz, metadata, policy, resolver, search, stream, Error, Result};
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

async fn instance(name: &str) -> Arc<Instance> {
    schema_once();
    let base = Instance::open_with(&db_url(), "target/scenario-data", false).await.expect("open the instance");
    let short = &Uuid::new_v4().simple().to_string()[..8];
    base.fork(format!("target/scenario-data/{name}-{short}").into())
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

fn read(path: &str) -> resolver::Request {
    resolver::Request { inputs: vec![dataset_input(path, Some("main"), "analysis")], ..Default::default() }
}

async fn drop_database(inst: &Arc<Instance>, name: &str) {
    if let Ok(p) = mudcat::instance::make_pool(&mudcat::instance::db_url(&inst.base_url, "postgres"), 1) {
        if let Ok(c) = p.get().await {
            let _ = c.batch_execute(&format!("drop database if exists \"{name}\" with (force)")).await;
        }
    }
}

// ---------------------------------------------------------------------------- UC5-01

/// Two groups in one instance cannot see each other.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn uc5_01() -> Result<()> {
    let inst = instance("uc5-01").await;
    // step 1, 2: the operator creates two tenants
    let (a, admin_a) = tenant(&inst, "group-a").await;
    let (b, admin_b) = tenant(&inst, "group-b").await;
    assert_eq!(a.id.get_version_num(), 4, "The tenant has an ID from a cryptographic random generator (122 random bits).");
    assert!(a.id != b.id, "the two tenants have different IDs");
    assert!(a.endpoint != b.endpoint && a.endpoint.starts_with("group-a-"), "The tenant has its own endpoint.");
    let c = inst.shared.get().await.unwrap();
    let rows = c
        .query(
            "select c.relname::text, a.attname::text from pg_index i join pg_class c on c.oid = i.indrelid
               join pg_attribute a on a.attrelid = c.oid and a.attnum = i.indkey[0] where i.indisprimary and c.relname = any($1)",
            &[&mudcat::tenants::TENANT_TABLES.iter().map(|s| s.to_string()).collect::<Vec<_>>()],
        )
        .await?;
    assert_eq!(rows.len(), mudcat::tenants::TENANT_TABLES.len(), "each tenant table has a primary key");
    assert!(rows.iter().all(|r| r.get::<_, String>(1) == "tenant_id"), "The two tenants use the same tables. Each row has its tenant ID first in its key.");

    // step 3: alice registers /scans/s1 at the endpoint of group-a
    let router = mudcat::api::router(inst.clone());
    grant_collection(&admin_a, "/", "writer", "user:alice").await?;
    let alice = auth::issue(&inst, a.id, &Principal::user("alice"), chrono::Duration::hours(1));
    let (st, body) = http(&router, "POST", "/register", &a.endpoint, Some(&alice), Some(json!([{"path": "/scans/s1", "content_hash": "c0ffee", "size": 10}]))).await;
    assert_eq!(st, 200, "the registration succeeds: {body}");
    let s1: Uuid = body[0]["artifact_id"].as_str().unwrap().parse().unwrap();
    let in_a: i64 = in_tx!(admin_a, |tx| tx.q1("select count(*) from artifact where id = $1", &[&s1]).await?.get(0));
    assert_eq!(in_a, 1, "The artifact exists in the tenant group-a.");

    // step 4: bob asks group-b for the ID of /scans/s1
    let bob = auth::issue(&inst, b.id, &Principal::user("bob"), chrono::Duration::hours(1));
    let (st, body) = http(&router, "GET", &format!("/entities/{s1}"), &b.endpoint, Some(&bob), None).await;
    let (st2, body2) = http(&router, "GET", &format!("/entities/{}", Uuid::now_v7()), &b.endpoint, Some(&bob), None).await;
    assert!(st == 404 && body["error"] == "not-found" && (st, &body) == (st2, &body2), "mudcat answers that the entity does not exist.");

    // step 5: bob searches group-b for s1
    stream::flush(&admin_a).await?;
    search::build(&admin_a).await?;
    let (st, body) = http(&router, "POST", "/search", &b.endpoint, Some(&bob), Some(json!({"text": "s1 scans"}))).await;
    assert_eq!(st, 200);
    assert!(!body.to_string().contains(&s1.to_string()), "The results contain no entity of group-a.");

    // step 6: bob uses the endpoint of group-a with a token for group-b
    let (st, body) = http(&router, "GET", &format!("/entities/{s1}"), &a.endpoint, Some(&bob), None).await;
    assert!(st == 401 && body["message"].as_str().unwrap_or("").contains("audience") && !body.to_string().contains("s1"), "mudcat refuses the request.");

    // step 7: a background job runs with no tenant
    let job = inst.session_without_tenant();
    let w: Result<()> = async {
        let mut c = job.client().await?;
        let mut tx = job.begin(&mut c).await?;
        namespace::ensure_collection(&mut tx, "/job-output").await?;
        tx.commit().await
    }
    .await;
    assert!(w.is_err(), "The job fails.");
    let seen: i64 = in_tx!(job, |tx| tx.q1("select count(*) from artifact", &[]).await?.get(0));
    assert_eq!(seen, 0, "It reads no data.");
    let relay = stream::relay(&job).await;
    assert!(relay.is_err(), "a relay with no tenant fails");
    let colls: i64 = in_tx!(admin_a, |tx| tx.q1("select count(*) from collection where path = '/job-output'", &[]).await?.get(0));
    let colls_b: i64 = in_tx!(admin_b, |tx| tx.q1("select count(*) from collection where path = '/job-output'", &[]).await?.get(0));
    assert!(colls == 0 && colls_b == 0, "It writes no data.");
    Ok(())
}

// ---------------------------------------------------------------------------- UC5-02

/// An embargo that ends by date or by publication.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn uc5_02() -> Result<()> {
    let inst = instance("uc5-02").await;
    let (_a, admin) = tenant(&inst, "group-a").await;
    let store = resource(&admin, "facility", "eu", "managed", false).await?;
    acl_root(&admin, "/group-a", Some(("facility", "group-a/"))).await?;
    grant_collection(&admin, "/group-a", "reader", "group:group-a#member").await?;
    grant_collection(&admin, "/group-a", "writer", "group:group-a#member").await?;
    grant(&admin, "group", "group-a", "member", "user:alice").await?;
    source(&admin, "beamtime", "instrument", json!({}), &[("use", "research")]).await?;
    in_tx!(admin, |tx| metadata::declare_attribute(&mut tx, &metadata::AttributeSpec { namespace: "facility".into(), name: "embargo_until".into(), datatype: "datetime".into(), access_condition: Some("embargo".into()), ..Default::default() }).await?);
    let files: Vec<(String, Vec<u8>)> = (0..10).map(|i| (format!("frame{i:02}.cbf"), file_bytes("opaque", i, "uc5-02"))).collect();
    let regs = register_files(&admin, &store, "/group-a/session-1", "group-a/session-1/", &files, Some("beamtime"), None).await?;
    verify_all(&admin, "facility").await?;
    // given: a dataset from a session on the date D; a snapshot with a part of the data is published
    let first = dataset(&admin, "/group-a/session-1/data", regs[..4].iter().map(|r| Member::version(r.version_id)).collect()).await?;
    let paper = in_tx!(admin, |tx| datasets::publish(&mut tx, "/group-a/session-1/data", "main", "paper").await?);
    commit(&admin, DatasetChange { dataset: "/group-a/session-1/data".into(), add: regs[4..].iter().map(|r| Member::version(r.version_id)).collect(), ..Default::default() }).await?;
    let ds = first.dataset;
    let d = inst.now();
    let end = d + chrono::Duration::days(3 * 365);
    in_tx!(admin, |tx| authz::grant(&mut tx, "dataset", &ds.to_string(), "reader", "authenticated:*", Some("embargo")).await?);
    in_tx!(admin, |tx| metadata::write(&mut tx, &metadata::WriteMetadata { entity: ds, namespace: "dc".into(), payload: json!({"description": "session 1 of group a"}), ..Default::default() }).await?);

    // step 1: the facility writes the embargo date
    let facility = inst.session(admin.tenant_id()?, Principal::user("admin"));
    in_tx!(facility, |tx| metadata::write(&mut tx, &metadata::WriteMetadata { entity: ds, namespace: "facility".into(), payload: json!({"embargo_until": end.to_rfc3339()}), ..Default::default() }).await?);
    let cond: Option<String> = in_tx!(admin, |tx| tx.q1("select access_condition from attribute_def where name = 'embargo_until'", &[]).await?.get(0));
    assert_eq!(cond.as_deref(), Some("embargo"), "The attribute is a condition for access to this dataset.");

    // step 2: eve resolves at D plus 1 year
    inst.clock.advance(chrono::Duration::days(365));
    let eve = inst.session(admin.tenant_id()?, Principal::user("eve"));
    let r = resolver::resolve(&eve, &read("/group-a/session-1/data")).await;
    assert!(matches!(&r, Err(e) if e.code() == "access-denied"), "mudcat denies the request.");
    let view = in_tx!(eve, |tx| datasets::restricted_view(&tx, ds).await?);
    let vt = view.to_string();
    assert!(view["description"] == "session 1 of group a" && !regs.iter().any(|r| vt.contains(&r.version_id.to_string())), "eve can see that the dataset exists, with the description that the policy permits, but no data.");

    // step 3: alice resolves at D plus 1 year
    let alice = inst.session(admin.tenant_id()?, Principal::user("alice"));
    let r = resolver::resolve(&alice, &read("/group-a/session-1/data")).await?;
    assert_eq!(r.tasks.len(), 10, "The resolution succeeds, because alice is in the group.");

    // step 4: alice mints a DOI for the published snapshot with a part of the data
    let doi = in_tx!(alice, |tx| datasets::mint_doi(&mut tx, paper.id).await?);
    let named = in_tx!(admin, |tx| namespace::resolve_identifier(&tx, "doi", &doi, None).await?);
    assert_eq!(named, Some(paper.id), "The snapshot has a DOI.");
    let anonymous = admin.as_principal(Principal::anonymous());
    let public = resolver::resolve(&anonymous, &resolver::Request { inputs: vec![resolver::Input { snapshot: Some(paper.id), role: "analysis".into(), ..Default::default() }], ..Default::default() }).await?;
    let got: BTreeSet<Uuid> = public.tasks.iter().map(|t| t.version).collect();
    assert_eq!(got, regs[..4].iter().map(|r| r.version_id).collect(), "The members of that snapshot are public at once.");
    assert!(resolver::resolve(&anonymous, &read("/group-a/session-1/data")).await.is_err(), "The other data of the dataset stays under embargo.");
    assert!(resolver::resolve(&eve, &read("/group-a/session-1/data")).await.is_err(), "The other data of the dataset stays under embargo (signed in).");

    // step 5: time passes until D plus 3 years and 1 day
    let before = inst.now();
    inst.clock.advance(chrono::Duration::days(2 * 365 + 1));
    let batch: i64 = in_tx!(admin, |tx| tx.q1("select count(*) from commit_log where at > $1", &[&before]).await?.get(0));
    let r = resolver::resolve(&eve, &read("/group-a/session-1/data")).await?;
    assert_eq!(r.tasks.len(), 10, "eve can resolve the complete dataset.");
    assert_eq!(batch, 0, "No batch job changed any record.");

    // step 6: the facility extends the embargo of a second dataset
    let second = dataset(&admin, "/group-a/session-2/data", vec![Member::version(regs[0].version_id)]).await?;
    let old = inst.now() + chrono::Duration::days(365);
    let new = old + chrono::Duration::days(365);
    in_tx!(facility, |tx| metadata::write(&mut tx, &metadata::WriteMetadata { entity: second.dataset, namespace: "facility".into(), payload: json!({"embargo_until": old.to_rfc3339()}), ..Default::default() }).await?);
    inst.clock.advance(chrono::Duration::seconds(2));
    let steward = inst.session(admin.tenant_id()?, Principal::user("steward"));
    grant_collection(&admin, "/", "owner", "user:steward").await?;
    in_tx!(steward, |tx| metadata::write(&mut tx, &metadata::WriteMetadata { entity: second.dataset, namespace: "facility".into(), payload: json!({"embargo_until": new.to_rfc3339()}), ..Default::default() }).await?);
    let hist: Vec<(Value, String, chrono::DateTime<chrono::Utc>)> = in_tx!(admin, |tx| tx
        .q(
            "select h.payload, h.actor, c.at from metadata_history h join commit_log c on c.commit_id = h.commit_id and c.tenant_id = h.tenant_id
              where h.entity_id = $1 and h.namespace = 'facility' order by h.commit_id",
            &[&second.dataset],
        )
        .await?
        .iter()
        .map(|r| (r.get(0), r.get(1), r.get(2)))
        .collect());
    assert!(
        hist.len() == 2 && hist[0].0["embargo_until"] == json!(old.to_rfc3339()) && hist[1].0["embargo_until"] == json!(new.to_rfc3339()) && hist[1].1 == "user:steward" && hist[0].2 < hist[1].2,
        "The history shows the old date, the new date, who changed it and when: {hist:?}"
    );
    Ok(())
}

// ---------------------------------------------------------------------------- UC5-03

/// The facility keeps data for 30 days only.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn uc5_03() -> Result<()> {
    let inst = instance("uc5-03").await;
    let (_a, admin) = tenant(&inst, "group-a").await;
    let scratch = resource_spec(&admin, ResourceSpec { name: "facility-scratch".into(), location: "facility".into(), jurisdiction: "fr".into(), mode: "managed".into(), protocol: "s3".into(), versioned: false, egress_allowed: true, bucket: None, attrs: json!({"retention_days": 30}) }).await?;
    let archive = resource_spec(&admin, ResourceSpec { name: "group-a-archive".into(), location: "cloud-a".into(), jurisdiction: "fr".into(), mode: "external".into(), protocol: "s3".into(), versioned: false, egress_allowed: true, bucket: None, attrs: json!({}) }).await?;
    acl_root(&admin, "/group-a", Some(("facility-scratch", "group-a/"))).await?;
    grant_collection(&admin, "/group-a", "reader", "group:group-a#member").await?;
    grant_collection(&admin, "/group-a", "writer", "group:group-a#member").await?;
    grant(&admin, "group", "group-a", "member", "user:alice").await?;
    grant(&admin, "group", "group-a", "member", "user:carol").await?;
    source(&admin, "session", "instrument", json!({}), &[("use", "research")]).await?;
    // given: a session of group-a with 2,000 files on facility-scratch
    let files: Vec<(String, Vec<u8>)> = (0..2000).map(|i| (format!("img{i:05}.tif"), file_bytes("opaque", i, "uc5-03"))).collect();
    let regs = register_files(&admin, &scratch, "/group-a/s1", "group-a/s1/", &files, Some("session"), None).await?;
    verify_all(&admin, "facility-scratch").await?;
    let ds = dataset(&admin, "/group-a/s1/data", regs.iter().map(|r| Member::version(r.version_id)).collect()).await?;

    // step 1: alice adds a rule for one copy on her group's own bucket
    let alice = inst.session(admin.tenant_id()?, Principal::user("alice"));
    in_tx!(alice, |tx| placement::add_rule(&mut tx, ds.dataset, &["group-a-archive".to_string()], 1, None).await?);
    let copied = placement::run_mover(&admin, &MoverFault::default()).await?;
    let verified = run_agent(&admin, "group-a-archive").await?;
    assert!(copied == 2000 && verified == 2000, "A mover copies the files. Site agents verify the checksums.");
    let complete = in_tx!(admin, |tx| placement::complete_on(&mut tx, ds.dataset).await?);
    assert!(complete.contains(&"group-a-archive".to_string()), "The dataset is complete and verified on \"group-a-archive\".");
    let versions_before: (i64, i64) = in_tx!(admin, |tx| {
        let v: i64 = tx.q1("select count(*) from version", &[]).await?.get(0);
        let m: i64 = tx.q1("select count(*) from metadata_payload", &[]).await?.get(0);
        (v, m)
    });

    // step 2: 31 days pass
    inst.clock.advance(chrono::Duration::days(31));
    let n = in_tx!(admin, |tx| placement::retention_sweep(&mut tx).await?);
    run_agent(&admin, "facility-scratch").await?;
    let states: BTreeMap<String, i64> = in_tx!(admin, |tx| tx
        .q("select state, count(*) from replica where resource_id = $1 group by state", &[&scratch.id])
        .await?
        .iter()
        .map(|r| (r.get(0), r.get(1)))
        .collect());
    assert!(n == 2000 && states.get("retired") == Some(&2000) && states.len() == 1, "The replicas on \"facility-scratch\" have the state retired: {states:?}");
    assert!(inst.store.list(&scratch.bucket)?.is_empty(), "The site agent deleted the bytes.");
    let versions_after: (i64, i64) = in_tx!(admin, |tx| {
        let v: i64 = tx.q1("select count(*) from version", &[]).await?.get(0);
        let m: i64 = tx.q1("select count(*) from metadata_payload", &[]).await?.get(0);
        (v, m)
    });
    assert_eq!(versions_before, versions_after, "The versions, their metadata and their lineage did not change.");

    // step 3: alice resolves the session dataset
    let r = resolver::resolve(&alice, &read("/group-a/s1/data")).await?;
    assert!(r.tasks.len() == 2000 && r.tasks.iter().all(|t| t.resource == "group-a-archive"), "The plan uses \"group-a-archive\".");

    // step 4: carol retires the only replicas of a second session dataset
    let files2: Vec<(String, Vec<u8>)> = (0..20).map(|i| (format!("img{i:05}.tif"), file_bytes("opaque", i, "uc5-03-s2"))).collect();
    let regs2 = register_files(&admin, &scratch, "/group-a/s2", "group-a/s2/", &files2, Some("session"), None).await?;
    verify_all(&admin, "facility-scratch").await?;
    let ds2 = dataset(&admin, "/group-a/s2/data", regs2.iter().map(|r| Member::version(r.version_id)).collect()).await?;
    let carol = inst.session(admin.tenant_id()?, Principal::user("carol"));
    let out = in_tx!(carol, |tx| placement::retire_replicas(&mut tx, ds2.dataset, "facility-scratch", true).await?);
    assert!(
        out["warnings"].as_array().map(|w| w.len() == 20 && w.iter().all(|x| x["warning"] == "this is the last replica of the version")).unwrap_or(false),
        "mudcat warns that this is the last replica of each version."
    );

    // step 5: carol resolves after the replicas ended
    run_agent(&admin, "facility-scratch").await?;
    let r = resolver::resolve(&carol, &read("/group-a/s2/data")).await;
    assert!(matches!(&r, Err(e) if e.code() == "no-replica"), "mudcat answers that no replica is available: {r:?}");
    let still: i64 = in_tx!(admin, |tx| tx.q1("select count(*) from version where id = any($1)", &[&regs2.iter().map(|r| r.version_id).collect::<Vec<_>>()]).await?.get(0));
    assert_eq!(still, 20, "The catalog records still exist.");

    // step 6: the group deleted a file in its own bucket; the site agent reports the inventory
    let gone = inst.store.list(&archive.bucket)?[0].0.clone();
    inst.store.delete(&archive.bucket, &gone, true)?;
    let report = placement::report_inventory(&admin, "group-a-archive", "agent-archive").await?;
    let lost = report["lost"].as_array().cloned().unwrap_or_default();
    assert!(lost.len() == 1 && lost[0]["url"] == json!(archive.url(&gone)), "If the group deleted a file in its own bucket, mudcat records the replica as lost.");
    let lost_state: String = in_tx!(admin, |tx| tx.q1("select state from replica where url = $1", &[&archive.url(&gone)]).await?.get(0));
    assert_eq!(lost_state, "lost");
    let scratch_states: i64 = in_tx!(admin, |tx| tx.q1("select count(*) from replica where resource_id = $1 and state = 'lost'", &[&scratch.id]).await?.get(0));
    assert!(report["dark_data"].as_array().map(|d| d.is_empty()).unwrap_or(false) && scratch_states == 0, "It does not treat this as an error of the facility.");
    Ok(())
}

// ---------------------------------------------------------------------------- UC5-04

/// A group manages its members in a GitHub organisation.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn uc5_04() -> Result<()> {
    let inst = instance("uc5-04").await;
    let (a, admin) = tenant(&inst, "group-a").await;
    let (b, _admin_b) = tenant(&inst, "group-b").await;
    let store = resource(&admin, "store", "eu", "managed", false).await?;
    acl_root(&admin, "/group-a", Some(("store", "group-a/"))).await?;
    grant_collection(&admin, "/group-a", "reader", "group:members#member").await?;
    grant_collection(&admin, "/group-a", "reader", "user:erin").await?;
    source(&admin, "beamtime", "instrument", json!({}), &[("use", "research")]).await?;
    let files: Vec<(String, Vec<u8>)> = (0..3).map(|i| (format!("d{i}.h5"), file_bytes("opaque", i, "uc5-04"))).collect();
    let regs = register_files(&admin, &store, "/group-a/raw", "group-a/raw/", &files, Some("beamtime"), None).await?;
    verify_all(&admin, "store").await?;
    let ds = dataset(&admin, "/group-a/raw/data", regs.iter().map(|r| Member::version(r.version_id)).collect()).await?;
    // given: the team beamtime-2026 of a GitHub organisation, with alice and dan; linked identities
    let team = |m: Vec<&str>| {
        inst.idp.teams.write().entry("github".into()).or_default().entry("lab-org".into()).or_default().insert("beamtime-2026".into(), m.iter().map(|x| x.to_string()).collect());
    };
    team(vec!["alice", "dan"]);
    let src = in_tx!(admin, |tx| {
        authz::link_identity(&mut tx, "github", "alice", "user:alice").await?;
        authz::link_identity(&mut tx, "github", "dan", "user:dan").await?;
        authz::create_membership_source(&mut tx, "members", "github", "lab-org", "beamtime-2026", 24 * 3600).await?
    });
    in_tx!(admin, |tx| authz::sync_membership(&mut tx, src).await?);

    // step 1: alice signs in with the device login of GitHub
    let (device, user_code) = auth::start_device_login(&inst, "github");
    auth::approve_device_login(&inst, "github", &user_code, "alice")?;
    let (token, who) = auth::poll_device_login(&inst, a.id, "github", &device).await?;
    let router = mudcat::api::router(inst.clone());
    let (st_a, _) = http(&router, "GET", &format!("/entities/{}", ds.dataset), &a.endpoint, Some(&token), None).await;
    let (st_b, _) = http(&router, "GET", &format!("/entities/{}", ds.dataset), &b.endpoint, Some(&token), None).await;
    assert!(who.0 == "user:alice" && st_a == 200 && st_b == 401, "alice gets a mudcat token that is valid for the endpoint of group-a only ({st_a}, {st_b}).");
    let kept_in_memory = inst.idp.tokens.read().keys().any(|k| k.starts_with("github-device-") || k.contains(&device));
    let kept_in_db: i64 = in_tx!(admin, |tx| tx
        .q1("select (select count(*) from audit where details::text like '%-device-%') + (select count(*) from linked_identity where external_id like '%-device-%')", &[])
        .await?
        .get(0));
    assert!(!kept_in_memory && kept_in_db == 0, "mudcat stores no token of GitHub after the exchange.");

    // step 2: GitHub reports that dan left the team
    team(vec!["alice"]);
    in_tx!(admin, |tx| authz::sync_membership(&mut tx, src).await?);
    let dan = inst.session(a.id, Principal::user("dan"));
    let groups = in_tx!(dan, |tx| authz::groups_of(&tx).await?);
    assert!(!groups.contains(&"members".to_string()), "dan is not a member of the group \"members\".");

    // step 3: dan resolves a dataset of group-a
    let r = resolver::resolve(&dan, &read("/group-a/raw/data")).await;
    assert!(matches!(&r, Err(e) if e.code() == "access-denied"), "mudcat denies the request.");

    // step 4: the interface of GitHub is not reachable for 25 hours
    let alice = inst.session(a.id, Principal::user("alice"));
    assert!(resolver::resolve(&alice, &read("/group-a/raw/data")).await.is_ok(), "alice reads through the team before the outage");
    inst.idp.down.write().insert("github".into());
    inst.clock.advance(chrono::Duration::hours(25));
    let sync = in_tx!(admin, |tx| authz::sync_membership(&mut tx, src).await);
    assert!(sync.is_err(), "the synchronisation fails while GitHub is down");
    let r = resolver::resolve(&alice, &read("/group-a/raw/data")).await;
    assert!(matches!(&r, Err(e) if e.to_string().contains("older than its limit")), "After 24 hours, the group \"members\" grants no access through this membership source: {r:?}");
    let erin = inst.session(a.id, Principal::user("erin"));
    assert!(resolver::resolve(&erin, &read("/group-a/raw/data")).await.is_ok(), "A grant that an owner gave directly to a user still works.");
    inst.idp.down.write().clear();

    // step 5: mallory signs in with a GitHub token; mallory is in the organisation, not in the team
    inst.idp.tokens.write().insert("gho_mallory".into(), "mallory".into());
    let (_mt, mallory) = auth::sign_in_provider(&inst, a.id, "github", "gho_mallory").await?;
    let ms = inst.session(a.id, mallory.clone());
    let r = resolver::resolve(&ms, &read("/group-a/raw/data")).await;
    assert!(matches!(&r, Err(e) if e.code() == "access-denied"), "The sign-in succeeds, but mallory has no access to the data of the team.");

    // step 6: a CI job signs in with the identity token of its pipeline
    let issuer_secret = b"ci-issuer-secret";
    // NOT ASSERTED as worded: a trust rule in mudcat maps (issuer, repository, branch) to a
    // principal; the collection that the pipeline may write is a grant to that principal, not a
    // field of the rule. The test asserts the effect: one collection writable, the others not.
    let ci_principal = "service:ci-group-a";
    auth::add_trust_rule(&admin, "https://ci.example", "lab-org/beamtime-pipeline", "main", ci_principal).await?;
    acl_root(&admin, "/group-a/pipeline-out", None).await?;
    grant_collection(&admin, "/group-a/pipeline-out", "writer", ci_principal).await?;
    let id_token = auth::ci_identity_token(issuer_secret, "https://ci.example", "lab-org/beamtime-pipeline", "main");
    let (ci_token, p) = auth::sign_in_pipeline(&inst, a.id, issuer_secret, &id_token).await?;
    assert_eq!(p.0, ci_principal, "the trust rule maps the pipeline to its principal");
    let (ok, _) = http(&router, "POST", "/register", &a.endpoint, Some(&ci_token), Some(json!([{"path": "/group-a/pipeline-out/result-1", "content_hash": "r1", "size": 1}]))).await;
    let (no, _) = http(&router, "POST", "/register", &a.endpoint, Some(&ci_token), Some(json!([{"path": "/group-a/raw/result-1", "content_hash": "r1", "size": 1}]))).await;
    assert!(ok == 200 && no == 403, "The job can write to the collection that the trust rule names, and to no other collection ({ok}, {no}).");
    let other_branch = auth::ci_identity_token(issuer_secret, "https://ci.example", "lab-org/beamtime-pipeline", "feature");
    assert!(auth::sign_in_pipeline(&inst, a.id, issuer_secret, &other_branch).await.is_err(), "a pipeline on another branch gets no token");
    Ok(())
}

// ---------------------------------------------------------------------------- UC5-05

async fn fingerprint(s: &Session) -> Result<BTreeMap<String, String>> {
    let sys = s.as_principal(Principal::system());
    let mut out = BTreeMap::new();
    let mut c = sys.client().await?;
    let tx = sys.begin(&mut c).await?;
    for (t, key) in [("artifact", "id::text"), ("version", "id::text"), ("commit_log", "commit_id::text"), ("snapshot", "id::text || coalesce(merkle_hash, '')"), ("event", "position::text"), ("entity", "id::text")] {
        let r = tx.q1(&format!("select count(*)::text || ':' || coalesce(md5(string_agg({key}, ',' order by {key})), '') from {t}"), &[]).await?;
        out.insert(t.to_string(), r.get(0));
    }
    tx.commit().await?;
    Ok(out)
}

async fn rows_in(inst: &Arc<Instance>, db: &str, tenant: Uuid) -> Result<i64> {
    let pool = inst.pool_for(db)?;
    let mut c = pool.get().await?;
    let tx = c.transaction().await?;
    tx.execute("select set_config('mudcat.tenant', $1, true)", &[&tenant.to_string()]).await?;
    let mut n = 0i64;
    for t in mudcat::tenants::TENANT_TABLES {
        if let Ok(r) = tx.query_one(&format!("select count(*) from {t}"), &[]).await {
            n += r.get::<_, i64>(0);
        }
    }
    Ok(n)
}

/// Move a tenant from the shared tier to its own database.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn uc5_05() -> Result<()> {
    let inst = instance("uc5-05").await;
    let (big, admin) = tenant(&inst, "group-big").await;
    let (_n, neighbour) = tenant(&inst, "group-n").await;
    let nregs: Vec<Register> = (0..20).map(|i| Register { path: format!("/n/f{i:02}"), content_hash: Some(format!("n{i}")), size: 1, ..Default::default() }).collect();
    in_tx!(neighbour, |tx| namespace::register_batch(&mut tx, &nregs).await?);
    let store = resource(&admin, "store", "eu", "managed", false).await?;
    acl_root(&admin, "/big", Some(("store", "big/"))).await?;
    grant_collection(&admin, "/big", "reader", "user:alice").await?;
    let files: Vec<(String, Vec<u8>)> = (0..10).map(|i| (format!("f{i}.bin"), file_bytes("opaque", i, "uc5-05"))).collect();
    let regs = register_files(&admin, &store, "/big/pub", "big/pub/", &files, None, None).await?;
    verify_all(&admin, "store").await?;
    dataset(&admin, "/big/release", regs.iter().map(|r| Member::version(r.version_id)).collect()).await?;
    let release = in_tx!(admin, |tx| datasets::publish(&mut tx, "/big/release", "main", "v1").await?);
    // given: many versions (100,000 here; 5,000,000 in the scenario), made in bulk
    let bulk: i64 = 100_000;
    in_tx!(admin, |tx| {
        let c = tx.commit_id().await?;
        let coll: Uuid = tx.q1("select id from collection where path = '/big'", &[]).await?.get(0);
        tx.exec("create temporary table tmp_art on commit drop as select gen_random_uuid() as id, g from generate_series(1, $1::bigint) g", &[&(bulk / 1000)]).await?;
        tx.exec("insert into artifact (id, collection_id, name, kind, created_commit) select id, $1, 'bulk-' || g, 'data', $2 from tmp_art", &[&coll, &c]).await?;
        tx.exec("insert into version (id, artifact_id, number, content_hash, size, created_commit) select gen_random_uuid(), a.id, v, md5(a.id::text || v), 100, $1 from tmp_art a cross join generate_series(1, 1000) v", &[&c]).await?;
    });
    stream::flush(&admin).await?;
    stream::flush(&neighbour).await?;
    let p: i64 = in_tx!(admin, |tx| tx.q1("select last_position from stream_state", &[]).await?.get(0));
    let before = fingerprint(&admin).await?;
    let neighbour_before = fingerprint(&neighbour).await?;
    let short = &big.id.simple().to_string()[..12];
    let target = format!("mudcat_uc505_{short}");

    // step 1: the operator moves the tenant
    let mover = {
        let inst = inst.clone();
        let target = target.clone();
        tokio::spawn(async move { mudcat::tenants::move_tenant(&inst, big.id, &target).await })
    };
    let started = std::time::Instant::now();
    while inst.tenant(big.id).map(|t| t.status != "moving").unwrap_or(true) {
        assert!(started.elapsed() < std::time::Duration::from_secs(20), "the move starts");
        tokio::task::yield_now().await;
    }
    let w: Result<()> = async {
        let mut c = admin.client().await?;
        let mut tx = admin.begin(&mut c).await?;
        namespace::register(&mut tx, &Register { path: "/big/during".into(), content_hash: Some("during".into()), size: 1, ..Default::default() }).await?;
        tx.commit().await
    }
    .await;
    let r: Result<i64> = async { Ok(in_tx!(admin, |tx| tx.q1("select count(*) from version", &[]).await?.get::<_, i64>(0))) }.await;
    let during_moving = inst.tenant(big.id).map(|t| t.status == "moving").unwrap_or(false);
    assert!(matches!(&w, Err(Error::Retryable(_))), "During the copy, mudcat accepts no writes for this tenant: {w:?}");
    assert!(r.is_ok() && during_moving, "and reads continue.");
    let moved = mover.await.unwrap();
    assert!(moved.is_ok(), "the move succeeds: {moved:?}");
    assert_eq!(fingerprint(&neighbour).await?, neighbour_before, "Other tenants in the shared tier see no change.");

    // step 2: compare the tenant before and after the move
    assert_eq!(fingerprint(&admin).await?, before, "Each record has the same key and the same ID as before.");
    let hash = in_tx!(admin, |tx| datasets::snapshot(&tx, release.id).await?.merkle_hash);
    assert_eq!(hash, release.merkle_hash, "Each published snapshot has the same Merkle hash.");

    // step 3: alice resolves at the endpoint of group-big
    let t = inst.tenant(big.id).unwrap();
    let router = mudcat::api::router(inst.clone());
    let alice = auth::issue(&inst, big.id, &Principal::user("alice"), chrono::Duration::hours(1));
    let (st, body) = http(&router, "POST", "/resolve", &t.endpoint, Some(&alice), Some(json!({"inputs": [{"dataset": "/big/release", "ref": "v1"}]}))).await;
    assert!(t.endpoint == big.endpoint && st == 200 && body["tasks"].as_array().map(|x| x.len() == 10).unwrap_or(false), "The endpoint did not change, and the request succeeds.");

    // step 4: the consumer continues from P
    in_tx!(admin, |tx| namespace::register(&mut tx, &Register { path: "/big/after".into(), content_hash: Some("after".into()), size: 1, ..Default::default() }).await?);
    stream::flush(&admin).await?;
    let mut pos = p;
    let mut positions = Vec::new();
    loop {
        let q = stream::poll(&admin, pos, 1000, None, false).await?;
        positions.extend(q.events.iter().map(|e| e["sequence"].as_i64().unwrap()));
        if q.next == pos {
            break;
        }
        pos = q.next;
    }
    assert!(!positions.is_empty() && positions.iter().enumerate().all(|(i, x)| *x == p + 1 + i as i64), "The consumer continues from P with no gap and no event out of order.");

    // step 5: a second move fails at one half
    let (mid, mid_admin) = tenant(&inst, "group-mid").await;
    in_tx!(mid_admin, |tx| namespace::register_batch(&mut tx, &nregs).await?);
    let mid_before = fingerprint(&mid_admin).await?;
    let target2 = format!("mudcat_uc505f_{}", &mid.id.simple().to_string()[..12]);
    *inst.move_fault_at.write() = Some(0.5);
    let failed = mudcat::tenants::move_tenant(&inst, mid.id, &target2).await;
    *inst.move_fault_at.write() = None;
    assert!(failed.is_err(), "the copy fails at one half");
    let mt = inst.tenant(mid.id).unwrap();
    let w = in_tx!(mid_admin, |tx| namespace::register(&mut tx, &Register { path: "/n/after-failure".into(), content_hash: Some("af".into()), size: 1, ..Default::default() }).await);
    assert!(mt.location == "shared" && mt.status == "active" && fingerprint(&mid_admin).await?["version"] != mid_before["version"] && w.is_ok(), "The tenant continues in the shared tier with all its data, and accepts writes again.");
    assert_eq!(rows_in(&inst, &target2, mid.id).await?, 0, "The dedicated database has no visible part of the tenant.");

    // step 6: rows of group-big in the shared tier after the successful move
    assert_eq!(rows_in(&inst, "shared", big.id).await?, 0, "No rows stay in the shared tier.");
    drop_database(&inst, &target).await;
    drop_database(&inst, &target2).await;
    Ok(())
}

// ---------------------------------------------------------------------------- UC5-06

/// Publish a dataset, correct it later, and give a copy to a second group.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn uc5_06() -> Result<()> {
    let inst = instance("uc5-06").await;
    let (a, admin) = tenant(&inst, "group-a").await;
    let (b, admin_b) = tenant(&inst, "group-b").await;
    let store = resource(&admin, "store", "eu", "managed", false).await?;
    acl_root(&admin, "/group-a", Some(("store", "group-a/"))).await?;
    let src = source(&admin, "computed-structures", "computed", json!({}), &[]).await?;
    in_tx!(admin, |tx| policy::set_terms(&mut tx, src, &policy::Terms::Spdx { id: "CC-BY-4.0".into() }).await?);
    let files: Vec<(String, Vec<u8>)> = (0..5).map(|i| (format!("s{i}.jsonl"), file_bytes("records", i, "uc5-06"))).collect();
    let regs = register_files(&admin, &store, "/group-a/structures", "group-a/structures/", &files, Some("computed-structures"), None).await?;
    verify_all(&admin, "store").await?;
    dataset(&admin, "/group-a/structures-ds", regs.iter().map(|r| Member::version(r.version_id)).collect()).await?;

    // step 1: publish release-1
    let r1 = in_tx!(admin, |tx| datasets::publish(&mut tx, "/group-a/structures-ds", "main", "release-1").await?);
    assert!(r1.status.as_deref() == Some("published") && r1.merkle_hash.is_some(), "The snapshot has a Merkle hash and the status published.");
    let first_members: BTreeSet<Uuid> = in_tx!(admin, |tx| datasets::members(&tx, r1.id).await?.iter().map(|e| e.member_id).collect());

    // step 2: mint a DOI
    let doi = in_tx!(admin, |tx| datasets::mint_doi(&mut tx, r1.id).await?);
    let ids = in_tx!(admin, |tx| namespace::identifiers_of(&tx, r1.id).await?);
    assert!(ids.iter().any(|(s, v)| s == "doi" && *v == doi), "The snapshot has a DOI as an identifier.");

    // step 3: export Croissant and DCAT
    let (cr, dc) = in_tx!(admin, |tx| (mudcat::export::croissant(&tx, r1.id).await?, mudcat::export::dcat(&tx, r1.id).await?));
    let crt = cr.to_string();
    let dct = dc.to_string();
    assert!(cr["identifier"] == json!(doi) && dc["dcterms:identifier"] == json!(doi), "The descriptions name the DOI");
    assert!(crt.contains("https://spdx.org/licenses/CC-BY-4.0") && dct.contains("https://spdx.org/licenses/CC-BY-4.0"), "the licence");
    assert!(cr["recordSet"][0]["field"].as_array().map(|f| f.iter().any(|x| x["name"] == "energy" && x["unit"] == "eV")).unwrap_or(false) && dct.contains("\"eV\""), "the fields with their units");
    let sha: BTreeSet<String> = in_tx!(admin, |tx| tx.q("select content_hash from version where id = any($1)", &[&regs.iter().map(|r| r.version_id).collect::<Vec<_>>()]).await?.iter().map(|r| r.get(0)).collect());
    assert!(sha.iter().all(|h| crt.contains(h.as_str()) && dct.contains(h.as_str())), "and the checksums.");

    // step 4: a corrected version of one member on main
    let fixed = vec![("s0.jsonl".to_string(), file_bytes("records", 0, "corrected"))];
    let v2 = register_files(&admin, &store, "/group-a/structures", "group-a/structures/v2-", &fixed, Some("computed-structures"), None).await?[0].clone();
    let head = in_tx!(admin, |tx| datasets::resolve_ref(&tx, datasets::dataset_id(&tx, "/group-a/structures-ds").await?, "main", None).await?);
    let c = commit(&admin, DatasetChange { dataset: "/group-a/structures-ds".into(), requirement: Some(head), replace: vec![(regs[0].version_id, Member::version(v2.version_id))], ..Default::default() }).await?;
    assert!(c.snapshot != r1.id, "A new snapshot exists on main.");
    let (by_doi, members_now) = in_tx!(admin, |tx| {
        let s = namespace::resolve_identifier(&tx, "doi", &doi, None).await?;
        let m: BTreeSet<Uuid> = datasets::members(&tx, r1.id).await?.iter().map(|e| e.member_id).collect();
        (s, m)
    });
    assert!(by_doi == Some(r1.id) && members_now == first_members, "The DOI still resolves to release-1 with its first members.");

    // step 5: clone release-1 to group-b under an agreement
    let objects_before: usize = inst.store.list(&store.bucket)?.len();
    let did_b = datasets::clone_to_tenant(&admin, r1.id, &admin_b, "/shared/structures", "agreement-2026-07").await?;
    let (refs_b, manifests_b, entries_b): (Vec<Uuid>, i64, i64) = in_tx!(admin_b, |tx| {
        let snap = datasets::resolve_ref(&tx, did_b, "main", None).await?;
        let r: Vec<Uuid> = tx.q("select manifest_tenant from manifest_ref where snapshot_id = $1", &[&snap]).await?.iter().map(|x| x.get(0)).collect();
        let m: i64 = tx.q1("select count(*) from manifest", &[]).await?.get(0);
        let e: i64 = tx.q1("select count(*) from manifest_entry", &[]).await?.get(0);
        (r, m, e)
    });
    assert!(!refs_b.is_empty() && refs_b.iter().all(|t| *t == a.id), "group-b has a dataset whose manifest references name the manifests of group-a, with a tenant mapping.");
    let members_b = in_tx!(admin_b, |tx| {
        let snap = datasets::resolve_ref(&tx, did_b, "main", None).await?;
        datasets::members(&tx, snap).await?.iter().map(|e| e.member_id).collect::<BTreeSet<Uuid>>()
    });
    assert_eq!(members_b, first_members, "the copy has the members of release-1");
    assert!(manifests_b == 0 && entries_b == 0 && inst.store.list(&store.bucket)?.len() == objects_before, "mudcat copied no manifest and no data.");
    let audit_a: i64 = in_tx!(admin, |tx| tx.q1("select count(*) from audit where action = 'share-with-tenant' and details->>'to_tenant' = $1", &[&b.id.to_string()]).await?.get(0));
    let audit_b: i64 = in_tx!(admin_b, |tx| tx.q1("select count(*) from audit where action = 'receive-from-tenant' and details->>'from_tenant' = $1", &[&a.id.to_string()]).await?.get(0));
    assert!(audit_a == 1 && audit_b == 1, "mudcat records the copy between tenants in the audit log.");

    // step 6: purge a version of release-1 that group-b still refers to
    let r = in_tx!(admin, |tx| policy::purge(&mut tx, &[regs[1].version_id], "cleanup").await);
    assert!(
        matches!(&r, Err(e) if e.code() == "referenced-by-tenant" && e.to_string().contains(&b.name)),
        "mudcat refuses to remove the version, and names the reference of group-b: {r:?}"
    );
    let still: i64 = in_tx!(admin, |tx| tx.q1("select count(*) from replica where version_id = $1 and state = 'verified'", &[&regs[1].version_id]).await?.get(0));
    assert_eq!(still, 1, "the version and its replica stay");
    Ok(())
}
