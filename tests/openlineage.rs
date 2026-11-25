//! PR-6: OpenLineage events become activities with imported relations.

use mudcat::in_tx;
use mudcat::instance::{Instance, Principal};
use serde_json::json;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn openlineage_import() -> mudcat::Result<()> {
    let url = std::env::var("MUDCAT_TEST_DB").unwrap_or_else(|_| "postgres://mudcat:mudcat@localhost:54329/mudcat_scenarios".into());
    let base = Instance::open(&url, "target/scenario-data").await?;
    let inst = base.fork(std::path::PathBuf::from(format!("target/scenario-data/ol-{}", uuid::Uuid::new_v4())));
    let t = mudcat::tenants::create_tenant(&inst, &format!("ol-{}", &uuid::Uuid::new_v4().simple().to_string()[..8]), "user:admin").await?;
    let s = inst.session(t.id, Principal::user("admin"));
    let known = in_tx!(s, |tx| mudcat::namespace::register(&mut tx, &mudcat::namespace::Register { path: "/warehouse/events".into(), content_hash: Some("h1".into()), size: 1, ..Default::default() }).await?);
    let ev = |kind: &str| json!({"eventType": kind, "producer": "spark", "run": {"runId": "r-1"}, "job": {"namespace": "etl", "name": "daily"},
        "inputs": [{"namespace": "s3", "name": "/warehouse/events"}], "outputs": [{"namespace": "s3", "name": "warehouse.daily_summary"}]});
    let a = in_tx!(s, |tx| mudcat::provenance::import_openlineage(&mut tx, &ev("START")).await?);
    let b = in_tx!(s, |tx| mudcat::provenance::import_openlineage(&mut tx, &ev("COMPLETE")).await?);
    assert_eq!(a["activity"], b["activity"], "the events of one run are one activity");
    assert_eq!(a["matched"], json!(["s3//warehouse/events"]), "the input matches the artifact by its path");
    assert_eq!(a["placeholders"], json!(["s3/warehouse.daily_summary"]), "an unknown output becomes a placeholder");
    let act: uuid::Uuid = a["activity"].as_str().unwrap().parse().unwrap();
    let (capture, ended): (String, bool) = in_tx!(s, |tx| {
        let r = tx.q1("select u.capture, a.ended_at is not null from usage u join activity a on a.id = u.activity_id where u.activity_id = $1 and u.entity_id = $2", &[&act, &known.version_id]).await?;
        (r.get(0), r.get(1))
    });
    assert_eq!(capture, "imported", "the relation records the capture mode imported");
    assert!(ended, "the COMPLETE event ends the activity");
    Ok(())
}
