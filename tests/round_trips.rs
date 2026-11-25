//! The round trips between mudcat and PostgreSQL for one operation, with a budget for each.
//!
//! A round trip is a wait for the database. In a deployment the database is on another host, so
//! the number of round trips, and not the work of a statement, sets the lowest latency of an
//! operation. Each transaction of an instance counts its round trips (`Metrics::db`): statements
//! that go to the database together, with no wait between them, count one time.
//!
//! The budgets are the counts as they are now. A change that needs more round trips fails this
//! test. A budget goes down when an operation gets cheaper, and it does not go up without a reason
//! in the commit message.

use mudcat::datasets::{DatasetChange, Member};
use mudcat::evals::fixtures::{acl_root, dataset, dataset_input, file_bytes, grant_collection, register_files, resource, source, train_request, verify_all};
use mudcat::instance::{Instance, Principal, Session};
use mudcat::namespace::Register;
use mudcat::{authz, datasets, namespace, query, resolver};
use std::future::Future;
use std::sync::Arc;

fn db_url() -> String {
    std::env::var("MUDCAT_TEST_DB").unwrap_or_else(|_| "postgres://mudcat:mudcat@localhost:54329/mudcat_scenarios_rt".into())
}

macro_rules! in_tx {
    ($sess:expr, |$tx:ident| $body:expr) => {{
        let __s = &$sess;
        let mut __c = __s.client().await.unwrap();
        #[allow(unused_mut)]
        let mut $tx = __s.begin(&mut __c).await.unwrap();
        let __r = $body;
        $tx.commit().await.unwrap();
        __r
    }};
}

struct Table {
    inst: Arc<Instance>,
    over: Vec<String>,
}

impl Table {
    /// Run an operation three times, on one connection. The count is of the last run: the
    /// statements are prepared by then, as they are on a connection of a server that runs.
    async fn row<F, Fut>(&mut self, name: &str, budget: u64, mut op: F)
    where
        F: FnMut(usize) -> Fut,
        Fut: Future<Output = ()>,
    {
        let (mut round_trips, mut statements) = (0, 0);
        for i in 0..3 {
            let before = self.inst.metrics.db();
            op(i).await;
            let after = self.inst.metrics.db();
            (round_trips, statements) = (after.0 - before.0, after.1 - before.1);
        }
        println!("{name:<48} | {round_trips:>3} round trips | {statements:>3} statements | budget {budget}");
        if round_trips > budget {
            self.over.push(format!("{name}: {round_trips} round trips, and the budget is {budget}"));
        }
    }
}

#[tokio::test]
async fn round_trips_of_one_operation_stay_in_their_budget() {
    let base = Instance::open(&db_url(), "target/scenario-data").await.expect("open the instance");
    let short = uuid::Uuid::new_v4().simple().to_string()[..8].to_string();
    let inst = base.fork(format!("target/scenario-data/rt-{short}").into());
    let t = mudcat::tenants::create_tenant(&inst, &format!("rt-{short}"), "user:admin").await.unwrap();
    let admin: Session = inst.session(t.id, Principal::user("admin"));
    let trainer: Session = inst.session(t.id, Principal::user("trainer"));

    let res = resource(&admin, "store", "cloud-a", "managed", false).await.unwrap();
    acl_root(&admin, "/data", Some(("store", "data/"))).await.unwrap();
    source(&admin, "src", "instrument", serde_json::json!({}), &[("train", "research")]).await.unwrap();
    grant_collection(&admin, "/data", "reader", "user:trainer").await.unwrap();
    let files: Vec<(String, Vec<u8>)> = (0..10).map(|i| (format!("f-{i}.bin"), file_bytes("opaque", i, "rt"))).collect();
    let regs = register_files(&admin, &res, "/data", "data/", &files, Some("src"), None).await.unwrap();
    verify_all(&admin, "store").await.unwrap();
    let members: Vec<Member> = regs.iter().map(|r| Member::version(r.version_id)).collect();
    dataset(&admin, "/data/clips", members).await.unwrap();
    in_tx!(admin, |tx| datasets::publish(&mut tx, "/data/clips", "main", "v1").await.unwrap());
    let one_version = regs[0].version_id;
    let extra: Vec<(String, Vec<u8>)> = (0..3).map(|i| (format!("x-{i}.bin"), file_bytes("opaque", 100 + i, "rt"))).collect();
    let extra = register_files(&admin, &res, "/data", "data/", &extra, Some("src"), None).await.unwrap();

    let mut table = Table { inst: inst.clone(), over: vec![] };
    println!();
    table
        .row("the scope of a transaction, with nothing in it", 3, |_| async {
            in_tx!(admin, |tx| {
                let _ = &tx;
            });
        })
        .await;
    table
        .row("one read statement", 4, |_| async {
            in_tx!(admin, |tx| tx.q1("select count(*) from collection", &[]).await.unwrap());
        })
        .await;
    table
        .row("one access decision", 15, |_| async {
            in_tx!(trainer, |tx| authz::check(&tx, "read", "version", one_version).await.unwrap());
        })
        .await;
    table
        .row("a query, first page", 8, |_| async {
            in_tx!(trainer, |tx| query::run(&tx, "/data/**", &query::QueryOptions { limit: Some(50), after: None, all_versions: false }).await.unwrap());
        })
        .await;
    table
        .row("register one artifact", 18, |i| {
            let admin = admin.clone();
            async move {
                in_tx!(admin, |tx| namespace::register(&mut tx, &Register { path: format!("/data/one-{i}.bin"), content_hash: Some(format!("h{i}")), size: 1, ..Default::default() }).await.unwrap());
            }
        })
        .await;
    table
        .row("commit: add one member to a dataset", 35, |i| {
            let admin = admin.clone();
            let v = extra[i].version_id;
            async move {
                in_tx!(admin, |tx| datasets::commit(&mut tx, &[DatasetChange { dataset: "/data/clips".into(), add: vec![Member::version(v)], ..Default::default() }]).await.unwrap());
            }
        })
        .await;
    table
        .row("resolve a published dataset with 10 members", 54, |_| async {
            let req = train_request(vec![dataset_input("/data/clips", Some("v1"), "training")], "train", "research");
            resolver::resolve(&trainer, &req).await.unwrap();
        })
        .await;
    println!();
    assert!(table.over.is_empty(), "over the budget:\n{}", table.over.join("\n"));
}
