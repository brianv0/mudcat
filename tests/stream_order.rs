//! The order of change events with a fixed interleaving of three transactions (EV-13).
//!
//! The change stream must give positions in commit order. The rule "publish the outbox rows of
//! transactions that are older than every transaction in progress" does not: transaction IDs are
//! given at the first write, not at the commit. This test fixes the interleaving, so that it
//! fails every time with that rule and passes every time with the rule of the implementation.

use mudcat::instance::{Instance, Principal};
use mudcat::stream;
use serde_json::{json, Value};
use uuid::Uuid;

fn db_url() -> String {
    std::env::var("MUDCAT_TEST_DB").unwrap_or_else(|_| "postgres://mudcat:mudcat@localhost:54329/mudcat_scenarios_order".into())
}

fn of_subject(events: &[Value], subject: Uuid) -> Vec<(i64, i64, String)> {
    events
        .iter()
        .filter(|e| e["subject"] == json!(subject))
        .map(|e| (e["sequence"].as_i64().unwrap(), e["revision"].as_i64().unwrap(), e["data"]["by"].as_str().unwrap_or("").to_string()))
        .collect()
}

#[tokio::test]
async fn events_are_in_commit_order_with_overlapping_transactions() {
    let base = Instance::open(&db_url(), "target/scenario-data").await.expect("open the instance");
    let short = &Uuid::new_v4().simple().to_string()[..8];
    let inst = base.fork(format!("target/scenario-data/stream-order-{short}").into());
    let t = mudcat::tenants::create_tenant(&inst, &format!("stream-order-{short}"), "user:admin").await.expect("create a tenant");
    let admin = inst.session(t.id, Principal::user("admin"));
    stream::flush(&admin).await.unwrap();
    let start = stream::poll(&admin, 0, 100_000, None, false).await.unwrap().last;

    let (x1, x2, e) = (Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4());
    let (mut cb, mut cc, mut ca) = (admin.client().await.unwrap(), admin.client().await.unwrap(), admin.client().await.unwrap());
    // B begins and writes first: it gets the lowest transaction ID.
    let mut b = admin.begin(&mut cb).await.unwrap();
    b.emit("probe.touched", x1, "probe", None, json!({})).await.unwrap();
    // C begins and writes; it stays open until the end.
    let mut c = admin.begin(&mut cc).await.unwrap();
    c.emit("probe.touched", x2, "probe", None, json!({})).await.unwrap();
    // A begins after B and C, and commits first.
    let mut a = admin.begin(&mut ca).await.unwrap();
    a.emit("probe.changed", e, "probe", None, json!({"by": "A"})).await.unwrap();
    a.commit().await.unwrap();
    // B writes the same entity after A, and commits second.
    b.emit("probe.changed", e, "probe", None, json!({"by": "B"})).await.unwrap();
    b.commit().await.unwrap();

    // C is still open. The events of A and of B are committed, and must be in commit order.
    stream::relay(&admin).await.unwrap();
    let p1 = stream::poll(&admin, start, 100_000, None, false).await.unwrap();
    let ev_e = of_subject(&p1.events, e);
    let a_event = ev_e.iter().find(|x| x.2 == "A");
    let b_event = ev_e.iter().find(|x| x.2 == "B");
    assert!(a_event.is_some(), "the event of A (committed first) is not published while C is open: {ev_e:?}");
    if let (Some(a), Some(b)) = (a_event, b_event) {
        assert_eq!(a.1, 1, "the event of A has revision 1");
        assert_eq!(b.1, 2, "the event of B has revision 2");
        assert!(a.0 < b.0, "the event of A (revision 1) has a lower position than the event of B (revision 2): {ev_e:?}");
    }
    assert!(b_event.is_none() || a_event.is_some(), "the event of B is published and the event of A is not: {ev_e:?}");

    // C commits. All events are published, in commit order, with no gap.
    c.commit().await.unwrap();
    stream::relay(&admin).await.unwrap();
    let p2 = stream::poll(&admin, start, 100_000, None, false).await.unwrap();
    let seqs: Vec<i64> = p2.events.iter().map(|e| e["sequence"].as_i64().unwrap()).collect();
    assert_eq!(seqs, (start + 1..=start + 4).collect::<Vec<_>>(), "four events, with positions that increase by one");
    let ev_e = of_subject(&p2.events, e);
    assert_eq!(ev_e.iter().map(|x| x.1).collect::<Vec<_>>(), vec![1, 2], "the revisions of the entity are 1 then 2 in position order");
    // the events that were published before keep their positions
    let first: Vec<(i64, i64, String)> = of_subject(&p1.events, e);
    assert!(first.iter().all(|f| ev_e.contains(f)), "the positions that were given do not change");
}
