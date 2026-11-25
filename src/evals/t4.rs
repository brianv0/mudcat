//! Drivers for the task templates of use case 4: governed retrieval for assistants.

use super::fixtures::*;
use super::Ctx;
use crate::error::{Error, Result};
use crate::in_tx;
use crate::namespace::{self, Register};
use crate::search::{self, SearchRequest};
use crate::stream;
use crate::{policy, resolver, Session};
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use uuid::Uuid;

/// The index of a consumer outside mudcat: document -> revision. It applies an event only if its
/// revision is newer than the one it has, so an event that comes again changes nothing.
#[derive(Default)]
struct Consumer {
    docs: BTreeMap<Uuid, i64>,
    position: i64,
    max_seen: i64,
    lower_seen: bool,
    duplicates: usize,
    paths: BTreeSet<String>,
}

impl Consumer {
    fn apply(&mut self, events: &[Value]) {
        for e in events {
            let p = e["sequence"].as_i64().unwrap_or(0);
            if p < self.max_seen && p <= self.position {
                self.lower_seen = true;
            }
            self.max_seen = self.max_seen.max(p);
            if e["kind"] != "artifact" {
                continue;
            }
            let id: Uuid = e["subject"].as_str().and_then(|s| s.parse().ok()).unwrap_or_default();
            let rev = e["revision"].as_i64().unwrap_or(0);
            match self.docs.get(&id) {
                Some(r) if *r >= rev => self.duplicates += 1,
                _ => {
                    if e["removal"] == true {
                        self.docs.remove(&id);
                    } else {
                        self.docs.insert(id, rev);
                        if let Some(path) = e["data"]["path"].as_str() {
                            self.paths.insert(path.to_string());
                        }
                    }
                }
            }
        }
    }
    async fn poll_all(&mut self, s: &Session) -> Result<()> {
        stream::flush(s).await?;
        loop {
            let p = stream::poll(s, self.position, 2000, None, false).await?;
            let n = p.events.len();
            self.apply(&p.events);
            if p.next < self.position {
                self.lower_seen = true;
            }
            self.position = p.next;
            if n == 0 && p.next >= p.last {
                return Ok(());
            }
        }
    }
}

/// T4.1: keep an index outside mudcat equal to the catalog, with the change stream only.
pub async fn stream_consumer(ctx: &mut Ctx) -> Result<()> {
    let writers = ctx.n("writers") as usize;
    let consumer_state = ctx.p("consumer");
    let rollbacks = ctx.is("rollbacks", "some");
    let total: usize = if ctx.scale == super::Scale::Quick { 2000 } else { 10_000 };
    let admin = ctx.admin.clone();
    in_tx!(admin, |tx| namespace::ensure_collection(&mut tx, "/docs").await?);
    let per_writer = total / writers;
    let done = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let rolled_back = Arc::new(parking_lot::Mutex::new(Vec::<String>::new()));
    let mut handles = Vec::new();
    for w in 0..writers {
        let s = ctx.session("user:admin");
        let rb = rolled_back.clone();
        handles.push(tokio::spawn(async move {
            let mut written = 0usize;
            let mut t = 0usize;
            while written < per_writer {
                let n = (10 + (w * 7 + t * 13) % 40).min(per_writer - written);
                let rollback = rollbacks && t % 4 == 1;
                let regs: Vec<Register> = (0..n.max(8))
                    .map(|i| {
                        let path = if rollback { format!("/docs/rb-w{w}-t{t}-{i}") } else { format!("/docs/w{w}-{:05}", written + i) };
                        Register { path, content_hash: Some(crate::instance::sha256_hex(format!("w{w}-t{t}-{i}").as_bytes())), size: 100, ..Default::default() }
                    })
                    .take(n.max(if rollback { 8 } else { n }))
                    .collect();
                let mut c = s.client().await?;
                let mut tx = s.begin(&mut c).await?;
                namespace::register_batch(&mut tx, &regs).await?;
                // a long transaction overlaps others
                tokio::time::sleep(std::time::Duration::from_millis(((w * 3 + t * 5) % 15) as u64)).await;
                if rollback {
                    rb.lock().extend(regs.iter().map(|r| r.path.clone()));
                    drop(tx); // rolls back
                } else {
                    tx.commit().await?;
                    written += n;
                }
                t += 1;
            }
            Ok::<_, Error>(())
        }));
    }
    let mut consumer = Consumer::default();
    let mut restart_seen = false;
    let consumer_session = ctx.session("user:admin");
    if consumer_state == "current" || consumer_state == "stops-and-restarts" {
        // the consumer polls while the writers write
        let mut stored = 0i64;
        let mut polls = 0;
        loop {
            let finished = handles.iter().all(|h| h.is_finished());
            let p = stream::poll(&consumer_session, consumer.position, 500, None, false).await?;
            consumer.apply(&p.events);
            if p.next < consumer.position {
                consumer.lower_seen = true;
            }
            consumer.position = p.next;
            polls += 1;
            if polls % 3 == 0 {
                stored = consumer.position; // the consumer stores its position now and then
            }
            if consumer_state == "stops-and-restarts" && polls == 6 && !restart_seen {
                // it stops, and starts again from its stored position: it gets some events a second time
                restart_seen = true;
                consumer.position = stored.saturating_sub(5).max(0);
                consumer.max_seen = consumer.position;
            }
            if finished {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
    }
    for h in handles {
        h.await.map_err(|e| Error::invalid(e.to_string()))??;
    }
    done.store(true, std::sync::atomic::Ordering::SeqCst);
    ctx.step().await?;
    let mut must_backfill = false;
    let mut backfill_ok = None;
    if consumer_state == "position-too-old" {
        // the consumer was away longer than the retention of change events
        ctx.inst.clock.advance(ctx.inst.config.event_retention + chrono::Duration::days(1));
        stream::prune(&consumer_session).await?;
        match stream::poll(&consumer_session, 0, 100, None, false).await {
            Err(e) if e.code() == "position-too-old" => {
                must_backfill = true;
                ctx.check("tells the consumer that its position is too old", true, e.to_string());
                let (pos, docs) = stream::backfill(&consumer_session, None).await?;
                for d in &docs {
                    if d["kind"] == "artifact" {
                        consumer.docs.insert(d["id"].as_str().unwrap().parse().unwrap(), d["revision"].as_i64().unwrap_or(0));
                    }
                }
                consumer.position = pos;
                consumer.max_seen = pos;
                // new writes after the backfill reach the consumer through the live stream
                let regs: Vec<Register> = (0..20).map(|i| Register { path: format!("/docs/after-{i:03}"), content_hash: Some(format!("after-{i}")), size: 1, ..Default::default() }).collect();
                in_tx!(admin, |tx| namespace::register_batch(&mut tx, &regs).await?);
                let before = consumer.docs.len();
                consumer.poll_all(&consumer_session).await?;
                backfill_ok = Some(consumer.docs.len() == before + 20);
            }
            Err(e) => return Err(e),
            Ok(_) => {
                ctx.check("tells the consumer that its position is too old", false, "no error");
            }
        }
    } else {
        // behind: it starts now, from position 0; the others finish their polling
        consumer.poll_all(&consumer_session).await?;
    }
    ctx.outcome(if must_backfill { "must-backfill" } else { "complete-in-order" });
    if let Some(ok) = backfill_ok {
        ctx.check("After a backfill, the live stream continues with no gap", ok, "");
    }
    // a second registration of the same document makes nothing new (INV-13)
    let first = in_tx!(admin, |tx| tx.q1("select a.name, v.content_hash from artifact a join version v on v.id = a.latest_version_id limit 1", &[]).await?);
    let name: String = first.get(0);
    let hash: String = first.get(1);
    in_tx!(admin, |tx| namespace::register_batch(&mut tx, &[Register { path: format!("/docs/{name}"), content_hash: Some(hash.clone()), size: 100, ..Default::default() }]).await?);
    consumer.poll_all(&consumer_session).await?;
    let catalog: BTreeSet<Uuid> = in_tx!(admin, |tx| tx.q("select id from artifact a where a.collection_id = (select id from collection where path = '/docs')", &[]).await?.iter().map(|r| r.get(0)).collect());
    let mine: BTreeSet<Uuid> = consumer.docs.keys().copied().collect();
    ctx.check("never gets a position lower than a position", !consumer.lower_seen || consumer_state == "stops-and-restarts", "");
    ctx.check("has exactly the documents of the catalog", mine == catalog, format!("consumer {}, catalog {}", mine.len(), catalog.len()));
    let rb = rolled_back.lock().clone();
    let leaked = rb.iter().filter(|p| consumer.paths.contains(*p)).count();
    ctx.check("got no event for a transaction that rolled back", !rb.is_empty() && leaked == 0, format!("{} rolled-back documents, {leaked} seen", rb.len()));
    ctx.check("Events that the consumer gets a second time change nothing", restart_seen && consumer.duplicates > 0 && mine == catalog, format!("{} events came again", consumer.duplicates));
    ctx.check("gets all events that it did not have, with no backfill", mine == catalog, "");
    ctx.step().await?;
    Ok(())
}

/// T4.2: after a change to access or content, the next request shows the new state at once.
pub async fn retrieval_after_change(ctx: &mut Ctx) -> Result<()> {
    let change = ctx.p("change");
    let request = ctx.p("request");
    let admin = ctx.admin.clone();
    let store = resource(&admin, "store", "cloud-a", "managed", false).await?;
    acl_root(&admin, "/docs/projects", Some(("store", "projects/"))).await?;
    acl_root(&admin, "/docs/other", Some(("store", "other/"))).await?;
    if change == "remove-direct-grant" {
        grant_collection(&admin, "/docs/projects", "reader", "user:ken").await?;
    } else {
        grant_collection(&admin, "/docs/projects", "reader", "group:contractors#member").await?;
        grant(&admin, "group", "contractors", "member", "user:ken").await?;
    }
    grant_collection(&admin, "/docs/projects", "reader", "user:lea").await?;
    grant_collection(&admin, "/docs/other", "reader", "group:staff#member").await?;
    grant(&admin, "group", "staff", "member", "user:ken").await?;
    grant(&admin, "group", "staff", "member", "user:lea").await?;
    grant(&admin, "compartment", "restricted", "member", "user:lea").await?;
    in_tx!(admin, |tx| {
        crate::metadata::declare_attribute(&mut tx, &crate::metadata::AttributeSpec { namespace: "tags".into(), name: "sensitivity".into(), datatype: "string".into(), is_tag: true, permitted: Some(vec!["normal".into(), "restricted".into()]), ..Default::default() }).await?;
        crate::metadata::set_tag_policy(&mut tx, "sensitivity", "restricted", "compartment", json!({"compartment": "restricted"})).await?;
    });
    let p_files = vec![("budget.txt".to_string(), b"project budget for the bridge, version one".to_vec())];
    let q_files = vec![("budget-copy.txt".to_string(), b"project budget for the bridge, a copy".to_vec())];
    let p = register_files(&admin, &store, "/docs/projects", "projects/", &p_files, None, None).await?[0].clone();
    let q = register_files(&admin, &store, "/docs/other", "other/", &q_files, None, None).await?[0].clone();
    for (v, text) in [(p.version_id, "project budget for the bridge"), (q.version_id, "project budget for the bridge (copy)")] {
        in_tx!(admin, |tx| crate::metadata::write(&mut tx, &crate::metadata::WriteMetadata { entity: v, namespace: "dc".into(), payload: json!({"description": text}), ..Default::default() }).await?);
    }
    verify_all(&admin, "store").await?;
    search::build(&admin).await?;
    ctx.step().await?;
    // the change
    let mut v2 = None;
    match change.as_str() {
        "remove-from-group" => revoke(&admin, "group", "contractors", "member", "user:ken").await?,
        "remove-direct-grant" => {
            let root = in_tx!(admin, |tx| namespace::collection(&tx, "/docs/projects").await?.acl_root);
            revoke(&admin, "collection", &root.to_string(), "reader", "user:ken").await?
        }
        "invalidate-document" => {
            in_tx!(admin, |tx| policy::invalidate(&mut tx, "artifact", &p.artifact_id.to_string(), "retention period ended", "records-officer").await?);
        }
        "delete-marker" => {
            in_tx!(admin, |tx| namespace::delete_artifact(&mut tx, "/docs/projects/budget.txt").await?);
        }
        "new-version" => {
            let f = vec![("budget.txt".to_string(), b"project budget for the bridge, version two".to_vec())];
            let r = register_files(&admin, &store, "/docs/projects", "projects/v2-", &f, None, None).await?[0].clone();
            in_tx!(admin, |tx| crate::metadata::write(&mut tx, &crate::metadata::WriteMetadata { entity: r.version_id, namespace: "dc".into(), payload: json!({"description": "project budget for the bridge, revised"}), ..Default::default() }).await?);
            verify_all(&admin, "store").await?;
            v2 = Some(r.version_id);
        }
        _ => {
            in_tx!(admin, |tx| crate::metadata::tag(&mut tx, p.artifact_id, "sensitivity", "restricted").await?);
        }
    }
    let stale = request == "search-with-stale-index";
    if !stale {
        search::build(&admin).await?;
    }
    stream::flush(&admin).await?;
    let (index_pos, last): (i64, i64) = in_tx!(admin, |tx| {
        let a: i64 = tx.q1("select position from index_state where name = 'search'", &[]).await?.get(0);
        let b: i64 = tx.q1("select last_position from stream_state", &[]).await?.get(0);
        (a, b)
    });
    let deny_before = audit_count(&admin, if request == "resolve" { "resolution" } else { "search" }, "deny").await?;
    let ken = ctx.session("user:ken");
    let lea = ctx.session("user:lea");
    // what each user gets: the versions of P and of Q in the answer
    let ask = |s: Session| {
        let request = request.clone();
        async move {
            if request == "resolve" {
                let mut got = BTreeSet::new();
                for path in ["/docs/projects/budget.txt", "/docs/other/budget-copy.txt"] {
                    let req = resolver::Request { inputs: vec![resolver::Input { artifact: Some(path.into()), role: "retrieval".into(), ..Default::default() }], ..Default::default() };
                    if let Ok(r) = resolver::resolve(&s, &req).await {
                        got.extend(r.tasks.iter().map(|t| t.version));
                    }
                }
                Ok::<_, Error>(got)
            } else {
                let r = search::search(&s, &SearchRequest { text: Some("project budget bridge".into()), ..Default::default() }).await?;
                Ok(r.hits.iter().filter_map(|h| h.version).collect())
            }
        }
    };
    let ken_got = ask(ken.clone()).await?;
    let lea_got = ask(lea.clone()).await?;
    if request != "resolve" {
        ctx.record.search_results.push((ctx.inst.now(), ken_got.iter().copied().collect()));
        ctx.record.search_results.push((ctx.inst.now(), lea_got.iter().copied().collect()));
    }
    let deny_after = audit_count(&admin, if request == "resolve" { "resolution" } else { "search" }, "deny").await?;
    let p_versions: BTreeSet<Uuid> = in_tx!(admin, |tx| tx.q("select id from version where artifact_id = $1", &[&p.artifact_id]).await?.iter().map(|r| r.get(0)).collect());
    let ken_p: BTreeSet<Uuid> = ken_got.intersection(&p_versions).copied().collect();
    let outcome = if change == "new-version" {
        if ken_p.contains(&v2.unwrap()) && !ken_p.contains(&p.version_id) { "shows-new-version" } else { "unexpected" }
    } else if ken_p.is_empty() {
        "hidden-at-once"
    } else {
        "visible"
    };
    ctx.outcome(outcome);
    ctx.check("The second document is in the answer as before", ken_got.contains(&q.version_id) && lea_got.contains(&q.version_id), "");
    ctx.check("contains nothing from the affected content", ken_p.is_empty(), format!("{ken_p:?}"));
    ctx.check("although the search index has not processed the change", index_pos < last, format!("index at {index_pos}, stream at {last}"));
    ctx.check("The second user still gets the document", lea_got.contains(&p.version_id), "");
    let cached = ctx.inst.metrics.cached_decisions.load(std::sync::atomic::Ordering::SeqCst);
    ctx.check("used no cached decision, and the audit log has the denial", cached == 0 && deny_after > deny_before, format!("denials {deny_before}->{deny_after}"));
    ctx.check("The second user does not get the document", lea_got.intersection(&p_versions).count() == 0, "");
    if change == "delete-marker" {
        let req = resolver::Request { inputs: vec![resolver::Input { version: Some(p.version_id), role: "retrieval".into(), ..Default::default() }], ..Default::default() };
        let r = resolve(ctx, &lea, &req).await;
        ctx.check("An older version still resolves by its version ID", r.map(|r| r.tasks.iter().any(|t| t.version == p.version_id)).unwrap_or(false), "");
    }
    if let Some(v2) = v2 {
        ctx.check("The answer has the new version, and not the old version", ken_p.contains(&v2) && !ken_p.contains(&p.version_id), "");
        let req = resolver::Request { inputs: vec![resolver::Input { version: Some(p.version_id), role: "retrieval".into(), ..Default::default() }], ..Default::default() };
        let r = resolve(ctx, &ken, &req).await;
        ctx.check("A citation of the old version still resolves to the old version", r.map(|r| r.tasks.iter().all(|t| t.version == p.version_id) && !r.tasks.is_empty()).unwrap_or(false), "");
    }
    ctx.step().await?;
    Ok(())
}
