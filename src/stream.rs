//! The change stream (architecture.md, "Change stream").
//!
//! A commit writes its events to the outbox in the same transaction. The relay reads only the
//! outbox rows of transactions that are older than every transaction still in progress, and
//! gives positions at publication, in the order of the commit timestamps. Thus no earlier event
//! can appear after a later position (EV-13), and an event exists if and only if its change
//! committed (EV-1). The built-in transport is an HTTP feed with a position (EV-8).

use crate::authz;
use crate::error::{Error, Result};
use crate::instance::{new_id, Session, Tx};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::{BTreeMap, HashSet};
use uuid::Uuid;

/// Publish the committed events of the outbox. Returns the number of events published.
pub async fn relay(sess: &Session) -> Result<usize> {
    let sys = sess.as_principal(crate::instance::Principal::system());
    let mut c = sys.client().await?;
    let tx = sys.begin(&mut c).await?;
    let tenant = sys.tenant_id()?;
    tx.exec("select pg_advisory_xact_lock(hashtextextended($1, 7))", &[&tenant.to_string()]).await?;
    tx.exec("insert into stream_state (last_position, floor) values (0, 0) on conflict do nothing", &[]).await?;
    // The oldest transaction still in progress in this database. The snapshot lists the
    // transactions in progress on the whole server; those of other databases cannot write this
    // outbox, so they must not hold the stream back.
    let threshold: String = tx
        .q1(
            "with snap as (select pg_current_snapshot() as s),
                  other as (select backend_xid::text::bigint as x from pg_stat_activity
                             where datname is distinct from current_database() and backend_xid is not null),
                  mine as (select x from snap, pg_snapshot_xip(snap.s) as x
                            where (x::text::bigint % 4294967296) not in (select x from other))
             select coalesce((select min(x) from mine), (select pg_snapshot_xmax(s) from snap))::text",
            &[],
        )
        .await?
        .get(0);
    let rows = tx
        .q(
            "select seq, commit_id, event from outbox
              where xid < $1::text::xid8
              order by coalesce(pg_xact_commit_timestamp((xid::text)::xid), 'epoch'::timestamptz), xid, seq",
            &[&threshold],
        )
        .await?;
    if rows.is_empty() {
        tx.commit().await?;
        return Ok(0);
    }
    let last: i64 = tx.q1("select last_position from stream_state for update", &[]).await?.get(0);
    let mut pos = last;
    let mut seqs = Vec::with_capacity(rows.len());
    let mut positions = Vec::with_capacity(rows.len());
    let mut commits = Vec::with_capacity(rows.len());
    let mut types = Vec::with_capacity(rows.len());
    let mut subjects = Vec::with_capacity(rows.len());
    let mut revisions = Vec::with_capacity(rows.len());
    let mut ats = Vec::with_capacity(rows.len());
    let mut actors = Vec::with_capacity(rows.len());
    let mut roots: Vec<Option<Uuid>> = Vec::with_capacity(rows.len());
    let mut removals = Vec::with_capacity(rows.len());
    let mut datas = Vec::with_capacity(rows.len());
    for r in &rows {
        pos += 1;
        let mut ev: Value = r.get(2);
        ev["sequence"] = json!(pos);
        ev["source"] = json!(format!("urn:mudcat:tenant:{tenant}"));
        ev["specversion"] = json!("1.0");
        ev["dataschema"] = json!(format!("https://mudcat.example/schemas/{}.json", ev["type"].as_str().unwrap_or("")));
        seqs.push(r.get::<_, i64>(0));
        positions.push(pos);
        commits.push(r.get::<_, i64>(1));
        types.push(ev["type"].as_str().unwrap_or("").to_string());
        subjects.push(ev["subject"].as_str().and_then(|s| s.parse::<Uuid>().ok()).unwrap_or_default());
        revisions.push(ev["revision"].as_i64().unwrap_or(0) as i32);
        ats.push(ev["time"].as_str().and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok()).map(|t| t.with_timezone(&chrono::Utc)).unwrap_or(tx.now));
        actors.push(ev["actor"].as_str().unwrap_or("").to_string());
        roots.push(ev["acl_root"].as_str().and_then(|s| s.parse().ok()));
        removals.push(ev["removal"].as_bool().unwrap_or(false));
        datas.push(ev);
    }
    tx.exec(
        "insert into event (position, commit_id, type, subject, revision, at, actor, acl_root, removal, data)
         select * from unnest($1::bigint[], $2::bigint[], $3::text[], $4::uuid[], $5::int[], $6::timestamptz[], $7::text[], $8::uuid[], $9::bool[], $10::jsonb[])",
        &[&positions, &commits, &types, &subjects, &revisions, &ats, &actors, &roots, &removals, &datas],
    )
    .await?;
    tx.exec("delete from outbox where seq = any($1)", &[&seqs]).await?;
    tx.exec("update stream_state set last_position = $1", &[&pos]).await?;
    tx.commit().await?;
    Ok(rows.len())
}

/// Remove change events that are older than the retention period.
pub async fn prune(sess: &Session) -> Result<i64> {
    let mut c = sess.client().await?;
    let tx = sess.begin(&mut c).await?;
    let cutoff = tx.now - tx.inst().config.event_retention;
    let r = tx.q1("with d as (delete from event where at < $1 returning position) select coalesce(max(position), 0)::bigint from d", &[&cutoff]).await?;
    let max: i64 = r.get(0);
    if max > 0 {
        tx.exec("update stream_state set floor = greatest(floor, $1)", &[&max]).await?;
    }
    tx.commit().await?;
    Ok(max)
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Poll {
    pub events: Vec<Value>,
    /// The position to poll from next time.
    pub next: i64,
    pub last: i64,
}

pub async fn subscribe(sess: &Session, scope: &str, stream: &str) -> Result<Uuid> {
    let mut c = sess.client().await?;
    let mut tx = sess.begin(&mut c).await?;
    let id = new_id();
    tx.exec(
        "insert into subscription (id, principal, scope, stream, created_at) values ($1,$2,$3,$4,$5)",
        &[&id, &sess.principal.0, &scope, &stream, &tx.now],
    )
    .await?;
    tx.register_entity(id, "subscription").await?;
    tx.commit().await?;
    Ok(id)
}

struct Filter {
    roots: Option<HashSet<Uuid>>,
    scope_roots: Option<HashSet<Uuid>>,
    admin: bool,
}

async fn filter_for(tx: &Tx<'_>, scope: Option<&str>) -> Result<Filter> {
    let roots = authz::readable_roots(tx).await?;
    let admin = roots.is_none();
    let scope_roots = match scope {
        Some(s) if s != "/" => {
            let like = format!("{}/%", s.trim_end_matches('/'));
            Some(tx.q("select distinct acl_root_id from collection where path = $1 or path like $2", &[&s, &like]).await?.iter().map(|r| r.get(0)).collect())
        }
        _ => None,
    };
    Ok(Filter { roots: roots.map(|r| r.into_iter().collect()), scope_roots, admin })
}

impl Filter {
    /// Send to a subscription only the events for entities that its identity can read (EV-6).
    fn allows(&self, ev: &Value) -> bool {
        let root: Option<Uuid> = ev["acl_root"].as_str().and_then(|s| s.parse().ok());
        match root {
            Some(r) => {
                self.roots.as_ref().map(|s| s.contains(&r)).unwrap_or(true) && self.scope_roots.as_ref().map(|s| s.contains(&r)).unwrap_or(true)
            }
            None => self.admin || ev["kind"] == "relationship",
        }
    }
}

/// Get the events after a position. A position older than the retention of change events fails
/// with an error, and the consumer must ask for a backfill.
pub async fn poll(sess: &Session, from: i64, limit: i64, subscription: Option<Uuid>, state: bool) -> Result<Poll> {
    relay(sess).await?;
    let mut c = sess.client().await?;
    let tx = sess.begin(&mut c).await?;
    let st = tx.qopt("select last_position, floor from stream_state", &[]).await?;
    let (last, floor): (i64, i64) = st.map(|r| (r.get(0), r.get(1))).unwrap_or((0, 0));
    if from < floor {
        return Err(Error::refused(
            "position-too-old",
            format!("the position {from} is older than the retention of change events (the oldest kept position is {}); ask for a backfill", floor + 1),
            json!({"position": from, "floor": floor}),
        ));
    }
    let scope = match subscription {
        Some(s) => {
            let r = tx.qopt("select principal, scope from subscription where id = $1", &[&s]).await?.ok_or(Error::NotFound)?;
            let p: String = r.get(0);
            if p != sess.principal.0 {
                return Err(Error::NotFound);
            }
            Some(r.get::<_, String>(1))
        }
        None => None,
    };
    let f = filter_for(&tx, scope.as_deref()).await?;
    let rows = tx.q("select position, data from event where position > $1 order by position limit $2", &[&from, &limit]).await?;
    let mut next = from;
    let mut events = Vec::new();
    for r in rows {
        let p: i64 = r.get(0);
        next = p;
        let ev: Value = r.get(1);
        if !f.allows(&ev) {
            continue;
        }
        events.push(ev);
    }
    if state {
        // state documents: the newest description of each entity that changed, or its removal
        let mut latest: BTreeMap<Uuid, Value> = BTreeMap::new();
        let mut order = Vec::new();
        for ev in &events {
            let s: Uuid = ev["subject"].as_str().and_then(|s| s.parse().ok()).unwrap_or_default();
            if !latest.contains_key(&s) {
                order.push(s);
            }
            latest.insert(s, ev.clone());
        }
        let mut docs = Vec::new();
        for s in order {
            let ev = &latest[&s];
            let mut d = if ev["removal"] == true {
                json!({"id": s, "removed": true, "kind": ev["kind"], "reason": ev["data"]["reason"]})
            } else {
                state_document(&tx, s).await?
            };
            d["sequence"] = ev["sequence"].clone();
            d["revision"] = ev["revision"].clone();
            docs.push(d);
        }
        events = docs;
    }
    Ok(Poll { events, next, last })
}

/// The state document of each entity that the identity can read, and the position where the
/// backfill started. The live stream continues from that position with no gap (EV-5).
pub async fn backfill(sess: &Session, subscription: Option<Uuid>) -> Result<(i64, Vec<Value>)> {
    relay(sess).await?;
    let mut c = sess.client().await?;
    let tx = sess.begin(&mut c).await?;
    let position: i64 = tx.qopt("select last_position from stream_state", &[]).await?.map(|r| r.get(0)).unwrap_or(0);
    let scope = match subscription {
        Some(s) => tx.qopt("select scope from subscription where id = $1", &[&s]).await?.map(|r| r.get::<_, String>(0)),
        None => None,
    };
    let f = filter_for(&tx, scope.as_deref()).await?;
    let rows = tx
        .q(
            "select a.id, c.acl_root_id from artifact a join collection c on c.id = a.collection_id and c.tenant_id = a.tenant_id
             union all select c.id, c.acl_root_id from collection c
             union all select d.id, c.acl_root_id from dataset d join collection c on c.id = d.collection_id and c.tenant_id = d.tenant_id",
            &[],
        )
        .await?;
    let mut docs = Vec::new();
    for r in rows {
        let id: Uuid = r.get(0);
        let root: Uuid = r.get(1);
        if !f.allows(&json!({"acl_root": root})) {
            continue;
        }
        let d = state_document(&tx, id).await?;
        if d["removed"] == true {
            continue;
        }
        docs.push(d);
    }
    Ok((position, docs))
}

/// The complete current description of an entity: identity, description, metadata, tags,
/// structure, statistics, lineage, policy and access (architecture.md, "Search document").
/// Customer-held values are never in it (ST-10).
pub async fn state_document(tx: &Tx<'_>, id: Uuid) -> Result<Value> {
    let ent = tx.qopt("select kind, revision, erased from entity where id = $1", &[&id]).await?;
    let (kind, revision) = match &ent {
        Some(r) if r.get::<_, bool>(2) => return Ok(json!({"id": id, "removed": true, "reason": "erased"})),
        Some(r) => (r.get::<_, String>(0), r.get::<_, i32>(1)),
        None => return Ok(json!({"id": id, "removed": true})),
    };
    let identifiers: Vec<Value> = crate::namespace::identifiers_of(tx, id).await?.into_iter().map(|(s, v)| json!({"scheme": s, "value": v})).collect();
    let tags: Vec<Value> = crate::metadata::effective_tags(tx, id).await?.into_iter().map(|(n, v)| json!({"name": n, "value": v})).collect();
    let metadata = crate::metadata::payloads(tx, id, false).await?;
    let mut doc = json!({"id": id, "kind": kind, "revision": revision, "identifiers": identifiers, "tags": tags, "metadata": metadata});
    match kind.as_str() {
        "artifact" => {
            let r = tx
                .q1(
                    "select a.name, a.kind, a.latest_version_id, c.path, c.acl_root_id from artifact a join collection c on c.id = a.collection_id and c.tenant_id = a.tenant_id where a.id = $1",
                    &[&id],
                )
                .await?;
            let latest: Option<Uuid> = r.get(2);
            let path = format!("{}/{}", r.get::<_, String>(3).trim_end_matches('/'), r.get::<_, String>(0));
            doc["path"] = json!(path);
            doc["artifact_kind"] = json!(r.get::<_, String>(1));
            let root: Uuid = r.get(4);
            if let Some(l) = latest {
                let v = crate::namespace::version(tx, l).await?;
                if v.is_delete_marker {
                    return Ok(json!({"id": id, "removed": true, "reason": "delete marker", "revision": revision}));
                }
                if crate::policy::is_invalidated(tx, l).await? {
                    return Ok(json!({"id": id, "removed": true, "reason": "invalidated", "revision": revision}));
                }
                doc["latest_version"] = json!({"id": v.id, "number": v.number, "content_hash": v.content_hash, "size": v.size});
                doc["structure"] = json!({"kind": v.structure["kind"], "fields": v.structure["fields"]});
                doc["statistics"] = json!({"size": v.size, "elements": v.element_count, "stats": v.stats});
                doc["metadata_version"] = crate::metadata::payloads(tx, v.id, false).await?;
                if let Some(s) = v.source_id {
                    let sr = tx.q1("select name, kind from source where id = $1", &[&s]).await?;
                    doc["lineage"] = json!({"root_sources": [{"id": s, "name": sr.get::<_, String>(0), "kind": sr.get::<_, String>(1)}]});
                    let grants = tx.q("select action, purpose from usage_grant where source_id = $1 and revoked_commit is null", &[&s]).await?;
                    doc["policy"] = json!({"permitted": grants.iter().map(|g| format!("{}:{}", g.get::<_, String>(0), g.get::<_, String>(1))).collect::<Vec<_>>()});
                }
            }
            doc["access"] = json!({"acl_root": root, "compartments": authz::compartments_of(tx, "artifact", id).await?});
        }
        "version" => {
            let v = crate::namespace::version(tx, id).await?;
            doc["path"] = json!(v.path);
            doc["artifact"] = json!(v.artifact_id);
            doc["number"] = json!(v.number);
            doc["content_hash"] = json!(v.content_hash);
            doc["structure"] = json!({"kind": v.structure["kind"]});
            doc["access"] = json!({"acl_root": v.acl_root});
        }
        "collection" => {
            let r = tx.q1("select path, acl_root_id from collection where id = $1", &[&id]).await?;
            doc["path"] = json!(r.get::<_, String>(0));
            doc["access"] = json!({"acl_root": r.get::<_, Uuid>(1)});
        }
        "dataset" => {
            let r = tx.q1("select path from dataset where id = $1", &[&id]).await?;
            doc["path"] = json!(r.get::<_, String>(0));
            let refs = tx.q("select name, kind, snapshot_id from ref where dataset_id = $1 order by name", &[&id]).await?;
            doc["refs"] = json!(refs.iter().map(|r| json!({"name": r.get::<_, String>(0), "kind": r.get::<_, String>(1), "snapshot": r.get::<_, Uuid>(2)})).collect::<Vec<_>>());
            doc["access"] = json!({"acl_root": crate::metadata::acl_root_of(tx, id).await?});
        }
        "snapshot" => {
            let s = crate::datasets::snapshot(tx, id).await?;
            doc["dataset"] = json!(s.dataset_id);
            doc["status"] = json!(s.status);
            doc["merkle_hash"] = json!(s.merkle_hash);
            doc["summary"] = s.summary;
            doc["access"] = json!({"acl_root": crate::metadata::acl_root_of(tx, id).await?});
        }
        _ => {}
    }
    Ok(doc)
}

/// Publish until the outbox of the tenant is empty. The outbox rows that a transaction can see
/// are committed, so this waits only for older transactions still in progress, which can hold
/// the relay back (the relay publishes only below the oldest transaction in progress).
pub async fn flush(sess: &Session) -> Result<()> {
    let sys = sess.as_principal(crate::instance::Principal::system());
    let started = std::time::Instant::now();
    loop {
        relay(&sys).await?;
        let n: i64 = {
            let mut c = sys.client().await?;
            let tx = sys.begin(&mut c).await?;
            tx.q1("select count(*) from outbox", &[]).await?.get(0)
        };
        if n == 0 || started.elapsed() > std::time::Duration::from_secs(120) {
            return Ok(());
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
}
