//! Placement: replication rules, transfers, completeness (model.md, section 8), and the parts
//! that run next to the data: site agents, movers and the read connector of a compute engine.

use crate::error::{Error, Result};
use crate::instance::{new_id, sha256_hex, Session, Tx};
use crate::namespace::{self, Register};
use crate::storage::parse_url;
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::Ordering;
use uuid::Uuid;

/// The member versions of a placement subject: a snapshot, a representation, or a dataset (its main branch).
pub async fn subject_versions(tx: &Tx<'_>, subject: Uuid) -> Result<Vec<Uuid>> {
    if let Some(r) = tx.qopt("select members from representation where id = $1", &[&subject]).await? {
        return Ok(r.get(0));
    }
    let snap = if tx.qopt("select 1 from dataset where id = $1", &[&subject]).await?.is_some() {
        crate::datasets::resolve_ref(tx, subject, "main", None).await?
    } else {
        subject
    };
    let mut out = Vec::new();
    let mut stack = vec![snap];
    while let Some(s) = stack.pop() {
        for e in crate::datasets::members(tx, s).await? {
            match e.member_kind.as_str() {
                "version" => out.push(e.member_id),
                "snapshot" => stack.push(e.member_id),
                _ => {}
            }
        }
    }
    Ok(out)
}

async fn jurisdiction_rules(tx: &Tx<'_>, versions: &[Uuid]) -> Result<Vec<(String, Vec<String>)>> {
    let infos = namespace::versions(tx, versions).await?;
    let colls: BTreeSet<Uuid> = infos.iter().map(|v| v.collection_id).collect();
    let mut out = Vec::new();
    for c in colls {
        for (name, value) in crate::metadata::effective_tags(tx, c).await? {
            for r in tx.q("select config from tag_policy where name = $1 and value = $2 and kind = 'jurisdiction'", &[&name, &value]).await? {
                let cfg: Value = r.get(0);
                let allowed = cfg["allowed_locations"].as_array().cloned().unwrap_or_default().iter().filter_map(|v| v.as_str().map(|s| s.to_string())).collect();
                out.push((format!("{name} = {value}"), allowed));
            }
        }
    }
    Ok(out)
}

/// Create a replication rule and the transfer requests that satisfy it (PL-2). A rule must obey
/// the jurisdiction rules of the data and the egress rules of its storage.
pub async fn add_rule(tx: &mut Tx<'_>, subject: Uuid, resources: &[String], copies: i32, lifetime: Option<chrono::Duration>) -> Result<Value> {
    let versions = subject_versions(tx, subject).await?;
    let mut targets = Vec::new();
    for r in resources {
        targets.push(namespace::resource(tx, r).await?);
    }
    for (rule, allowed) in jurisdiction_rules(tx, &versions).await? {
        for t in &targets {
            if !allowed.contains(&t.location) {
                tx.sess.audit("materialisation", "replicate", &format!("subject:{subject}"), false, Some("jurisdiction"), json!({"rule": rule, "target": t.name})).await?;
                return Err(Error::refused(
                    "jurisdiction",
                    format!("mudcat rejects the rule: the data cannot leave its jurisdiction ({rule}); {} is in {}", t.name, t.location),
                    json!({"rule": rule, "target": t.name, "allowed_locations": allowed}),
                ));
            }
        }
    }
    let commit = tx.commit_id().await?;
    let id = new_id();
    let expires = lifetime.map(|l| tx.now + l);
    let ids: Vec<Uuid> = targets.iter().map(|t| t.id).collect();
    tx.exec(
        "insert into replication_rule (id, subject_id, resources, copies, expires_at, owner, created_commit) values ($1,$2,$3,$4,$5,$6,$7)",
        &[&id, &subject, &ids, &copies, &expires, &tx.principal().0, &commit],
    )
    .await?;
    let reps = namespace::replicas_of(tx, &versions).await?;
    let mut transfers = 0;
    for t in &targets {
        for v in &versions {
            let here: Vec<_> = reps.iter().filter(|r| r.version_id == *v && r.resource_id == t.id && !["lost", "retired"].contains(&r.state.as_str())).collect();
            if !here.is_empty() {
                tx.exec("update replica set lock_until = greatest(coalesce(lock_until, $2), $2) where id = $1", &[&here[0].id, &expires.unwrap_or(tx.now + chrono::Duration::days(36500))]).await?;
                continue;
            }
            let Some(src) = reps.iter().find(|r| r.version_id == *v && r.state == "verified") else { continue };
            if !src.egress_allowed && src.location != t.location {
                return Err(Error::refused("egress", format!("data on {} cannot leave the resource", src.resource), json!({"resource": src.resource})));
            }
            let (_, key) = parse_url(&src.url)?;
            let url = t.url(&key);
            let rid = namespace::add_replica(tx, *v, t, &url, "copying", src.representation_id, None).await?;
            tx.exec("update replica set lock_until = $2 where id = $1", &[&rid, &expires.unwrap_or(tx.now + chrono::Duration::days(36500))]).await?;
            let tid = new_id();
            tx.exec(
                "insert into transfer (id, rule_id, version_id, source_replica_id, target_resource_id, target_replica_id, state, requested_at) values ($1,$2,$3,$4,$5,$6,'requested',$7)",
                &[&tid, &id, v, &src.id, &t.id, &rid, &tx.now],
            )
            .await?;
            transfers += 1;
        }
        refresh_completeness(tx, subject, t.id).await?;
    }
    tx.emit("replication-rule.created", id, "replication-rule", None, json!({"subject": subject, "resources": resources, "copies": copies, "expires_at": expires, "transfers": transfers}))
        .await?;
    Ok(json!({"rule": id, "transfers": transfers, "expires_at": expires}))
}

/// Compute the completeness record of a subject on a resource from the states of the replicas (PL-1).
pub async fn refresh_completeness(tx: &mut Tx<'_>, subject: Uuid, resource: Uuid) -> Result<Value> {
    let versions = subject_versions(tx, subject).await?;
    let verified: i64 = tx
        .q1("select count(distinct version_id) from replica where version_id = any($1) and resource_id = $2 and state = 'verified'", &[&versions, &resource])
        .await?
        .get(0);
    let total = versions.len() as i64;
    let complete = total > 0 && if crate::mutant("completeness-any-verified") { verified > 0 } else { verified == total };
    let commit = tx.commit_id().await?;
    let prev: Option<bool> = tx.qopt("select complete from completeness where subject_id = $1 and resource_id = $2", &[&subject, &resource]).await?.map(|r| r.get(0));
    tx.exec(
        "insert into completeness (subject_id, resource_id, complete, verified, total, at, commit_id) values ($1,$2,$3,$4,$5,$6,$7)
         on conflict (tenant_id, subject_id, resource_id) do update set complete = excluded.complete, verified = excluded.verified, total = excluded.total, at = excluded.at, commit_id = excluded.commit_id",
        &[&subject, &resource, &complete, &verified, &total, &tx.now, &commit],
    )
    .await?;
    if prev != Some(complete) {
        tx.emit("completeness.changed", subject, "completeness", None, json!({"resource": resource, "complete": complete, "verified": verified, "total": total})).await?;
    }
    Ok(json!({"subject": subject, "resource": resource, "complete": complete, "verified": verified, "total": total}))
}

/// Refresh the completeness of every rule and record that covers some of the versions.
async fn refresh_for_versions(tx: &mut Tx<'_>, versions: &BTreeSet<Uuid>, resource: Uuid) -> Result<()> {
    let subjects: Vec<Uuid> = tx
        .q("select distinct subject_id from completeness where resource_id = $1 union select subject_id from replication_rule where $1 = any(resources)", &[&resource])
        .await?
        .iter()
        .map(|r| r.get(0))
        .collect();
    for s in subjects {
        if subject_versions(tx, s).await?.iter().any(|v| versions.contains(v)) {
            refresh_completeness(tx, s, resource).await?;
        }
    }
    Ok(())
}

async fn refresh_for_version(tx: &mut Tx<'_>, version: Uuid, resource: Uuid) -> Result<()> {
    refresh_for_versions(tx, &BTreeSet::from([version]), resource).await
}

/// QY-4: the storage resources on which a subject is complete and verified.
pub async fn complete_on(tx: &mut Tx<'_>, subject: Uuid) -> Result<Vec<String>> {
    let versions = subject_versions(tx, subject).await?;
    let res: Vec<Uuid> = tx.q("select distinct resource_id from replica where version_id = any($1)", &[&versions]).await?.iter().map(|r| r.get(0)).collect();
    let mut out = Vec::new();
    for r in res {
        let c = refresh_completeness(tx, subject, r).await?;
        if c["complete"] == true {
            out.push(namespace::resource_by_id(tx, r).await?.name);
        }
    }
    out.sort();
    Ok(out)
}

/// End the rules whose lifetime is over. Their replicas lose their locks, and cache eviction may
/// then remove them.
pub async fn expire_rules(tx: &mut Tx<'_>) -> Result<Vec<Uuid>> {
    let rows = tx.q("update replication_rule set state = 'expired' where state = 'active' and expires_at < $1 returning id", &[&tx.now]).await?;
    let ids: Vec<Uuid> = rows.iter().map(|r| r.get(0)).collect();
    for id in &ids {
        tx.emit("replication-rule.removed", *id, "replication-rule", None, json!({"reason": "lifetime ended"})).await?;
    }
    Ok(ids)
}

/// Retire the replicas of a dataset on a resource. Returns a warning for each version whose last
/// replica this is.
pub async fn retire_replicas(tx: &mut Tx<'_>, subject: Uuid, resource: &str, confirm: bool) -> Result<Value> {
    let res = namespace::resource(tx, resource).await?;
    let versions = subject_versions(tx, subject).await?;
    let reps = namespace::replicas_of(tx, &versions).await?;
    let mut warnings = Vec::new();
    let mut tasks = Vec::new();
    for r in reps.iter().filter(|r| r.resource_id == res.id && r.state != "retired") {
        let others = reps.iter().filter(|o| o.version_id == r.version_id && o.id != r.id && !["lost", "retired"].contains(&o.state.as_str())).count();
        if others == 0 {
            warnings.push(json!({"version": r.version_id, "warning": "this is the last replica of the version"}));
        }
        if confirm {
            let id: i64 = tx
                .q1(
                    "insert into agent_task (kind, resource_id, payload) values ('delete', $1, $2) returning id",
                    &[&res.id, &json!({"replica": r.id, "version": r.version_id, "url": r.url, "all_versions": false, "reason": "retired"})],
                )
                .await?
                .get(0);
            tasks.push(id);
        }
    }
    Ok(json!({"warnings": warnings, "last_replica_of": warnings.len(), "deletion_tasks": tasks}))
}

/// Enqueue the deletion of replicas whose retention on their resource ended (for example a
/// facility that keeps data for 30 days).
pub async fn retention_sweep(tx: &mut Tx<'_>) -> Result<usize> {
    let rows = tx
        .q(
            "select r.id, r.version_id, r.url, r.resource_id from replica r join storage_resource s on s.id = r.resource_id and s.tenant_id = r.tenant_id
               join commit_log c on c.commit_id = r.created_commit and c.tenant_id = r.tenant_id
              where s.attrs ? 'retention_days' and r.state not in ('retired','lost')
                and c.at < $1::timestamptz - make_interval(days => (s.attrs->>'retention_days')::int)
                and not exists (select 1 from agent_task t where t.kind = 'delete' and t.payload->>'replica' = r.id::text)",
            &[&tx.now],
        )
        .await?;
    for r in &rows {
        tx.exec(
            "insert into agent_task (kind, resource_id, payload) values ('delete', $1, $2)",
            &[&r.get::<_, Uuid>(3), &json!({"replica": r.get::<_, Uuid>(0), "version": r.get::<_, Uuid>(1), "url": r.get::<_, String>(2), "all_versions": false, "reason": "retention ended"})],
        )
        .await?;
    }
    Ok(rows.len())
}

/// Ask the site agents to verify the replicas on a resource.
pub async fn enqueue_verify(tx: &mut Tx<'_>, resource: &str, only_unverified: bool) -> Result<usize> {
    let res = namespace::resource(tx, resource).await?;
    let rows = tx
        .q(
            "select id from replica where resource_id = $1 and state not in ('retired','lost','copying') and (not $2 or state <> 'verified')",
            &[&res.id, &only_unverified],
        )
        .await?;
    for r in &rows {
        tx.exec("insert into agent_task (kind, resource_id, payload) values ('verify', $1, $2)", &[&res.id, &json!({"replica": r.get::<_, Uuid>(0)})]).await?;
    }
    Ok(rows.len())
}

// ---------------------------------------------------------------------------- movers

#[derive(Clone, Debug, Default)]
pub struct MoverFault {
    /// The first n transfers arrive with wrong bytes.
    pub corrupt: usize,
    /// The mover stops after this number of transfers.
    pub stop_after: Option<usize>,
}

/// An external mover (FTS3, Globus, rclone...). It copies the bytes of requested transfers; then
/// the site agent of the target verifies them.
pub async fn run_mover(sess: &Session, fault: &MoverFault) -> Result<usize> {
    let mut c = sess.client().await?;
    let mut tx = sess.begin(&mut c).await?;
    let rows = tx
        .q(
            "select t.id, t.version_id, s.url, t.target_replica_id, tr.url, t.target_resource_id from transfer t
               join replica s on s.id = t.source_replica_id and s.tenant_id = t.tenant_id
               join replica tr on tr.id = t.target_replica_id and tr.tenant_id = t.tenant_id
              where t.state = 'requested' order by t.requested_at, t.id",
            &[],
        )
        .await?;
    let mut n = 0;
    let store = &sess.inst.store;
    for r in rows {
        if let Some(k) = fault.stop_after {
            if n >= k {
                break;
            }
        }
        let (sb, sk) = parse_url(&r.get::<_, String>(2))?;
        let (tb, tk) = parse_url(&r.get::<_, String>(4))?;
        let data = store.read_local("mover", &sb, &sk)?;
        store.put("mover", &tb, &tk, &data)?;
        sess.inst.metrics.bytes_copied_for_catalog.fetch_add(data.len() as u64, Ordering::SeqCst);
        if n < fault.corrupt {
            store.corrupt(&tb, &tk)?;
        }
        let tid: Uuid = r.get(0);
        let rep: Uuid = r.get(3);
        let res: Uuid = r.get(5);
        tx.exec("update transfer set state = 'done', finished_at = $2 where id = $1", &[&tid, &tx.now]).await?;
        namespace::set_replica_state(&mut tx, rep, "registered", json!({"transfer": tid})).await?;
        tx.exec("insert into agent_task (kind, resource_id, payload) values ('verify', $1, $2)", &[&res, &json!({"replica": rep})]).await?;
        tx.emit("transfer.finished", tid, "transfer", None, json!({"version": r.get::<_, Uuid>(1)})).await?;
        n += 1;
    }
    tx.commit().await?;
    Ok(n)
}

// ---------------------------------------------------------------------------- site agents

#[derive(Clone, Debug, Default, PartialEq)]
pub enum AgentFault {
    #[default]
    None,
    /// The agent takes its leases and stops before it finishes the tasks.
    StopsAfterLease,
}

/// A site agent. It runs next to one storage resource, opens its own connection to the catalog,
/// and leases tasks. A lease expires, so the work of a failed agent goes to another agent.
pub struct SiteAgent {
    pub name: String,
    pub resource: String,
    pending: parking_lot::Mutex<Vec<Value>>,
}

impl SiteAgent {
    pub fn new(name: &str, resource: &str) -> Self {
        SiteAgent { name: name.into(), resource: resource.into(), pending: parking_lot::Mutex::new(Vec::new()) }
    }

    /// Lease and run up to `max` tasks. Returns the number of tasks finished.
    pub async fn run(&self, sess: &Session, max: i64, fault: AgentFault) -> Result<usize> {
        let leased = {
            let mut c = sess.client().await?;
            let tx = sess.begin(&mut c).await?;
            let res = namespace::resource(&tx, &self.resource).await?;
            let lease_until = tx.now + tx.inst().config.lease;
            let rows = tx
                .q(
                    "update agent_task set state = 'leased', lease_owner = $2, lease_expires = $3, attempts = attempts + 1
                      where id in (select id from agent_task where resource_id = $1 and (state = 'queued' or (state = 'leased' and lease_expires < $4))
                                    order by id limit $5 for update skip locked)
                     returning id, kind, payload",
                    &[&res.id, &self.name, &lease_until, &tx.now, &max],
                )
                .await?;
            tx.commit().await?;
            rows.iter().map(|r| (r.get::<_, i64>(0), r.get::<_, String>(1), r.get::<_, Value>(2))).collect::<Vec<_>>()
        };
        if fault == AgentFault::StopsAfterLease {
            return Ok(0);
        }
        let mut done = 0;
        let mut c = sess.client().await?;
        let mut tx = sess.begin(&mut c).await?;
        let mut touched: BTreeMap<Uuid, BTreeSet<Uuid>> = BTreeMap::new();
        for (id, kind, payload) in leased {
            let result = match kind.as_str() {
                "verify" => self.verify(&mut tx, &payload, &mut touched).await?,
                "delete" => self.delete(&mut tx, &payload, &mut touched).await?,
                "rewrite" => json!({"scheduled": "a transform activity writes the files again without the removed content"}),
                _ => json!({"skipped": kind}),
            };
            tx.exec("update agent_task set state = 'done', result = $2 where id = $1 and lease_owner = $3", &[&id, &result, &self.name]).await?;
            done += 1;
        }
        let events: Vec<Value> = std::mem::take(&mut *self.pending.lock());
        tx.emit_many(events).await?;
        for (res, versions) in touched {
            refresh_for_versions(&mut tx, &versions, res).await?;
        }
        tx.commit().await?;
        Ok(done)
    }

    /// Verify a replica: compare the checksum of the bytes with the checksum of the version. A
    /// replica becomes verified only after this comparison succeeds (INV-8).
    async fn verify(&self, tx: &mut Tx<'_>, p: &Value, touched: &mut BTreeMap<Uuid, BTreeSet<Uuid>>) -> Result<Value> {
        let rid: Uuid = p["replica"].as_str().and_then(|s| s.parse().ok()).ok_or_else(|| Error::invalid("no replica"))?;
        let Some(r) = tx.qopt("select r.url, v.content_hash, r.version_id, r.resource_id from replica r join version v on v.id = r.version_id and v.tenant_id = r.tenant_id where r.id = $1", &[&rid]).await? else {
            return Ok(json!({"skipped": "the replica no longer exists"}));
        };
        let url: String = r.get(0);
        let expected: Option<String> = r.get(1);
        let version: Uuid = r.get(2);
        let resource: Uuid = r.get(3);
        let (b, k) = parse_url(&url)?;
        let (state, observed) = match tx.inst().store.read_local(&self.name, &b, &k) {
            Ok(data) => {
                tx.inst().metrics.bytes_read_for_catalog.fetch_add(data.len() as u64, Ordering::SeqCst);
                let h = sha256_hex(&data);
                (if Some(&h) == expected.as_ref() { "verified" } else { "suspect" }, Some(h))
            }
            Err(_) => ("lost", None),
        };
        let ok = state == "verified";
        tx.exec(
            "insert into verification (replica_id, at, algorithm, expected, observed, ok, agent) values ($1,$2,'sha256',$3,$4,$5,$6) on conflict do nothing",
            &[&rid, &tx.now, &expected.clone().unwrap_or_default(), &observed, &ok, &self.name],
        )
        .await?;
        let commit = tx.commit_id().await?;
        tx.exec(
            "with u as (update replica set observed_checksum = $2, verified_at = $3, state = $4 where id = $1)
             insert into replica_history (replica_id, commit_id, state) values ($1, $5, $4) on conflict (tenant_id, replica_id, commit_id) do update set state = excluded.state",
            &[&rid, &observed, &tx.now, &state, &commit],
        )
        .await?;
        let rev: i32 = tx
            .q1("insert into entity (id, kind, revision, created_commit) values ($1, 'replica', 1, $2) on conflict (tenant_id, id) do update set revision = entity.revision + 1 returning revision", &[&rid, &commit])
            .await?
            .get(0);
        self.pending.lock().push(tx.event_json(commit, &format!("replica.{state}"), rid, "replica", rev, None, false, json!({"version": version, "expected": expected, "observed": observed})));
        touched.entry(resource).or_default().insert(version);
        Ok(json!({"state": state}))
    }

    /// Delete the bytes of a replica and write a deletion receipt. On storage that keeps old
    /// object versions, a purge deletes them too.
    async fn delete(&self, tx: &mut Tx<'_>, p: &Value, touched: &mut BTreeMap<Uuid, BTreeSet<Uuid>>) -> Result<Value> {
        let rid: Uuid = p["replica"].as_str().and_then(|s| s.parse().ok()).ok_or_else(|| Error::invalid("no replica"))?;
        let vid: Uuid = p["version"].as_str().and_then(|s| s.parse().ok()).unwrap_or_default();
        let url = p["url"].as_str().unwrap_or_default().to_string();
        let res = namespace::resource(tx, &self.resource).await?;
        let (b, k) = parse_url(&url)?;
        let all = p["all_versions"].as_bool().unwrap_or(false);
        let n = tx.inst().store.delete(&b, &k, all || !res.versioned)?;
        let commit = tx.commit_id().await?;
        let receipt = new_id();
        let reason = p["reason"].as_str().map(|s| s.to_string());
        tx.exec(
            "insert into deletion_receipt (id, replica_id, version_id, resource_id, url, object_versions, at, agent, reason, commit_id) values ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)",
            &[&receipt, &rid, &vid, &res.id, &url, &(n as i32), &tx.now, &self.name, &reason, &commit],
        )
        .await?;
        if tx.qopt("select 1 from replica where id = $1", &[&rid]).await?.is_some() {
            namespace::set_replica_state(tx, rid, "retired", json!({"receipt": receipt})).await?;
            touched.entry(res.id).or_default().insert(vid);
        }
        tx.emit("replica.purged", rid, "replica", None, json!({"receipt": receipt, "version": vid, "url": url, "object_versions": n, "agent": self.name})).await?;
        Ok(json!({"receipt": receipt, "object_versions": n}))
    }
}

/// An inventory report from a site agent: register files that a mounted collection describes,
/// report dark data and lost replicas (PL-6), and differences in Unix groups.
pub async fn report_inventory(sess: &Session, resource: &str, agent: &str) -> Result<Value> {
    let mut c = sess.client().await?;
    let mut tx = sess.begin(&mut c).await?;
    let res = namespace::resource(&tx, resource).await?;
    let objects = tx.inst().store.list(&res.bucket)?;
    let known: BTreeMap<String, (Uuid, String, Uuid)> = tx
        .q("select url, id, state, version_id from replica where resource_id = $1", &[&res.id])
        .await?
        .iter()
        .map(|r| (r.get::<_, String>(0), (r.get::<_, Uuid>(1), r.get::<_, String>(2), r.get::<_, Uuid>(3))))
        .collect();
    let mounts: Vec<(String, String, Uuid)> = tx
        .q("select path, prefix, id from collection where resource_id = $1 order by length(prefix) desc", &[&res.id])
        .await?
        .iter()
        .map(|r| (r.get::<_, String>(0), r.get::<_, Option<String>>(1).unwrap_or_default(), r.get::<_, Uuid>(2)))
        .collect();
    let mut registered = 0;
    let mut dark = Vec::new();
    let present: BTreeSet<String> = objects.iter().map(|(k, _)| res.url(k)).collect();
    let mut batch = Vec::new();
    for (key, size) in &objects {
        let url = res.url(key);
        if known.contains_key(&url) {
            continue;
        }
        match mounts.iter().find(|(_, p, _)| !p.is_empty() && key.starts_with(p.as_str())) {
            Some((path, prefix, _)) => {
                let data = tx.inst().store.read_local(agent, &res.bucket, key)?;
                tx.inst().metrics.bytes_read_for_catalog.fetch_add(data.len() as u64, Ordering::SeqCst);
                let (structure, elements, stats) = crate::plugins::extract(key, &data);
                let rel = &key[prefix.len()..];
                let src: Option<String> = tx
                    .qopt("select payload->>'source' from metadata_payload where entity_id = (select id from collection where path = $1) and payload ? 'source'", &[path])
                    .await?
                    .and_then(|r| r.get(0));
                batch.push(Register {
                    path: format!("{}/{}", path.trim_end_matches('/'), rel),
                    content_hash: Some(sha256_hex(&data)),
                    size: *size as i64,
                    structure: Some(structure),
                    elements,
                    stats: Some(stats),
                    source: src,
                    replica: Some(namespace::ReplicaSpec { resource: res.name.clone(), key: Some(key.clone()), ..Default::default() }),
                    ..Default::default()
                });
                registered += 1;
            }
            None => dark.push(json!({"url": url, "size": size})),
        }
    }
    for b in batch.chunks(1000) {
        namespace::register_batch(&mut tx, b).await?;
    }
    let mut lost = Vec::new();
    for (url, (rid, state, vid)) in &known {
        if !present.contains(url) && !["lost", "retired"].contains(&state.as_str()) {
            namespace::set_replica_state(&mut tx, *rid, "lost", json!({"reason": "the inventory has no file", "version": vid})).await?;
            lost.push(json!({"replica": rid, "url": url, "version": vid}));
            refresh_for_version(&mut tx, *vid, res.id).await?;
        }
    }
    // POSIX resources: the Unix group of each file against the access control of its collection
    let mut mismatches = Vec::new();
    if res.protocol == "posix" {
        for (key, _) in &objects {
            let Some(group) = tx.inst().store.group(&res.bucket, key) else { continue };
            let url = res.url(key);
            let Some((_, _, vid)) = known.get(&url) else { continue };
            let v = namespace::version(&tx, *vid).await?;
            let readers: Vec<String> = tx
                .q("select subject_id from relation where object_type = 'collection' and object_id = $1 and subject_type = 'group'", &[&v.acl_root.to_string()])
                .await?
                .iter()
                .map(|r| r.get(0))
                .collect();
            if !readers.contains(&group) {
                mismatches.push(json!({"url": url, "unix_group": group, "acl_groups": readers}));
            }
        }
    }
    tx.commit().await?;
    Ok(json!({"resource": resource, "objects": objects.len(), "registered": registered, "dark_data": dark, "lost": lost, "group_mismatches": mismatches}))
}

// ---------------------------------------------------------------------------- read connector

#[derive(Clone, Debug, Default, serde::Serialize)]
pub struct ReadReport {
    pub tasks: usize,
    pub bytes: u64,
    pub failures: Vec<String>,
    pub observed_activity: Option<Uuid>,
}

/// The read connector of a compute engine (Ray Data in the design). It reads each task of a plan
/// directly from storage with the issued credentials, and reports the reads under the run token,
/// so that mudcat records observed usage (RD-1, RD-8, PR-4).
pub async fn read_plan(sess: &Session, client: &str, res: &crate::resolver::Resolution) -> Result<ReadReport> {
    let mut rep = ReadReport::default();
    let now = sess.now().timestamp();
    let mut seen = BTreeSet::new();
    for t in &res.tasks {
        rep.tasks += 1;
        if !seen.insert(t.url.clone()) {
            continue; // several selectors of one object read one object in this simulation
        }
        let Some(cred) = crate::resolver::credential_for(res, &t.url) else {
            rep.failures.push(format!("no credential for {}", t.url));
            continue;
        };
        match sess.inst.store.get(client, &t.url, cred, now) {
            Ok(d) => rep.bytes += d.len() as u64,
            Err(e) => rep.failures.push(format!("{}: {e}", t.url)),
        }
    }
    let mut c = sess.client().await?;
    let mut tx = sess.begin(&mut c).await?;
    rep.observed_activity = Some(crate::provenance::report_reads(&mut tx, &res.run_token, rep.tasks).await?);
    tx.commit().await?;
    Ok(rep)
}
