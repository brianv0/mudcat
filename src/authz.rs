//! The authorization port and its built-in adapter (architecture.md, "Authorization port").
//!
//! Access control is a set of Zanzibar-style relationship tuples. The built-in adapter keeps them
//! in the catalog database and evaluates them there (AC-2). The catalog adds conditions that are
//! different for each object, such as an embargo date, as a predicate over the metadata of the
//! object (model.md, "The catalog evaluates per-object conditions").
//!
//! Neutral schema (compiled to SpiceDB and OpenFGA by [`neutral_schema`]):
//!
//! ```text
//! collection: owner, writer, reader, acl_root, parent
//!   read  = acl_root->reader + acl_root->writer + acl_root->owner + tenant->admin, intersected with the compartments of the object
//!   write = acl_root->writer + acl_root->owner + tenant->admin
//! artifact:   reader, excluded;  read = (reader + collection->read) - excluded
//! dataset:    owner, writer, reader;  read = reader + writer + owner + collection->read
//! snapshot:   reader;  read = reader + dataset->read
//! group:      member (direct, group#member, or from a membership source with an age limit)
//! compartment: member
//! ```

use crate::error::{Error, Result};
use crate::instance::Tx;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::{BTreeSet, HashMap};
use uuid::Uuid;

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Decision {
    pub allow: bool,
    pub reason: String,
    /// The tuple that gave the access.
    pub via: Option<String>,
    /// The age in seconds of synchronised memberships that the decision used.
    pub membership_age_secs: Option<i64>,
    /// Memberships that mudcat did not use because they were older than their limit.
    pub stale_memberships: Vec<Value>,
    pub cached: bool,
}

/// The subjects that a principal is: itself, its groups, and the public wildcards.
#[derive(Clone, Debug, Default)]
pub struct Subjects {
    pub keys: Vec<String>,
    pub ages: HashMap<String, i64>,
    pub stale: Vec<Value>,
}

fn key(t: &str, id: &str, rel: &str) -> String {
    format!("{t}:{id}#{rel}")
}

/// Parse a subject such as `user:alice`, `group:team#member` or `user:*`.
pub fn parse_subject(s: &str) -> Result<(String, String, String)> {
    let (t, rest) = s.split_once(':').ok_or_else(|| Error::invalid(format!("not a subject: {s}")))?;
    let (id, rel) = match rest.split_once('#') {
        Some((id, rel)) => (id, rel),
        None => (rest, ""),
    };
    Ok((t.into(), id.into(), rel.into()))
}

fn unreachable(tx: &Tx<'_>) -> Result<()> {
    if tx.inst().authz_unreachable.load(std::sync::atomic::Ordering::SeqCst) {
        // fail closed (AC-6)
        return Err(Error::refused("authorization-unavailable", "the authorization service gives no decision; mudcat denies the request", Value::Null));
    }
    Ok(())
}

pub async fn subjects(tx: &Tx<'_>) -> Result<Subjects> {
    let p = tx.principal();
    let mut s = Subjects { keys: vec![key("user", "*", "")], ..Default::default() };
    if p.is_anonymous() {
        return Ok(s);
    }
    s.keys.push(key("authenticated", "*", ""));
    let (t, id) = p.subject();
    let mut frontier = vec![(t.clone(), id.clone(), String::new())];
    s.keys.push(key(&t, &id, ""));
    let mut seen: BTreeSet<String> = s.keys.iter().cloned().collect();
    for _depth in 0..8 {
        if frontier.is_empty() {
            break;
        }
        let ft: Vec<String> = frontier.iter().map(|f| f.0.clone()).collect();
        let fi: Vec<String> = frontier.iter().map(|f| f.1.clone()).collect();
        let fr: Vec<String> = frontier.iter().map(|f| f.2.clone()).collect();
        let rows = tx
            .q(
                "select r.object_id, m.id, m.last_sync_at, m.max_age_secs, m.provider, m.org, m.team
                   from relation r
                   join unnest($1::text[], $2::text[], $3::text[]) as f(t, i, rl) on r.subject_type = f.t and r.subject_id = f.i and r.subject_relation = f.rl
                   left join membership_source m on m.id = r.membership_source and m.tenant_id = r.tenant_id
                  where r.object_type = 'group' and r.relation = 'member'",
                &[&ft, &fi, &fr],
            )
            .await?;
        frontier.clear();
        for r in rows {
            let group: String = r.get(0);
            let src: Option<Uuid> = r.get(1);
            if src.is_some() {
                let last: Option<chrono::DateTime<chrono::Utc>> = r.get(2);
                let max_age: i64 = r.get(3);
                let age = last.map(|l| (tx.now - l).num_seconds()).unwrap_or(i64::MAX);
                if age > max_age {
                    s.stale.push(json!({"group": group, "age_secs": age, "limit_secs": max_age, "source": format!("{}:{}/{}", r.get::<_, String>(4), r.get::<_, String>(5), r.get::<_, String>(6))}));
                    continue;
                }
                s.ages.insert(group.clone(), age);
            }
            let k = key("group", &group, "member");
            if seen.insert(k.clone()) {
                s.keys.push(k);
                frontier.push(("group".into(), group, "member".into()));
            }
        }
    }
    Ok(s)
}

/// The candidate grants of a permission on an object: (object type, object id, relations).
async fn candidates(tx: &Tx<'_>, permission: &str, object_type: &str, id: Uuid) -> Result<Vec<(String, String, Vec<&'static str>)>> {
    let read_rels = vec!["reader", "writer", "owner"];
    let write_rels = vec!["writer", "owner"];
    let rels = |p: &str| if p == "write" { write_rels.clone() } else { read_rels.clone() };
    let mut out = vec![("tenant".to_string(), "self".to_string(), vec!["admin"])];
    match object_type {
        "collection" => {
            let r = tx.qopt("select acl_root_id from collection where id = $1", &[&id]).await?.ok_or(Error::NotFound)?;
            let root: Uuid = r.get(0);
            out.push(("collection".into(), root.to_string(), rels(permission)));
        }
        "artifact" | "version" | "model" => {
            let aid: Uuid = if object_type == "version" {
                tx.qopt("select artifact_id from version where id = $1", &[&id]).await?.ok_or(Error::NotFound)?.get(0)
            } else {
                id
            };
            let r = tx
                .qopt("select c.acl_root_id from artifact a join collection c on c.id = a.collection_id and c.tenant_id = a.tenant_id where a.id = $1", &[&aid])
                .await?
                .ok_or(Error::NotFound)?;
            let root: Uuid = r.get(0);
            if permission == "read" {
                out.push(("artifact".into(), aid.to_string(), vec!["reader"]));
            }
            out.push(("collection".into(), root.to_string(), rels(permission)));
        }
        "dataset" => {
            let r = tx.qopt("select collection_id from dataset where id = $1", &[&id]).await?.ok_or(Error::NotFound)?;
            out.push(("dataset".into(), id.to_string(), rels(permission)));
            if let Some(c) = r.get::<_, Option<Uuid>>(0) {
                let root: Uuid = tx.q1("select acl_root_id from collection where id = $1", &[&c]).await?.get(0);
                out.push(("collection".into(), root.to_string(), rels(permission)));
            }
        }
        "snapshot" => {
            let r = tx.qopt("select dataset_id from snapshot where id = $1", &[&id]).await?.ok_or(Error::NotFound)?;
            if permission == "read" {
                out.push(("snapshot".into(), id.to_string(), vec!["reader"]));
            }
            let d: Uuid = r.get(0);
            out.extend(Box::pin(candidates(tx, permission, "dataset", d)).await?);
        }
        other => out.push((other.to_string(), id.to_string(), rels(permission))),
    }
    Ok(out)
}

/// The compartments whose membership an object needs (intersection), from the tags of the object
/// and from legal holds on invalidated content (the retention compartment).
pub async fn compartments_of(tx: &Tx<'_>, object_type: &str, id: Uuid) -> Result<Vec<String>> {
    let mut entities = vec![id];
    if object_type == "version" {
        if let Some(r) = tx.qopt("select artifact_id from version where id = $1", &[&id]).await? {
            entities.push(r.get(0));
        }
    }
    let tags = crate::metadata::effective_tags_many(tx, &entities).await?;
    let mut out = BTreeSet::new();
    for (name, value) in tags {
        let rows = tx.q("select config from tag_policy where name = $1 and value = $2 and kind = 'compartment'", &[&name, &value]).await?;
        for r in rows {
            let cfg: Value = r.get(0);
            if let Some(c) = cfg["compartment"].as_str() {
                out.insert(c.to_string());
            }
        }
    }
    // a legal hold moves held content that is invalidated into its retention compartment
    let rows = tx
        .q(
            "select distinct h.id from hold_scope s join legal_hold h on h.id = s.hold_id and h.tenant_id = s.tenant_id
              where s.entity_id = any($1) and h.released_at is null",
            &[&entities],
        )
        .await?;
    if !rows.is_empty() && object_type == "version" && crate::policy::is_invalidated(tx, id).await? {
        for r in rows {
            out.insert(format!("retention-{}", r.get::<_, Uuid>(0)));
        }
    }
    Ok(out.into_iter().collect())
}

/// Evaluate a condition of a tuple on its object, in the catalog.
async fn condition_holds(tx: &Tx<'_>, condition: &str, object_type: &str, object_id: &str) -> Result<bool> {
    match condition {
        "embargo" => {
            let Ok(id) = object_id.parse::<Uuid>() else { return Ok(false) };
            let until = embargo_until(tx, object_type, id).await?;
            Ok(until.map(|u| u <= tx.now).unwrap_or(true))
        }
        _ => Ok(false),
    }
}

/// The embargo date of an object: the value of an attribute whose definition makes it the
/// condition `embargo` for access.
pub async fn embargo_until(tx: &Tx<'_>, _object_type: &str, id: Uuid) -> Result<Option<chrono::DateTime<chrono::Utc>>> {
    let rows = tx
        .q(
            "select p.payload ->> d.name from metadata_payload p
               join attribute_def d on d.namespace = p.namespace and d.tenant_id = p.tenant_id and d.access_condition = 'embargo'
              where p.entity_id = $1 and p.status = 'visible' and p.payload ? d.name",
            &[&id],
        )
        .await?;
    let mut out = None;
    for r in rows {
        if let Some(s) = r.get::<_, Option<String>>(0) {
            if let Ok(t) = chrono::DateTime::parse_from_rfc3339(&s) {
                out = Some(t.with_timezone(&chrono::Utc));
            }
        }
    }
    Ok(out)
}

/// Check one permission of the principal of the transaction on an object. The built-in adapter
/// reads the tuples in the same database, so a revocation applies to each check that starts
/// after it (AC-7). It never uses a cached decision.
pub async fn check(tx: &Tx<'_>, permission: &str, object_type: &str, id: Uuid) -> Result<Decision> {
    unreachable(tx)?;
    if tx.principal().is_system() {
        return Ok(Decision { allow: true, reason: "system".into(), ..Default::default() });
    }
    let subj = subjects(tx).await?;
    let mut d = Decision { stale_memberships: subj.stale.clone(), ..Default::default() };
    let cands = candidates(tx, permission, object_type, id).await?;
    // an exclusion on an artifact removes access that its collection gives
    if matches!(object_type, "artifact" | "version" | "model") {
        let aid = &cands.iter().find(|c| c.0 == "artifact").map(|c| c.1.clone());
        if let Some(aid) = aid {
            let ex = tx
                .qopt(
                    "select 1 from relation where object_type = 'artifact' and object_id = $1 and relation = 'excluded'
                        and (subject_type || ':' || subject_id || '#' || subject_relation) = any($2)",
                    &[aid, &subj.keys],
                )
                .await?;
            if ex.is_some() {
                d.reason = "the artifact is withdrawn from the subject".into();
                return Ok(d);
            }
        }
    }
    let mut found = false;
    for (ot, oid, rels) in &cands {
        let rels: Vec<String> = rels.iter().map(|s| s.to_string()).collect();
        let rows = tx
            .q(
                "select relation, subject_type || ':' || subject_id || '#' || subject_relation, condition from relation
                  where object_type = $1 and object_id = $2 and relation = any($3)
                    and (subject_type || ':' || subject_id || '#' || subject_relation) = any($4)",
                &[ot, oid, &rels, &subj.keys],
            )
            .await?;
        for r in rows {
            let cond: Option<String> = r.get(2);
            if let Some(c) = &cond {
                if !condition_holds(tx, c, ot, oid).await? {
                    continue;
                }
            }
            let via: String = r.get(1);
            let rel: String = r.get(0);
            d.via = Some(format!("{ot}:{oid}#{rel}@{via}"));
            if let Some(g) = via.strip_prefix("group:").and_then(|g| g.strip_suffix("#member")) {
                d.membership_age_secs = subj.ages.get(g).copied();
            }
            found = true;
            break;
        }
        if found {
            break;
        }
    }
    if !found {
        d.reason = if !subj.stale.is_empty() {
            format!("no grant; synchronised membership is older than its limit: {}", Value::Array(subj.stale.clone()))
        } else {
            format!("no grant of {permission} on {object_type}:{id}")
        };
        return Ok(d);
    }
    // compartments are an intersection
    if permission == "read" {
        for c in compartments_of(tx, object_type, id).await? {
            let m = tx
                .qopt(
                    "select 1 from relation where object_type = 'compartment' and object_id = $1 and relation = 'member'
                        and (subject_type || ':' || subject_id || '#' || subject_relation) = any($2)",
                    &[&c, &subj.keys],
                )
                .await?;
            if m.is_none() {
                d.reason = format!("the object is in the compartment {c}, and the subject is not a member");
                d.via = None;
                return Ok(d);
            }
        }
    }
    d.allow = true;
    d.reason = "granted".into();
    Ok(d)
}

/// Check a permission at an enforcement point, write the decision to the audit log, and fail
/// with a refusal if it is a denial.
pub async fn require(tx: &Tx<'_>, point: &str, permission: &str, object_type: &str, id: Uuid) -> Result<Decision> {
    let d = match check(tx, permission, object_type, id).await {
        Ok(d) => d,
        Err(e) => {
            let msg = e.to_string();
            tx.sess.audit(point, permission, &format!("{object_type}:{id}"), false, Some(&msg), json!({})).await?;
            return Err(e);
        }
    };
    tx.sess
        .audit(point, permission, &format!("{object_type}:{id}"), d.allow, Some(&d.reason), serde_json::to_value(&d)?)
        .await?;
    if !d.allow {
        return Err(Error::refused("access-denied", format!("{} may not {permission} {object_type}:{id}: {}", tx.principal(), d.reason), serde_json::to_value(&d)?));
    }
    Ok(d)
}

/// The ACL roots that the principal can read (LookupResources), for the coarse filter of search
/// and of the change stream.
pub async fn readable_roots(tx: &Tx<'_>) -> Result<Option<Vec<Uuid>>> {
    unreachable(tx)?;
    if tx.principal().is_system() {
        return Ok(None);
    }
    let subj = subjects(tx).await?;
    let admin = tx
        .qopt(
            "select 1 from relation where object_type='tenant' and relation='admin' and (subject_type || ':' || subject_id || '#' || subject_relation) = any($1)",
            &[&subj.keys],
        )
        .await?;
    if admin.is_some() {
        return Ok(None);
    }
    let rows = tx
        .q(
            "select distinct object_id from relation where object_type = 'collection' and relation in ('reader','writer','owner')
               and (subject_type || ':' || subject_id || '#' || subject_relation) = any($1)",
            &[&subj.keys],
        )
        .await?;
    Ok(Some(rows.iter().filter_map(|r| r.get::<_, String>(0).parse().ok()).collect()))
}

/// Write a relationship tuple (a grant), idempotently, with history and an event.
pub async fn grant(tx: &mut Tx<'_>, object_type: &str, object_id: &str, relation: &str, subject: &str, condition: Option<&str>) -> Result<()> {
    let tenant = tx.sess.tenant_id()?;
    let (st, sid, srel) = parse_subject(subject)?;
    let commit = tx.commit_id().await?;
    tx.exec(
        "insert into relation (object_type, object_id, relation, subject_type, subject_id, subject_relation, condition, created_commit)
         values ($1,$2,$3,$4,$5,$6,$7,$8)
         on conflict (tenant_id, object_type, object_id, relation, subject_type, subject_id, subject_relation) do update set condition = excluded.condition",
        &[&object_type, &object_id, &relation, &st, &sid, &srel, &condition, &commit],
    )
    .await?;
    let tuple = format!("{object_type}:{object_id}#{relation}@{subject}");
    tx.exec("insert into relation_history (commit_id, op, tuple) values ($1, 'write', $2) on conflict do nothing", &[&commit, &tuple]).await?;
    let subject_entity = object_id.parse::<Uuid>().unwrap_or_else(|_| uuid_for(&format!("{}/{object_type}:{object_id}", tenant)));
    tx.emit("relationship.written", subject_entity, "relationship", None, json!({"tuple": tuple})).await?;
    Ok(())
}

/// Delete a relationship tuple (a revocation).
pub async fn revoke(tx: &mut Tx<'_>, object_type: &str, object_id: &str, relation: &str, subject: &str) -> Result<bool> {
    let (st, sid, srel) = parse_subject(subject)?;
    let n = tx
        .exec(
            "delete from relation where object_type=$1 and object_id=$2 and relation=$3 and subject_type=$4 and subject_id=$5 and subject_relation=$6",
            &[&object_type, &object_id, &relation, &st, &sid, &srel],
        )
        .await?;
    let commit = tx.commit_id().await?;
    let tuple = format!("{object_type}:{object_id}#{relation}@{subject}");
    tx.exec("insert into relation_history (commit_id, op, tuple) values ($1, 'delete', $2) on conflict do nothing", &[&commit, &tuple]).await?;
    let tenant = tx.sess.tenant_id()?;
    let subject_entity = object_id.parse::<Uuid>().unwrap_or_else(|_| uuid_for(&format!("{tenant}/{object_type}:{object_id}")));
    tx.emit("relationship.removed", subject_entity, "relationship", None, json!({"tuple": tuple})).await?;
    Ok(n > 0)
}

/// A relationship that follows from catalog structure; the catalog owns it.
pub async fn write_structural(tx: &mut Tx<'_>, ot: &str, oid: &str, rel: &str, st: &str, sid: &str) -> Result<()> {
    let commit = tx.commit_id().await?;
    tx.exec(
        "insert into relation (object_type, object_id, relation, subject_type, subject_id, created_commit) values ($1,$2,$3,$4,$5,$6) on conflict do nothing",
        &[&ot, &oid, &rel, &st, &sid, &commit],
    )
    .await?;
    Ok(())
}

/// A stable UUID for an object that has no UUID, such as a group name.
pub fn uuid_for(s: &str) -> Uuid {
    let h = crate::instance::sha256_hex(s.as_bytes());
    Uuid::from_slice(&hex::decode(&h[..32]).unwrap()).unwrap()
}

/// The neutral schema, compiled for SpiceDB and for OpenFGA (architecture.md, "A neutral schema").
pub fn neutral_schema(target: &str) -> String {
    match target {
        "spicedb" => r#"definition user {}
definition authenticated {}
definition tenant { relation admin: user }
definition group { relation member: user | group#member }
definition compartment { relation member: user | group#member }
definition collection {
  relation tenant: tenant
  relation acl_root: collection
  relation parent: collection
  relation owner: user | group#member
  relation writer: user | group#member
  relation reader: user | group#member | user:* | authenticated:*
  permission read = acl_root->reader + acl_root->writer + acl_root->owner + tenant->admin
  permission write = acl_root->writer + acl_root->owner + tenant->admin
}
definition artifact {
  relation collection: collection
  relation reader: user | group#member
  relation excluded: user | group#member
  permission read = (reader + collection->read) - excluded
}
definition dataset {
  relation collection: collection
  relation reader: user | group#member | user:* | authenticated:* with embargo
  relation writer: user | group#member
  relation owner: user | group#member
  permission read = reader + writer + owner + collection->read
}
definition snapshot {
  relation dataset: dataset
  relation reader: user | group#member | user:*
  permission read = reader + dataset->read
}
caveat embargo(embargo_until timestamp, now timestamp) { now >= embargo_until }
"#
        .into(),
        "openfga" => r#"model
  schema 1.1
type user
type authenticated
type tenant
  relations
    define admin: [user]
type group
  relations
    define member: [user, group#member]
type compartment
  relations
    define member: [user, group#member]
type collection
  relations
    define tenant: [tenant]
    define acl_root: [collection]
    define parent: [collection]
    define owner: [user, group#member]
    define writer: [user, group#member]
    define reader: [user, group#member, user:*, authenticated:*]
    define read: reader from acl_root or writer from acl_root or owner from acl_root or admin from tenant
    define write: writer from acl_root or owner from acl_root or admin from tenant
type artifact
  relations
    define collection: [collection]
    define reader: [user, group#member]
    define excluded: [user, group#member]
    define read: (reader or read from collection) but not excluded
type dataset
  relations
    define collection: [collection]
    define reader: [user, group#member, user:*, authenticated:* with embargo]
    define writer: [user, group#member]
    define owner: [user, group#member]
    define read: reader or writer or owner or read from collection
type snapshot
  relations
    define dataset: [dataset]
    define reader: [user, group#member, user:*]
    define read: reader or read from dataset
condition embargo(embargo_until: timestamp, now: timestamp) { now >= embargo_until }
"#
        .into(),
        _ => String::new(),
    }
}

// ---------------------------------------------------------------------------- membership sources

/// A group whose members come from a team in an outside service (use case 5).
pub async fn create_membership_source(tx: &mut Tx<'_>, group: &str, provider: &str, org: &str, team: &str, max_age_secs: i64) -> Result<Uuid> {
    let id = crate::instance::new_id();
    tx.exec(
        "insert into membership_source (id, group_id, provider, org, team, max_age_secs) values ($1,$2,$3,$4,$5,$6)",
        &[&id, &group, &provider, &org, &team, &max_age_secs],
    )
    .await?;
    tx.emit("membership-source.created", id, "membership-source", None, json!({"group": group, "team": format!("{provider}:{org}/{team}")})).await?;
    Ok(id)
}

/// Synchronise the members of a group from its membership source. If the provider cannot be
/// reached, nothing changes, and the membership ages.
pub async fn sync_membership(tx: &mut Tx<'_>, source: Uuid) -> Result<Vec<String>> {
    let r = tx.qopt("select group_id, provider, org, team from membership_source where id = $1", &[&source]).await?.ok_or(Error::NotFound)?;
    let (group, provider, org, team): (String, String, String, String) = (r.get(0), r.get(1), r.get(2), r.get(3));
    if tx.inst().idp.down.read().contains(&provider) {
        return Err(Error::refused("provider-unreachable", format!("{provider} cannot be reached; membership of {group} is not synchronised"), Value::Null));
    }
    let members: Vec<String> = tx
        .inst()
        .idp
        .teams
        .read()
        .get(&provider)
        .and_then(|o| o.get(&org))
        .and_then(|t| t.get(&team))
        .cloned()
        .unwrap_or_default();
    // external accounts map to mudcat principals through linked identities
    let mut principals = Vec::new();
    for m in &members {
        if let Some(r) = tx.qopt("select principal from linked_identity where provider = $1 and external_id = $2", &[&provider, m]).await? {
            principals.push(r.get::<_, String>(0));
        }
    }
    let commit = tx.commit_id().await?;
    let current: Vec<String> = tx
        .q(
            "select subject_type || ':' || subject_id from relation where object_type='group' and object_id=$1 and relation='member' and membership_source=$2",
            &[&group, &source],
        )
        .await?
        .iter()
        .map(|r| r.get(0))
        .collect();
    for p in &current {
        if !principals.contains(p) {
            revoke(tx, "group", &group, "member", p).await?;
        }
    }
    for p in &principals {
        let (st, sid, _) = parse_subject(p)?;
        tx.exec(
            "insert into relation (object_type, object_id, relation, subject_type, subject_id, subject_relation, membership_source, synced_at, created_commit)
             values ('group', $1, 'member', $2, $3, '', $4, $5, $6)
             on conflict (tenant_id, object_type, object_id, relation, subject_type, subject_id, subject_relation) do update set synced_at = excluded.synced_at, membership_source = excluded.membership_source",
            &[&group, &st, &sid, &source, &tx.now, &commit],
        )
        .await?;
        if !current.contains(p) {
            let tuple = format!("group:{group}#member@{p}");
            let tenant = tx.sess.tenant_id()?;
            tx.emit("relationship.written", uuid_for(&format!("{tenant}/group:{group}")), "relationship", None, json!({"tuple": tuple})).await?;
        }
    }
    tx.exec("update membership_source set last_sync_at = $2 where id = $1", &[&source, &tx.now]).await?;
    Ok(principals)
}

pub async fn link_identity(tx: &mut Tx<'_>, provider: &str, external: &str, principal: &str) -> Result<()> {
    tx.exec(
        "insert into linked_identity (provider, external_id, principal) values ($1,$2,$3) on conflict (tenant_id, provider, external_id) do update set principal = excluded.principal",
        &[&provider, &external, &principal],
    )
    .await?;
    Ok(())
}

/// The groups of which a principal is a member now (direct and synchronised).
pub async fn groups_of(tx: &Tx<'_>) -> Result<Vec<String>> {
    let s = subjects(tx).await?;
    Ok(s.keys.iter().filter_map(|k| k.strip_prefix("group:").and_then(|g| g.strip_suffix("#member")).map(|g| g.to_string())).collect())
}
