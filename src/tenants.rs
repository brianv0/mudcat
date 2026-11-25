//! Tenants: creation, and the move of a tenant to another database (hosted.md, "Tenancy").

use crate::error::{Error, Result};
use crate::instance::{apply_schema, Instance, Principal, TenantInfo};
use futures::{SinkExt, StreamExt};
use serde_json::{json, Value};
use std::sync::Arc;
use uuid::Uuid;

/// The tenant tables, in an order that respects their foreign keys.
pub const TENANT_TABLES: &[&str] = &[
    "commit_log", "entity", "erasure_record", "identifier", "storage_resource", "collection", "artifact", "version", "replica",
    "replica_history", "verification", "deletion_receipt", "metadata_namespace", "attribute_def", "metadata_payload", "metadata_history",
    "attr_index", "tag", "tag_policy", "enricher", "dataset", "snapshot", "manifest", "manifest_entry", "manifest_ref", "ref", "ref_history",
    "annotation_set", "representation", "selection", "equivalence", "source", "terms_document", "usage_grant", "duty", "policy_cache",
    "review_item", "invalidation", "legal_hold", "hold_scope", "model_flag", "relation", "relation_history", "membership_source",
    "linked_identity", "trust_rule", "audit", "activity", "usage", "generation", "derivation", "resolution", "credential_log",
    "similarity_criterion", "leakage_assertion", "identity_index", "replication_rule", "transfer", "completeness", "agent_task", "outbox",
    "event", "stream_state", "subscription", "search_doc", "search_word", "index_state",
];

/// Create a tenant. Its ID comes from a cryptographic random generator: a UUIDv4 has 122 random
/// bits (ID-11). The tenant gets its own endpoint.
pub async fn create_tenant(inst: &Arc<Instance>, name: &str, admin: &str) -> Result<TenantInfo> {
    let id = Uuid::new_v4();
    let endpoint = format!("{name}.{}", inst.config.endpoint_domain);
    {
        let c = inst.shared.get().await?;
        c.execute(
            "insert into tenant (id, name, endpoint, status, location, created_at) values ($1,$2,$3,'active','shared',$4)",
            &[&id, &name, &endpoint, &inst.now()],
        )
        .await?;
    }
    let info = TenantInfo { id, name: name.into(), endpoint, status: "active".into(), location: "shared".into() };
    inst.tenants.write().insert(id, info.clone());
    let sess = inst.session(id, Principal::system());
    let mut c = sess.client().await?;
    let mut tx = sess.begin(&mut c).await?;
    tx.exec("insert into stream_state (last_position, floor) values (0, 0) on conflict do nothing", &[]).await?;
    tx.exec("insert into index_state (name, active_version, position) values ('search', 1, 0) on conflict do nothing", &[]).await?;
    let root = crate::namespace::ensure_collection(&mut tx, "/").await?;
    crate::authz::grant(&mut tx, "tenant", "self", "admin", admin, None).await?;
    crate::authz::grant(&mut tx, "collection", &root.id.to_string(), "owner", admin, None).await?;
    tx.emit("tenant.created", id, "tenant", None, json!({"name": name})).await?;
    tx.commit().await?;
    Ok(info)
}

async fn set_status(inst: &Arc<Instance>, tenant: Uuid, status: &str, location: Option<&str>) -> Result<()> {
    let c = inst.shared.get().await?;
    match location {
        Some(l) => c.execute("update tenant set status = $2, location = $3 where id = $1", &[&tenant, &status, &l]).await?,
        None => c.execute("update tenant set status = $2 where id = $1", &[&tenant, &status]).await?,
    };
    if let Some(t) = inst.tenants.write().get_mut(&tenant) {
        t.status = status.into();
        if let Some(l) = location {
            t.location = l.into();
        }
    }
    Ok(())
}

/// The limits of a tenant move.
#[derive(Clone, Copy, Debug)]
pub struct MoveOptions {
    /// How long the mover waits for the write transactions that had passed the fence. After this
    /// time the move fails, and the tenant continues where it was.
    pub fence_wait: std::time::Duration,
    /// How long the mover waits, after the switch, for the transactions that began before it and
    /// can still read the old database. After this time the rows stay there, and the report of
    /// the move says so ([`remove_left_rows`] removes them later).
    pub drain_wait: std::time::Duration,
}

impl Default for MoveOptions {
    fn default() -> Self {
        MoveOptions { fence_wait: std::time::Duration::from_secs(60), drain_wait: std::time::Duration::from_secs(600) }
    }
}

/// Move a tenant to another database. The records keep their keys, because the tenant ID is
/// first in every key (OP-14). During the copy mudcat accepts no writes for the tenant, and reads
/// continue. If the copy fails, the target database shows no part of the tenant, and the tenant
/// continues where it was.
///
/// More than one process can serve the tenant, and each has a registry of its own, so no step
/// here relies on a registry. Three things in the database carry the move:
///
/// - **The fence**, an advisory lock of the tenant in its database. A writer holds it, shared,
///   to the end of its transaction and does not wait for it; the mover takes it exclusively and
///   so waits for the writers that had passed.
/// - **The home marker**, the row of the tenant in the table `tenant` of the database where its
///   rows are. Each transaction reads it when it begins, and each writer reads it again when it
///   has the fence. The mover changes it before it opens the fence, so a session that begins in
///   the old database after the switch is refused, and its process takes the registry entry again.
/// - **The drain.** Transactions that began before the switch can still read the old database.
///   The mover removes the rows there only when those have ended.
pub async fn move_tenant(inst: &Arc<Instance>, tenant: Uuid, to_db: &str) -> Result<Value> {
    move_tenant_with(inst, tenant, to_db, MoveOptions::default()).await
}

pub async fn move_tenant_with(inst: &Arc<Instance>, tenant: Uuid, to_db: &str, options: MoveOptions) -> Result<Value> {
    // the registry of this process is a cache too: the mover reads the entry again
    let info = inst.reload_tenant(tenant).await?.ok_or(Error::NotFound)?;
    let from = info.location.clone();
    if from == to_db {
        return Err(Error::invalid("the tenant is already in that database"));
    }
    if to_db != "shared" {
        let target_url = crate::instance::db_url(&inst.base_url, to_db);
        crate::instance::Instance::ensure_db(&target_url).await?;
    }
    let target = inst.pool_for(to_db)?;
    apply_schema(&target).await?;
    let source = inst.pool_for(&from)?;
    // One mover for a tenant: the test of the status and its change are one statement.
    let claimed = inst.shared.get().await?.execute("update tenant set status = 'moving' where id = $1 and status = 'active' and location = $2", &[&tenant, &from]).await?;
    inst.reload_tenant(tenant).await?;
    if claimed != 1 {
        return Err(Error::Retryable("the tenant is moving already, or it is not in that database any more".into()));
    }
    let key = tenant.to_string();
    let fence = source.get().await?;
    let mut have_fence = false;

    // 1. The copy, under the fence.
    let copied: Result<Value> = async {
        if from != "shared" {
            set_marker(&source, tenant, "moving").await?;
        }
        // wait for the write transactions of the tenant that had passed the fence, for a limited time
        fence.batch_execute(&format!("set lock_timeout = {}", options.fence_wait.as_millis().max(1))).await?;
        let locked = fence.execute("select pg_advisory_lock(hashtextextended($1, 11))", &[&key]).await;
        fence.batch_execute("reset lock_timeout").await?;
        if let Err(e) = locked {
            return Err(Error::Retryable(format!("a write transaction of the tenant stayed open for {:?}; the tenant did not move ({e})", options.fence_wait)));
        }
        have_fence = true;
        let pause = inst.move_pause.read().clone();
        if let Some(p) = pause {
            p.stop_here().await;
        }
        let fault = *inst.move_fault_at.read();
        copy_tenant(inst, tenant, &source, &target, to_db, fault).await
    }
    .await;

    // 2. The switch, still under the fence: the registry entry names the new database.
    let switched: Result<Value> = match copied {
        Ok(stats) => match set_status(inst, tenant, "active", Some(to_db)).await {
            Ok(()) => Ok(stats),
            Err(e) => {
                // The copy is committed and no registry entry names it. It goes, so that the
                // target shows no part of the tenant.
                let _ = delete_rows(&target, tenant, to_db != "shared").await;
                Err(e)
            }
        },
        Err(e) => Err(e),
    };
    let mut marked = true;
    let mut switch_time = None;
    match &switched {
        Ok(_) => {
            // The marker of a dedicated source, also under the fence. From here a transaction
            // that begins in the old database is refused, in every process.
            if from != "shared" {
                marked = set_marker(&source, tenant, "moved").await.is_ok();
            }
            switch_time = fence.query_one("select clock_timestamp()", &[]).await.ok().map(|r| r.get::<_, chrono::DateTime<chrono::Utc>>(0));
        }
        Err(_) => {
            // the tenant continues where it was
            if from != "shared" {
                let _ = set_marker(&source, tenant, "active").await;
            }
            if let Ok(c) = inst.shared.get().await {
                let _ = c.execute("update tenant set status = 'active' where id = $1 and status = 'moving' and location = $2", &[&tenant, &from]).await;
            }
            let _ = inst.reload_tenant(tenant).await;
        }
    }
    // The fence opens on every path. A connection that cannot release the lock leaves the pool,
    // and the lock ends with its session.
    if have_fence && fence.execute("select pg_advisory_unlock(hashtextextended($1, 11))", &[&key]).await.is_err() {
        drop(deadpool_postgres::Object::take(fence));
    }
    let stats = switched?;

    // 3. The rows in the old database. The tenant has moved; a failure here does not undo that.
    let cleanup = if !marked {
        format!("pending: the old database {from} could not be marked; the rows stay there")
    } else {
        match remove_from_source(inst, tenant, &from, switch_time, options.drain_wait).await {
            Ok(s) => s,
            Err(e) => format!("pending: {e}"),
        }
    };
    Ok(json!({"tenant": tenant, "from": from, "to": to_db, "rows": stats, "source_cleanup": cleanup}))
}

/// The home marker of a tenant in a dedicated database (in the shared database the marker is the
/// registry entry, which `set_status` writes).
async fn set_marker(db: &deadpool_postgres::Pool, tenant: Uuid, status: &str) -> Result<()> {
    db.get().await?.execute("update tenant set status = $2 where id = $1", &[&tenant, &status]).await?;
    Ok(())
}

/// Remove the rows that a tenant left in a database after a move, when the report of the move
/// said that they stayed. It refuses if the tenant is in that database.
pub async fn remove_left_rows(inst: &Arc<Instance>, tenant: Uuid, from_db: &str, wait: std::time::Duration) -> Result<String> {
    let info = inst.reload_tenant(tenant).await?.ok_or(Error::NotFound)?;
    if info.location == from_db || info.status != "active" {
        return Err(Error::invalid(format!("the tenant is in {} with the status {}; its rows there stay", info.location, info.status)));
    }
    remove_from_source(inst, tenant, from_db, None, wait).await
}

/// Remove the rows of a tenant from the database that it left. Transactions that began before
/// the switch can still read them (reads continue during a move), so the rows go only when no
/// transaction in that database is older than `switched` (or than now, if it is not given).
/// Returns `done`, or the reason why the rows stay.
async fn remove_from_source(inst: &Arc<Instance>, tenant: Uuid, from: &str, switched: Option<chrono::DateTime<chrono::Utc>>, wait: std::time::Duration) -> Result<String> {
    let source = inst.pool_for(from)?;
    if from != "shared" {
        // no new transaction of the tenant begins here (the move did this already; a later call repeats it)
        set_marker(&source, tenant, "moved").await?;
    }
    let c = source.get().await?;
    let switched: chrono::DateTime<chrono::Utc> = match switched {
        Some(t) => t,
        None => c.query_one("select clock_timestamp()", &[]).await?.get(0),
    };
    let started = std::time::Instant::now();
    loop {
        // The copy of another move is long and reads no tenant but its own: it does not count.
        let old: i64 = c
            .query_one(
                "select count(*) from pg_stat_activity where datname = current_database() and pid <> pg_backend_pid() and backend_type = 'client backend' \
                 and xact_start is not null and xact_start <= $1 and application_name <> 'mudcat-move'",
                &[&switched],
            )
            .await?
            .get(0);
        if old == 0 || crate::mutant("cleanup-no-drain") {
            break;
        }
        if started.elapsed() >= wait {
            return Ok(format!("pending: {old} transactions that began before the move were still open after {wait:?}; the rows stay in {from}"));
        }
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }
    drop(c);
    delete_rows(&source, tenant, from != "shared").await?;
    Ok("done".into())
}

/// Delete the rows of a tenant in one database, and its home marker if the database is a
/// dedicated one.
async fn delete_rows(db: &deadpool_postgres::Pool, tenant: Uuid, with_marker: bool) -> Result<()> {
    let mut c = db.get().await?;
    let tx = c.transaction().await?;
    tx.batch_execute("set local application_name = 'mudcat-move'").await?;
    tx.execute("select set_config('mudcat.tenant', $1, true)", &[&tenant.to_string()]).await?;
    tx.execute("select set_config('mudcat.moving', 'on', true)", &[]).await?;
    for t in TENANT_TABLES.iter().rev() {
        tx.execute(&format!("delete from {t}"), &[]).await?;
    }
    if with_marker {
        tx.execute("delete from tenant where id = $1", &[&tenant]).await?;
    }
    tx.commit().await?;
    Ok(())
}

async fn copy_tenant(inst: &Arc<Instance>, tenant: Uuid, source: &deadpool_postgres::Pool, target: &deadpool_postgres::Pool, to_db: &str, fault: Option<f64>) -> Result<Value> {
    let mut sc = source.get().await?;
    // one consistent snapshot of the tenant for the whole copy
    let stx = sc.build_transaction().isolation_level(tokio_postgres::IsolationLevel::RepeatableRead).start().await?;
    // the drain of another move does not wait for this transaction (see `remove_from_source`)
    stx.batch_execute("set local application_name = 'mudcat-move'").await?;
    stx.execute("select set_config('mudcat.tenant', $1, true)", &[&tenant.to_string()]).await?;
    let mut tc = target.get().await?;
    let ttx = tc.transaction().await?;
    ttx.execute("select set_config('mudcat.tenant', $1, true)", &[&tenant.to_string()]).await?;
    let t = inst.tenant(tenant).ok_or(Error::NotFound)?;
    if to_db != "shared" {
        // The home marker of the tenant in a dedicated database. It is visible with the rows,
        // when the copy commits. (In the shared database the marker is the registry entry.)
        ttx.execute(
            "insert into tenant (id, name, endpoint, status, location, created_at) values ($1,$2,$3,'active','dedicated',now()) \
             on conflict (id) do update set status = 'active', location = 'dedicated'",
            &[&tenant, &t.name, &t.endpoint],
        )
        .await?;
    }
    let mut counts = serde_json::Map::new();
    let stop_at = fault.map(|f| (TENANT_TABLES.len() as f64 * f) as usize);
    for (i, table) in TENANT_TABLES.iter().enumerate() {
        if Some(i) == stop_at {
            // the transaction of the target rolls back: it shows no part of the tenant
            return Err(Error::Other(anyhow::anyhow!("the copy failed at table {i} of {} ({table})", TENANT_TABLES.len())));
        }
        let n: i64 = stx.query_one(&format!("select count(*) from {table}"), &[]).await?.get(0);
        counts.insert(table.to_string(), json!(n));
        if n == 0 {
            continue;
        }
        // COPY FROM does not run on a table with row-level security, so the rows go through a
        // staging table; the insert from it passes the policy of the tenant
        ttx.batch_execute(&format!("create temporary table stage_{table} (like {table} including defaults) on commit drop")).await?;
        // by column names: the same columns can have another order in another database
        let cols: Vec<String> = stx
            .query("select column_name::text from information_schema.columns where table_schema = current_schema() and table_name = $1 order by ordinal_position", &[table])
            .await?
            .iter()
            .map(|r| format!("\"{}\"", r.get::<_, String>(0)))
            .collect();
        let cols = cols.join(", ");
        let out = stx.copy_out(&format!("copy (select {cols} from {table}) to stdout (format binary)")).await?;
        let sink = ttx.copy_in(&format!("copy stage_{table} ({cols}) from stdin (format binary)")).await?;
        futures::pin_mut!(out);
        futures::pin_mut!(sink);
        while let Some(chunk) = out.next().await {
            sink.send(chunk?).await?;
        }
        sink.finish().await?;
        ttx.batch_execute(&format!("insert into {table} ({cols}) select {cols} from stage_{table}")).await?;
    }
    // sequences of the target stay ahead of the copied values
    ttx.batch_execute(
        "select setval('commit_seq', greatest((select coalesce(max(commit_id), 1) from commit_log), (select last_value from commit_seq)));",
    )
    .await?;
    for (table, col) in [("audit", "id"), ("outbox", "seq"), ("agent_task", "id")] {
        ttx.batch_execute(&format!(
            "select setval(pg_get_serial_sequence('{table}', '{col}'), greatest((select coalesce(max({col}), 1) from {table}), (select last_value from {table}_{col}_seq)))"
        ))
        .await?;
    }
    ttx.commit().await?;
    stx.commit().await?;
    // the commit sequence of the old database must not hand out lower IDs either
    Ok(Value::Object(counts))
}

impl Instance {
    pub async fn ensure_db(url: &str) -> Result<()> {
        let cfg: tokio_postgres::Config = url.parse().map_err(|e: tokio_postgres::Error| Error::Pool(e.to_string()))?;
        let name = cfg.get_dbname().unwrap_or("mudcat").to_string();
        let admin = crate::instance::db_url(url, "postgres");
        let (client, conn) = tokio_postgres::connect(&admin, tokio_postgres::NoTls).await?;
        tokio::spawn(conn);
        if client.query_opt("select 1 from pg_database where datname = $1", &[&name]).await?.is_none() {
            let _ = client.batch_execute(&format!("create database \"{name}\"")).await;
        }
        Ok(())
    }
}
