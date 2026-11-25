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
    "event", "stream_state", "subscription", "search_doc", "index_state",
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

/// Move a tenant to another database. The records keep their keys, because the tenant ID is
/// first in every key (OP-14). During the copy mudcat accepts no writes for the tenant, and reads
/// continue. If the copy fails, the target database shows no part of the tenant, and the tenant
/// continues where it was.
pub async fn move_tenant(inst: &Arc<Instance>, tenant: Uuid, to_db: &str) -> Result<Value> {
    let info = inst.tenant(tenant).ok_or(Error::NotFound)?;
    let from = info.location.clone();
    if from == to_db {
        return Err(Error::invalid("the tenant is already in that database"));
    }
    let target_url = crate::instance::db_url(&inst.base_url, to_db);
    crate::instance::Instance::ensure_db(&target_url).await?;
    let target = inst.pool_for(to_db)?;
    apply_schema(&target).await?;
    let source = inst.pool_for(&from)?;
    set_status(inst, tenant, "moving", None).await?;
    // wait for write transactions of the tenant that started before the move
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    let fault = *inst.move_fault_at.read();
    let result = copy_tenant(inst, tenant, &source, &target, fault).await;
    match result {
        Err(e) => {
            set_status(inst, tenant, "active", None).await?;
            Err(e)
        }
        Ok(stats) => {
            set_status(inst, tenant, "active", Some(to_db)).await?;
            // remove the rows of the tenant from the old database
            let mut c = source.get().await?;
            let tx = c.transaction().await?;
            tx.execute("select set_config('mudcat.tenant', $1, true)", &[&tenant.to_string()]).await?;
            tx.execute("select set_config('mudcat.moving', 'on', true)", &[]).await?;
            for t in TENANT_TABLES.iter().rev() {
                tx.execute(&format!("delete from {t}"), &[]).await?;
            }
            if from != "shared" {
                tx.execute("delete from tenant where id = $1", &[&tenant]).await?;
            }
            tx.commit().await?;
            Ok(json!({"tenant": tenant, "from": from, "to": to_db, "rows": stats}))
        }
    }
}

async fn copy_tenant(inst: &Arc<Instance>, tenant: Uuid, source: &deadpool_postgres::Pool, target: &deadpool_postgres::Pool, fault: Option<f64>) -> Result<Value> {
    let mut sc = source.get().await?;
    // one consistent snapshot of the tenant for the whole copy
    let stx = sc.build_transaction().isolation_level(tokio_postgres::IsolationLevel::RepeatableRead).start().await?;
    stx.execute("select set_config('mudcat.tenant', $1, true)", &[&tenant.to_string()]).await?;
    let mut tc = target.get().await?;
    let ttx = tc.transaction().await?;
    ttx.execute("select set_config('mudcat.tenant', $1, true)", &[&tenant.to_string()]).await?;
    let t = inst.tenant(tenant).unwrap();
    ttx.execute(
        "insert into tenant (id, name, endpoint, status, location, created_at) values ($1,$2,$3,'active',$4,now()) on conflict (id) do nothing",
        &[&tenant, &t.name, &t.endpoint, &"dedicated"],
    )
    .await?;
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
        let out = stx.copy_out(&format!("copy (select * from {table}) to stdout (format binary)")).await?;
        let sink = ttx.copy_in(&format!("copy stage_{table} from stdin (format binary)")).await?;
        futures::pin_mut!(out);
        futures::pin_mut!(sink);
        while let Some(chunk) = out.next().await {
            sink.send(chunk?).await?;
        }
        sink.finish().await?;
        ttx.batch_execute(&format!("insert into {table} select * from stage_{table}")).await?;
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
