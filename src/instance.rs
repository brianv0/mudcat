//! An instance is a running deployment of mudcat: the services and their databases.
//!
//! The tenant layer is here. A [`Session`] carries the tenant and the principal of a request.
//! [`Session::begin`] opens a database transaction that is scoped to the tenant: it sets
//! `mudcat.tenant`, and row-level security then filters every table. The code below this layer
//! has no tenant parameter (architecture.md, "Tenants").

use crate::error::{Error, Result};
use crate::storage::{ManifestStore, ObjectStore};
use chrono::{DateTime, Duration, Utc};
use deadpool_postgres::{Manager, ManagerConfig, Pool, RecyclingMethod};
use parking_lot::RwLock;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use std::sync::Arc;
use tokio_postgres::types::ToSql;
use tokio_postgres::{NoTls, Row};
use uuid::Uuid;

pub const SCHEMA: &str = include_str!("schema.sql");
pub const EVALUATOR_VERSION: &str = "mudcat-policy/0.1.0";

/// The clock of the instance. Simulations move it forward; the default is the wall clock.
#[derive(Default)]
pub struct Clock {
    offset_ms: AtomicI64,
}

impl Clock {
    pub fn now(&self) -> DateTime<Utc> {
        Utc::now() + Duration::milliseconds(self.offset_ms.load(Ordering::SeqCst))
    }
    pub fn advance(&self, d: Duration) {
        self.offset_ms.fetch_add(d.num_milliseconds(), Ordering::SeqCst);
    }
}

/// Counters that the evals and benchmarks read.
#[derive(Default)]
pub struct Metrics {
    /// Data bytes that mudcat services served to clients. There is no code path that changes it (RD-1).
    pub bytes_served_by_catalog: AtomicU64,
    /// Data bytes that site agents or plugins read for mudcat (verification, extraction).
    pub bytes_read_for_catalog: AtomicU64,
    /// Data bytes that movers copied for mudcat.
    pub bytes_copied_for_catalog: AtomicU64,
    /// Decisions that came from a cache. The built-in adapter never caches (AC-7).
    pub cached_decisions: AtomicU64,
}

#[derive(Clone)]
pub struct Config {
    /// Change events older than this are removed from the change stream.
    pub event_retention: Duration,
    /// A snapshot-committed event lists the members only up to this number.
    pub event_member_limit: usize,
    /// Lease time of a site-agent task.
    pub lease: Duration,
    /// Domain of the endpoints of tenants.
    pub endpoint_domain: String,
    /// Prefix of the DOIs that the instance mints.
    pub doi_prefix: String,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            event_retention: Duration::days(7),
            event_member_limit: 1000,
            lease: Duration::minutes(10),
            endpoint_domain: "mudcat.localhost".into(),
            doi_prefix: "10.99999".into(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct TenantInfo {
    pub id: Uuid,
    pub name: String,
    pub endpoint: String,
    pub status: String,
    pub location: String,
}

/// A principal: a user, a service account, `anonymous` or `system`.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Principal(pub String);

impl Principal {
    pub fn user(name: &str) -> Self {
        Principal(format!("user:{name}"))
    }
    pub fn service(name: &str) -> Self {
        Principal(format!("service:{name}"))
    }
    pub fn anonymous() -> Self {
        Principal("anonymous".into())
    }
    pub fn system() -> Self {
        Principal("system".into())
    }
    pub fn is_anonymous(&self) -> bool {
        self.0 == "anonymous"
    }
    pub fn is_system(&self) -> bool {
        self.0 == "system"
    }
    /// The subject of the principal in relationship tuples: (type, id).
    pub fn subject(&self) -> (String, String) {
        match self.0.split_once(':') {
            Some((t, id)) => (t.to_string(), id.to_string()),
            None => (self.0.clone(), String::new()),
        }
    }
}

impl std::fmt::Display for Principal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// Simulated outside identity providers (a GitHub organisation with teams).
#[derive(Default)]
pub struct IdentityProviders {
    /// provider -> org -> team -> members
    pub teams: RwLock<HashMap<String, HashMap<String, HashMap<String, Vec<String>>>>>,
    /// provider token -> external user
    pub tokens: RwLock<HashMap<String, String>>,
    /// providers that cannot be reached
    pub down: RwLock<std::collections::HashSet<String>>,
}

pub struct Instance {
    pub base_url: String,
    pub shared: Pool,
    pub pools: RwLock<HashMap<String, Pool>>,
    pub tenants: RwLock<HashMap<Uuid, TenantInfo>>,
    pub clock: Clock,
    pub store: ObjectStore,
    /// The manifest store of the tenants (element lists, annotation sets, sealed manifests).
    pub manifests: ManifestStore,
    /// The metadata store behind the customer gateway: customer-held values (ST-10).
    pub customer_gateway: RwLock<HashMap<String, Value>>,
    pub data_dir: PathBuf,
    pub secret: Vec<u8>,
    pub config: Config,
    pub metrics: Metrics,
    pub idp: IdentityProviders,
    /// When true, the authorization port cannot give decisions (a simulated outage, AC-6).
    pub authz_unreachable: AtomicBool,
    /// When set, a tenant move fails after this fraction of the tables (fault injection).
    pub move_fault_at: RwLock<Option<f64>>,
}

pub fn make_pool(url: &str, size: usize) -> Result<Pool> {
    let cfg: tokio_postgres::Config = url.parse().map_err(|e: tokio_postgres::Error| Error::Pool(e.to_string()))?;
    let mgr = Manager::from_config(cfg, NoTls, ManagerConfig { recycling_method: RecyclingMethod::Fast });
    Pool::builder(mgr).max_size(size).build().map_err(|e| Error::Pool(e.to_string()))
}

/// The URL of another database on the same server.
pub fn db_url(base: &str, db: &str) -> String {
    match base.rfind('/') {
        Some(i) => format!("{}/{}", &base[..i], db),
        None => format!("{base}/{db}"),
    }
}

impl Instance {
    /// Connect to the database at `url`, make it if it does not exist, and apply the schema.
    pub async fn open(url: &str, data_dir: impl Into<PathBuf>) -> Result<Arc<Instance>> {
        Self::open_with(url, data_dir, true).await
    }

    /// Like [`Instance::open`]; with `apply = false` it does not apply the schema (for a process
    /// that opens many instances on a database whose schema is already applied).
    pub async fn open_with(url: &str, data_dir: impl Into<PathBuf>, apply: bool) -> Result<Arc<Instance>> {
        let data_dir = data_dir.into();
        let shared = make_pool(url, 120)?;
        if apply {
            ensure_database(url).await?;
            apply_schema(&shared).await?;
        }
        let secret = load_secret(&data_dir)?;
        let inst = Instance {
            base_url: url.to_string(),
            shared: shared.clone(),
            pools: RwLock::new(HashMap::from([("shared".to_string(), shared)])),
            tenants: RwLock::new(HashMap::new()),
            clock: Clock::default(),
            store: ObjectStore::new(data_dir.join("storage"), secret.clone()),
            manifests: ManifestStore::new(data_dir.join("manifests")),
            customer_gateway: RwLock::new(HashMap::new()),
            data_dir,
            secret,
            config: Config::default(),
            metrics: Metrics::default(),
            idp: IdentityProviders::default(),
            authz_unreachable: AtomicBool::new(false),
            move_fault_at: RwLock::new(None),
        };
        inst.load_tenants().await?;
        Ok(Arc::new(inst))
    }

    /// A new instance that shares the databases of this one, with its own clock, storage,
    /// counters and identity providers. The evals run each variant in a fork.
    pub fn fork(&self, data_dir: PathBuf) -> Arc<Instance> {
        let _ = std::fs::create_dir_all(&data_dir);
        Arc::new(Instance {
            base_url: self.base_url.clone(),
            shared: self.shared.clone(),
            pools: RwLock::new(self.pools.read().clone()),
            tenants: RwLock::new(self.tenants.read().clone()),
            clock: Clock::default(),
            store: ObjectStore::new(data_dir.join("storage"), self.secret.clone()),
            manifests: ManifestStore::new(data_dir.join("manifests")),
            customer_gateway: RwLock::new(HashMap::new()),
            data_dir,
            secret: self.secret.clone(),
            config: self.config.clone(),
            metrics: Metrics::default(),
            idp: IdentityProviders::default(),
            authz_unreachable: AtomicBool::new(false),
            move_fault_at: RwLock::new(None),
        })
    }

    pub async fn load_tenants(&self) -> Result<()> {
        let c = self.shared.get().await?;
        let rows = c.query("select id, name, endpoint, status, location from tenant", &[]).await?;
        let mut map = self.tenants.write();
        for r in rows {
            let t = TenantInfo { id: r.get(0), name: r.get(1), endpoint: r.get(2), status: r.get(3), location: r.get(4) };
            map.insert(t.id, t);
        }
        Ok(())
    }

    pub fn now(&self) -> DateTime<Utc> {
        self.clock.now()
    }

    /// The name of the database that holds a location.
    pub fn db_name(&self, location: &str) -> String {
        if location == "shared" {
            self.base_url.rsplit('/').next().unwrap_or("mudcat").split('?').next().unwrap_or("mudcat").to_string()
        } else {
            location.to_string()
        }
    }

    pub fn tenant(&self, id: Uuid) -> Option<TenantInfo> {
        self.tenants.read().get(&id).cloned()
    }

    pub fn tenant_by_endpoint(&self, endpoint: &str) -> Option<TenantInfo> {
        self.tenants.read().values().find(|t| t.endpoint == endpoint).cloned()
    }

    pub fn tenant_by_name(&self, name: &str) -> Option<TenantInfo> {
        self.tenants.read().values().find(|t| t.name == name).cloned()
    }

    pub fn pool_for(&self, location: &str) -> Result<Pool> {
        if let Some(p) = self.pools.read().get(location) {
            return Ok(p.clone());
        }
        let p = make_pool(&db_url(&self.base_url, location), 60)?;
        self.pools.write().insert(location.to_string(), p.clone());
        Ok(p)
    }

    /// A session for a tenant and a principal.
    pub fn session(self: &Arc<Self>, tenant: Uuid, principal: Principal) -> Session {
        Session { inst: self.clone(), tenant: Some(tenant), principal, place: None }
    }

    /// A session with no tenant, as a background job that forgot its tenant would have.
    pub fn session_without_tenant(self: &Arc<Self>) -> Session {
        Session { inst: self.clone(), tenant: None, principal: Principal::system(), place: None }
    }
}

async fn ensure_database(url: &str) -> Result<()> {
    let cfg: tokio_postgres::Config = url.parse().map_err(|e: tokio_postgres::Error| Error::Pool(e.to_string()))?;
    let name = cfg.get_dbname().unwrap_or("mudcat").to_string();
    let admin = db_url(url, "postgres");
    let (client, conn) = tokio_postgres::connect(&admin, NoTls).await?;
    tokio::spawn(conn);
    let exists = client.query_opt("select 1 from pg_database where datname = $1", &[&name]).await?;
    if exists.is_none() {
        // two processes can race here; the loser sees a duplicate error, which is harmless
        let _ = client.batch_execute(&format!("create database \"{name}\"")).await;
    }
    Ok(())
}

pub async fn apply_schema(pool: &Pool) -> Result<()> {
    let c = pool.get().await?;
    // serialise schema changes between processes
    c.batch_execute("select pg_advisory_lock(4242)").await?;
    let r = c.batch_execute(SCHEMA).await;
    c.batch_execute("select pg_advisory_unlock(4242)").await?;
    r?;
    Ok(())
}

fn load_secret(dir: &std::path::Path) -> Result<Vec<u8>> {
    std::fs::create_dir_all(dir)?;
    let path = dir.join("instance.secret");
    if let Ok(s) = std::fs::read(&path) {
        return Ok(s);
    }
    let s: [u8; 32] = rand::random();
    std::fs::write(&path, s)?;
    Ok(s.to_vec())
}

/// The tenant and the principal of a request.
#[derive(Clone)]
pub struct Session {
    pub inst: Arc<Instance>,
    pub tenant: Option<Uuid>,
    pub principal: Principal,
    /// The compute location of the caller, if it states one.
    pub place: Option<String>,
}

impl Session {
    pub fn tenant_id(&self) -> Result<Uuid> {
        self.tenant.ok_or_else(|| Error::invalid("the job has no tenant"))
    }
    pub fn as_principal(&self, p: Principal) -> Session {
        Session { principal: p, ..self.clone() }
    }
    pub fn at_place(&self, place: &str) -> Session {
        Session { place: Some(place.to_string()), ..self.clone() }
    }
    pub fn now(&self) -> DateTime<Utc> {
        self.inst.now()
    }
    fn pool(&self) -> Result<Pool> {
        match self.tenant.and_then(|t| self.inst.tenant(t)) {
            Some(t) => self.inst.pool_for(&t.location),
            None => Ok(self.inst.shared.clone()),
        }
    }
    pub async fn client(&self) -> Result<deadpool_postgres::Object> {
        Ok(self.pool()?.get().await?)
    }
    /// Open a transaction scoped to the tenant of the session.
    pub async fn begin<'a>(&'a self, client: &'a mut deadpool_postgres::Object) -> Result<Tx<'a>> {
        let t = client.transaction().await?;
        let tenant = self.tenant.map(|t| t.to_string()).unwrap_or_default();
        t.execute("select set_config('mudcat.tenant', $1, true)", &[&tenant]).await?;
        let location = self.tenant.and_then(|t| self.inst.tenant(t)).map(|t| t.location).unwrap_or_else(|| "shared".into());
        Ok(Tx { t, sess: self, commit: None, now: self.now(), location, emitted: false })
    }

    /// Write one entry to the audit log, in its own transaction, so that a denial stays in the
    /// log when the request fails (AC-8, INV-9).
    pub async fn audit(&self, point: &str, action: &str, object: &str, allow: bool, reason: Option<&str>, details: Value) -> Result<()> {
        let mut c = self.client().await?;
        let tx = self.begin(&mut c).await?;
        tx.audit(point, action, object, allow, reason, details).await?;
        tx.commit().await
    }
}

/// A database transaction scoped to one tenant.
pub struct Tx<'a> {
    pub t: deadpool_postgres::Transaction<'a>,
    pub sess: &'a Session,
    commit: Option<i64>,
    /// The time of the transaction, from the clock of the instance.
    pub now: DateTime<Utc>,
    /// The database of the tenant when the transaction began.
    pub location: String,
    emitted: bool,
}

impl<'a> Tx<'a> {
    pub fn principal(&self) -> &Principal {
        &self.sess.principal
    }
    pub fn inst(&self) -> &Arc<Instance> {
        &self.sess.inst
    }

    pub async fn q(&self, sql: &str, p: &[&(dyn ToSql + Sync)]) -> Result<Vec<Row>> {
        Ok(self.t.query(sql, p).await?)
    }
    pub async fn q1(&self, sql: &str, p: &[&(dyn ToSql + Sync)]) -> Result<Row> {
        let mut rows = self.t.query(sql, p).await?;
        if rows.len() != 1 {
            return Err(Error::Other(anyhow::anyhow!("expected one row, got {}: {}", rows.len(), sql.split_whitespace().collect::<Vec<_>>().join(" ").chars().take(160).collect::<String>())));
        }
        Ok(rows.pop().unwrap())
    }
    pub async fn qopt(&self, sql: &str, p: &[&(dyn ToSql + Sync)]) -> Result<Option<Row>> {
        Ok(self.t.query_opt(sql, p).await?)
    }
    pub async fn exec(&self, sql: &str, p: &[&(dyn ToSql + Sync)]) -> Result<u64> {
        Ok(self.t.execute(sql, p).await?)
    }

    /// The commit record of this transaction. The first write makes it (EV-14).
    pub async fn commit_id(&mut self) -> Result<i64> {
        if let Some(c) = self.commit {
            return Ok(c);
        }
        let tenant = self.sess.tenant_id()?;
        // the fence of a tenant move: writers hold a shared lock; the move takes it exclusively
        let db: String = self
            .t
            .query_one("select current_database()::text from (select pg_advisory_xact_lock_shared(hashtextextended($1, 11))) x", &[&tenant.to_string()])
            .await?
            .get(0);
        if let Some(t) = self.sess.inst.tenant(tenant) {
            if t.status == "moving" || t.location != self.location || db != self.sess.inst.db_name(&t.location) {
                return Err(Error::Retryable("the tenant is moving to another database; try again".into()));
            }
        }
        let id: i64 = self.t.query_one("select nextval('commit_seq')", &[]).await?.get(0);
        self.t
            .execute("insert into commit_log (commit_id, actor, at) values ($1, $2, $3)", &[&id, &self.sess.principal.0, &self.now])
            .await?;
        self.commit = Some(id);
        Ok(id)
    }

    /// Record a change to an entity: raise its revision and append an event to the outbox, in
    /// this transaction. The event exists only if the transaction commits (EV-1).
    pub async fn emit(&mut self, typ: &str, subject: Uuid, kind: &str, acl_root: Option<Uuid>, data: Value) -> Result<i32> {
        self.emit_full(typ, subject, kind, acl_root, false, data).await
    }

    pub async fn emit_removal(&mut self, typ: &str, subject: Uuid, kind: &str, acl_root: Option<Uuid>, data: Value) -> Result<i32> {
        self.emit_full(typ, subject, kind, acl_root, true, data).await
    }

    async fn emit_full(&mut self, typ: &str, subject: Uuid, kind: &str, acl_root: Option<Uuid>, removal: bool, data: Value) -> Result<i32> {
        let commit = self.commit_id().await?;
        let rev: i32 = self
            .q1(
                "insert into entity (id, kind, revision, created_commit) values ($1, $2, 1, $3)
                 on conflict (tenant_id, id) do update set revision = entity.revision + 1 returning revision",
                &[&subject, &kind, &commit],
            )
            .await?
            .get(0);
        let ev = json!({
            "id": Uuid::now_v7(),
            "type": format!("mudcat.{typ}.v1"),
            "subject": subject,
            "kind": kind,
            "revision": rev,
            "commit": commit,
            "actor": self.sess.principal.0,
            "time": self.now,
            "acl_root": acl_root,
            "removal": removal,
            "data": data,
        });
        self.exec("insert into outbox (commit_id, event) values ($1, $2)", &[&commit, &ev]).await?;
        self.emitted = true;
        Ok(rev)
    }

    /// The envelope of an event, for writers that append many events at once.
    pub fn event_json(&self, commit: i64, typ: &str, subject: Uuid, kind: &str, revision: i32, acl_root: Option<Uuid>, removal: bool, data: Value) -> Value {
        json!({
            "id": Uuid::now_v7(), "type": format!("mudcat.{typ}.v1"), "subject": subject, "kind": kind, "revision": revision,
            "commit": commit, "actor": self.sess.principal.0, "time": self.now, "acl_root": acl_root, "removal": removal, "data": data,
        })
    }

    /// Append many events to the outbox in one statement.
    pub async fn emit_many(&mut self, events: Vec<Value>) -> Result<()> {
        if events.is_empty() {
            return Ok(());
        }
        let commit = self.commit_id().await?;
        for chunk in events.chunks(5000) {
            let v: Vec<Value> = chunk.to_vec();
            self.exec("insert into outbox (commit_id, event) select $1, e from unnest($2::jsonb[]) e", &[&commit, &v]).await?;
        }
        self.emitted = true;
        Ok(())
    }

    /// Register an entity with no event (for entities whose creation another event reports).
    pub async fn register_entity(&mut self, id: Uuid, kind: &str) -> Result<()> {
        let commit = self.commit_id().await?;
        self.exec(
            "insert into entity (id, kind, revision, created_commit) values ($1, $2, 1, $3) on conflict do nothing",
            &[&id, &kind, &commit],
        )
        .await?;
        Ok(())
    }

    pub async fn audit(&self, point: &str, action: &str, object: &str, allow: bool, reason: Option<&str>, details: Value) -> Result<()> {
        let decision = if allow { "allow" } else { "deny" };
        self.exec(
            "insert into audit (at, principal, point, action, object, decision, reason, cached, details)
             values ($1, $2, $3, $4, $5, $6, $7, false, $8)",
            &[&self.now, &self.sess.principal.0, &point, &action, &object, &decision, &reason, &details],
        )
        .await?;
        Ok(())
    }

    /// Commit. A transaction that wrote events takes the commit lock of its tenant, gives its
    /// events the next commit-order number, and holds the lock until the commit completes. Thus
    /// the numbers are in the order in which the commits become visible (EV-13).
    pub async fn commit(self) -> Result<()> {
        if let Some(c) = self.commit {
            if self.emitted {
                let tenant = self.sess.tenant_id()?;
                self.t.execute("select pg_advisory_xact_lock(hashtextextended($1, 13))", &[&tenant.to_string()]).await?;
                self.t.execute("update outbox set ord = (select nextval('outbox_ord_seq')) where commit_id = $1", &[&c]).await?;
            }
        }
        self.t.commit().await?;
        Ok(())
    }
}

/// Make a new ID: a UUIDv7 (ID-1).
pub fn new_id() -> Uuid {
    Uuid::now_v7()
}

/// sha256 of bytes, as hex.
pub fn sha256_hex(data: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    hex::encode(Sha256::digest(data))
}
