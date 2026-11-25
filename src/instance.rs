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
    /// Round trips to the database that the transactions of this instance made. Statements that
    /// go to the database together, without a wait for an answer between them, count one time.
    pub db_round_trips: AtomicU64,
    /// Statements that the transactions of this instance ran.
    pub db_statements: AtomicU64,
}

impl Metrics {
    /// (round trips, statements) so far.
    pub fn db(&self) -> (u64, u64) {
        (self.db_round_trips.load(Ordering::Relaxed), self.db_statements.load(Ordering::Relaxed))
    }
    fn count_db(&self, round_trips: u64, statements: u64) {
        self.db_round_trips.fetch_add(round_trips, Ordering::Relaxed);
        self.db_statements.fetch_add(statements, Ordering::Relaxed);
    }
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

/// A stop in a tenant move, for tests of the write fence: the mover holds the fence and has not
/// started to copy. A test waits for `reached`, makes its checks, and calls `resume`.
pub struct MovePause {
    reached: tokio::sync::Semaphore,
    resume: tokio::sync::Semaphore,
}

impl MovePause {
    pub fn new() -> Arc<MovePause> {
        Arc::new(MovePause { reached: tokio::sync::Semaphore::new(0), resume: tokio::sync::Semaphore::new(0) })
    }
    /// Returns when a mover is at the stop.
    pub async fn reached(&self) {
        if let Ok(p) = self.reached.acquire().await {
            p.forget();
        }
    }
    /// Lets the mover continue (also one that gets to the stop later).
    pub fn resume(&self) {
        self.resume.add_permits(1);
    }
    pub(crate) async fn stop_here(&self) {
        self.reached.add_permits(1);
        if let Ok(p) = self.resume.acquire().await {
            p.forget();
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
    /// A stop in a tenant move, for tests (see [`MovePause`]).
    pub move_pause: RwLock<Option<Arc<MovePause>>>,
}

pub fn make_pool(url: &str, size: usize) -> Result<Pool> {
    let mut cfg: tokio_postgres::Config = url.parse().map_err(|e: tokio_postgres::Error| Error::Pool(e.to_string()))?;
    // Statements are prepared one time for each connection (see `Tx::q`). The plan of a prepared
    // statement is still made for the values of each execution, as it was when each statement
    // was sent as text: a plan for unknown values cannot use the range of a path prefix.
    if cfg.get_options().is_none() {
        cfg.options("-c plan_cache_mode=force_custom_plan");
    }
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
            move_pause: RwLock::new(None),
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
            move_pause: RwLock::new(None),
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

    /// Read one tenant from the table `tenant` of the shared database into the registry of this
    /// process. The registry is a cache of that table: another process can make or move a
    /// tenant. `None`: there is no such tenant, and it leaves the registry.
    pub async fn reload_tenant(&self, id: Uuid) -> Result<Option<TenantInfo>> {
        let c = self.shared.get().await?;
        let row = c.query_opt("select id, name, endpoint, status, location from tenant where id = $1", &[&id]).await?;
        drop(c);
        let mut map = self.tenants.write();
        match row {
            Some(r) => {
                let t = TenantInfo { id: r.get(0), name: r.get(1), endpoint: r.get(2), status: r.get(3), location: r.get(4) };
                map.insert(t.id, t.clone());
                Ok(Some(t))
            }
            None => {
                map.remove(&id);
                Ok(None)
            }
        }
    }

    /// The tenant of an endpoint. On a miss the table is read, so that a process finds a tenant
    /// that another process made.
    pub async fn find_tenant_by_endpoint(&self, endpoint: &str) -> Result<Option<TenantInfo>> {
        if let Some(t) = self.tenant_by_endpoint(endpoint) {
            return Ok(Some(t));
        }
        let c = self.shared.get().await?;
        let row = c.query_opt("select id, name, endpoint, status, location from tenant where endpoint = $1", &[&endpoint]).await?;
        drop(c);
        Ok(row.map(|r| {
            let t = TenantInfo { id: r.get(0), name: r.get(1), endpoint: r.get(2), status: r.get(3), location: r.get(4) };
            self.tenants.write().insert(t.id, t.clone());
            t
        }))
    }

    /// The error for a transaction in a database that is not the home of its tenant. The
    /// registry of this process takes the entry again first, so the client that tries again
    /// gets the right database. A tenant that does not exist is not found.
    async fn not_at_home(&self, id: Uuid) -> Error {
        match self.reload_tenant(id).await {
            Ok(Some(_)) => Error::Retryable("the tenant is in another database, or it is moving; try again".into()),
            Ok(None) => Error::NotFound,
            Err(e) => e,
        }
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

/// Is a tenant at home in the database of a transaction? `marker` is `status|location` of the
/// row of the tenant in the table `tenant` of that database, and `location` is the name that the
/// registry of this process has for the database. In the shared database the row is the registry
/// entry, so its location must be the shared tier. A dedicated database has a row only for a
/// tenant that lives in it. A tenant that is moving can be read where it is, and not written;
/// a tenant that has moved away can be neither.
fn at_home(marker: Option<&str>, location: &str, write: bool) -> bool {
    let Some((status, row_location)) = marker.and_then(|m| m.split_once('|')) else { return false };
    let here = if location == "shared" { row_location == "shared" } else { row_location == "dedicated" };
    here && (status == "active" || (status == "moving" && !write))
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
    // apply a schema text only once: DDL on tables in use would wait for, and deadlock with,
    // the transactions of other processes that use the database
    let hash = sha256_hex(SCHEMA.as_bytes());
    c.batch_execute("create table if not exists schema_hash (hash text primary key)").await?;
    if c.query_opt("select 1 from schema_hash where hash = $1", &[&hash]).await?.is_some() {
        c.batch_execute("select pg_advisory_unlock(4242)").await?;
        return Ok(());
    }
    let r = c.batch_execute(SCHEMA).await;
    if r.is_ok() {
        c.execute("insert into schema_hash (hash) values ($1) on conflict do nothing", &[&hash]).await?;
    }
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
        // Some statements are built for a request (the query language), so the prepared
        // statements of a connection have a limit; past it they all go and are prepared again.
        if client.statement_cache.size() > 2000 {
            client.statement_cache.clear();
        }
        let t = client.transaction().await?;
        let tenant = self.tenant.map(|t| t.to_string()).unwrap_or_default();
        let location = self.tenant.and_then(|t| self.inst.tenant(t)).map(|t| t.location).unwrap_or_else(|| "shared".into());
        // One statement sets the tenant of the transaction and reads the home marker of the
        // tenant in this database. The registry of this process is a cache: another process can
        // have moved the tenant, and then this database is not the place to read or to write.
        // (The statement is prepared one time for each connection: a statement that is planned
        // again in each transaction costs more than the lookup.)
        let known = t.statement_cache.size();
        let begin = Box::pin(t.prepare_cached("select set_config('mudcat.tenant', $1, true), (select status || '|' || location from tenant where id = nullif($1, '')::uuid)")).await?;
        let row = Box::pin(t.query_one(&begin, &[&tenant])).await?;
        // BEGIN, and the scope statement (and its preparation, the first time on a connection)
        self.inst.metrics.count_db(2 + (t.statement_cache.size() > known) as u64, 2);
        if let Some(id) = self.tenant {
            let marker: Option<String> = row.get(1);
            if !at_home(marker.as_deref(), &location, false) && !crate::mutant("no-home-check") {
                drop(t);
                return Err(self.inst.not_at_home(id).await);
            }
        }
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

    /// A statement, prepared one time for each connection. A statement that is sent as text
    /// costs two round trips each time: one to prepare it, and one to run it.
    async fn statement(&self, sql: &str) -> Result<tokio_postgres::Statement> {
        let known = self.t.statement_cache.size();
        // (The futures of the database driver are large. They are on the heap, so that each
        // function of mudcat that waits for a statement does not carry them in its own future:
        // with them inside, deep call chains used up the stack of a thread in a debug build.)
        let st = Box::pin(self.t.prepare_cached(sql)).await?;
        if self.t.statement_cache.size() > known {
            self.sess.inst.metrics.count_db(1, 0);
        }
        Ok(st)
    }
    /// One statement in a round trip of its own.
    fn count_one(&self) {
        self.sess.inst.metrics.count_db(1, 1);
    }
    pub async fn q(&self, sql: &str, p: &[&(dyn ToSql + Sync)]) -> Result<Vec<Row>> {
        let st = self.statement(sql).await?;
        self.count_one();
        Ok(Box::pin(self.t.query(&st, p)).await?)
    }
    pub async fn q1(&self, sql: &str, p: &[&(dyn ToSql + Sync)]) -> Result<Row> {
        let st = self.statement(sql).await?;
        self.count_one();
        let mut rows = Box::pin(self.t.query(&st, p)).await?;
        if rows.len() != 1 {
            return Err(Error::Other(anyhow::anyhow!("expected one row, got {}: {}", rows.len(), sql.split_whitespace().collect::<Vec<_>>().join(" ").chars().take(160).collect::<String>())));
        }
        Ok(rows.pop().unwrap())
    }
    pub async fn qopt(&self, sql: &str, p: &[&(dyn ToSql + Sync)]) -> Result<Option<Row>> {
        let st = self.statement(sql).await?;
        self.count_one();
        Ok(Box::pin(self.t.query_opt(&st, p)).await?)
    }
    pub async fn exec(&self, sql: &str, p: &[&(dyn ToSql + Sync)]) -> Result<u64> {
        let st = self.statement(sql).await?;
        self.count_one();
        Ok(Box::pin(self.t.execute(&st, p)).await?)
    }
    /// A query between two statements that have no parameters (a setting for the query, and the
    /// setting back). The three go to the database together and it runs them in this order: one
    /// round trip.
    pub async fn q_between(&self, before: &str, sql: &str, p: &[&(dyn ToSql + Sync)], after: &str) -> Result<Vec<Row>> {
        let (b, st, a) = (self.statement(before).await?, self.statement(sql).await?, self.statement(after).await?);
        self.sess.inst.metrics.count_db(1, 3);
        let (set, rows, reset) = Box::pin(async { tokio::join!(self.t.execute(&b, &[]), self.t.query(&st, p), self.t.execute(&a, &[])) }).await;
        set?;
        let rows = rows?;
        reset?;
        Ok(rows)
    }

    /// The commit record of this transaction. The first write makes it (EV-14).
    pub async fn commit_id(&mut self) -> Result<i64> {
        if let Some(c) = self.commit {
            return Ok(c);
        }
        let tenant = self.sess.tenant_id()?;
        // during a move, refuse at once; the client tries again
        if let Some(t) = self.sess.inst.tenant(tenant) {
            if t.status == "moving" || t.location != self.location {
                return Err(Error::Retryable("the tenant is moving to another database; try again".into()));
            }
        }
        // Three statements go to the database together and it runs them in this order: one
        // round trip for the scope of a write.
        //
        // 1. The fence of a tenant move. A writer holds this lock, shared, to the end of its
        //    transaction, and the mover takes it exclusively before it copies. The writer does
        //    not wait for it: a mover that holds the lock, or that waits for it, makes this
        //    request fail, and the client tries again. So the mover waits only for the writers
        //    that had passed the fence, and new writers cannot keep it waiting.
        // 2. The home marker, read again with the fence held. The registry of this process
        //    cannot answer: another process can have moved the tenant. It is a statement of its
        //    own, after the lock (under READ COMMITTED it sees what the mover committed before
        //    it opened the fence); a transaction with one snapshot for all its statements would
        //    not see it.
        // 3. The commit record. If 1 or 2 refuses, the caller gets the error and the
        //    transaction ends without a commit, so the record goes with it.
        let key = tenant.to_string();
        let fence = self.statement(if crate::mutant("no-move-fence") { "select $1::text is not null" } else { "select pg_try_advisory_xact_lock_shared(hashtextextended($1, 11))" }).await?;
        let home = self.statement("select status || '|' || location from tenant where id = $1").await?;
        let record = self.statement("insert into commit_log (commit_id, actor, at) values (nextval('commit_seq'), $1, $2) returning commit_id").await?;
        let (p1, p2, p3): ([&(dyn ToSql + Sync); 1], [&(dyn ToSql + Sync); 1], [&(dyn ToSql + Sync); 2]) = ([&key], [&tenant], [&self.sess.principal.0, &self.now]);
        let (passed, marker, recorded) = Box::pin(async { tokio::join!(self.t.query_one(&fence, &p1), self.t.query_opt(&home, &p2), self.t.query_one(&record, &p3)) }).await;
        self.sess.inst.metrics.count_db(1, 3);
        if !passed?.get::<_, bool>(0) {
            return Err(Error::Retryable("the tenant is moving to another database; try again".into()));
        }
        let marker: Option<String> = marker?.map(|r| r.get(0));
        if !at_home(marker.as_deref(), &self.location, true) && !crate::mutant("no-home-check") {
            return Err(self.sess.inst.not_at_home(tenant).await);
        }
        let id: i64 = recorded?.get(0);
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
                // the commit lock and the order number go to the database together: one round trip
                let key = tenant.to_string();
                let lock = self.statement("select pg_advisory_xact_lock(hashtextextended($1, 13))").await?;
                let order = self.statement("update outbox set ord = (select nextval('outbox_ord_seq')) where commit_id = $1").await?;
                let (p1, p2): ([&(dyn ToSql + Sync); 1], [&(dyn ToSql + Sync); 1]) = ([&key], [&c]);
                let (locked, ordered) = Box::pin(async { tokio::join!(self.t.execute(&lock, &p1), self.t.execute(&order, &p2)) }).await;
                self.sess.inst.metrics.count_db(1, 2);
                locked?;
                ordered?;
            }
        }
        self.sess.inst.metrics.count_db(1, 1);
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

#[cfg(test)]
mod tests {
    use super::at_home;

    #[test]
    fn the_home_marker() {
        // the shared database: the row is the registry entry, and must name the shared tier
        assert!(at_home(Some("active|shared"), "shared", true));
        assert!(at_home(Some("active|shared"), "shared", false));
        assert!(!at_home(Some("active|mudcat_ded_1"), "shared", false), "the tenant moved to a dedicated database");
        // a dedicated database: a row only for a tenant that lives there
        assert!(at_home(Some("active|dedicated"), "mudcat_ded_1", true));
        assert!(!at_home(None, "mudcat_ded_1", false), "the tenant left, and its row is gone");
        assert!(!at_home(Some("moved|dedicated"), "mudcat_ded_1", false), "the tenant left, and its rows wait for removal");
        // a registry that names the wrong tier for this database
        assert!(!at_home(Some("active|dedicated"), "shared", false));
        assert!(!at_home(Some("active|shared"), "mudcat_ded_1", false));
        // a tenant that is moving is read where it is, and not written
        assert!(at_home(Some("moving|shared"), "shared", false));
        assert!(!at_home(Some("moving|shared"), "shared", true));
        assert!(at_home(Some("moving|dedicated"), "mudcat_ded_1", false));
        assert!(!at_home(Some("moving|dedicated"), "mudcat_ded_1", true));
        // no row, or a row that is not a marker
        assert!(!at_home(None, "shared", false));
        assert!(!at_home(Some(""), "shared", false));
    }
}
