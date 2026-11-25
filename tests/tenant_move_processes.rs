//! A tenant move when more than one server process serves the tenant (OP-14; hosted.md, "A move
//! between tiers"; the check of T5.4 "Each write that mudcat accepted is in the tenant one time").
//!
//! Each process keeps a registry of tenants in memory. It is a cache of the table `tenant`. The
//! process that moves a tenant knows the new database at once; no other process does. Here each
//! "process" is an instance of its own (a fork has its own registry) on the same databases. The
//! steps of each test are in a fixed order, so that a defect fails every time.

use mudcat::instance::{Instance, MovePause, Principal, Session, TenantInfo};
use mudcat::Error;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::timeout;
use uuid::Uuid;

fn db_url() -> String {
    std::env::var("MUDCAT_TEST_DB").unwrap_or_else(|_| "postgres://mudcat:mudcat@localhost:54329/mudcat_scenarios_move".into())
}

type Outcome = std::result::Result<(), String>;

macro_rules! expect {
    ($cond:expr, $($msg:tt)+) => {
        if !$cond {
            return Err(format!($($msg)+));
        }
    };
}

struct World {
    /// the process that moves the tenant
    mover: Arc<Instance>,
    /// a second process, which started when the tenant was in the shared tier
    other: Arc<Instance>,
    tenant: TenantInfo,
    /// the database that the tenant moves to
    target: String,
}

impl World {
    async fn new(name: &str) -> World {
        let base = Instance::open(&db_url(), "target/scenario-data").await.expect("open the instance");
        let short = Uuid::new_v4().simple().to_string()[..8].to_string();
        let mover = base.fork(format!("target/scenario-data/move-{name}-{short}-a").into());
        let tenant = mudcat::tenants::create_tenant(&mover, &format!("move-{name}-{short}"), "user:admin").await.expect("create a tenant");
        let other = mover.fork(format!("target/scenario-data/move-{name}-{short}-b").into());
        World { mover, other, tenant, target: format!("mudcat_ded_mv_{short}") }
    }
    fn at_mover(&self) -> Session {
        self.mover.session(self.tenant.id, Principal::user("admin"))
    }
    fn at_other(&self) -> Session {
        self.other.session(self.tenant.id, Principal::user("admin"))
    }
    /// The rows of the tenant in one database, read with no help from a registry.
    async fn collections_in(&self, location: &str) -> i64 {
        let pool = self.mover.pool_for(location).expect("a pool");
        let mut c = pool.get().await.expect("a connection");
        let tx = c.transaction().await.expect("a transaction");
        tx.execute("select set_config('mudcat.tenant', $1, true)", &[&self.tenant.id.to_string()]).await.expect("set the tenant");
        tx.query_one("select count(*) from collection", &[]).await.expect("count").get(0)
    }
    async fn cleanup(&self) {
        let admin = mudcat::instance::db_url(&db_url(), "postgres");
        if let Ok(p) = mudcat::instance::make_pool(&admin, 1) {
            if let Ok(c) = p.get().await {
                let _ = c.batch_execute(&format!("drop database if exists \"{}\" with (force)", self.target)).await;
            }
        }
        if let Ok(c) = self.mover.shared.get().await {
            let _ = c.execute("delete from tenant where id = $1 and location = $2", &[&self.tenant.id, &self.target]).await;
        }
    }
}

/// One write: a new collection, in a transaction of its own.
async fn write(s: &Session, path: &str) -> mudcat::Result<()> {
    let mut c = s.client().await?;
    let mut tx = s.begin(&mut c).await?;
    mudcat::namespace::ensure_collection(&mut tx, path).await?;
    tx.commit().await
}

/// One read: the number of collections with this path that the tenant has.
async fn has(s: &Session, path: &str) -> mudcat::Result<i64> {
    let mut c = s.client().await?;
    let tx = s.begin(&mut c).await?;
    let n: i64 = tx.q1("select count(*) from collection where path = $1", &[&path]).await?.get(0);
    tx.commit().await?;
    Ok(n)
}

fn is_retry<T>(r: &mudcat::Result<T>) -> bool {
    matches!(r, Err(Error::Retryable(_)))
}

/// A client that tries again when mudcat tells it to, a few times.
async fn write_with_retries(s: &Session, path: &str) -> std::result::Result<usize, String> {
    for attempt in 1..=5 {
        match write(s, path).await {
            Ok(()) => return Ok(attempt),
            Err(Error::Retryable(_)) => tokio::time::sleep(Duration::from_millis(20)).await,
            Err(e) => return Err(format!("the write of {path} failed with an error that is not a request to try again: {e}")),
        }
    }
    Err(format!("the write of {path} was refused 5 times"))
}

async fn finish(w: World, r: Outcome) {
    w.cleanup().await;
    if let Err(e) = r {
        panic!("{e}");
    }
}

#[tokio::test]
async fn a_write_through_another_process_after_a_move_is_in_the_tenant() {
    let w = World::new("write").await;
    let r: Outcome = async {
        write(&w.at_mover(), "/w/before").await.map_err(|e| e.to_string())?;
        mudcat::tenants::move_tenant(&w.mover, w.tenant.id, &w.target).await.map_err(|e| format!("the move failed: {e}"))?;
        // The other process still has the shared tier in its registry. Its client gets either
        // an error that lets it try again, or an accepted write; an accepted write is in the tenant.
        let first = write(&w.at_other(), "/w/after").await;
        expect!(first.is_ok() || is_retry(&first), "the first write after the move: {:?}", first.err().map(|e| e.to_string()));
        if first.is_err() {
            write_with_retries(&w.at_other(), "/w/after").await?;
        }
        let n = has(&w.at_mover(), "/w/after").await.map_err(|e| e.to_string())?;
        expect!(n == 1, "mudcat accepted the write of /w/after through the other process, and the tenant has it {n} times");
        let left = w.collections_in("shared").await;
        expect!(left == 0, "{left} collections of the tenant are in the shared tier after the move");
        Ok(())
    }
    .await;
    finish(w, r).await;
}

#[tokio::test]
async fn a_read_through_another_process_after_a_move_sees_the_tenant() {
    let w = World::new("read").await;
    let r: Outcome = async {
        write(&w.at_mover(), "/r/one").await.map_err(|e| e.to_string())?;
        mudcat::tenants::move_tenant(&w.mover, w.tenant.id, &w.target).await.map_err(|e| format!("the move failed: {e}"))?;
        let mut got = has(&w.at_other(), "/r/one").await;
        if is_retry(&got) {
            got = has(&w.at_other(), "/r/one").await;
        }
        let n = got.map_err(|e| format!("the read through the other process: {e}"))?;
        expect!(n == 1, "the other process answered that the tenant has /r/one {n} times; it has it one time");
        Ok(())
    }
    .await;
    finish(w, r).await;
}

#[tokio::test]
async fn a_move_waits_for_open_writes_and_refuses_new_ones_at_once() {
    let w = World::new("fence").await;
    let pause = MovePause::new();
    *w.mover.move_pause.write() = Some(pause.clone());
    let r: Outcome = async {
        write(&w.at_mover(), "/f/before").await.map_err(|e| e.to_string())?;
        // A write transaction of the other process has passed the fence and is open.
        let s = w.at_other();
        let mut c0 = s.client().await.map_err(|e| e.to_string())?;
        let mut open = s.begin(&mut c0).await.map_err(|e| e.to_string())?;
        mudcat::namespace::ensure_collection(&mut open, "/f/open").await.map_err(|e| e.to_string())?;

        let (mover, tenant, target) = (w.mover.clone(), w.tenant.id, w.target.clone());
        let mv = tokio::spawn(async move { mudcat::tenants::move_tenant(&mover, tenant, &target).await });

        // Wait until the mover has claimed the tenant (it makes the target database first, which
        // can take a while); a write before that is an ordinary write.
        let shared = w.mover.shared.get().await.map_err(|e| e.to_string())?;
        let claimed = std::time::Instant::now();
        loop {
            let status: String = shared.query_one("select status from tenant where id = $1", &[&w.tenant.id]).await.map_err(|e| e.to_string())?.get(0);
            if status == "moving" {
                break;
            }
            expect!(claimed.elapsed() < Duration::from_secs(30), "the mover did not claim the tenant");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        // The mover must not start to copy before that transaction ends.
        let early = timeout(Duration::from_millis(700), pause.reached()).await;
        expect!(early.is_err(), "the copy started while a write transaction that had passed the fence was open: its write is lost");
        // While the mover waits, a new write is refused at once, in every process, and reads continue.
        let during = timeout(Duration::from_secs(5), write(&w.at_other(), "/f/during")).await;
        expect!(during.is_ok(), "a write through the other process blocked while the mover waited for the fence");
        expect!(is_retry(during.as_ref().unwrap()), "a write while the tenant moves must get an error to try again: {:?}", during.unwrap().err().map(|e| e.to_string()));
        let n = has(&w.at_other(), "/f/before").await.map_err(|e| format!("a read while the tenant moves: {e}"))?;
        expect!(n == 1, "a read while the tenant moves saw /f/before {n} times");

        open.commit().await.map_err(|e| format!("the open write transaction could not commit: {e}"))?;
        expect!(timeout(Duration::from_secs(20), pause.reached()).await.is_ok(), "the mover did not start to copy after the open write transaction ended");
        // The mover has the fence and is about to copy.
        let during = timeout(Duration::from_secs(5), write(&w.at_other(), "/f/during")).await;
        expect!(during.is_ok(), "a write through the other process blocked during the copy");
        expect!(is_retry(during.as_ref().unwrap()), "a write during the copy must get an error to try again: {:?}", during.unwrap().err().map(|e| e.to_string()));
        let n = has(&w.at_other(), "/f/before").await.map_err(|e| format!("a read during the copy: {e}"))?;
        expect!(n == 1, "a read during the copy saw /f/before {n} times");

        pause.resume();
        let moved = timeout(Duration::from_secs(60), mv).await.map_err(|_| "the move did not end".to_string())?.map_err(|e| e.to_string())?;
        moved.map_err(|e| format!("the move failed: {e}"))?;

        // The write that was open is in the tenant; the refused write succeeds when the client tries again.
        write_with_retries(&w.at_other(), "/f/during").await?;
        for path in ["/f/before", "/f/open", "/f/during"] {
            let n = has(&w.at_mover(), path).await.map_err(|e| e.to_string())?;
            expect!(n == 1, "after the move the tenant has {path} {n} times");
        }
        let left = w.collections_in("shared").await;
        expect!(left == 0, "{left} collections of the tenant are in the shared tier after the move");
        Ok(())
    }
    .await;
    pause.resume();
    finish(w, r).await;
}

#[tokio::test]
async fn a_read_transaction_that_is_open_at_the_switch_keeps_its_rows() {
    let w = World::new("drain").await;
    let r: Outcome = async {
        write(&w.at_mover(), "/k/one").await.map_err(|e| e.to_string())?;
        // A read transaction is open in the old database when the tenant moves (reads continue).
        let s = w.at_other();
        let mut cr = s.client().await.map_err(|e| e.to_string())?;
        let reader = s.begin(&mut cr).await.map_err(|e| e.to_string())?;
        let before: i64 = reader.q1("select count(*) from collection", &[]).await.map_err(|e| e.to_string())?.get(0);

        let (mover, tenant, target) = (w.mover.clone(), w.tenant.id, w.target.clone());
        let mv = tokio::spawn(async move { mudcat::tenants::move_tenant(&mover, tenant, &target).await });
        // wait until new sessions go to the new database
        let shared = w.mover.shared.get().await.map_err(|e| e.to_string())?;
        let started = std::time::Instant::now();
        loop {
            let row = shared.query_one("select status, location from tenant where id = $1", &[&w.tenant.id]).await.map_err(|e| e.to_string())?;
            if row.get::<_, String>(0) == "active" && row.get::<_, String>(1) == w.target {
                break;
            }
            expect!(started.elapsed() < Duration::from_secs(30), "the tenant did not get to the new database");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        // an implementation that removes the old rows at once has done it by now
        tokio::time::sleep(Duration::from_millis(400)).await;
        let after: i64 = reader.q1("select count(*) from collection", &[]).await.map_err(|e| e.to_string())?.get(0);
        expect!(after == before, "a read transaction that began before the move saw {before} collections, and then {after} in the same transaction");
        reader.commit().await.map_err(|e| e.to_string())?;

        let moved = timeout(Duration::from_secs(60), mv).await.map_err(|_| "the move did not end after the read transaction ended".to_string())?.map_err(|e| e.to_string())?;
        let report = moved.map_err(|e| format!("the move failed: {e}"))?;
        expect!(report["source_cleanup"] == "done", "the report of the move: {report}");
        let left = w.collections_in("shared").await;
        expect!(left == 0, "{left} collections of the tenant are in the shared tier after the move");
        Ok(())
    }
    .await;
    finish(w, r).await;
}

#[tokio::test]
async fn a_move_back_to_the_shared_tier_with_another_process() {
    let w = World::new("back").await;
    let r: Outcome = async {
        write(&w.at_mover(), "/b/one").await.map_err(|e| e.to_string())?;
        mudcat::tenants::move_tenant(&w.mover, w.tenant.id, &w.target).await.map_err(|e| format!("the move failed: {e}"))?;
        // the other process learns the dedicated database
        write_with_retries(&w.at_other(), "/b/two").await?;
        mudcat::tenants::move_tenant(&w.mover, w.tenant.id, "shared").await.map_err(|e| format!("the move back failed: {e}"))?;
        // now the registry of the other process names a database that the tenant has left
        let first = write(&w.at_other(), "/b/three").await;
        expect!(first.is_ok() || is_retry(&first), "the first write after the move back: {:?}", first.err().map(|e| e.to_string()));
        if first.is_err() {
            write_with_retries(&w.at_other(), "/b/three").await?;
        }
        for path in ["/b/one", "/b/two", "/b/three"] {
            let n = has(&w.at_mover(), path).await.map_err(|e| e.to_string())?;
            expect!(n == 1, "after the move back the tenant has {path} {n} times");
            let mut got = has(&w.at_other(), path).await;
            if is_retry(&got) {
                got = has(&w.at_other(), path).await;
            }
            let n = got.map_err(|e| e.to_string())?;
            expect!(n == 1, "after the move back the other process sees {path} {n} times");
        }
        let left = w.collections_in(&w.target).await;
        expect!(left == 0, "{left} collections of the tenant are in the dedicated database after the move back");
        Ok(())
    }
    .await;
    finish(w, r).await;
}

#[tokio::test]
async fn a_tenant_that_another_process_made_is_found() {
    let base = Instance::open(&db_url(), "target/scenario-data").await.expect("open the instance");
    let short = Uuid::new_v4().simple().to_string()[..8].to_string();
    // this process starts first
    let other = base.fork(format!("target/scenario-data/move-new-{short}-b").into());
    let maker = base.fork(format!("target/scenario-data/move-new-{short}-a").into());
    let t = mudcat::tenants::create_tenant(&maker, &format!("move-new-{short}"), "user:admin").await.expect("create a tenant");
    assert!(other.tenant(t.id).is_none(), "the registry of the other process cannot have the tenant yet");
    let found = other.find_tenant_by_endpoint(&t.endpoint).await.expect("the lookup");
    assert_eq!(found.map(|x| x.id), Some(t.id), "a process finds a tenant that another process made");
    let s = other.session(t.id, Principal::user("admin"));
    write(&s, "/n/x").await.expect("a write through the process that did not make the tenant");
    assert_eq!(has(&maker.session(t.id, Principal::user("admin")), "/n/x").await.unwrap(), 1);
    // a tenant that does not exist is not found, and a session for it gets no transaction
    assert!(other.find_tenant_by_endpoint("nobody.mudcat.localhost").await.unwrap().is_none());
    let ghost = other.session(Uuid::new_v4(), Principal::user("admin"));
    assert!(matches!(write(&ghost, "/n/y").await, Err(Error::NotFound)), "a session for a tenant that does not exist");
}
