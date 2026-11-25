//! The branch requirement of commits with two transactions in a fixed order (ID-7; model.md,
//! "Commits with requirements").
//!
//! Two writers commit to the same branch. The first transaction (A) makes its commit and stays
//! open. The second (B) starts the same kind of commit on another connection. B must wait for the
//! row lock on the ref until A ends; there is no fixed sleep, the test looks at `pg_stat_activity`
//! until the backend of B waits for a lock. Then A commits and B goes on:
//!
//! - a commit that removes or replaces members and names the snapshot S, which A has moved the
//!   branch away from, ends with the conflict error that names the current snapshot (A's);
//! - a commit that only adds members, with no requirement, applies to the snapshot of A: its parent
//!   is A's snapshot and the head has the members of both.
//!
//! Without the lock (`MUDCAT_MUTANT=no-branch-lock`) B does not wait, and each of the two tests fails.

use mudcat::datasets::{self, DatasetChange, Member};
use mudcat::evals::fixtures::*;
use mudcat::namespace::{self, Register};
use mudcat::{in_tx, Error, Instance, Principal, Result, Session};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::task::JoinHandle;
use uuid::Uuid;

async fn setup(tag: &str) -> Result<(Arc<Instance>, Session)> {
    let url = std::env::var("MUDCAT_TEST_DB").unwrap_or_else(|_| "postgres://mudcat:mudcat@localhost:54329/mudcat_scenarios".into());
    let base = Instance::open(&url, "target/scenario-data").await?;
    let id = Uuid::new_v4().simple().to_string();
    let inst = base.fork(std::path::PathBuf::from("target/scenario-data/work").join(format!("{tag}-{id}")));
    let t = mudcat::tenants::create_tenant(&inst, &format!("{tag}-{}", &id[..10]), "user:admin").await?;
    let s = inst.session(t.id, Principal::user("admin"));
    Ok((inst, s))
}

/// `n` versions that are not members of anything yet.
async fn versions(s: &Session, tag: &str, n: usize) -> Result<Vec<Uuid>> {
    let regs: Vec<Register> = (0..n).map(|i| Register { path: format!("/c/{tag}-{i}"), content_hash: Some(format!("{tag}-hash-{i}")), size: 1, ..Default::default() }).collect();
    Ok(in_tx!(s, |tx| namespace::register_batch(&mut tx, &regs).await?).iter().map(|r| r.version_id).collect())
}

/// B: another connection and transaction. It reports the process ID of its backend, makes the
/// commit and ends the transaction.
async fn start_b(s: &Session, change: DatasetChange) -> (JoinHandle<Result<datasets::Committed>>, i32) {
    let (tx_pid, rx_pid) = tokio::sync::oneshot::channel();
    let sb = s.clone();
    let handle = tokio::spawn(async move {
        let mut c = sb.client().await?;
        let mut tx = sb.begin(&mut c).await?;
        let pid: i32 = tx.q1("select pg_backend_pid()", &[]).await?.get(0);
        let _ = tx_pid.send(pid);
        let r = datasets::commit(&mut tx, &[change]).await;
        match r {
            Ok(mut v) => {
                tx.commit().await?;
                Ok(v.remove(0))
            }
            Err(e) => Err(e),
        }
    });
    let pid = rx_pid.await.expect("the transaction of B reports its backend");
    (handle, pid)
}

/// Wait up to 5 seconds until the backend `pid` waits for a lock. Fails at once if B has ended.
async fn wait_until_waits(s: &Session, pid: i32, b: &JoinHandle<Result<datasets::Committed>>) -> std::result::Result<(), String> {
    let c = s.client().await.map_err(|e| e.to_string())?;
    let started = Instant::now();
    loop {
        let n: i64 = c
            .query_one("select count(*) from pg_stat_activity where pid = $1 and datname = current_database() and wait_event_type = 'Lock'", &[&pid])
            .await
            .map_err(|e| e.to_string())?
            .get(0);
        if n == 1 {
            return Ok(());
        }
        if b.is_finished() {
            return Err("the second commit ended while the first transaction was open: it did not wait for the lock on the branch ref".into());
        }
        if started.elapsed() > Duration::from_secs(5) {
            return Err("the second commit did not wait for a lock in 5 seconds while the first transaction was open".into());
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

async fn head(s: &Session, path: &str) -> Result<Uuid> {
    Ok(in_tx!(s, |tx| datasets::resolve_ref(&tx, datasets::dataset_id(&tx, path).await?, "main", None).await?))
}

/// Two commits that remove or replace a member, and both name S as their requirement.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_second_commit_with_a_requirement_waits_and_then_gets_the_conflict() -> Result<()> {
    let (_inst, s) = setup("interleave-strict").await?;
    let v = versions(&s, "strict", 4).await?;
    let first = dataset(&s, "/c/strict", v.iter().map(|x| Member::version(*x)).collect()).await?;
    let snap_s = first.snapshot;

    // A makes its commit (it removes v[0] and names S) and stays open
    let mut ca = s.client().await?;
    let mut a = s.begin(&mut ca).await?;
    let ca_commit = datasets::commit(&mut a, &[DatasetChange { dataset: "/c/strict".into(), requirement: Some(snap_s), remove: vec![v[0]], ..Default::default() }]).await?.remove(0);
    let snap_a = ca_commit.snapshot;

    // B makes the same kind of commit (it removes v[1]) and names S too
    let (b, pid) = start_b(&s, DatasetChange { dataset: "/c/strict".into(), requirement: Some(snap_s), remove: vec![v[1]], ..Default::default() }).await;
    let waited = wait_until_waits(&s, pid, &b).await;
    a.commit().await?;
    if let Err(m) = waited {
        let _ = b.await;
        panic!("{m}");
    }
    let r = tokio::time::timeout(Duration::from_secs(10), b).await.expect("B ends after A has committed").expect("B did not panic");
    let e = r.err().expect("B has a requirement on S, and the branch has moved: it ends with an error");
    assert!(matches!(e, Error::Conflict { .. }), "the error of B is a conflict ({e})");
    assert_eq!(e.details()["current"], serde_json::json!(snap_a), "the conflict names the current snapshot, the snapshot of A ({})", e.details());
    assert!(e.to_string().contains(&snap_a.to_string()), "the message of the conflict names the current snapshot ({e})");
    assert_eq!(head(&s, "/c/strict").await?, snap_a, "the ref points to the snapshot of A");
    Ok(())
}

/// Two commits that only add members, with no requirement.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_second_commit_that_only_adds_waits_and_then_applies_to_the_snapshot_of_the_first() -> Result<()> {
    let (_inst, s) = setup("interleave-add").await?;
    let v = versions(&s, "add", 4).await?;
    let first = dataset(&s, "/c/add", v[..2].iter().map(|x| Member::version(*x)).collect()).await?;
    let _ = first;

    // A adds v[2] and stays open
    let mut ca = s.client().await?;
    let mut a = s.begin(&mut ca).await?;
    let snap_a = datasets::commit(&mut a, &[DatasetChange { dataset: "/c/add".into(), add: vec![Member::version(v[2])], ..Default::default() }]).await?.remove(0).snapshot;

    // B adds v[3]
    let (b, pid) = start_b(&s, DatasetChange { dataset: "/c/add".into(), add: vec![Member::version(v[3])], ..Default::default() }).await;
    let waited = wait_until_waits(&s, pid, &b).await;
    a.commit().await?;
    if let Err(m) = waited {
        let _ = b.await;
        panic!("{m}");
    }
    let r = tokio::time::timeout(Duration::from_secs(10), b).await.expect("B ends after A has committed").expect("B did not panic")?;
    let parent = in_tx!(s, |tx| datasets::snapshot(&tx, r.snapshot).await?.parent_id);
    assert_eq!(parent, Some(snap_a), "the parent of the snapshot of B is the snapshot of A");
    let h = head(&s, "/c/add").await?;
    assert_eq!(h, r.snapshot, "the ref points to the snapshot of B");
    let members: std::collections::BTreeSet<Uuid> = in_tx!(s, |tx| datasets::members(&tx, h).await?.iter().map(|e| e.member_id).collect());
    assert_eq!(members, v.iter().copied().collect(), "the head has the members of both commits and the first two");
    Ok(())
}
