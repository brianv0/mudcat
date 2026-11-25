//! Benchmarks that characterise the performance of mudcat (requirements PF-3 to PF-8).
//!
//! Usage: mudcat-bench [--versions N] [--only name]...
//! Each benchmark runs in its own tenant of a database `mudcat_bench`. The results go to stdout as
//! a table and to evals/results/bench.json.

use clap::Parser;
use mudcat::datasets::{self, DatasetChange, Member};
use mudcat::in_tx;
use mudcat::instance::{Instance, Principal, Session};
use mudcat::namespace::{self, Register, ReplicaSpec};
use mudcat::{policy, provenance, resolver, search, stream};
use serde_json::{json, Value};
use std::sync::Arc;
use std::time::Instant;
use uuid::Uuid;

#[derive(Parser)]
struct Args {
    #[arg(long, env = "MUDCAT_DB", default_value = "postgres://mudcat:mudcat@localhost:54329/mudcat_bench")]
    db: String,
    #[arg(long, default_value = "target/bench-data")]
    data: String,
    /// The number of versions for the registration, query and move benchmarks.
    #[arg(long, default_value_t = 1_000_000)]
    versions: usize,
    #[arg(long)]
    only: Vec<String>,
    #[arg(long, default_value = "evals/results/bench.json")]
    out: String,
    /// Run only the queries, on an existing tenant whose payloads are already written.
    #[arg(long)]
    queries_on: Option<Uuid>,
}

fn pct(v: &mut [f64], p: f64) -> f64 {
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    if v.is_empty() {
        return 0.0;
    }
    v[((v.len() as f64 - 1.0) * p).round() as usize]
}

fn ms(d: f64) -> String {
    format!("{:.1} ms", d * 1e3)
}

struct Bench {
    inst: Arc<Instance>,
    results: Vec<Value>,
}

impl Bench {
    async fn tenant(&self, name: &str) -> anyhow::Result<Session> {
        let t = mudcat::tenants::create_tenant(&self.inst, &format!("bench-{name}-{}", &Uuid::new_v4().simple().to_string()[..6]), "user:admin").await?;
        let s = self.inst.session(t.id, Principal::user("admin"));
        in_tx!(s, |tx| {
            namespace::create_resource(&mut tx, namespace::ResourceSpec { name: "store".into(), location: "eu".into(), jurisdiction: "eu".into(), mode: "managed".into(), protocol: "s3".into(), egress_allowed: true, ..Default::default() }).await?;
            policy::create_source(&mut tx, &policy::SourceSpec { name: "src".into(), kind: "instrument".into(), ..Default::default() }).await?;
            let sid = policy::source_id(&tx, "src").await?;
            policy::grant_usage(&mut tx, sid, "train", "research", None, Some("bench"), None).await?;
            namespace::set_acl_root(&mut tx, "/bench").await?;
            namespace::mount(&mut tx, "/bench", "store", "bench/").await?;
        });
        Ok(s)
    }
    fn report(&mut self, name: &str, req: &str, v: Value) {
        println!("{name:42} {req:6} {}", v);
        self.results.push(json!({"benchmark": name, "requirement": req, "result": v}));
    }
}

/// Fresh planner statistics after a bulk load (autovacuum does this in normal operation).
async fn analyze(s: &Session) -> anyhow::Result<()> {
    let c = s.client().await?;
    c.batch_execute("analyze").await?;
    Ok(())
}

fn regs(prefix: &str, from: usize, n: usize, verified: bool) -> Vec<Register> {
    (from..from + n)
        .map(|i| Register {
            path: format!("/bench/{prefix}/d{:04}/f{i:08}", i / 10_000),
            content_hash: Some(mudcat::instance::sha256_hex(format!("{prefix}{i}").as_bytes())),
            size: 1000 + (i % 7000) as i64,
            source: Some("src".into()),
            replica: Some(ReplicaSpec { resource: "store".into(), state: verified.then(|| "verified".to_string()), ..Default::default() }),
            ..Default::default()
        })
        .collect()
}

/// PF-3: bulk registration from concurrent workers, in transactions of a batch each.
async fn registration(b: &mut Bench, total: usize) -> anyhow::Result<Session> {
    let mut last = None;
    for (workers, batch) in [(1usize, 1000usize), (4, 1000), (8, 5000), (16, 5000)] {
        let n = if workers == 16 { total } else { (total / 4).max(20_000) };
        let s = b.tenant(&format!("reg{workers}")).await?;
        let started = Instant::now();
        let per = n / workers;
        let mut hs = Vec::new();
        for w in 0..workers {
            let s = s.clone();
            hs.push(tokio::spawn(async move {
                let mut lat = Vec::new();
                for chunk in regs(&format!("w{w}"), 0, per, true).chunks(batch) {
                    let t0 = Instant::now();
                    let mut c = s.client().await?;
                    let mut tx = s.begin(&mut c).await?;
                    namespace::register_batch(&mut tx, chunk).await?;
                    tx.commit().await?;
                    lat.push(t0.elapsed().as_secs_f64());
                }
                Ok::<_, anyhow::Error>(lat)
            }));
        }
        let mut lat = Vec::new();
        for h in hs {
            lat.extend(h.await??);
        }
        let secs = started.elapsed().as_secs_f64();
        let rate = (per * workers) as f64 / secs;
        analyze(&s).await?;
        b.report(
            &format!("bulk registration, {workers} workers, batch {batch}"),
            "PF-3",
            json!({"versions": per * workers, "seconds": secs, "versions_per_minute": (rate * 60.0) as u64, "batch_p50": ms(pct(&mut lat.clone(), 0.5)), "batch_p95": ms(pct(&mut lat, 0.95))}),
        );
        last = Some(s);
    }
    // a repeated registration is idempotent and costs the same order of time
    let s = last.clone().unwrap();
    let t0 = Instant::now();
    let again = regs("w0", 0, 5000, true);
    let out = in_tx!(s, |tx| namespace::register_batch(&mut tx, &again).await?);
    b.report("repeated registration of 5000 (idempotent)", "OP-5", json!({"seconds": t0.elapsed().as_secs_f64(), "new_versions": out.iter().filter(|r| r.created).count()}));
    // the relay: publish the outbox of the largest tenant
    let t0 = Instant::now();
    let n = stream::relay(&s).await?;
    let secs = t0.elapsed().as_secs_f64();
    b.report("change stream relay (outbox -> positions)", "EV-13", json!({"events": n, "seconds": secs, "events_per_second": (n as f64 / secs) as u64}));
    let t0 = Instant::now();
    let mut pos = 0;
    let mut got = 0;
    loop {
        let p = stream::poll(&s, pos, 10_000, None, false).await?;
        got += p.events.len();
        if p.events.is_empty() {
            break;
        }
        pos = p.next;
    }
    let secs = t0.elapsed().as_secs_f64();
    b.report("change stream poll (pages of 10000)", "EV-3", json!({"events": got, "seconds": secs, "events_per_second": (got as f64 / secs) as u64}));
    // latency from commit to visible in the stream
    let mut lat = Vec::new();
    for i in 0..20 {
        let t0 = Instant::now();
        in_tx!(s, |tx| namespace::register(&mut tx, &Register { path: format!("/bench/lat/x{i}"), content_hash: Some(format!("lat{i}")), size: 1, ..Default::default() }).await?);
        loop {
            let p = stream::poll(&s, pos, 100, None, false).await?;
            if !p.events.is_empty() {
                pos = p.next;
                break;
            }
        }
        lat.push(t0.elapsed().as_secs_f64());
    }
    b.report("commit to visible in the change stream", "EV-1", json!({"p50": ms(pct(&mut lat.clone(), 0.5)), "p95": ms(pct(&mut lat, 0.95))}));
    Ok(last.unwrap())
}

/// PF-4 and PF-9: the first page of metadata queries over the versions of a tenant.
async fn queries(b: &mut Bench, s: &Session) -> anyhow::Result<()> {
    queries_setup(b, s).await?;
    queries_run(b, s).await
}

async fn queries_setup(b: &mut Bench, s: &Session) -> anyhow::Result<()> {
    let t0 = Instant::now();
    in_tx!(s, |tx| {
        for (name, dt, indexed) in [("run", "number", true), ("status", "string", true), ("energy", "number", false)] {
            mudcat::metadata::declare_attribute(&mut tx, &mudcat::metadata::AttributeSpec { namespace: "daq".into(), name: name.into(), datatype: dt.into(), indexed, ..Default::default() }).await?;
        }
        let c = tx.commit_id().await?;
        tx.exec(
            "insert into metadata_payload (entity_id, namespace, payload, origin, asserted_by, commit_id)
             select v.id, 'daq', jsonb_build_object('run', (v.size % 1000), 'status', case when v.size % 3 = 0 then 'valid' else 'invalid' end,
                    'energy', (v.size % 97) * 1.5, 'detector', 'd' || (v.size % 10)), 'plugin', 'bench', $1 from version v",
            &[&c],
        )
        .await?;
        tx.exec(
            "insert into attr_index (attr_id, entity_id, num_value) select d.id, p.entity_id, (p.payload->>'run')::float8 from metadata_payload p, attribute_def d where d.name = 'run' and p.namespace = 'daq'",
            &[],
        )
        .await?;
        tx.exec(
            "insert into attr_index (attr_id, entity_id, text_value) select d.id, p.entity_id, p.payload->>'status' from metadata_payload p, attribute_def d where d.name = 'status' and p.namespace = 'daq'",
            &[],
        )
        .await?;
    });
    let n: i64 = in_tx!(s, |tx| tx.q1("select count(*) from version", &[]).await?.get(0));
    {
        let c = s.client().await?;
        c.batch_execute("analyze version; analyze artifact; analyze collection; analyze attr_index; analyze metadata_payload").await?;
    }
    b.report("metadata payloads and typed index (setup)", "PF-9", json!({"versions": n, "seconds": t0.elapsed().as_secs_f64()}));
    in_tx!(s, |tx| {
        let root = namespace::collection(&tx, "/bench").await?.acl_root;
        mudcat::authz::grant(&mut tx, "collection", &root.to_string(), "reader", "user:reader", None).await?;
    });
    Ok(())
}

async fn queries_run(b: &mut Bench, s: &Session) -> anyhow::Result<()> {
    let n: i64 = in_tx!(s, |tx| tx.q1("select count(*) from version", &[]).await?.get(0));
    let reader = s.as_principal(Principal::user("reader"));
    for (label, q) in [
        ("path glob only", "/bench/**"),
        ("column: size > 6200", "/bench/** where size > 6200"),
        ("indexed: run > 990 and status = valid", "/bench/** where run > 990 and status = \"valid\""),
        ("indexed, unselective: run > 10", "/bench/** where run > 10"),
        ("declared, not indexed: energy > 140", "/bench/** where energy > 140"),
        ("undeclared equality (GIN): detector = d3", "/bench/** where detector = \"d3\""),
        ("glob on names: name =~ f0000*", "/bench/** where name =~ \"f00001*\""),
        ("policy predicate: permitted(train, research)", "/bench/** where permitted(train, purpose=research) and run = 5"),
    ] {
        let mut lat = Vec::new();
        let mut rows = 0;
        for _ in 0..7 {
            let t0 = Instant::now();
            let r = in_tx!(reader, |tx| mudcat::query::run(&tx, q, &mudcat::query::QueryOptions { limit: Some(100), ..Default::default() }).await?);
            lat.push(t0.elapsed().as_secs_f64());
            rows = r["items"].as_array().map(|a| a.len()).unwrap_or(0);
        }
        b.report(&format!("query first page: {label}"), "PF-4", json!({"versions": n, "rows": rows, "p50": ms(pct(&mut lat.clone(), 0.5)), "max": ms(pct(&mut lat, 1.0))}));
    }
    Ok(())
}

/// PF-5: commit latency with concurrent writers on one branch.
async fn commits(b: &mut Bench) -> anyhow::Result<()> {
    let s = b.tenant("commit").await?;
    let base: Vec<Uuid> = in_tx!(s, |tx| namespace::register_batch(&mut tx, &regs("base", 0, 21_000, true)).await?).iter().map(|r| r.version_id).collect();
    in_tx!(s, |tx| {
        datasets::create_dataset(&mut tx, "/bench/ds", &Default::default()).await?;
        datasets::commit(&mut tx, &[DatasetChange { dataset: "/bench/ds".into(), add: base[..1000].iter().map(|v| Member::version(*v)).collect(), ..Default::default() }]).await?;
    });
    analyze(&s).await?;
    let base = Arc::new(base);
    for writers in [1usize, 8, 32] {
        for strict in [false, true] {
            let per = if writers == 32 { 10 } else { 40 };
            let started = Instant::now();
            let mut hs = Vec::new();
            for w in 0..writers {
                let s = s.clone();
                let base = base.clone();
                hs.push(tokio::spawn(async move {
                    let mut lat = Vec::new();
                    let mut retries = 0;
                    for k in 0..per {
                        let off = 1000 + ((w * per + k) * 10) % 20_000;
                        let members: Vec<Member> = base[off..off + 10].iter().map(|v| Member::version(*v)).collect();
                        let t0 = Instant::now();
                        loop {
                            let req = if strict {
                                let head = in_tx!(s, |tx| datasets::resolve_ref(&tx, datasets::dataset_id(&tx, "/bench/ds").await?, "main", None).await?);
                                Some(head)
                            } else {
                                None
                            };
                            // a strict commit replaces nothing here but names the snapshot, as remove and replace do
                            let ch = DatasetChange { dataset: "/bench/ds".into(), add: members.clone(), requirement: req, ..Default::default() };
                            let r = async {
                                let mut c = s.client().await?;
                                let mut tx = s.begin(&mut c).await?;
                                datasets::commit(&mut tx, &[ch]).await?;
                                tx.commit().await
                            }
                            .await;
                            match r {
                                Ok(()) => break,
                                Err(mudcat::Error::Conflict { .. }) => retries += 1,
                                Err(e) => return Err(anyhow::anyhow!("{e}")),
                            }
                        }
                        lat.push(t0.elapsed().as_secs_f64());
                    }
                    Ok::<_, anyhow::Error>((lat, retries))
                }));
            }
            let mut lat = Vec::new();
            let mut retries = 0;
            for h in hs {
                let (l, r) = h.await??;
                lat.extend(l);
                retries += r;
            }
            let secs = started.elapsed().as_secs_f64();
            let kind = if strict { "with requirement on the snapshot" } else { "add-only, branch requirement" };
            b.report(
                &format!("commit, {writers} writers, {kind}"),
                "PF-5",
                json!({"commits": lat.len(), "commits_per_second": (lat.len() as f64 / secs) as u64, "p50": ms(pct(&mut lat.clone(), 0.5)), "p95": ms(pct(&mut lat.clone(), 0.95)), "p99": ms(pct(&mut lat, 0.99)), "retries": retries}),
            );
        }
    }
    // the size of the snapshot does not change the cost of a commit much: rows, not files
    let snap = in_tx!(s, |tx| datasets::resolve_ref(&tx, datasets::dataset_id(&tx, "/bench/ds").await?, "main", None).await?);
    let members = in_tx!(s, |tx| datasets::members(&tx, snap).await?.len());
    let t0 = Instant::now();
    in_tx!(s, |tx| datasets::publish(&mut tx, "/bench/ds", "main", "v1").await?);
    b.report("publish (Merkle hash over members)", "ID-5", json!({"members": members, "seconds": t0.elapsed().as_secs_f64()}));
    Ok(())
}

/// Resolution of large snapshots, with an invalidation set; and PF-8.
async fn resolution(b: &mut Bench) -> anyhow::Result<()> {
    for n in [10_000usize, 100_000] {
        let s = b.tenant(&format!("res{n}")).await?;
        let mut ids = Vec::new();
        for c in regs("r", 0, n, true).chunks(10_000) {
            ids.extend(in_tx!(s, |tx| namespace::register_batch(&mut tx, c).await?).into_iter().map(|r| r.version_id));
        }
        in_tx!(s, |tx| {
            datasets::create_dataset(&mut tx, "/bench/set", &Default::default()).await?;
            datasets::commit(&mut tx, &[DatasetChange { dataset: "/bench/set".into(), add: ids.iter().map(|v| Member::version(*v)).collect(), ..Default::default() }]).await?;
        });
        analyze(&s).await?;
        let mut lat = Vec::new();
        let mut tasks = 0;
        for _ in 0..3 {
            let t0 = Instant::now();
            let r = resolver::resolve(&s, &resolver::Request { inputs: vec![resolver::Input { dataset: Some("/bench/set".into()), role: "training".into(), ..Default::default() }], action: Some("train".into()), purpose: Some("research".into()), ..Default::default() }).await?;
            lat.push(t0.elapsed().as_secs_f64());
            tasks = r.tasks.len();
        }
        b.report(&format!("resolve a snapshot of {n} members (train, research)"), "RD-2", json!({"tasks": tasks, "p50": ms(pct(&mut lat.clone(), 0.5)), "per_member_us": pct(&mut lat, 0.5) / n as f64 * 1e6}));
        // with 1000 invalidations in the set
        in_tx!(s, |tx| for v in ids.iter().step_by((n / 1000).max(1)).take(1000) {
            tx.exec("insert into invalidation (id, target_kind, target, reason, requested_by, commit_id, at) values ($1,'version',$2,'bench','bench',0,now())", &[&Uuid::now_v7(), &v.to_string()]).await?;
        });
        let t0 = Instant::now();
        let r = resolver::resolve(&s, &resolver::Request { inputs: vec![resolver::Input { dataset: Some("/bench/set".into()), role: "training".into(), ..Default::default() }], action: Some("train".into()), purpose: Some("research".into()), ..Default::default() }).await?;
        b.report(&format!("resolve {n} members with 1000 invalidations"), "UP-9", json!({"tasks": r.tasks.len(), "seconds": t0.elapsed().as_secs_f64()}));
        // PF-8: the policy predicate from the cache, against a full evaluation
        let snap = in_tx!(s, |tx| datasets::resolve_ref(&tx, datasets::dataset_id(&tx, "/bench/set").await?, "main", None).await?);
        let t0 = Instant::now();
        in_tx!(s, |tx| policy::refresh_policy_cache(&tx, snap).await?);
        let refresh = t0.elapsed().as_secs_f64();
        let mut lat = Vec::new();
        for _ in 0..20 {
            let t0 = Instant::now();
            let ok = in_tx!(s, |tx| policy::permitted_predicate(&tx, snap, "train", "research").await?);
            assert_eq!(ok, Some(true));
            lat.push(t0.elapsed().as_secs_f64());
        }
        b.report(&format!("permitted(train, research) on a snapshot of {n}"), "PF-8", json!({"cache_lookup_p50": ms(pct(&mut lat, 0.5)), "cache_refresh": ms(refresh), "reads_members": false}));
    }
    Ok(())
}

/// PF-6 and PF-7: ancestry over chains of models and wide training runs.
async fn lineage(b: &mut Bench) -> anyhow::Result<()> {
    let s = b.tenant("lineage").await?;
    let ids: Vec<Uuid> = in_tx!(s, |tx| namespace::register_batch(&mut tx, &regs("l", 0, 1000, true)).await?).iter().map(|r| r.version_id).collect();
    in_tx!(s, |tx| {
        datasets::create_dataset(&mut tx, "/bench/train", &Default::default()).await?;
        datasets::commit(&mut tx, &[DatasetChange { dataset: "/bench/train".into(), add: ids.iter().map(|v| Member::version(*v)).collect(), ..Default::default() }]).await?;
    });
    analyze(&s).await?;
    let r = resolver::resolve(&s, &resolver::Request { inputs: vec![resolver::Input { dataset: Some("/bench/train".into()), role: "training".into(), ..Default::default() }], action: Some("train".into()), purpose: Some("research".into()), ..Default::default() }).await?;
    let mut prev: Option<Uuid> = None;
    for i in 0..200 {
        let act = in_tx!(s, |tx| provenance::record_run(&mut tx, &provenance::RunSpec { kind: "training".into(), resolution: if i == 0 { Some(r.id) } else { None }, ..Default::default() }).await?);
        let m = in_tx!(s, |tx| provenance::publish_model(&mut tx, &provenance::ModelSpec { path: format!("/models/m{i:03}"), content_hash: format!("w{i}"), size: 1, activity: Some(act), derived_from: prev.map(|p| vec![(p, "fineTunedFrom".into())]).unwrap_or_default(), ..Default::default() }).await?);
        prev = Some(m);
    }
    for depth in [10usize, 50, 200, 400] {
        let mut lat = Vec::new();
        let mut nodes = 0;
        for _ in 0..5 {
            let t0 = Instant::now();
            let v = in_tx!(s, |tx| provenance::lineage(&tx, prev.unwrap(), &provenance::LineageQuery { direction: "ancestors".into(), depth: Some(depth), max_nodes: Some(100_000), ..Default::default() }).await);
            lat.push(t0.elapsed().as_secs_f64());
            nodes = match v {
                Ok(v) => v["nodes"].as_array().map(|a| a.len()).unwrap_or(0) as i64,
                Err(e) if e.code() == "lineage-limit" => -1,
                Err(e) => return Err(anyhow::anyhow!("{e}")),
            };
        }
        b.report(&format!("ancestors of a model, depth limit {depth}"), "PF-6", json!({"nodes": if nodes < 0 { json!("limit error (PF-7)") } else { json!(nodes) }, "p50": ms(pct(&mut lat, 0.5))}));
    }
    let t0 = Instant::now();
    let v = in_tx!(s, |tx| provenance::lineage(&tx, prev.unwrap(), &provenance::LineageQuery { direction: "ancestors".into(), depth: Some(1000), max_nodes: Some(100), ..Default::default() }).await);
    b.report("ancestors with a node limit of 100", "PF-7", json!({"error": v.err().map(|e| e.to_string()), "seconds": t0.elapsed().as_secs_f64()}));
    // QY-1 over the chain: the models that used a source
    let t0 = Instant::now();
    let models = in_tx!(s, |tx| provenance::models_using_source(&tx, policy::source_id(&tx, "src").await?).await?);
    b.report("models that used a source (QY-1)", "QY-1", json!({"models": models.len(), "seconds": t0.elapsed().as_secs_f64()}));
    Ok(())
}

/// Search: index build and query latency.
async fn search_bench(b: &mut Bench) -> anyhow::Result<()> {
    let s = b.tenant("search").await?;
    let words = ["pedestrian", "night", "rain", "cyclist", "highway", "snow", "tunnel", "bridge", "truck", "intersection", "fog", "sunset"];
    let n = 50_000;
    let mut rs = regs("s", 0, n, true);
    for (i, r) in rs.iter_mut().enumerate() {
        r.path = format!("/bench/clips/{}_{}_{i:06}.mp4", words[i % words.len()], words[(i / 12) % words.len()]);
    }
    for c in rs.chunks(10_000) {
        in_tx!(s, |tx| namespace::register_batch(&mut tx, c).await?);
    }
    analyze(&s).await?;
    let t0 = Instant::now();
    search::build(&s).await?;
    let secs = t0.elapsed().as_secs_f64();
    b.report("search index build from the change stream", "OP-2", json!({"documents": n, "seconds": secs, "documents_per_second": (n as f64 / secs) as u64}));
    let reader = s.as_principal(Principal::user("reader"));
    in_tx!(s, |tx| {
        let root = namespace::collection(&tx, "/bench").await?.acl_root;
        mudcat::authz::grant(&mut tx, "collection", &root.to_string(), "reader", "user:reader", None).await?;
    });
    for (label, q) in [("exact words", "pedestrian night"), ("one spelling error", "pedestrain night"), ("three words", "cyclist in fog tunnel")] {
        let mut lat = Vec::new();
        let mut hits = 0;
        for _ in 0..7 {
            let t0 = Instant::now();
            let r = search::search(&reader, &search::SearchRequest { text: Some(q.into()), limit: Some(20), ..Default::default() }).await?;
            lat.push(t0.elapsed().as_secs_f64());
            hits = r.hits.len();
        }
        b.report(&format!("search: {label} ({n} documents)"), "EV-9", json!({"hits": hits, "p50": ms(pct(&mut lat.clone(), 0.5)), "p95": ms(pct(&mut lat, 0.95))}));
    }
    let t0 = Instant::now();
    search::rebuild(&s).await?;
    b.report("search index rebuild (new version, then switch)", "OP-3", json!({"documents": n, "seconds": t0.elapsed().as_secs_f64()}));
    Ok(())
}

/// OP-14: the move of a tenant to its own database.
async fn tenant_move(b: &mut Bench, versions: usize) -> anyhow::Result<()> {
    let s = b.tenant("move").await?;
    let n = versions.min(1_000_000);
    for c in regs("m", 0, n, true).chunks(10_000) {
        in_tx!(s, |tx| namespace::register_batch(&mut tx, c).await?);
    }
    stream::relay(&s).await?;
    analyze(&s).await?;
    let rows: i64 = {
        let mut c = s.client().await?;
        let tx = s.begin(&mut c).await?;
        let mut t = 0i64;
        for table in mudcat::tenants::TENANT_TABLES {
            t += tx.q1(&format!("select count(*) from {table}"), &[]).await?.get::<_, i64>(0);
        }
        t
    };
    let target = format!("mudcat_bench_ded_{}", &s.tenant_id()?.simple().to_string()[..8]);
    let t0 = Instant::now();
    mudcat::tenants::move_tenant(&b.inst, s.tenant_id()?, &target).await?;
    let secs = t0.elapsed().as_secs_f64();
    b.report("move a tenant to a dedicated database", "OP-14", json!({"versions": n, "rows": rows, "seconds": secs, "rows_per_second": (rows as f64 / secs) as u64}));
    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let a = Args::parse();
    let inst = Instance::open(&a.db, &a.data).await?;
    let mut b = Bench { inst, results: Vec::new() };
    let run = |n: &str| a.only.is_empty() || a.only.iter().any(|o| o == n);
    let started = Instant::now();
    if let Some(t) = a.queries_on {
        let s = b.inst.session(t, Principal::user("admin"));
        queries_run(&mut b, &s).await?;
        return Ok(());
    }
    if run("registration") || run("queries") {
        let s = registration(&mut b, a.versions).await?;
        if run("queries") {
            queries(&mut b, &s).await?;
        }
    }
    if run("commits") {
        commits(&mut b).await?;
    }
    if run("resolution") {
        resolution(&mut b).await?;
    }
    if run("lineage") {
        lineage(&mut b).await?;
    }
    if run("search") {
        search_bench(&mut b).await?;
    }
    if run("move") {
        tenant_move(&mut b, a.versions).await?;
    }
    let host = json!({"cpus": std::thread::available_parallelism().map(|n| n.get()).unwrap_or(0), "seconds": started.elapsed().as_secs_f64()});
    if let Some(dir) = std::path::Path::new(&a.out).parent() {
        std::fs::create_dir_all(dir)?;
    }
    std::fs::write(&a.out, serde_json::to_string_pretty(&json!({"host": host, "results": b.results}))?)?;
    println!("wrote {}", a.out);
    Ok(())
}
