//! Reference scenarios of use case 2: training data for scientific models, from several sources
//! and sites (evals/scenarios/uc2-science-training.yaml).

use mudcat::datasets::{self, DatasetChange, DatasetOptions, Member};
use mudcat::evals::fixtures::*;
use mudcat::instance::sha256_hex;
use mudcat::namespace::{self, Register, ReplicaSpec, Resource, ResourceSpec};
use mudcat::policy::{self, SourceSpec};
use mudcat::provenance::{self, LineageQuery, ModelSpec, RunSpec};
use mudcat::resolver::{self, Input};
use mudcat::storage::Credential;
use mudcat::{in_tx, leakage, metadata, placement, Instance, Principal, Result, Session};
use serde_json::{json, Value};
use std::collections::BTreeSet;
use std::sync::Arc;
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

async fn publish_tag(s: &Session, path: &str, tag: &str) -> Result<datasets::SnapshotInfo> {
    Ok(in_tx!(s, |tx| datasets::publish(&mut tx, path, "main", tag).await?))
}

async fn head(s: &Session, path: &str) -> Result<Uuid> {
    Ok(in_tx!(s, |tx| datasets::resolve_ref(&tx, datasets::dataset_id(&tx, path).await?, "main", None).await?))
}

async fn computed_source(s: &Session, name: &str, method: Value) -> Result<Uuid> {
    Ok(in_tx!(s, |tx| {
        let id = policy::create_source(&mut tx, &SourceSpec { name: name.into(), kind: "computed".into(), attrs: json!({}), method: Some(method) }).await?;
        policy::grant_usage(&mut tx, id, "train", "research", None, Some("group agreement"), None).await?;
        id
    }))
}

/// Write files and register them with domain keys (for similarity criteria).
async fn register_keyed(s: &Session, res: &Resource, coll: &str, prefix: &str, files: &[(String, Vec<u8>, Option<String>)], source: &str) -> Result<Vec<Uuid>> {
    let mut regs = Vec::new();
    for (name, data, key) in files {
        let k = format!("{prefix}{name}");
        s.inst.store.put("producer", &res.bucket, &k, data)?;
        regs.push(Register {
            path: format!("{coll}/{name}"),
            content_hash: Some(sha256_hex(data)),
            size: data.len() as i64,
            source: Some(source.into()),
            domain_key: key.clone(),
            replica: Some(ReplicaSpec { resource: res.name.clone(), key: Some(k), ..Default::default() }),
            ..Default::default()
        });
    }
    let mut out = Vec::new();
    for c in regs.chunks(2000) {
        out.extend(in_tx!(s, |tx| namespace::register_batch(&mut tx, c).await?).into_iter().map(|r| r.version_id));
    }
    Ok(out)
}

async fn train_model(s: &Session, input: Input, path: &str) -> Result<(Uuid, resolver::Resolution, Uuid)> {
    let r = resolver::resolve(s, &train_request(vec![input], "train", "research")).await?;
    let act = in_tx!(s, |tx| provenance::record_run(&mut tx, &RunSpec { kind: "training".into(), code: Some("git:abc123".into()), image: Some("sha256:0f0f".into()), params: json!({"seed": 7}), resolution: Some(r.id), ..Default::default() }).await?);
    let mv = in_tx!(s, |tx| provenance::publish_model(&mut tx, &ModelSpec { path: path.into(), content_hash: sha256_hex(path.as_bytes()), size: 100, activity: Some(act), ..Default::default() }).await?);
    Ok((mv, r, act))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn uc2_01() -> Result<()> {
    let (inst, s) = setup("uc2-01").await?;
    // NOT ASSERTED: "a self-hosted instance with one tenant" is a deployment fact; the test uses one tenant of the dev instance.
    let store = resource(&s, "store", "cloud", "managed", false).await?;
    acl_root(&s, "/potentials", Some(("store", "pot/"))).await?;
    let method = json!({"functional": "PBE", "hubbard_u": "no"});
    computed_source(&s, "group-a", method.clone()).await?;
    computed_source(&s, "group-b", method).await?;
    let mut snaps = Vec::new();
    for (d, src) in [("a", "group-a"), ("b", "group-b")] {
        let files: Vec<(String, Vec<u8>)> = (0..5).map(|i| (format!("{d}{i}.extxyz"), file_bytes("records", i, d))).collect();
        let regs = register_files(&s, &store, &format!("/potentials/{d}-files"), &format!("pot/{d}-files/"), &files, Some(src), None).await?;
        dataset(&s, &format!("/potentials/{d}"), regs.iter().map(|r| Member::version(r.version_id)).collect()).await?;
        snaps.push(publish_tag(&s, &format!("/potentials/{d}"), "v1").await?);
    }
    verify_all(&s, "store").await?;
    let c = dataset(&s, "/potentials/train-v1", vec![Member::snapshot(snaps[0].id), Member::snapshot(snaps[1].id)]).await?;
    let entries = in_tx!(s, |tx| datasets::members(&tx, c.snapshot).await?);
    let hashes: BTreeSet<String> = snaps.iter().map(|x| x.merkle_hash.clone().unwrap()).collect();
    assert!(entries.len() == 2 && entries.iter().all(|e| e.member_kind == "snapshot" && hashes.contains(&e.member_hash)), "The snapshot of train-v1 has two members, each a snapshot with its Merkle hash.");
    let p = publish_tag(&s, "/potentials/train-v1", "v1").await?;
    let v1 = in_tx!(s, |tx| datasets::resolve_ref(&tx, p.dataset_id, "v1", None).await?);
    assert!(p.status.as_deref() == Some("published") && p.merkle_hash.is_some() && v1 == p.id, "The snapshot is published, has a Merkle hash, and the tag ref v1 points to it.");
    let (mv, r, act) = train_model(&s, dataset_input("/potentials/train-v1", Some("v1"), "training"), "/models/potential-1").await?;
    assert_eq!(r.inputs[0]["snapshot"], json!(p.id), "A resolution exists that names the exact snapshot.");
    let (record, kind): (Option<Uuid>, String) = in_tx!(s, |tx| { let r = tx.q1("select run_record_version, kind from activity where id = $1", &[&act]).await?; (r.get(0), r.get(1)) });
    let rv_exists: bool = in_tx!(s, |tx| tx.qopt("select 1 from version where id = $1", &[&record.unwrap()]).await?.is_some());
    assert!(kind == "training" && rv_exists, "The run is an activity. The full run record is a catalogued version.");
    let lin = in_tx!(s, |tx| provenance::lineage(&tx, mv, &LineageQuery { direction: "ancestors".into(), ..Default::default() }).await?);
    let ids: BTreeSet<String> = lin["nodes"].as_array().unwrap().iter().map(|n| n["id"].as_str().unwrap().to_string()).collect();
    assert!(ids.contains(&act.to_string()) && ids.contains(&r.id.to_string()) && ids.contains(&snaps[0].id.to_string()) && ids.contains(&snaps[1].id.to_string()), "The model version has lineage to the run, to the resolution, and to A and B.");
    inst.clock.advance(chrono::Duration::days(365));
    let lin = in_tx!(s, |tx| provenance::lineage(&tx, mv, &LineageQuery { direction: "ancestors".into(), ..Default::default() }).await?);
    let nodes = lin["nodes"].as_array().unwrap();
    let a = nodes.iter().find(|n| n["type"] == "activity" && n["id"] == json!(act)).unwrap();
    let snap_hashes: BTreeSet<String> = nodes.iter().filter(|n| n["type"] == "snapshot").filter_map(|n| n["merkle_hash"].as_str().map(|x| x.to_string())).collect();
    assert!(a["code"] == "git:abc123" && a["image"] == "sha256:0f0f" && a["params"]["seed"] == 7 && hashes.is_subset(&snap_hashes), "The answer gives the code version, the image, the parameters, and the snapshots of A and B with their hashes.");
    let again = resolver::resolve_again(&s, r.id).await?;
    let before: BTreeSet<Uuid> = r.tasks.iter().map(|t| t.version).collect();
    let after: BTreeSet<Uuid> = again.tasks.iter().map(|t| t.version).collect();
    assert!(before == after && again.decision["newly_excluded"].as_array().unwrap().is_empty(), "The same members resolve. mudcat reports no excluded elements.");
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn uc2_02() -> Result<()> {
    let (_inst, s) = setup("uc2-02").await?;
    let ma = json!({"functional": "PBE", "hubbard_u": "no"});
    let mc = json!({"functional": "PBE", "hubbard_u": "yes"});
    computed_source(&s, "group-a", ma.clone()).await?;
    computed_source(&s, "group-c", mc.clone()).await?;
    let mut snaps = Vec::new();
    for (d, src) in [("a", "group-a"), ("c", "group-c")] {
        let regs: Vec<Register> = (0..3).map(|i| Register { path: format!("/potentials/{d}-files/s{i}"), content_hash: Some(format!("{d}{i}")), size: 1, source: Some(src.into()), ..Default::default() }).collect();
        let vs = in_tx!(s, |tx| namespace::register_batch(&mut tx, &regs).await?);
        dataset(&s, &format!("/potentials/{d}"), vs.iter().map(|v| Member::version(v.version_id)).collect()).await?;
        snaps.push(publish_tag(&s, &format!("/potentials/{d}"), "v1").await?.id);
    }
    let c = dataset(&s, "/potentials/train-v2", vec![Member::snapshot(snaps[0]), Member::snapshot(snaps[1])]).await?;
    let w = c.warnings.as_array().unwrap();
    assert!(w.len() == 1 && w[0]["methods"].as_array().unwrap().len() == 2, "The commit succeeds, and the snapshot has a warning that its members have two methods with no declared equivalence.");
    let r = in_tx!(s, |tx| datasets::publish(&mut tx, "/potentials/train-v2", "main", "v2").await);
    let e = r.err().expect("mudcat refuses the publication");
    assert!(e.code() == "methods-differ" && e.to_string().contains("hubbard_u") && e.to_string().contains("\"no\"") && e.to_string().contains("\"yes\""), "mudcat refuses the publication. The error names the two methods and the attribute that differs. ({e})");
    let lead = s.as_principal(Principal::user("lead"));
    let eq = in_tx!(lead, |tx| datasets::declare_equivalence(&mut tx, &ma, &mc, Some("energy alignment, script git:def456"), "the energies were aligned to one reference").await?);
    let row = in_tx!(s, |tx| tx.q1("select declared_by, at, reason, after from equivalence where id = $1", &[&eq]).await?);
    assert!(row.get::<_, String>(0) == "user:lead" && !row.get::<_, String>(2).is_empty() && row.get::<_, Option<String>>(3).is_some(), "The equivalence records who declared it, when, and the reason.");
    let p = in_tx!(s, |tx| datasets::publish(&mut tx, "/potentials/train-v2", "main", "v2").await?);
    assert!(p.status.as_deref() == Some("published") && p.equivalences.contains(&eq), "The publication succeeds. The snapshot refers to the declared equivalence.");
    let q = in_tx!(s, |tx| provenance::datasets_with_equivalences(&tx).await?);
    assert!(q.iter().any(|x| x["dataset"] == "/potentials/train-v2" && x["equivalences"].as_array().unwrap().contains(&json!(eq))), "The answer contains train-v2 and the equivalence.");
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn uc2_03() -> Result<()> {
    let (_inst, s) = setup("uc2-03").await?;
    // NOT ASSERTED at the stated scale (1,000,000 candidates, 1,200 similar, a bench of 5,000):
    // this test uses 2,000 candidates, 24 similar, and a bench of 100; the counts scale with them.
    let store = resource(&s, "store", "cloud", "managed", false).await?;
    acl_root(&s, "/m", Some(("store", "m/"))).await?;
    source(&s, "dft-runs", "computed", json!({}), &[("train", "research")]).await?;
    in_tx!(s, |tx| {
        leakage::register_criterion(&mut tx, "prototype", "domain-similarity", json!({"threshold": 0.95})).await?;
        metadata::declare_attribute(&mut tx, &metadata::AttributeSpec { namespace: "tags".into(), name: "use".into(), datatype: "string".into(), is_tag: true, permitted: Some(vec!["not-for-training".into()]), ..Default::default() }).await?;
    });
    let bench_files: Vec<(String, Vec<u8>, Option<String>)> = (0..100).map(|j| (format!("b{j:03}"), format!("bench {j}").into_bytes(), Some(format!("proto-{j}")))).collect();
    let bench = register_keyed(&s, &store, "/m/bench-files", "m/bench/", &bench_files, "dft-runs").await?;
    let bd = dataset(&s, "/m/bench", bench.iter().map(|v| Member::version(*v)).collect()).await?;
    publish_tag(&s, "/m/bench", "v1").await?;
    in_tx!(s, |tx| {
        metadata::tag(&mut tx, bd.dataset, "use", "not-for-training").await?;
        metadata::write(&mut tx, &metadata::WriteMetadata { entity: bd.dataset, namespace: "evaluation".into(), payload: json!({"leakage_criteria": ["prototype"]}), ..Default::default() }).await?;
    });
    let cand_files: Vec<(String, Vec<u8>, Option<String>)> = (0..2000).map(|i| (format!("c{i:04}"), format!("candidate {i}").into_bytes(), Some(if i < 24 { format!("proto-{i}") } else { format!("cand-{i}") }))).collect();
    let cands = register_keyed(&s, &store, "/m/cand-files", "m/cand/", &cand_files, "dft-runs").await?;
    dataset(&s, "/m/candidates", cands.iter().map(|v| Member::version(*v)).collect()).await?;
    verify_all(&s, "store").await?;
    let similar: BTreeSet<Uuid> = cands[..24].iter().copied().collect();

    let sel = in_tx!(s, |tx| datasets::create_selection(&mut tx, "/m/candidates", "train-clean", &json!({"not_similar": {"to": "/m/bench", "criterion": "prototype"}})).await?);
    assert_eq!(sel["elements"], 1976, "The selection has 998,800 elements (scaled: 2,000 - 24).");
    let a = in_tx!(s, |tx| leakage::check(&mut tx, &json!({"selection": "/m/candidates#train-clean"}), &json!({"dataset": "/m/bench"}), "prototype").await?);
    assert!(a["result"] == "satisfied" && a["criterion_version"] == 1 && a["evidence"].is_object(), "A leakage assertion exists with the result satisfied, the criterion version, and the evidence.");
    let more: Vec<(String, Vec<u8>, Option<String>)> = (0..50).map(|i| (format!("n{i:02}"), format!("new {i}").into_bytes(), Some(if i < 2 { format!("proto-{}", 24 + i) } else { format!("new-{i}") }))).collect();
    let new = register_keyed(&s, &store, "/m/cand-files", "m/cand/", &more, "dft-runs").await?;
    verify_all(&s, "store").await?;
    let c = commit(&s, DatasetChange { dataset: "/m/candidates".into(), add: new.iter().map(|v| Member::version(*v)).collect(), ..Default::default() }).await;
    assert!(c.is_ok(), "The commit succeeds.");
    let r = resolver::resolve(
        &s,
        &train_request(
            vec![
                Input { dataset: Some("/m/candidates".into()), reference: Some("main".into()), selection: Some(json!({"name": "/m/candidates#train-clean"})), role: "training".into(), ..Default::default() },
                dataset_input("/m/bench", Some("v1"), "test"),
            ],
            "train",
            "research",
        ),
    )
    .await?;
    let got: BTreeSet<Uuid> = r.tasks.iter().map(|t| t.version).collect();
    let training: BTreeSet<Uuid> = got.difference(&bench.iter().copied().collect()).copied().collect();
    assert!(!got.contains(&new[0]) && !got.contains(&new[1]) && similar.is_disjoint(&got) && training.len() == 2024, "The 2 similar structures are not in the resolved training input, because the selection is a predicate.");
    // a model trained on all of candidates with no selection
    let (model, _, _) = train_model(&s, dataset_input("/m/candidates", Some("main"), "training"), "/models/m-all").await?;
    let ev = in_tx!(s, |tx| leakage::evaluate(&mut tx, model, "/m/bench", json!({"mae": 0.02})).await?);
    let asr = &ev["assertions"][0];
    assert!(ev["assertions"].as_array().unwrap().len() == 1, "Before mudcat accepts the result, it checks the criterion against the training inputs of the model.");
    assert!(asr["result"] == "violated" && asr["related"] == 26, "The leakage assertion has the result violated, with the number of related elements. ({asr})");
    assert!(ev["leakage"] == "violated" && ev["evaluation_result"].is_string(), "The evaluation result carries the violated assertion.");
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn uc2_04() -> Result<()> {
    let (inst, s) = setup("uc2-04").await?;
    let hpc = resource_spec(&s, ResourceSpec { name: "hpc".into(), location: "hpc".into(), jurisdiction: "us".into(), mode: "external".into(), protocol: "posix".into(), egress_allowed: true, ..Default::default() }).await?;
    let cloud = resource(&s, "cloud-bucket", "cloud", "managed", false).await?;
    acl_root(&s, "/run/raw", Some(("hpc", "raw/"))).await?;
    acl_root(&s, "/run/curated", Some(("cloud-bucket", "cur/"))).await?;
    for p in ["/run/raw", "/run/curated"] {
        grant_collection(&s, p, "reader", "group:physics#member").await?;
    }
    grant(&s, "group", "physics", "member", "user:researcher").await?;
    source(&s, "sim", "computed", json!({}), &[("train", "research")]).await?;
    let raw_files: Vec<(String, Vec<u8>)> = (0..5).map(|i| (format!("x{i}.h5"), file_bytes("opaque", i, "raw"))).collect();
    let cur_files: Vec<(String, Vec<u8>)> = (0..5).map(|i| (format!("x{i}.zarr"), file_bytes("opaque", i, "cur"))).collect();
    let raw = register_files(&s, &hpc, "/run/raw", "raw/", &raw_files, Some("sim"), None).await?;
    let cur = register_files(&s, &cloud, "/run/curated", "cur/", &cur_files, Some("sim"), None).await?;
    verify_all(&s, "hpc").await?;
    verify_all(&s, "cloud-bucket").await?;
    let snap = dataset(&s, "/run/raw/S", raw.iter().map(|r| Member::version(r.version_id)).collect()).await?.snapshot;
    let raw_ids: Vec<Uuid> = raw.iter().map(|r| r.version_id).collect();
    let cur_ids: Vec<Uuid> = cur.iter().map(|r| r.version_id).collect();
    in_tx!(s, |tx| {
        datasets::add_representation(&mut tx, snap, "raw", &raw_ids, json!({}), None).await?;
        datasets::add_representation(&mut tx, snap, "curated", &cur_ids, json!({"to": "analysis-ready zarr"}), None).await?;
    });
    let researcher = s.as_principal(Principal::user("researcher"));
    let mut req = train_request(vec![Input { snapshot: Some(snap), role: "training".into(), ..Default::default() }], "train", "research");
    req.place = Some("cloud".into());
    let r = resolver::resolve(&researcher, &req).await?;
    let got: BTreeSet<Uuid> = r.tasks.iter().map(|t| t.version).collect();
    assert!(got == cur_ids.iter().copied().collect() && r.tasks.iter().all(|t| t.resource == "cloud-bucket"), "The plan uses the representation \"curated\" and the cloud replicas.");
    assert!(!r.credentials.is_empty() && r.credentials.iter().all(|c| matches!(c.credential, Credential::Prefix { .. })), "The credentials have a prefix scope.");

    let mut req2 = train_request(vec![Input { snapshot: Some(snap), representation: Some("raw".into()), role: "training".into(), ..Default::default() }], "train", "research");
    req2.place = Some("hpc".into());
    req2.mode = Some("before-submission".into());
    let r2 = resolver::resolve(&researcher, &req2).await?;
    let tenant = s.tenant_id()?;
    let manifest = inst.manifests.read(tenant, r2.id)?;
    assert!(r2.manifest_file.is_some() && manifest.len() == 5 && manifest.iter().all(|m| m["path"].as_str().unwrap().starts_with('/')), "The plan is a manifest file with POSIX paths. The job can read it with no connection to mudcat.");
    assert!(r2.credentials.iter().all(|c| c.credential == Credential::Site) && r2.notices.iter().any(|n| n["kind"] == "site-enforces-access"), "mudcat issues no storage credentials. The result says that the site enforces access.");
    // the job reads by path, and writes its run record to a spool directory
    let mut bytes = 0;
    for m in &manifest {
        bytes += inst.store.get("hpc-job", &format!("file://{}", m["path"].as_str().unwrap()), &Credential::Site, 0)?.len();
    }
    assert!(bytes > 0, "The job reads the files by path.");
    let spool = inst.data_dir.join("spool");
    std::fs::create_dir_all(&spool)?;
    let record = json!({"resolution": r2.id, "code": "git:hpc1", "location": "hpc", "tasks": manifest.len(), "status": "ok"});
    std::fs::write(spool.join("run-1.json"), record.to_string())?;
    assert!(spool.join("run-1.json").exists(), "The job writes its run record to a spool directory.");
    // the site agent records the run from the spool
    let spooled: Value = serde_json::from_slice(&std::fs::read(spool.join("run-1.json"))?)?;
    let act = in_tx!(s, |tx| provenance::record_run(&mut tx, &RunSpec { kind: "training".into(), code: spooled["code"].as_str().map(|x| x.to_string()), location: Some("hpc".into()), resolution: spooled["resolution"].as_str().and_then(|x| x.parse().ok()), capture: Some("declared".into()), record: spooled.clone(), ..Default::default() }).await?);
    let row = in_tx!(s, |tx| tx.q1("select capture, location from activity where id = $1", &[&act]).await?);
    assert!(row.get::<_, String>(0) == "declared" && row.get::<_, Option<String>>(1).as_deref() == Some("hpc"), "The run exists in the catalog with the capture mode declared, and with its location at the HPC centre.");
    // the inventory of the HPC file system
    inst.store.put("lab", &hpc.bucket, "scratch/dark.dat", b"a file that no replica describes")?;
    inst.store.delete(&hpc.bucket, "raw/x0.h5", true)?;
    inst.store.set_group(&hpc.bucket, "raw/x1.h5", "chemistry");
    let rep = placement::report_inventory(&s, "hpc", "agent-hpc").await?;
    assert!(rep["dark_data"].as_array().unwrap().iter().any(|d| d["url"].as_str().unwrap().ends_with("scratch/dark.dat")) && rep["lost"].as_array().unwrap().len() == 1, "mudcat reports each file that no replica record describes, and each replica record with no file.");
    assert!(rep["group_mismatches"].as_array().unwrap().iter().any(|m| m["unix_group"] == "chemistry"), "mudcat reports files whose Unix group does not agree with the access control of their collection.");
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn uc2_05() -> Result<()> {
    let (inst, s) = setup("uc2-05").await?;
    let store = resource(&s, "store", "cloud", "managed", false).await?;
    acl_root(&s, "/weather", Some(("store", "wx/"))).await?;
    source(&s, "era", "computed", json!({}), &[("train", "research")]).await?;
    in_tx!(s, |tx| leakage::register_criterion(&mut tx, "overlap", "time-range-overlap", json!({})).await?);
    let version_of = |years: i64, month: usize| -> (Register, Vec<u8>) {
        let data = format!("reanalysis {years} years, month {month} {}", "z".repeat(100 * (years as usize) + month)).into_bytes();
        let structure = json!({"kind": "array", "dimensions": [{"name": "time", "start": 1979, "size": years}, {"name": "level", "size": 37}, {"name": "latitude", "size": 721}, {"name": "longitude", "size": 1440}], "chunk": [10], "element_count": years * 37});
        let key = format!("wx/reanalysis-m{month}.zarr");
        (Register { path: "/weather/data/reanalysis".into(), content_hash: Some(sha256_hex(&data)), size: data.len() as i64, structure: Some(structure), source: Some("era".into()), replica: Some(ReplicaSpec { resource: "store".into(), key: Some(key), ..Default::default() }), ..Default::default() }, data)
    };
    let put = |reg: &Register, data: &[u8]| -> Result<()> {
        inst.store.put("producer", &store.bucket, reg.replica.as_ref().unwrap().key.as_ref().unwrap(), data)
    };
    let (r0, d0) = version_of(41, 0);
    put(&r0, &d0)?;
    let v0 = in_tx!(s, |tx| namespace::register_batch(&mut tx, &[r0.clone()]).await?)[0].version_id;
    in_tx!(s, |tx| datasets::create_dataset(&mut tx, "/weather/reanalysis", &DatasetOptions { is_series: true, ..Default::default() }).await?);
    let mut snaps = vec![commit(&s, DatasetChange { dataset: "/weather/reanalysis".into(), add: vec![Member::version(v0)], ..Default::default() }).await?.snapshot];
    let mut prev = v0;
    let mut first_summary = Value::Null;
    for month in 1..=4 {
        inst.clock.advance(chrono::Duration::days(30));
        let (r, d) = version_of(41 + month as i64, month);
        put(&r, &d)?;
        let v = in_tx!(s, |tx| namespace::register_batch(&mut tx, &[r.clone()]).await?)[0].version_id;
        let c = commit(&s, DatasetChange { dataset: "/weather/reanalysis".into(), requirement: Some(*snaps.last().unwrap()), replace: vec![(prev, Member::version(v))], ..Default::default() }).await?;
        if month == 1 {
            first_summary = c.summary.clone();
        }
        snaps.push(c.snapshot);
        prev = v;
    }
    verify_all(&s, "store").await?;
    assert!(first_summary["operation"] == "replace" && first_summary["added"] == 1 && first_summary["removed"] == 1 && first_summary["added_bytes"].as_i64().unwrap() > 0, "A new snapshot exists. Its change summary says one member replaced, and gives the added bytes. ({first_summary})");
    let lists: i64 = in_tx!(s, |tx| tx.q1("select count(*) from version where element_manifest is not null", &[]).await?.get(0));
    assert_eq!(lists, 0, "mudcat made no list of array elements.");
    let input = Input { dataset: Some("/weather/reanalysis".into()), reference: Some("main".into()), selector: Some(json!({"time": [1979, 2018]})), role: "training".into(), ..Default::default() };
    let (_model, r, _) = train_model(&s, input, "/models/weather-1").await?;
    assert_eq!(r.inputs[0]["selector"], json!({"time": [1979, 2018]}), "The resolution records the selector as the element-level input.");
    assert!(r.tasks.len() == 4 && r.tasks.iter().all(|t| t.addressing == "selector" && t.selector.as_ref().unwrap()["chunk"].is_array() && t.elements.is_none()), "The plan lists chunks, not elements.");
    let model = _model;
    let a = in_tx!(s, |tx| leakage::check(&mut tx, &json!({"model": model}), &json!({"dataset": "/weather/reanalysis", "selector": {"time": [2020, 2020]}}), "overlap").await?);
    assert!(a["result"] == "satisfied" && a["evidence"]["method"] == "selectors" && a["evidence"]["bytes_scanned"] == 0, "The result is satisfied. mudcat computed it from the two selectors, with no scan of data.");
    let b = in_tx!(s, |tx| leakage::check(&mut tx, &json!({"model": model}), &json!({"dataset": "/weather/reanalysis", "selector": {"time": [2018, 2019]}}), "overlap").await?);
    assert!(b["result"] == "violated" && b["evidence"]["overlap"] == json!([[2018.0, 2018.0]]), "The result is violated, with the range that overlaps. ({b})");
    let as_of = inst.now() - chrono::Duration::days(90);
    let reader = s.clone();
    let rr = resolver::resolve(&reader, &resolver::Request { inputs: vec![Input { dataset: Some("/weather/reanalysis".into()), as_of: Some(as_of), role: "analysis".into(), ..Default::default() }], ..Default::default() }).await?;
    assert_eq!(rr.inputs[0]["snapshot"], json!(snaps[1]), "The result is the snapshot of that month.");
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn uc2_06() -> Result<()> {
    let (_inst, s) = setup("uc2-06").await?;
    let store = resource(&s, "store", "cloud", "managed", false).await?;
    acl_root(&s, "/upstream", Some(("store", "up/"))).await?;
    source(&s, "upstream-group", "computed", json!({}), &[("train", "research")]).await?;
    let x1 = register_files(&s, &store, "/upstream", "up/", &[("X".to_string(), b"structure X, version one".to_vec())], Some("upstream-group"), None).await?[0].clone();
    verify_all(&s, "store").await?;
    in_tx!(s, |tx| datasets::create_dataset(&mut tx, "/potentials/train-v1", &DatasetOptions { follow: Some("/upstream".into()), ..Default::default() }).await?);
    in_tx!(s, |tx| datasets::apply_follow_rule(&mut tx, "/potentials/train-v1").await?);
    let v1 = publish_tag(&s, "/potentials/train-v1", "v1").await?;
    let (m1, _, _) = train_model(&s, dataset_input("/potentials/train-v1", Some("v1"), "training"), "/models/M1").await?;
    // the upstream group corrects X
    let x2 = register_files(&s, &store, "/upstream", "up/v2/", &[("X".to_string(), b"structure X, corrected".to_vec())], Some("upstream-group"), None).await?[0].clone();
    verify_all(&s, "store").await?;
    let still = resolver::resolve(&s, &resolver::Request { inputs: vec![Input { version: Some(x1.version_id), role: "analysis".into(), ..Default::default() }], ..Default::default() }).await?;
    assert!(x2.number == 2 && x2.artifact_id == x1.artifact_id && still.tasks[0].version == x1.version_id, "X has version 2. Version 1 still resolves.");
    let c = in_tx!(s, |tx| datasets::apply_follow_rule(&mut tx, "/potentials/train-v1").await?).expect("a commit by the follow rule");
    let main_members: BTreeSet<Uuid> = in_tx!(s, |tx| datasets::members(&tx, c.snapshot).await?.iter().map(|e| e.member_id).collect());
    assert!(main_members.contains(&x2.version_id) && !main_members.contains(&x1.version_id), "A new snapshot on main contains X version 2.");
    let (v1_now, v1_members) = in_tx!(s, |tx| {
        let snap = datasets::resolve_ref(&tx, v1.dataset_id, "v1", None).await?;
        (snap, datasets::members(&tx, snap).await?.iter().map(|e| e.member_id).collect::<BTreeSet<Uuid>>())
    });
    assert!(v1_now == v1.id && v1_members.contains(&x1.version_id) && v1_members.len() == 1, "The tag ref v1 and its snapshot did not change.");
    let ch = in_tx!(s, |tx| datasets::changes(&tx, v1.id, c.snapshot, None, 100).await?);
    assert!(ch["removed"][0]["member"] == json!(x1.version_id) && ch["added"][0]["member"] == json!(x2.version_id), "The answer lists X version 1 as removed and X version 2 as added.");
    let newer = in_tx!(s, |tx| provenance::models_with_newer_inputs(&tx).await?);
    assert!(newer.iter().any(|m| m["model_version"] == json!(m1) && m["changed_inputs"][0]["input"] == "/upstream/X"), "The answer contains M1, with X as the input that changed. ({newer:?})");
    let r = commit(&s, DatasetChange { dataset: "/potentials/train-v1".into(), branch: "v1".into(), add: vec![Member::version(x2.version_id)], ..Default::default() }).await;
    assert!(matches!(&r, Err(e) if e.code() == "tag-ref-immutable"), "mudcat rejects the commit, because v1 is a tag ref and its snapshot is published.");
    let _ = head(&s, "/potentials/train-v1").await?;
    Ok(())
}
