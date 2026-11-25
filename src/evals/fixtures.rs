//! Helpers that build the start state of a variant.

use super::Ctx;
use crate::datasets::{self, DatasetChange, Member};
use crate::error::{Error, Result};
use crate::instance::{sha256_hex, Session};
use crate::namespace::{self, Register, ReplicaSpec, Resource, ResourceSpec};
use crate::policy::{self, SourceSpec};
use crate::resolver::{self, Resolution};
use serde_json::{json, Value};
use uuid::Uuid;

/// Run a block in a transaction of a session, and commit it.
#[macro_export]
macro_rules! in_tx {
    ($sess:expr, |$tx:ident| $body:expr) => {{
        let __s = &$sess;
        let mut __c = __s.client().await?;
        #[allow(unused_mut)]
        let mut $tx = __s.begin(&mut __c).await?;
        let __r = $body;
        $tx.commit().await?;
        __r
    }};
}

pub async fn resource(s: &Session, name: &str, location: &str, mode: &str, versioned: bool) -> Result<Resource> {
    Ok(in_tx!(s, |tx| namespace::create_resource(&mut tx, ResourceSpec {
        name: name.into(), location: location.into(), jurisdiction: location.into(), mode: mode.into(), protocol: "s3".into(),
        versioned, egress_allowed: true, bucket: None, attrs: json!({})
    }).await?))
}

pub async fn resource_spec(s: &Session, spec: ResourceSpec) -> Result<Resource> {
    Ok(in_tx!(s, |tx| namespace::create_resource(&mut tx, spec).await?))
}

/// A collection that is an ACL root, mounted on its own prefix of a resource.
pub async fn acl_root(s: &Session, path: &str, resource: Option<(&str, &str)>) -> Result<namespace::Collection> {
    Ok(in_tx!(s, |tx| {
        let c = namespace::set_acl_root(&mut tx, path).await?;
        if let Some((r, p)) = resource {
            namespace::mount(&mut tx, path, r, p).await?;
        }
        let _ = c;
        namespace::collection(&tx, path).await?
    }))
}

pub async fn grant_collection(s: &Session, path: &str, relation: &str, subject: &str) -> Result<()> {
    in_tx!(s, |tx| {
        let c = namespace::collection(&tx, path).await?;
        crate::authz::grant(&mut tx, "collection", &c.acl_root.to_string(), relation, subject, None).await?
    });
    Ok(())
}

pub async fn grant(s: &Session, ot: &str, oid: &str, rel: &str, subject: &str) -> Result<()> {
    in_tx!(s, |tx| crate::authz::grant(&mut tx, ot, oid, rel, subject, None).await?);
    Ok(())
}

pub async fn revoke(s: &Session, ot: &str, oid: &str, rel: &str, subject: &str) -> Result<()> {
    in_tx!(s, |tx| crate::authz::revoke(&mut tx, ot, oid, rel, subject).await?);
    Ok(())
}

pub async fn source(s: &Session, name: &str, kind: &str, attrs: Value, grants: &[(&str, &str)]) -> Result<Uuid> {
    Ok(in_tx!(s, |tx| {
        let id = policy::create_source(&mut tx, &SourceSpec { name: name.into(), kind: kind.into(), attrs, method: None }).await?;
        for (a, p) in grants {
            policy::grant_usage(&mut tx, id, a, p, None, Some("fixture"), None).await?;
        }
        id
    }))
}

/// The bytes of a simulated file of a structure kind.
pub fn file_bytes(kind: &str, i: usize, salt: &str) -> Vec<u8> {
    match kind {
        "records" => format!(
            "{{\"kind\":\"records\",\"fields\":[{{\"name\":\"energy\",\"datatype\":\"double\",\"unit\":\"eV\"}},{{\"name\":\"label\",\"datatype\":\"string\"}}]}}\n{{\"id\":\"r{i}-0\",\"energy\":{i}.1,\"s\":\"{salt}\"}}\n{{\"id\":\"r{i}-1\",\"energy\":{i}.2}}\n{{\"id\":\"r{i}-2\",\"energy\":{i}.3}}\n"
        )
        .into_bytes(),
        "container" => format!("{{\"kind\":\"container\"}}\n{{\"id\":\"m{i}/a.json\",\"s\":\"{salt}\"}}\n{{\"id\":\"m{i}/b.json\"}}\n").into_bytes(),
        "stream" => format!("{{\"kind\":\"stream\",\"duration_s\":10.0,\"frame_rate\":30,\"channels\":[{{\"name\":\"camera\"}}]}}\nframes-{i}-{salt}-{}\n", "x".repeat(200)).into_bytes(),
        "array" => format!("{{\"kind\":\"array\",\"dimensions\":[{{\"name\":\"time\",\"start\":0,\"size\":100}}],\"chunk\":[25],\"element_count\":400}}\nchunks-{i}-{salt}-{}\n", "y".repeat(200)).into_bytes(),
        _ => format!("opaque-{i}-{salt}-{}", "z".repeat(100 + i % 50)).into_bytes(),
    }
}

/// Write files to a bucket, and register them where they are, as a producer does on managed
/// storage. Returns the versions.
pub async fn register_files(s: &Session, resource: &Resource, coll: &str, prefix: &str, files: &[(String, Vec<u8>)], source: Option<&str>, replica_state: Option<&str>) -> Result<Vec<namespace::Registered>> {
    let mut regs = Vec::with_capacity(files.len());
    for (name, data) in files {
        let key = format!("{prefix}{name}");
        s.inst.store.put("producer", &resource.bucket, &key, data)?;
        let (structure, elements, stats) = crate::plugins::extract(name, data);
        regs.push(Register {
            path: format!("{}/{}", coll.trim_end_matches('/'), name),
            content_hash: Some(sha256_hex(data)),
            size: data.len() as i64,
            structure: Some(structure),
            elements,
            stats: Some(stats),
            source: source.map(|s| s.to_string()),
            replica: Some(ReplicaSpec { resource: resource.name.clone(), key: Some(key), state: replica_state.map(|s| s.to_string()), ..Default::default() }),
            ..Default::default()
        });
    }
    let mut out = Vec::new();
    for chunk in regs.chunks(2000) {
        out.extend(in_tx!(s, |tx| namespace::register_batch(&mut tx, chunk).await?));
    }
    Ok(out)
}

/// Verify every replica on a resource with a site agent.
pub async fn verify_all(s: &Session, resource: &str) -> Result<usize> {
    in_tx!(s, |tx| crate::placement::enqueue_verify(&mut tx, resource, true).await?);
    let agent = crate::placement::SiteAgent::new(&format!("agent-{resource}"), resource);
    let mut n = 0;
    loop {
        let k = agent.run(s, 5000, crate::placement::AgentFault::None).await?;
        n += k;
        if k == 0 {
            break;
        }
    }
    Ok(n)
}

pub async fn run_agent(s: &Session, resource: &str) -> Result<usize> {
    let agent = crate::placement::SiteAgent::new(&format!("agent-{resource}"), resource);
    let mut n = 0;
    loop {
        let k = agent.run(s, 5000, crate::placement::AgentFault::None).await?;
        n += k;
        if k == 0 {
            break;
        }
    }
    Ok(n)
}

pub async fn dataset(s: &Session, path: &str, members: Vec<Member>) -> Result<datasets::Committed> {
    Ok(in_tx!(s, |tx| {
        datasets::create_dataset(&mut tx, path, &Default::default()).await?;
        datasets::commit(&mut tx, &[DatasetChange { dataset: path.into(), add: members, ..Default::default() }]).await?.remove(0)
    }))
}

pub async fn commit(s: &Session, ch: DatasetChange) -> Result<datasets::Committed> {
    Ok(in_tx!(s, |tx| datasets::commit(&mut tx, &[ch]).await?).remove(0))
}

/// Publish the head of main with a tag ref, and record its members and hash for INV-2.
pub async fn publish(ctx: &mut Ctx, s: &Session, path: &str, tag: &str) -> Result<datasets::SnapshotInfo> {
    let info = in_tx!(s, |tx| datasets::publish(&mut tx, path, "main", tag).await?);
    record_published(ctx, info.id).await?;
    Ok(info)
}

pub async fn record_published(ctx: &mut Ctx, snap: Uuid) -> Result<()> {
    let s = ctx.admin.as_principal(crate::Principal::system());
    let (members, hash) = in_tx!(s, |tx| {
        let m = datasets::members(&tx, snap).await?;
        let i = datasets::snapshot(&tx, snap).await?;
        (m.iter().map(|e| e.member_id).collect(), i.merkle_hash.unwrap_or_default())
    });
    ctx.record.published.insert(snap, (members, hash));
    Ok(())
}

/// Resolve, and record the resolution for the invariants.
pub async fn resolve(ctx: &mut Ctx, s: &Session, req: &resolver::Request) -> std::result::Result<Resolution, Error> {
    let r = resolver::resolve(s, req).await;
    if let Ok(res) = &r {
        ctx.record.resolutions.push(res.clone());
        ctx.record.principals.insert(res.id, s.clone());
    }
    r
}

pub async fn resolve_again(ctx: &mut Ctx, s: &Session, id: Uuid) -> std::result::Result<Resolution, Error> {
    let r = resolver::resolve_again(s, id).await;
    if let Ok(res) = &r {
        ctx.record.resolutions.push(res.clone());
        ctx.record.principals.insert(res.id, s.clone());
    }
    r
}

pub fn dataset_input(path: &str, r: Option<&str>, role: &str) -> resolver::Input {
    resolver::Input { dataset: Some(path.into()), reference: r.map(|x| x.to_string()), role: role.into(), ..Default::default() }
}

pub fn train_request(inputs: Vec<resolver::Input>, action: &str, purpose: &str) -> resolver::Request {
    resolver::Request { inputs, action: Some(action.into()), purpose: Some(purpose.into()), ..Default::default() }
}

/// The number of audit entries.
pub async fn audit_count(s: &Session, point: &str, decision: &str) -> Result<i64> {
    let sys = s.as_principal(crate::Principal::system());
    Ok(in_tx!(sys, |tx| tx.q1("select count(*) from audit where point = $1 and decision = $2", &[&point, &decision]).await?.get::<_, i64>(0)))
}

pub fn err_code(e: &Error) -> String {
    e.code().to_string()
}
