//! Drivers for the task templates of use case 5: a shared facility with many small groups.

use super::fixtures::*;
use super::Ctx;
use crate::datasets::{self, DatasetChange, Member};
use crate::error::{Error, Result};
use crate::in_tx;
use crate::instance::Principal;
use crate::namespace::{self, Register};
use crate::{auth, authz, resolver, search, stream};
use http_body_util::BodyExt;
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use tower::ServiceExt;
use uuid::Uuid;

async fn http(router: &axum::Router, method: &str, uri: &str, host: &str, token: Option<&str>, body: Option<Value>) -> Result<(u16, Value, f64)> {
    let mut b = axum::http::Request::builder().method(method).uri(uri).header("host", host);
    if let Some(t) = token {
        b = b.header("authorization", format!("Bearer {t}"));
    }
    let req = match body {
        Some(v) => b.header("content-type", "application/json").body(axum::body::Body::from(v.to_string())),
        None => b.body(axum::body::Body::empty()),
    }
    .map_err(|e| Error::invalid(e.to_string()))?;
    let t0 = std::time::Instant::now();
    let resp = router.clone().oneshot(req).await.map_err(|e| Error::invalid(e.to_string()))?;
    let status = resp.status().as_u16();
    let bytes = resp.into_body().collect().await.map_err(|e| Error::invalid(e.to_string()))?.to_bytes();
    let secs = t0.elapsed().as_secs_f64();
    Ok((status, serde_json::from_slice(&bytes).unwrap_or(Value::Null), secs))
}

fn uuids(v: &Value, out: &mut BTreeSet<Uuid>) {
    match v {
        Value::String(s) => {
            if let Ok(u) = s.parse::<Uuid>() {
                out.insert(u);
            }
            // UUIDs inside text, for example in an MCP text content
            for w in s.split(|c: char| !(c.is_ascii_hexdigit() || c == '-')) {
                if w.len() == 36 {
                    if let Ok(u) = w.parse::<Uuid>() {
                        out.insert(u);
                    }
                }
            }
        }
        Value::Array(a) => a.iter().for_each(|x| uuids(x, out)),
        Value::Object(o) => o.values().for_each(|x| uuids(x, out)),
        _ => {}
    }
}

struct TenantData {
    artifact: Uuid,
    version: Uuid,
    dataset: Uuid,
    subscription: Uuid,
}

/// The data of one tenant for T5.1: the same paths, identifiers and checksums in each tenant.
async fn tenant_data(s: &crate::Session, inst: &Arc<crate::Instance>) -> Result<TenantData> {
    let store = resource(s, "store", "eu", "managed", false).await?;
    acl_root(s, "/scans", Some(("store", "scans/"))).await?;
    grant_collection(s, "/scans", "reader", "user:alice").await?;
    source(s, "beamline", "instrument", json!({}), &[("use", "research")]).await?;
    let files = vec![("s1.h5".to_string(), b"the same bytes in each tenant".to_vec())];
    let r = register_files(s, &store, "/scans", "scans/", &files, Some("beamline"), None).await?[0].clone();
    verify_all(s, "store").await?;
    let c = dataset(s, "/scans/session-1", vec![Member::version(r.version_id)]).await?;
    in_tx!(s, |tx| crate::metadata::write(&mut tx, &crate::metadata::WriteMetadata { entity: r.version_id, namespace: "dc".into(), payload: json!({"description": "scan s1 of the session"}), ..Default::default() }).await?);
    search::build(s).await?;
    let sub = stream::subscribe(&s.as_principal(Principal::user("alice")), "/scans", "change").await?;
    let _ = inst;
    Ok(TenantData { artifact: r.artifact_id, version: r.version_id, dataset: c.dataset, subscription: sub })
}

/// T5.1: a user of one tenant tries each interface, and gets the data of that tenant only.
pub async fn tenant_isolation(ctx: &mut Ctx) -> Result<()> {
    let interface = ctx.p("interface");
    let with_token = ctx.is("token", "valid-for-own-tenant");
    let other_endpoint = ctx.is("endpoint", "other-tenant");
    let other_target = ctx.is("target", "id-of-other-tenant");
    let inst = ctx.inst.clone();
    let other = crate::tenants::create_tenant(&inst, &format!("{}-b", ctx.tenant.name), "user:admin-b").await?;
    let mine = tenant_data(&ctx.admin.clone(), &inst).await?;
    let theirs = tenant_data(&inst.session(other.id, Principal::user("admin-b")), &inst).await?;
    ctx.step().await?;
    let router = crate::api::router(inst.clone());
    let token = auth::issue(&inst, ctx.tenant.id, &Principal::user("alice"), chrono::Duration::hours(1));
    let host = if other_endpoint { other.endpoint.clone() } else { ctx.tenant.endpoint.clone() };
    let tok = if with_token { Some(token.as_str()) } else { None };
    let request = |target: &TenantData, which: &str| -> (String, String, Option<Value>) {
        let id = match which {
            "artifact" => target.artifact,
            "version" => target.version,
            _ => target.dataset,
        };
        match interface.as_str() {
            "query-by-id" => ("GET".into(), format!("/entities/{id}"), None),
            "search" => ("POST".into(), "/search".into(), Some(if other_target { json!({"similar_to": target.version}) } else { json!({"text": "scan s1 session"}) })),
            "change-stream" => ("GET".into(), if other_target { format!("/stream?from=0&subscription={}", target.subscription) } else { "/stream?from=0".into() }, None),
            "resolve" => ("POST".into(), "/resolve".into(), Some(json!({"inputs": [{"version": target.version, "role": "test"}]}))),
            "lineage" => ("GET".into(), format!("/lineage/{}", target.version), None),
            "iceberg-gateway" => ("GET".into(), format!("/iceberg/v1/namespaces/scans/tables/{}", target.dataset), None),
            _ => ("POST".into(), "/mcp".into(), Some(json!({"jsonrpc": "2.0", "id": 1, "method": "tools/call", "params": {"name": "describe", "arguments": {"id": id}}}))),
        }
    };
    let target = if other_target { &theirs } else { &mine };
    let (m, uri, body) = request(target, "artifact");
    let (status, resp, secs) = http(&router, &m, &uri, &host, tok, body.clone()).await?;
    let mcp_error = resp.get("error").and_then(|e| e.get("data")).cloned();
    let code = mcp_error.as_ref().and_then(|d| d["error"].as_str()).or_else(|| resp["error"].as_str()).unwrap_or("").to_string();
    let message = mcp_error.as_ref().and_then(|d| d["message"].as_str()).or_else(|| resp["message"].as_str()).unwrap_or("").to_string();
    // with the search interface, a target of another tenant is simply not there
    let search_empty = interface == "search" && other_target && resp["hits"].as_array().map(|h| h.is_empty()).unwrap_or(false) && status == 200;
    let outcome = if status == 401 && message.contains("no token") {
        "refused-no-token"
    } else if status == 401 && message.contains("audience") {
        "refused-wrong-audience"
    } else if code == "not-found" || search_empty {
        "not-found"
    } else if status == 200 && code.is_empty() {
        "own-data-only"
    } else {
        "unexpected"
    };
    ctx.outcome(outcome);
    // the IDs in the answer
    let mut ids = BTreeSet::new();
    uuids(&resp, &mut ids);
    let ids_v: Vec<Uuid> = ids.iter().copied().collect();
    let os = inst.session(other.id, Principal::system());
    let foreign: i64 = in_tx!(os, |tx| tx.q1("select count(*) from entity where id = any($1)", &[&ids_v]).await?.get(0));
    let ms = ctx.admin.as_principal(Principal::system());
    let own: Vec<Uuid> = in_tx!(ms, |tx| tx.q("select id from entity where id = any($1)", &[&ids_v]).await?.iter().map(|r| r.get(0)).collect());
    ctx.record.answers.push((ctx.tenant.id, own.clone()));
    ctx.check("Each entity in the answer through", foreign == 0, format!("{foreign} IDs of the other tenant in the answer"));
    let text = resp.to_string();
    let own_present = match interface.as_str() {
        "resolve" | "lineage" => text.contains(&mine.version.to_string()),
        "iceberg-gateway" => text.contains(&mine.dataset.to_string()),
        "search" => text.matches(&mine.artifact.to_string()).count() == 1,
        _ => text.contains(&mine.artifact.to_string()) || text.contains(&mine.version.to_string()),
    };
    ctx.check("The answer has the artifact of the own tenant one time", own_present && !text.contains(&theirs.artifact.to_string()), "");
    if outcome == "not-found" {
        // compare with an ID that does not exist: status, body and time
        let ghost = TenantData { artifact: Uuid::now_v7(), version: Uuid::now_v7(), dataset: Uuid::now_v7(), subscription: Uuid::now_v7() };
        let (m2, uri2, body2) = request(&ghost, "artifact");
        let mut t_other = vec![secs];
        let mut t_ghost = vec![];
        let mut same = true;
        for _ in 0..3 {
            let (s2, r2, t2) = http(&router, &m2, &uri2, &host, tok, body2.clone()).await?;
            let (s1, r1, t1) = http(&router, &m, &uri, &host, tok, body.clone()).await?;
            t_ghost.push(t2);
            t_other.push(t1);
            let strip = |mut v: Value| {
                if let Some(o) = v.as_object_mut() {
                    o.remove("id");
                }
                v
            };
            if s1 != s2 || strip(r1) != strip(r2) {
                same = false;
            }
        }
        let med = |v: &mut Vec<f64>| {
            v.sort_by(|a, b| a.partial_cmp(b).unwrap());
            v[v.len() / 2]
        };
        let (a, b) = (med(&mut t_other), med(&mut t_ghost));
        ctx.check("The answer is the same as the answer for an ID that does not exist", same && (a - b).abs() < 0.05, format!("median {:.1} ms and {:.1} ms", a * 1e3, b * 1e3));
    }
    let only_error = resp.as_object().map(|o| o.keys().all(|k| ["error", "message", "details"].contains(&k.as_str()))).unwrap_or(false);
    ctx.check("mudcat returns no data", status == 401 && only_error && foreign == 0 && own.is_empty(), format!("{status}"));
    ctx.check("The refusal is because the audience of the token", message.contains("audience"), message.clone());
    if outcome == "refused-wrong-audience" {
        let (m3, uri3, body3) = request(&mine, "artifact");
        let (s3, r3, _) = http(&router, &m3, &uri3, &host, tok, body3).await?;
        ctx.check("The refusal is the same as the refusal for a request for own data", s3 == status && r3 == resp, "");
    }
    ctx.step().await?;
    Ok(())
}

/// T5.2: read a dataset that has an embargo date.
pub async fn embargo(ctx: &mut Ctx) -> Result<()> {
    let after = ctx.is("time", "after-end");
    let actor = ctx.p("actor");
    let in_doi = ctx.is("data", "in-doi-snapshot");
    let admin = ctx.admin.clone();
    let store = resource(&admin, "facility", "eu", "managed", false).await?;
    acl_root(&admin, "/group-a", Some(("facility", "group-a/"))).await?;
    grant_collection(&admin, "/group-a", "reader", "group:group-a#member").await?;
    grant(&admin, "group", "group-a", "member", "user:alice").await?;
    source(&admin, "beamtime", "instrument", json!({}), &[("use", "research")]).await?;
    in_tx!(admin, |tx| crate::metadata::declare_attribute(&mut tx, &crate::metadata::AttributeSpec { namespace: "facility".into(), name: "embargo_until".into(), datatype: "datetime".into(), access_condition: Some("embargo".into()), ..Default::default() }).await?);
    let files: Vec<(String, Vec<u8>)> = (0..10).map(|i| (format!("frame{i:02}.cbf"), file_bytes("opaque", i, "emb"))).collect();
    let regs = register_files(&admin, &store, "/group-a/session-1", "group-a/session-1/", &files, Some("beamtime"), None).await?;
    verify_all(&admin, "facility").await?;
    // a published snapshot with a part of the data, with a DOI; then more data on main
    let first = dataset(&admin, "/group-a/session-1/data", regs[..4].iter().map(|r| Member::version(r.version_id)).collect()).await?;
    let doi_snap = publish(ctx, &admin, "/group-a/session-1/data", "paper").await?;
    let doi = in_tx!(admin, |tx| datasets::mint_doi(&mut tx, doi_snap.id).await?);
    commit(&admin, DatasetChange { dataset: "/group-a/session-1/data".into(), add: regs[4..].iter().map(|r| Member::version(r.version_id)).collect(), ..Default::default() }).await?;
    let end = ctx.inst.now() + chrono::Duration::days(3 * 365);
    in_tx!(admin, |tx| {
        crate::metadata::write(&mut tx, &crate::metadata::WriteMetadata { entity: first.dataset, namespace: "facility".into(), payload: json!({"embargo_until": end.to_rfc3339()}), ..Default::default() }).await?;
        crate::metadata::write(&mut tx, &crate::metadata::WriteMetadata { entity: first.dataset, namespace: "dc".into(), payload: json!({"description": "session 1 of group a"}), ..Default::default() }).await?;
        // after the embargo, the policy lets each signed-in user read the dataset
        authz::grant(&mut tx, "dataset", &first.dataset.to_string(), "reader", "authenticated:*", Some("embargo")).await?;
    });
    ctx.step().await?;
    let before_advance = ctx.inst.now();
    if after {
        ctx.inst.clock.advance(chrono::Duration::days(3 * 365 + 1));
    } else {
        ctx.inst.clock.advance(chrono::Duration::days(365));
    }
    let batch_commits: i64 = in_tx!(admin, |tx| tx.q1("select count(*) from commit_log where at > $1", &[&before_advance]).await?.get(0));
    ctx.check("No batch job changed a record", batch_commits == 0, format!("{batch_commits} commits during the embargo"));
    let who = match actor.as_str() {
        "group-member" => ctx.session("user:alice"),
        "signed-in-outsider" => ctx.session("user:eve"),
        _ => ctx.admin.as_principal(Principal::anonymous()),
    };
    let input = if in_doi { resolver::Input { snapshot: Some(doi_snap.id), role: "analysis".into(), ..Default::default() } } else { dataset_input("/group-a/session-1/data", Some("main"), "analysis") };
    let creds_before: i64 = in_tx!(admin, |tx| tx.q1("select count(*) from credential_log", &[]).await?.get(0));
    let r = resolve(ctx, &who, &resolver::Request { inputs: vec![input], ..Default::default() }).await;
    let creds_after: i64 = in_tx!(admin, |tx| tx.q1("select count(*) from credential_log", &[]).await?.get(0));
    match &r {
        Ok(res) => {
            ctx.outcome("readable");
            ctx.check("issues a plan and credentials for the data", !res.tasks.is_empty() && !res.credentials.is_empty(), "");
            let doi_members: BTreeSet<Uuid> = regs[..4].iter().map(|r| r.version_id).collect();
            let got: BTreeSet<Uuid> = res.tasks.iter().map(|t| t.version).collect();
            ctx.check("covers the members of the DOI snapshot only", got == doi_members, format!("{} members", got.len()));
        }
        Err(e) => {
            ctx.outcome(if e.code() == "access-denied" { "denied" } else { "error" });
            ctx.check("mudcat issues no credentials", creds_after == creds_before, e.to_string());
            let view = in_tx!(who, |tx| datasets::restricted_view(&tx, first.dataset).await);
            let ok = match (&view, actor.as_str()) {
                (Err(Error::NotFound), "anonymous") => true,
                (Ok(v), "signed-in-outsider") => {
                    let mut ids = BTreeSet::new();
                    uuids(v, &mut ids);
                    v["description"].is_string() && v["public_snapshots"][0]["doi"] == json!(doi) && !regs.iter().any(|r| ids.contains(&r.version_id))
                }
                _ => false,
            };
            ctx.check("sees of the dataset only what the data policy shows", ok, format!("{view:?}"));
        }
    }
    ctx.step().await?;
    Ok(())
}

/// T5.3: the access of a person follows the membership of a team in an outside service.
pub async fn membership_source(ctx: &mut Ctx) -> Result<()> {
    let event = ctx.p("event");
    let direct = ctx.is("grant", "direct");
    let path = ctx.p("path");
    let admin = ctx.admin.clone();
    let inst = ctx.inst.clone();
    let store = resource(&admin, "store", "eu", "managed", false).await?;
    acl_root(&admin, "/lab", Some(("store", "lab/"))).await?;
    grant_collection(&admin, "/lab", "reader", "group:members#member").await?;
    if direct {
        grant_collection(&admin, "/lab", "reader", "user:dan").await?;
    }
    source(&admin, "src", "instrument", json!({}), &[("use", "research")]).await?;
    let files = vec![("structure-1.cif".to_string(), b"data of the group".to_vec())];
    let reg = register_files(&admin, &store, "/lab", "lab/", &files, Some("src"), None).await?[0].clone();
    in_tx!(admin, |tx| crate::metadata::write(&mut tx, &crate::metadata::WriteMetadata { entity: reg.version_id, namespace: "dc".into(), payload: json!({"description": "crystal structure of the group"}), ..Default::default() }).await?);
    verify_all(&admin, "store").await?;
    search::build(&admin).await?;
    let team = |members: Vec<&str>| {
        inst.idp.teams.write().entry("github".into()).or_default().entry("lab-org".into()).or_default().insert("beamtime".into(), members.iter().map(|m| m.to_string()).collect());
    };
    team(if event == "member-added" { vec!["alice"] } else { vec!["alice", "dan"] });
    let src = in_tx!(admin, |tx| {
        authz::link_identity(&mut tx, "github", "dan", "user:dan").await?;
        authz::link_identity(&mut tx, "github", "alice", "user:alice").await?;
        authz::create_membership_source(&mut tx, "members", "github", "lab-org", "beamtime", 24 * 3600).await?
    });
    in_tx!(admin, |tx| authz::sync_membership(&mut tx, src).await?);
    ctx.step().await?;
    match event.as_str() {
        "member-added" => {
            team(vec!["alice", "dan"]);
            in_tx!(admin, |tx| authz::sync_membership(&mut tx, src).await?);
        }
        "member-removed" => {
            team(vec!["alice"]);
            in_tx!(admin, |tx| authz::sync_membership(&mut tx, src).await?);
        }
        _ => {
            inst.idp.down.write().insert("github".into());
            let hours = if event == "provider-down-short" { 1 } else { 25 };
            ctx.inst.clock.advance(chrono::Duration::hours(hours));
            let r = in_tx!(admin, |tx| authz::sync_membership(&mut tx, src).await);
            ctx.check("provider", r.is_err(), "");
        }
    }
    ctx.step().await?;
    let dan = ctx.session("user:dan");
    let deny_before: i64 = in_tx!(admin, |tx| tx.q1("select count(*) from audit where decision = 'deny' and principal = 'user:dan'", &[]).await?.get(0));
    let (ok, message, age) = if path == "resolve" {
        let r = resolve(ctx, &dan, &resolver::Request { inputs: vec![resolver::Input { artifact: Some("/lab/structure-1.cif".into()), role: "analysis".into(), ..Default::default() }], ..Default::default() }).await;
        match r {
            Ok(res) => (true, String::new(), res.decision["access"][0]["membership_age_secs"].as_i64()),
            Err(e) => (false, e.to_string(), None),
        }
    } else {
        let r = search::search(&dan, &search::SearchRequest { text: Some("crystal structure".into()), ..Default::default() }).await?;
        let hit = r.hits.iter().any(|h| h.version == Some(reg.version_id));
        let last: Option<(String, Value)> = in_tx!(admin, |tx| tx
            .qopt("select coalesce(reason, ''), details from audit where principal = 'user:dan' and point = 'search' order by id desc limit 1", &[])
            .await?
            .map(|r| (r.get(0), r.get(1))));
        let (reason, details) = last.unwrap_or_default();
        (hit, if hit { String::new() } else { reason }, details["membership_age_secs"].as_i64())
    };
    let deny_after: i64 = in_tx!(admin, |tx| tx.q1("select count(*) from audit where decision = 'deny' and principal = 'user:dan'", &[]).await?.get(0));
    ctx.outcome(if ok { "access" } else { "denied" });
    ctx.check("The request succeeds", ok, "");
    ctx.check("states the age of the membership", age.map(|a| (3500..=3700).contains(&a)).unwrap_or(false), format!("{age:?}"));
    ctx.check("mudcat denies the request, and the audit log has the denial", !ok && deny_after > deny_before, message.clone());
    ctx.check("older than the limit", message.contains("older than its limit"), message.clone());
    let groups = in_tx!(dan, |tx| authz::groups_of(&tx).await?);
    ctx.check("The direct grant still works", ok && !groups.contains(&"members".to_string()), format!("{groups:?}"));
    ctx.step().await?;
    Ok(())
}

async fn fingerprint(s: &crate::Session) -> Result<BTreeMap<String, String>> {
    let mut out = BTreeMap::new();
    let sys = s.as_principal(Principal::system());
    let mut c = sys.client().await?;
    let tx = sys.begin(&mut c).await?;
    for (t, key) in [("artifact", "id::text"), ("version", "id::text"), ("commit_log", "commit_id::text"), ("snapshot", "id::text || coalesce(merkle_hash, '')"), ("event", "position::text"), ("entity", "id::text")] {
        let r = tx.q1(&format!("select count(*)::text || ':' || coalesce(md5(string_agg({key}, ',' order by {key})), '') from {t}"), &[]).await?;
        out.insert(t.to_string(), r.get(0));
    }
    tx.commit().await?;
    Ok(out)
}

/// T5.4: move a tenant from the shared tier to its own database; clients see no difference.
pub async fn tenant_move(ctx: &mut Ctx) -> Result<()> {
    let n = ctx.n("versions");
    let fault = ctx.is("fault", "fails-at-one-half");
    let consumer = ctx.is("consumer", "at-a-position");
    let writes = ctx.is("writes", "during-the-move");
    let inst = ctx.inst.clone();
    let admin = ctx.admin.clone();
    let tid = ctx.tenant.id;
    // a neighbour in the shared tier
    let neighbour = crate::tenants::create_tenant(&inst, &format!("{}-n", ctx.tenant.name), "user:admin-n").await?;
    let ns = inst.session(neighbour.id, Principal::user("admin-n"));
    let nregs: Vec<Register> = (0..20).map(|i| Register { path: format!("/n/f{i}"), content_hash: Some(format!("n{i}")), size: 1, ..Default::default() }).collect();
    in_tx!(ns, |tx| namespace::register_batch(&mut tx, &nregs).await?);
    // the tenant: n versions in artifacts of 1000 versions, made in bulk
    let store = resource(&admin, "store", "eu", "managed", false).await?;
    acl_root(&admin, "/big", Some(("store", "big/"))).await?;
    let files: Vec<(String, Vec<u8>)> = (0..10).map(|i| (format!("f{i}.bin"), file_bytes("opaque", i, "big"))).collect();
    let regs = register_files(&admin, &store, "/big/pub", "big/pub/", &files, None, None).await?;
    verify_all(&admin, "store").await?;
    dataset(&admin, "/big/release", regs.iter().map(|r| Member::version(r.version_id)).collect()).await?;
    let published = publish(ctx, &admin, "/big/release", "v1").await?;
    let bulk = n - 10;
    if bulk > 0 {
        // portable bulk fill: artifacts, then versions
        in_tx!(admin, |tx| {
            let c = tx.commit_id().await?;
            let coll: Uuid = tx.q1("select id from collection where path = '/big'", &[]).await?.get(0);
            let arts = (bulk + 999) / 1000;
            tx.exec("create temporary table tmp_art on commit drop as select gen_random_uuid() as id, g from generate_series(1, $1::bigint) g", &[&arts]).await?;
            tx.exec("insert into artifact (id, collection_id, name, kind, created_commit) select id, $1, 'bulk-' || g, 'data', $2 from tmp_art", &[&coll, &c]).await?;
            tx.exec(
                "insert into version (id, artifact_id, number, content_hash, size, created_commit)
                 select gen_random_uuid(), a.id, v, md5(a.id::text || v), 100, $1 from tmp_art a cross join generate_series(1, 1000) v
                  where (a.g - 1) * 1000 + v <= $2",
                &[&c, &bulk],
            )
            .await?;
        });
    }
    ctx.step().await?;
    stream::flush(&admin).await?;
    stream::flush(&ns).await?;
    let position: i64 = in_tx!(admin, |tx| tx.q1("select last_position from stream_state", &[]).await?.get(0));
    let before = fingerprint(&admin).await?;
    let neighbour_before = fingerprint(&ns).await?;
    let target = format!("mudcat_ded_{}", &tid.simple().to_string()[..12]);
    *inst.move_fault_at.write() = if fault { Some(0.5) } else { None };
    // writes by clients during the move
    let accepted = Arc::new(parking_lot::Mutex::new(Vec::<String>::new()));
    let retry_errors = Arc::new(parking_lot::Mutex::new(Vec::<String>::new()));
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let writer = if writes {
        let s = ctx.session("user:admin");
        let (acc, errs, stop) = (accepted.clone(), retry_errors.clone(), stop.clone());
        Some(tokio::spawn(async move {
            let mut i = 0;
            while !stop.load(std::sync::atomic::Ordering::SeqCst) || i < 3 {
                let path = format!("/big/w/doc-{i}");
                let r: Result<()> = async {
                    let mut c = s.client().await?;
                    let mut tx = s.begin(&mut c).await?;
                    namespace::register(&mut tx, &Register { path: path.clone(), content_hash: Some(format!("w{i}")), size: 1, ..Default::default() }).await?;
                    tx.commit().await
                }
                .await;
                match r {
                    Ok(()) => acc.lock().push(path),
                    Err(e) => errs.lock().push(format!("{}:{e}", e.code())),
                }
                i += 1;
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            }
        }))
    } else {
        None
    };
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    let moved = crate::tenants::move_tenant(&inst, tid, &target).await;
    stop.store(true, std::sync::atomic::Ordering::SeqCst);
    if let Some(w) = writer {
        w.await.map_err(|e| Error::invalid(e.to_string()))?;
    }
    *inst.move_fault_at.write() = None;
    ctx.outcome(if moved.is_ok() { "moved" } else { "stays" });
    if let Err(e) = &moved {
        eprintln!("move of {}: {e}", ctx.variant.id);
        if !fault {
            ctx.check("the move", false, format!("{e}"));
        }
    }
    let accepted = accepted.lock().clone();
    // the keys and IDs of the records, without the writes during the move
    let mut after = fingerprint(&admin).await?;
    let mut before_cmp = before.clone();
    if writes {
        // writes change some tables; compare the records that existed before
        for t in ["artifact", "version", "commit_log", "event", "entity"] {
            after.remove(t);
            before_cmp.remove(t);
        }
        let ids_ok: bool = in_tx!(admin, |tx| tx.q1("select count(*) from version where content_hash like '________________________________'", &[]).await?.get::<_, i64>(0) >= bulk);
        ctx.check("Each record has the same key and the same ID", ids_ok && after == before_cmp, "");
    } else {
        ctx.check("Each record has the same key and the same ID", after == before, format!("{after:?}"));
    }
    let hash_now = in_tx!(admin, |tx| datasets::snapshot(&tx, published.id).await?.merkle_hash);
    ctx.check("Each published snapshot has the same Merkle hash", hash_now == published.merkle_hash, "");
    let t = inst.tenant(tid).unwrap();
    let router = crate::api::router(inst.clone());
    let token = auth::issue(&inst, tid, &Principal::user("admin"), chrono::Duration::hours(1));
    let (st, _, _) = http(&router, "GET", &format!("/entities/{}", regs[0].artifact_id), &t.endpoint, Some(&token), None).await?;
    ctx.check("The endpoint of the tenant did not change", t.endpoint == ctx.tenant.endpoint && st == 200, format!("{st}"));
    ctx.check("Other tenants in the shared tier see no change", fingerprint(&ns).await? == neighbour_before, "");
    if writes {
        let in_tenant: Vec<String> = in_tx!(admin, |tx| tx.q("select c.path || '/' || a.name from artifact a join collection c on c.id = a.collection_id where c.path = '/big/w'", &[]).await?.iter().map(|r| r.get(0)).collect());
        let each_once = accepted.iter().all(|p| in_tenant.iter().filter(|x| *x == p).count() == 1) && in_tenant.len() == accepted.len();
        let errs = retry_errors.lock().clone();
        ctx.check("Each write that mudcat accepted is in the tenant one time", each_once && errs.iter().all(|e| e.starts_with("retry:")), format!("{} accepted, {} refused: {:?}", accepted.len(), errs.len(), errs.first()));
    }
    // the rows in each database
    let count_in = |db: String| {
        let inst = inst.clone();
        async move {
            let pool = inst.pool_for(&db)?;
            let mut c = pool.get().await?;
            let tx = c.transaction().await?;
            tx.execute("select set_config('mudcat.tenant', $1, true)", &[&tid.to_string()]).await?;
            let mut n = 0i64;
            for t in crate::tenants::TENANT_TABLES {
                let r = tx.query_one(&format!("select count(*) from {t}"), &[]).await;
                if let Ok(r) = r {
                    n += r.get::<_, i64>(0);
                }
            }
            Ok::<_, Error>(n)
        }
    };
    if moved.is_ok() {
        ctx.check("No row of the tenant stays in the shared tier", count_in("shared".into()).await? == 0, "");
    } else {
        let w = in_tx!(admin, |tx| namespace::register(&mut tx, &Register { path: "/big/after-failure".into(), content_hash: Some("af".into()), size: 1, ..Default::default() }).await);
        ctx.check("all its data in the shared tier and accepts writes again", count_in("shared".into()).await? > 0 && w.is_ok() && inst.tenant(tid).unwrap().location == "shared", "");
        let ded = inst.pool_for(&target).ok();
        let n_ded = if ded.is_some() { count_in(target.clone()).await.unwrap_or(0) } else { 0 };
        ctx.check("The dedicated database shows no part of the tenant", n_ded == 0, format!("{n_ded} rows"));
    }
    if consumer {
        stream::flush(&admin).await?;
        let mut pos = position;
        let mut events = Vec::new();
        loop {
            let p = stream::poll(&admin, pos, 5000, None, false).await?;
            let k = p.events.len();
            events.extend(p.events);
            pos = p.next;
            if k == 0 {
                break;
            }
        }
        let positions: Vec<i64> = events.iter().map(|e| e["sequence"].as_i64().unwrap_or(0)).collect();
        let no_gap = positions.iter().enumerate().all(|(i, p)| *p == position + 1 + i as i64);
        let only_writes = events.iter().all(|e| e["data"]["path"].as_str().map(|p| p.starts_with("/big/w") || p.starts_with("/big/after")).unwrap_or(true) && !e["type"].as_str().unwrap_or("").contains("move"));
        ctx.check("The consumer continues from its position with no gap", no_gap, format!("{} events", positions.len()));
        ctx.check("The consumer sees no event of the move that failed", no_gap && only_writes, "");
    }
    ctx.step().await?;
    // the runner drops the dedicated database after the last check of the invariants
    ctx.record.cleanup_databases.push(target.clone());
    Ok(())
}
