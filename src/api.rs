//! The HTTP interfaces of an instance: the REST interface, the change feed, the Iceberg REST
//! catalog, metadata export and the MCP server (architecture.md, "Catalog API" and "Gateways").
//!
//! The tenant resolver is the outermost layer: it reads the tenant from the hostname of the
//! request, and checks that the audience of the token is that tenant. It does not trust a header
//! that a gateway adds (OP-12). The code below this layer gets a session and knows no tenant.

use crate::error::Error;
use crate::instance::{Instance, Principal, Session};
use crate::{auth, datasets, namespace, provenance, resolver, search, stream};
use axum::extract::{Path, Query, State};
use axum::http::HeaderMap;
use bytes::Bytes;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::Deserialize;
use serde_json::{json, Value};
use std::sync::Arc;
use uuid::Uuid;

pub struct ApiError(pub Error);

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let status = StatusCode::from_u16(self.0.http_status()).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
        (status, Json(self.0.to_json())).into_response()
    }
}

impl From<Error> for ApiError {
    fn from(e: Error) -> Self {
        ApiError(e)
    }
}

type R = Result<Json<Value>, ApiError>;

/// The tenant resolver: hostname and token audience (OP-12).
pub fn session_for(inst: &Arc<Instance>, headers: &axum::http::HeaderMap) -> Result<Session, Error> {
    let host = headers.get("host").and_then(|h| h.to_str().ok()).unwrap_or("").split(':').next().unwrap_or("").to_string();
    let tenant = inst.tenant_by_endpoint(&host).ok_or(Error::NotFound)?;
    let token = headers
        .get("authorization")
        .and_then(|h| h.to_str().ok())
        .and_then(|h| h.strip_prefix("Bearer "))
        .ok_or_else(|| Error::Unauthenticated("the request has no token".into()))?;
    let claims = auth::verify(inst, token)?;
    if claims.aud != tenant.id {
        return Err(Error::Unauthenticated("the audience of the token is not the tenant of this endpoint".into()));
    }
    let mut s = inst.session(tenant.id, Principal(claims.sub));
    s.place = claims.place;
    Ok(s)
}

#[derive(Clone)]
pub struct App {
    pub inst: Arc<Instance>,
}

fn sess(app: &App, headers: &HeaderMap) -> Result<Session, ApiError> {
    Ok(session_for(&app.inst, headers)?)
}

pub fn router(inst: Arc<Instance>) -> Router {
    Router::new()
        .route("/entities/{id}", get(get_entity))
        .route("/entities/{id}/jsonld", get(get_jsonld))
        .route("/search", post(post_search))
        .route("/stream", get(get_stream))
        .route("/stream/backfill", post(post_backfill))
        .route("/stream/subscriptions", post(post_subscription))
        .route("/resolve", post(post_resolve))
        .route("/resolutions/{id}/again", post(post_resolve_again))
        .route("/lineage/{id}", get(get_lineage))
        .route("/query", post(post_query))
        .route("/register", post(post_register))
        .route("/commit", post(post_commit))
        .route("/datasets/{id}/changes", get(get_changes))
        .route("/snapshots/{id}/export", get(get_export))
        .route("/iceberg/v1/config", get(iceberg_config))
        .route("/iceberg/v1/namespaces", get(iceberg_namespaces))
        .route("/iceberg/v1/namespaces/{ns}/tables/{table}", get(iceberg_load_table))
        .route("/mcp", post(post_mcp))
        .route("/openapi.json", get(openapi))
        .route("/context.jsonld", get(|| async { Json(crate::export::context()) }))
        .with_state(App { inst })
}

async fn describe(s: &Session, id: Uuid) -> Result<Value, Error> {
    let mut c = s.client().await?;
    let tx = s.begin(&mut c).await?;
    let kind = namespace::entity_kind(&tx, id).await?;
    let obj = match kind.as_str() {
        "artifact" | "version" | "dataset" | "collection" | "snapshot" => Some(kind.as_str()),
        _ => None,
    };
    if let Some(o) = obj {
        crate::authz::require(&tx, "read", "read", o, id).await?;
    }
    stream::state_document(&tx, id).await
}

async fn get_entity(State(app): State<App>, Path(id): Path<Uuid>, headers: HeaderMap) -> R {
    let s = sess(&app, &headers)?;
    Ok(Json(describe(&s, id).await?))
}

async fn get_jsonld(State(app): State<App>, Path(id): Path<Uuid>, headers: HeaderMap) -> R {
    let s = sess(&app, &headers)?;
    let d = describe(&s, id).await?;
    Ok(Json(crate::export::to_jsonld(s.tenant_id()?, &d)))
}

fn body<T: serde::de::DeserializeOwned>(b: Bytes) -> Result<T, ApiError> {
    serde_json::from_slice(&b).map_err(|e| ApiError(Error::invalid(e.to_string())))
}

async fn post_search(State(app): State<App>, headers: HeaderMap, raw: Bytes) -> R {
    let s = sess(&app, &headers)?;
    let q: search::SearchRequest = body(raw)?;
    Ok(Json(serde_json::to_value(search::search(&s, &q).await?).unwrap()))
}

#[derive(Deserialize)]
struct StreamQ {
    #[serde(default)]
    from: i64,
    #[serde(default)]
    limit: Option<i64>,
    #[serde(default)]
    state: bool,
    #[serde(default)]
    subscription: Option<Uuid>,
}

async fn get_stream(State(app): State<App>, Query(q): Query<StreamQ>, headers: HeaderMap) -> R {
    let s = sess(&app, &headers)?;
    Ok(Json(serde_json::to_value(stream::poll(&s, q.from, q.limit.unwrap_or(1000), q.subscription, q.state).await?).unwrap()))
}

async fn post_backfill(State(app): State<App>, headers: HeaderMap, raw: Bytes) -> R {
    let s = sess(&app, &headers)?;
    let v: Value = body(raw).unwrap_or(json!({}));
    let sub = v["subscription"].as_str().and_then(|x| x.parse().ok());
    let (pos, docs) = stream::backfill(&s, sub).await?;
    Ok(Json(json!({"position": pos, "documents": docs})))
}

async fn post_subscription(State(app): State<App>, headers: HeaderMap, raw: Bytes) -> R {
    let s = sess(&app, &headers)?;
    let v: Value = body(raw)?;
    let id = stream::subscribe(&s, v["scope"].as_str().unwrap_or("/"), v["stream"].as_str().unwrap_or("state")).await?;
    Ok(Json(json!({"subscription": id, "principal": s.principal.0})))
}

async fn post_resolve(State(app): State<App>, headers: HeaderMap, raw: Bytes) -> R {
    let s = sess(&app, &headers)?;
    let r: resolver::Request = body(raw)?;
    Ok(Json(serde_json::to_value(resolver::resolve(&s, &r).await?).unwrap()))
}

async fn post_resolve_again(State(app): State<App>, Path(id): Path<Uuid>, headers: HeaderMap) -> R {
    let s = sess(&app, &headers)?;
    Ok(Json(serde_json::to_value(resolver::resolve_again(&s, id).await?).unwrap()))
}

#[derive(Deserialize)]
struct LineageQ {
    #[serde(default)]
    direction: Option<String>,
    #[serde(default)]
    depth: Option<usize>,
}

async fn get_lineage(State(app): State<App>, Path(id): Path<Uuid>, Query(q): Query<LineageQ>, headers: HeaderMap) -> R {
    let s = sess(&app, &headers)?;
    let mut c = s.client().await.map_err(ApiError)?;
    let tx = s.begin(&mut c).await?;
    let kind = namespace::entity_kind(&tx, id).await?;
    if ["artifact", "version", "dataset", "snapshot"].contains(&kind.as_str()) {
        crate::authz::require(&tx, "read", "read", &kind, id).await?;
    }
    let lq = provenance::LineageQuery { direction: q.direction.unwrap_or_else(|| "ancestors".into()), depth: q.depth, ..Default::default() };
    Ok(Json(provenance::lineage(&tx, id, &lq).await?))
}

async fn post_query(State(app): State<App>, headers: HeaderMap, raw: Bytes) -> R {
    let s = sess(&app, &headers)?;
    let v: Value = body(raw)?;
    let opts: crate::query::QueryOptions = serde_json::from_value(v.clone()).map_err(|e| ApiError(Error::invalid(e.to_string())))?;
    let mut c = s.client().await.map_err(ApiError)?;
    let tx = s.begin(&mut c).await?;
    let mut out = crate::query::run(&tx, v["q"].as_str().unwrap_or(""), &opts).await?;
    if let Some(o) = out.as_object_mut() {
        o.remove("sql");
    }
    Ok(Json(out))
}

async fn post_register(State(app): State<App>, headers: HeaderMap, raw: Bytes) -> R {
    let s = sess(&app, &headers)?;
    let regs: Vec<namespace::Register> = body(raw)?;
    let mut c = s.client().await.map_err(ApiError)?;
    let mut tx = s.begin(&mut c).await?;
    let out = namespace::register_batch(&mut tx, &regs).await?;
    tx.commit().await?;
    // the rows are committed before mudcat reports the registration as complete (OP-4)
    Ok(Json(serde_json::to_value(out).unwrap()))
}

async fn post_commit(State(app): State<App>, headers: HeaderMap, raw: Bytes) -> R {
    let s = sess(&app, &headers)?;
    let changes: Vec<datasets::DatasetChange> = body(raw)?;
    let mut c = s.client().await.map_err(ApiError)?;
    let mut tx = s.begin(&mut c).await?;
    let out = datasets::commit(&mut tx, &changes).await?;
    tx.commit().await?;
    Ok(Json(serde_json::to_value(out).unwrap()))
}

#[derive(Deserialize)]
struct ChangesQ {
    from: Uuid,
    to: Uuid,
    #[serde(default)]
    after: Option<Uuid>,
    #[serde(default)]
    page: Option<usize>,
}

async fn get_changes(State(app): State<App>, Path(_id): Path<Uuid>, Query(q): Query<ChangesQ>, headers: HeaderMap) -> R {
    let s = sess(&app, &headers)?;
    let mut c = s.client().await.map_err(ApiError)?;
    let tx = s.begin(&mut c).await?;
    crate::authz::require(&tx, "read", "read", "snapshot", q.to).await?;
    Ok(Json(datasets::changes(&tx, q.from, q.to, q.after, q.page.unwrap_or(1000)).await?))
}

#[derive(Deserialize)]
struct ExportQ {
    format: String,
}

async fn get_export(State(app): State<App>, Path(id): Path<Uuid>, Query(q): Query<ExportQ>, headers: HeaderMap) -> R {
    let s = sess(&app, &headers)?;
    let mut c = s.client().await.map_err(ApiError)?;
    let tx = s.begin(&mut c).await?;
    crate::authz::require(&tx, "export", "read", "snapshot", id).await?;
    Ok(Json(match q.format.as_str() {
        "croissant" => crate::export::croissant(&tx, id).await?,
        _ => crate::export::dcat(&tx, id).await?,
    }))
}

// ---------------------------------------------------------------------------- Iceberg REST

async fn iceberg_config(State(app): State<App>, headers: HeaderMap) -> R {
    let _ = sess(&app, &headers)?;
    Ok(Json(json!({"defaults": {}, "overrides": {}, "endpoints": ["GET /v1/namespaces", "GET /v1/namespaces/{namespace}/tables/{table}"]})))
}

async fn iceberg_namespaces(State(app): State<App>, headers: HeaderMap) -> R {
    let s = sess(&app, &headers)?;
    let mut c = s.client().await.map_err(ApiError)?;
    let tx = s.begin(&mut c).await?;
    let roots = crate::authz::readable_roots(&tx).await?;
    let rows = tx.q("select c.path from dataset d join collection c on c.id = d.collection_id and c.tenant_id = d.tenant_id where $1::uuid[] is null or c.acl_root_id = any($1)", &[&roots]).await?;
    let mut ns: Vec<Vec<String>> = rows.iter().map(|r| r.get::<_, String>(0).trim_start_matches('/').split('/').map(|x| x.to_string()).collect()).collect();
    ns.sort();
    ns.dedup();
    Ok(Json(json!({"namespaces": ns})))
}

/// loadTable for a records dataset. The table metadata is generated from the manifests of the
/// head snapshot; mudcat stores no state in Iceberg metadata files.
async fn iceberg_load_table(State(app): State<App>, Path((ns, table)): Path<(String, String)>, headers: HeaderMap) -> R {
    let s = sess(&app, &headers)?;
    let mut c = s.client().await.map_err(ApiError)?;
    let tx = s.begin(&mut c).await?;
    let did = match table.parse::<Uuid>() {
        Ok(id) => datasets::dataset_id(&tx, &id.to_string()).await?,
        Err(_) => datasets::dataset_id(&tx, &format!("/{}/{}", ns.replace('\u{1f}', "/").replace('.', "/"), table)).await?,
    };
    crate::authz::require(&tx, "read", "read", "dataset", did).await?;
    let snap = datasets::resolve_ref(&tx, did, "main", None).await?;
    let members = datasets::members(&tx, snap).await?;
    let ids: Vec<Uuid> = members.iter().filter(|e| e.member_kind == "version").map(|e| e.member_id).collect();
    let versions = namespace::versions(&tx, &ids).await?;
    let fields: Vec<Value> = versions.first().and_then(|v| v.structure["fields"].as_array().cloned()).unwrap_or_default();
    let schema_fields: Vec<Value> = fields
        .iter()
        .enumerate()
        .map(|(i, f)| json!({"id": i + 1, "name": f["name"], "required": f["required"].as_bool().unwrap_or(false), "type": f["physical_type"].as_str().unwrap_or("string")}))
        .collect();
    Ok(Json(json!({
        "metadata-location": format!("mudcat://snapshots/{snap}/metadata.json"),
        "metadata": {"format-version": 2, "table-uuid": did, "location": format!("mudcat://datasets/{did}"),
                     "current-snapshot-id": snap.as_u128() as u64 & 0x7fff_ffff_ffff_ffff,
                     "schemas": [{"type": "struct", "schema-id": 0, "fields": schema_fields}], "current-schema-id": 0,
                     "snapshots": [{"snapshot-id": snap.as_u128() as u64 & 0x7fff_ffff_ffff_ffff, "summary": {"operation": "append", "mudcat-snapshot": snap.to_string()}}],
                     "data-files": versions.iter().map(|v| json!({"path": v.path, "file_size_in_bytes": v.size, "record_count": v.element_count})).collect::<Vec<_>>()},
        "config": {},
    })))
}

// ---------------------------------------------------------------------------- MCP

/// The MCP server for agents (IN-6). It runs with the identity and permissions of the caller.
async fn post_mcp(State(app): State<App>, headers: HeaderMap, raw: Bytes) -> Response {
    let s = match sess(&app, &headers) {
        Ok(s) => s,
        Err(e) => return e.into_response(),
    };
    let msg: Value = match body(raw) {
        Ok(v) => v,
        Err(e) => return e.into_response(),
    };
    let id = msg["id"].clone();
    let method = msg["method"].as_str().unwrap_or("");
    let result: Result<Value, Error> = match method {
        "initialize" => Ok(json!({"protocolVersion": "2025-06-18", "serverInfo": {"name": "mudcat", "version": env!("CARGO_PKG_VERSION")}, "capabilities": {"tools": {}}})),
        "tools/list" => Ok(json!({"tools": [
            {"name": "search", "description": "Search the catalog", "inputSchema": {"type": "object", "properties": {"text": {"type": "string"}}}},
            {"name": "describe", "description": "Describe an entity by its ID", "inputSchema": {"type": "object", "properties": {"id": {"type": "string"}}, "required": ["id"]}},
            {"name": "resolve", "description": "Resolve data for a job", "inputSchema": {"type": "object"}},
            {"name": "lineage", "description": "Ancestors or descendants of an entity", "inputSchema": {"type": "object", "properties": {"id": {"type": "string"}, "direction": {"type": "string"}}}},
            {"name": "policy_check", "description": "Check if data permits an action and a purpose", "inputSchema": {"type": "object"}},
        ]})),
        "tools/call" => mcp_call(&s, &msg["params"]).await.map(|v| json!({"content": [{"type": "text", "text": v.to_string()}], "structuredContent": v})),
        _ => Err(Error::invalid(format!("unknown method {method}"))),
    };
    match result {
        Ok(r) => Json(json!({"jsonrpc": "2.0", "id": id, "result": r})).into_response(),
        Err(e) => {
            let status = e.http_status();
            Json(json!({"jsonrpc": "2.0", "id": id, "error": {"code": -32000 - status as i64, "message": e.to_string(), "data": e.to_json()}})).into_response()
        }
    }
}

async fn mcp_call(s: &Session, p: &Value) -> Result<Value, Error> {
    let args = &p["arguments"];
    match p["name"].as_str().unwrap_or("") {
        "search" => Ok(serde_json::to_value(search::search(s, &serde_json::from_value(args.clone())?).await?)?),
        "describe" => describe(s, args["id"].as_str().and_then(|x| x.parse().ok()).ok_or_else(|| Error::invalid("id"))?).await,
        "resolve" => Ok(serde_json::to_value(resolver::resolve(s, &serde_json::from_value(args.clone())?).await?)?),
        "lineage" => {
            let id: Uuid = args["id"].as_str().and_then(|x| x.parse().ok()).ok_or_else(|| Error::invalid("id"))?;
            let mut c = s.client().await?;
            let tx = s.begin(&mut c).await?;
            let kind = namespace::entity_kind(&tx, id).await?;
            if ["artifact", "version", "dataset", "snapshot"].contains(&kind.as_str()) {
                crate::authz::require(&tx, "read", "read", &kind, id).await?;
            }
            provenance::lineage(&tx, id, &provenance::LineageQuery { direction: args["direction"].as_str().unwrap_or("ancestors").into(), ..Default::default() }).await
        }
        "policy_check" => {
            let mut c = s.client().await?;
            let tx = s.begin(&mut c).await?;
            let snap: Uuid = args["snapshot"].as_str().and_then(|x| x.parse().ok()).ok_or_else(|| Error::invalid("snapshot"))?;
            crate::authz::require(&tx, "read", "read", "snapshot", snap).await?;
            let sources: Vec<Uuid> = datasets::snapshot_sources(&tx, snap).await?.into_iter().collect();
            Ok(serde_json::to_value(crate::policy::evaluate(&tx, &sources, args["action"].as_str().unwrap_or("train"), args["purpose"].as_str().unwrap_or("research")).await?)?)
        }
        other => Err(Error::invalid(format!("unknown tool {other}"))),
    }
}

async fn openapi() -> Json<Value> {
    Json(json!({
        "openapi": "3.1.0",
        "info": {"title": "mudcat", "version": env!("CARGO_PKG_VERSION")},
        "servers": [{"url": "https://{tenant}.mudcat.localhost"}],
        "components": {"securitySchemes": {"bearer": {"type": "http", "scheme": "bearer", "bearerFormat": "JWT"}}},
        "security": [{"bearer": []}],
        "paths": {
            "/entities/{id}": {"get": {"summary": "Describe an entity by its ID"}},
            "/entities/{id}/jsonld": {"get": {"summary": "The entity as JSON-LD with the published context"}},
            "/search": {"post": {"summary": "Search, with a check of each result against the catalog"}},
            "/stream": {"get": {"summary": "The change stream from a position; state=true gives state documents"}},
            "/stream/backfill": {"post": {"summary": "State documents of every readable entity, and the position to continue from"}},
            "/stream/subscriptions": {"post": {"summary": "A subscription with the identity of the caller"}},
            "/resolve": {"post": {"summary": "Resolve the inputs of a job: plan, credentials, run token, policy decision"}},
            "/resolutions/{id}/again": {"post": {"summary": "Resolve the exact inputs of a resolution again, with current policy"}},
            "/lineage/{id}": {"get": {"summary": "Ancestors or descendants, with limits"}},
            "/query": {"post": {"summary": "The query language: a path glob and a predicate, in pages"}},
            "/register": {"post": {"summary": "Bulk registration, idempotent on (artifact, checksum)"}},
            "/commit": {"post": {"summary": "Commits with requirements and updates"}},
            "/datasets/{id}/changes": {"get": {"summary": "Members added and removed between two snapshots, in pages"}},
            "/snapshots/{id}/export": {"get": {"summary": "DCAT or Croissant description of a published snapshot"}},
            "/iceberg/v1/config": {"get": {}}, "/iceberg/v1/namespaces": {"get": {}}, "/iceberg/v1/namespaces/{ns}/tables/{table}": {"get": {}},
            "/mcp": {"post": {"summary": "MCP server (JSON-RPC): search, describe, resolve, lineage, policy_check"}},
        }
    }))
}
