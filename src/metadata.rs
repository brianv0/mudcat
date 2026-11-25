//! Metadata: payloads, attribute definitions, namespaces, tags and enrichers (model.md, section 3).

use crate::error::{Error, Result};
use crate::instance::{new_id, Tx};
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};
use std::collections::BTreeMap;
use uuid::Uuid;

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct AttributeSpec {
    pub namespace: String,
    pub name: String,
    pub datatype: String,
    #[serde(default)]
    pub unit: Option<String>,
    #[serde(default)]
    pub custody: Option<String>,
    #[serde(default)]
    pub indexed: bool,
    #[serde(default)]
    pub is_tag: bool,
    #[serde(default)]
    pub permitted: Option<Vec<String>>,
    #[serde(default)]
    pub propagate: bool,
    #[serde(default)]
    pub access_condition: Option<String>,
    #[serde(default)]
    pub description: Option<String>,
}

/// Declare an attribute (a field) in a namespace. A tag is an attribute with a closed list of values.
pub async fn declare_attribute(tx: &mut Tx<'_>, a: &AttributeSpec) -> Result<Uuid> {
    if let Some(r) = tx.qopt("select id from attribute_def where namespace = $1 and name = $2", &[&a.namespace, &a.name]).await? {
        return Ok(r.get(0));
    }
    let id = new_id();
    let custody = a.custody.clone().unwrap_or_else(|| "vendor-held".into());
    let permitted = a.permitted.as_ref().map(|p| json!(p));
    tx.exec(
        "insert into attribute_def (id, namespace, name, datatype, unit, custody, indexed, is_tag, permitted, propagate, access_condition, description)
         values ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)",
        &[&id, &a.namespace, &a.name, &a.datatype, &a.unit, &custody, &a.indexed, &a.is_tag, &permitted, &a.propagate, &a.access_condition, &a.description],
    )
    .await?;
    tx.emit("field.declared", id, "attribute", None, json!({"namespace": a.namespace, "name": a.name, "datatype": a.datatype})).await?;
    Ok(id)
}

/// Give a namespace an owner and the only principals that can write it (ST-9).
pub async fn set_namespace(tx: &mut Tx<'_>, name: &str, owner: &str, writers: &[String]) -> Result<()> {
    let writers: Vec<String> = writers.to_vec();
    tx.exec(
        "insert into metadata_namespace (name, owner, writers) values ($1,$2,$3)
         on conflict (tenant_id, name) do update set owner = excluded.owner, writers = excluded.writers",
        &[&name, &owner, &writers],
    )
    .await?;
    Ok(())
}

pub async fn register_enricher(tx: &mut Tx<'_>, name: &str, version: &str, namespace: &str, needs_review: bool, place: &str) -> Result<()> {
    if place == "control-plane" && !name.starts_with("mudcat-") {
        // third-party enrichers run next to the data or in the customer gateway (ST-16)
        return Err(Error::refused("enricher-place", "a third-party enricher cannot run in the control plane", Value::Null));
    }
    tx.exec(
        "insert into enricher (name, version, namespace, needs_review, place) values ($1,$2,$3,$4,$5) on conflict do nothing",
        &[&name, &version, &namespace, &needs_review, &place],
    )
    .await?;
    let owner = format!("enricher:{name}");
    set_namespace(tx, namespace, &owner, &[owner.clone()]).await?;
    Ok(())
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct WriteMetadata {
    pub entity: Uuid,
    pub namespace: String,
    pub payload: Value,
    /// person | enricher | model | plugin
    #[serde(default = "person")]
    pub origin: String,
    #[serde(default)]
    pub enricher: Option<String>,
    #[serde(default)]
    pub enricher_version: Option<String>,
    #[serde(default)]
    pub confidence: Option<f32>,
}
fn person() -> String {
    "person".into()
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Written {
    pub status: String,
    pub undeclared: Vec<String>,
}

/// The ACL root of an entity, for events and for search documents.
pub async fn acl_root_of(tx: &Tx<'_>, id: Uuid) -> Result<Option<Uuid>> {
    let r = tx
        .qopt(
            "select coalesce(
                (select c.acl_root_id from collection c where c.id = $1),
                (select c.acl_root_id from artifact a join collection c on c.id = a.collection_id and c.tenant_id = a.tenant_id where a.id = $1),
                (select c.acl_root_id from version v join artifact a on a.id = v.artifact_id and a.tenant_id = v.tenant_id
                   join collection c on c.id = a.collection_id and c.tenant_id = a.tenant_id where v.id = $1),
                (select c.acl_root_id from dataset d join collection c on c.id = d.collection_id and c.tenant_id = d.tenant_id where d.id = $1),
                (select c.acl_root_id from snapshot s join dataset d on d.id = s.dataset_id and d.tenant_id = s.tenant_id
                   join collection c on c.id = d.collection_id and c.tenant_id = d.tenant_id where s.id = $1))",
            &[&id],
        )
        .await?;
    Ok(r.and_then(|r| r.get(0)))
}

/// Write one metadata payload: the set of values of one entity in one namespace (model.md, section 3).
pub async fn write(tx: &mut Tx<'_>, w: &WriteMetadata) -> Result<Written> {
    // authority: a namespace with an owner accepts writes only from the principals it names (ST-9)
    if let Some(r) = tx.qopt("select owner, writers from metadata_namespace where name = $1", &[&w.namespace]).await? {
        let writers: Vec<String> = r.get(1);
        let p = tx.principal().0.clone();
        let writer_ok = writers.contains(&p) || w.enricher.as_ref().map(|e| writers.contains(&format!("enricher:{e}"))).unwrap_or(false);
        if !writers.is_empty() && !writer_ok {
            return Err(Error::refused(
                "namespace-not-writable",
                format!("{p} may not write the namespace {}; its owner names {:?}", w.namespace, writers),
                json!({"namespace": w.namespace}),
            ));
        }
    }
    // an enricher writes only in a namespace that it owns (ST-13)
    let mut needs_review = false;
    if let Some(e) = &w.enricher {
        let r = tx
            .qopt("select namespace, needs_review from enricher where name = $1 order by version desc limit 1", &[e])
            .await?
            .ok_or_else(|| Error::invalid(format!("unknown enricher {e}")))?;
        let ns: String = r.get(0);
        if ns != w.namespace {
            return Err(Error::refused("namespace-not-owned", format!("the enricher {e} owns {ns}, not {}", w.namespace), Value::Null));
        }
        needs_review = r.get(1);
    }
    let obj = w.payload.as_object().ok_or_else(|| Error::invalid("a payload is an object"))?;
    let defs = tx
        .q("select id, name, datatype, custody, indexed, permitted from attribute_def where namespace = $1", &[&w.namespace])
        .await?;
    let mut by_name = BTreeMap::new();
    for d in &defs {
        by_name.insert(d.get::<_, String>(1), d);
    }
    let mut stored = Map::new();
    let mut undeclared = Vec::new();
    let mut index_rows = Vec::new();
    for (k, v) in obj {
        match by_name.get(k) {
            None => {
                undeclared.push(k.clone());
                stored.insert(k.clone(), v.clone());
            }
            Some(d) => {
                let datatype: String = d.get(2);
                validate(k, &datatype, v)?;
                if let Some(Value::Array(p)) = d.get::<_, Option<Value>>(5) {
                    if !p.contains(v) {
                        return Err(Error::refused("not-valid", format!("{v} is not a permitted value of {k}"), json!({"attribute": k})));
                    }
                }
                let custody: String = d.get(3);
                if custody == "customer-held" {
                    // the value stays in the customer gateway; the catalog stores only that it exists (ST-10)
                    let tenant = tx.sess.tenant_id()?;
                    tx.inst().customer_gateway.write().insert(format!("{tenant}/{}/{}/{k}", w.entity, w.namespace), v.clone());
                    stored.insert(k.clone(), json!({"$customer-held": true}));
                } else {
                    stored.insert(k.clone(), v.clone());
                    if d.get::<_, bool>(4) {
                        index_rows.push((d.get::<_, Uuid>(0), datatype, v.clone()));
                    }
                }
            }
        }
    }
    let status = if needs_review { "proposed" } else { "visible" };
    let commit = tx.commit_id().await?;
    let payload = Value::Object(stored);
    let asserted_by = tx.principal().0.clone();
    let origin = if w.enricher.is_some() && w.origin == "person" { "enricher".to_string() } else { w.origin.clone() };
    tx.exec(
        "insert into metadata_payload (entity_id, namespace, payload, origin, asserted_by, enricher, enricher_version, confidence, status, commit_id)
         values ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
         on conflict (tenant_id, entity_id, namespace) do update set payload = excluded.payload, origin = excluded.origin,
           asserted_by = excluded.asserted_by, enricher = excluded.enricher, enricher_version = excluded.enricher_version,
           confidence = excluded.confidence, status = excluded.status, commit_id = excluded.commit_id, reviewed_by = null, reviewed_at = null",
        &[&w.entity, &w.namespace, &payload, &origin, &asserted_by, &w.enricher, &w.enricher_version, &w.confidence, &status, &commit],
    )
    .await?;
    tx.exec(
        "insert into metadata_history (entity_id, namespace, commit_id, payload, actor) values ($1,$2,$3,$4,$5)
         on conflict (tenant_id, entity_id, namespace, commit_id) do update set payload = excluded.payload",
        &[&w.entity, &w.namespace, &commit, &payload, &asserted_by],
    )
    .await?;
    // the typed attribute index (PF-9): one table, no new database index for a new attribute
    for (attr, datatype, v) in index_rows {
        let (n, t, ts) = typed(&datatype, &v);
        tx.exec(
            "insert into attr_index (attr_id, entity_id, num_value, text_value, ts_value) values ($1,$2,$3,$4,$5)
             on conflict (tenant_id, attr_id, entity_id) do update set num_value = excluded.num_value, text_value = excluded.text_value, ts_value = excluded.ts_value",
            &[&attr, &w.entity, &n, &t, &ts],
        )
        .await?;
    }
    let root = acl_root_of(tx, w.entity).await?;
    let kind = crate::namespace::entity_kind(tx, w.entity).await.unwrap_or_else(|_| "entity".into());
    let typ = if status == "proposed" { "metadata.proposed" } else { "metadata.written" };
    tx.emit(typ, w.entity, &kind, root, json!({"namespace": w.namespace, "payload": payload, "origin": origin, "status": status})).await?;
    Ok(Written { status: status.into(), undeclared })
}

fn validate(k: &str, datatype: &str, v: &Value) -> Result<()> {
    let ok = match datatype {
        "number" | "integer" => v.is_number(),
        "boolean" => v.is_boolean(),
        "datetime" => v.as_str().map(|s| chrono::DateTime::parse_from_rfc3339(s).is_ok()).unwrap_or(false),
        "string" | "text" => v.is_string(),
        _ => true,
    };
    if ok {
        Ok(())
    } else {
        Err(Error::refused("not-valid", format!("the value {v} of {k} is not a {datatype} (ST-6)"), json!({"attribute": k, "datatype": datatype})))
    }
}

fn typed(datatype: &str, v: &Value) -> (Option<f64>, Option<String>, Option<chrono::DateTime<chrono::Utc>>) {
    match datatype {
        "number" | "integer" => (v.as_f64(), None, None),
        "datetime" => (None, None, v.as_str().and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok()).map(|t| t.with_timezone(&chrono::Utc))),
        _ => (None, v.as_str().map(|s| s.to_string()).or_else(|| Some(v.to_string())), None),
    }
}

/// Accept or reject an enricher output that needs review (ST-15).
pub async fn review(tx: &mut Tx<'_>, entity: Uuid, namespace: &str, accept: bool) -> Result<()> {
    let status = if accept { "visible" } else { "rejected" };
    let n = tx
        .exec(
            "update metadata_payload set status = $3, reviewed_by = $4, reviewed_at = $5 where entity_id = $1 and namespace = $2 and status = 'proposed'",
            &[&entity, &namespace, &status, &tx.principal().0, &tx.now],
        )
        .await?;
    if n == 0 {
        return Err(Error::NotFound);
    }
    let root = acl_root_of(tx, entity).await?;
    let kind = crate::namespace::entity_kind(tx, entity).await.unwrap_or_else(|_| "entity".into());
    let typ = if accept { "metadata.accepted" } else { "metadata.rejected" };
    tx.emit(typ, entity, &kind, root, json!({"namespace": namespace})).await?;
    Ok(())
}

/// The visible payloads of an entity, as {namespace: {payload, origin, ...}}.
pub async fn payloads(tx: &Tx<'_>, entity: Uuid, include_proposed: bool) -> Result<Value> {
    let rows = tx
        .q(
            "select namespace, payload, origin, asserted_by, enricher, enricher_version, status, reviewed_by from metadata_payload
              where entity_id = $1 and (status = 'visible' or ($2 and status = 'proposed')) order by namespace",
            &[&entity, &include_proposed],
        )
        .await?;
    let mut m = Map::new();
    for r in rows {
        m.insert(
            r.get(0),
            json!({"values": r.get::<_, Value>(1), "origin": r.get::<_, String>(2), "asserted_by": r.get::<_, String>(3),
                   "enricher": r.get::<_, Option<String>>(4), "enricher_version": r.get::<_, Option<String>>(5),
                   "status": r.get::<_, String>(6), "reviewed_by": r.get::<_, Option<String>>(7)}),
        );
    }
    Ok(Value::Object(m))
}

// ---------------------------------------------------------------------------- tags

/// Assign a tag. A tag that propagates also goes to every entity derived from this one.
pub async fn tag(tx: &mut Tx<'_>, entity: Uuid, name: &str, value: &str) -> Result<Vec<Uuid>> {
    let def = tx
        .qopt("select permitted, propagate from attribute_def where name = $1 and is_tag", &[&name])
        .await?
        .ok_or_else(|| Error::invalid(format!("{name} is not a declared tag")))?;
    if let Some(Value::Array(p)) = def.get::<_, Option<Value>>(0) {
        if !p.contains(&json!(value)) {
            return Err(Error::refused("not-valid", format!("{value} is not a permitted value of the tag {name}"), Value::Null));
        }
    }
    let commit = tx.commit_id().await?;
    tx.exec(
        "insert into tag (entity_id, name, value, commit_id) values ($1,$2,$3,$4)
         on conflict (tenant_id, entity_id, name) do update set value = excluded.value, commit_id = excluded.commit_id, propagated_from = null",
        &[&entity, &name, &value, &commit],
    )
    .await?;
    let root = acl_root_of(tx, entity).await?;
    let kind = crate::namespace::entity_kind(tx, entity).await.unwrap_or_else(|_| "entity".into());
    tx.emit("tag.assigned", entity, &kind, root, json!({"name": name, "value": value})).await?;
    let mut propagated = Vec::new();
    if def.get::<_, bool>(1) {
        let down = crate::provenance::downstream(tx, &[entity], 100_000).await?;
        let mut conflicts = Vec::new();
        for d in down {
            if d == entity {
                continue;
            }
            if let Some(r) = tx.qopt("select value, propagated_from from tag where entity_id = $1 and name = $2", &[&d, &name]).await? {
                let v: String = r.get(0);
                let from: Option<Uuid> = r.get(1);
                if from.is_none() && v != value {
                    conflicts.push(d); // a value set on the entity itself wins; the conflict is flagged
                    continue;
                }
            }
            tx.exec(
                "insert into tag (entity_id, name, value, propagated_from, commit_id) values ($1,$2,$3,$4,$5)
                 on conflict (tenant_id, entity_id, name) do update set value = excluded.value, propagated_from = excluded.propagated_from, commit_id = excluded.commit_id",
                &[&d, &name, &value, &entity, &commit],
            )
            .await?;
            propagated.push(d);
        }
        if !conflicts.is_empty() {
            tx.emit("tag.conflict", entity, &kind, root, json!({"name": name, "entities": conflicts})).await?;
        }
    }
    Ok(propagated)
}

/// Attach a policy to a tag value: a compartment or a jurisdiction rule (model.md, "Tags").
pub async fn set_tag_policy(tx: &mut Tx<'_>, name: &str, value: &str, kind: &str, config: Value) -> Result<()> {
    tx.exec(
        "insert into tag_policy (name, value, kind, config) values ($1,$2,$3,$4) on conflict (tenant_id, name, value, kind) do update set config = excluded.config",
        &[&name, &value, &kind, &config],
    )
    .await?;
    Ok(())
}

/// The tags of entities, with the tags that they inherit from their collections (ST-17). A value
/// that an entity sets itself wins over an inherited value; a lower collection wins over a higher one.
pub async fn effective_tags_many(tx: &Tx<'_>, entities: &[Uuid]) -> Result<Vec<(String, String)>> {
    let mut out: BTreeMap<String, String> = BTreeMap::new();
    // the collection of each entity
    let rows = tx
        .q(
            "select c.path from collection c where c.id = any($1)
             union select c.path from artifact a join collection c on c.id = a.collection_id and c.tenant_id = a.tenant_id where a.id = any($1)
             union select c.path from version v join artifact a on a.id = v.artifact_id and a.tenant_id = v.tenant_id
                     join collection c on c.id = a.collection_id and c.tenant_id = a.tenant_id where v.id = any($1)
             union select c.path from dataset d join collection c on c.id = d.collection_id and c.tenant_id = d.tenant_id where d.id = any($1)",
            &[&entities],
        )
        .await?;
    let mut prefixes = Vec::new();
    for r in rows {
        let p: String = r.get(0);
        let mut cur = Some(p.as_str());
        while let Some(c) = cur {
            prefixes.push(c.to_string());
            cur = crate::namespace::parent_path(c);
        }
    }
    if !prefixes.is_empty() {
        let order = if crate::mutant("tag-inherit-top-wins") { "desc" } else { "asc" };
        let rows = tx
            .q(
                &format!(
                    "select t.name, t.value from tag t join collection c on c.id = t.entity_id and c.tenant_id = t.tenant_id
                      where c.path = any($1) order by length(c.path) {order}"
                ),
                &[&prefixes],
            )
            .await?;
        for r in rows {
            out.insert(r.get(0), r.get(1));
        }
    }
    let rows = tx.q("select name, value from tag where entity_id = any($1)", &[&entities]).await?;
    for r in rows {
        out.insert(r.get(0), r.get(1));
    }
    Ok(out.into_iter().collect())
}

pub async fn effective_tags(tx: &Tx<'_>, entity: Uuid) -> Result<Vec<(String, String)>> {
    effective_tags_many(tx, &[entity]).await
}
