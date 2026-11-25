//! Metadata export: JSON-LD with the published context (IN-1), and DCAT and Croissant 1.1
//! descriptions of published snapshots (IN-4). Export is an enforcement point: the duties of a
//! model limit its export (use case 3).

use crate::error::{Error, Result};
use crate::instance::Tx;
use serde_json::{json, Value};
use uuid::Uuid;

pub fn context() -> Value {
    json!({"@context": {
        "mc": "https://mudcat.example/ns#", "prov": "http://www.w3.org/ns/prov#", "dcat": "http://www.w3.org/ns/dcat#",
        "dcterms": "http://purl.org/dc/terms/", "csvw": "http://www.w3.org/ns/csvw#", "odrl": "http://www.w3.org/ns/odrl/2/",
        "oa": "http://www.w3.org/ns/oa#", "qudt": "http://qudt.org/schema/qudt/", "dpv": "https://w3id.org/dpv#",
        "id": "@id", "kind": "@type", "path": "mc:path", "revision": "mc:revision", "metadata": "mc:metadata", "tags": "mc:tag",
        "merkle_hash": "mc:contentHash", "content_hash": "mc:contentHash", "identifiers": "mc:identifier",
    }})
}

/// The JSON-LD form of an entity: its state document with the context.
pub fn to_jsonld(tenant: Uuid, doc: &Value) -> Value {
    let mut d = doc.clone();
    d["@context"] = context()["@context"].clone();
    if let Some(id) = doc["id"].as_str() {
        d["@id"] = json!(format!("urn:mudcat:{tenant}:{id}"));
    }
    d
}

async fn describe_snapshot(tx: &Tx<'_>, snap: Uuid) -> Result<(Value, Vec<Value>, Vec<String>, Option<String>, Vec<Value>)> {
    let s = crate::datasets::snapshot(tx, snap).await?;
    if s.status.as_deref() != Some("published") {
        return Err(Error::refused("not-published", "mudcat exports published snapshots only", Value::Null));
    }
    let path = crate::datasets::dataset_path(tx, s.dataset_id).await?;
    let doi: Option<String> = tx.qopt("select value from identifier where entity_id = $1 and scheme = 'doi' and valid_to is null", &[&snap]).await?.map(|r| r.get(0));
    let entries = crate::datasets::members(tx, snap).await?;
    let ids: Vec<Uuid> = entries.iter().filter(|e| e.member_kind == "version").map(|e| e.member_id).collect();
    let versions = crate::namespace::versions(tx, &ids).await?;
    let sources: Vec<Uuid> = versions.iter().filter_map(|v| v.source_id).collect();
    let licences: Vec<String> = tx
        .q("select distinct content->>'id' from terms_document where source_id = any($1) and format = 'spdx'", &[&sources])
        .await?
        .iter()
        .filter_map(|r| r.get::<_, Option<String>>(0))
        .collect();
    let mut fields = Vec::new();
    for v in &versions {
        for f in v.structure["fields"].as_array().cloned().unwrap_or_default() {
            if !fields.iter().any(|x: &Value| x["name"] == f["name"]) {
                fields.push(f);
            }
        }
    }
    let files: Vec<Value> = versions.iter().map(|v| json!({"path": v.path, "sha256": v.content_hash, "size": v.size, "id": v.id})).collect();
    Ok((json!({"snapshot": snap, "dataset": path, "merkle_hash": s.merkle_hash, "members": s.member_count}), fields, licences, doi, files))
}

pub async fn dcat(tx: &Tx<'_>, snap: Uuid) -> Result<Value> {
    let (s, fields, licences, doi, files) = describe_snapshot(tx, snap).await?;
    Ok(json!({
        "@context": {"dcat": "http://www.w3.org/ns/dcat#", "dcterms": "http://purl.org/dc/terms/", "spdx": "http://spdx.org/rdf/terms#", "csvw": "http://www.w3.org/ns/csvw#", "qudt": "http://qudt.org/schema/qudt/"},
        "@type": "dcat:Dataset",
        "@id": doi.as_ref().map(|d| format!("https://doi.org/{d}")).unwrap_or_else(|| format!("urn:mudcat:snapshot:{snap}")),
        "dcterms:identifier": doi,
        "dcterms:title": s["dataset"],
        "dcterms:license": licences.iter().map(|l| format!("https://spdx.org/licenses/{l}")).collect::<Vec<_>>(),
        "dcat:version": snap,
        "mc:contentHash": s["merkle_hash"],
        "csvw:tableSchema": {"csvw:column": fields.iter().map(|f| json!({"csvw:name": f["name"], "csvw:datatype": f["datatype"], "qudt:unit": f["unit"]})).collect::<Vec<_>>()},
        "dcat:distribution": files.iter().map(|f| json!({"@type": "dcat:Distribution", "dcat:accessURL": format!("urn:mudcat:version:{}", f["id"].as_str().unwrap_or("")), "dcat:byteSize": f["size"],
                                                           "spdx:checksum": {"spdx:algorithm": "sha256", "spdx:checksumValue": f["sha256"]}})).collect::<Vec<_>>(),
    }))
}

pub async fn croissant(tx: &Tx<'_>, snap: Uuid) -> Result<Value> {
    let (s, fields, licences, doi, files) = describe_snapshot(tx, snap).await?;
    Ok(json!({
        "@context": {"@vocab": "https://schema.org/", "cr": "http://mlcommons.org/croissant/", "sc": "https://schema.org/"},
        "@type": "sc:Dataset",
        "conformsTo": "http://mlcommons.org/croissant/1.1",
        "name": s["dataset"],
        "identifier": doi,
        "url": doi.as_ref().map(|d| format!("https://doi.org/{d}")),
        "license": licences.iter().map(|l| format!("https://spdx.org/licenses/{l}")).collect::<Vec<_>>(),
        "version": snap,
        "distribution": files.iter().map(|f| json!({"@type": "cr:FileObject", "@id": f["id"], "name": f["path"], "contentSize": f["size"], "sha256": f["sha256"]})).collect::<Vec<_>>(),
        "recordSet": [{"@type": "cr:RecordSet", "name": "records", "field": fields.iter().map(|f| json!({"@type": "cr:Field", "name": f["name"], "dataType": f["datatype"], "unit": f["unit"]})).collect::<Vec<_>>()}],
    }))
}

/// Export a model to a recipient. The duties of the model (for example "share only with
/// approved users") limit its export and its grants.
pub async fn check_model_export(tx: &Tx<'_>, model_version: Uuid, recipient: &str) -> Result<()> {
    let duties = crate::provenance::model_duties(tx, model_version).await?;
    for d in duties {
        if d["duty"] == "share-only-with-approved-users" {
            let group = d["params"]["group"].as_str().unwrap_or("approved-users");
            let (st, sid, _) = crate::authz::parse_subject(recipient)?;
            let ok = tx
                .qopt("select 1 from relation where object_type = 'group' and object_id = $1 and relation = 'member' and subject_type = $2 and subject_id = $3", &[&group, &st, &sid])
                .await?;
            if ok.is_none() {
                tx.sess.audit("export", "export", &format!("version:{model_version}"), false, Some("duty"), json!({"duty": d, "recipient": recipient})).await?;
                return Err(Error::refused(
                    "duty",
                    format!("mudcat refuses the export: the model has the duty \"share only with approved users\" from the source {}, and {recipient} is not an approved user", d["source"].as_str().unwrap_or("?")),
                    json!({"duty": d}),
                ));
            }
        }
    }
    tx.sess.audit("export", "export", &format!("version:{model_version}"), true, None, json!({"recipient": recipient})).await?;
    Ok(())
}
