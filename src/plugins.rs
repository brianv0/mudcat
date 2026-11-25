//! Format plugins: enrichers that read the bytes, in a site agent, and produce a structure
//! descriptor, extracted statistics and, where the structure enumerates them, the element list
//! (architecture.md, "Format plugins and site agents").
//!
//! The simulated files of this repository carry a one-line JSON header that stands in for the
//! header or index of a real format (an MP4 `moov` box, a Parquet footer, Zarr metadata).

use crate::instance::sha256_hex;
use serde_json::{json, Value};

pub const PLUGIN_VERSION: &str = "mudcat-formats/0.1.0";

/// (structure descriptor, element list, statistics)
pub fn extract(key: &str, data: &[u8]) -> (Value, Option<Vec<Value>>, Value) {
    let text = std::str::from_utf8(data).unwrap_or("");
    let mut lines = text.lines();
    let header: Option<Value> = lines.next().and_then(|l| serde_json::from_str(l).ok()).filter(|v: &Value| v.get("kind").is_some());
    let ext = key.rsplit('.').next().unwrap_or("").to_lowercase();
    let mut structure = match &header {
        Some(h) => h.clone(),
        None => json!({"kind": match ext.as_str() {
            "mp4" | "mkv" | "mcap" | "bag" | "wav" | "flac" => "stream",
            "parquet" | "csv" | "jsonl" | "lance" | "tfrecord" => "records",
            "zarr" | "nc" => "array",
            "tar" | "zip" | "h5" | "hdf5" => "container",
            _ => "opaque",
        }}),
    };
    structure["plugin"] = json!(PLUGIN_VERSION);
    structure["format"] = json!(ext);
    let kind = structure["kind"].as_str().unwrap_or("opaque").to_string();
    let mut stats = json!({});
    if let Some(d) = structure["duration_s"].as_f64() {
        stats["duration_s"] = json!(d);
        stats["time"] = json!([0.0, d]);
    }
    let elements = if kind == "records" || kind == "container" {
        let rows: Vec<Value> = lines
            .enumerate()
            .map(|(i, l)| {
                let v: Value = serde_json::from_str(l).unwrap_or(json!({}));
                let id = v["id"].as_str().map(|s| s.to_string()).unwrap_or_else(|| format!("{i}"));
                let mut e = json!({"id": id, "selector": {"type": if kind == "records" { "row-range" } else { "member-path" }, "row": i}, "checksum": sha256_hex(l.as_bytes()), "size": l.len()});
                if let Some(f) = v["fingerprint"].as_str() {
                    e["fingerprint"] = json!(f);
                }
                e
            })
            .collect();
        stats["rows"] = json!(rows.len());
        Some(rows)
    } else {
        None
    };
    (structure, elements, stats)
}
