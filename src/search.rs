//! Search (architecture.md, "Search" and "Index builders"). The index is derived: an index builder
//! fills it from the change stream and the registered manifests, and can rebuild it while the old
//! index serves queries (OP-2, OP-3). Before mudcat returns a result, the catalog checks access
//! control and the invalidation set again, because the index can be behind (EV-11).

use crate::authz;
use crate::error::Result;
use crate::instance::{Principal, Session, Tx};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use uuid::Uuid;

pub const EMBED_MODEL: &str = "mudcat-hash-embed/1";
const DIM: usize = 64;

/// Split names at separators and at changes of letter case: `rawFrames_2026` -> "raw frames 2026".
pub fn split_name(s: &str) -> String {
    let mut out = String::new();
    let mut prev: Option<char> = None;
    for ch in s.chars() {
        if !ch.is_alphanumeric() {
            out.push(' ');
        } else {
            if let Some(p) = prev {
                if (p.is_lowercase() && ch.is_uppercase()) || (p.is_alphabetic() && ch.is_numeric()) || (p.is_numeric() && ch.is_alphabetic()) {
                    out.push(' ');
                }
            }
            out.extend(ch.to_lowercase());
        }
        prev = Some(ch);
    }
    out.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn tokens(text: &str) -> Vec<String> {
    split_name(text).split(' ').filter(|t| t.len() > 1).map(|t| t.to_string()).collect()
}

/// A text embedding: a hashed bag of words and character trigrams, normalised. It stands in for an
/// embedding model; each vector records the version of the model (EMBED_MODEL).
pub fn embed(text: &str) -> Vec<f32> {
    let mut v = vec![0f32; DIM];
    for t in tokens(text) {
        let h = crate::instance::sha256_hex(t.as_bytes());
        let i = usize::from_str_radix(&h[..4], 16).unwrap() % DIM;
        v[i] += 1.0;
        let padded = format!(" {t} ");
        let chars: Vec<char> = padded.chars().collect();
        for w in chars.windows(3) {
            let g: String = w.iter().collect();
            let h = crate::instance::sha256_hex(g.as_bytes());
            let i = usize::from_str_radix(&h[..4], 16).unwrap() % DIM;
            v[i] += 0.3;
        }
    }
    let n = v.iter().map(|x| x * x).sum::<f32>().sqrt();
    if n > 0.0 {
        for x in v.iter_mut() {
            *x /= n;
        }
    }
    v
}

pub fn cosine(a: &[f32], b: &[f32]) -> f32 {
    a.iter().zip(b).map(|(x, y)| x * y).sum()
}

async fn active_version(tx: &Tx<'_>) -> Result<i32> {
    tx.exec("insert into index_state (name, active_version, position) values ('search', 1, 0) on conflict do nothing", &[]).await?;
    Ok(tx.q1("select active_version from index_state where name = 'search'", &[]).await?.get(0))
}

#[derive(Clone, Debug)]
struct Doc {
    id: Uuid,
    kind: String,
    parent: Option<Uuid>,
    doc: Value,
    body: String,
    embedding: Vec<f32>,
    acl_root: Option<Uuid>,
    revision: i32,
}

async fn upsert(tx: &Tx<'_>, version: i32, d: &Doc) -> Result<()> {
    tx.exec(
        "insert into search_doc (index_version, entity_id, kind, parent_id, doc, body, tsv, embedding, acl_root, revision)
         values ($1,$2,$3,$4,$5,$6, to_tsvector('english', $6), $7, $8, $9)
         on conflict (tenant_id, index_version, entity_id) do update set kind = excluded.kind, parent_id = excluded.parent_id, doc = excluded.doc,
           body = excluded.body, tsv = excluded.tsv, embedding = excluded.embedding, acl_root = excluded.acl_root, revision = excluded.revision",
        &[&version, &d.id, &d.kind, &d.parent, &d.doc, &d.body, &d.embedding, &d.acl_root, &d.revision],
    )
    .await?;
    Ok(())
}

fn words_of(body: &str) -> BTreeSet<String> {
    tokens(body).into_iter().filter(|w| w.len() > 3 && w.chars().all(|c| c.is_alphabetic())).collect()
}

/// Add words to the vocabulary, in sorted order, so that concurrent builders lock them in the
/// same order.
async fn add_words(tx: &Tx<'_>, words: &BTreeSet<String>) -> Result<()> {
    let w: Vec<String> = words.iter().cloned().collect();
    for chunk in w.chunks(5000) {
        let c: Vec<String> = chunk.to_vec();
        tx.exec("insert into search_word (word) select unnest($1::text[]) order by 1 on conflict do nothing", &[&c]).await?;
    }
    Ok(())
}

fn text_of(v: &Value, out: &mut String) {
    match v {
        Value::String(s) => {
            out.push_str(s);
            out.push(' ');
        }
        Value::Array(a) => a.iter().for_each(|x| text_of(x, out)),
        Value::Object(o) => o.iter().filter(|(k, _)| !k.starts_with('$')).for_each(|(_, x)| text_of(x, out)),
        Value::Number(n) => {
            out.push_str(&n.to_string());
            out.push(' ');
        }
        _ => {}
    }
}

/// The search document of an artifact or a dataset, from its state document.
async fn entity_doc(tx: &Tx<'_>, id: Uuid) -> Result<Option<Doc>> {
    let st = crate::stream::state_document(tx, id).await?;
    if st["removed"] == true {
        return Ok(None);
    }
    let kind = st["kind"].as_str().unwrap_or("").to_string();
    if !["artifact", "dataset", "collection"].contains(&kind.as_str()) {
        return Ok(None);
    }
    let mut body = String::new();
    if let Some(p) = st["path"].as_str() {
        body.push_str(&split_name(p));
        body.push(' ');
    }
    text_of(&st["metadata"], &mut body);
    text_of(&st["metadata_version"], &mut body);
    text_of(&st["tags"], &mut body);
    text_of(&st["structure"]["fields"], &mut body);
    let acl_root = st["access"]["acl_root"].as_str().and_then(|s| s.parse().ok());
    let revision = st["revision"].as_i64().unwrap_or(0) as i32;
    let parent = st["latest_version"]["id"].as_str().and_then(|s| s.parse().ok());
    Ok(Some(Doc { id, kind, parent, embedding: embed(&body), body, doc: st, acl_root, revision }))
}

/// The documents of the annotation entries (captions, chunks) of the annotation sets in the head
/// of the main branch of a dataset. Each one has the version that it describes as its parent.
async fn annotation_docs(tx: &Tx<'_>, dataset: Uuid) -> Result<Vec<Doc>> {
    let head = crate::datasets::resolve_ref(tx, dataset, "main", None).await?;
    let sets: Vec<Uuid> = crate::datasets::members(tx, head).await?.iter().filter(|e| e.member_kind == "annotation-set").map(|e| e.member_id).collect();
    let mut out = Vec::new();
    let tenant = tx.sess.tenant_id()?;
    let mut roots: HashMap<Uuid, (Option<Uuid>, Vec<(String, String)>)> = HashMap::new();
    for s in sets {
        let info = crate::datasets::annotation_set_info(tx, s).await?;
        let rows = tx.inst().manifests.read(tenant, s)?;
        for r in rows {
            let Some(text) = r["text"].as_str().or_else(|| r["caption"].as_str()) else { continue };
            let Some(v) = r["version"].as_str().and_then(|s| s.parse::<Uuid>().ok()) else { continue };
            if !roots.contains_key(&v) {
                let root = crate::metadata::acl_root_of(tx, v).await?;
                let tags = crate::metadata::effective_tags(tx, v).await?;
                roots.insert(v, (root, tags));
            }
            let (root, tags) = roots[&v].clone();
            let id = r["id"].as_str().and_then(|s| s.parse().ok()).unwrap_or_else(|| authz::uuid_for(&format!("{s}/{}/{}", v, r["selector"])));
            let embedding = match r["embedding"].as_array() {
                Some(e) => e.iter().map(|x| x.as_f64().unwrap_or(0.0) as f32).collect(),
                None => embed(text),
            };
            let mut body = text.to_string();
            for (n, val) in &tags {
                body.push_str(&format!(" {n} {val}"));
            }
            let doc = json!({"id": id, "kind": "annotation", "version": v, "selector": r["selector"], "text": text, "set": info["name"], "set_version": info["version"],
                             "dataset": dataset, "tags": tags.iter().map(|(n, v)| json!({"name": n, "value": v})).collect::<Vec<_>>(), "embedding_model": EMBED_MODEL});
            out.push(Doc { id, kind: "annotation".into(), parent: Some(v), doc, body, embedding, acl_root: root, revision: info["version"].as_i64().unwrap_or(1) as i32 });
        }
    }
    Ok(out)
}

async fn refresh_entity(tx: &Tx<'_>, version: i32, id: Uuid, words: &mut BTreeSet<String>) -> Result<()> {
    match entity_doc(tx, id).await? {
        Some(d) => {
            words.extend(words_of(&d.body));
            upsert(tx, version, &d).await
        }
        None => {
            tx.exec("delete from search_doc where index_version = $1 and entity_id = $2", &[&version, &id]).await?;
            Ok(())
        }
    }
}

async fn refresh_dataset_annotations(tx: &Tx<'_>, version: i32, dataset: Uuid) -> Result<()> {
    tx.exec("delete from search_doc where index_version = $1 and kind = 'annotation' and doc->>'dataset' = $2", &[&version, &dataset.to_string()]).await?;
    let mut words = BTreeSet::new();
    for d in annotation_docs(tx, dataset).await? {
        words.extend(words_of(&d.body));
        upsert(tx, version, &d).await?;
    }
    add_words(tx, &words).await
}

/// Bring the active index up to date with the change stream. Returns the number of events applied.
pub async fn build(sess: &Session) -> Result<usize> {
    let sys = sess.as_principal(Principal::system());
    crate::stream::flush(&sys).await?;
    let mut c = sys.client().await?;
    let tx = sys.begin(&mut c).await?;
    let version = active_version(&tx).await?;
    let pos: i64 = tx.q1("select position from index_state where name = 'search'", &[]).await?.get(0);
    let rows = tx.q("select position, subject, type, data from event where position > $1 order by position", &[&pos]).await?;
    let mut last = pos;
    let mut entities = BTreeSet::new();
    let mut datasets = BTreeSet::new();
    // the main transaction writes only after the workers finish, so that it holds no row lock
    // that a worker waits for
    let mut removed_parents: Vec<Uuid> = Vec::new();
    let mut removed_docs: Vec<Uuid> = Vec::new();
    for r in &rows {
        last = r.get(0);
        let subject: Uuid = r.get(1);
        let typ: String = r.get(2);
        let data: Value = r.get(3);
        let kind = data["kind"].as_str().unwrap_or("");
        match kind {
            "artifact" | "dataset" | "collection" => {
                entities.insert(subject);
            }
            "version" => {
                if let Some(a) = data["data"]["artifact"].as_str().and_then(|s| s.parse().ok()) {
                    entities.insert(a);
                } else if let Some(r) = tx.qopt("select artifact_id from version where id = $1", &[&subject]).await? {
                    entities.insert(r.get::<_, Uuid>(0));
                }
                if typ.starts_with("mudcat.removal") {
                    removed_parents.push(subject);
                }
            }
            "chunk" => {
                removed_docs.push(subject);
            }
            "snapshot" | "annotation-set" => {
                if let Some(d) = data["data"]["dataset"].as_str().and_then(|s| s.parse().ok()) {
                    datasets.insert(d);
                }
            }
            _ => {}
        }
    }
    // the documents are independent: build them with several workers, each in its own transaction
    let entities: Vec<Uuid> = entities.into_iter().collect();
    let workers = if entities.len() > 500 { 4 } else { 1 };
    let chunk = entities.len().div_ceil(workers).max(1);
    let mut handles = Vec::new();
    for part in entities.chunks(chunk) {
        let part = part.to_vec();
        let s = sys.clone();
        handles.push(tokio::spawn(async move {
            let mut c = s.client().await?;
            let tx = s.begin(&mut c).await?;
            let mut words = BTreeSet::new();
            for e in part {
                refresh_entity(&tx, version, e, &mut words).await?;
            }
            add_words(&tx, &words).await?;
            tx.commit().await
        }));
    }
    for h in handles {
        h.await.map_err(|e| crate::Error::invalid(e.to_string()))??;
    }
    tx.exec("delete from search_doc where index_version = $1 and parent_id = any($2)", &[&version, &removed_parents]).await?;
    tx.exec("delete from search_doc where index_version = $1 and entity_id = any($2)", &[&version, &removed_docs]).await?;
    for d in &datasets {
        refresh_dataset_annotations(&tx, version, *d).await?;
    }
    tx.exec("update index_state set position = $1 where name = 'search'", &[&last]).await?;
    tx.commit().await?;
    Ok(rows.len())
}

/// Build a new version of the index from the catalog and the manifests, while the old version
/// serves queries; then switch to it and drop the old one (OP-3).
pub async fn rebuild(sess: &Session) -> Result<i32> {
    let sys = sess.as_principal(Principal::system());
    crate::stream::flush(&sys).await?;
    let mut c = sys.client().await?;
    let tx = sys.begin(&mut c).await?;
    let old = active_version(&tx).await?;
    let new = old + 1;
    let pos: i64 = tx.qopt("select last_position from stream_state", &[]).await?.map(|r| r.get(0)).unwrap_or(0);
    tx.exec("delete from search_doc where index_version = $1", &[&new]).await?;
    let mut words = BTreeSet::new();
    for r in tx.q("select id from artifact union all select id from dataset union all select id from collection", &[]).await? {
        if let Some(d) = entity_doc(&tx, r.get(0)).await? {
            words.extend(words_of(&d.body));
            upsert(&tx, new, &d).await?;
        }
    }
    for r in tx.q("select id from dataset", &[]).await? {
        for d in annotation_docs(&tx, r.get(0)).await? {
            words.extend(words_of(&d.body));
            upsert(&tx, new, &d).await?;
        }
    }
    add_words(&tx, &words).await?;
    tx.exec("update index_state set active_version = $1, position = $2 where name = 'search'", &[&new, &pos]).await?;
    tx.exec("delete from search_doc where index_version = $1", &[&old]).await?;
    tx.commit().await?;
    Ok(new)
}

/// The documents of the active index, for a comparison before and after a rebuild (INV-16).
pub async fn snapshot_index(sess: &Session) -> Result<BTreeMap<Uuid, (String, String)>> {
    let mut c = sess.client().await?;
    let tx = sess.begin(&mut c).await?;
    let v = active_version(&tx).await?;
    let rows = tx.q("select entity_id, kind, body from search_doc where index_version = $1", &[&v]).await?;
    Ok(rows.iter().map(|r| (r.get(0), (r.get(1), r.get(2)))).collect())
}

fn docs_root(doc: &Value) -> Option<Uuid> {
    doc["acl_root"].as_str().and_then(|s| s.parse().ok())
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct SearchRequest {
    #[serde(default)]
    pub text: Option<String>,
    /// "More like this one": an entity or a version.
    #[serde(default)]
    pub similar_to: Option<Uuid>,
    /// {"tag": {"name", "value"}} or {"kind": "..."}
    #[serde(default)]
    pub filter: Option<Value>,
    #[serde(default)]
    pub limit: Option<usize>,
    /// Exclude metadata that a model inferred.
    #[serde(default)]
    pub exclude_inferred: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Hit {
    pub entity: Uuid,
    pub kind: String,
    pub version: Option<Uuid>,
    pub path: Option<String>,
    pub score: f64,
    pub matched: Value,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct SearchResult {
    pub hits: Vec<Hit>,
    /// Results that the index gave and that the check against the catalog removed.
    pub removed_by_check: usize,
    pub ranking: String,
}

/// Search with lexical, tolerant and vector match, combined by reciprocal rank fusion (EV-9,
/// EV-10). The index filters on permissions; then the catalog checks each result (EV-11).
pub async fn search(sess: &Session, req: &SearchRequest) -> Result<SearchResult> {
    let mut c = sess.client().await?;
    let tx = sess.begin(&mut c).await?;
    let version = active_version(&tx).await?;
    let roots: Option<Vec<Uuid>> = authz::readable_roots(&tx).await?;
    let limit = req.limit.unwrap_or(20);
    let tag_filter = req.filter.as_ref().and_then(|f| f.get("tag")).map(|t| json!([{"name": t["name"], "value": t["value"]}]));
    let kind_filter = req.filter.as_ref().and_then(|f| f["kind"].as_str().map(|s| s.to_string()));
    let mut rank_lists: Vec<Vec<Uuid>> = Vec::new();
    let mut docs: HashMap<Uuid, (String, Option<Uuid>, Value, Vec<f32>)> = HashMap::new();
    // the coarse filter on permissions is applied to the candidates below, so that a denial
    // reaches the audit log; $2 stays in the statements and is always null
    let roots_all = roots.clone();
    let roots: Option<Vec<Uuid>> = None;
    let base_filter = "index_version = $1 and ($2::uuid[] is null or acl_root = any($2)) and ($3::jsonb is null or doc->'tags' @> $3) and ($4::text is null or kind = $4)";
    if let Some(text) = &req.text {
        let q = tokens(text).join(" ");
        let lex = tx
            .q(
                &format!("select entity_id, kind, parent_id, doc, embedding from search_doc where {base_filter} and tsv @@ plainto_tsquery('english', $5)
                          order by ts_rank(tsv, plainto_tsquery('english', $5)) desc limit 300"),
                &[&version, &roots, &tag_filter, &kind_filter, &q],
            )
            .await;
        // plainto_tsquery needs all words; also an "any word" query for recall
        let any_q = tokens(text).join(" | ");
        let lex_any = tx
            .q(
                &format!("select entity_id, kind, parent_id, doc, embedding from search_doc where {base_filter} and tsv @@ to_tsquery('english', $5)
                          order by ts_rank(tsv, to_tsquery('english', $5)) desc limit 300"),
                &[&version, &roots, &tag_filter, &kind_filter, &any_q],
            )
            .await?;
        let mut list = Vec::new();
        for r in lex.unwrap_or_default().iter().chain(lex_any.iter()) {
            let id: Uuid = r.get(0);
            if !list.contains(&id) {
                list.push(id);
            }
            docs.insert(id, (r.get(1), r.get(2), r.get(3), r.get::<_, Option<Vec<f32>>>(4).unwrap_or_default()));
        }
        rank_lists.push(list);
        // tolerant: words of the vocabulary that are close to a word of the query (trigram
        // similarity), for spelling errors; the expanded query then runs as a lexical query
        tx.exec("select set_limit(0.4)", &[]).await?;
        let mut alternatives = Vec::new();
        for t in tokens(text).into_iter().filter(|t| t.len() > 3) {
            let close: Vec<String> = tx
                .q("select word from search_word where word % $1 order by similarity(word, $1) desc limit 3", &[&t])
                .await?
                .iter()
                .map(|r| r.get(0))
                .collect();
            let mut alts = vec![t.clone()];
            alts.extend(close.into_iter().filter(|w| *w != t));
            alternatives.push(format!("({})", alts.join(" | ")));
        }
        if !alternatives.is_empty() {
            let tq = alternatives.join(" | ");
            let tol = tx
                .q(
                    &format!("select entity_id, kind, parent_id, doc, embedding from search_doc where {base_filter} and tsv @@ to_tsquery('english', $5)
                              order by ts_rank(tsv, to_tsquery('english', $5)) desc limit 300"),
                    &[&version, &roots, &tag_filter, &kind_filter, &tq],
                )
                .await?;
            let mut list = Vec::new();
            for r in &tol {
                let id: Uuid = r.get(0);
                list.push(id);
                docs.insert(id, (r.get(1), r.get(2), r.get(3), r.get::<_, Option<Vec<f32>>>(4).unwrap_or_default()));
            }
            rank_lists.push(list);
        }
        // semantic: vector similarity over the candidates
        let qv = embed(text);
        let mut scored: Vec<(Uuid, f32)> = docs.iter().map(|(id, d)| (*id, cosine(&qv, &d.3))).collect();
        scored.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap());
        rank_lists.push(scored.into_iter().map(|s| s.0).collect());
    }
    if let Some(sim) = req.similar_to {
        let target = tx
            .qopt("select embedding from search_doc where index_version = $1 and (entity_id = $2 or parent_id = $2) limit 1", &[&version, &sim])
            .await?;
        if let Some(t) = target {
            let tv: Vec<f32> = t.get::<_, Option<Vec<f32>>>(0).unwrap_or_default();
            let rows = tx
                .q(&format!("select entity_id, kind, parent_id, doc, embedding from search_doc where {base_filter} limit 50000"), &[&version, &roots, &tag_filter, &kind_filter])
                .await?;
            let mut scored = Vec::new();
            for r in &rows {
                let id: Uuid = r.get(0);
                let parent: Option<Uuid> = r.get(2);
                if id == sim || parent == Some(sim) {
                    continue;
                }
                let e: Vec<f32> = r.get::<_, Option<Vec<f32>>>(4).unwrap_or_default();
                scored.push((id, cosine(&tv, &e)));
                docs.insert(id, (r.get(1), parent, r.get(3), e));
            }
            scored.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap());
            rank_lists.push(scored.into_iter().take(500).map(|s| s.0).collect());
        }
    }
    if req.text.is_none() && req.similar_to.is_none() {
        let rows = tx
            .q(&format!("select entity_id, kind, parent_id, doc, embedding from search_doc where {base_filter} order by entity_id limit 500"), &[&version, &roots, &tag_filter, &kind_filter])
            .await?;
        let mut list = Vec::new();
        for r in &rows {
            let id: Uuid = r.get(0);
            list.push(id);
            docs.insert(id, (r.get(1), r.get(2), r.get(3), vec![]));
        }
        rank_lists.push(list);
    }
    // the filter on permissions: the ACL roots that the subject can read
    let subj = authz::subjects(&tx).await?;
    let coarse_reason = if subj.stale.is_empty() {
        "the subject can read no ACL root of the document".to_string()
    } else {
        format!("the subject can read no ACL root of the document; synchronised membership is older than its limit: {}", Value::Array(subj.stale.clone()))
    };
    let mut denied_by_filter = 0usize;
    let mut coarse_denied: Vec<Value> = Vec::new();
    let roots_set: Option<HashSet<Uuid>> = roots_all.map(|r| r.into_iter().collect());
    if let Some(rs) = &roots_set {
        let ids: Vec<Uuid> = docs.keys().copied().collect();
        let root_of: HashMap<Uuid, Option<Uuid>> = tx
            .q("select entity_id, acl_root from search_doc where index_version = $1 and entity_id = any($2)", &[&version, &ids])
            .await?
            .iter()
            .map(|r| (r.get(0), r.get(1)))
            .collect();
        let mut audited = 0;
        for list in rank_lists.iter_mut() {
            let mut keep = Vec::new();
            for id in list.iter() {
                let root = root_of.get(id).copied().flatten().or_else(|| docs_root(&docs[id].2));
                if root.map(|r| rs.contains(&r)).unwrap_or(false) {
                    keep.push(*id);
                } else {
                    denied_by_filter += 1;
                    if audited < 20 {
                        audited += 1;
                        coarse_denied.push(json!({"object": format!("search-doc:{id}"), "reason": coarse_reason, "cached": false}));
                    }
                }
            }
            *list = keep;
        }
    }
    // reciprocal rank fusion
    let mut score: HashMap<Uuid, f64> = HashMap::new();
    for list in &rank_lists {
        for (i, id) in list.iter().enumerate() {
            *score.entry(*id).or_default() += 1.0 / (60.0 + i as f64 + 1.0);
        }
    }
    let mut ranked: Vec<(Uuid, f64)> = score.into_iter().collect();
    ranked.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap().then(a.0.cmp(&b.0)));

    // the check against the catalog, for each result (EV-11), as one bulk check
    let mut allowed_log: Vec<Value> = Vec::new();
    let mut denied_log: Vec<Value> = coarse_denied;
    let mut hits = Vec::new();
    let mut removed = 0;
    let mut seen_parents = HashSet::new();
    let invalid_cache: HashSet<Uuid> = {
        let parents: Vec<Uuid> = ranked.iter().filter_map(|(id, _)| docs[id].1).collect();
        crate::policy::invalidated_versions(&tx, &parents).await?.into_iter().collect()
    };
    for (id, s) in ranked {
        if hits.len() >= limit {
            break;
        }
        let (kind, parent, doc, _) = docs[&id].clone();
        let hit = match kind.as_str() {
            "annotation" | "artifact" => {
                let Some(mut v) = parent else { continue };
                if invalid_cache.contains(&v) {
                    removed += 1;
                    continue;
                }
                let Ok(info) = crate::namespace::version(&tx, v).await else {
                    removed += 1;
                    continue;
                };
                // the newest state: a delete marker hides the artifact; a new version replaces the old
                let latest: Option<Uuid> = tx.q1("select latest_version_id from artifact where id = $1", &[&info.artifact_id]).await?.get(0);
                if let Some(l) = latest {
                    if l != v {
                        let li = crate::namespace::version(&tx, l).await?;
                        if li.is_delete_marker || crate::policy::is_invalidated(&tx, l).await? {
                            removed += 1;
                            continue;
                        }
                        v = l;
                    }
                }
                let d = authz::check_as(&tx, &subj, "read", "version", v).await?;
                if !d.allow {
                    removed += 1;
                    denied_log.push(json!({"object": format!("version:{v}"), "reason": d.reason, "cached": false}));
                    continue;
                }
                if !seen_parents.insert(v) {
                    continue;
                }
                allowed_log.push(json!({"object": format!("version:{v}"), "via": d.via, "membership_age_secs": d.membership_age_secs}));
                Hit { entity: info.artifact_id, kind: "artifact".into(), version: Some(v), path: Some(crate::namespace::version(&tx, v).await?.path), score: s, matched: if kind == "annotation" { json!({"text": doc["text"], "selector": doc["selector"], "set": doc["set"], "set_version": doc["set_version"], "annotation": id}) } else { json!({"path": doc["path"]}) } }
            }
            "dataset" => {
                let d = authz::check_as(&tx, &subj, "read", "dataset", id).await.unwrap_or_default();
                if !d.allow {
                    removed += 1;
                    continue;
                }
                Hit { entity: id, kind, version: None, path: doc["path"].as_str().map(|s| s.to_string()), score: s, matched: json!({}) }
            }
            "collection" => {
                let d = authz::check_as(&tx, &subj, "read", "collection", id).await.unwrap_or_default();
                if !d.allow {
                    removed += 1;
                    continue;
                }
                Hit { entity: id, kind, version: None, path: doc["path"].as_str().map(|s| s.to_string()), score: s, matched: json!({}) }
            }
            _ => continue,
        };
        hits.push(hit);
    }
    // the decisions of the search, in the audit log: one entry for the denials, one for the grants
    let age = allowed_log.iter().filter_map(|a| a["membership_age_secs"].as_i64()).max();
    if !denied_log.is_empty() {
        let reason = denied_log[0]["reason"].as_str().unwrap_or("").to_string();
        sess.audit("search", "read", &format!("search-results:{}", denied_log.len()), false, Some(&reason), json!({"decisions": denied_log, "cached": false})).await?;
    }
    if !allowed_log.is_empty() {
        sess.audit("search", "read", &format!("search-results:{}", allowed_log.len()), true, Some("granted"), json!({"decisions": allowed_log, "membership_age_secs": age, "cached": false})).await?;
    }
    Ok(SearchResult { hits, removed_by_check: removed + denied_by_filter, ranking: "reciprocal rank fusion of lexical, tolerant (trigram) and vector ranks".into() })
}
