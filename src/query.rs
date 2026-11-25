//! The query language (architecture.md, "Query compiler"). It extends Datacat's: a path glob and
//! a predicate, for example
//!
//! ```text
//! /raw/** where size > 6200 and status =~ "valid*" and permitted(train, purpose=commercial)
//! ```
//!
//! Queries compile to SQL with parameters. Declared attributes with the `indexed` flag use the
//! typed attribute index; other declared attributes use their payloads; undeclared attributes
//! permit equality only (model.md, section 3). Results are versions (the latest of each artifact
//! by default), in pages with a cursor, filtered by the ACL roots that the caller can read.

use crate::error::{Error, Result};
use crate::instance::Tx;
use serde_json::{json, Value};
use tokio_postgres::types::ToSql;
use uuid::Uuid;

#[derive(Clone, Debug, PartialEq)]
enum Tok {
    Ident(String),
    Str(String),
    Num(f64),
    Op(String),
    LParen,
    RParen,
    Comma,
}

fn lex(s: &str) -> Result<Vec<Tok>> {
    let c: Vec<char> = s.chars().collect();
    let mut i = 0;
    let mut out = Vec::new();
    while i < c.len() {
        let ch = c[i];
        if ch.is_whitespace() {
            i += 1;
        } else if ch == '(' {
            out.push(Tok::LParen);
            i += 1;
        } else if ch == ')' {
            out.push(Tok::RParen);
            i += 1;
        } else if ch == ',' {
            out.push(Tok::Comma);
            i += 1;
        } else if ch == '"' || ch == '\'' {
            let q = ch;
            i += 1;
            let start = i;
            while i < c.len() && c[i] != q {
                i += 1;
            }
            out.push(Tok::Str(c[start..i].iter().collect()));
            i += 1;
        } else if "=!<>~".contains(ch) {
            let start = i;
            while i < c.len() && "=!<>~".contains(c[i]) {
                i += 1;
            }
            out.push(Tok::Op(c[start..i].iter().collect()));
        } else if ch.is_ascii_digit() || (ch == '-' && i + 1 < c.len() && c[i + 1].is_ascii_digit()) {
            let start = i;
            i += 1;
            while i < c.len() && (c[i].is_ascii_digit() || c[i] == '.' || c[i] == 'e') {
                i += 1;
            }
            let t: String = c[start..i].iter().collect();
            out.push(Tok::Num(t.parse().map_err(|_| Error::invalid(format!("not a number: {t}")))?));
        } else {
            let start = i;
            while i < c.len() && (c[i].is_alphanumeric() || "_./*-:@".contains(c[i])) {
                i += 1;
            }
            if i == start {
                return Err(Error::invalid(format!("unexpected character {ch:?}")));
            }
            out.push(Tok::Ident(c[start..i].iter().collect()));
        }
    }
    Ok(out)
}

#[derive(Clone, Debug)]
enum Expr {
    And(Box<Expr>, Box<Expr>),
    Or(Box<Expr>, Box<Expr>),
    Not(Box<Expr>),
    Cmp(String, String, Tok),
    Call(String, Vec<(Option<String>, Tok)>),
}

struct Parser {
    t: Vec<Tok>,
    i: usize,
}

impl Parser {
    fn peek(&self) -> Option<&Tok> {
        self.t.get(self.i)
    }
    fn next(&mut self) -> Option<Tok> {
        let t = self.t.get(self.i).cloned();
        self.i += 1;
        t
    }
    fn word(&self, w: &str) -> bool {
        matches!(self.peek(), Some(Tok::Ident(x)) if x.eq_ignore_ascii_case(w))
    }
    fn or(&mut self) -> Result<Expr> {
        let mut l = self.and()?;
        while self.word("or") {
            self.i += 1;
            l = Expr::Or(Box::new(l), Box::new(self.and()?));
        }
        Ok(l)
    }
    fn and(&mut self) -> Result<Expr> {
        let mut l = self.unary()?;
        while self.word("and") {
            self.i += 1;
            l = Expr::And(Box::new(l), Box::new(self.unary()?));
        }
        Ok(l)
    }
    fn unary(&mut self) -> Result<Expr> {
        if self.word("not") {
            self.i += 1;
            return Ok(Expr::Not(Box::new(self.unary()?)));
        }
        if self.peek() == Some(&Tok::LParen) {
            self.i += 1;
            let e = self.or()?;
            if self.next() != Some(Tok::RParen) {
                return Err(Error::invalid("a parenthesis is not closed"));
            }
            return Ok(e);
        }
        let Some(Tok::Ident(name)) = self.next() else { return Err(Error::invalid("a condition starts with a name")) };
        if self.peek() == Some(&Tok::LParen) {
            self.i += 1;
            let mut args = Vec::new();
            while self.peek() != Some(&Tok::RParen) {
                let a = self.next().ok_or_else(|| Error::invalid("a call is not closed"))?;
                if let (Tok::Ident(k), Some(Tok::Op(o))) = (&a, self.peek()) {
                    if o == "=" {
                        self.i += 1;
                        let v = self.next().ok_or_else(|| Error::invalid("no value"))?;
                        args.push((Some(k.clone()), v));
                    } else {
                        args.push((None, a));
                    }
                } else {
                    args.push((None, a));
                }
                if self.peek() == Some(&Tok::Comma) {
                    self.i += 1;
                }
            }
            self.i += 1;
            return Ok(Expr::Call(name.to_lowercase(), args));
        }
        let Some(Tok::Op(op)) = self.next() else { return Err(Error::invalid(format!("no operator after {name}"))) };
        let v = self.next().ok_or_else(|| Error::invalid(format!("no value after {name} {op}")))?;
        Ok(Expr::Cmp(name, op, v))
    }
}

struct Compiler<'a> {
    params: Vec<Box<dyn ToSql + Sync + Send>>,
    defs: &'a std::collections::HashMap<String, (Uuid, String, bool)>,
    /// for each condition on the typed index: true if few entities match it
    selective: std::collections::HashMap<String, bool>,
}

/// The number of entities above which a condition is not selective: the query then probes the
/// index for each row in key order, and below it the query starts from the index.
const SELECTIVE: i64 = 20_000;

fn cmp_key(id: Uuid, op: &str, v: &Tok) -> String {
    format!("{id}|{op}|{}", tok_str(v))
}

/// The conditions of an expression on the typed attribute index.
fn indexed_conditions(e: &Expr, defs: &std::collections::HashMap<String, (Uuid, String, bool)>, out: &mut Vec<(Uuid, String, String, Tok)>) {
    match e {
        Expr::And(a, b) | Expr::Or(a, b) => {
            indexed_conditions(a, defs, out);
            indexed_conditions(b, defs, out);
        }
        Expr::Not(a) => indexed_conditions(a, defs, out),
        Expr::Cmp(name, op, v) => {
            let key = name.rsplit('.').next().unwrap().to_string();
            if let Some((id, dt, true)) = defs.get(name).or_else(|| defs.get(&key)).cloned() {
                out.push((id, dt, op.clone(), v.clone()));
            }
        }
        Expr::Call(..) => {}
    }
}

const COLUMNS: &[(&str, &str, &str)] = &[
    ("size", "v.size", "number"),
    ("number", "v.number", "number"),
    ("elements", "v.element_count", "number"),
    ("name", "a.name", "string"),
    ("path", "(c.path || '/' || a.name)", "string"),
    ("kind", "a.kind", "string"),
    ("structure", "(v.structure->>'kind')", "string"),
    ("content_hash", "v.content_hash", "string"),
];

fn sql_op(op: &str) -> Result<&'static str> {
    Ok(match op {
        "=" | "==" => "=",
        "!=" | "<>" => "<>",
        "<" => "<",
        "<=" => "<=",
        ">" => ">",
        ">=" => ">=",
        "=~" => "like",
        o => return Err(Error::invalid(format!("unknown operator {o}"))),
    })
}

fn glob_to_like(s: &str) -> String {
    s.replace('%', "\\%").replace('_', "\\_").replace('*', "%").replace('?', "_")
}

impl<'a> Compiler<'a> {
    fn param(&mut self, v: Box<dyn ToSql + Sync + Send>) -> String {
        self.params.push(v);
        format!("${}", self.params.len())
    }
    fn value(&mut self, t: &Tok, op: &str) -> Result<(String, &'static str)> {
        match t {
            Tok::Num(n) => Ok((self.param(Box::new(*n)), "number")),
            Tok::Str(s) | Tok::Ident(s) => {
                let s = if op == "=~" { glob_to_like(s) } else { s.clone() };
                Ok((self.param(Box::new(s)), "string"))
            }
            _ => Err(Error::invalid("not a value")),
        }
    }
    fn expr(&mut self, e: &Expr) -> Result<String> {
        match e {
            Expr::And(a, b) => Ok(format!("({} and {})", self.expr(a)?, self.expr(b)?)),
            Expr::Or(a, b) => Ok(format!("({} or {})", self.expr(a)?, self.expr(b)?)),
            Expr::Not(a) => Ok(format!("(not {})", self.expr(a)?)),
            Expr::Cmp(name, op, v) => {
                let o = sql_op(op)?;
                if let Some((_, col, ty)) = COLUMNS.iter().find(|c| c.0 == name) {
                    let (p, vt) = self.value(v, op)?;
                    let col = if *ty == "number" && vt == "number" { format!("{col}::double precision") } else { col.to_string() };
                    return Ok(format!("{col} {o} {p}"));
                }
                // a metadata attribute: ns.name or name
                let key = name.rsplit('.').next().unwrap().to_string();
                match self.defs.get(name).or_else(|| self.defs.get(&key)).cloned() {
                    Some((id, datatype, true)) => {
                        let (p, _) = self.value(v, op)?;
                        let idp = self.param(Box::new(id));
                        let col = match datatype.as_str() {
                            "number" | "integer" => "x.num_value",
                            "datetime" => "x.ts_value",
                            _ => "x.text_value",
                        };
                        let rhs = if datatype == "datetime" { format!("{p}::timestamptz") } else { p };
                        if self.selective.get(&cmp_key(id, op, v)).copied().unwrap_or(false) {
                            // few matches: start from the index; artifact-level values stand for the versions of the artifact
                            Ok(format!(
                                "v.id in (select x.entity_id from attr_index x where x.attr_id = {idp} and {col} {o} {rhs}
                                          union all select v2.id from attr_index x join version v2 on v2.artifact_id = x.entity_id and v2.tenant_id = x.tenant_id
                                           where x.attr_id = {idp} and {col} {o} {rhs})"
                            ))
                        } else {
                            // many matches: probe the index for each row, in key order
                            Ok(format!(
                                "(exists (select 1 from attr_index x where x.attr_id = {idp} and x.entity_id = v.id and {col} {o} {rhs})
                                  or exists (select 1 from attr_index x where x.attr_id = {idp} and x.entity_id = a.id and {col} {o} {rhs}))"
                            ))
                        }
                    }
                    Some((_, datatype, false)) => {
                        let (p, vt) = self.value(v, op)?;
                        let k = self.param(Box::new(key.clone()));
                        let lhs = if vt == "number" && (datatype == "number" || datatype == "integer") { format!("(m.payload->>{k})::double precision") } else { format!("(m.payload->>{k})") };
                        Ok(format!(
                            "(exists (select 1 from metadata_payload m where m.entity_id = v.id and m.status = 'visible' and {lhs} {o} {p})
                              or exists (select 1 from metadata_payload m where m.entity_id = a.id and m.status = 'visible' and {lhs} {o} {p}))"
                        ))
                    }
                    None => {
                        // undeclared: equality and containment only, through the GIN index
                        if o != "=" {
                            return Err(Error::invalid(format!("{name} is not declared; declare it to use {op} on it")));
                        }
                        let doc = match v {
                            Tok::Num(n) => json!({ key.clone(): n }),
                            Tok::Str(s) | Tok::Ident(s) => json!({ key.clone(): s }),
                            _ => Value::Null,
                        };
                        let p = self.param(Box::new(doc));
                        Ok(format!(
                            "(exists (select 1 from metadata_payload m where m.entity_id = v.id and m.status = 'visible' and m.payload @> {p})
                              or exists (select 1 from metadata_payload m where m.entity_id = a.id and m.status = 'visible' and m.payload @> {p}))"
                        ))
                    }
                }
            }
            Expr::Call(f, args) => {
                let pos: Vec<String> = args.iter().filter(|a| a.0.is_none()).map(|a| tok_str(&a.1)).collect();
                let named = |k: &str| args.iter().find(|a| a.0.as_deref() == Some(k)).map(|a| tok_str(&a.1));
                match f.as_str() {
                    // usage policy: a join from the source to the small table of grants
                    "permitted" => {
                        let action = pos.first().cloned().ok_or_else(|| Error::invalid("permitted(action, purpose=...)"))?;
                        let purpose = named("purpose").or_else(|| pos.get(1).cloned()).ok_or_else(|| Error::invalid("permitted needs a purpose"))?;
                        let a = self.param(Box::new(action));
                        let p = self.param(Box::new(purpose));
                        Ok(format!(
                            "(v.source_id in (select g.source_id from usage_grant g where g.action = {a} and (g.purpose = {p} or g.purpose = 'any')
                                      and g.revoked_commit is null and (g.not_after is null or g.not_after >= now()))
                             and not (v.id::text in (select target from invalidation where target_kind = 'version')
                                   or a.id::text in (select target from invalidation where target_kind = 'artifact')
                                   or v.source_id::text in (select target from invalidation where target_kind = 'source')
                                   or coalesce(v.content_hash, '') in (select target from invalidation where target_kind = 'fingerprint')))"
                        ))
                    }
                    // a short fixed path: model -> generating activity -> resolution -> inputs
                    "used_in" => {
                        let model = pos.first().cloned().ok_or_else(|| Error::invalid("used_in(<model>)"))?;
                        let role = named("role").unwrap_or_else(|| "training".into());
                        let m = self.param(Box::new(model));
                        let r = self.param(Box::new(role));
                        Ok(format!(
                            "v.id in (select e.member_id from identifier idf
                                        join version mv on mv.artifact_id = idf.entity_id and mv.tenant_id = idf.tenant_id
                                        join generation g on g.entity_id = mv.id and g.tenant_id = mv.tenant_id
                                        join activity ac on ac.id = g.activity_id and ac.tenant_id = g.tenant_id
                                        join resolution rs on rs.id = ac.resolution_id and rs.tenant_id = ac.tenant_id
                                        cross join lateral jsonb_array_elements(rs.inputs) inp
                                        join manifest_ref mr on mr.snapshot_id = (inp->>'snapshot')::uuid and mr.tenant_id = rs.tenant_id
                                        join manifest_entry e on e.manifest_id = mr.manifest_id and e.tenant_id = mr.manifest_tenant and not (e.member_id = any(mr.excluded))
                                       where idf.scheme = 'path' and idf.value = {m} and idf.valid_to is null and inp->>'role' = {r})"
                        ))
                    }
                    "derived_from" => {
                        let p = self.param(Box::new(pos.first().cloned().unwrap_or_default()));
                        Ok(format!(
                            "v.id in (select d.entity_id from derivation d join identifier idf on idf.scheme = 'path' and idf.value = {p} and idf.valid_to is null
                                       join version src on src.artifact_id = idf.entity_id where d.from_id = src.id)"
                        ))
                    }
                    "tag" => {
                        let n = self.param(Box::new(pos.first().cloned().unwrap_or_default()));
                        let val = self.param(Box::new(pos.get(1).cloned().unwrap_or_default()));
                        Ok(format!("exists (select 1 from tag t where t.entity_id in (v.id, a.id) and t.name = {n} and t.value = {val})"))
                    }
                    other => Err(Error::invalid(format!("unknown function {other}"))),
                }
            }
        }
    }
}

fn tok_str(t: &Tok) -> String {
    match t {
        Tok::Ident(s) | Tok::Str(s) => s.clone(),
        Tok::Num(n) => n.to_string(),
        _ => String::new(),
    }
}

#[derive(Clone, Debug, Default, serde::Serialize, serde::Deserialize)]
pub struct QueryOptions {
    #[serde(default)]
    pub limit: Option<i64>,
    /// The cursor: the last version ID of the previous page.
    #[serde(default)]
    pub after: Option<Uuid>,
    /// all versions, not only the latest of each artifact
    #[serde(default)]
    pub all_versions: bool,
}

/// Compile and run a query. Returns one page and the cursor of the next page.
pub async fn run(tx: &Tx<'_>, q: &str, opts: &QueryOptions) -> Result<Value> {
    let (glob, cond) = match q.find(" where ") {
        Some(i) => (q[..i].trim(), Some(q[i + 7..].trim())),
        None if q.trim_start().starts_with('/') => (q.trim(), None),
        None => ("/**", Some(q.trim())),
    };
    let defs: std::collections::HashMap<String, (Uuid, String, bool)> = tx
        .q("select id, namespace, name, datatype, indexed from attribute_def", &[])
        .await?
        .iter()
        .flat_map(|r| {
            let v = (r.get::<_, Uuid>(0), r.get::<_, String>(3), r.get::<_, bool>(4));
            vec![(format!("{}.{}", r.get::<_, String>(1), r.get::<_, String>(2)), v.clone()), (r.get::<_, String>(2), v)]
        })
        .collect();
    let mut comp = Compiler { params: Vec::new(), defs: &defs, selective: Default::default() };
    let mut wh = Vec::new();
    // the path glob over the collection namespace
    let g = glob.trim_end_matches('/');
    if let Some(base) = g.strip_suffix("/**") {
        let b = if base.is_empty() { "/".to_string() } else { base.to_string() };
        let p1 = comp.param(Box::new(b.clone()));
        let p2 = comp.param(Box::new(format!("{}/%", b.trim_end_matches('/'))));
        wh.push(format!("(c.path = {p1} or c.path like {p2})"));
    } else if let Some(base) = g.strip_suffix("/*") {
        let p = comp.param(Box::new(if base.is_empty() { "/".to_string() } else { base.to_string() }));
        wh.push(format!("c.path = {p}"));
    } else if g.contains('*') {
        let p = comp.param(Box::new(glob_to_like(g)));
        wh.push(format!("(c.path || '/' || a.name) like {p}"));
    } else {
        let p = comp.param(Box::new(g.to_string()));
        wh.push(format!("(c.path = {p} or (c.path || '/' || a.name) = {p})"));
    }
    if let Some(cnd) = cond {
        let toks = lex(cnd)?;
        let mut p = Parser { t: toks, i: 0 };
        let e = p.or()?;
        if p.i < p.t.len() {
            return Err(Error::invalid("the query has text after its end"));
        }
        // the smaller side first: count, up to a cap, the entities that each indexed condition matches
        let mut conds = Vec::new();
        indexed_conditions(&e, &defs, &mut conds);
        for (id, dt, op, v) in conds {
            let o = sql_op(&op)?;
            let col = match dt.as_str() {
                "number" | "integer" => "num_value",
                "datetime" => "ts_value",
                _ => "text_value",
            };
            let n: i64 = match (&v, dt.as_str()) {
                (Tok::Num(x), "number" | "integer") => {
                    tx.q1(&format!("select count(*) from (select 1 from attr_index where attr_id = $1 and {col} {o} $2 limit {SELECTIVE}) s"), &[&id, x]).await?.get(0)
                }
                (_, "datetime") => {
                    let t = tok_str(&v);
                    tx.q1(&format!("select count(*) from (select 1 from attr_index where attr_id = $1 and {col} {o} $2::text::timestamptz limit {SELECTIVE}) s"), &[&id, &t]).await?.get(0)
                }
                _ => {
                    let t = if op == "=~" { glob_to_like(&tok_str(&v)) } else { tok_str(&v) };
                    tx.q1(&format!("select count(*) from (select 1 from attr_index where attr_id = $1 and {col} {o} $2 limit {SELECTIVE}) s"), &[&id, &t]).await?.get(0)
                }
            };
            comp.selective.insert(cmp_key(id, &op, &v), n < SELECTIVE);
        }
        wh.push(comp.expr(&e)?);
    }
    if !opts.all_versions {
        wh.push("not v.is_delete_marker".into());
    }
    // access: the ACL roots that the caller can read
    if let Some(roots) = crate::authz::readable_roots(tx).await? {
        let p = comp.param(Box::new(roots));
        wh.push(format!("c.acl_root_id = any({p})"));
    }
    // the cursor: the artifact of the last row (latest versions), or the last version (all versions)
    let key = if opts.all_versions { "v.id" } else { "a.id" };
    if let Some(a) = opts.after {
        let p = comp.param(Box::new(a));
        wh.push(format!("{key} > {p}"));
    }
    let limit = opts.limit.unwrap_or(100).clamp(1, 10_000);
    let lp = comp.param(Box::new(limit + 1));
    // the latest versions: drive from the artifacts, in key order, and join the latest version by
    // its primary key, so that the first page needs no sort of the whole tenant
    let from = if opts.all_versions {
        "version v join artifact a on a.id = v.artifact_id and a.tenant_id = v.tenant_id
           join collection c on c.id = a.collection_id and c.tenant_id = a.tenant_id"
    } else {
        "artifact a join collection c on c.id = a.collection_id and c.tenant_id = a.tenant_id
           join version v on v.tenant_id = a.tenant_id and v.id = a.latest_version_id"
    };
    let sql = format!(
        "select v.id, c.path || '/' || a.name, v.number, v.size, v.content_hash, {key} from {from}
          where {} order by {key} limit {lp}",
        wh.join(" and ")
    );
    let params: Vec<&(dyn ToSql + Sync)> = comp.params.iter().map(|p| p.as_ref() as &(dyn ToSql + Sync)).collect();
    let rows = tx.t.query(sql.as_str(), &params).await?;
    let more = rows.len() as i64 > limit;
    let items: Vec<Value> = rows
        .iter()
        .take(limit as usize)
        .map(|r| json!({"version": r.get::<_, Uuid>(0), "path": r.get::<_, String>(1).replacen("//", "/", 1), "number": r.get::<_, i32>(2), "size": r.get::<_, i64>(3), "content_hash": r.get::<_, Option<String>>(4)}))
        .collect();
    let next = if more { rows.get(limit as usize - 1).map(|r| json!(r.get::<_, Uuid>(5))) } else { None };
    Ok(json!({"items": items, "next": next, "sql": sql}))
}
