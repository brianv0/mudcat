//! The eval runner (evals/README.md). For each variant of each task template, it builds the start
//! state from the parameters, gives the goal to an actor (here: a scripted actor that uses the
//! mudcat interfaces), and examines the end state. The checks are on the outcome, not on the
//! steps. The runner checks the invariants of the template after each step.

pub mod fixtures;
pub mod invariants;
pub mod t1;
pub mod t2;
pub mod t3;
pub mod t4;
pub mod t5;

use crate::error::Result;
use crate::instance::{Instance, Principal, Session, TenantInfo};
use crate::resolver::Resolution;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use uuid::Uuid;

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Variant {
    pub id: String,
    pub template: String,
    pub use_case: i64,
    pub goal: String,
    pub params: BTreeMap<String, Value>,
    pub fixture: String,
    pub expected: String,
    pub checks: Vec<String>,
    pub invariants: Vec<String>,
    #[serde(default)]
    pub same_outcome_for_each_value_of: Vec<String>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct TemplateCheck {
    pub check: String,
    #[serde(default)]
    pub outcome: Option<Value>,
    #[serde(default)]
    pub when: Option<BTreeMap<String, Value>>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct Template {
    pub id: String,
    pub checks: Vec<TemplateCheck>,
}

fn as_list(v: &Value) -> Vec<Value> {
    match v {
        Value::Array(a) => a.clone(),
        x => vec![x.clone()],
    }
}

fn param_str(v: &Value) -> String {
    match v {
        Value::String(s) => s.clone(),
        x => x.to_string(),
    }
}

impl Template {
    /// The raw text of each check that applies to a variant, as generate.py selects them.
    pub fn applicable(&self, v: &Variant) -> Vec<String> {
        self.checks
            .iter()
            .filter(|c| c.outcome.as_ref().map(|o| as_list(o).iter().any(|x| x.as_str() == Some(v.expected.as_str()))).unwrap_or(true))
            .filter(|c| {
                c.when.as_ref().map(|w| w.iter().all(|(k, val)| as_list(val).iter().any(|x| param_str(x) == param_str(&v.params[k])))).unwrap_or(true)
            })
            .map(|c| c.check.clone())
            .collect()
    }
}

/// The record of what happened in a variant, for the invariants.
#[derive(Default)]
pub struct Record {
    pub resolutions: Vec<Resolution>,
    /// published snapshot -> (members, Merkle hash) at publication
    pub published: BTreeMap<Uuid, (BTreeSet<Uuid>, String)>,
    /// the principal of each resolution, for INV-15
    pub principals: BTreeMap<Uuid, Session>,
    /// IDs that mudcat answered with, by tenant of the request (INV-6)
    pub answers: Vec<(Uuid, Vec<Uuid>)>,
    pub search_results: Vec<(chrono::DateTime<chrono::Utc>, Vec<Uuid>)>,
    pub holds: Vec<Uuid>,
    pub notes: Vec<String>,
    /// databases that the variant made and that the runner drops at its end
    pub cleanup_databases: Vec<String>,
    /// the other tenants that the variant made
    pub other_tenants: Vec<Uuid>,
}

pub struct Ctx {
    pub inst: Arc<Instance>,
    pub tenant: TenantInfo,
    pub admin: Session,
    pub params: BTreeMap<String, Value>,
    pub variant: Variant,
    pub checks: Vec<(String, bool, String)>,
    pub outcome: Option<String>,
    pub record: Record,
    pub invariant_results: BTreeMap<String, (bool, String)>,
    pub scale: Scale,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Scale {
    /// The values of the templates, as they are.
    Full,
    /// Large cover values are capped at 2000, to make a fast run.
    Quick,
    /// Cover values are capped at 50 and the sizes that drivers hard-code get smaller in the same
    /// way: the whole suite runs in about two minutes in a debug build.
    Smoke,
}

impl Scale {
    pub fn parse(s: &str) -> Option<Scale> {
        match s {
            "full" => Some(Scale::Full),
            "quick" => Some(Scale::Quick),
            "smoke" => Some(Scale::Smoke),
            _ => None,
        }
    }
    pub fn name(self) -> &'static str {
        match self {
            Scale::Full => "full",
            Scale::Quick => "quick",
            Scale::Smoke => "smoke",
        }
    }
    /// A size that a driver hard-codes for the full scale: `full` as it is, and a cap for the others.
    pub fn size(self, full: usize, quick: usize, smoke: usize) -> usize {
        match self {
            Scale::Full => full,
            Scale::Quick => full.min(quick),
            Scale::Smoke => full.min(smoke),
        }
    }
}

impl Ctx {
    pub fn p(&self, k: &str) -> String {
        param_str(self.params.get(k).unwrap_or(&Value::Null))
    }
    /// A numeric parameter. With the quick scale, large values are capped.
    pub fn n(&self, k: &str) -> i64 {
        let v = self.params.get(k).and_then(|v| v.as_i64()).unwrap_or(0);
        match self.scale {
            Scale::Full => v,
            Scale::Quick => v.min(2000),
            Scale::Smoke => v.min(50),
        }
    }
    pub fn is(&self, k: &str, v: &str) -> bool {
        self.p(k) == v
    }
    /// Record the result of a check. `key` is a part of the text of the check in tasks.yaml.
    pub fn check(&mut self, key: &str, ok: bool, note: impl Into<String>) {
        self.checks.push((key.to_string(), ok, note.into()));
    }
    pub fn outcome(&mut self, o: &str) {
        self.outcome = Some(o.to_string());
    }
    pub fn session(&self, p: &str) -> Session {
        self.inst.session(self.tenant.id, Principal(p.to_string()))
    }
    /// Run the invariants of the template (after a step).
    pub async fn step(&mut self) -> Result<()> {
        let ids = self.variant.invariants.clone();
        for id in ids {
            let (ok, note) = invariants::check(self, &id).await?;
            let e = self.invariant_results.entry(id).or_insert((true, String::new()));
            if !ok && e.0 {
                *e = (false, note);
            }
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct VariantResult {
    pub id: String,
    pub params: BTreeMap<String, Value>,
    pub expected: String,
    pub observed: Option<String>,
    pub outcome_ok: bool,
    pub checks: Vec<(String, bool, String)>,
    pub invariants: BTreeMap<String, (bool, String)>,
    pub passed: bool,
    pub error: Option<String>,
    pub seconds: f64,
}

pub fn load_templates(path: &str) -> anyhow::Result<BTreeMap<String, Template>> {
    let ts: Vec<Template> = serde_yaml::from_str(&std::fs::read_to_string(path)?)?;
    Ok(ts.into_iter().map(|t| (t.id.clone(), t)).collect())
}

pub fn load_variants(path: &str) -> anyhow::Result<Vec<Variant>> {
    let mut out = Vec::new();
    for l in std::fs::read_to_string(path)?.lines() {
        if !l.trim().is_empty() {
            out.push(serde_json::from_str(l)?);
        }
    }
    Ok(out)
}

async fn drive(ctx: &mut Ctx) -> Result<()> {
    match ctx.variant.template.as_str() {
        "T1.1" => t1::resolve_and_read(ctx).await,
        "T1.2" => t1::concurrent_commits(ctx).await,
        "T1.3" => t1::stage_near_compute(ctx).await,
        "T2.1" => t2::combine_methods(ctx).await,
        "T2.2" => t2::leakage_check(ctx).await,
        "T2.3" => t2::reproduce_later(ctx).await,
        "T3.1" => t3::policy_gate(ctx).await,
        "T3.2" => t3::removal(ctx).await,
        "T3.3" => t3::content_summary(ctx).await,
        "T4.1" => t4::stream_consumer(ctx).await,
        "T4.2" => t4::retrieval_after_change(ctx).await,
        "T5.1" => t5::tenant_isolation(ctx).await,
        "T5.2" => t5::embargo(ctx).await,
        "T5.3" => t5::membership_source(ctx).await,
        "T5.4" => t5::tenant_move(ctx).await,
        t => Err(crate::Error::invalid(format!("no driver for {t}"))),
    }
}

/// Run one variant in a fork of the instance, with a fresh tenant.
pub async fn run_variant(base: &Arc<Instance>, tpl: &Template, v: &Variant, scale: Scale, work: &std::path::Path) -> VariantResult {
    let started = std::time::Instant::now();
    let dir = work.join(v.id.replace('#', "-"));
    let _ = std::fs::remove_dir_all(&dir);
    let inst = base.fork(dir.clone());
    let name = format!("ev-{}-{}", v.id.replace(['#', '.'], "-").to_lowercase(), &Uuid::new_v4().simple().to_string()[..6]);
    let mut result = VariantResult {
        id: v.id.clone(),
        params: v.params.clone(),
        expected: v.expected.clone(),
        observed: None,
        outcome_ok: false,
        checks: vec![],
        invariants: BTreeMap::new(),
        passed: false,
        error: None,
        seconds: 0.0,
    };
    let tenant = match crate::tenants::create_tenant(&inst, &name, "user:admin").await {
        Ok(t) => t,
        Err(e) => {
            result.error = Some(format!("tenant: {e}"));
            return result;
        }
    };
    let admin = inst.session(tenant.id, Principal::user("admin"));
    let mut ctx = Ctx {
        inst: inst.clone(),
        tenant,
        admin,
        params: v.params.clone(),
        variant: v.clone(),
        checks: vec![],
        outcome: None,
        record: Record::default(),
        invariant_results: BTreeMap::new(),
        scale,
    };
    let r = drive(&mut ctx).await;
    if let Err(e) = &r {
        result.error = Some(format!("{e}"));
    } else if let Err(e) = ctx.step().await {
        result.error = Some(format!("invariants: {e}"));
    }
    // map the results of the driver to the applicable checks of the template
    let applicable = tpl.applicable(v);
    let mut checks = Vec::new();
    for raw in &applicable {
        let found: Vec<&(String, bool, String)> = ctx.checks.iter().filter(|(k, _, _)| raw.contains(k.as_str())).collect();
        if found.is_empty() {
            checks.push((raw.clone(), false, "not evaluated by the driver".to_string()));
        } else {
            let ok = found.iter().all(|f| f.1);
            let note = found.iter().map(|f| f.2.clone()).filter(|n| !n.is_empty()).collect::<Vec<_>>().join("; ");
            checks.push((raw.clone(), ok, note));
        }
    }
    result.observed = ctx.outcome.clone();
    result.outcome_ok = ctx.outcome.as_deref() == Some(v.expected.as_str());
    result.invariants = ctx.invariant_results.clone();
    for i in &v.invariants {
        result.invariants.entry(i.clone()).or_insert((false, "never checked".into()));
    }
    result.passed = result.error.is_none() && result.outcome_ok && checks.iter().all(|c| c.1) && result.invariants.values().all(|i| i.0);
    result.checks = checks;
    result.seconds = started.elapsed().as_secs_f64();
    let _ = std::fs::remove_dir_all(&dir);
    for db in &ctx.record.cleanup_databases {
        if let Ok(p) = crate::instance::make_pool(&crate::instance::db_url(&inst.base_url, "postgres"), 1) {
            if let Ok(c) = p.get().await {
                let _ = c.batch_execute(&format!("drop database if exists \"{db}\" with (force)")).await;
            }
        }
        // the registry must not name a tenant in a database that is gone
        if let Ok(c) = inst.shared.get().await {
            let _ = c.execute("delete from tenant where location = $1", &[db]).await;
        }
        inst.tenants.write().retain(|_, t| &t.location != db);
    }
    result
}

/// The stamp that the runner and the benchmarks record in their results: the git commit, whether
/// the working tree had changes, the scale and the date.
pub fn stamp(scale: &str) -> Value {
    let git = |args: &[&str]| std::process::Command::new("git").args(args).output().ok().filter(|o| o.status.success()).map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string());
    // untracked files in evals/results or target do not count; those are outputs
    let dirty = git(&["status", "--porcelain", "--untracked-files=no"]).map(|s| !s.is_empty());
    serde_json::json!({
        "git_commit": git(&["rev-parse", "HEAD"]),
        "git_dirty": dirty,
        "scale": scale,
        "date": chrono::Utc::now().to_rfc3339(),
    })
}

/// The URL of database `name` on the server of `server`.
pub fn db_url_for(server: &str, name: &str) -> String {
    crate::instance::db_url(server, name)
}
