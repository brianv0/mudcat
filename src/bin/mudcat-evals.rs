//! Run the evals: every variant of every task template, with the invariants.
//!
//! Usage: mudcat-evals [--template T1.1] [--variant T1.1#003] [--scale full|quick|smoke] [--jobs N]

use clap::Parser;
use mudcat::evals::{self, Scale, VariantResult};
use mudcat::instance::Instance;
use std::collections::BTreeMap;
use std::sync::Arc;

#[derive(Parser)]
struct Args {
    #[arg(long, env = "MUDCAT_DB", default_value = "postgres://mudcat:mudcat@localhost:54329/mudcat_evals")]
    db: String,
    #[arg(long, default_value = "target/evals-data")]
    data: String,
    #[arg(long)]
    template: Vec<String>,
    #[arg(long)]
    variant: Vec<String>,
    #[arg(long, default_value = "full")]
    scale: String,
    #[arg(long, default_value_t = 4)]
    jobs: usize,
    #[arg(long, default_value = "evals/results")]
    out: String,
    /// Print each check of each variant.
    #[arg(long)]
    verbose: bool,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let a = Args::parse();
    if !std::path::Path::new("evals/generated/variants.jsonl").exists() {
        let st = std::process::Command::new("python3").args(["evals/generate.py", "--write"]).status()?;
        anyhow::ensure!(st.success(), "generate.py failed");
    }
    let templates = evals::load_templates("evals/tasks.yaml")?;
    let mut variants = evals::load_variants("evals/generated/variants.jsonl")?;
    if !a.template.is_empty() {
        variants.retain(|v| a.template.contains(&v.template));
    }
    if !a.variant.is_empty() {
        variants.retain(|v| a.variant.contains(&v.id));
    }
    let scale = Scale::parse(&a.scale).ok_or_else(|| anyhow::anyhow!("unknown scale {:?}: full, quick or smoke", a.scale))?;
    let inst = Instance::open(&a.db, &a.data).await?;
    let work = std::path::PathBuf::from(&a.data).join("work");
    let sem = Arc::new(tokio::sync::Semaphore::new(a.jobs));
    let started = std::time::Instant::now();
    let mut handles = Vec::new();
    for v in variants.clone() {
        let inst = inst.clone();
        let tpl = templates[&v.template].clone();
        let sem = sem.clone();
        let work = work.clone();
        handles.push(tokio::spawn(async move {
            let _p = sem.acquire().await.unwrap();
            evals::run_variant(&inst, &tpl, &v, scale, &work).await
        }));
    }
    let mut results: Vec<VariantResult> = Vec::new();
    for h in handles {
        let r = h.await?;
        let mark = if r.passed { "PASS" } else { "FAIL" };
        println!("{mark} {} {:?} expected={} observed={:?} ({:.1}s)", r.id, r.params, r.expected, r.observed.as_deref().unwrap_or("-"), r.seconds);
        if !r.passed || a.verbose {
            if let Some(e) = &r.error {
                println!("    error: {e}");
            }
            for (c, ok, note) in &r.checks {
                if !ok || a.verbose {
                    println!("    {} {c} {}", if *ok { "ok  " } else { "FAIL" }, if note.is_empty() { String::new() } else { format!("[{note}]") });
                }
            }
            for (i, (ok, note)) in &r.invariants {
                if !ok || a.verbose {
                    println!("    {} {i} {note}", if *ok { "ok  " } else { "FAIL" });
                }
            }
        }
        results.push(r);
    }
    let mut by_t: BTreeMap<String, (usize, usize, f64)> = BTreeMap::new();
    for r in &results {
        let t = r.id.split('#').next().unwrap().to_string();
        let e = by_t.entry(t).or_default();
        e.1 += 1;
        e.2 += r.seconds;
        if r.passed {
            e.0 += 1;
        }
    }
    println!("\ntemplate  passed/variants  seconds");
    for (t, (p, n, s)) in &by_t {
        println!("{t:8}  {p:>3}/{n:<3}            {s:.1}");
    }
    let passed = results.iter().filter(|r| r.passed).count();
    let checks: usize = results.iter().map(|r| r.checks.len()).sum();
    let checks_ok: usize = results.iter().map(|r| r.checks.iter().filter(|c| c.1).count()).sum();
    println!("\n{passed}/{} variants passed; {checks_ok}/{checks} checks; scale {}; {:.1}s", results.len(), a.scale, started.elapsed().as_secs_f64());
    std::fs::create_dir_all(&a.out)?;
    let name = if a.template.is_empty() && a.variant.is_empty() { format!("{}/latest-{}.json", a.out, a.scale) } else { format!("{}/partial.json", a.out) };
    std::fs::write(&name, serde_json::to_string_pretty(&serde_json::json!({"scale": a.scale, "passed": passed, "variants": results.len(), "checks": checks, "checks_passed": checks_ok, "results": results}))?)?;
    if passed != results.len() {
        std::process::exit(1);
    }
    Ok(())
}
