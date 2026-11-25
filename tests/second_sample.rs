//! Tests for two mechanisms that no task template verifies, found by the second sample of the
//! mutation gate (evals/README.md, "The second sample"): the jurisdiction rules of every
//! collection of an input (RD-9), and the depth limit of a lineage traversal (PF-7).

use mudcat::datasets::Member;
use mudcat::evals::fixtures::*;
use mudcat::metadata::{self, AttributeSpec};
use mudcat::provenance::{self, LineageQuery, ModelSpec};
use mudcat::resolver;
use mudcat::{in_tx, Instance, Principal, Result, Session};
use serde_json::json;
use std::sync::Arc;
use uuid::Uuid;

async fn setup(tag: &str) -> Result<(Arc<Instance>, Session)> {
    let url = std::env::var("MUDCAT_TEST_DB").unwrap_or_else(|_| "postgres://mudcat:mudcat@localhost:54329/mudcat_scenarios".into());
    let base = Instance::open(&url, "target/scenario-data").await?;
    let id = Uuid::new_v4().simple().to_string();
    let inst = base.fork(std::path::PathBuf::from("target/scenario-data/work").join(format!("{tag}-{id}")));
    let t = mudcat::tenants::create_tenant(&inst, &format!("{tag}-{}", &id[..10]), "user:admin").await?;
    let s = inst.session(t.id, Principal::user("admin"));
    Ok((inst, s))
}

/// A collection that is its own ACL root, on a storage resource of its own, with one verified file.
/// With `restricted`, the data may be read only from the location cn (a jurisdiction rule).
async fn collection_with_file(s: &Session, path: &str, res: &str, restricted: bool) -> Result<Uuid> {
    let r = resource(s, res, if restricted { "cn" } else { "eu" }, "managed", false).await?;
    let prefix = format!("{}/", res);
    acl_root(s, path, Some((res, &prefix))).await?;
    if restricted {
        in_tx!(s, |tx| {
            let c = mudcat::namespace::collection(&tx, path).await?.id;
            metadata::tag(&mut tx, c, "jurisdiction", "CN-export-controlled").await?
        });
    }
    let files = vec![("f.bin".to_string(), format!("the file of {path}").into_bytes())];
    let regs = register_files(s, &r, path, &prefix, &files, Some("src"), None).await?;
    verify_all(s, res).await?;
    Ok(regs[0].version_id)
}

/// RD-9: the jurisdiction rules of each collection that the inputs come from apply to the caller,
/// whatever the order of the collections, not the rules of the first one only.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn jurisdiction_rules_apply_to_every_collection_of_the_inputs() -> Result<()> {
    let (_inst, s) = setup("second-jurisdiction").await?;
    source(&s, "src", "instrument", json!({}), &[("train", "research")]).await?;
    in_tx!(s, |tx| {
        metadata::declare_attribute(&mut tx, &AttributeSpec { namespace: "tags".into(), name: "jurisdiction".into(), datatype: "string".into(), is_tag: true, permitted: Some(vec!["CN-export-controlled".into()]), ..Default::default() }).await?;
        metadata::set_tag_policy(&mut tx, "jurisdiction", "CN-export-controlled", "jurisdiction", json!({"allowed_locations": ["cn"]})).await?;
    });
    // the restricted collection is created before the free one in the first dataset, and after it in the second
    let r1 = collection_with_file(&s, "/j1/restricted", "cn-store-1", true).await?;
    let f1 = collection_with_file(&s, "/j1/free", "eu-store-1", false).await?;
    let f2 = collection_with_file(&s, "/j2/free", "eu-store-2", false).await?;
    let r2 = collection_with_file(&s, "/j2/restricted", "cn-store-2", true).await?;
    dataset(&s, "/d/first", vec![Member::version(r1), Member::version(f1)]).await?;
    dataset(&s, "/d/second", vec![Member::version(f2), Member::version(r2)]).await?;
    dataset(&s, "/d/free", vec![Member::version(f1), Member::version(f2)]).await?;

    for ds in ["/d/first", "/d/second"] {
        let mut req = train_request(vec![dataset_input(ds, Some("main"), "training")], "train", "research");
        req.place = Some("eu".into());
        let e = resolver::resolve(&s, &req).await.err().unwrap_or_else(|| panic!("mudcat resolved {ds} for a caller in eu, and one collection of it may be read only in cn"));
        assert!(e.code() == "jurisdiction" && e.to_string().contains("CN-export-controlled"), "{ds}: the error names the jurisdiction rule ({e})");
    }
    // the control: data with no rule resolves for the same caller
    let mut req = train_request(vec![dataset_input("/d/free", Some("main"), "training")], "train", "research");
    req.place = Some("eu".into());
    let ok = resolver::resolve(&s, &req).await?;
    assert_eq!(ok.tasks.len(), 2, "the data with no rule resolves for the caller in eu");
    Ok(())
}

/// PF-7: a traversal that passes its depth limit fails with an error that states the limit. It
/// does not go one level further and return a result that looks complete.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_lineage_traversal_fails_exactly_at_its_depth_limit() -> Result<()> {
    let (_inst, s) = setup("second-lineage").await?;
    // m3 was fine-tuned from m2, m2 from m1, m1 from m0: three levels of ancestors
    let mut chain: Vec<Uuid> = Vec::new();
    for i in 0..4 {
        let derived_from = chain.last().map(|b| vec![(*b, "fineTunedFrom".to_string())]).unwrap_or_default();
        let m = in_tx!(s, |tx| provenance::publish_model(&mut tx, &ModelSpec { path: format!("/models/m{i}"), content_hash: format!("weights-{i}"), size: 1, derived_from, ..Default::default() }).await?);
        chain.push(m);
    }
    let start = chain[3];
    let q = |depth: usize| LineageQuery { direction: "ancestors".into(), depth: Some(depth), ..Default::default() };
    let ok = in_tx!(s, |tx| provenance::lineage(&tx, start, &q(3)).await?);
    assert_eq!(ok["nodes"].as_array().map(|n| n.len()), Some(4), "with the depth limit 3 the traversal returns the chain of four models");
    for depth in [1, 2] {
        let r = in_tx!(s, |tx| provenance::lineage(&tx, start, &q(depth)).await);
        let e = r.err().unwrap_or_else(|| panic!("the traversal with the depth limit {depth} returned a result, and the chain is three levels deep"));
        assert!(e.code() == "lineage-limit" && e.to_string().contains(&format!("depth limit of {depth}")), "the error states the limit {depth} ({e})");
    }
    Ok(())
}
