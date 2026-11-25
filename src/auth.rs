//! Tokens and sign-in (architecture.md, "Identity and authentication"; use case 5).
//!
//! A mudcat token is a signed JWT (HS256) whose audience is one tenant. The tenant resolver
//! accepts a token only at the endpoint of its audience (OP-12). Small groups sign in with an
//! outside account (a GitHub account in the evals), and pipelines with the identity token of
//! their CI system and a trust rule. mudcat stores no token of the outside service.

use crate::error::{Error, Result};
use crate::instance::{Instance, Principal, Session};
use base64::engine::general_purpose::URL_SAFE_NO_PAD as B64;
use base64::Engine;
use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::Sha256;
use std::sync::Arc;
use uuid::Uuid;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Claims {
    pub sub: String,
    pub aud: Uuid,
    pub iat: i64,
    pub exp: i64,
    #[serde(default)]
    pub place: Option<String>,
}

fn sign(secret: &[u8], msg: &str) -> String {
    let mut mac = Hmac::<Sha256>::new_from_slice(secret).expect("key");
    mac.update(msg.as_bytes());
    B64.encode(mac.finalize().into_bytes())
}

pub fn issue(inst: &Instance, tenant: Uuid, principal: &Principal, ttl: chrono::Duration) -> String {
    let now = inst.now().timestamp();
    let claims = Claims { sub: principal.0.clone(), aud: tenant, iat: now, exp: now + ttl.num_seconds(), place: None };
    let h = B64.encode(br#"{"alg":"HS256","typ":"JWT"}"#);
    let c = B64.encode(serde_json::to_vec(&claims).unwrap());
    let msg = format!("{h}.{c}");
    format!("{msg}.{}", sign(&inst.secret, &msg))
}

pub fn verify(inst: &Instance, token: &str) -> Result<Claims> {
    let parts: Vec<&str> = token.split('.').collect();
    if parts.len() != 3 {
        return Err(Error::Unauthenticated("the token is not valid".into()));
    }
    let msg = format!("{}.{}", parts[0], parts[1]);
    if sign(&inst.secret, &msg) != parts[2] {
        return Err(Error::Unauthenticated("the token is not valid".into()));
    }
    let claims: Claims = serde_json::from_slice(&B64.decode(parts[1]).map_err(|_| Error::Unauthenticated("bad token".into()))?)
        .map_err(|_| Error::Unauthenticated("bad token".into()))?;
    if claims.exp < inst.now().timestamp() {
        return Err(Error::Unauthenticated("the token expired".into()));
    }
    Ok(claims)
}

/// Exchange an outside account token (GitHub, through the device flow or a personal token) for
/// a mudcat token for one tenant. The outside token is used once and not stored.
pub async fn sign_in_provider(inst: &Arc<Instance>, tenant: Uuid, provider: &str, provider_token: &str) -> Result<(String, Principal)> {
    if inst.idp.down.read().contains(provider) {
        return Err(Error::refused("provider-unreachable", format!("{provider} cannot be reached"), Value::Null));
    }
    let external = inst
        .idp
        .tokens
        .read()
        .get(provider_token)
        .cloned()
        .ok_or_else(|| Error::Unauthenticated(format!("{provider} does not accept the token")))?;
    let sess = inst.session(tenant, Principal::system());
    let mut c = sess.client().await?;
    let tx = sess.begin(&mut c).await?;
    let p = tx
        .qopt("select principal from linked_identity where provider = $1 and external_id = $2", &[&provider, &external])
        .await?
        .map(|r| Principal(r.get(0)))
        .unwrap_or_else(|| Principal(format!("user:{provider}-{external}")));
    tx.audit("sign-in", "sign-in", &p.0, true, Some(provider), json!({"external": external})).await?;
    tx.commit().await?;
    Ok((issue(inst, tenant, &p, chrono::Duration::hours(8)), p))
}

/// An identity token of a CI pipeline, as the CI system signs it (simulated).
pub fn ci_identity_token(issuer_secret: &[u8], issuer: &str, repository: &str, branch: &str) -> String {
    let c = B64.encode(serde_json::to_vec(&json!({"iss": issuer, "repository": repository, "ref": format!("refs/heads/{branch}")})).unwrap());
    format!("{c}.{}", sign(issuer_secret, &c))
}

pub async fn add_trust_rule(sess: &Session, issuer: &str, repository: &str, branch: &str, principal: &str) -> Result<Uuid> {
    let mut c = sess.client().await?;
    let tx = sess.begin(&mut c).await?;
    let id = crate::instance::new_id();
    tx.exec("insert into trust_rule (id, issuer, repository, branch, principal) values ($1,$2,$3,$4,$5)", &[&id, &issuer, &repository, &branch, &principal]).await?;
    tx.commit().await?;
    Ok(id)
}

/// Exchange the identity token of a pipeline for a mudcat token, through a trust rule.
pub async fn sign_in_pipeline(inst: &Arc<Instance>, tenant: Uuid, issuer_secret: &[u8], token: &str) -> Result<(String, Principal)> {
    let (c, s) = token.split_once('.').ok_or_else(|| Error::Unauthenticated("bad identity token".into()))?;
    if sign(issuer_secret, c) != s {
        return Err(Error::Unauthenticated("the identity token is not valid".into()));
    }
    let claims: Value = serde_json::from_slice(&B64.decode(c).map_err(|_| Error::Unauthenticated("bad token".into()))?)?;
    let branch = claims["ref"].as_str().unwrap_or("").trim_start_matches("refs/heads/").to_string();
    let sess = inst.session(tenant, Principal::system());
    let mut cl = sess.client().await?;
    let tx = sess.begin(&mut cl).await?;
    let r = tx
        .qopt(
            "select principal from trust_rule where issuer = $1 and repository = $2 and branch = $3",
            &[&claims["iss"].as_str().unwrap_or(""), &claims["repository"].as_str().unwrap_or(""), &branch],
        )
        .await?
        .ok_or_else(|| Error::Unauthenticated("no trust rule accepts the identity token".into()))?;
    let p = Principal(r.get(0));
    tx.commit().await?;
    Ok((issue(inst, tenant, &p, chrono::Duration::hours(1)), p))
}

/// The device sign-in flow of an outside provider (simulated): mudcat asks the provider for a
/// device code, the person approves the user code at the provider, and mudcat then polls with the
/// device code. It gets a provider token, exchanges it at once for a mudcat token, and keeps
/// neither the device code nor the provider token. Returns (device code, user code).
pub fn start_device_login(inst: &Instance, provider: &str) -> (String, String) {
    let device = hex::encode(rand::random::<[u8; 16]>());
    let user = hex::encode(rand::random::<[u8; 4]>()).to_uppercase();
    inst.idp.tokens.write().insert(format!("device:{provider}:{user}"), format!("pending:{device}"));
    (device, user)
}

/// The person approves the user code at the provider, as the external user `external`.
pub fn approve_device_login(inst: &Instance, provider: &str, user_code: &str, external: &str) -> Result<()> {
    let mut t = inst.idp.tokens.write();
    let pending = t.remove(&format!("device:{provider}:{user_code}")).ok_or_else(|| Error::invalid("unknown user code"))?;
    let device = pending.trim_start_matches("pending:").to_string();
    let provider_token = format!("{provider}-device-{}", hex::encode(rand::random::<[u8; 12]>()));
    t.insert(provider_token.clone(), external.to_string());
    t.insert(format!("device-done:{provider}:{device}"), provider_token);
    Ok(())
}

/// mudcat polls with the device code. When the person has approved, it exchanges the provider
/// token for a mudcat token for the tenant, and forgets the provider token.
pub async fn poll_device_login(inst: &Arc<Instance>, tenant: Uuid, provider: &str, device_code: &str) -> Result<(String, Principal)> {
    let provider_token = inst
        .idp
        .tokens
        .write()
        .remove(&format!("device-done:{provider}:{device_code}"))
        .ok_or_else(|| Error::Unauthenticated("the device login is not approved yet".into()))?;
    let out = sign_in_provider(inst, tenant, provider, &provider_token).await;
    // the provider token is used once and not kept
    inst.idp.tokens.write().remove(&provider_token);
    out
}
