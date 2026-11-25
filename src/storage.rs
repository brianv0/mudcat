//! A simulated storage service: buckets and POSIX file systems on the local disk.
//!
//! mudcat does not store data. This module plays the part of object storage and HPC file
//! systems, so that evals can check that clients read bytes directly from storage with the
//! credentials that the resolver issued (RD-1, RD-3), that site agents verify checksums, and that
//! purges delete every object version.

use crate::error::{Error, Result};
use hmac::{Hmac, Mac};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use sha2::Sha256;
use std::collections::HashMap;
use std::path::{Path, PathBuf};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(tag = "kind", rename_all = "kebab-case")]
pub enum Credential {
    /// Vended credentials for every object under a prefix.
    Prefix { bucket: String, prefix: String, expires: i64, sig: String },
    /// A signed URL for one object.
    SignedUrl { url: String, expires: i64, sig: String },
    /// No credential: the site enforces access (for example Unix permissions on an HPC file system).
    Site,
}

#[derive(Clone, Debug)]
pub struct Access {
    pub client: String,
    pub bucket: String,
    pub key: String,
    pub bytes: u64,
    pub op: &'static str,
}

#[derive(Clone, Debug, Default)]
pub struct BucketConfig {
    pub versioned: bool,
    pub posix: bool,
}

pub struct ObjectStore {
    root: PathBuf,
    secret: Vec<u8>,
    buckets: Mutex<HashMap<String, BucketConfig>>,
    log: Mutex<Vec<Access>>,
    /// Unix group of files on POSIX resources.
    groups: Mutex<HashMap<(String, String), String>>,
}

pub fn url_of(bucket: &str, key: &str, posix: bool) -> String {
    if posix {
        format!("file:///{bucket}/{key}")
    } else {
        format!("s3://{bucket}/{key}")
    }
}

pub fn parse_url(url: &str) -> Result<(String, String)> {
    let rest = url
        .strip_prefix("s3://")
        .or_else(|| url.strip_prefix("file:///"))
        .ok_or_else(|| Error::invalid(format!("not a storage URL: {url}")))?;
    let (b, k) = rest.split_once('/').ok_or_else(|| Error::invalid(format!("no key in URL: {url}")))?;
    Ok((b.to_string(), k.to_string()))
}

impl ObjectStore {
    pub fn new(root: PathBuf, secret: Vec<u8>) -> Self {
        let _ = std::fs::create_dir_all(&root);
        ObjectStore { root, secret, buckets: Mutex::new(HashMap::new()), log: Mutex::new(Vec::new()), groups: Mutex::new(HashMap::new()) }
    }

    pub fn create_bucket(&self, bucket: &str, cfg: BucketConfig) -> Result<()> {
        std::fs::create_dir_all(self.root.join(bucket))?;
        self.buckets.lock().insert(bucket.to_string(), cfg);
        Ok(())
    }

    fn cfg(&self, bucket: &str) -> BucketConfig {
        self.buckets.lock().get(bucket).cloned().unwrap_or_default()
    }

    fn path(&self, bucket: &str, key: &str) -> PathBuf {
        self.root.join(bucket).join(key)
    }

    fn versions_dir(&self, bucket: &str, key: &str) -> PathBuf {
        self.root.join(".versions").join(bucket).join(key)
    }

    /// Write an object. On a versioned bucket, the old object stays as an old version.
    pub fn put(&self, client: &str, bucket: &str, key: &str, data: &[u8]) -> Result<()> {
        let p = self.path(bucket, key);
        if let Some(dir) = p.parent() {
            std::fs::create_dir_all(dir)?;
        }
        if self.cfg(bucket).versioned && p.exists() {
            let vd = self.versions_dir(bucket, key);
            std::fs::create_dir_all(&vd)?;
            let n = std::fs::read_dir(&vd)?.count();
            std::fs::rename(&p, vd.join(format!("{n}")))?;
        }
        std::fs::write(&p, data)?;
        self.log.lock().push(Access { client: client.into(), bucket: bucket.into(), key: key.into(), bytes: data.len() as u64, op: "put" });
        Ok(())
    }

    pub fn exists(&self, bucket: &str, key: &str) -> bool {
        self.path(bucket, key).exists()
    }

    /// The number of object versions, the current object included.
    pub fn object_versions(&self, bucket: &str, key: &str) -> usize {
        let cur = usize::from(self.path(bucket, key).exists());
        let old = std::fs::read_dir(self.versions_dir(bucket, key)).map(|d| d.count()).unwrap_or(0);
        cur + old
    }

    /// Read an object as a client, with a credential that the resolver issued.
    pub fn get(&self, client: &str, url: &str, cred: &Credential, now: i64) -> Result<Vec<u8>> {
        let (bucket, key) = parse_url(url)?;
        let ok = match cred {
            Credential::Prefix { bucket: b, prefix, expires, sig } => {
                *expires >= now && b == &bucket && key.starts_with(prefix.as_str()) && *sig == self.sign(&format!("prefix|{b}|{prefix}|{expires}"))
            }
            Credential::SignedUrl { url: u, expires, sig } => *expires >= now && u == url && *sig == self.sign(&format!("url|{u}|{expires}")),
            Credential::Site => self.cfg(&bucket).posix,
        };
        if !ok {
            return Err(Error::refused("access-denied", format!("the storage refuses {url}: the credential does not cover it"), serde_json::Value::Null));
        }
        let data = std::fs::read(self.path(&bucket, &key)).map_err(|_| Error::NotFound)?;
        self.log.lock().push(Access { client: client.into(), bucket, key, bytes: data.len() as u64, op: "get" });
        Ok(data)
    }

    /// Read an object as a site agent, which runs next to the storage.
    pub fn read_local(&self, agent: &str, bucket: &str, key: &str) -> Result<Vec<u8>> {
        let data = std::fs::read(self.path(bucket, key)).map_err(|_| Error::NotFound)?;
        self.log.lock().push(Access { client: agent.into(), bucket: bucket.into(), key: key.into(), bytes: data.len() as u64, op: "agent-read" });
        Ok(data)
    }

    /// Delete an object. With `all_versions`, also delete its old versions. Returns the number
    /// of object versions that were deleted.
    pub fn delete(&self, bucket: &str, key: &str, all_versions: bool) -> Result<usize> {
        let mut n = 0;
        let p = self.path(bucket, key);
        if p.exists() {
            if self.cfg(bucket).versioned && !all_versions {
                let vd = self.versions_dir(bucket, key);
                std::fs::create_dir_all(&vd)?;
                let k = std::fs::read_dir(&vd)?.count();
                std::fs::rename(&p, vd.join(format!("{k}")))?;
            } else {
                std::fs::remove_file(&p)?;
            }
            n += 1;
        }
        if all_versions {
            let vd = self.versions_dir(bucket, key);
            if vd.exists() {
                n += std::fs::read_dir(&vd)?.count();
                std::fs::remove_dir_all(&vd)?;
            }
        }
        Ok(n)
    }

    /// List the objects of a bucket (an inventory report).
    pub fn list(&self, bucket: &str) -> Result<Vec<(String, u64)>> {
        let base = self.root.join(bucket);
        let mut out = Vec::new();
        walk(&base, &base, &mut out)?;
        out.sort();
        Ok(out)
    }

    /// Change the bytes of an object, as a faulty transfer would.
    pub fn corrupt(&self, bucket: &str, key: &str) -> Result<()> {
        let p = self.path(bucket, key);
        let mut data = std::fs::read(&p)?;
        if data.is_empty() {
            data.push(0);
        }
        data[0] ^= 0xff;
        std::fs::write(&p, data)?;
        Ok(())
    }

    pub fn set_group(&self, bucket: &str, key: &str, group: &str) {
        self.groups.lock().insert((bucket.into(), key.into()), group.into());
    }
    pub fn group(&self, bucket: &str, key: &str) -> Option<String> {
        self.groups.lock().get(&(bucket.to_string(), key.to_string())).cloned()
    }

    pub fn sign(&self, msg: &str) -> String {
        let mut mac = Hmac::<Sha256>::new_from_slice(&self.secret).expect("key");
        mac.update(msg.as_bytes());
        hex::encode(mac.finalize().into_bytes())
    }

    pub fn prefix_credential(&self, bucket: &str, prefix: &str, expires: i64) -> Credential {
        Credential::Prefix { bucket: bucket.into(), prefix: prefix.into(), expires, sig: self.sign(&format!("prefix|{bucket}|{prefix}|{expires}")) }
    }

    pub fn signed_url(&self, url: &str, expires: i64) -> Credential {
        Credential::SignedUrl { url: url.into(), expires, sig: self.sign(&format!("url|{url}|{expires}")) }
    }

    /// The access log: who read or wrote which bytes.
    pub fn accesses(&self) -> Vec<Access> {
        self.log.lock().clone()
    }
    pub fn clear_log(&self) {
        self.log.lock().clear();
    }
}

fn walk(base: &Path, dir: &Path, out: &mut Vec<(String, u64)>) -> Result<()> {
    let Ok(rd) = std::fs::read_dir(dir) else { return Ok(()) };
    for e in rd {
        let e = e?;
        let p = e.path();
        if p.is_dir() {
            walk(base, &p, out)?;
        } else {
            let key = p.strip_prefix(base).unwrap().to_string_lossy().to_string();
            out.push((key, e.metadata()?.len()));
        }
    }
    Ok(())
}

/// The manifest store of the tenants: element lists and annotation sets, as JSON Lines files.
/// Manifests are immutable after they are written.
pub struct ManifestStore {
    root: PathBuf,
}

impl ManifestStore {
    pub fn new(root: PathBuf) -> Self {
        ManifestStore { root }
    }
    fn path(&self, tenant: uuid::Uuid, id: uuid::Uuid) -> PathBuf {
        self.root.join(tenant.to_string()).join(format!("{id}.jsonl"))
    }
    pub fn write(&self, tenant: uuid::Uuid, id: uuid::Uuid, rows: &[serde_json::Value]) -> Result<String> {
        let p = self.path(tenant, id);
        std::fs::create_dir_all(p.parent().unwrap())?;
        let mut s = String::with_capacity(rows.len() * 64);
        for r in rows {
            s.push_str(&r.to_string());
            s.push('\n');
        }
        std::fs::write(&p, s)?;
        Ok(format!("{tenant}/{id}.jsonl"))
    }
    pub fn read(&self, tenant: uuid::Uuid, id: uuid::Uuid) -> Result<Vec<serde_json::Value>> {
        let s = std::fs::read_to_string(self.path(tenant, id)).map_err(|_| Error::NotFound)?;
        s.lines().map(|l| serde_json::from_str(l).map_err(Error::from)).collect()
    }
    pub fn remove(&self, tenant: uuid::Uuid, id: uuid::Uuid) -> Result<()> {
        let _ = std::fs::remove_file(self.path(tenant, id));
        Ok(())
    }
}
