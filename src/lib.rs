//! mudcat: a multimodal data catalog.
//!
//! This crate is the reference implementation of the design in `docs/`. The catalog database
//! is PostgreSQL (the only service that mudcat needs, OP-1). The modules follow the areas of
//! the model: namespace and storage, metadata, datasets, usage policy, access control,
//! resolution, provenance, placement, the change stream and search.

pub mod api;
pub mod auth;
pub mod authz;
pub mod datasets;
pub mod error;
pub mod evals;
pub mod export;
pub mod instance;
pub mod leakage;
pub mod metadata;
pub mod namespace;
pub mod placement;
pub mod plugins;
pub mod policy;
pub mod provenance;
pub mod query;
pub mod resolver;
pub mod search;
pub mod storage;
pub mod stream;
pub mod tenants;

pub use error::{Error, Result};
pub use instance::{Instance, Principal, Session, Tx};
