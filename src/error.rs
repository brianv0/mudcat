//! Errors. Each error has a stable code, a message for people, and details for programs.

use serde_json::{json, Value};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// The entity does not exist, or it belongs to another tenant (INV-7: the two are the same answer).
    #[error("not found")]
    NotFound,
    /// A commit requirement is not true (ID-7).
    #[error("conflict: {message}")]
    Conflict { message: String, details: Value },
    /// The request is refused by access control, usage policy or a rule. `code` is stable.
    #[error("{code}: {message}")]
    Refused { code: &'static str, message: String, details: Value },
    /// The request is not valid.
    #[error("invalid: {0}")]
    Invalid(String),
    /// No token, a token that is not valid, or a token for another audience.
    #[error("unauthenticated: {0}")]
    Unauthenticated(String),
    /// The request can be tried again, for example during a tenant move.
    #[error("retry: {0}")]
    Retryable(String),
    #[error("database: {}", db_message(.0))]
    Db(#[from] tokio_postgres::Error),
    #[error("pool: {0}")]
    Pool(String),
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

fn db_message(e: &tokio_postgres::Error) -> String {
    match e.as_db_error() {
        Some(d) => format!("{} ({}){}", d.message(), d.code().code(), d.detail().map(|x| format!(": {x}")).unwrap_or_default()),
        None => e.to_string(),
    }
}

impl Error {
    pub fn refused(code: &'static str, message: impl Into<String>, details: Value) -> Self {
        Error::Refused { code, message: message.into(), details }
    }
    pub fn conflict(message: impl Into<String>, details: Value) -> Self {
        Error::Conflict { message: message.into(), details }
    }
    pub fn invalid(message: impl Into<String>) -> Self {
        Error::Invalid(message.into())
    }
    /// The stable code of the error.
    pub fn code(&self) -> &str {
        match self {
            Error::NotFound => "not-found",
            Error::Conflict { .. } => "conflict",
            Error::Refused { code, .. } => code,
            Error::Invalid(_) => "invalid",
            Error::Unauthenticated(_) => "unauthenticated",
            Error::Retryable(_) => "retry",
            Error::Db(_) | Error::Pool(_) | Error::Other(_) => "internal",
        }
    }
    pub fn details(&self) -> Value {
        match self {
            Error::Conflict { details, .. } | Error::Refused { details, .. } => details.clone(),
            _ => Value::Null,
        }
    }
    pub fn http_status(&self) -> u16 {
        match self {
            Error::NotFound => 404,
            Error::Conflict { .. } => 409,
            Error::Refused { code, .. } if *code == "access-denied" => 403,
            Error::Refused { .. } => 422,
            Error::Invalid(_) => 400,
            Error::Unauthenticated(_) => 401,
            Error::Retryable(_) => 503,
            _ => 500,
        }
    }
    pub fn to_json(&self) -> Value {
        json!({"error": self.code(), "message": self.to_string(), "details": self.details()})
    }
    /// True if a database error is a serialization failure or a deadlock, which a retry fixes.
    pub fn is_db_retry(&self) -> bool {
        if let Error::Db(e) = self {
            if let Some(code) = e.code() {
                return code.code() == "40001" || code.code() == "40P01";
            }
        }
        false
    }
}

impl From<deadpool_postgres::PoolError> for Error {
    fn from(e: deadpool_postgres::PoolError) -> Self {
        Error::Pool(e.to_string())
    }
}

impl From<serde_json::Error> for Error {
    fn from(e: serde_json::Error) -> Self {
        Error::Invalid(e.to_string())
    }
}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Error::Other(e.into())
    }
}
