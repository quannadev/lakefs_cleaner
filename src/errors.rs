use thiserror::Error;
#[derive(Debug, Error)]
pub enum CleanerError {
    #[error("init error: {0}")]
    Init(String),
    #[error("validation error: {0}")]
    Validation(String),
    #[error("lakefs error: {0}")]
    Lakefs(String),
    #[error("duckdb error")]
    Duckdb(#[from] duckdb::Error),
    #[error("unknown error: {0}")]
    Unknown(String),
}
