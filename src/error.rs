use tokio::sync::TryLockError;

#[cfg(feature = "streaming")]
use crate::event_stores::streaming;

#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    #[error("Aggregate is not found: {0}")]
    /// The aggregate with the specified ID was not found.
    NotFound(String),
    #[error("Version conflict: {0}")]
    /// Optimistic locking failed due to a version conflict.
    VersionConflict(usize),
    #[error("tokio_postgres error: {0}")]
    TokioPgError(#[from] tokio_postgres::Error),
    #[error("serde_json error: {0}")]
    SerdeJsonError(#[from] serde_json::Error),
    #[cfg(feature = "streaming")]
    #[error("streaming error: {0}")]
    StreamingError(#[from] streaming::StreamItemError),
    #[error("txid parsing error: {0}")]
    TxIdParsingError(#[from] std::num::ParseIntError),
    #[error("Lock error: {0}")]
    LockError(#[from] TryLockError),
    #[error("Transaction in progress")]
    TransactionInProgress,
}

impl PartialEq for Error {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Error::NotFound(a), Error::NotFound(b)) => a == b,
            (Error::VersionConflict(a), Error::VersionConflict(b)) => a == b,
            _ => false,
        }
    }
}

pub type Result<T> = std::result::Result<T, Error>;
