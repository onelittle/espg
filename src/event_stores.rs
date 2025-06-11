#[cfg(feature = "inmem")]
mod in_memory;
#[cfg(feature = "postgres")]
pub mod postgres;

use std::fmt::Display;

use crate::Aggregate;
#[cfg(feature = "inmem")]
pub use in_memory::InMemoryEventStore;
#[cfg(feature = "postgres")]
pub use postgres::PostgresEventStore;
use serde::{Deserialize, Serialize};
use tokio_stream::{Stream, wrappers::UnboundedReceiverStream};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Aggregate is not found: {0}")]
    /// The aggregate with the specified ID was not found.
    NotFound(String),
    #[error("Version conflict: {0}")]
    /// Optimistic locking failed due to a version conflict.
    VersionConflict(usize),
    #[error("tokio_postgres error: {0}")]
    #[cfg(feature = "postgres")]
    /// Only available when the `postgres` feature is enabled.
    TokioPgError(#[from] tokio_postgres::Error),
    #[error("serde_json error: {0}")]
    #[cfg(feature = "postgres")]
    /// Only available when the `postgres` feature is enabled.
    SerdeJsonError(#[from] serde_json::Error),
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

#[derive(Clone, Serialize, Deserialize)]
pub struct Diagnostics {
    pub loaded_events: usize,
    pub snapshotted: bool,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct Commit<T> {
    pub id: String,
    pub version: usize,
    pub inner: T,
    pub diagnostics: Option<Diagnostics>,
}

#[allow(async_fn_in_trait)]
pub trait EventStore<T>
where
    T: Aggregate + Default,
{
    type StoreError;
    type StreamError: Display + std::error::Error + Send + Sync;

    async fn append(&self, id: &str, event: T::Event) -> Result<()>;
    async fn commit(&self, id: &str, version: usize, event: T::Event) -> Result<()>;
    async fn try_get_events_since(&self, id: &str, version: usize)
    -> Result<Commit<Vec<T::Event>>>;
    async fn try_get_events(&self, id: &str) -> Result<Commit<Vec<T::Event>>> {
        self.try_get_events_since(id, 0).await
    }
    async fn get_events(&self, id: &str) -> Option<Commit<Vec<T::Event>>> {
        match self.try_get_events(id).await {
            Ok(events) => Some(events),
            Err(Error::NotFound(_)) => None,
            Err(_) => None,
        }
    }
    async fn try_get_commit(&self, id: &str) -> Result<Commit<T>> {
        let events = self.try_get_events(id).await?;
        let id = id.to_string();
        Ok(Commit {
            id,
            version: events.version,
            inner: T::from_slice(&events.inner),
            diagnostics: None,
        })
    }
    async fn get_commit(&self, id: &str) -> Option<Commit<T>> {
        match self.try_get_commit(id).await {
            Ok(commit) => Some(commit),
            Err(Error::NotFound(_)) => None,
            Err(_) => None,
        }
    }
    async fn try_get_aggregate(&self, id: &str) -> Result<T> {
        self.try_get_commit(id).await.map(|r| r.inner)
    }
    async fn get_aggregate(&self, id: &str) -> Option<T> {
        self.get_commit(id).await.map(|r| r.inner)
    }
    #[allow(unused_variables)]
    async fn store_snapshot(&self, id: &str) -> Result<()> {
        Ok(())
    }
    #[allow(unused_variables)]
    async fn load_snapshot(&self, id: &str) -> Result<Option<Commit<T>>> {
        Ok(None)
    }

    async fn transmit(&self, id: &str, event: Commit<T::Event>) -> Result<()>;
    /// Consumes the event store and returns a stream of events.
    async fn stream(
        &self,
    ) -> impl Stream<Item = std::result::Result<Commit<T::Event>, Self::StreamError>> {
        let (_tx, rx) = tokio::sync::mpsc::unbounded_channel();
        UnboundedReceiverStream::new(rx)
            as UnboundedReceiverStream<std::result::Result<Commit<T::Event>, Self::StreamError>>
    }
    async fn stream_for<E>(
        &self,
        _id: &str,
    ) -> impl Stream<Item = std::result::Result<Commit<T::Event>, Self::StreamError>> {
        // TODO: Implement filtering by id
        self.stream().await
    }
}
