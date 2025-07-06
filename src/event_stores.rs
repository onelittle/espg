#[cfg(feature = "inmem")]
mod in_memory;
#[cfg(feature = "postgres")]
pub mod postgres;
#[cfg(feature = "postgres")]
pub(crate) mod sql_helpers;
#[cfg(feature = "streaming")]
mod streaming;

use std::collections::HashMap;

use crate::{Aggregate, Commit, Id, util::Loadable};
use async_trait::async_trait;
#[cfg(feature = "inmem")]
pub use in_memory::InMemoryEventStore;
#[cfg(feature = "postgres")]
pub use postgres::PostgresEventStore;
use serde::{Serialize, de::DeserializeOwned};
#[cfg(feature = "streaming")]
pub use streaming::{StreamItem, StreamingEventStore};

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
    #[cfg(feature = "postgres")]
    /// Only available when the `postgres` feature is enabled.
    TokioPgError(#[from] tokio_postgres::Error),
    #[error("serde_json error: {0}")]
    #[cfg(feature = "postgres")]
    /// Only available when the `postgres` feature is enabled.
    SerdeJsonError(#[from] serde_json::Error),
    #[cfg(feature = "streaming")]
    #[error("streaming error: {0}")]
    StreamingError(#[from] streaming::StreamItemError),
    #[error("txid parsing error: {0}")]
    TxIdParsingError(#[from] std::num::ParseIntError),
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

#[async_trait]
pub trait EventStore: Sync {
    async fn append<X: Aggregate>(&self, id: &Id<X>, event: X::Event) -> Result<()>;
    async fn commit<X: Aggregate + Serialize>(
        &self,
        id: &Id<X>,
        version: usize,
        event: X::Event,
    ) -> Result<()>;
    async fn try_get_events_between<X: Aggregate>(
        &self,
        id: &Id<X>,
        start_version: usize,
        end_version: Option<usize>,
    ) -> Result<Commit<Vec<X::Event>>>;
    async fn try_get_events_since<X: Aggregate>(
        &self,
        id: &Id<X>,
        version: usize,
    ) -> Result<Commit<Vec<X::Event>>> {
        self.try_get_events_between(id, version + 1, None).await
    }
    async fn try_get_events_before<X: Aggregate>(
        &self,
        id: &Id<X>,
        version: usize,
    ) -> Result<Commit<Vec<X::Event>>> {
        self.try_get_events_between(id, 1, Some(version - 1)).await
    }
    async fn try_get_events<X: Aggregate>(&self, id: &Id<X>) -> Result<Commit<Vec<X::Event>>> {
        self.try_get_events_since(id, 0).await
    }
    async fn get_events<X: Aggregate>(&self, id: &Id<X>) -> Option<Commit<Vec<X::Event>>> {
        match self.try_get_events(id).await {
            Ok(events) => Some(events),
            Err(Error::NotFound(_)) => None,
            Err(_) => None,
        }
    }
    async fn try_get_commit<X: Aggregate>(&self, id: &Id<X>) -> Result<Commit<X>> {
        let events = self.try_get_events(id).await?;
        let id = id.to_string();
        Ok(Commit {
            id,
            version: events.version,
            inner: X::from_slice(&events.inner),
            diagnostics: None,
            global_seq: events.global_seq,
        })
    }
    async fn get_commit<X: Aggregate>(&self, id: &Id<X>) -> Option<Commit<X>> {
        match self.try_get_commit(id).await {
            Ok(commit) => Some(commit),
            Err(Error::NotFound(_)) => None,
            Err(_) => None,
        }
    }
    async fn get_commits<X: Aggregate>(&self, ids: &[Id<X>]) -> Result<HashMap<Id<X>, Commit<X>>> {
        let mut commits = HashMap::new();
        for id in ids {
            let commit = self
                .get_commit(id)
                .await
                .ok_or(Error::NotFound(id.to_string()))?;
            commits.insert(id.clone(), commit);
        }
        Ok(commits)
    }
    async fn try_get_aggregate<X: Aggregate + Default>(&self, id: &Id<X>) -> Result<X> {
        self.try_get_commit(id).await.map(|r| r.inner)
    }
    async fn get_aggregate<X: Aggregate + Default>(&self, id: &Id<X>) -> Option<X> {
        self.get_commit(id).await.map(|r| r.inner)
    }
    #[allow(unused_variables)]
    async fn store_snapshot<X: Aggregate + Serialize>(&self, id: &Id<X>) -> Result<()> {
        Ok(())
    }
    #[allow(unused_variables)]
    async fn load_snapshot<X: Aggregate + DeserializeOwned>(
        &self,
        id: &Id<X>,
    ) -> Result<Option<Commit<X>>> {
        Ok(None)
    }

    /// Loads one or more aggregates based on the passed value.
    ///
    /// - [`Id<Aggregate>`](crate::Id) can be used to load a single aggregate
    /// - [`Vec<Id<Aggregate>>`](std::vec::Vec) can be used to load multiple aggregates
    /// - [`HashMap<K, V = Id<Aggregate>>`](std::collections::HashMap) can be used to load multiple aggregates as a hash
    #[allow(private_bounds)]
    fn load<L: Loadable>(&self, value: L) -> impl Future<Output = Result<L::Output>>
    where
        Self: Sized,
    {
        value.load(self)
    }
}

pub struct Transaction<T> {
    pub(crate) txn: T,
}

pub async fn retry_on_version_conflict<F, Fut, R>(mut f: F) -> Result<R>
where
    F: FnMut() -> Fut + Send,
    Fut: std::future::Future<Output = Result<R>> + Send,
{
    loop {
        match f().await {
            Ok(res) => return Ok(res),
            Err(Error::VersionConflict(_)) => {
                eprintln!("Version conflict occurred, retrying...");
            }
            Err(e) => return Err(e),
        }
    }
}
