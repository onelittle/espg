#[cfg(feature = "inmem")]
mod in_memory;
#[cfg(feature = "postgres")]
pub mod postgres;
#[cfg(feature = "postgres")]
pub(crate) mod sql_helpers;
#[cfg(feature = "streaming")]
mod streaming;

use crate::{Aggregate, Id, util::Txid};
use async_trait::async_trait;
#[cfg(feature = "streaming")]
use futures::Stream;
#[cfg(feature = "inmem")]
pub use in_memory::InMemoryEventStore;
#[cfg(feature = "postgres")]
pub use postgres::PostgresEventStore;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
#[cfg(feature = "streaming")]
pub use streaming::EventStream;

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
    StreamingError(#[from] StreamItemError),
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

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct Diagnostics {
    pub loaded_events: usize,
    pub snapshotted: bool,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct Commit<T> {
    pub id: String,
    pub version: usize,
    pub inner: T,
    pub diagnostics: Option<Diagnostics>,
    pub global_seq: Option<Txid>,
}
#[cfg(feature = "async-graphql")]
#[async_graphql::Object]
impl<T: async_graphql::OutputType> Commit<T> {
    async fn id(&self) -> String {
        self.id.clone()
    }

    async fn version(&self) -> usize {
        self.version
    }

    async fn inner(&self) -> &T {
        &self.inner
    }
}

#[cfg(feature = "async-graphql")]
impl<T: async_graphql::OutputType> async_graphql::TypeName for Commit<T> {
    fn type_name() -> std::borrow::Cow<'static, str> {
        format!("{}Commit", <T as async_graphql::OutputType>::type_name()).into()
    }
}

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

    async fn retry_on_version_conflict<'a, 'b, F, Fut, R>(&'a self, mut f: F) -> Result<R>
    where
        F: FnMut() -> Fut + Send + 'b,
        Fut: std::future::Future<Output = Result<R>> + Send + 'b,
        'a: 'b,
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
}

#[cfg(feature = "streaming")]
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum StreamItemError {
    #[cfg(feature = "postgres")]
    #[error("Tokio Postgres error: {0}")]
    TokioPgError(#[from] tokio_postgres::Error),
}

#[cfg(feature = "streaming")]
pub type StreamItem<T> = std::result::Result<Commit<T>, StreamItemError>;

#[cfg(feature = "streaming")]
#[allow(async_fn_in_trait)]
pub trait StreamingEventStore {
    #[cfg(feature = "streaming")]
    async fn stream<T: Aggregate>(self) -> Result<impl Stream<Item = StreamItem<T::Event>>>;
}
