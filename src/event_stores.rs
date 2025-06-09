mod in_memory;
mod postgres;

use crate::Aggregate;
pub use in_memory::InMemoryEventStore;
pub use postgres::PostgresEventStore;
use tokio_stream::Stream;

#[derive(Debug)]
pub enum Error {
    NotFound(String),
    AccessConflict,
    VersionConflict,
}

pub type Result<T> = std::result::Result<T, Error>;

pub struct Commit<T> {
    pub version: usize,
    pub inner: T,
}

#[allow(async_fn_in_trait)]
pub trait StreamingEventStore<T>
where
    Self: EventStore<T>,
    T: Aggregate,
    T::Event: 'static,
{
    async fn transmit(&mut self, id: &str, event: T::Event) -> Result<()>;
    async fn stream(
        &self,
    ) -> impl Stream<
        Item = std::result::Result<
            T::Event,
            tokio_stream::wrappers::errors::BroadcastStreamRecvError,
        >,
    >;
}

#[allow(async_fn_in_trait)]
pub trait EventStore<T>
where
    T: Aggregate,
{
    async fn append(&mut self, id: &str, event: T::Event) -> Result<()>;
    async fn commit(&mut self, id: &str, version: usize, event: T::Event) -> Result<()>;
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
        Ok(Commit {
            version: events.version,
            inner: T::from_slice(&events.inner),
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
    async fn store_snapshot(&mut self, id: &str) -> Result<()> {
        Ok(())
    }
    #[allow(unused_variables)]
    async fn load_snapshot(&self, id: &str) -> Result<Option<Commit<T>>> {
        Ok(None)
    }
}
