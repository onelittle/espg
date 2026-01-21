#[cfg(feature = "inmem")]
mod in_memory;
pub mod postgres;
pub(crate) mod sql_helpers;
#[cfg(feature = "streaming")]
pub(crate) mod streaming;

use std::sync::atomic::AtomicUsize;

use crate::{Aggregate, Commit, Id, util::Loadable};
pub use crate::{Error, Result};
#[cfg(feature = "streaming")]
use crate::{Subscriber, subscriber::SubscriptionState};
use async_trait::async_trait;
#[cfg(feature = "inmem")]
pub use in_memory::InMemoryEventStore;
pub use postgres::PostgresEventStore;
use serde::{Serialize, de::DeserializeOwned};
#[cfg(feature = "streaming")]
pub use streaming::{StreamItem, StreamingEventStore};

#[async_trait]
pub trait EventStore: Sync {
    #[cfg(feature = "uuid")]
    async fn create<X: Aggregate>(&self, event: X::Event) -> Result<Id<X>> {
        retry_on_version_conflict(async || {
            let id: Id<X> = uuid::Uuid::new_v4().into();
            self.commit(&id, 1, event.clone()).await.map(|_| id)
        })
        .await
    }
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
    async fn all<X: Aggregate>(&self) -> Option<Vec<Commit<X>>>;
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
    async fn get_commits<X: Aggregate>(&self, ids: &[&Id<X>]) -> Result<Vec<Commit<X>>> {
        let mut commits = vec![];
        for id in ids {
            commits.push(self.try_get_commit(id).await?)
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
    fn load<L: Loadable>(&self, value: L) -> impl Future<Output = Result<L::Output>>
    where
        Self: Sized,
    {
        value.load(self)
    }

    #[cfg(feature = "streaming")]
    async fn get_subscription_state<A: Aggregate + 'static, S: Subscriber<A>>(
        &self,
    ) -> Result<SubscriptionState<A, S>>;
}

pub struct Transaction<T> {
    pub(crate) txn: T,
    pub(crate) query_count: AtomicUsize,
    pub(crate) write_count: AtomicUsize,
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
