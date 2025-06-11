use std::convert::Infallible;
use std::sync::Arc;
#[cfg(test)]
use std::sync::atomic::AtomicUsize;

use indexmap::IndexMap;
use tokio::sync::broadcast::error::SendError;
use tokio_stream::{Stream, wrappers::BroadcastStream};

use super::{Commit, Error, EventStore, Result};
use crate::Aggregate;

type CommitTuple<T> = (usize, Vec<T>);

type RwLock<T> = tokio::sync::RwLock<T>;

pub struct InMemoryEventStore<T>
where
    T: Aggregate,
{
    pub(crate) store: Arc<RwLock<IndexMap<String, CommitTuple<T::Event>>>>,
    broadcast: tokio::sync::broadcast::Sender<Commit<T::Event>>,
    _rx: tokio::sync::broadcast::Receiver<Commit<T::Event>>,
    #[cfg(test)]
    pub(crate) version_conflicts: AtomicUsize,
}

impl<T> Clone for InMemoryEventStore<T>
where
    T: Aggregate,
{
    fn clone(&self) -> Self {
        let tx = self.broadcast.clone();
        let rx = tx.subscribe();
        InMemoryEventStore {
            store: Arc::clone(&self.store),
            broadcast: tx,
            _rx: rx,
            #[cfg(test)]
            version_conflicts: AtomicUsize::new(0),
        }
    }
}

impl<T> Default for InMemoryEventStore<T>
where
    T: Aggregate,
    T::Event: Clone,
{
    fn default() -> Self {
        let (tx, rx) = tokio::sync::broadcast::channel::<Commit<T::Event>>(100);
        InMemoryEventStore {
            store: Arc::new(RwLock::new(IndexMap::new())),
            broadcast: tx,
            _rx: rx,
            #[cfg(test)]
            version_conflicts: AtomicUsize::new(0),
        }
    }
}

impl<T> EventStore<T> for InMemoryEventStore<T>
where
    T: Aggregate + Default,
    T::Event: Clone + Send + 'static,
{
    type StoreError = Infallible;
    type StreamError = tokio_stream::wrappers::errors::BroadcastStreamRecvError;

    async fn append(&self, id: &str, action: T::Event) -> Result<()> {
        let version = {
            let mut store = self.store.write().await;
            let previous_commit = store.entry(id.to_string()).or_default();
            previous_commit.0 += 1;
            previous_commit.1.push(action.clone());
            previous_commit.0
        };
        let commit = Commit {
            id: id.to_string(),
            version, // Version is not used in this context
            inner: action,
            diagnostics: None,
        };
        self.transmit(id, commit).await?;
        Ok(())
    }

    async fn commit(&self, id: &str, version: usize, action: T::Event) -> Result<()> {
        let version = {
            let mut store = self.store.write().await;
            let previous_commit = store.entry(id.to_string()).or_default();
            if previous_commit.0 >= version {
                #[cfg(test)]
                self.version_conflicts
                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                return Err(Error::VersionConflict(version));
            }
            previous_commit.0 += 1;
            previous_commit.1.push(action.clone());
            previous_commit.0
        };
        let commit = Commit {
            id: id.to_string(),
            version,
            inner: action,
            diagnostics: None,
        };
        self.transmit(id, commit).await?;
        Ok(())
    }

    async fn try_get_events_since(
        &self,
        id: &str,
        version: usize,
    ) -> Result<Commit<Vec<T::Event>>> {
        let store = self.store.read().await;
        let stored_commit = store
            .get(id)
            .ok_or(Error::NotFound("Aggregate not found".to_string()))?;
        Ok(Commit {
            id: id.to_string(),
            version: stored_commit.0,
            diagnostics: None,
            inner: stored_commit
                .1
                .iter()
                .enumerate()
                .skip_while(|(v, _)| *v < version)
                .map(|(_, event)| event)
                .cloned()
                .collect(),
        })
    }

    async fn transmit(&self, _: &str, commit: Commit<T::Event>) -> Result<()> {
        match &self.broadcast.send(commit) {
            Ok(_) => {}
            Err(SendError(_event)) => {
                eprintln!("Stream closed, cannot send event");
            }
        }
        Ok(())
    }

    async fn stream(
        &self,
    ) -> impl Stream<
        Item = std::result::Result<
            Commit<T::Event>,
            tokio_stream::wrappers::errors::BroadcastStreamRecvError,
        >,
    > {
        BroadcastStream::new(self.broadcast.subscribe()) as BroadcastStream<Commit<T::Event>>
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
#[allow(clippy::expect_used)]
mod tests {
    use crate::EventStore;
    use crate::tests::{Event, State};

    use super::InMemoryEventStore;

    #[tokio::test]
    async fn test_in_memory_commands() -> Result<(), crate::event_stores::Error> {
        let event_store: InMemoryEventStore<State> = Default::default();

        event_store.append("test1", Event::Increment(10)).await?;
        event_store.append("test1", Event::Decrement(4)).await?;
        let aggregate = event_store
            .get_aggregate("test1")
            .await
            .expect("Aggregate not found");
        assert_eq!(aggregate.value, 6);
        event_store.append("test1", Event::Increment(3)).await?;
        let aggregate = event_store
            .get_aggregate("test1")
            .await
            .expect("Aggregate not found");
        assert_eq!(aggregate.value, 9);
        assert_eq!(
            event_store
                .version_conflicts
                .load(std::sync::atomic::Ordering::SeqCst),
            0
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_concurrent_commits() -> Result<(), crate::event_stores::Error> {
        let event_store: InMemoryEventStore<State> = Default::default();

        let handles: Vec<_> = (0..10)
            .map(|i| {
                let store = event_store.clone();
                tokio::spawn(async move {
                    store.append("test2", Event::Increment(i)).await.unwrap();
                })
            })
            .collect();

        for handle in handles {
            handle.await.expect("Thread panicked");
        }

        let aggregate = event_store.get_aggregate("test2").await.unwrap();
        assert_eq!(aggregate.value, 45, "Expected sum of increments to be 45");

        Ok(())
    }
}
