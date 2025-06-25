#[cfg(test)]
use std::sync::atomic::AtomicUsize;
use std::{any::Any, sync::Arc};

use indexmap::IndexMap;
#[cfg(feature = "streaming")]
use tokio::sync::broadcast::error::SendError;
#[cfg(feature = "streaming")]
use tokio_stream::wrappers::{UnboundedReceiverStream, errors::BroadcastStreamRecvError};

use super::{Commit, Error, EventStore, Result};
#[cfg(feature = "streaming")]
use crate::StreamingEventStore;
use crate::{Aggregate, Id};

type CommitTuple = (usize, Vec<Box<dyn Any + Send + Sync>>);

type RwLock<T> = tokio::sync::RwLock<T>;

pub struct InMemoryEventStore<T>
where
    T: Aggregate,
{
    pub(crate) store: Arc<RwLock<IndexMap<String, CommitTuple>>>,
    broadcast: tokio::sync::broadcast::Sender<Commit<T::Event>>,
    _rx: Option<tokio::sync::broadcast::Receiver<Commit<T::Event>>>,
    #[cfg(test)]
    pub(crate) version_conflicts: AtomicUsize,
}

impl<T> Clone for InMemoryEventStore<T>
where
    T: Aggregate,
{
    fn clone(&self) -> Self {
        let tx = self.broadcast.clone();
        InMemoryEventStore {
            store: self.store.clone(),
            broadcast: tx,
            _rx: None,
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
        let (tx, _rx) = tokio::sync::broadcast::channel::<Commit<T::Event>>(100);
        InMemoryEventStore {
            store: Arc::new(RwLock::new(IndexMap::new())),
            broadcast: tx,
            _rx: Some(_rx),
            #[cfg(test)]
            version_conflicts: AtomicUsize::new(0),
        }
    }
}

impl<T> EventStore<T> for InMemoryEventStore<T>
where
    T: Aggregate + Default,
    T::Event: Clone + Send + Sync + 'static,
{
    async fn append(&self, id: &Id<T>, action: T::Event) -> Result<()> {
        let version = {
            let mut store = self.store.write().await;
            let previous_commit = store.entry(id.to_string()).or_default();
            previous_commit.0 += 1;
            previous_commit.1.push(Box::new(action.clone()));
            previous_commit.0
        };
        let _commit = Commit {
            id: id.to_string(),
            version, // Version is not used in this context
            inner: action,
            diagnostics: None,
        };
        #[cfg(feature = "streaming")]
        self.transmit(_commit).await?;
        Ok(())
    }

    async fn commit(&self, id: &Id<T>, version: usize, action: T::Event) -> Result<()> {
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
            previous_commit.1.push(Box::new(action.clone()));
            previous_commit.0
        };
        let _commit = Commit {
            id: id.to_string(),
            version,
            inner: action,
            diagnostics: None,
        };
        #[cfg(feature = "streaming")]
        self.transmit(_commit).await?;
        Ok(())
    }

    async fn try_get_events_since(
        &self,
        id: &Id<T>,
        version: usize,
    ) -> Result<Commit<Vec<T::Event>>> {
        let store = self.store.read().await;
        let stored_commit = store
            .get(&id.0)
            .ok_or(Error::NotFound("Aggregate not found".to_string()))?;

        let inner: Vec<T::Event> = stored_commit
            .1
            .iter()
            .enumerate()
            .skip_while(|(v, _)| *v < version)
            .map(|(_, event)| {
                event
                    .downcast_ref::<T::Event>()
                    .expect("Event should be of type T::Event")
                    .clone()
            })
            .collect();
        Ok(Commit {
            id: id.to_string(),
            version: stored_commit.0,
            diagnostics: None,
            inner,
        })
    }
    #[cfg(feature = "streaming")]
    async fn transmit(&self, commit: Commit<T::Event>) -> Result<()> {
        match &self.broadcast.send(commit) {
            Ok(_) => {
                eprintln!("Transmitted event successfully");
            }
            Err(SendError(_event)) => {
                eprintln!("Stream closed, cannot send event");
            }
        }
        Ok(())
    }
}

#[cfg(feature = "streaming")]
impl<'a, T: Aggregate + Default> StreamingEventStore<'a, T> for InMemoryEventStore<T>
where
    T::Event: Clone + Send + 'static,
{
    type StreamType = UnboundedReceiverStream<Commit<T::Event>>;

    #[cfg(feature = "streaming")]
    async fn stream(self) -> Result<Self::StreamType> {
        use tokio_stream::wrappers::{BroadcastStream, UnboundedReceiverStream};
        let rx = self.broadcast.subscribe();
        let mut stream = BroadcastStream::new(rx);
        let (tx1, rx1) = tokio::sync::mpsc::unbounded_channel::<Commit<T::Event>>();
        let store = self.store.read().await;
        let stream2 = UnboundedReceiverStream::new(rx1);

        for (id, (_, events)) in store.iter() {
            for (version_index, event) in events.iter().enumerate() {
                let event = event
                    .downcast_ref::<T::Event>()
                    .expect("Event should be of type T::Event")
                    .clone();
                let version = version_index + 1; // Versioning starts from 1
                let commit = Commit {
                    id: id.clone(),
                    version,
                    inner: event,
                    diagnostics: None,
                };
                if tx1.send(commit).is_err() {
                    eprintln!("Stream closed, cannot send initial event");
                }
            }
        }

        tokio::spawn(async move {
            use tokio_stream::StreamExt;

            while let Some(result) = stream.next().await {
                eprintln!("Processing event from stream");
                match result {
                    Ok(commit) => {
                        if tx1.send(commit).is_err() {
                            eprintln!("Stream closed, cannot send event");
                        }
                    }
                    Err(BroadcastStreamRecvError::Lagged(_)) => {
                        eprintln!("Stream lagged, skipping events");
                    }
                }
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            eprintln!("Stream processing ended...");
        });
        Ok(stream2)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
#[allow(clippy::expect_used)]
mod tests {
    use crate::tests::{Event, State};
    use crate::{Aggregate as _, EventStore};

    use super::InMemoryEventStore;

    #[tokio::test]
    async fn test_in_memory_commands() -> Result<(), crate::event_stores::Error> {
        let event_store: InMemoryEventStore<State> = Default::default();

        let id = State::id("test1");
        event_store.append(&id, Event::Increment(10)).await?;
        event_store.append(&id, Event::Decrement(4)).await?;
        let aggregate = event_store
            .get_aggregate(&id)
            .await
            .expect("Aggregate not found");
        assert_eq!(aggregate.value, 6);
        event_store.append(&id, Event::Increment(3)).await?;
        let aggregate = event_store
            .get_aggregate(&id)
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
                    let id = State::id("test2");
                    store.append(&id, Event::Increment(i)).await.unwrap();
                })
            })
            .collect();

        for handle in handles {
            handle.await.expect("Thread panicked");
        }

        let id = State::id("test2");
        let aggregate = event_store.get_aggregate(&id).await.unwrap();
        assert_eq!(aggregate.value, 45, "Expected sum of increments to be 45");

        Ok(())
    }

    #[tokio::test]
    #[cfg(feature = "streaming")]
    async fn test_streaming() -> Result<(), crate::event_stores::Error> {
        use crate::StreamingEventStore;

        let event_store: InMemoryEventStore<State> = Default::default();
        let stream = event_store
            .clone()
            .stream()
            .await
            .expect("msg: Failed to create event stream");

        let events_to_send = vec![
            Event::Increment(10),
            Event::Decrement(5),
            Event::Increment(5),
            Event::Decrement(2),
        ];

        let events_to_receive = events_to_send.clone();
        let id = State::id("test5");
        for event in events_to_send {
            event_store.append(&id, event).await?;
        }

        let handle = tokio::spawn(async move {
            use tokio_stream::StreamExt;

            let mut stream = stream.take(4);
            let mut n = 0;
            let mut iter = events_to_receive.iter();
            while let Some(commit) = stream.next().await {
                let expected_event = iter.next().expect("Got more events than expected");
                assert_eq!(
                    commit.id, "test5",
                    "Expected event ID to be 'test5' at index {}",
                    n
                );
                assert_eq!(
                    commit.version,
                    n + 1,
                    "Expected version to match at index {}",
                    n
                );
                assert!(
                    commit.diagnostics.is_none(),
                    "Expected no diagnostics at index {}",
                    n
                );
                assert_eq!(
                    commit.inner,
                    expected_event.clone(),
                    "Expected event at index {} to match",
                    n
                );
                n += 1;
            }
            n
        });

        let timeout = tokio::time::timeout(tokio::time::Duration::from_secs(5), handle);
        match timeout.await {
            Ok(Ok(n)) => {
                assert_eq!(n, 4, "Expected 4 events in the stream");
            }
            Ok(Err(e)) => {
                panic!("Stream failed with error: {}", e);
            }
            Err(_) => {
                panic!("Stream did not complete in time");
            }
        }

        Ok(())
    }

    #[tokio::test]
    #[cfg(feature = "streaming")]
    async fn test_streaming_after_writes() -> Result<(), crate::event_stores::Error> {
        use crate::StreamingEventStore;

        let event_store: InMemoryEventStore<State> = Default::default();

        let events_to_send = vec![
            Event::Increment(10),
            Event::Decrement(5),
            Event::Increment(5),
            Event::Decrement(2),
        ];

        let events_to_receive = events_to_send.clone();
        let id = State::id("test5");
        for event in events_to_send {
            event_store.append(&id, event).await?;
        }

        let stream = event_store
            .clone()
            .stream()
            .await
            .expect("msg: Failed to create event stream");

        let handle = tokio::spawn(async move {
            use tokio_stream::StreamExt;

            let mut stream = stream.take(4);
            let mut n = 0;
            let mut iter = events_to_receive.iter();
            while let Some(commit) = stream.next().await {
                let expected_event = iter.next().expect("Got more events than expected");
                assert_eq!(
                    commit.id, "test5",
                    "Expected event ID to be 'test5' at index {}",
                    n
                );
                assert_eq!(
                    commit.version,
                    n + 1,
                    "Expected version to match at index {}",
                    n
                );
                assert!(
                    commit.diagnostics.is_none(),
                    "Expected no diagnostics at index {}",
                    n
                );
                assert_eq!(
                    commit.inner,
                    expected_event.clone(),
                    "Expected event at index {} to match",
                    n
                );
                n += 1;
            }
            n
        });

        let timeout = tokio::time::timeout(tokio::time::Duration::from_secs(5), handle);
        match timeout.await {
            Ok(Ok(n)) => {
                assert_eq!(n, 4, "Expected 4 events in the stream");
            }
            Ok(Err(e)) => {
                panic!("Stream failed with error: {}", e);
            }
            Err(_) => {
                panic!("Stream did not complete in time");
            }
        }

        Ok(())
    }
}
