use std::sync::atomic::AtomicI64;
#[cfg(test)]
use std::sync::atomic::AtomicUsize;
use std::{any::Any, sync::Arc};

use async_trait::async_trait;
#[cfg(feature = "streaming")]
use futures::Stream;
use indexmap::IndexMap;
#[cfg(feature = "streaming")]
use tokio_stream::wrappers::errors::BroadcastStreamRecvError;

use super::{Commit, Error, EventStore, Result};
#[cfg(feature = "streaming")]
use crate::StreamingEventStore;
use crate::{Aggregate, Id, util::Txid};

type CommitTuple = Vec<(Txid, Box<dyn Any + Send + Sync>)>;

type RwLock<T> = tokio::sync::RwLock<T>;

pub struct InMemoryEventStore {
    pub(crate) store: Arc<RwLock<IndexMap<String, CommitTuple>>>,
    pub(crate) subscriptions: Arc<RwLock<IndexMap<(String, String), Txid>>>,
    txid: Arc<AtomicI64>,
    broadcast: tokio::sync::broadcast::Sender<Commit<String>>,
    _rx: Option<tokio::sync::broadcast::Receiver<Commit<String>>>,
    #[cfg(test)]
    pub(crate) version_conflicts: AtomicUsize,
}

impl InMemoryEventStore {
    pub async fn len(&self) -> usize {
        let store = self.store.read().await;
        let mut events = 0;
        for (_, event_list) in store.iter() {
            events += event_list.len();
        }
        events
    }

    pub async fn is_empty(&self) -> bool {
        let store = self.store.read().await;
        store.is_empty()
    }
}

impl Clone for InMemoryEventStore {
    fn clone(&self) -> Self {
        let tx = self.broadcast.clone();
        InMemoryEventStore {
            store: self.store.clone(),
            subscriptions: self.subscriptions.clone(),
            txid: self.txid.clone(),
            broadcast: tx,
            _rx: None,
            #[cfg(test)]
            version_conflicts: AtomicUsize::new(0),
        }
    }
}

impl Default for InMemoryEventStore {
    fn default() -> Self {
        let (tx, _rx) = tokio::sync::broadcast::channel::<Commit<String>>(100);
        InMemoryEventStore {
            store: Arc::new(RwLock::new(IndexMap::new())),
            subscriptions: Arc::new(RwLock::new(IndexMap::new())),
            txid: Arc::new(AtomicI64::new(1)),
            broadcast: tx,
            _rx: Some(_rx),
            #[cfg(test)]
            version_conflicts: AtomicUsize::new(0),
        }
    }
}

impl InMemoryEventStore {
    #[cfg(feature = "streaming")]
    async fn transmit<X: Aggregate>(&self, commit: Commit<X::Event>) -> Result<()> {
        use tokio::sync::broadcast::error::SendError;

        #[allow(clippy::expect_used)]
        let commit = Commit {
            id: commit.id,
            version: commit.version,
            inner: serde_json::to_string(&commit.inner).expect("Failed to serialize event to JSON"),
            diagnostics: commit.diagnostics,
            global_seq: commit.global_seq,
        };
        match &self.broadcast.send(commit) {
            Ok(_) => {}
            Err(SendError(_event)) => {
                eprintln!("Stream closed, cannot send event");
            }
        }
        Ok(())
    }
}

#[async_trait]
impl EventStore for InMemoryEventStore {
    async fn append<X: Aggregate>(&self, id: &Id<X>, action: X::Event) -> Result<()> {
        let (global_seq, version) = {
            let mut store = self.store.write().await;
            let previous_commit = store.entry(id.to_string()).or_default();
            let global_seq = self
                .txid
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
                .into();
            eprintln!("Global sequence: {}", global_seq);
            previous_commit.push((global_seq, Box::new(action.clone())));
            (Some(global_seq), previous_commit.len())
        };
        let _commit = Commit {
            id: id.to_string(),
            version, // Version is not used in this context
            inner: action,
            diagnostics: None,
            global_seq,
        };
        #[cfg(feature = "streaming")]
        self.transmit::<X>(_commit).await?;
        Ok(())
    }

    async fn commit<X: Aggregate>(
        &self,
        id: &Id<X>,
        version: usize,
        action: X::Event,
    ) -> Result<()> {
        let (global_seq, version) = {
            let mut store = self.store.write().await;
            let previous_commit = store.entry(id.to_string()).or_default();
            if previous_commit.len() != version - 1 {
                #[cfg(test)]
                self.version_conflicts
                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                return Err(Error::VersionConflict(version));
            }
            let global_seq = self
                .txid
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
                .into();
            previous_commit.push((global_seq, Box::new(action.clone())));
            eprintln!("Global sequence: {}", global_seq);
            (Some(global_seq), previous_commit.len())
        };
        let _commit = Commit {
            id: id.to_string(),
            version,
            inner: action,
            diagnostics: None,
            global_seq,
        };
        #[cfg(feature = "streaming")]
        self.transmit::<X>(_commit).await?;
        Ok(())
    }

    async fn try_get_events_since<X: Aggregate>(
        &self,
        id: &Id<X>,
        version: usize,
    ) -> Result<Commit<Vec<X::Event>>> {
        let store = self.store.read().await;
        let stored_commit = store
            .get(&id.0.to_string())
            .ok_or(Error::NotFound("Aggregate not found".to_string()))?;

        let inner: Vec<X::Event> = stored_commit
            .iter()
            .enumerate()
            .skip_while(|(v, _)| *v < version)
            .map(|(_, (_, event))| {
                #[allow(clippy::expect_used)]
                event
                    .downcast_ref::<X::Event>()
                    .expect("Event should be of type T::Event")
                    .clone()
            })
            .collect();
        Ok(Commit {
            id: id.to_string(),
            version: stored_commit.len(),
            diagnostics: None,
            inner,
            global_seq: None, // Global sequence is not used in this context
        })
    }
}

#[cfg(feature = "streaming")]
impl StreamingEventStore for InMemoryEventStore {
    async fn stream<X: Aggregate>(self) -> Result<impl Stream<Item = Commit<X::Event>>> {
        use tokio_stream::wrappers::{BroadcastStream, UnboundedReceiverStream};
        let rx = self.broadcast.subscribe();
        let mut stream = BroadcastStream::new(rx);
        let (tx1, rx1) = tokio::sync::mpsc::unbounded_channel::<Commit<X::Event>>();
        let store = self.store.read().await;
        let stream2 = UnboundedReceiverStream::new(rx1);

        for (id, tuples) in store.iter() {
            for (version_index, (txid, event)) in tuples.iter().enumerate() {
                #[allow(clippy::expect_used)]
                let event = event
                    .downcast_ref::<X::Event>()
                    .expect("Event should be of type T::Event")
                    .clone();
                let version = version_index + 1; // Versioning starts from 1
                let commit = Commit {
                    id: id.clone(),
                    version,
                    inner: event,
                    diagnostics: None,
                    global_seq: Some(*txid), // Global sequence is not used in this context
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
                        let result = serde_json::from_str::<X::Event>(&commit.inner);
                        match result {
                            Ok(event) => {
                                let commit = Commit {
                                    id: commit.id,
                                    version: commit.version,
                                    inner: event,
                                    diagnostics: commit.diagnostics,
                                    global_seq: commit.global_seq,
                                };
                                if tx1.send(commit).is_err() {
                                    eprintln!("Stream closed, cannot send event");
                                }
                            }
                            Err(e) => {
                                eprintln!("Failed to deserialize event: {}", e);
                                match e.classify() {
                                    serde_json::error::Category::Syntax => {
                                        eprintln!("Syntax error in event JSON: {}", e);
                                    }
                                    serde_json::error::Category::Data => {
                                        eprintln!("Data error in event JSON: {}", e);
                                    }
                                    serde_json::error::Category::Eof => {
                                        eprintln!("Unexpected end of file in event JSON: {}", e);
                                    }
                                    serde_json::error::Category::Io => {
                                        eprintln!("I/O error while reading event JSON: {}", e);
                                    }
                                }
                                continue;
                            }
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
        let event_store: InMemoryEventStore = Default::default();

        let id = State::id("test1");
        event_store.append(&id, Event::Increment(10)).await?;
        event_store.append(&id, Event::Decrement(4)).await?;
        assert_eq!(event_store.len().await, 2);
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
        let event_store: InMemoryEventStore = Default::default();

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

        let event_store: InMemoryEventStore = Default::default();
        let stream = event_store
            .clone()
            .stream::<State>()
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

        let event_store: InMemoryEventStore = Default::default();

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
            .stream::<State>()
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
