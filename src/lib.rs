mod aggregate;
pub mod event_stores;

pub use aggregate::{Aggregate, Id};
#[cfg(feature = "inmem")]
pub use event_stores::InMemoryEventStore;
#[cfg(feature = "postgres")]
pub use event_stores::PostgresEventStore;
pub use event_stores::{Commit, Error, EventStore, Result};

#[cfg(feature = "streaming")]
pub use event_stores::{EventStream, StreamingEventStore};

#[cfg(feature = "streaming")]
#[cfg(feature = "postgres")]
pub use event_stores::postgres::PostgresEventStream;

#[cfg(test)]
#[allow(dead_code)]
mod tests {
    use serde::{Deserialize, Serialize};

    use super::Aggregate;

    #[derive(Default, Serialize, Deserialize)]
    pub struct State {
        pub value: i32,
    }

    #[derive(Clone, Serialize, Deserialize, PartialEq, Debug)]
    pub enum Event {
        Increment(i32),
        Decrement(i32),
    }

    impl Aggregate for State {
        const NAME: &'static str = "espg::tests::State";
        type Event = Event;

        fn reduce(mut self, event: &Self::Event) -> Self {
            match event {
                Event::Increment(amount) => self.value += amount,
                Event::Decrement(amount) => self.value -= amount,
            }
            self
        }

        fn snapshot_key() -> Option<&'static str> {
            Some("state_snapshot")
        }
    }

    pub mod commands {
        use super::{Event, State};
        use crate::{EventStore, Id, Result};

        pub async fn increment(
            event_store: &impl EventStore,
            id: &Id<State>,
            amount: i32,
        ) -> Result<()> {
            event_store
                .retry_on_version_conflict(|| async {
                    let aggregate = event_store.try_get_commit(id).await?;
                    let event = Event::Increment(amount);
                    event_store.commit(id, aggregate.version + 1, event).await?;
                    Ok(())
                })
                .await
        }

        pub async fn decrement(
            event_store: &impl EventStore,
            id: &Id<State>,
            amount: i32,
        ) -> Result<()> {
            event_store
                .retry_on_version_conflict(|| async {
                    let aggregate = event_store.try_get_commit(id).await?;
                    let event = Event::Decrement(amount);
                    event_store.commit(id, aggregate.version + 1, event).await?;
                    Ok(())
                })
                .await
        }
    }

    #[test]
    fn test_aggregate_reducer() {
        let initial = State { value: 0 };
        let incremented = initial.reduce(&Event::Increment(5));
        assert_eq!(incremented.value, 5);
        let decremented = incremented.reduce(&Event::Decrement(3));
        assert_eq!(decremented.value, 2);
    }

    #[test]
    fn test_from_vec() {
        let events: Vec<Event> = vec![
            Event::Increment(5),
            Event::Decrement(3),
            Event::Increment(2),
        ];
        let aggregate = State::from_slice(&events);
        assert_eq!(aggregate.value, 4);
    }

    #[test]
    fn test_snapshot_key() {
        assert_eq!(State::snapshot_key(), Some("state_snapshot"));
    }

    #[test]
    fn test_name() {
        assert_eq!(State::NAME, "espg::tests::State");
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
#[allow(clippy::unwrap_used)]
#[cfg(feature = "postgres")]
mod test_helper {
    use tokio::io::AsyncReadExt;

    pub(crate) struct TestDb {
        pub(crate) name: String,
        _stream: tokio::net::UnixStream,
    }

    pub(crate) async fn get_test_database() -> TestDb {
        // Connect to socket at cwd/tmp/test_manager.sock
        let path = std::env::var("PGMANAGER_SOCKET").expect("PGMANAGER_SOCKET must be set");
        let mut stream = tokio::net::UnixStream::connect(path)
            .await
            .expect("Failed to connect to test manager socket");
        let mut buffer = [0; 1024];
        let read = stream
            .read(&mut buffer)
            .await
            .expect("Failed to read from test manager socket");
        if read == 0 {
            panic!("Test manager socket closed unexpectedly");
        }
        let response = String::from_utf8_lossy(&buffer);
        if response.starts_with("OK:") {
            let db_name = response.strip_prefix("OK:").unwrap().trim().to_string();
            // Remove embedded null characters
            let db_name = db_name.replace('\0', "");
            return TestDb {
                name: db_name.clone(),
                _stream: stream,
            };
        }

        if response.starts_with("EMPTY:") {
            panic!(
                "No databases available: {}",
                response.strip_prefix("ERROR:").unwrap().trim()
            );
        }

        panic!("Unexpected response from test manager: {}", response);
    }
}
