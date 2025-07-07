mod aggregate;
#[cfg(feature = "async-graphql")]
mod async_graphql;
mod commit;
mod event_stores;
mod id;
#[cfg(feature = "rocket")]
mod rocket;
#[cfg(feature = "streaming")]
mod subscriber;
mod util;

pub use aggregate::Aggregate;
pub use commit::Commit;
#[cfg(feature = "inmem")]
pub use event_stores::InMemoryEventStore;
pub use event_stores::PostgresEventStore;
pub use event_stores::{Error, EventStore, Result, retry_on_version_conflict};
pub use id::{Id, id};

#[cfg(feature = "streaming")]
pub use event_stores::{StreamItem, StreamingEventStore};

#[cfg(feature = "streaming")]
pub use event_stores::postgres::PostgresEventStream;

#[cfg(feature = "streaming")]
pub use subscriber::{Subscriber, Subscription};

#[cfg(test)]
mod tests {
    use serde::{Deserialize, Serialize};

    use super::Aggregate;

    #[derive(Default, Serialize, Deserialize)]
    #[cfg_attr(feature = "async-graphql", derive(async_graphql::SimpleObject))]
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
        use crate::{EventStore, Id, Result, retry_on_version_conflict};

        pub async fn increment(
            event_store: &impl EventStore,
            id: &Id<State>,
            amount: i32,
        ) -> Result<()> {
            retry_on_version_conflict(async || {
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
            retry_on_version_conflict(async || {
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
mod test_helper {
    use std::sync::atomic::AtomicBool;
    use tokio::io::AsyncReadExt;

    pub(crate) struct TestDb {
        pub(crate) name: String,
        _stream: tokio::net::UnixStream,
        initialized: AtomicBool,
    }

    impl TestDb {
        pub fn connection_string(&self) -> String {
            format!("postgres://localhost:5432/{}", self.name)
        }

        pub async fn tokio_postgres_config(&self) -> tokio_postgres::Config {
            let mut config = tokio_postgres::Config::new();
            config.host("localhost").port(5432).dbname(&self.name);
            self.initialize(&config)
                .await
                .expect("Failed to initialize test database");
            config
        }

        async fn connect(
            &self,
        ) -> (
            tokio_postgres::Client,
            tokio_postgres::Connection<tokio_postgres::Socket, tokio_postgres::tls::NoTlsStream>,
        ) {
            let config = self.tokio_postgres_config().await;
            config
                .connect(tokio_postgres::NoTls)
                .await
                .expect("Failed to connect to test database")
        }

        pub async fn client(&self) -> tokio_postgres::Client {
            let (client, connection) = self.connect().await;
            tokio::spawn(async move {
                if let Err(e) = connection.await {
                    eprintln!("Connection error: {}", e);
                }
            });

            client
        }

        pub async fn initialize(&self, config: &tokio_postgres::Config) -> crate::Result<()> {
            if self.initialized.load(std::sync::atomic::Ordering::SeqCst) {
                return Ok(());
            }

            let (client, connection) = config.connect(tokio_postgres::NoTls).await?;

            tokio::spawn(async move {
                if let Err(e) = connection.await {
                    eprintln!("Connection error: {}", e);
                }
            });

            crate::event_stores::PostgresEventStore::initialize(&client).await?;
            crate::event_stores::PostgresEventStore::clear(&client).await?;
            self.initialized
                .store(true, std::sync::atomic::Ordering::SeqCst);

            Ok(())
        }
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

            eprintln!("Using test database: {}", db_name);
            return TestDb {
                name: db_name.clone(),
                _stream: stream,
                initialized: AtomicBool::new(false),
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
