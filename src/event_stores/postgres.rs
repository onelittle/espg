use std::{convert::Infallible, sync::Arc};

use super::{Error, Result};
use crate::{
    Aggregate, EventStore,
    event_stores::{Commit, Diagnostics},
};
use futures::{FutureExt, TryStreamExt};
use futures::{StreamExt, stream};
use futures_channel::mpsc;
use serde::{Serialize, de::DeserializeOwned};
use serde_json::{Value, json};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_postgres::{
    AsyncMessage, GenericClient, Row,
    types::{FromSql, Json},
};
use tokio_stream::wrappers::UnboundedReceiverStream;

pub struct PostgresEventStore<T> {
    client: Arc<tokio_postgres::Client>,
    aggregate_type: std::marker::PhantomData<T>,
    snapshot_interval: usize,
}

impl<'b, T> PostgresEventStore<T>
where
    T: Aggregate,
{
    pub async fn new(client: Arc<tokio_postgres::Client>) -> Self {
        PostgresEventStore {
            client,
            aggregate_type: std::marker::PhantomData,
            snapshot_interval: 10,
        }
    }

    pub async fn begin(
        &'b mut self,
    ) -> std::result::Result<PostgresEventStoreTransaction<'b, T>, tokio_postgres::Error> {
        #[allow(clippy::expect_used)]
        let transaction = Arc::get_mut(&mut self.client)
            .expect("Client must be unique to begin a transaction")
            .transaction()
            .await?;
        Ok(PostgresEventStoreTransaction {
            transaction,
            snapshot_interval: self.snapshot_interval,
            r#type: std::marker::PhantomData,
        })
    }

    pub async fn initialize(&self) -> std::result::Result<(), tokio_postgres::Error> {
        // Create the necessary tables if they do not exist
        self.client
            .execute(
                r#"
                CREATE TABLE IF NOT EXISTS events (
                    aggregate_id TEXT NOT NULL,
                    aggregate_type TEXT NOT NULL,
                    version INT NOT NULL,
                    action JSONB NOT NULL,
                    PRIMARY KEY (aggregate_id, aggregate_type, version)
                )
            "#,
                &[],
            )
            .await?;

        self.client
            .execute(
                r#"
                CREATE TABLE IF NOT EXISTS snapshots (
                    aggregate_id TEXT NOT NULL,
                    aggregate_type TEXT NOT NULL,
                    key TEXT NOT NULL,
                    version INT NOT NULL,
                    snapshot JSONB NOT NULL,
                    PRIMARY KEY (aggregate_id, aggregate_type, key)
                )
            "#,
                &[],
            )
            .await?;

        Ok(())
    }

    pub async fn clear(&self) -> std::result::Result<(), tokio_postgres::Error> {
        self.client
            .batch_execute(
                r#"
                DELETE FROM events;
                DELETE FROM snapshots;
            "#,
            )
            .await?;
        Ok(())
    }
}

pub struct PostgresEventStoreTransaction<'a, T> {
    transaction: tokio_postgres::Transaction<'a>,
    snapshot_interval: usize,
    r#type: std::marker::PhantomData<T>,
}

impl<'a, T> PostgresEventStoreTransaction<'a, T> {
    pub async fn complete(self) -> Result<()> {
        // TODO: Handle potential conflicts
        self.transaction.commit().await?;
        Ok(())
    }

    pub async fn rollback(self) -> Result<()> {
        self.transaction.rollback().await?;
        Ok(())
    }
}

mod private {
    pub trait Sealed {}
}

impl<T> private::Sealed for PostgresEventStore<T> {}
impl<T> private::Sealed for PostgresEventStoreTransaction<'_, T> {}

trait WithGenericClient<B>: private::Sealed
where
    B: Aggregate,
{
    fn snapshot_interval(&self) -> usize;
    fn get_client(&self) -> &impl tokio_postgres::GenericClient;
}

impl<T> WithGenericClient<T> for PostgresEventStore<T>
where
    T: Aggregate,
{
    fn snapshot_interval(&self) -> usize {
        self.snapshot_interval
    }
    fn get_client(&self) -> &impl tokio_postgres::GenericClient {
        self.client.as_ref()
    }
}

impl<'r, T> WithGenericClient<T> for PostgresEventStoreTransaction<'r, T>
where
    T: Aggregate,
{
    fn snapshot_interval(&self) -> usize {
        self.snapshot_interval
    }
    fn get_client(&self) -> &impl tokio_postgres::GenericClient {
        &self.transaction
    }
}

impl<T, U: WithGenericClient<T>> EventStore<T> for U
where
    T: Aggregate + Default + Serialize + DeserializeOwned,
    T::Event: Clone + Serialize + DeserializeOwned + Send + 'static,
{
    type StoreError = tokio_postgres::Error;
    type StreamError = Infallible;

    async fn try_get_events_since(
        &self,
        id: &str,
        version: usize,
    ) -> Result<super::Commit<Vec<<T as Aggregate>::Event>>> {
        let rows = self
            .get_client()
            .query(
                "SELECT version, action FROM events WHERE aggregate_id = $1 AND aggregate_type = $2 AND version > $3 ORDER BY version",
                &[&id, &T::name(), &(version as i32)],
            )
            .await?;

        let version = rows
            .last()
            .map(|row| row.get::<_, i32>(0) as usize)
            .unwrap_or(version);

        if version == 0 {
            return Err(Error::NotFound("No events found".to_string()));
        }

        let mut events: Vec<T::Event> = Vec::with_capacity(rows.len());
        for row in rows {
            let action: Value = row.get(1);
            events.push(serde_json::from_value(action)?);
        }

        Ok(super::Commit {
            id: id.to_string(),
            version,
            diagnostics: None,
            inner: events,
        })
    }

    async fn try_get_commit(&self, id: &str) -> Result<Commit<T>> {
        match self.load_snapshot(id).await? {
            Some(snapshot_commit) => {
                let events = self
                    .try_get_events_since(id, snapshot_commit.version)
                    .await?;
                Ok(Commit {
                    id: id.to_string(),
                    version: events.version,
                    diagnostics: Some(Diagnostics {
                        loaded_events: events.inner.len(),
                        snapshotted: true,
                    }),
                    inner: events.inner.iter().fold(snapshot_commit.inner, T::reduce),
                })
            }
            None => {
                let events = self.try_get_events(id).await?;
                Ok(Commit {
                    id: id.to_string(),
                    version: events.version,
                    diagnostics: Some(Diagnostics {
                        loaded_events: events.inner.len(),
                        snapshotted: false,
                    }),
                    inner: T::from_slice(&events.inner),
                })
            }
        }
    }

    async fn commit(&self, id: &str, version: usize, action: T::Event) -> Result<()> {
        let json_action: Value = serde_json::to_value(action.clone())?;
        let client = self.get_client();
        client
            .execute(
                "INSERT INTO events (aggregate_id, aggregate_type, version, action) VALUES ($1, $2, $3, $4)",
                &[&id, &T::name(), &(version as i32), &json_action],
            )
            .await
            .map_err(|e| {
                if e.code() == Some(&tokio_postgres::error::SqlState::UNIQUE_VIOLATION) {
                    Error::VersionConflict(version)
                } else {
                    panic!("Failed to insert event: {}", e)
                }
            })?;

        if version % self.snapshot_interval() == 0 {
            self.store_snapshot(id).await?;
        }

        self.transmit(
            id,
            Commit {
                id: id.to_string(),
                version,
                diagnostics: None,
                inner: action,
            },
        )
        .await?;
        eprintln!("Committed event for {}: version {}", id, version);

        Ok(())
    }

    async fn store_snapshot(&self, id: &str) -> Result<()> {
        if let Some(key) = T::snapshot_key() {
            let Commit {
                id: _,
                version,
                diagnostics: _,
                inner,
            } = self.try_get_commit(id).await?;
            let snapshot: Value = serde_json::to_value(inner)?;
            self.get_client()
                .execute(
                    r#"
                    INSERT INTO snapshots (aggregate_id, aggregate_type, key, version, snapshot)
                    VALUES ($1, $2, $3, $4, $5)
                    ON CONFLICT (aggregate_id, aggregate_type, key) DO UPDATE SET version = EXCLUDED.version, snapshot = EXCLUDED.snapshot
            "#,
                    &[&id, &T::name(), &key, &(version as i32), &snapshot],
                )
                .await?;
        }
        Ok(())
    }

    async fn load_snapshot(&self, id: &str) -> Result<Option<Commit<T>>> {
        if let Some(key) = T::snapshot_key() {
            let row = self
                .get_client()
                .query_opt(
                    "SELECT version, snapshot FROM snapshots WHERE aggregate_id = $1 AND aggregate_type = $2 AND key = $3",
                    &[&id, &T::name(), &key],
                )
                .await?;

            if let Some(row) = row {
                let version = row.get::<'_, _, i32>(0) as usize;
                let snapshot_col: Json<T> = row.get(1);
                let inner = snapshot_col.0;
                return Ok(Some(Commit {
                    id: id.to_string(),
                    version,
                    diagnostics: Some(Diagnostics {
                        loaded_events: 0,
                        snapshotted: true,
                    }),
                    inner,
                }));
            }
        }
        Ok(None)
    }

    async fn append(&self, id: &str, action: T::Event) -> Result<()> {
        let mut version = {
            match self.try_get_commit(id).await {
                Ok(commit) => commit.version + 1,
                Err(Error::NotFound(_)) => 1,
                Err(e) => return Err(e),
            }
        };
        let mut attempts_left = 3; // Limit the number of retries to avoid infinite loops
        loop {
            match self.commit(id, version, action.clone()).await {
                Ok(_) => return Ok(()),
                Err(Error::VersionConflict(previous_version)) => {
                    eprintln!(
                        "Version conflict detected, retrying with {}...",
                        previous_version
                    );
                    version = previous_version + 1;
                }
                Err(e) => return Err(e),
            }

            attempts_left -= 1;
            if attempts_left == 0 {
                return Err(Error::VersionConflict(version));
            }
        }
    }

    async fn transmit(&self, _id: &str, commit: Commit<<T as Aggregate>::Event>) -> Result<()> {
        let client = self.get_client();

        // Execute listen/notify
        client
            .execute(
                r#"
             SELECT pg_notify('event_notifications', $1)
             "#,
                &[&serde_json::to_string(&commit)?],
            )
            .await?;

        Ok(())
    }
}

// Utility struct to handle inaccessible clients
#[allow(dead_code)]
pub struct DeadClient(tokio_postgres::Client);

pub async fn stream<T, U, V>(
    client: tokio_postgres::Client,
    mut connection: tokio_postgres::Connection<U, V>,
) -> Result<(UnboundedReceiverStream<Commit<T::Event>>, DeadClient)>
where
    T: Aggregate,
    T::Event: Clone + Serialize + DeserializeOwned + Send + 'static,
    U: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    V: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let (tx, mut rx) = mpsc::unbounded();
    let stream =
        stream::poll_fn(move |cx| connection.poll_message(cx)).map_err(|e| panic!("{}", e));
    #[allow(clippy::unwrap_used)]
    let connection = stream.forward(tx).map(|r| r.unwrap());
    tokio::spawn(connection);

    #[allow(clippy::expect_used)]
    client.batch_execute("LISTEN event_notifications;").await?;

    let (tx2, rx2) = tokio::sync::mpsc::unbounded_channel::<Commit<T::Event>>();
    let mut max_version_seen = None;
    {
        let tx2 = tx2.clone();
        let rows = client
            .query(
                r#"SELECT aggregate_id, "version", action FROM events WHERE aggregate_type = $1 ORDER BY version"#,
                &[&T::name()],
            )
            .await?;

        for row in rows {
            let aggregate_id: String = row.get(0);
            let version: i32 = row.get(1);
            let action: Json<T::Event> = row.get(2);

            // Create a Commit from the row data
            let commit = Commit {
                id: aggregate_id,
                version: version as usize,
                diagnostics: None,
                inner: action.0,
            };

            // Update max_version_seen if this commit's version is greater
            match (version, max_version_seen) {
                (v, Some(max)) if v as usize > max => max_version_seen = Some(v as usize),
                (v, None) => max_version_seen = Some(v as usize),
                _ => {}
            };

            // Send the commit to the channel
            if tx2.send(commit).is_err() {
                eprintln!("Stream closed, cannot send event");
                break;
            }
        }
    }
    tokio::task::spawn(async move {
        loop {
            match rx.next().await {
                Some(AsyncMessage::Notification(notification)) => {
                    eprintln!("Received notification: {}", notification.payload());
                    #[allow(clippy::expect_used)]
                    let commit: Commit<T::Event> = serde_json::from_str(notification.payload())
                        .expect("Failed to parse notification payload");
                    if Some(commit.version) <= max_version_seen {
                        eprintln!(
                            "Ignoring commit with version {} as it is not newer than max seen {}",
                            commit.version,
                            max_version_seen.unwrap_or(0)
                        );
                        continue;
                    }
                    if tx2.send(commit).is_err() {
                        eprintln!("Stream closed, cannot send event");
                        break;
                    }
                }
                Some(_) => {
                    eprintln!("Received unexpected message, ignoring");
                }
                None => {
                    eprintln!("No more notifications, stopping listener");
                    break;
                }
            }
        }
    });

    // TODO: Return a struct here that owns client as a private field
    Ok((UnboundedReceiverStream::new(rx2), DeadClient(client)))
}

#[cfg(test)]
#[allow(clippy::expect_used)]
#[allow(clippy::unwrap_used)]
mod tests {
    use tokio_postgres::Socket;

    use super::*;
    use crate::tests::{Event, State};

    async fn get_connection_string() -> std::result::Result<String, tokio_postgres::Error> {
        let thread_id = format!("{:?}", std::thread::current().id())
            .replace("ThreadId(", "")
            .replace(")", "");
        let db_name = format!("espg_test_{}", thread_id);

        let connection_string = "postgres://theodorton@localhost:5432/postgres";
        let (client, connection) =
            tokio_postgres::connect(connection_string, tokio_postgres::NoTls).await?;

        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("Connection error: {}", e);
            }
        });

        let query = client
            .batch_execute(format!("CREATE DATABASE {};", db_name).as_str())
            .await;

        match query {
            Ok(_) => {}
            Err(e) => {
                if e.as_db_error().is_some_and(|db_error| {
                    db_error.code() == &tokio_postgres::error::SqlState::DUPLICATE_DATABASE
                }) {
                    eprintln!("Database {} already exists, using it.", db_name);
                } else {
                    return Err(e);
                }
            }
        }

        eprintln!("Using database: {}", db_name);
        let connection_string = format!("postgres://theodorton@localhost:5432/{}", db_name);

        Ok(connection_string)
    }

    async fn init_conn(
        spawn_conn: bool,
    ) -> std::result::Result<
        (
            tokio_postgres::Client,
            Option<tokio_postgres::Connection<Socket, tokio_postgres::tls::NoTlsStream>>,
        ),
        tokio_postgres::Error,
    > {
        let connection_string = get_connection_string().await?;
        let (client, connection) =
            tokio_postgres::connect(&connection_string, tokio_postgres::NoTls)
                .await
                .expect("Failed to connect to Postgres");
        let connection = if spawn_conn {
            tokio::spawn(async move {
                if let Err(e) = connection.await {
                    eprintln!("Connection error: {}", e);
                }
            });
            None
        } else {
            Some(connection)
        };
        Ok((client, connection))
    }

    async fn init_event_store(
        client: Arc<tokio_postgres::Client>,
    ) -> std::result::Result<PostgresEventStore<State>, tokio_postgres::Error> {
        let event_store = PostgresEventStore::<State>::new(client).await;
        event_store
            .initialize()
            .await
            .expect("Failed to initialize event store");

        event_store
            .clear()
            .await
            .expect("Failed to clear event store");

        Ok(event_store)
    }

    async fn init_event_stream() -> std::result::Result<
        (UnboundedReceiverStream<Commit<Event>>, DeadClient),
        tokio_postgres::Error,
    > {
        if let (client, Some(connection)) = init_conn(false).await? {
            Ok(
                super::stream::<State, Socket, tokio_postgres::tls::NoTlsStream>(
                    client, connection,
                )
                .await
                .expect("Failed to initialize event stream"),
            )
        } else {
            panic!("Failed to initialize Postgres connection");
        }
    }

    #[tokio::test]
    async fn test_postgres_event_store() -> Result<()> {
        let (client, _connection) = init_conn(true).await?;
        let client = Arc::new(client);
        let mut event_store = init_event_store(client).await?;
        let transaction: PostgresEventStoreTransaction<State> = event_store.begin().await?;

        transaction.append("test1", Event::Increment(10)).await?;
        transaction.append("test1", Event::Decrement(4)).await?;
        let aggregate = transaction
            .get_aggregate("test1")
            .await
            .expect("Aggregate not found");
        assert_eq!(aggregate.value, 6);
        transaction.append("test1", Event::Increment(3)).await?;
        let aggregate = transaction
            .get_aggregate("test1")
            .await
            .expect("Aggregate not found");
        assert_eq!(aggregate.value, 9);

        Ok(())
    }

    #[tokio::test]
    async fn test_get_missing_aggregate() -> Result<()> {
        let (client, _connection) = init_conn(true)
            .await
            .expect("msg: Failed to connect to Postgres");
        let client = Arc::new(client);
        let event_store = init_event_store(client)
            .await
            .expect("msg: Failed to create event store");

        let aggregate = event_store.get_aggregate("missing").await;
        assert!(aggregate.is_none(), "Expected None for missing aggregate");
        let events = event_store.try_get_events("missing").await;
        assert!(
            matches!(events, Err(crate::Error::NotFound(_))),
            "Expected NotFound error"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_commit_conflict() -> Result<()> {
        let (client, _connection) = init_conn(true)
            .await
            .expect("msg: Failed to connect to Postgres");
        let client = Arc::new(client);
        let event_store = init_event_store(client)
            .await
            .expect("msg: Failed to create event store");

        event_store.append("test2", Event::Increment(10)).await?;
        let commit = event_store
            .get_commit("test2")
            .await
            .expect("Commit not found");
        assert_eq!(commit.version, 1);
        assert_eq!(commit.inner.value, 10);

        // Attempting to commit with a version conflict
        let result = event_store.commit("test2", 1, Event::Decrement(5)).await;
        assert_eq!(Err(Error::VersionConflict(1)), result);

        Ok(())
    }

    #[tokio::test]
    async fn test_rollback() -> Result<()> {
        let (client, _connection) = init_conn(true)
            .await
            .expect("msg: Failed to connect to Postgres");
        let client = Arc::new(client);
        let mut event_store = init_event_store(client).await.unwrap();
        event_store.commit("test3", 1, Event::Increment(10)).await?;

        let commit = event_store.try_get_commit("test3").await?;
        assert_eq!(commit.version, 1);
        assert_eq!(commit.inner.value, 10);

        let transaction = event_store.begin().await?;
        transaction.append("test3", Event::Decrement(4)).await?;
        transaction.rollback().await?;

        let commit = event_store.try_get_commit("test3").await?;
        assert_eq!(commit.version, 1);
        assert_eq!(commit.inner.value, 10);

        Ok(())
    }

    #[tokio::test]
    async fn test_snapshot() -> Result<()> {
        let (client, _connection) = init_conn(true)
            .await
            .expect("msg: Failed to connect to Postgres");
        let client = Arc::new(client);
        let event_store = init_event_store(client)
            .await
            .expect("msg: Failed to create event store");

        for _ in 0..22 {
            event_store.append("test4", Event::Increment(1)).await?;
        }

        let snapshot = event_store.load_snapshot("test4").await?;
        assert!(snapshot.is_some(), "Snapshot should exist");
        let snapshot = snapshot.unwrap();
        let diag = snapshot.diagnostics.unwrap();
        assert!(diag.snapshotted, "Snapshot should be marked as snapshotted");
        assert_eq!(
            diag.loaded_events, 0,
            "No events should be loaded from snapshot"
        );
        assert_eq!(snapshot.inner.value, 20, "Snapshot value should be 20");

        let commit = event_store
            .get_commit("test4")
            .await
            .expect("Aggregate not found");
        let diag = commit.diagnostics.unwrap();
        assert!(diag.snapshotted, "Commit should be snapshotted");
        let aggregate = commit.inner;
        assert_eq!(aggregate.value, 22, "Aggregate value should be 22");
        assert_eq!(
            diag.loaded_events, 2,
            "Four events should be loaded from commit"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_inheriting_snapshot_interval() -> Result<()> {
        let (client, _connection) = init_conn(true)
            .await
            .expect("msg: Failed to connect to Postgres");
        let client = Arc::new(client);
        let mut event_store = init_event_store(client)
            .await
            .expect("msg: Failed to create event store");

        let parent_interval = event_store.snapshot_interval();
        let transaction = event_store.begin().await?;
        assert_eq!(
            parent_interval,
            transaction.snapshot_interval(),
            "Snapshot intervals should match"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_streaming() -> Result<()> {
        let (client, _connection) = init_conn(true)
            .await
            .expect("msg: Failed to connect to Postgres");
        let client = Arc::new(client);
        let event_store = init_event_store(client)
            .await
            .expect("msg: Failed to create event store");
        // Implement Stream as a custom struct with `next().await` method
        // Drop the client when the last event is received
        let stream = init_event_stream()
            .await
            .expect("msg: Failed to create event stream");

        let events_to_send = vec![
            Event::Increment(10),
            Event::Decrement(5),
            Event::Increment(5),
            Event::Decrement(2),
        ];

        let events_to_receive = events_to_send.clone();
        for event in events_to_send {
            event_store.append("test5", event).await?;
        }

        let handle = tokio::spawn(async move {
            let _client = stream.1;
            let mut stream = stream.0.take(4);
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
    async fn test_streaming_after_writes() -> Result<()> {
        let (client, _connection) = init_conn(true)
            .await
            .expect("msg: Failed to connect to Postgres");
        let client = Arc::new(client);
        let event_store = init_event_store(client)
            .await
            .expect("msg: Failed to create event store");

        let events_to_send = vec![
            Event::Increment(10),
            Event::Decrement(5),
            Event::Increment(5),
            Event::Decrement(2),
        ];

        let events_to_receive = events_to_send.clone();
        for event in events_to_send {
            event_store.append("test5", event).await?;
        }

        let stream = init_event_stream()
            .await
            .expect("msg: Failed to create event stream");
        let handle = tokio::spawn(async move {
            let _client = stream.1;
            let mut stream = stream.0.take(4);
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
    #[should_panic(expected = "Client must be unique to begin a transaction")]
    async fn test_panic_on_multiple_transactions() {
        let (client, _connection) = init_conn(true)
            .await
            .expect("msg: Failed to connect to Postgres");
        let client = Arc::new(client);
        let mut event_store_a = init_event_store(client.clone())
            .await
            .expect("msg: Failed to create event store a");
        let mut event_store_b = init_event_store(client)
            .await
            .expect("msg: Failed to create event store b");

        let _ = event_store_a
            .begin()
            .await
            .expect("Failed to begin transaction A");
        let _ = event_store_b.begin().await;
    }
}
