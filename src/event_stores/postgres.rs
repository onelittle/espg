use super::{Error, Result};
use crate::{
    Aggregate, EventStore,
    event_stores::{Commit, Diagnostics},
};
#[cfg(feature = "streaming")]
use crate::{EventStream, StreamingEventStore};
#[cfg(feature = "streaming")]
use futures_channel::mpsc;
use serde::{Serialize, de::DeserializeOwned};
use serde_json::Value;
use tokio_postgres::{GenericClient, types::Json};

pub struct PostgresEventStore<'a, T, Db: GenericClient> {
    client: &'a Db,
    aggregate_type: std::marker::PhantomData<T>,
    snapshot_interval: usize,
}

impl<'a, T, Db> PostgresEventStore<'a, T, Db>
where
    T: Aggregate,
    Db: GenericClient,
{
    pub fn new(client: &'a Db) -> PostgresEventStore<'a, T, Db> {
        PostgresEventStore {
            client,
            aggregate_type: std::marker::PhantomData,
            snapshot_interval: 10,
        }
    }
}

#[cfg(feature = "deadpool")]
impl<'a, 'b, T> From<&'a deadpool_postgres::ClientWrapper>
    for PostgresEventStore<'b, T, tokio_postgres::Client>
where
    'a: 'b,
    T: Aggregate,
{
    fn from(client: &'a deadpool_postgres::ClientWrapper) -> Self {
        let client = &**client;
        PostgresEventStore::new(client)
    }
}

pub async fn initialize(
    client: &tokio_postgres::Client,
) -> std::result::Result<(), tokio_postgres::Error> {
    // Create the necessary tables if they do not exist
    client
        .batch_execute(super::sql_helpers::CREATE_EVENTS)
        .await?;

    client
        .batch_execute(super::sql_helpers::CREATE_SNAPSHOTS)
        .await?;

    Ok(())
}

pub async fn clear(
    client: &tokio_postgres::Client,
) -> std::result::Result<(), tokio_postgres::Error> {
    client
        .batch_execute(
            r#"
            DELETE FROM events;
            DELETE FROM snapshots;
        "#,
        )
        .await?;
    Ok(())
}

impl<T, Db: GenericClient> EventStore<T> for PostgresEventStore<'_, T, Db>
where
    T: Aggregate + Default + Serialize + DeserializeOwned,
    T::Event: Clone + Serialize + DeserializeOwned + Send + 'static,
{
    async fn try_get_events_since(
        &self,
        id: &str,
        version: usize,
    ) -> Result<super::Commit<Vec<<T as Aggregate>::Event>>> {
        let rows = self
            .client
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
        self.client
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

        if version % self.snapshot_interval == 0 {
            self.store_snapshot(id).await?;
        }

        #[cfg(feature = "streaming")]
        self.transmit(Commit {
            id: id.to_string(),
            version,
            diagnostics: None,
            inner: action,
        })
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
            self.client
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
                .client
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

    #[cfg(feature = "streaming")]
    async fn transmit(&self, commit: Commit<<T as Aggregate>::Event>) -> Result<()> {
        // Execute listen/notify
        self.client
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

#[cfg(feature = "streaming")]
pub struct PostgresEventStream<T: Aggregate> {
    client: tokio_postgres::Client,
    connection:
        tokio_postgres::Connection<tokio_postgres::Socket, tokio_postgres::tls::NoTlsStream>,
    marker: std::marker::PhantomData<T>,
}

#[cfg(feature = "streaming")]
impl<T: Aggregate> PostgresEventStream<T>
where
    T::Event: DeserializeOwned + Send + 'static,
{
    pub async fn new(
        client: tokio_postgres::Client,
        connection: tokio_postgres::Connection<
            tokio_postgres::Socket,
            tokio_postgres::tls::NoTlsStream,
        >,
    ) -> Self {
        PostgresEventStream {
            client,
            connection,
            marker: std::marker::PhantomData,
        }
    }

    async fn listen(
        client: &tokio_postgres::Client,
        mut connection: tokio_postgres::Connection<
            tokio_postgres::Socket,
            tokio_postgres::tls::NoTlsStream,
        >,
    ) -> Result<tokio::sync::mpsc::UnboundedReceiver<Commit<T::Event>>> {
        use futures::FutureExt;
        use futures::StreamExt;
        use futures::TryStreamExt;

        let (tx, mut rx) = mpsc::unbounded();
        let stream = futures::stream::poll_fn(move |cx| connection.poll_message(cx))
            .map_err(|e| panic!("{}", e));
        #[allow(clippy::unwrap_used)]
        let connection = stream.forward(tx).map(|r| r.unwrap());
        tokio::spawn(connection);

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
            use futures::stream::StreamExt;
            loop {
                match rx.next().await {
                    Some(tokio_postgres::AsyncMessage::Notification(notification)) => {
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

        Ok(rx2)
    }
}

#[cfg(feature = "streaming")]
impl<'a, T: Aggregate + Default + Serialize + DeserializeOwned> StreamingEventStore<'a, T>
    for PostgresEventStream<T>
where
    T::Event: Clone + Serialize + DeserializeOwned + Send + 'static,
{
    type StreamType =
        EventStream<tokio::sync::mpsc::UnboundedReceiver<Commit<T::Event>>, tokio_postgres::Client>;

    async fn stream(self) -> Result<Self::StreamType> {
        let client = self.client;
        let rx2 = PostgresEventStream::<T>::listen(&client, self.connection).await?;
        #[allow(clippy::expect_used)]
        client.batch_execute("LISTEN event_notifications;").await?;

        Ok(EventStream::new(rx2, client))
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
#[allow(clippy::unwrap_used)]
mod tests {
    use tokio_postgres::{Socket, Transaction};
    #[cfg(feature = "streaming")]
    use tokio_stream::StreamExt;

    use super::*;
    use crate::tests::{Event, State};

    async fn get_connection_string(
        db_name: Option<&str>,
    ) -> std::result::Result<String, tokio_postgres::Error> {
        let thread_id = format!("{:?}", std::thread::current().id())
            .replace("ThreadId(", "")
            .replace(")", "");
        let db_name = db_name
            .map(|n| n.to_string())
            .unwrap_or_else(|| format!("espg_test_{}", thread_id));

        eprintln!("Using database name: {}", db_name);

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
        db_name: Option<&str>,
        spawn_conn: bool,
    ) -> std::result::Result<
        (
            tokio_postgres::Client,
            Option<tokio_postgres::Connection<Socket, tokio_postgres::tls::NoTlsStream>>,
        ),
        tokio_postgres::Error,
    > {
        let connection_string = get_connection_string(db_name).await?;
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

    async fn init_event_store<'a, 'b>(
        client: &'a tokio_postgres::Client,
    ) -> std::result::Result<
        PostgresEventStore<'b, State, tokio_postgres::Client>,
        tokio_postgres::Error,
    >
    where
        'a: 'b,
    {
        let event_store = PostgresEventStore::new(client);
        super::initialize(client)
            .await
            .expect("Failed to initialize event store");

        super::clear(client)
            .await
            .expect("Failed to clear event store");

        Ok(event_store)
    }

    #[cfg(feature = "streaming")]
    async fn init_event_stream(
        db_name: Option<&str>,
    ) -> std::result::Result<
        EventStream<tokio::sync::mpsc::UnboundedReceiver<Commit<Event>>, tokio_postgres::Client>,
        tokio_postgres::Error,
    > {
        if let (client, Some(connection)) = init_conn(db_name, false).await? {
            let es = PostgresEventStream::<State>::new(client, connection).await;
            Ok(es.stream().await.expect("Failed to create event stream"))
        } else {
            panic!("Failed to initialize Postgres connection");
        }
    }

    #[tokio::test]
    async fn test_postgres_event_store() -> Result<()> {
        let test_db = crate::test_helper::get_test_database().await;
        let (mut client, _connection) = init_conn(Some(&test_db.name), true).await?;
        super::initialize(&client).await?;
        super::clear(&client).await?;

        let db_transaction = client.transaction().await?;
        let transaction = PostgresEventStore::new(&db_transaction);

        transaction.append("test1", Event::Increment(10)).await?;
        transaction.append("test1", Event::Decrement(4)).await?;
        let aggregate: State = transaction
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
        let test_db = crate::test_helper::get_test_database().await;
        let (client, _connection) = init_conn(Some(&test_db.name), true).await?;
        let event_store = init_event_store(&client)
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
        let test_db = crate::test_helper::get_test_database().await;
        let (client, _connection) = init_conn(Some(&test_db.name), true).await?;
        let event_store = init_event_store(&client)
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
        let test_db = crate::test_helper::get_test_database().await;
        let (mut client, _connection) = init_conn(Some(&test_db.name), true).await?;

        let event_store = init_event_store(&client).await.unwrap();
        event_store.commit("test3", 1, Event::Increment(10)).await?;

        let commit = event_store.try_get_commit("test3").await?;
        assert_eq!(commit.version, 1);
        assert_eq!(commit.inner.value, 10);

        let db_transaction = client.transaction().await?;
        let transaction: PostgresEventStore<State, Transaction> =
            PostgresEventStore::new(&db_transaction);
        transaction.append("test3", Event::Decrement(4)).await?;
        db_transaction.rollback().await?;

        let event_store = PostgresEventStore::new(&client);
        let commit: Commit<State> = event_store.try_get_commit("test3").await?;
        assert_eq!(commit.version, 1);
        assert_eq!(commit.inner.value, 10);

        Ok(())
    }

    #[tokio::test]
    async fn test_snapshot() -> Result<()> {
        let test_db = crate::test_helper::get_test_database().await;
        let (client, _connection) = init_conn(Some(&test_db.name), true).await?;

        let event_store = init_event_store(&client)
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
    #[cfg(feature = "streaming")]
    async fn test_streaming() -> Result<()> {
        let test_db = crate::test_helper::get_test_database().await;
        let (client, _connection) = init_conn(Some(&test_db.name), true).await?;

        let event_store = init_event_store(&client)
            .await
            .expect("msg: Failed to create event store");
        // Implement Stream as a custom struct with `next().await` method
        // Drop the client when the last event is received
        let stream = init_event_stream(Some(&test_db.name))
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
    async fn test_streaming_after_writes() -> Result<()> {
        let test_db = crate::test_helper::get_test_database().await;
        let (client, _connection) = init_conn(Some(&test_db.name), true).await?;

        let event_store = init_event_store(&client)
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

        let stream = init_event_stream(Some(&test_db.name))
            .await
            .expect("msg: Failed to create event stream");
        let handle = tokio::spawn(async move {
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
    async fn test_deadpool_integration() -> Result<()> {
        use deadpool_postgres::{Config, ManagerConfig, RecyclingMethod, Runtime};
        use tokio_postgres::NoTls;

        let mut cfg = Config::new();
        cfg.url = Some(
            get_connection_string(None)
                .await
                .expect("Failed to get connection string"),
        );
        cfg.manager = Some(ManagerConfig {
            recycling_method: RecyclingMethod::Fast,
        });
        let pool = cfg.create_pool(Some(Runtime::Tokio1), NoTls).unwrap();

        let client = pool.get().await.expect("Failed to get client from pool");
        let event_store = init_event_store(&client).await?;

        event_store.append("test6", Event::Increment(10)).await?;
        event_store.append("test6", Event::Decrement(5)).await?;

        let aggregate = event_store.get_aggregate("test6").await;
        assert!(aggregate.is_some(), "Expected aggregate to be found");
        assert_eq!(aggregate.unwrap().value, 5);

        Ok(())
    }
}
