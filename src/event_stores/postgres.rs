use super::{Error, Result};
use crate::{
    Aggregate, EventStore, Id,
    event_stores::{Commit, Diagnostics},
    util::Txid,
};
#[cfg(feature = "streaming")]
use crate::{EventStream, StreamingEventStore, event_stores::StreamItem};
use async_trait::async_trait;
#[cfg(feature = "streaming")]
use futures::Stream;
#[cfg(feature = "streaming")]
use futures_channel::mpsc;
use serde_json::Value;
use tokio_postgres::{GenericClient, types::Json};

pub struct PostgresEventStore<'a, Db> {
    pub(crate) client: &'a Db,
    snapshot_interval: usize,
}

impl<Db> PostgresEventStore<'_, Db>
where
    Db: GenericClient,
{
    pub fn new<'a>(client: &'a Db) -> PostgresEventStore<'a, Db> {
        PostgresEventStore {
            client,
            snapshot_interval: 10,
        }
    }

    #[allow(clippy::expect_used)]
    pub async fn len(&self) -> usize {
        let row = self
            .client
            .query_one("SELECT COUNT(*) FROM events", &[])
            .await
            .expect("Failed to count events");
        row.get::<_, i64>(0) as usize
    }

    #[allow(clippy::expect_used)]
    pub async fn is_empty(&self) -> bool {
        let row = self
            .client
            .query_one("SELECT * FROM events LIMIT 1", &[])
            .await
            .expect("Failed to count events");
        row.is_empty()
    }
}

#[mutants::skip]
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

    client
        .batch_execute(super::sql_helpers::CREATE_SUBSCRIPTIONS)
        .await?;

    client
        .batch_execute(super::sql_helpers::CREATE_TRIGGER)
        .await?;

    Ok(())
}

#[mutants::skip]
pub async fn clear(
    client: &tokio_postgres::Client,
) -> std::result::Result<(), tokio_postgres::Error> {
    client
        .batch_execute(
            r#"
            DELETE FROM events;
            DELETE FROM snapshots;
            DELETE FROM subscriptions;
        "#,
        )
        .await?;
    Ok(())
}

#[mutants::skip]
pub async fn destroy(
    client: &tokio_postgres::Client,
) -> std::result::Result<(), tokio_postgres::Error> {
    client
        .batch_execute(
            r#"
            DROP TABLE IF EXISTS events;
            DROP TABLE IF EXISTS snapshots;
            DROP TABLE IF EXISTS subscriptions;
        "#,
        )
        .await?;
    Ok(())
}

#[async_trait]
impl<Db: GenericClient + Sync> EventStore for PostgresEventStore<'_, Db> {
    async fn try_get_events_since<X: Aggregate>(
        &self,
        id: &Id<X>,
        version: usize,
    ) -> Result<super::Commit<Vec<X::Event>>> {
        let rows = self
            .client
            .query(
                "SELECT version, action, global_seq::text FROM events WHERE aggregate_id = $1 AND aggregate_type = $2 AND version > $3 ORDER BY version",
                &[&id.0, &X::NAME, &(version as i32)],
            )
            .await?;

        let (version, global_seq) = rows
            .last()
            .map(|row| {
                let version: i32 = row.get(0);
                let global_seq: String = row.get(2);
                let global_seq: Txid = global_seq.into();
                (version as usize, Some(global_seq))
            })
            .unwrap_or((version, None));

        if version == 0 {
            return Err(Error::NotFound("No events found".to_string()));
        }

        let mut events: Vec<X::Event> = Vec::with_capacity(rows.len());
        for row in rows {
            let action: Value = row.get(1);
            events.push(serde_json::from_value(action)?);
        }

        Ok(super::Commit {
            id: id.to_string(),
            version,
            diagnostics: None,
            inner: events,
            global_seq,
        })
    }

    async fn try_get_commit<X: Aggregate>(&self, id: &Id<X>) -> Result<Commit<X>> {
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
                    global_seq: None,
                    inner: events.inner.iter().fold(snapshot_commit.inner, X::reduce),
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
                    global_seq: None,
                    inner: X::from_slice(&events.inner),
                })
            }
        }
    }

    async fn commit<X: Aggregate>(
        &self,
        id: &Id<X>,
        version: usize,
        action: X::Event,
    ) -> Result<()> {
        let json_action: Value = serde_json::to_value(action.clone())?;
        self.client
            .execute(
                "INSERT INTO events (aggregate_id, aggregate_type, version, action) VALUES ($1, $2, $3, $4)",
                &[&id.0, &X::NAME, &(version as i32), &json_action],
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
            self.store_snapshot::<X>(id).await?;
        }

        Ok(())
    }

    async fn store_snapshot<X: Aggregate>(&self, id: &Id<X>) -> Result<()> {
        let Some(key) = X::snapshot_key() else {
            return Ok(());
        };
        let Commit {
            id: _,
            version,
            diagnostics: _,
            inner,
            global_seq: _,
        } = self.try_get_commit(id).await?;
        let snapshot: Value = serde_json::to_value(inner)?;
        self.client
            .execute(
                r#"
                INSERT INTO snapshots (aggregate_id, aggregate_type, key, version, snapshot)
                VALUES ($1, $2, $3, $4, $5)
                ON CONFLICT (aggregate_id, aggregate_type, key) DO UPDATE SET version = EXCLUDED.version, snapshot = EXCLUDED.snapshot
        "#,
                &[&id.0, &X::NAME, &key, &(version as i32), &snapshot],
            )
            .await?;
        Ok(())
    }

    async fn load_snapshot<X: Aggregate>(&self, id: &Id<X>) -> Result<Option<Commit<X>>> {
        let Some(key) = X::snapshot_key() else {
            return Ok(None);
        };
        let row = self
                .client
                .query_opt(
                    "SELECT version, snapshot FROM snapshots WHERE aggregate_id = $1 AND aggregate_type = $2 AND key = $3",
                    &[&id.0, &X::NAME, &key],
                )
                .await?;

        let Some(row) = row else { return Ok(None) };
        let version = row.get::<'_, _, i32>(0) as usize;
        let snapshot_col: Json<X> = row.get(1);
        let inner = snapshot_col.0;
        Ok(Some(Commit {
            id: id.to_string(),
            version,
            diagnostics: Some(Diagnostics {
                loaded_events: 0,
                snapshotted: true,
            }),
            global_seq: None,
            inner,
        }))
    }

    async fn append<X: Aggregate>(&self, id: &Id<X>, action: X::Event) -> Result<()> {
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
}

#[cfg(feature = "streaming")]
pub struct PostgresEventStream {
    client: tokio_postgres::Client,
    connection:
        tokio_postgres::Connection<tokio_postgres::Socket, tokio_postgres::tls::NoTlsStream>,
}

#[cfg(feature = "streaming")]
impl PostgresEventStream {
    pub async fn new(
        config: tokio_postgres::Config,
    ) -> std::result::Result<Self, tokio_postgres::Error> {
        let (client, connection) = config.connect(tokio_postgres::NoTls).await?;
        Ok(PostgresEventStream { client, connection })
    }

    #[mutants::skip]
    async fn listen<T: Aggregate>(
        client: tokio_postgres::Client,
        mut connection: tokio_postgres::Connection<
            tokio_postgres::Socket,
            tokio_postgres::tls::NoTlsStream,
        >,
    ) -> tokio::sync::mpsc::UnboundedReceiver<StreamItem<T::Event>> {
        use std::collections::HashMap;

        use futures::FutureExt;
        use futures::StreamExt;
        use futures::TryStreamExt;
        use md5::Digest;
        use md5::Md5;

        let (tx, mut rx) = mpsc::unbounded();
        let stream = futures::stream::poll_fn(move |cx| connection.poll_message(cx))
            .map_err(|e| panic!("{}", e));
        #[allow(clippy::unwrap_used)]
        let connection = stream.forward(tx).map(|r| r.unwrap());
        tokio::spawn(connection);

        let (tx2, rx2) = tokio::sync::mpsc::unbounded_channel();
        let mut max_versions_seen: HashMap<String, i32> = Default::default();
        {
            let tx2 = tx2.clone();
            #[allow(clippy::expect_used)]
            let rows = client
            .query(
                r#"SELECT aggregate_id, "version", action, global_seq FROM events WHERE aggregate_type = $1"#,
                &[&T::NAME],
            )
            .await.expect("Failed to query events");

            for row in rows {
                let aggregate_id: String = row.get(0);
                let version: i32 = row.get(1);
                let action: Json<T::Event> = row.get(2);
                let global_seq: String = row.get(3);
                let global_seq = global_seq.into();

                // Create a Commit from the row data
                let commit = Commit {
                    id: aggregate_id,
                    version: version as usize,
                    diagnostics: None,
                    inner: action.0,
                    global_seq: Some(global_seq),
                };

                // Update max_version_seen if this commit's version is greater
                max_versions_seen
                    .entry(commit.id.clone())
                    .and_modify(|v| *v = (*v).max(version))
                    .or_insert(version);

                // Send the commit to the channel
                if tx2.send(Ok(commit)).is_err() {
                    eprintln!("Stream closed, cannot send event");
                    break;
                }
            }
        }
        let mut hasher = Md5::new();
        hasher.update(format!("events:{}", T::NAME).as_bytes());
        let channel = format!("{:x}", hasher.finalize());

        eprintln!("Listening for notifications on channel: {}", channel);

        #[allow(clippy::expect_used)]
        client
            .batch_execute(&format!(r#"LISTEN "{channel}";"#))
            .await
            .expect("Failed to listen for notifications");

        tokio::task::spawn(async move {
            use futures::stream::StreamExt;
            loop {
                match rx.next().await {
                    Some(tokio_postgres::AsyncMessage::Notification(notification)) => {
                        let aggregate_id = T::id(notification.payload());

                        let max_version =
                            max_versions_seen.get(&aggregate_id.0).cloned().unwrap_or(0);

                        let rows = client
                        .query(
                            r#"SELECT aggregate_id, "version", action, global_seq FROM events WHERE aggregate_type = $1 AND version > $2 AND aggregate_id = $3 ORDER BY version"#,
                            &[&T::NAME, &max_version, &aggregate_id.0],
                        )
                        .await;

                        let rows = match rows {
                            Ok(rows) => rows,
                            Err(e) => {
                                use crate::event_stores::StreamItemError;

                                eprintln!("Error querying events: {}", e);
                                if tx2.send(Err(StreamItemError::from(e))).is_err() {
                                    eprintln!("Stream closed, cannot send error");
                                }
                                break;
                            }
                        };

                        for row in rows {
                            let aggregate_id: String = row.get(0);
                            let version: i32 = row.get(1);
                            let action: Json<T::Event> = row.get(2);
                            let global_seq: String = row.get(3);
                            let global_seq = global_seq.into();

                            // Create a Commit from the row data
                            let commit = Commit {
                                id: aggregate_id,
                                version: version as usize,
                                diagnostics: None,
                                inner: action.0,
                                global_seq: Some(global_seq),
                            };

                            // Update max_version_seen if this commit's version is greater
                            max_versions_seen
                                .entry(commit.id.clone())
                                .and_modify(|v| *v = (*v).max(version))
                                .or_insert(version);

                            // Send the commit to the channel
                            if tx2.send(Ok(commit)).is_err() {
                                eprintln!("Stream closed, cannot send event");
                                break;
                            }
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

        rx2
    }
}

#[cfg(feature = "streaming")]
impl StreamingEventStore for PostgresEventStream {
    async fn stream<X: Aggregate>(self) -> Result<impl Stream<Item = StreamItem<X::Event>>> {
        let client = self.client;
        let rx2 = PostgresEventStream::listen::<X>(client, self.connection).await;
        Ok(EventStream::new(rx2, ()))
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
#[allow(clippy::unwrap_used)]
mod tests {
    #[cfg(feature = "streaming")]
    use tokio_stream::StreamExt;

    use super::*;
    use crate::tests::{Event, State};

    async fn event_store<'a, 'b>(
        client: &'a tokio_postgres::Client,
    ) -> std::result::Result<PostgresEventStore<'b, tokio_postgres::Client>, tokio_postgres::Error>
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

    #[tokio::test]
    async fn test_postgres_event_store() -> Result<()> {
        let test_db = crate::test_helper::get_test_database().await;
        let mut client = test_db.client().await;
        super::initialize(&client).await?;
        super::clear(&client).await?;

        let db_transaction = client.transaction().await?;
        let transaction = PostgresEventStore::new(&db_transaction);

        let id = State::id("test1");
        transaction.append(&id, Event::Increment(10)).await?;
        transaction.append(&id, Event::Decrement(4)).await?;
        let aggregate: State = transaction
            .get_aggregate(&id)
            .await
            .expect("Aggregate not found");
        assert_eq!(aggregate.value, 6);
        transaction.append(&id, Event::Increment(3)).await?;
        let aggregate = transaction
            .get_aggregate(&id)
            .await
            .expect("Aggregate not found");
        assert_eq!(aggregate.value, 9);

        Ok(())
    }

    #[tokio::test]
    async fn test_get_missing_aggregate() -> Result<()> {
        let test_db = crate::test_helper::get_test_database().await;
        let client = test_db.client().await;
        let event_store = event_store(&client)
            .await
            .expect("msg: Failed to create event store");

        let id = State::id("missing");
        let aggregate = event_store.get_aggregate(&id).await;
        assert!(aggregate.is_none(), "Expected None for missing aggregate");
        let events = event_store.try_get_events(&id).await;
        assert!(
            matches!(events, Err(crate::Error::NotFound(_))),
            "Expected NotFound error"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_commit_conflict() -> Result<()> {
        let test_db = crate::test_helper::get_test_database().await;
        let client = test_db.client().await;
        let event_store = event_store(&client)
            .await
            .expect("msg: Failed to create event store");

        let id = State::id("test2");
        event_store.append(&id, Event::Increment(10)).await?;
        let commit = event_store.get_commit(&id).await.expect("Commit not found");
        assert_eq!(commit.version, 1);
        assert_eq!(commit.inner.value, 10);

        // Attempting to commit with a version conflict
        let result = event_store.commit(&id, 1, Event::Decrement(5)).await;
        assert_eq!(Err(Error::VersionConflict(1)), result);

        Ok(())
    }

    #[tokio::test]
    async fn test_rollback() -> Result<()> {
        let test_db = crate::test_helper::get_test_database().await;
        let mut client = test_db.client().await;

        let id = State::id("test3");
        let event_store = event_store(&client).await.unwrap();
        event_store.commit(&id, 1, Event::Increment(10)).await?;

        let commit = event_store.try_get_commit(&id).await?;
        assert_eq!(commit.version, 1);
        assert_eq!(commit.inner.value, 10);

        let db_transaction = client.transaction().await?;
        let transaction = PostgresEventStore::new(&db_transaction);
        transaction.append(&id, Event::Decrement(4)).await?;
        db_transaction.rollback().await?;

        let event_store = PostgresEventStore::new(&client);
        let commit: Commit<State> = event_store.try_get_commit(&id).await?;
        assert_eq!(commit.version, 1);
        assert_eq!(commit.inner.value, 10);

        Ok(())
    }

    #[tokio::test]
    async fn test_snapshot() -> Result<()> {
        let test_db = crate::test_helper::get_test_database().await;
        let client = test_db.client().await;

        let event_store = event_store(&client)
            .await
            .expect("msg: Failed to create event store");

        let id = State::id("test4");
        for _ in 0..22 {
            event_store.append(&id, Event::Increment(1)).await?;
        }

        let snapshot = event_store.load_snapshot(&id).await?;
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
            .get_commit(&id)
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
        let client = test_db.client().await;
        super::initialize(&client).await?;
        super::clear(&client).await?;

        let event_store = PostgresEventStore::new(&client);
        // Implement Stream as a custom struct with `next().await` method
        // Drop the client when the last event is received
        let stream = PostgresEventStream::new(test_db.tokio_postgres_config().await)
            .await?
            .stream::<State>()
            .await?;

        let events_to_send = vec![
            Event::Increment(10),
            Event::Decrement(5),
            Event::Increment(5),
            Event::Decrement(2),
        ];

        let id = State::id("test5");
        let events_to_receive = events_to_send.clone();
        for event in events_to_send {
            event_store.append(&id, event).await?;
        }

        let handle = tokio::spawn(async move {
            let mut stream = stream.take(4);
            let mut n = 0;
            let mut iter = events_to_receive.iter();
            while let Some(commit) = stream.next().await {
                let commit = commit.expect("Stream returned an error");

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
        let client = test_db.client().await;
        super::initialize(&client).await?;
        super::clear(&client).await?;

        let event_store = PostgresEventStore::new(&client);

        let events_to_send = vec![
            Event::Increment(10),
            Event::Decrement(5),
            Event::Increment(5),
            Event::Decrement(2),
        ];

        let id = State::id("test5");
        let events_to_receive = events_to_send.clone();
        for event in events_to_send {
            event_store.append(&id, event).await?;
        }

        let stream = PostgresEventStream::new(test_db.tokio_postgres_config().await)
            .await?
            .stream::<State>()
            .await?;
        let handle = tokio::spawn(async move {
            let mut stream = stream.take(4);
            let mut n = 0;
            let mut iter = events_to_receive.iter();
            while let Some(commit) = stream.next().await {
                let commit = commit.expect("Stream returned an error");

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

        let test_db = crate::test_helper::get_test_database().await;
        let mut cfg = Config::new();
        cfg.url = Some(test_db.connection_string());
        cfg.manager = Some(ManagerConfig {
            recycling_method: RecyclingMethod::Fast,
        });
        let pool = cfg.create_pool(Some(Runtime::Tokio1), NoTls).unwrap();

        let client = pool.get().await.expect("Failed to get client from pool");
        let event_store = event_store(&client).await?;

        let id = State::id("test6");
        event_store.append(&id, Event::Increment(10)).await?;
        event_store.append(&id, Event::Decrement(5)).await?;

        let aggregate = event_store.get_aggregate(&id).await;
        assert!(aggregate.is_some(), "Expected aggregate to be found");
        assert_eq!(aggregate.unwrap().value, 5);

        Ok(())
    }

    #[tokio::test]
    async fn test_not_found() -> Result<()> {
        let test_db = crate::test_helper::get_test_database().await;
        let client = test_db.client().await;
        let event_store = event_store(&client)
            .await
            .expect("msg: Failed to create event store");

        let id = State::id("test8");
        let result = event_store.try_get_commit(&id).await;
        assert!(
            matches!(result, Err(Error::NotFound(_))),
            "Expected NotFound error"
        );

        let result = event_store.get_commit(&id).await;
        assert!(result.is_none(), "Expected None for non-existent commit");

        Ok(())
    }

    #[tokio::test]
    async fn test_commands() -> Result<()> {
        let test_db = crate::test_helper::get_test_database().await;
        let client = test_db.client().await;
        let event_store = event_store(&client)
            .await
            .expect("msg: Failed to create event store");

        let id = State::id("test7");
        event_store.commit(&id, 1, Event::Increment(0)).await?;
        for _ in 0..10 {
            crate::tests::commands::increment(&event_store, &id, 1).await?;
        }
        for _ in 0..5 {
            crate::tests::commands::decrement(&event_store, &id, 1).await?;
        }

        assert_eq!(
            event_store.get_aggregate(&id).await.unwrap().value,
            5,
            "Expected final value to be 5"
        );

        Ok(())
    }
}
