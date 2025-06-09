use serde::{Serialize, de::DeserializeOwned};
use serde_json::Value;
use tokio_postgres::{GenericClient, Transaction, types::Json};

use super::{Error, Result};
use crate::{
    Aggregate, EventStore,
    event_stores::{Commit, Diagnostics},
};

pub struct PostgresEventStore<T> {
    client: tokio_postgres::Client,
    aggregate_type: std::marker::PhantomData<T>,
    snapshot_interval: usize,
}

impl<T> PostgresEventStore<T>
where
    T: Aggregate,
{
    pub async fn new(client: tokio_postgres::Client) -> Self {
        PostgresEventStore {
            client,
            aggregate_type: std::marker::PhantomData,
            snapshot_interval: 10,
        }
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

    pub async fn begin<'b>(&'b mut self) -> Result<PostgresEventStoreTransaction<'b, T>> {
        let transaction: Transaction<'b> = self.client.transaction().await?;
        Ok(PostgresEventStoreTransaction::new(transaction, self.snapshot_interval).await)
    }

    pub fn client(self) -> tokio_postgres::Client {
        self.client
    }
}

pub struct PostgresEventStoreTransaction<'r, T> {
    transaction: tokio_postgres::Transaction<'r>,
    snapshot_interval: usize,
    r#type: std::marker::PhantomData<T>,
}

impl<'r, T> PostgresEventStoreTransaction<'r, T>
where
    T: Aggregate,
{
    pub async fn complete(self) -> Result<()> {
        self.transaction
            .commit()
            .await
            .map_err(|e| {
                if e.code() == Some(&tokio_postgres::error::SqlState::UNIQUE_VIOLATION) {
                    todo!("Handle potential multiple version conflicts in transactions");
                } else {
                    panic!("Failed to commit transaction: {}", e)
                }
            })
            .expect("Failed to commit transaction");
        Ok(())
    }
}

impl<'r, T> PostgresEventStoreTransaction<'r, T>
where
    T: Aggregate,
{
    async fn new(transaction: tokio_postgres::Transaction<'r>, snapshot_interval: usize) -> Self {
        PostgresEventStoreTransaction {
            r#type: std::marker::PhantomData,
            transaction,
            snapshot_interval,
        }
    }

    pub async fn rollback(self) -> Result<()> {
        self.transaction.rollback().await?;
        Ok(())
    }
}

trait GenericPostgresEventStore {}
impl<T> GenericPostgresEventStore for PostgresEventStore<T> {}
impl<T> GenericPostgresEventStore for PostgresEventStoreTransaction<'_, T> {}

trait WithGenericClient<B>
where
    B: Aggregate,
{
    fn snapshot_interval(&self) -> usize;
    fn get_client(&self) -> &impl tokio_postgres::GenericClient;
    fn get_mut_client(&mut self) -> &mut impl tokio_postgres::GenericClient;
}

impl<T> WithGenericClient<T> for PostgresEventStore<T>
where
    T: Aggregate,
{
    fn snapshot_interval(&self) -> usize {
        self.snapshot_interval
    }
    fn get_client(&self) -> &impl tokio_postgres::GenericClient {
        &self.client
    }

    fn get_mut_client(&mut self) -> &mut impl tokio_postgres::GenericClient {
        &mut self.client
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

    fn get_mut_client(&mut self) -> &mut impl tokio_postgres::GenericClient {
        &mut self.transaction
    }
}

impl<T, U: WithGenericClient<T> + GenericPostgresEventStore> EventStore<T> for U
where
    T: Aggregate + Serialize + DeserializeOwned,
    T::Event: Clone + Serialize + DeserializeOwned,
{
    type StoreError = tokio_postgres::Error;

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

    async fn commit(&mut self, id: &str, version: usize, action: T::Event) -> Result<()> {
        let json_action: Value = serde_json::to_value(action.clone())?;
        let client = self.get_mut_client();
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

        Ok(())
    }

    async fn store_snapshot(&mut self, id: &str) -> Result<()> {
        if let Some(key) = T::snapshot_key() {
            let Commit {
                version,
                diagnostics: _,
                inner,
            } = self.try_get_commit(id).await?;
            let snapshot: Value = serde_json::to_value(inner)?;
            self.get_mut_client()
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

    async fn append(&mut self, id: &str, action: T::Event) -> Result<()> {
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

#[cfg(test)]
#[allow(clippy::expect_used)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::tests::{Event, State};

    async fn init_event_store()
    -> std::result::Result<PostgresEventStore<State>, tokio_postgres::Error> {
        let db_name = "espg_test";
        let connection_string = format!("postgres://theodorton@localhost:5432/{}", db_name);
        let (client, connection) =
            tokio_postgres::connect(&connection_string, tokio_postgres::NoTls)
                .await
                .expect("Failed to connect to Postgres");
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("Connection error: {}", e);
            }
        });

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

    #[tokio::test]
    async fn test_postgres_event_store() -> Result<()> {
        let mut event_store = init_event_store()
            .await
            .expect("msg: Failed to create event store");

        let mut transaction = event_store.begin().await?;

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
        let event_store = init_event_store()
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
        let mut event_store = init_event_store()
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
        assert!(result.is_err(), "Expected version conflict error");

        Ok(())
    }

    #[tokio::test]
    async fn test_rollback() -> Result<()> {
        let mut event_store = init_event_store()
            .await
            .expect("msg: Failed to create event store");

        let mut transaction = event_store.begin().await?;
        transaction.append("test3", Event::Increment(10)).await?;
        transaction.complete().await?;

        let mut transaction = event_store.begin().await?;
        transaction.append("test3", Event::Decrement(4)).await?;
        transaction.rollback().await?;

        let commit = event_store
            .get_commit("test3")
            .await
            .expect("Commit not found");
        assert_eq!(commit.version, 1);
        assert_eq!(commit.inner.value, 10);

        Ok(())
    }

    #[tokio::test]
    async fn test_snapshot() -> Result<()> {
        let mut event_store = init_event_store()
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
        let mut event_store = init_event_store()
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
}
