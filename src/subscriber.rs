#[cfg(feature = "inmem")]
use crate::event_stores::InMemoryEventStore;
use crate::{Aggregate, Commit, EventStore};
use futures::StreamExt;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

pub struct Subscription {
    cancellation_token: CancellationToken,
    pub handle: JoinHandle<crate::Result<()>>,
}

impl Subscription {
    pub async fn cancel(self) -> crate::Result<()> {
        self.cancellation_token.cancel();
        match self.handle.await {
            Ok(result) => result,
            Err(e) => match e.try_into_panic() {
                Ok(panic) => {
                    panic!("Subscriber panicked: {:?}", panic);
                }
                Err(_) => {
                    panic!("Subscriber task was cancelled");
                }
            },
        }
    }
}

pub trait Subscriber<T: Aggregate + 'static> {
    const NAME: &'static str;

    fn handle_event(
        &self,
        store: &impl EventStore,
        commit: Commit<T::Event>,
    ) -> impl std::future::Future<Output = ()> + std::marker::Send;

    #[cfg(test)]
    fn tick(&self) -> impl std::future::Future<Output = ()> + std::marker::Send {
        // Default implementation does nothing
        async {}
    }

    #[cfg(feature = "postgres")]
    #[allow(async_fn_in_trait)]
    async fn start_postgres(self, config: tokio_postgres::Config) -> crate::Result<Subscription>
    where
        Self: Sized + Send + 'static,
    {
        use crate::{PostgresEventStore, PostgresEventStream, StreamingEventStore};

        let parent_token = CancellationToken::new();
        let child_token = parent_token.child_token();

        let (mut client, connection) = config.connect(tokio_postgres::NoTls).await?;

        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("Connection error: {}", e);
            }
        });

        // Make sure the row is present in the database so we can lock it
        client
            .execute(
                "INSERT INTO subscriptions (name, aggregate_type) VALUES ($1, $2) ON CONFLICT DO NOTHING",
                &[&Self::NAME, &T::NAME],
            )
            .await?;

        let handle = tokio::spawn(async move {
            use tokio::select;

            let subscriber = self;

            let streaming_event_store = PostgresEventStream::new(config).await?;
            let mut stream = streaming_event_store.stream::<T>().await?;

            loop {
                use crate::util::Txid;

                select! {
                    biased;

                    _ = child_token.cancelled() => {
                        break;
                    }
                    commit = stream.next() => {
                        let Some(commit) = commit else {
                            eprintln!("Stream ended, exiting subscriber");
                            break; // Exit if the stream ends
                        };

                        let commit = match commit {
                            Ok(commit) => commit,
                            Err(e) => {
                                return Err(crate::Error::StreamingError(e));
                            }
                        };

                        let tx = client.transaction().await?;
                        let row = tx.query_one(r#"
                            SELECT last_seq::text AS last_seq
                            FROM subscriptions
                            WHERE name = $1
                                AND aggregate_type = $2
                            FOR UPDATE
                        "#, &[&Self::NAME, &T::NAME]).await?;

                        #[cfg(test)]
                        subscriber.tick().await;

                        let last_seq_string: String = row.get(0);
                        let last_seq: Txid = last_seq_string.into();
                        let Some(commit_last_seq) = commit.global_seq else {
                            eprintln!("Commit without global sequence, skipping");
                            continue; // Skip commits without a global sequence
                        };

                        if commit_last_seq <= last_seq {
                            let _ = tx.rollback().await;
                            continue;
                        }

                        let store = PostgresEventStore::new(&tx);
                        subscriber.handle_event(&store, commit).await;

                        let commit_last_seq: String = commit_last_seq.to_string();
                        #[allow(clippy::expect_used)]
                        tx.execute("UPDATE subscriptions SET last_seq = $1::text WHERE name = $2 AND aggregate_type = $3", &[&commit_last_seq, &Self::NAME, &T::NAME]).await.expect("Failed to update subscription last_seq");
                        #[allow(clippy::expect_used)]
                        tx.commit().await.expect("Failed to commit transaction");
                    }
                }
            }

            Ok(())
        });

        Ok(Subscription {
            cancellation_token: parent_token,
            handle,
        })
    }

    #[cfg(feature = "inmem")]
    #[allow(async_fn_in_trait)]
    async fn start_inmem(self, event_store: InMemoryEventStore) -> crate::Result<Subscription>
    where
        Self: Sized + Send + 'static,
    {
        use crate::StreamingEventStore;

        let parent_token = CancellationToken::new();
        let child_token = parent_token.child_token();

        let key = (Self::NAME.to_string(), T::NAME.to_string());

        {
            let mut subscriptions = event_store.subscriptions.write().await;
            if !subscriptions.contains_key(&key) {
                use crate::util::Txid;

                subscriptions.insert(key.clone(), Txid(0));
            }
        }

        let mut stream = event_store.clone().stream::<T>().await?;

        let handle = tokio::spawn(async move {
            use tokio::select;

            let subscriber = self;

            loop {
                select! {
                    biased;

                    _ = child_token.cancelled() => {
                        break;
                    }
                    commit = stream.next() => {
                        let Some(commit) = commit else {
                            eprintln!("Stream ended, exiting subscriber");
                            break; // Exit if the stream ends
                        };

                        let commit = match commit {
                            Ok(commit) => commit,
                            Err(e) => {
                                return Err(crate::Error::StreamingError(e));
                            }
                        };

                        let Some(commit_seq) = commit.global_seq else {
                            eprintln!("Commit without global sequence, skipping");
                            continue; // Skip commits without a global sequence
                        };

                        #[cfg(test)]
                        subscriber.tick().await;

                        let mut subscriptions = event_store.subscriptions.write().await;
                        #[allow(clippy::expect_used)]
                        let offset = subscriptions.get_mut(&key).expect("Subscription not found");

                        eprintln!("Commit seq: {commit_seq}, offset: {offset}");
                        if commit_seq <= *offset {
                            continue; // Skip already processed events
                        }

                        subscriber.handle_event(&event_store, commit).await;

                        // Update the offset for the subscription
                        *offset = commit_seq;
                    }
                }
            }

            Ok(())
        });

        Ok(Subscription {
            cancellation_token: parent_token,
            handle,
        })
    }
}

#[cfg(test)]
#[cfg(feature = "postgres")]
#[cfg(feature = "inmem")]
#[allow(clippy::expect_used)]
#[allow(clippy::unwrap_used)]
mod tests {
    use std::sync::Arc;
    use tokio::sync::Mutex;

    use super::*;
    use crate::event_stores::postgres;
    use crate::tests::{Event, State};
    use crate::{EventStore, InMemoryEventStore, PostgresEventStore, Result};

    #[derive(Default)]
    pub struct TestSubscriber {
        invocations: Arc<Mutex<usize>>,
        tick: Arc<Mutex<usize>>,
    }

    impl Subscriber<State> for TestSubscriber {
        const NAME: &'static str = "TestSubscriber";

        async fn tick(&self) {
            let mut val = self.tick.lock().await;
            *val += 1;
        }

        async fn handle_event(&self, _store: &impl EventStore, _commit: Commit<Event>) {
            let mut val = self.invocations.lock().await;
            *val += 1;
        }
    }

    #[tokio::test]
    #[cfg(feature = "postgres")]
    async fn test_subscriber() -> Result<()> {
        let test_db = crate::test_helper::get_test_database().await;
        let client = test_db.client().await;
        let config = test_db.tokio_postgres_config().await;

        postgres::initialize(&client).await?;
        postgres::clear(&client).await?;

        // let event_store = PostgresEventStore::new(&client);
        // let subscriber = TestSubscriber::default();
        let subscriber = TestSubscriber::default();
        subscriber.start_postgres(config).await.unwrap();

        // It should save a subscription in the database
        let row = client
            .query_one(
                "SELECT * FROM subscriptions WHERE name = $1 AND aggregate_type = $2",
                &[&TestSubscriber::NAME, &State::NAME],
            )
            .await
            .ok();
        assert!(row.is_some());
        let row = row.unwrap();
        assert_eq!(row.get::<_, String>(1), TestSubscriber::NAME);
        assert_eq!(row.get::<_, String>(2), State::NAME);

        Ok(())
    }

    #[tokio::test]
    #[cfg(feature = "postgres")]
    async fn test_subscriber_concurrency() -> Result<()> {
        let test_db = crate::test_helper::get_test_database().await;
        let client = test_db.client().await;
        let config = test_db.tokio_postgres_config().await;

        postgres::initialize(&client).await?;
        postgres::clear(&client).await?;

        let event_store = PostgresEventStore::new(&client);
        let invocations = Arc::new(Mutex::new(0));
        let tick = Arc::new(Mutex::new(0));
        let mut subscriptions = vec![];
        for _ in 0..3 {
            let invocations = invocations.clone();
            let tick = tick.clone();
            let config = config.clone();
            let subscriber = TestSubscriber { invocations, tick };
            let sub = subscriber.start_postgres(config).await?;
            subscriptions.push(sub);
        }

        let id = State::id("test_state");
        for _ in 0..10 {
            event_store.append(&id, Event::Increment(10)).await?;
        }

        // Wait for all subscribers to have seen the events
        while *tick.lock().await < 30 {
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }

        for subscription in subscriptions {
            subscription.cancel().await?;
        }

        assert_eq!(*tick.lock().await, 30);
        assert_eq!(*invocations.lock().await, 10);

        Ok(())
    }

    #[tokio::test]
    #[cfg(feature = "inmem")]
    async fn test_inmem_subscriber() -> Result<()> {
        use crate::util::Txid;

        let event_store = InMemoryEventStore::default();

        // let event_store = PostgresEventStore::new(&client);
        // let subscriber = TestSubscriber::default();
        let subscriber = TestSubscriber::default();
        subscriber.start_inmem(event_store.clone()).await.unwrap();

        // It should save a subscription in the database
        let subs = event_store.subscriptions.read().await;
        let row = subs.get(&(TestSubscriber::NAME.to_string(), State::NAME.to_string()));
        assert!(row.is_some());
        let offset = row.unwrap();
        assert_eq!(*offset, Txid(0));

        Ok(())
    }

    #[tokio::test]
    #[cfg(feature = "inmem")]
    async fn test_inmem_subscriber_concurrency() -> Result<()> {
        let event_store = InMemoryEventStore::default();
        let invocations = Arc::new(Mutex::new(0));
        let tick = Arc::new(Mutex::new(0));
        let mut subscriptions = vec![];
        for _ in 0..3 {
            let invocations = invocations.clone();
            let tick = tick.clone();
            let event_store = event_store.clone();
            let subscriber = TestSubscriber { invocations, tick };
            let sub = subscriber.start_inmem(event_store).await.unwrap();
            subscriptions.push(sub);
        }

        let id = State::id("test_state");
        for _ in 0..10 {
            event_store.append(&id, Event::Increment(10)).await?;
        }

        // Wait for all subscribers to have seen the events
        while *tick.lock().await < 30 {
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }

        for subscription in subscriptions {
            subscription.cancel().await?;
        }

        assert_eq!(*tick.lock().await, 30);
        assert_eq!(*invocations.lock().await, 10);

        Ok(())
    }
}
