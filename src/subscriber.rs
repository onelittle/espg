use crate::{Aggregate, Commit, EventStore, PostgresEventStore};
use futures::StreamExt;
use tokio_postgres::GenericClient;
use tokio_util::sync::CancellationToken;

pub struct Subscription {
    cancellation_token: CancellationToken,
}

impl Subscription {
    pub fn cancel(self) {
        self.cancellation_token.cancel();
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

    #[allow(async_fn_in_trait)]
    async fn start(self, config: tokio_postgres::Config) -> crate::Result<Subscription>
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
        let event_store = PostgresEventStore::new(&client);
        event_store.ensure_subscription(&self).await?;

        tokio::spawn(async move {
            use tokio::select;

            let subscriber = self;

            let streaming_event_store = PostgresEventStream::new(config).await?;
            let mut stream = streaming_event_store.stream::<T>().await?;

            loop {
                select! {
                    biased;

                    _ = child_token.cancelled() => {
                        break;
                    }
                    commit = stream.next() => {
                        if let Some(commit) = commit {
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
                            let last_seq: i64 = last_seq_string.parse().expect("Failed to parse last_seq");
                            let commit_last_seq: i64 = commit.global_seq.expect("Commit global_seq is None");

                            if commit_last_seq <= last_seq {
                                tx.rollback().await.expect("Failed to rollback transaction");
                                continue;
                            }

                            let store = PostgresEventStore::new(&tx);
                            subscriber.handle_event(&store, commit).await;

                            let commit_last_seq: String = commit_last_seq.to_string();
                            tx.execute("UPDATE subscriptions SET last_seq = $1::text WHERE name = $2 AND aggregate_type = $3", &[&commit_last_seq, &Self::NAME, &T::NAME]).await.expect("Failed to update subscription last_seq");
                            tx.commit().await.expect("Failed to commit transaction");
                        } else {
                            break;
                        }
                    }
                }
            }

            Ok::<(), crate::Error>(())
        });

        Ok(Subscription {
            cancellation_token: parent_token,
        })
    }
}

trait SubscriberExt {
    async fn ensure_subscription<T: Aggregate + 'static, S: Subscriber<T>>(
        &self,
        subscriber: &S,
    ) -> crate::Result<()>;
}

impl<'a, Db: GenericClient> SubscriberExt for PostgresEventStore<'a, Db> {
    async fn ensure_subscription<T: Aggregate + 'static, S: Subscriber<T>>(
        &self,
        _: &S,
    ) -> crate::Result<()> {
        self.client
            .execute(
                "INSERT INTO subscriptions (name, aggregate_type) VALUES ($1, $2) ON CONFLICT DO NOTHING",
                &[&S::NAME, &T::NAME],
            )
            .await?;
        Ok(())
    }
}

#[cfg(test)]
#[cfg(feature = "postgres")]
#[allow(clippy::expect_used)]
#[allow(clippy::unwrap_used)]
mod tests {
    use std::sync::Arc;
    use tokio::sync::Mutex;

    use super::*;
    use crate::event_stores::postgres;
    use crate::tests::{Event, State};
    use crate::{EventStore, PostgresEventStore, Result};

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
    async fn test_subscriber() -> Result<()> {
        let test_db = crate::test_helper::get_test_database().await;
        let client = test_db.connect_and_discard_conn().await;
        let config = test_db.tokio_postgres_config();

        postgres::initialize(&client).await?;
        postgres::clear(&client).await?;

        // let event_store = PostgresEventStore::new(&client);
        // let subscriber = TestSubscriber::default();
        let subscriber = TestSubscriber::default();
        subscriber.start(config).await.unwrap();

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
    async fn test_subscriber_concurrency() -> Result<()> {
        let test_db = crate::test_helper::get_test_database().await;
        let client = test_db.connect_and_discard_conn().await;
        let config = test_db.tokio_postgres_config();

        postgres::initialize(&client).await?;
        postgres::clear(&client).await?;

        let event_store = PostgresEventStore::new(&client);
        let invocations = Arc::new(Mutex::new(0));
        let tick = Arc::new(Mutex::new(0));
        let mut handles = vec![];
        for _ in 0..3 {
            let invocations = invocations.clone();
            let tick = tick.clone();
            let config = config.clone();
            let handle = tokio::task::spawn(async move {
                let subscriber = TestSubscriber { invocations, tick };
                subscriber.start(config).await.unwrap()
            });
            handles.push(handle);
        }

        let id = State::id("test_state");
        for _ in 0..10 {
            event_store.append(&id, Event::Increment(10)).await?;
        }

        // Wait for all subscribers to have seen the events
        while *tick.lock().await < 30 {
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }

        for handle in handles {
            let subscription = handle.await.unwrap();
            subscription.cancel();
        }

        assert_eq!(*tick.lock().await, 30);
        assert_eq!(*invocations.lock().await, 10);

        Ok(())
    }
}
