mod aggregate;
pub mod event_stores;

pub use aggregate::Aggregate;
#[cfg(feature = "inmem")]
pub use event_stores::InMemoryEventStore;
#[cfg(feature = "postgres")]
pub use event_stores::PostgresEventStore;
pub use event_stores::{Commit, Error, EventStore, Result};

#[cfg(feature = "postgres")]
#[cfg(feature = "streaming")]
pub use event_stores::postgres::stream as postgres_stream;

#[cfg(feature = "streaming")]
pub use event_stores::{EventStream, StreamingEventStore};

#[allow(async_fn_in_trait)]
pub trait Commands<'a, E: EventStore<T> + 'a, T: Aggregate + Default> {
    fn new(event_store: &'a E) -> Self;
    fn event_store(&'a self) -> &'a E;

    async fn commit(&'a self, id: &str, version: usize, event: T::Event) -> Result<()> {
        self.event_store().commit(id, version, event).await
    }

    async fn append(&'a self, id: &str, event: T::Event) -> Result<()> {
        self.event_store().append(id, event).await
    }

    async fn retry_on_version_conflict<'b, F, Fut>(&'a self, mut f: F) -> Result<()>
    where
        F: FnMut() -> Fut,
        Fut: std::future::Future<Output = Result<()>> + 'b,
        'a: 'b,
    {
        loop {
            match f().await {
                Ok(_) => return Ok(()),
                Err(Error::VersionConflict(_)) => {
                    eprintln!("Version conflict occurred, retrying...");
                }
                Err(e) => return Err(e),
            }
        }
    }
}

#[cfg(test)]
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
}
