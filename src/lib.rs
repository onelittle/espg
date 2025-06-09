mod aggregate;
mod event_stores;

pub use aggregate::Aggregate;
pub use event_stores::InMemoryEventStore;
pub use event_stores::PostgresEventStore;
pub use event_stores::{Error, EventStore, Result, StreamingEventStore};

#[cfg(test)]
mod tests {
    use serde::{Deserialize, Serialize};

    use super::Aggregate;

    #[derive(Default, Serialize, Deserialize)]
    pub struct State {
        pub value: i32,
    }

    #[derive(Clone, Debug, Serialize, Deserialize)]
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
