use serde::{Serialize, de::DeserializeOwned};

use crate::Id;

pub trait Aggregate
where
    Self: Default + Serialize + DeserializeOwned + Send + Sync,
{
    type Event: Clone + Send + Sync + 'static + DeserializeOwned + serde::Serialize;

    const NAME: &'static str;
    const SNAPSHOT_INTERVAL: usize = 10;

    fn reduce(self, event: &Self::Event) -> Self;
    fn from_slice(events: &[Self::Event]) -> Self
    where
        Self: Default,
    {
        events
            .iter()
            .fold(Self::default(), |state, event| state.reduce(event))
    }
    // TODO: This should return a digest of the reducer
    fn snapshot_key() -> Option<&'static str> {
        None
    }

    fn id(val: impl Into<Id<Self>>) -> Id<Self>
    where
        Self: Sized,
    {
        val.into()
    }
}
