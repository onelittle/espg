use std::marker::PhantomData;

pub struct Id<T: Aggregate>(pub String, PhantomData<T>);

impl<T: Aggregate> std::fmt::Display for Id<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

pub trait Aggregate {
    type Event;

    // TODO: make this a const fn when stable
    fn name() -> &'static str {
        std::any::type_name::<Self>()
    }
    fn reduce(self, event: &Self::Event) -> Self;
    fn from_slice(events: &[Self::Event]) -> Self
    where
        Self: Default,
    {
        events
            .iter()
            .fold(Self::default(), |state, event| state.reduce(event))
    }
    fn snapshot_key() -> Option<&'static str> {
        None
    }
    fn id(id: impl Into<String>) -> Id<Self>
    where
        Self: Sized,
    {
        Id(id.into(), PhantomData)
    }
}
