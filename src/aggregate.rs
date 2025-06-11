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
}
