use std::marker::PhantomData;

#[cfg(feature = "rocket")]
use rocket::request::FromParam;
use serde::{Serialize, de::DeserializeOwned};

pub struct Id<T: Aggregate>(pub String, PhantomData<T>);

impl<T: Aggregate> Clone for Id<T> {
    fn clone(&self) -> Self {
        Id(self.0.clone(), PhantomData)
    }
}

impl<T: Aggregate> std::fmt::Display for Id<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

pub trait Aggregate
where
    Self: Default + Serialize + DeserializeOwned + Send + Sync,
{
    type Event: Clone + Send + Sync + 'static + DeserializeOwned + serde::Serialize;

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

#[cfg(feature = "rocket")]
impl<T: Aggregate> FromParam<'_> for Id<T> {
    type Error = String;

    fn from_param(param: &str) -> Result<Self, Self::Error> {
        Ok(Id(param.to_string(), PhantomData))
    }
}
