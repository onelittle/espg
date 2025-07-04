use std::marker::PhantomData;

#[cfg(feature = "rocket")]
use rocket::request::FromParam;
use serde::{Serialize, de::DeserializeOwned};

pub struct Id<T: Aggregate>(pub String, PhantomData<T>);

pub fn id<T: Aggregate>(id: impl Into<String>) -> Id<T> {
    T::id(id)
}

#[cfg(feature = "async-graphql")]
#[async_graphql::Scalar]
impl<T: Aggregate> async_graphql::ScalarType for Id<T> {
    fn parse(value: async_graphql::Value) -> async_graphql::InputValueResult<Self> {
        if let async_graphql::Value::String(s) = &value {
            Ok(Id(s.clone(), PhantomData))
        } else {
            Err(async_graphql::InputValueError::expected_type(value))
        }
    }

    fn to_value(&self) -> async_graphql::Value {
        async_graphql::Value::String(self.0.clone())
    }
}

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

    const NAME: &'static str;

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tests::State;

    #[test]
    fn test_id_display() {
        let id: Id<State> = id("test_id");
        assert_eq!(id.to_string(), "test_id");
    }
}
