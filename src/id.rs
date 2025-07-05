use std::marker::PhantomData;

#[cfg(feature = "rocket")]
use rocket::request::FromParam;
use serde::{Deserialize, Serialize};

#[derive(Debug)]
pub struct Id<T>(pub String, PhantomData<T>);

impl<T> PartialEq for Id<T> {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

impl<T> Eq for Id<T> {}

impl<T> std::hash::Hash for Id<T> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.0.hash(state);
    }
}

impl<T> Serialize for Id<T> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(&self.0)
    }
}

impl<'de, T> Deserialize<'de> for Id<T> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        Ok(Id(s, PhantomData))
    }
}

pub fn id<T>(id: impl Into<String>) -> Id<T> {
    Id(id.into(), PhantomData)
}

#[cfg(feature = "async-graphql")]
#[async_graphql::Scalar]
impl<T: Send + Sync> async_graphql::ScalarType for Id<T> {
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

impl<T> Clone for Id<T> {
    fn clone(&self) -> Self {
        Id(self.0.clone(), PhantomData)
    }
}

impl<T> std::fmt::Display for Id<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[cfg(feature = "rocket")]
impl<T> FromParam<'_> for Id<T> {
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
