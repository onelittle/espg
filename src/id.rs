use std::{fmt::Debug, marker::PhantomData};

use serde::{Deserialize, Serialize};

pub struct Id<T>(pub String, pub(crate) PhantomData<T>);

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

impl<T> Debug for Id<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
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

impl<T> From<Id<T>> for String {
    fn from(id: Id<T>) -> Self {
        id.0
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
