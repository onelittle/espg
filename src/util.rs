use std::{
    fmt::Display,
    ops::{Add, AddAssign},
};

use serde::{Deserialize, Serialize};

#[derive(Copy, Clone, Debug, PartialEq)]
pub struct Txid(pub(crate) i64);

impl PartialOrd for Txid {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.0.cmp(&other.0))
    }
}

impl Display for Txid {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl Default for Txid {
    fn default() -> Self {
        Txid(1)
    }
}

impl Add<i64> for Txid {
    type Output = Self;

    fn add(self, other: i64) -> Self::Output {
        Txid(self.0 + other)
    }
}

impl AddAssign<i64> for Txid {
    fn add_assign(&mut self, other: i64) {
        self.0 += other;
    }
}

#[allow(clippy::expect_used)]
impl From<String> for Txid {
    fn from(val: String) -> Self {
        eprintln!("Parsing txid from string: {}", val);
        Txid(val.parse().expect("Failed to parse txid"))
    }
}

impl From<i64> for Txid {
    fn from(val: i64) -> Self {
        Txid(val)
    }
}

impl From<&Txid> for usize {
    fn from(value: &Txid) -> Self {
        value.0 as usize
    }
}

impl Serialize for Txid {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_i64(self.0)
    }
}

impl<'a> Deserialize<'a> for Txid {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'a>,
    {
        let value = i64::deserialize(deserializer)?;
        Ok(Txid(value))
    }
}
