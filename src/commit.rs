use serde::{Deserialize, Serialize};

use crate::{Aggregate, Id, util::Txid};

#[derive(Clone, Serialize, Deserialize, Debug)]
pub(crate) struct Diagnostics {
    pub loaded_events: usize,
    pub snapshotted: bool,
}

#[derive(Default, Clone, Serialize, Deserialize, Debug)]
pub struct Commit<T> {
    pub id: String,
    pub version: usize,
    #[serde(flatten)]
    pub inner: T,
    #[serde(skip)]
    pub(crate) diagnostics: Option<Diagnostics>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) global_seq: Option<Txid>,
}

impl<T> Commit<T> {
    pub fn new(id: impl Into<String>, version: usize, inner: T) -> Self {
        Self {
            id: id.into(),
            version,
            inner,
            diagnostics: None,
            global_seq: None,
        }
    }

    pub fn into_inner(self) -> T {
        self.inner
    }

    pub fn with_id(self, id: Id<T>) -> Self {
        Commit {
            id: id.into(),
            version: self.version,
            inner: self.inner,
            diagnostics: self.diagnostics,
            global_seq: self.global_seq,
        }
    }

    pub fn with_inner<U>(self, inner: U) -> Commit<U> {
        Commit {
            id: self.id,
            version: self.version,
            inner,
            diagnostics: self.diagnostics,
            global_seq: self.global_seq,
        }
    }
}

impl<T: Aggregate> Commit<T> {
    pub fn reduce(&mut self, other: Commit<T::Event>) {
        let aggregate = std::mem::take(&mut self.inner);
        self.inner = aggregate.reduce(&other.inner);
        self.version = other.version;
    }
}
