use serde::{Deserialize, Serialize};

use crate::util::Txid;

#[derive(Clone, Serialize, Deserialize, Debug)]
pub(crate) struct Diagnostics {
    pub loaded_events: usize,
    pub snapshotted: bool,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct Commit<T> {
    pub id: String,
    pub version: usize,
    pub inner: T,
    pub(crate) diagnostics: Option<Diagnostics>,
    pub(crate) global_seq: Option<Txid>,
}

impl<T> Commit<T> {
    pub fn into_inner(self) -> T {
        self.inner
    }
}

#[cfg(feature = "async-graphql")]
#[async_graphql::Object]
impl<T: async_graphql::OutputType> Commit<T> {
    async fn id(&self) -> String {
        self.id.clone()
    }

    async fn version(&self) -> usize {
        self.version
    }

    async fn inner(&self) -> &T {
        &self.inner
    }
}

#[cfg(feature = "async-graphql")]
impl<T: async_graphql::OutputType> async_graphql::TypeName for Commit<T> {
    fn type_name() -> std::borrow::Cow<'static, str> {
        format!("{}Commit", <T as async_graphql::OutputType>::type_name()).into()
    }
}
