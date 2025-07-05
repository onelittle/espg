use std::collections::HashMap;

use crate::{Aggregate, Error, EventStore, Id, Result};

pub(crate) trait Loadable {
    type Output;

    fn load(self, store: &impl EventStore) -> impl Future<Output = Result<Self::Output>>;
}

impl<X: Aggregate + Default> Loadable for Id<X> {
    type Output = X;

    async fn load(self, store: &impl EventStore) -> Result<Self::Output> {
        store
            .get_commit(&self)
            .await
            .map(|commit| commit.inner)
            .ok_or_else(|| Error::NotFound(self.to_string()))
    }
}

impl<X: Aggregate> Loadable for Vec<Id<X>> {
    type Output = Vec<X>;

    async fn load(self, store: &impl EventStore) -> Result<Self::Output> {
        let ids = &self;
        let mut commits = store.get_commits(ids).await?;
        self.iter()
            .map(|id| {
                commits
                    .remove(id)
                    .map(|commit| commit.inner)
                    .ok_or_else(|| Error::NotFound(id.to_string()))
            })
            .collect()
    }
}

impl<T: Loadable> Loadable for Option<T> {
    type Output = Option<T::Output>;

    async fn load(self, store: &impl EventStore) -> Result<Self::Output> {
        match self {
            Some(id) => Ok(Some(id.load(store).await?)),
            None => Ok(None),
        }
    }
}

impl<K: std::cmp::Eq + std::hash::Hash, V: Loadable> Loadable for HashMap<K, V> {
    type Output = HashMap<K, V::Output>;

    async fn load(self, store: &impl EventStore) -> Result<Self::Output> {
        let mut results = HashMap::new();
        for (key, id) in self {
            results.insert(key, id.load(store).await?);
        }
        Ok(results)
    }
}
