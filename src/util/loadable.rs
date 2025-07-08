use std::collections::HashMap;

use crate::{Aggregate, Commit, Error, EventStore, Id, Result};

pub trait Loadable {
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
    type Output = Vec<Commit<X>>;

    async fn load(self, store: &impl EventStore) -> Result<Self::Output> {
        let ids: Vec<&Id<X>> = self.iter().collect();
        store.get_commits(&ids).await
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

impl<K: std::cmp::Eq + std::hash::Hash, X: Aggregate> Loadable for HashMap<K, Id<X>> {
    type Output = HashMap<K, Commit<X>>;

    async fn load(self, store: &impl EventStore) -> Result<Self::Output> {
        let as_array: Vec<(K, Id<X>)> = self.into_iter().collect();
        let ids: Vec<&Id<X>> = as_array.iter().map(|(_, id)| id).collect();
        let commits = store.get_commits(&ids).await?;
        let mut results = HashMap::new();
        for (commit, (k, _)) in commits.into_iter().zip(as_array.into_iter()) {
            results.insert(k, commit);
        }
        Ok(results)
    }
}

impl<K: std::cmp::Eq + std::hash::Hash, X: Aggregate> Loadable for HashMap<K, Vec<Id<X>>> {
    type Output = HashMap<K, Vec<Commit<X>>>;

    async fn load(self, store: &impl EventStore) -> Result<Self::Output> {
        let as_array: Vec<(K, Vec<Id<X>>)> = self.into_iter().collect();
        let mut ids: Vec<Id<X>> = as_array.iter().fold(vec![], |mut vec, (_, ids)| {
            vec.extend_from_slice(&ids.to_vec());
            vec
        });
        let mut commits = ids.clone().load(store).await?;
        let mut results = HashMap::new();
        for (k, v) in as_array.into_iter() {
            let mut commit_vec = vec![];
            for id in &v {
                let idx = ids
                    .iter()
                    .position(|v| v == id)
                    .ok_or(Error::NotFound(format!("Commit not found for ID: {}", id)))?;
                // Remove the ID at idx from ids to avoid duplicates
                eprintln!("Removing ID at index {}: {:?}", idx, ids[idx]);
                ids.remove(idx);
                let commit = commits.remove(idx);
                commit_vec.push(commit);
            }
            results.insert(k, commit_vec);
        }
        Ok(results)
    }
}

impl<K: std::cmp::Eq + std::hash::Hash, X: Aggregate> Loadable for indexmap::IndexMap<K, Id<X>> {
    type Output = indexmap::IndexMap<K, Commit<X>>;

    async fn load(self, store: &impl EventStore) -> Result<Self::Output> {
        let as_array: Vec<(K, Id<X>)> = self.into_iter().collect();
        let ids: Vec<&Id<X>> = as_array.iter().map(|(_, id)| id).collect();
        let commits = store.get_commits(&ids).await?;
        let mut results = Self::Output::new();
        for (commit, (k, _)) in commits.into_iter().zip(as_array.into_iter()) {
            results.insert(k, commit);
        }
        Ok(results)
    }
}
