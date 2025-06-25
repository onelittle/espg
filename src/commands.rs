use async_trait::async_trait;

use crate::{Aggregate, Error, EventStore, Id, Result};

#[async_trait]
pub trait Commands<'a> {
    fn event_store(&'a self) -> &'a impl EventStore;

    async fn commit<X: Aggregate>(
        &'a self,
        id: &Id<X>,
        version: usize,
        event: X::Event,
    ) -> Result<()> {
        self.event_store().commit(id, version, event).await
    }

    async fn append<X: Aggregate>(&'a self, id: &Id<X>, event: X::Event) -> Result<()> {
        self.event_store().append(id, event).await
    }

    async fn retry_on_version_conflict<'b, F, Fut, R>(&'a self, mut f: F) -> Result<()>
    where
        F: FnMut() -> Fut + Send + 'b,
        Fut: std::future::Future<Output = Result<R>> + Send + 'b,
        'a: 'b,
    {
        loop {
            match f().await {
                Ok(_) => return Ok(()),
                Err(Error::VersionConflict(_)) => {
                    eprintln!("Version conflict occurred, retrying...");
                }
                Err(e) => return Err(e),
            }
        }
    }
}
