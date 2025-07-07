use std::{
    pin::Pin,
    task::{Context, Poll},
};
use tokio::sync::mpsc::UnboundedReceiver;
use tokio_stream::Stream;

use super::Result;
use crate::{Aggregate, Commit, Id};

#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum StreamItemError {
    #[error("Tokio Postgres error: {0}")]
    TokioPgError(#[from] tokio_postgres::Error),
}

pub type StreamItem<T> = std::result::Result<Commit<T>, StreamItemError>;

#[allow(async_fn_in_trait)]
pub trait StreamingEventStore {
    async fn stream<T: Aggregate>(self) -> Result<impl Stream<Item = StreamItem<T::Event>>>;

    async fn stream_by_id<T: Aggregate + 'static>(
        self,
        id: Id<T>,
    ) -> Result<impl Stream<Item = StreamItem<T::Event>>>
    where
        Self: Sized,
    {
        use futures::StreamExt;

        let main_stream = self.stream::<T>().await?;
        Ok(main_stream.filter(move |item| {
            use futures::future;

            future::ready(match item {
                Ok(commit) => commit.id == id.to_string(),
                Err(_) => false,
            })
        }))
    }
}

#[derive(Debug)]
pub struct EventStream<R, X> {
    inner: R,
    _client: X,
}

impl<I, X> EventStream<UnboundedReceiver<I>, X> {
    /// Create a new `UnboundedReceiverStream`.
    pub fn new(recv: UnboundedReceiver<I>, client: X) -> Self {
        Self {
            inner: recv,
            _client: client,
        }
    }
}

impl<T, X: Unpin> Stream for EventStream<UnboundedReceiver<T>, X> {
    type Item = T;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.inner.poll_recv(cx)
    }
}
