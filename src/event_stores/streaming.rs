use std::{
    pin::Pin,
    task::{Context, Poll},
};
use tokio::sync::mpsc::UnboundedReceiver;
use tokio_stream::Stream;

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
