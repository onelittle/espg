use std::{
    pin::Pin,
    task::{Context, Poll},
};
use tokio::sync::broadcast::Receiver;
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

    /// Get back the inner `UnboundedReceiver`.
    pub fn into_inner(self) -> UnboundedReceiver<I> {
        self.inner
    }
}

impl<T, X> EventStream<Receiver<T>, X> {
    /// Closes the receiving half of a channel without dropping it.
    ///
    /// This prevents any further messages from being sent on the channel while
    /// still enabling the receiver to drain messages that are buffered.
    pub fn close(&mut self) {
        // Note: This is a no-op for `std::sync::mpsc::Receiver` since it does not have a close method.
    }
}

impl<T, X> EventStream<UnboundedReceiver<T>, X> {
    /// Closes the receiving half of a channel without dropping it.
    ///
    /// This prevents any further messages from being sent on the channel while
    /// still enabling the receiver to drain messages that are buffered.
    pub fn close(&mut self) {
        self.inner.close();
    }
}

impl<T, X: Unpin> Stream for EventStream<UnboundedReceiver<T>, X> {
    type Item = T;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.inner.poll_recv(cx)
    }
}

impl<T, X> AsRef<UnboundedReceiver<T>> for EventStream<UnboundedReceiver<T>, X> {
    fn as_ref(&self) -> &UnboundedReceiver<T> {
        &self.inner
    }
}

impl<T, X> AsMut<UnboundedReceiver<T>> for EventStream<UnboundedReceiver<T>, X> {
    fn as_mut(&mut self) -> &mut UnboundedReceiver<T> {
        &mut self.inner
    }
}
