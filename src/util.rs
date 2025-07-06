mod loadable;
mod txid;

pub(crate) use loadable::Loadable;
use tokio::task::{AbortHandle, JoinHandle};
pub(crate) use txid::Txid;

/// A wrapper for a task handle that aborts the task when dropped.
pub(crate) struct TaskDropGuard {
    handle: AbortHandle,
}

impl TaskDropGuard {
    pub(crate) fn new<T>(handle: JoinHandle<T>) -> Self {
        TaskDropGuard {
            handle: handle.abort_handle(),
        }
    }
}

impl Drop for TaskDropGuard {
    fn drop(&mut self) {
        self.handle.abort();
    }
}
