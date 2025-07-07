mod loadable;
mod txid;

pub(crate) use loadable::Loadable;
pub(crate) use txid::Txid;

#[cfg(feature = "streaming")]
mod task_drop_guard {
    use tokio::task::{AbortHandle, JoinHandle};

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
}

#[cfg(feature = "streaming")]
pub(crate) use task_drop_guard::TaskDropGuard;
