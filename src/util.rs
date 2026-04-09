//! Utility helpers used across the crate.

use std::sync::{Mutex, MutexGuard, RwLock, RwLockReadGuard, RwLockWriteGuard};

/// Extension trait for [`Mutex`] that recovers from poisoning.
///
/// A poisoned lock means a thread panicked while holding it. We accept
/// the potentially-inconsistent state rather than cascading the panic
/// to every other thread (which would crash the entire server).
pub(crate) trait LockExt<T> {
    fn lock_or_poison(&self) -> MutexGuard<'_, T>;
}

/// Extension trait for [`RwLock`] that recovers from poisoning.
///
/// Same rationale as [`LockExt`]: recover rather than cascade panics.
pub(crate) trait RwLockExt<T> {
    fn read_or_poison(&self) -> RwLockReadGuard<'_, T>;
    fn write_or_poison(&self) -> RwLockWriteGuard<'_, T>;
}

impl<T> LockExt<T> for Mutex<T> {
    fn lock_or_poison(&self) -> MutexGuard<'_, T> {
        match self.lock() {
            Ok(guard) => guard,
            Err(poisoned) => {
                tracing::error!("recovering from poisoned mutex");
                poisoned.into_inner()
            }
        }
    }
}

impl<T> RwLockExt<T> for RwLock<T> {
    fn read_or_poison(&self) -> RwLockReadGuard<'_, T> {
        match self.read() {
            Ok(guard) => guard,
            Err(poisoned) => {
                tracing::error!("recovering from poisoned rwlock (read)");
                poisoned.into_inner()
            }
        }
    }

    fn write_or_poison(&self) -> RwLockWriteGuard<'_, T> {
        match self.write() {
            Ok(guard) => guard,
            Err(poisoned) => {
                tracing::error!("recovering from poisoned rwlock (write)");
                poisoned.into_inner()
            }
        }
    }
}
