//! Shared "which workers care about this subject" bitmask.
//!
//! Used by the sharded-worker dispatch path (see ADR-012). When a worker
//! receives a PUB it needs to know which *other* workers have subscribers
//! matching the subject so it can forward the message to their inboxes
//! instead of iterating a global sub list.
//!
//! The data structure is a refcounted map from subject to a per-worker
//! bitmask. On every SUB the owning worker bumps its refcount for that
//! subject and, on a 0→1 transition, sets its bit in the shared atomic
//! mask. On UNSUB it decrements and clears the bit on 1→0.
//!
//! The PUB hot path only needs a single `RwLock` read + `HashMap::get`
//! + atomic `load` — no iteration, no allocation.
//!
//! ## Limitations (step 1 of ADR-012)
//!
//! - **Exact subjects only.** Wildcard patterns are NOT tracked here yet;
//!   the caller must fall back to iterating per-worker sub lists for
//!   wildcard matches. Wildcard support lands in step 5 when this type
//!   is integrated with `deliver_to_subs_core`.
//! - **Max 64 workers per process.** The mask is a `u64`. This cap is
//!   plenty for realistic deployments and lets the PUB path use a single
//!   atomic load.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::RwLock;

use rustc_hash::FxHasher;

/// Hard cap on the number of worker threads a single `ServerState` can
/// host. Bitmask ops assume this fits in a `u64`.
pub const MAX_WORKERS: usize = 64;

/// Per-subject state: a per-worker refcount array and a cached bitmask
/// derived from it.
///
/// The mask is the source of truth on the PUB hot path — one `Relaxed`
/// load gives us the set of workers with interest. The per-worker
/// refcounts are only touched on SUB/UNSUB (write lock held on the
/// parent map) and only exist so `remove()` can tell when the last sub
/// for (worker, subject) is gone.
pub struct WorkerInterestEntry {
    /// Bit `i` set iff worker `i` has at least one sub for this subject.
    /// Read on every matching PUB; written only on 0→1 / 1→0 transitions.
    mask: AtomicU64,
    /// Refcount per worker. Fixed-size array to avoid per-entry heap
    /// allocation; `MAX_WORKERS` atomics × 4 B = 256 B, plus the mask
    /// = 264 B per entry. At 10k subjects that's ~2.6 MB — fine.
    per_worker: [AtomicU32; MAX_WORKERS],
}

impl WorkerInterestEntry {
    fn new() -> Self {
        // Manual const-init because `[AtomicU32::new(0); N]` is non-Copy.
        const ZERO: AtomicU32 = AtomicU32::new(0);
        Self {
            mask: AtomicU64::new(0),
            per_worker: [ZERO; MAX_WORKERS],
        }
    }

    /// Returns `true` if the worker's bit transitioned 0 → 1.
    fn add(&self, worker_id: usize) -> bool {
        let prev = self.per_worker[worker_id].fetch_add(1, Ordering::AcqRel);
        if prev == 0 {
            self.mask
                .fetch_or(1u64 << worker_id, Ordering::AcqRel);
            true
        } else {
            false
        }
    }

    /// Returns `true` if the worker's bit transitioned 1 → 0. If the
    /// refcount was already zero (bug or double-unsub), returns `false`
    /// without underflowing.
    fn sub(&self, worker_id: usize) -> bool {
        // Compare-and-swap loop to avoid underflow if remove() is called
        // more times than insert().
        let slot = &self.per_worker[worker_id];
        let mut cur = slot.load(Ordering::Acquire);
        loop {
            if cur == 0 {
                return false;
            }
            match slot.compare_exchange_weak(
                cur,
                cur - 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    if cur == 1 {
                        self.mask
                            .fetch_and(!(1u64 << worker_id), Ordering::AcqRel);
                        return true;
                    }
                    return false;
                }
                Err(actual) => cur = actual,
            }
        }
    }

    /// Current bitmask of interested workers.
    #[inline]
    pub fn mask(&self) -> u64 {
        self.mask.load(Ordering::Acquire)
    }

    /// Total refcount across all workers — used only by tests and debug.
    #[cfg(test)]
    fn total_count(&self) -> u32 {
        self.per_worker
            .iter()
            .map(|c| c.load(Ordering::Relaxed))
            .sum()
    }
}

/// Type alias matching `sub_list`'s style for fast hashing.
type FxHashMap<K, V> = HashMap<K, V, std::hash::BuildHasherDefault<FxHasher>>;

/// Shared subject → worker-bitmask refcount map.
pub struct WorkerInterest {
    exact: RwLock<FxHashMap<String, WorkerInterestEntry>>,
}

impl WorkerInterest {
    pub fn new() -> Self {
        Self {
            exact: RwLock::new(FxHashMap::default()),
        }
    }

    /// Record that `worker_id` has added a sub for `subject`. Safe to
    /// call from any thread; briefly takes the write lock only if the
    /// entry doesn't already exist.
    ///
    /// Panics in debug if `worker_id >= MAX_WORKERS`.
    pub fn insert(&self, subject: &str, worker_id: usize) {
        debug_assert!(worker_id < MAX_WORKERS, "worker_id out of range");

        // Fast path: entry already exists, only a read lock + atomic add.
        {
            let map = self.exact.read().unwrap_or_else(|e| e.into_inner());
            if let Some(entry) = map.get(subject) {
                entry.add(worker_id);
                return;
            }
        }

        // Slow path: insert a fresh entry under the write lock.
        let mut map = self.exact.write().unwrap_or_else(|e| e.into_inner());
        let entry = map
            .entry(subject.to_string())
            .or_insert_with(WorkerInterestEntry::new);
        entry.add(worker_id);
    }

    /// Record that `worker_id` has removed a sub for `subject`.
    ///
    /// Does not remove empty entries from the map — keeping the
    /// allocation around means repeated SUB/UNSUB churn on a single
    /// subject doesn't thrash the map. A future cleanup pass can GC
    /// zero-mask entries if memory becomes a concern.
    pub fn remove(&self, subject: &str, worker_id: usize) {
        debug_assert!(worker_id < MAX_WORKERS, "worker_id out of range");

        let map = self.exact.read().unwrap_or_else(|e| e.into_inner());
        if let Some(entry) = map.get(subject) {
            entry.sub(worker_id);
        }
    }

    /// Return the bitmask of workers with interest in `subject`. Bit `i`
    /// is set iff worker `i` has at least one sub matching. Unknown
    /// subjects return 0.
    ///
    /// Only considers exact-subject subs for now; wildcard matching will
    /// be layered on top when this type is integrated with
    /// `deliver_to_subs_core` (step 5 of ADR-012).
    #[inline]
    pub fn matching_workers(&self, subject: &str) -> u64 {
        let map = self.exact.read().unwrap_or_else(|e| e.into_inner());
        map.get(subject).map(|e| e.mask()).unwrap_or(0)
    }

    /// Current number of subject entries in the map. Test/debug only.
    #[cfg(test)]
    pub(crate) fn entry_count(&self) -> usize {
        self.exact.read().unwrap_or_else(|e| e.into_inner()).len()
    }
}

impl Default for WorkerInterest {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unknown_subject_returns_zero_mask() {
        let wi = WorkerInterest::new();
        assert_eq!(wi.matching_workers("foo"), 0);
    }

    #[test]
    fn single_worker_insert_sets_bit() {
        let wi = WorkerInterest::new();
        wi.insert("foo", 0);
        assert_eq!(wi.matching_workers("foo"), 0b1);
        wi.insert("foo", 3);
        assert_eq!(wi.matching_workers("foo"), 0b1001);
    }

    #[test]
    fn remove_clears_bit_when_refcount_hits_zero() {
        let wi = WorkerInterest::new();
        wi.insert("foo", 2);
        assert_eq!(wi.matching_workers("foo"), 0b100);
        wi.remove("foo", 2);
        assert_eq!(wi.matching_workers("foo"), 0);
    }

    #[test]
    fn multiple_subs_from_same_worker_refcount_correctly() {
        let wi = WorkerInterest::new();
        wi.insert("foo", 1);
        wi.insert("foo", 1);
        wi.insert("foo", 1);
        assert_eq!(wi.matching_workers("foo"), 0b10);

        // First two removes should leave the bit set.
        wi.remove("foo", 1);
        assert_eq!(wi.matching_workers("foo"), 0b10);
        wi.remove("foo", 1);
        assert_eq!(wi.matching_workers("foo"), 0b10);

        // Final remove clears the bit.
        wi.remove("foo", 1);
        assert_eq!(wi.matching_workers("foo"), 0);
    }

    #[test]
    fn different_workers_same_subject_share_entry() {
        let wi = WorkerInterest::new();
        wi.insert("foo", 0);
        wi.insert("foo", 1);
        wi.insert("foo", 7);
        assert_eq!(wi.matching_workers("foo"), 0b1000_0011);
        assert_eq!(wi.entry_count(), 1);
    }

    #[test]
    fn different_subjects_are_isolated() {
        let wi = WorkerInterest::new();
        wi.insert("foo", 0);
        wi.insert("bar", 1);
        wi.insert("baz", 2);
        assert_eq!(wi.matching_workers("foo"), 0b001);
        assert_eq!(wi.matching_workers("bar"), 0b010);
        assert_eq!(wi.matching_workers("baz"), 0b100);
        assert_eq!(wi.matching_workers("qux"), 0);
        assert_eq!(wi.entry_count(), 3);
    }

    #[test]
    fn remove_on_unknown_subject_is_noop() {
        let wi = WorkerInterest::new();
        wi.remove("never_seen", 0);
        assert_eq!(wi.matching_workers("never_seen"), 0);
        assert_eq!(wi.entry_count(), 0);
    }

    #[test]
    fn remove_underflow_is_safe() {
        let wi = WorkerInterest::new();
        wi.insert("foo", 5);
        wi.remove("foo", 5);
        // Second remove with refcount already zero should be a no-op.
        wi.remove("foo", 5);
        wi.remove("foo", 5);
        assert_eq!(wi.matching_workers("foo"), 0);
    }

    #[test]
    fn highest_worker_id_works() {
        let wi = WorkerInterest::new();
        wi.insert("foo", MAX_WORKERS - 1);
        assert_eq!(wi.matching_workers("foo"), 1u64 << (MAX_WORKERS - 1));
    }

    #[test]
    fn entry_retains_total_count_across_workers() {
        let wi = WorkerInterest::new();
        wi.insert("foo", 0);
        wi.insert("foo", 0);
        wi.insert("foo", 1);

        let map = wi.exact.read().unwrap();
        let entry = map.get("foo").unwrap();
        assert_eq!(entry.total_count(), 3);
        assert_eq!(entry.mask(), 0b11);
    }

    #[test]
    fn entry_survives_last_remove_for_reuse() {
        let wi = WorkerInterest::new();
        wi.insert("foo", 0);
        wi.remove("foo", 0);
        // Entry still present; mask is zero but allocation is retained.
        assert_eq!(wi.matching_workers("foo"), 0);
        assert_eq!(wi.entry_count(), 1);

        // Re-inserting reuses the same entry.
        wi.insert("foo", 2);
        assert_eq!(wi.matching_workers("foo"), 0b100);
        assert_eq!(wi.entry_count(), 1);
    }

    #[test]
    fn concurrent_insert_remove_from_multiple_threads() {
        use std::sync::Arc;
        use std::thread;

        let wi = Arc::new(WorkerInterest::new());
        let n_threads = 4;
        let n_iters = 1000;
        let mut handles = Vec::new();
        for tid in 0..n_threads {
            let wi = Arc::clone(&wi);
            handles.push(thread::spawn(move || {
                for _ in 0..n_iters {
                    wi.insert("hot", tid);
                    wi.remove("hot", tid);
                }
            }));
        }
        for h in handles {
            let _ = h.join();
        }
        // After all threads finish paired insert/remove, the mask must
        // be zero. Any observable non-zero state would indicate a bug
        // in the refcount-to-mask transition logic.
        assert_eq!(wi.matching_workers("hot"), 0);
    }
}
