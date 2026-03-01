use super::error::{Result, TransactionError};
use super::manager::TransactionId;
use dashmap::DashMap;
use std::sync::Arc;

/// Lock mode
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum LockMode {
    Shared,
    Exclusive,
}

/// Lock key (table or tuple identifier)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct LockKey {
    pub table_id: u32,
    pub tuple_id: Option<u64>,
}

impl LockKey {
    /// Creates a table-level lock key
    pub fn table(table_id: u32) -> Self {
        Self { table_id, tuple_id: None }
    }

    /// Creates a tuple-level lock key
    pub fn tuple(table_id: u32, tuple_id: u64) -> Self {
        Self { table_id, tuple_id: Some(tuple_id) }
    }
}

/// Lock entry
#[derive(Debug)]
struct LockEntry {
    holders: Vec<(TransactionId, LockMode)>,
}

/// Lock manager
pub struct LockManager {
    locks: Arc<DashMap<LockKey, LockEntry>>,
}

impl LockManager {
    /// Creates a new lock manager
    pub fn new() -> Self {
        Self { locks: Arc::new(DashMap::new()) }
    }

    /// Acquires a lock
    pub fn acquire(&self, xid: TransactionId, key: LockKey, mode: LockMode) -> Result<()> {
        let mut entry = self.locks.entry(key).or_insert(LockEntry { holders: Vec::new() });

        // Check compatibility
        for (holder_xid, holder_mode) in &entry.holders {
            if *holder_xid == xid {
                // Already holds lock
                return Ok(());
            }

            if !self.is_compatible(mode, *holder_mode) {
                return Err(TransactionError::Deadlock);
            }
        }

        entry.holders.push((xid, mode));
        Ok(())
    }

    /// Releases a lock
    pub fn release(&self, xid: TransactionId, key: LockKey) -> Result<()> {
        if let Some(mut entry) = self.locks.get_mut(&key) {
            entry.holders.retain(|(holder_xid, _)| *holder_xid != xid);

            if entry.holders.is_empty() {
                drop(entry);
                self.locks.remove(&key);
            }
        }
        Ok(())
    }

    /// Releases all locks held by a transaction
    pub fn release_all(&self, xid: TransactionId) {
        let keys: Vec<LockKey> = self
            .locks
            .iter()
            .filter(|entry| entry.holders.iter().any(|(holder_xid, _)| *holder_xid == xid))
            .map(|entry| *entry.key())
            .collect();

        for key in keys {
            let _ = self.release(xid, key);
        }
    }

    /// Checks if two lock modes are compatible
    fn is_compatible(&self, mode1: LockMode, mode2: LockMode) -> bool {
        match (mode1, mode2) {
            (LockMode::Shared, LockMode::Shared) => true,
            _ => false,
        }
    }
}

impl Default for LockManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_acquire_shared_lock() {
        let mgr = LockManager::new();
        let key = LockKey::table(1);

        mgr.acquire(1, key, LockMode::Shared).unwrap();
        mgr.acquire(2, key, LockMode::Shared).unwrap();
    }

    #[test]
    fn test_acquire_exclusive_lock() {
        let mgr = LockManager::new();
        let key = LockKey::table(1);

        mgr.acquire(1, key, LockMode::Exclusive).unwrap();
        assert!(mgr.acquire(2, key, LockMode::Exclusive).is_err());
    }

    #[test]
    fn test_release_lock() {
        let mgr = LockManager::new();
        let key = LockKey::table(1);

        mgr.acquire(1, key, LockMode::Exclusive).unwrap();
        mgr.release(1, key).unwrap();
        mgr.acquire(2, key, LockMode::Exclusive).unwrap();
    }

    #[test]
    fn test_release_all_locks() {
        let mgr = LockManager::new();

        mgr.acquire(1, LockKey::table(1), LockMode::Shared).unwrap();
        mgr.acquire(1, LockKey::table(2), LockMode::Shared).unwrap();

        mgr.release_all(1);

        mgr.acquire(2, LockKey::table(1), LockMode::Exclusive).unwrap();
    }
}
