use super::manager::TransactionId;

/// Snapshot for snapshot isolation
#[derive(Debug, Clone)]
pub struct Snapshot {
    pub xmin: TransactionId,  // Oldest active transaction
    pub xmax: TransactionId,  // Next transaction ID
    pub active: Vec<TransactionId>,  // In-progress transactions
}

impl Snapshot {
    /// Creates a new snapshot
    pub fn new(xmin: TransactionId, xmax: TransactionId, active: Vec<TransactionId>) -> Self {
        Self { xmin, xmax, active }
    }
    
    /// Checks if a transaction is visible in this snapshot
    pub fn is_visible(&self, xid: TransactionId) -> bool {
        // Transaction created after snapshot
        if xid >= self.xmax {
            return false;
        }
        
        // Transaction still in progress
        if self.active.contains(&xid) {
            return false;
        }
        
        // Transaction committed before snapshot
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_snapshot_visibility() {
        let snapshot = Snapshot::new(10, 20, vec![12, 15]);
        
        // Committed before snapshot
        assert!(snapshot.is_visible(5));
        assert!(snapshot.is_visible(11));
        
        // In progress
        assert!(!snapshot.is_visible(12));
        assert!(!snapshot.is_visible(15));
        
        // Created after snapshot
        assert!(!snapshot.is_visible(20));
        assert!(!snapshot.is_visible(25));
    }
}
