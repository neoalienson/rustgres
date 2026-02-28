//! Edge case tests for transaction management

#[cfg(test)]
mod tests {
    use crate::transaction::*;
    use crate::transaction::manager::FIRST_NORMAL_XID;

    #[test]
    fn test_commit_nonexistent_transaction() {
        let mgr = TransactionManager::new();
        assert!(mgr.commit(999).is_err());
    }

    #[test]
    fn test_abort_nonexistent_transaction() {
        let mgr = TransactionManager::new();
        assert!(mgr.abort(999).is_err());
    }

    #[test]
    fn test_double_commit() {
        let mgr = TransactionManager::new();
        let txn = mgr.begin();
        mgr.commit(txn.xid).unwrap();
        assert!(mgr.commit(txn.xid).is_err());
    }

    #[test]
    fn test_commit_after_abort() {
        let mgr = TransactionManager::new();
        let txn = mgr.begin();
        mgr.abort(txn.xid).unwrap();
        assert!(mgr.commit(txn.xid).is_err());
    }

    #[test]
    fn test_abort_after_commit() {
        let mgr = TransactionManager::new();
        let txn = mgr.begin();
        mgr.commit(txn.xid).unwrap();
        mgr.abort(txn.xid).unwrap();
    }

    #[test]
    fn test_snapshot_with_no_active_transactions() {
        let mgr = TransactionManager::new();
        let snapshot = mgr.get_snapshot();
        assert_eq!(snapshot.active.len(), 0);
    }

    #[test]
    fn test_snapshot_after_all_committed() {
        let mgr = TransactionManager::new();
        let txn1 = mgr.begin();
        let txn2 = mgr.begin();
        mgr.commit(txn1.xid).unwrap();
        mgr.commit(txn2.xid).unwrap();
        let snapshot = mgr.get_snapshot();
        assert_eq!(snapshot.active.len(), 0);
    }

    #[test]
    fn test_many_concurrent_transactions() {
        let mgr = TransactionManager::new();
        let mut txns = vec![];
        for _ in 0..100 {
            txns.push(mgr.begin());
        }
        let snapshot = mgr.get_snapshot();
        assert_eq!(snapshot.active.len(), 100);
    }

    #[test]
    fn test_transaction_id_overflow_safety() {
        let mgr = TransactionManager::new();
        for _ in 0..1000 {
            let txn = mgr.begin();
            assert!(txn.xid >= FIRST_NORMAL_XID);
        }
    }

    #[test]
    fn test_is_committed_nonexistent() {
        let mgr = TransactionManager::new();
        assert!(!mgr.is_committed(999));
    }

    #[test]
    fn test_is_in_progress_after_commit() {
        let mgr = TransactionManager::new();
        let txn = mgr.begin();
        mgr.commit(txn.xid).unwrap();
        assert!(!mgr.is_in_progress(txn.xid));
    }

    #[test]
    fn test_snapshot_xmin_with_gaps() {
        let mgr = TransactionManager::new();
        let txn1 = mgr.begin();
        let txn2 = mgr.begin();
        let txn3 = mgr.begin();
        mgr.commit(txn2.xid).unwrap();
        let snapshot = mgr.get_snapshot();
        assert_eq!(snapshot.xmin, txn1.xid);
    }
}
