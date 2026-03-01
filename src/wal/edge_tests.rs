//! Edge case tests for Write-Ahead Logging

#[cfg(test)]
mod tests {
    use crate::wal::*;

    use crate::storage::PageId;

    #[test]
    fn test_flush_empty_buffer() {
        let writer = WALWriter::new();
        let lsn = writer.flush().unwrap();
        assert_eq!(lsn, 0);
    }

    #[test]
    fn test_double_flush() {
        let writer = WALWriter::new();
        writer.write(10, RecordType::Insert, Some(PageId(1)), vec![]).unwrap();
        writer.flush().unwrap();
        let lsn = writer.flush().unwrap();
        assert_eq!(lsn, 1);
    }

    #[test]
    fn test_lsn_monotonic_increase() {
        let writer = WALWriter::new();
        let mut prev_lsn = 0;
        for i in 0..100 {
            let lsn = writer.write(i, RecordType::Insert, None, vec![]).unwrap();
            assert!(lsn > prev_lsn);
            prev_lsn = lsn;
        }
    }

    #[test]
    fn test_record_with_empty_data() {
        let writer = WALWriter::new();
        let lsn = writer.write(10, RecordType::Commit, None, vec![]).unwrap();
        assert_eq!(lsn, 1);
    }

    #[test]
    fn test_record_with_large_data() {
        let writer = WALWriter::new();
        let data = vec![0u8; 1_000_000];
        let lsn = writer.write(10, RecordType::Insert, Some(PageId(1)), data).unwrap();
        assert_eq!(lsn, 1);
    }

    #[test]
    fn test_record_without_page_id() {
        let record = WALRecord::new(1, 10, RecordType::Commit, None, vec![]);
        assert!(record.page_id.is_none());
        let bytes = record.to_bytes();
        assert!(!bytes.is_empty());
    }

    #[test]
    fn test_all_record_types() {
        let writer = WALWriter::new();
        writer.write(1, RecordType::Insert, None, vec![]).unwrap();
        writer.write(2, RecordType::Update, None, vec![]).unwrap();
        writer.write(3, RecordType::Delete, None, vec![]).unwrap();
        writer.write(4, RecordType::Commit, None, vec![]).unwrap();
        writer.write(5, RecordType::Abort, None, vec![]).unwrap();
        writer.write(6, RecordType::Checkpoint, None, vec![]).unwrap();
        assert_eq!(writer.current_lsn(), 6);
    }

    #[test]
    fn test_flushed_lsn_tracking() {
        let writer = WALWriter::new();
        assert_eq!(writer.flushed_lsn(), 0);
        writer.write(10, RecordType::Insert, None, vec![]).unwrap();
        assert_eq!(writer.flushed_lsn(), 0);
        writer.flush().unwrap();
        assert_eq!(writer.flushed_lsn(), 1);
    }

    #[test]
    fn test_current_lsn_before_any_writes() {
        let writer = WALWriter::new();
        assert_eq!(writer.current_lsn(), 0);
    }

    #[test]
    fn test_multiple_flushes_with_writes() {
        let writer = WALWriter::new();
        writer.write(1, RecordType::Insert, None, vec![]).unwrap();
        writer.flush().unwrap();
        writer.write(2, RecordType::Update, None, vec![]).unwrap();
        writer.flush().unwrap();
        assert_eq!(writer.flushed_lsn(), 2);
    }

    #[test]
    fn test_record_serialization_consistency() {
        let record = WALRecord::new(42, 100, RecordType::Update, Some(PageId(5)), vec![1, 2, 3]);
        let bytes1 = record.to_bytes();
        let bytes2 = record.to_bytes();
        assert_eq!(bytes1, bytes2);
    }

    #[test]
    fn test_max_transaction_id() {
        let writer = WALWriter::new();
        let lsn = writer.write(u64::MAX, RecordType::Insert, None, vec![]).unwrap();
        assert_eq!(lsn, 1);
    }
}
