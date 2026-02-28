//! Edge case tests for storage layer

#[cfg(test)]
mod tests {
    use crate::storage::*;

    #[test]
    fn test_fetch_same_page_multiple_times() {
        let pool = BufferPool::new(10);
        let page_id = PageId(1);
        pool.fetch(page_id).unwrap();
        pool.fetch(page_id).unwrap();
        pool.fetch(page_id).unwrap();
        assert_eq!(pool.size(), 1);
    }

    #[test]
    fn test_unpin_nonexistent_page() {
        let pool = BufferPool::new(10);
        let page_id = PageId(999);
        assert!(pool.unpin(page_id, false).is_err());
    }

    #[test]
    fn test_buffer_pool_pin_count() {
        let pool = BufferPool::new(10);
        let page_id = PageId(1);
        // Fetch multiple times increases pin count
        pool.fetch(page_id).unwrap();
        pool.fetch(page_id).unwrap();
        pool.fetch(page_id).unwrap();
        // Need to unpin same number of times
        pool.unpin(page_id, false).unwrap();
        pool.unpin(page_id, false).unwrap();
        pool.unpin(page_id, false).unwrap();
    }

    #[test]
    fn test_eviction_with_dirty_pages() {
        let pool = BufferPool::new(2);
        pool.fetch(PageId(1)).unwrap();
        pool.unpin(PageId(1), true).unwrap();
        pool.fetch(PageId(2)).unwrap();
        pool.unpin(PageId(2), false).unwrap();
        pool.fetch(PageId(3)).unwrap();
        assert_eq!(pool.size(), 2);
    }

    #[test]
    fn test_multiple_unpin_same_page() {
        let pool = BufferPool::new(10);
        let page_id = PageId(1);
        pool.fetch(page_id).unwrap();
        pool.unpin(page_id, false).unwrap();
        pool.unpin(page_id, false).unwrap();
        pool.unpin(page_id, false).unwrap();
    }

    #[test]
    fn test_fetch_after_eviction() {
        let pool = BufferPool::new(2);
        pool.fetch(PageId(1)).unwrap();
        pool.unpin(PageId(1), false).unwrap();
        pool.fetch(PageId(2)).unwrap();
        pool.unpin(PageId(2), false).unwrap();
        pool.fetch(PageId(3)).unwrap();
        pool.unpin(PageId(3), false).unwrap();
        // Re-fetch evicted page
        pool.fetch(PageId(1)).unwrap();
        assert_eq!(pool.size(), 2);
    }

    #[test]
    fn test_buffer_pool_size_one() {
        let pool = BufferPool::new(1);
        pool.fetch(PageId(1)).unwrap();
        pool.unpin(PageId(1), false).unwrap();
        pool.fetch(PageId(2)).unwrap();
        assert_eq!(pool.size(), 1);
    }

    #[test]
    fn test_flush_empty_pool() {
        let pool = BufferPool::new(10);
        assert!(pool.flush_all().is_ok());
    }

    #[test]
    fn test_flush_with_no_dirty_pages() {
        let pool = BufferPool::new(10);
        pool.fetch(PageId(1)).unwrap();
        pool.unpin(PageId(1), false).unwrap();
        assert!(pool.flush_all().is_ok());
    }

    #[test]
    fn test_large_page_id() {
        let pool = BufferPool::new(10);
        let page_id = PageId(u32::MAX);
        pool.fetch(page_id).unwrap();
        assert_eq!(pool.size(), 1);
    }

    #[test]
    fn test_zero_page_id() {
        let pool = BufferPool::new(10);
        let page_id = PageId(0);
        pool.fetch(page_id).unwrap();
        assert_eq!(pool.size(), 1);
    }

    #[test]
    fn test_lru_eviction_order() {
        let pool = BufferPool::new(3);
        pool.fetch(PageId(1)).unwrap();
        pool.unpin(PageId(1), false).unwrap();
        pool.fetch(PageId(2)).unwrap();
        pool.unpin(PageId(2), false).unwrap();
        pool.fetch(PageId(3)).unwrap();
        pool.unpin(PageId(3), false).unwrap();
        // Access page 1 again (should move to end of LRU)
        pool.fetch(PageId(1)).unwrap();
        pool.unpin(PageId(1), false).unwrap();
        // Page 2 should be evicted first
        pool.fetch(PageId(4)).unwrap();
        assert_eq!(pool.size(), 3);
    }
}
