use crate::catalog::Value;
use crate::storage::error::{Result, StorageError};
use dashmap::DashMap;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

pub type Key = Vec<u8>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TupleId {
    pub page_id: u32,
    pub slot: u16,
}

pub struct HashIndex {
    buckets: Arc<DashMap<u64, Vec<(Key, TupleId)>>>,
    num_buckets: usize,
}

impl HashIndex {
    pub fn new() -> Self {
        Self::with_buckets(1024)
    }

    pub fn with_buckets(num_buckets: usize) -> Self {
        Self { buckets: Arc::new(DashMap::new()), num_buckets }
    }

    fn hash_key(&self, key: &[u8]) -> u64 {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        hasher.finish() % self.num_buckets as u64
    }

    pub fn insert(&self, key: Key, tuple_id: TupleId) -> Result<()> {
        let hash = self.hash_key(&key);
        self.buckets.entry(hash).or_insert_with(Vec::new).push((key, tuple_id));
        Ok(())
    }

    pub fn get(&self, key: &[u8]) -> Option<Vec<TupleId>> {
        let hash = self.hash_key(key);
        self.buckets.get(&hash).map(|bucket| {
            bucket.iter().filter(|(k, _)| k.as_slice() == key).map(|(_, tid)| *tid).collect()
        })
    }

    pub fn delete(&self, key: &[u8]) -> Result<bool> {
        let hash = self.hash_key(key);
        if let Some(mut bucket) = self.buckets.get_mut(&hash) {
            let original_len = bucket.len();
            bucket.retain(|(k, _)| k.as_slice() != key);
            let deleted = bucket.len() < original_len;
            if bucket.is_empty() {
                drop(bucket);
                self.buckets.remove(&hash);
            }
            Ok(deleted)
        } else {
            Ok(false)
        }
    }

    pub fn clear(&self) {
        self.buckets.clear();
    }

    pub fn len(&self) -> usize {
        self.buckets.iter().map(|entry| entry.value().len()).sum()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl Default for HashIndex {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hash_index_insert_and_get() {
        let index = HashIndex::new();
        let key = vec![1, 2, 3];
        let tuple_id = TupleId { page_id: 1, slot: 0 };

        index.insert(key.clone(), tuple_id).unwrap();
        let result = index.get(&key).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], tuple_id);
    }

    #[test]
    fn test_hash_index_get_nonexistent() {
        let index = HashIndex::new();
        let key = vec![1, 2, 3];
        assert!(index.get(&key).is_none());
    }

    #[test]
    fn test_hash_index_multiple_values() {
        let index = HashIndex::new();
        let key = vec![1, 2, 3];
        let tid1 = TupleId { page_id: 1, slot: 0 };
        let tid2 = TupleId { page_id: 1, slot: 1 };

        index.insert(key.clone(), tid1).unwrap();
        index.insert(key.clone(), tid2).unwrap();

        let result = index.get(&key).unwrap();
        assert_eq!(result.len(), 2);
        assert!(result.contains(&tid1));
        assert!(result.contains(&tid2));
    }

    #[test]
    fn test_hash_index_delete() {
        let index = HashIndex::new();
        let key = vec![1, 2, 3];
        let tuple_id = TupleId { page_id: 1, slot: 0 };

        index.insert(key.clone(), tuple_id).unwrap();
        assert!(index.delete(&key).unwrap());
        assert!(index.get(&key).is_none());
    }

    #[test]
    fn test_hash_index_delete_nonexistent() {
        let index = HashIndex::new();
        let key = vec![1, 2, 3];
        assert!(!index.delete(&key).unwrap());
    }

    #[test]
    fn test_hash_index_collision_handling() {
        let index = HashIndex::with_buckets(1);
        let key1 = vec![1, 2, 3];
        let key2 = vec![4, 5, 6];
        let tid1 = TupleId { page_id: 1, slot: 0 };
        let tid2 = TupleId { page_id: 2, slot: 0 };

        index.insert(key1.clone(), tid1).unwrap();
        index.insert(key2.clone(), tid2).unwrap();

        let result1 = index.get(&key1).unwrap();
        let result2 = index.get(&key2).unwrap();
        assert_eq!(result1[0], tid1);
        assert_eq!(result2[0], tid2);
    }

    #[test]
    fn test_hash_index_clear() {
        let index = HashIndex::new();
        let key = vec![1, 2, 3];
        let tuple_id = TupleId { page_id: 1, slot: 0 };

        index.insert(key.clone(), tuple_id).unwrap();
        assert!(!index.is_empty());

        index.clear();
        assert!(index.is_empty());
        assert!(index.get(&key).is_none());
    }

    #[test]
    fn test_hash_index_len() {
        let index = HashIndex::new();
        assert_eq!(index.len(), 0);

        index.insert(vec![1], TupleId { page_id: 1, slot: 0 }).unwrap();
        assert_eq!(index.len(), 1);

        index.insert(vec![2], TupleId { page_id: 1, slot: 1 }).unwrap();
        assert_eq!(index.len(), 2);

        index.delete(&[1]).unwrap();
        assert_eq!(index.len(), 1);
    }
}
