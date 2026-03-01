use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

pub struct PartitionStrategy {
    num_partitions: usize,
}

impl PartitionStrategy {
    pub fn new(num_partitions: usize) -> Self {
        let num_partitions = num_partitions.next_power_of_two();
        Self { num_partitions }
    }

    pub fn partition_key(&self, key: &[u8]) -> usize {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        let hash = hasher.finish();
        (hash as usize) & (self.num_partitions - 1)
    }

    pub fn num_partitions(&self) -> usize {
        self.num_partitions
    }

    pub fn optimal_partitions(num_workers: usize) -> usize {
        (num_workers * 2).next_power_of_two()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_partition_strategy() {
        let strategy = PartitionStrategy::new(4);
        assert_eq!(strategy.num_partitions(), 4);

        let key = b"test_key";
        let partition = strategy.partition_key(key);
        assert!(partition < 4);
    }

    #[test]
    fn test_power_of_two() {
        let strategy = PartitionStrategy::new(5);
        assert_eq!(strategy.num_partitions(), 8);
    }

    #[test]
    fn test_optimal_partitions() {
        assert_eq!(PartitionStrategy::optimal_partitions(4), 8);
        assert_eq!(PartitionStrategy::optimal_partitions(8), 16);
    }

    #[test]
    fn test_consistent_hashing() {
        let strategy = PartitionStrategy::new(4);
        let key = b"consistent";
        let p1 = strategy.partition_key(key);
        let p2 = strategy.partition_key(key);
        assert_eq!(p1, p2);
    }
}
