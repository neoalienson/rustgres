#[cfg(test)]
mod tests {
    use crate::executor::merge_join::MergeJoin;
    use crate::executor::{SimpleExecutor, SimpleTuple, ExecutorError};

    struct MockExecutor {
        tuples: Vec<SimpleTuple>,
        position: usize,
    }

    impl MockExecutor {
        fn new(tuples: Vec<SimpleTuple>) -> Self {
            Self { tuples, position: 0 }
        }
    }

    impl SimpleExecutor for MockExecutor {
        fn open(&mut self) -> Result<(), ExecutorError> {
            self.position = 0;
            Ok(())
        }

        fn next(&mut self) -> Result<Option<SimpleTuple>, ExecutorError> {
            if self.position < self.tuples.len() {
                let tuple = self.tuples[self.position].clone();
                self.position += 1;
                Ok(Some(tuple))
            } else {
                Ok(None)
            }
        }

        fn close(&mut self) -> Result<(), ExecutorError> {
            Ok(())
        }
    }

    #[test]
    fn test_merge_join_both_empty() {
        let mut join = MergeJoin::new(
            Box::new(MockExecutor::new(vec![])),
            Box::new(MockExecutor::new(vec![])),
        );

        join.open().unwrap();
        assert!(join.next().unwrap().is_none());
        join.close().unwrap();
    }

    #[test]
    fn test_merge_join_large_left() {
        let left: Vec<SimpleTuple> = (0..100).map(|i| SimpleTuple { data: vec![i] }).collect();
        let right = vec![SimpleTuple { data: vec![50] }];
        
        let mut join = MergeJoin::new(
            Box::new(MockExecutor::new(left)),
            Box::new(MockExecutor::new(right)),
        );

        join.open().unwrap();
        assert_eq!(join.next().unwrap().unwrap().data, vec![50, 50]);
        assert!(join.next().unwrap().is_none());
        join.close().unwrap();
    }

    #[test]
    fn test_merge_join_large_right() {
        let left = vec![SimpleTuple { data: vec![50] }];
        let right: Vec<SimpleTuple> = (0..100).map(|i| SimpleTuple { data: vec![i] }).collect();
        
        let mut join = MergeJoin::new(
            Box::new(MockExecutor::new(left)),
            Box::new(MockExecutor::new(right)),
        );

        join.open().unwrap();
        assert_eq!(join.next().unwrap().unwrap().data, vec![50, 50]);
        assert!(join.next().unwrap().is_none());
        join.close().unwrap();
    }

    #[test]
    fn test_merge_join_many_duplicates_left() {
        let left = vec![SimpleTuple { data: vec![1] }; 10];
        let right = vec![SimpleTuple { data: vec![1] }];
        
        let mut join = MergeJoin::new(
            Box::new(MockExecutor::new(left)),
            Box::new(MockExecutor::new(right)),
        );

        join.open().unwrap();
        let mut count = 0;
        while join.next().unwrap().is_some() {
            count += 1;
        }
        assert_eq!(count, 10);
        join.close().unwrap();
    }

    #[test]
    fn test_merge_join_many_duplicates_right() {
        let left = vec![SimpleTuple { data: vec![1] }];
        let right = vec![SimpleTuple { data: vec![1] }; 10];
        
        let mut join = MergeJoin::new(
            Box::new(MockExecutor::new(left)),
            Box::new(MockExecutor::new(right)),
        );

        join.open().unwrap();
        let mut count = 0;
        while join.next().unwrap().is_some() {
            count += 1;
        }
        assert_eq!(count, 10);
        join.close().unwrap();
    }

    #[test]
    fn test_merge_join_many_duplicates_both() {
        let left = vec![SimpleTuple { data: vec![1] }; 5];
        let right = vec![SimpleTuple { data: vec![1] }; 5];
        
        let mut join = MergeJoin::new(
            Box::new(MockExecutor::new(left)),
            Box::new(MockExecutor::new(right)),
        );

        join.open().unwrap();
        let mut count = 0;
        while join.next().unwrap().is_some() {
            count += 1;
        }
        assert_eq!(count, 25);
        join.close().unwrap();
    }

    #[test]
    fn test_merge_join_all_left_smaller() {
        let left = vec![SimpleTuple { data: vec![1] }, SimpleTuple { data: vec![2] }];
        let right = vec![SimpleTuple { data: vec![3] }, SimpleTuple { data: vec![4] }];
        
        let mut join = MergeJoin::new(
            Box::new(MockExecutor::new(left)),
            Box::new(MockExecutor::new(right)),
        );

        join.open().unwrap();
        assert!(join.next().unwrap().is_none());
        join.close().unwrap();
    }

    #[test]
    fn test_merge_join_all_right_smaller() {
        let left = vec![SimpleTuple { data: vec![3] }, SimpleTuple { data: vec![4] }];
        let right = vec![SimpleTuple { data: vec![1] }, SimpleTuple { data: vec![2] }];
        
        let mut join = MergeJoin::new(
            Box::new(MockExecutor::new(left)),
            Box::new(MockExecutor::new(right)),
        );

        join.open().unwrap();
        assert!(join.next().unwrap().is_none());
        join.close().unwrap();
    }

    #[test]
    fn test_merge_join_interleaved() {
        let left = vec![
            SimpleTuple { data: vec![1] },
            SimpleTuple { data: vec![3] },
            SimpleTuple { data: vec![5] },
        ];
        let right = vec![
            SimpleTuple { data: vec![2] },
            SimpleTuple { data: vec![3] },
            SimpleTuple { data: vec![4] },
        ];
        
        let mut join = MergeJoin::new(
            Box::new(MockExecutor::new(left)),
            Box::new(MockExecutor::new(right)),
        );

        join.open().unwrap();
        assert_eq!(join.next().unwrap().unwrap().data, vec![3, 3]);
        assert!(join.next().unwrap().is_none());
        join.close().unwrap();
    }

    #[test]
    fn test_merge_join_multiple_matches() {
        let left = vec![
            SimpleTuple { data: vec![1] },
            SimpleTuple { data: vec![2] },
            SimpleTuple { data: vec![2] },
            SimpleTuple { data: vec![3] },
        ];
        let right = vec![
            SimpleTuple { data: vec![2] },
            SimpleTuple { data: vec![2] },
            SimpleTuple { data: vec![3] },
        ];
        
        let mut join = MergeJoin::new(
            Box::new(MockExecutor::new(left)),
            Box::new(MockExecutor::new(right)),
        );

        join.open().unwrap();
        let mut count = 0;
        while join.next().unwrap().is_some() {
            count += 1;
        }
        assert_eq!(count, 5);
        join.close().unwrap();
    }

    #[test]
    fn test_merge_join_single_left_many_right() {
        let left = vec![SimpleTuple { data: vec![5] }];
        let right = vec![
            SimpleTuple { data: vec![5] },
            SimpleTuple { data: vec![5] },
            SimpleTuple { data: vec![5] },
        ];
        
        let mut join = MergeJoin::new(
            Box::new(MockExecutor::new(left)),
            Box::new(MockExecutor::new(right)),
        );

        join.open().unwrap();
        let mut count = 0;
        while join.next().unwrap().is_some() {
            count += 1;
        }
        assert_eq!(count, 3);
        join.close().unwrap();
    }

    #[test]
    fn test_merge_join_large_dataset() {
        let left: Vec<SimpleTuple> = (0..100).map(|i| SimpleTuple { data: vec![i] }).collect();
        let right: Vec<SimpleTuple> = (0..100).map(|i| SimpleTuple { data: vec![i] }).collect();
        
        let mut join = MergeJoin::new(
            Box::new(MockExecutor::new(left)),
            Box::new(MockExecutor::new(right)),
        );

        join.open().unwrap();
        let mut count = 0;
        while join.next().unwrap().is_some() {
            count += 1;
        }
        assert_eq!(count, 100);
        join.close().unwrap();
    }

    #[test]
    fn test_merge_join_empty_data() {
        let left = vec![SimpleTuple { data: vec![] }];
        let right = vec![SimpleTuple { data: vec![] }];
        
        let mut join = MergeJoin::new(
            Box::new(MockExecutor::new(left)),
            Box::new(MockExecutor::new(right)),
        );

        join.open().unwrap();
        assert!(join.next().unwrap().is_some());
        join.close().unwrap();
    }
}
