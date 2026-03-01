//! Edge case tests for LIMIT/OFFSET operator

#[cfg(test)]
mod tests {
    use crate::executor::{Executor, ExecutorError, Limit};
    use std::collections::HashMap;

    struct TestExecutor {
        tuples: Vec<HashMap<String, Vec<u8>>>,
        index: usize,
    }

    impl TestExecutor {
        fn new(tuples: Vec<HashMap<String, Vec<u8>>>) -> Self {
            Self { tuples, index: 0 }
        }
    }

    impl Executor for TestExecutor {
        fn open(&mut self) -> Result<(), ExecutorError> {
            self.index = 0;
            Ok(())
        }

        fn next(&mut self) -> Result<Option<HashMap<String, Vec<u8>>>, ExecutorError> {
            if self.index < self.tuples.len() {
                let tuple = self.tuples[self.index].clone();
                self.index += 1;
                Ok(Some(tuple))
            } else {
                Ok(None)
            }
        }

        fn close(&mut self) -> Result<(), ExecutorError> {
            Ok(())
        }
    }

    fn make_tuple(id: u8) -> HashMap<String, Vec<u8>> {
        let mut map = HashMap::new();
        map.insert("id".to_string(), vec![id]);
        map
    }

    #[test]
    fn test_limit_larger_than_data() {
        let tuples = vec![make_tuple(1), make_tuple(2)];
        let mock = TestExecutor::new(tuples);
        let mut limit = Limit::new(Box::new(mock), Some(100), None);

        limit.open().unwrap();
        assert!(limit.next().unwrap().is_some());
        assert!(limit.next().unwrap().is_some());
        assert!(limit.next().unwrap().is_none());
        limit.close().unwrap();
    }

    #[test]
    fn test_offset_equals_data_size() {
        let tuples = vec![make_tuple(1), make_tuple(2), make_tuple(3)];
        let mock = TestExecutor::new(tuples);
        let mut limit = Limit::new(Box::new(mock), None, Some(3));

        limit.open().unwrap();
        assert!(limit.next().unwrap().is_none());
        limit.close().unwrap();
    }

    #[test]
    fn test_limit_one() {
        let tuples = vec![make_tuple(1), make_tuple(2), make_tuple(3)];
        let mock = TestExecutor::new(tuples);
        let mut limit = Limit::new(Box::new(mock), Some(1), None);

        limit.open().unwrap();
        let t = limit.next().unwrap().unwrap();
        assert_eq!(t.get("id").unwrap(), &vec![1]);
        assert!(limit.next().unwrap().is_none());
        limit.close().unwrap();
    }

    #[test]
    fn test_offset_one() {
        let tuples = vec![make_tuple(1), make_tuple(2)];
        let mock = TestExecutor::new(tuples);
        let mut limit = Limit::new(Box::new(mock), None, Some(1));

        limit.open().unwrap();
        let t = limit.next().unwrap().unwrap();
        assert_eq!(t.get("id").unwrap(), &vec![2]);
        assert!(limit.next().unwrap().is_none());
        limit.close().unwrap();
    }

    #[test]
    fn test_limit_offset_both_one() {
        let tuples = vec![make_tuple(1), make_tuple(2), make_tuple(3)];
        let mock = TestExecutor::new(tuples);
        let mut limit = Limit::new(Box::new(mock), Some(1), Some(1));

        limit.open().unwrap();
        let t = limit.next().unwrap().unwrap();
        assert_eq!(t.get("id").unwrap(), &vec![2]);
        assert!(limit.next().unwrap().is_none());
        limit.close().unwrap();
    }

    #[test]
    fn test_empty_input_with_limit() {
        let tuples = vec![];
        let mock = TestExecutor::new(tuples);
        let mut limit = Limit::new(Box::new(mock), Some(10), None);

        limit.open().unwrap();
        assert!(limit.next().unwrap().is_none());
        limit.close().unwrap();
    }

    #[test]
    fn test_empty_input_with_offset() {
        let tuples = vec![];
        let mock = TestExecutor::new(tuples);
        let mut limit = Limit::new(Box::new(mock), None, Some(5));

        limit.open().unwrap();
        assert!(limit.next().unwrap().is_none());
        limit.close().unwrap();
    }

    #[test]
    fn test_large_offset() {
        let tuples = vec![make_tuple(1), make_tuple(2)];
        let mock = TestExecutor::new(tuples);
        let mut limit = Limit::new(Box::new(mock), None, Some(1000));

        limit.open().unwrap();
        assert!(limit.next().unwrap().is_none());
        limit.close().unwrap();
    }

    #[test]
    fn test_limit_offset_sum_equals_data() {
        let tuples = vec![make_tuple(1), make_tuple(2), make_tuple(3), make_tuple(4)];
        let mock = TestExecutor::new(tuples);
        let mut limit = Limit::new(Box::new(mock), Some(2), Some(2));

        limit.open().unwrap();
        let t1 = limit.next().unwrap().unwrap();
        assert_eq!(t1.get("id").unwrap(), &vec![3]);
        let t2 = limit.next().unwrap().unwrap();
        assert_eq!(t2.get("id").unwrap(), &vec![4]);
        assert!(limit.next().unwrap().is_none());
        limit.close().unwrap();
    }

    #[test]
    fn test_limit_offset_sum_exceeds_data() {
        let tuples = vec![make_tuple(1), make_tuple(2), make_tuple(3)];
        let mock = TestExecutor::new(tuples);
        let mut limit = Limit::new(Box::new(mock), Some(5), Some(2));

        limit.open().unwrap();
        let t = limit.next().unwrap().unwrap();
        assert_eq!(t.get("id").unwrap(), &vec![3]);
        assert!(limit.next().unwrap().is_none());
        limit.close().unwrap();
    }

    #[test]
    fn test_reopen_resets_state() {
        let tuples = vec![make_tuple(1), make_tuple(2), make_tuple(3)];
        let mock = TestExecutor::new(tuples);
        let mut limit = Limit::new(Box::new(mock), Some(2), None);

        limit.open().unwrap();
        assert!(limit.next().unwrap().is_some());
        assert!(limit.next().unwrap().is_some());
        assert!(limit.next().unwrap().is_none());
        limit.close().unwrap();

        // Reopen should reset
        limit.open().unwrap();
        assert!(limit.next().unwrap().is_some());
        assert!(limit.next().unwrap().is_some());
        assert!(limit.next().unwrap().is_none());
        limit.close().unwrap();
    }
}
