//! Edge case tests for Aggregate operator

#[cfg(test)]
mod tests {
    use crate::executor::{Aggregate, AggregateFunction, Executor, ExecutorError};
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

    fn make_tuple(value: u8) -> HashMap<String, Vec<u8>> {
        let mut map = HashMap::new();
        map.insert("value".to_string(), vec![value]);
        map
    }

    #[test]
    fn test_count_single_row() {
        let tuples = vec![make_tuple(1)];
        let input = TestExecutor::new(tuples);
        let mut agg = Aggregate::new(Box::new(input), AggregateFunction::Count, None);

        agg.open().unwrap();
        let result = agg.next().unwrap().unwrap();
        assert_eq!(result.get("count").unwrap()[0], 1);
        agg.close().unwrap();
    }

    #[test]
    fn test_sum_single_value() {
        let tuples = vec![make_tuple(42)];
        let input = TestExecutor::new(tuples);
        let mut agg = Aggregate::new(
            Box::new(input),
            AggregateFunction::Sum,
            Some("value".to_string()),
        );

        agg.open().unwrap();
        let result = agg.next().unwrap().unwrap();
        assert_eq!(result.get("sum").unwrap()[0], 42);
        agg.close().unwrap();
    }

    #[test]
    fn test_avg_single_value() {
        let tuples = vec![make_tuple(50)];
        let input = TestExecutor::new(tuples);
        let mut agg = Aggregate::new(
            Box::new(input),
            AggregateFunction::Avg,
            Some("value".to_string()),
        );

        agg.open().unwrap();
        let result = agg.next().unwrap().unwrap();
        assert_eq!(result.get("avg").unwrap()[0], 50);
        agg.close().unwrap();
    }

    #[test]
    fn test_min_single_value() {
        let tuples = vec![make_tuple(99)];
        let input = TestExecutor::new(tuples);
        let mut agg = Aggregate::new(
            Box::new(input),
            AggregateFunction::Min,
            Some("value".to_string()),
        );

        agg.open().unwrap();
        let result = agg.next().unwrap().unwrap();
        assert_eq!(result.get("min").unwrap()[0], 99);
        agg.close().unwrap();
    }

    #[test]
    fn test_max_single_value() {
        let tuples = vec![make_tuple(1)];
        let input = TestExecutor::new(tuples);
        let mut agg = Aggregate::new(
            Box::new(input),
            AggregateFunction::Max,
            Some("value".to_string()),
        );

        agg.open().unwrap();
        let result = agg.next().unwrap().unwrap();
        assert_eq!(result.get("max").unwrap()[0], 1);
        agg.close().unwrap();
    }

    #[test]
    fn test_sum_all_zeros() {
        let tuples = vec![make_tuple(0), make_tuple(0), make_tuple(0)];
        let input = TestExecutor::new(tuples);
        let mut agg = Aggregate::new(
            Box::new(input),
            AggregateFunction::Sum,
            Some("value".to_string()),
        );

        agg.open().unwrap();
        let result = agg.next().unwrap().unwrap();
        assert_eq!(result.get("sum").unwrap()[0], 0);
        agg.close().unwrap();
    }

    #[test]
    fn test_avg_empty_returns_zero() {
        let tuples = vec![];
        let input = TestExecutor::new(tuples);
        let mut agg = Aggregate::new(
            Box::new(input),
            AggregateFunction::Avg,
            Some("value".to_string()),
        );

        agg.open().unwrap();
        let result = agg.next().unwrap().unwrap();
        assert_eq!(result.get("avg").unwrap()[0], 0);
        agg.close().unwrap();
    }

    #[test]
    fn test_sum_empty_returns_zero() {
        let tuples = vec![];
        let input = TestExecutor::new(tuples);
        let mut agg = Aggregate::new(
            Box::new(input),
            AggregateFunction::Sum,
            Some("value".to_string()),
        );

        agg.open().unwrap();
        let result = agg.next().unwrap().unwrap();
        assert_eq!(result.get("sum").unwrap()[0], 0);
        agg.close().unwrap();
    }

    #[test]
    fn test_max_empty_returns_none() {
        let tuples = vec![];
        let input = TestExecutor::new(tuples);
        let mut agg = Aggregate::new(
            Box::new(input),
            AggregateFunction::Max,
            Some("value".to_string()),
        );

        agg.open().unwrap();
        assert!(agg.next().unwrap().is_none());
        agg.close().unwrap();
    }

    #[test]
    fn test_min_all_same_value() {
        let tuples = vec![make_tuple(5), make_tuple(5), make_tuple(5)];
        let input = TestExecutor::new(tuples);
        let mut agg = Aggregate::new(
            Box::new(input),
            AggregateFunction::Min,
            Some("value".to_string()),
        );

        agg.open().unwrap();
        let result = agg.next().unwrap().unwrap();
        assert_eq!(result.get("min").unwrap()[0], 5);
        agg.close().unwrap();
    }

    #[test]
    fn test_max_all_same_value() {
        let tuples = vec![make_tuple(7), make_tuple(7), make_tuple(7)];
        let input = TestExecutor::new(tuples);
        let mut agg = Aggregate::new(
            Box::new(input),
            AggregateFunction::Max,
            Some("value".to_string()),
        );

        agg.open().unwrap();
        let result = agg.next().unwrap().unwrap();
        assert_eq!(result.get("max").unwrap()[0], 7);
        agg.close().unwrap();
    }

    #[test]
    fn test_count_large_dataset() {
        let tuples: Vec<_> = (0..255).map(|i| make_tuple(i as u8)).collect();
        let input = TestExecutor::new(tuples);
        let mut agg = Aggregate::new(Box::new(input), AggregateFunction::Count, None);

        agg.open().unwrap();
        let result = agg.next().unwrap().unwrap();
        assert_eq!(result.get("count").unwrap()[0], 255);
        agg.close().unwrap();
    }

    #[test]
    fn test_reopen_recomputes() {
        let tuples = vec![make_tuple(10), make_tuple(20)];
        let input = TestExecutor::new(tuples);
        let mut agg = Aggregate::new(
            Box::new(input),
            AggregateFunction::Sum,
            Some("value".to_string()),
        );

        agg.open().unwrap();
        let result1 = agg.next().unwrap().unwrap();
        assert_eq!(result1.get("sum").unwrap()[0], 30);
        assert!(agg.next().unwrap().is_none());
        agg.close().unwrap();

        // Reopen should recompute
        agg.open().unwrap();
        let result2 = agg.next().unwrap().unwrap();
        assert_eq!(result2.get("sum").unwrap()[0], 30);
        agg.close().unwrap();
    }

    #[test]
    fn test_avg_rounds_down() {
        let tuples = vec![make_tuple(10), make_tuple(11)];
        let input = TestExecutor::new(tuples);
        let mut agg = Aggregate::new(
            Box::new(input),
            AggregateFunction::Avg,
            Some("value".to_string()),
        );

        agg.open().unwrap();
        let result = agg.next().unwrap().unwrap();
        // (10 + 11) / 2 = 10 (integer division)
        assert_eq!(result.get("avg").unwrap()[0], 10);
        agg.close().unwrap();
    }
}
