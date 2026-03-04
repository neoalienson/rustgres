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

    fn test_aggregate(
        func: AggregateFunction,
        tuples: Vec<HashMap<String, Vec<u8>>>,
        col: Option<&str>,
        expected_key: &str,
        expected_val: Option<u8>,
    ) {
        let input = TestExecutor::new(tuples);
        let mut agg = Aggregate::new(Box::new(input), func, col.map(|s| s.to_string()));
        agg.open().unwrap();
        if let Some(val) = expected_val {
            let result = agg.next().unwrap().unwrap();
            assert_eq!(result.get(expected_key).unwrap()[0], val);
        } else {
            assert!(agg.next().unwrap().is_none());
        }
        agg.close().unwrap();
    }

    #[test]
    fn test_count_single_row() {
        test_aggregate(AggregateFunction::Count, vec![make_tuple(1)], None, "count", Some(1));
    }

    #[test]
    fn test_sum_single_value() {
        test_aggregate(
            AggregateFunction::Sum,
            vec![make_tuple(42)],
            Some("value"),
            "sum",
            Some(42),
        );
    }

    #[test]
    fn test_avg_single_value() {
        test_aggregate(
            AggregateFunction::Avg,
            vec![make_tuple(50)],
            Some("value"),
            "avg",
            Some(50),
        );
    }

    #[test]
    fn test_min_single_value() {
        test_aggregate(
            AggregateFunction::Min,
            vec![make_tuple(99)],
            Some("value"),
            "min",
            Some(99),
        );
    }

    #[test]
    fn test_max_single_value() {
        test_aggregate(AggregateFunction::Max, vec![make_tuple(1)], Some("value"), "max", Some(1));
    }

    #[test]
    fn test_sum_all_zeros() {
        test_aggregate(
            AggregateFunction::Sum,
            vec![make_tuple(0), make_tuple(0), make_tuple(0)],
            Some("value"),
            "sum",
            Some(0),
        );
    }

    #[test]
    fn test_avg_empty_returns_zero() {
        test_aggregate(AggregateFunction::Avg, vec![], Some("value"), "avg", Some(0));
    }

    #[test]
    fn test_sum_empty_returns_zero() {
        test_aggregate(AggregateFunction::Sum, vec![], Some("value"), "sum", Some(0));
    }

    #[test]
    fn test_max_empty_returns_none() {
        test_aggregate(AggregateFunction::Max, vec![], Some("value"), "max", None);
    }

    #[test]
    fn test_min_all_same_value() {
        test_aggregate(
            AggregateFunction::Min,
            vec![make_tuple(5), make_tuple(5), make_tuple(5)],
            Some("value"),
            "min",
            Some(5),
        );
    }

    #[test]
    fn test_max_all_same_value() {
        test_aggregate(
            AggregateFunction::Max,
            vec![make_tuple(7), make_tuple(7), make_tuple(7)],
            Some("value"),
            "max",
            Some(7),
        );
    }

    #[test]
    fn test_count_large_dataset() {
        let tuples: Vec<_> = (0..255).map(|i| make_tuple(i as u8)).collect();
        test_aggregate(AggregateFunction::Count, tuples, None, "count", Some(255));
    }

    #[test]
    fn test_reopen_recomputes() {
        let tuples = vec![make_tuple(10), make_tuple(20)];
        let input = TestExecutor::new(tuples);
        let mut agg =
            Aggregate::new(Box::new(input), AggregateFunction::Sum, Some("value".to_string()));

        agg.open().unwrap();
        let result1 = agg.next().unwrap().unwrap();
        assert_eq!(result1.get("sum").unwrap()[0], 30);
        assert!(agg.next().unwrap().is_none());
        agg.close().unwrap();

        agg.open().unwrap();
        let result2 = agg.next().unwrap().unwrap();
        assert_eq!(result2.get("sum").unwrap()[0], 30);
        agg.close().unwrap();
    }

    #[test]
    fn test_avg_rounds_down() {
        test_aggregate(
            AggregateFunction::Avg,
            vec![make_tuple(10), make_tuple(11)],
            Some("value"),
            "avg",
            Some(10),
        );
    }
}
