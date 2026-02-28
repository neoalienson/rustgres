#[cfg(test)]
mod tests {
    use crate::executor::{SimpleExecutor, SimpleTuple as Tuple, Intersect};
    use crate::executor::mock::MockExecutor;

    #[test]
    fn test_intersect_single_match() {
        let left = MockExecutor::new(vec![
            Tuple { data: vec![1] },
            Tuple { data: vec![2] },
            Tuple { data: vec![3] },
        ]);
        let right = MockExecutor::new(vec![Tuple { data: vec![2] }]);
        let mut intersect = Intersect::new(Box::new(left), Box::new(right));
        intersect.open().unwrap();

        let result = intersect.next().unwrap().unwrap();
        assert_eq!(result.data[0], 2);
        assert!(intersect.next().unwrap().is_none());
        intersect.close().unwrap();
    }

    #[test]
    fn test_intersect_duplicates_in_left() {
        let left = MockExecutor::new(vec![
            Tuple { data: vec![1] },
            Tuple { data: vec![1] },
            Tuple { data: vec![2] },
        ]);
        let right = MockExecutor::new(vec![Tuple { data: vec![1] }]);
        let mut intersect = Intersect::new(Box::new(left), Box::new(right));
        intersect.open().unwrap();

        let mut count = 0;
        while intersect.next().unwrap().is_some() {
            count += 1;
        }
        assert_eq!(count, 2);
        intersect.close().unwrap();
    }

    #[test]
    fn test_intersect_large_left_small_right() {
        let mut left_tuples = Vec::new();
        for i in 0..100 {
            left_tuples.push(Tuple { data: vec![(i % 10) as u8] });
        }
        let right = MockExecutor::new(vec![Tuple { data: vec![5] }]);
        let mut intersect = Intersect::new(Box::new(MockExecutor::new(left_tuples)), Box::new(right));
        intersect.open().unwrap();

        let mut count = 0;
        while intersect.next().unwrap().is_some() {
            count += 1;
        }
        assert!(count > 0);
        intersect.close().unwrap();
    }

    #[test]
    fn test_intersect_wide_tuples() {
        let left = MockExecutor::new(vec![
            Tuple { data: vec![1, 2, 3, 4, 5] },
            Tuple { data: vec![6, 7, 8, 9, 10] },
        ]);
        let right = MockExecutor::new(vec![Tuple { data: vec![1, 2, 3, 4, 5] }]);
        let mut intersect = Intersect::new(Box::new(left), Box::new(right));
        intersect.open().unwrap();

        let result = intersect.next().unwrap().unwrap();
        assert_eq!(result.data.len(), 5);
        assert!(intersect.next().unwrap().is_none());
        intersect.close().unwrap();
    }

    #[test]
    fn test_intersect_subset() {
        let left = MockExecutor::new(vec![
            Tuple { data: vec![1] },
            Tuple { data: vec![2] },
        ]);
        let right = MockExecutor::new(vec![
            Tuple { data: vec![1] },
            Tuple { data: vec![2] },
            Tuple { data: vec![3] },
        ]);
        let mut intersect = Intersect::new(Box::new(left), Box::new(right));
        intersect.open().unwrap();

        let mut count = 0;
        while intersect.next().unwrap().is_some() {
            count += 1;
        }
        assert_eq!(count, 2);
        intersect.close().unwrap();
    }

    #[test]
    fn test_intersect_superset() {
        let left = MockExecutor::new(vec![
            Tuple { data: vec![1] },
            Tuple { data: vec![2] },
            Tuple { data: vec![3] },
        ]);
        let right = MockExecutor::new(vec![
            Tuple { data: vec![1] },
            Tuple { data: vec![2] },
        ]);
        let mut intersect = Intersect::new(Box::new(left), Box::new(right));
        intersect.open().unwrap();

        let mut count = 0;
        while intersect.next().unwrap().is_some() {
            count += 1;
        }
        assert_eq!(count, 2);
        intersect.close().unwrap();
    }
}
