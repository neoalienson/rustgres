#[cfg(test)]
mod tests {
    use crate::executor::window::{Window, WindowFunction};
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
    fn test_row_number_zero_rows() {
        let mock = Box::new(MockExecutor::new(vec![]));
        let mut window = Window::new(mock, WindowFunction::RowNumber);

        window.open().unwrap();
        assert!(window.next().unwrap().is_none());
        window.close().unwrap();
    }

    #[test]
    fn test_row_number_one_row() {
        let tuples = vec![SimpleTuple { data: vec![1] }];
        let mock = Box::new(MockExecutor::new(tuples));
        let mut window = Window::new(mock, WindowFunction::RowNumber);

        window.open().unwrap();
        let r = window.next().unwrap().unwrap();
        assert_eq!(&r.data[r.data.len() - 8..], &1i64.to_le_bytes());
        assert!(window.next().unwrap().is_none());
        window.close().unwrap();
    }

    #[test]
    fn test_row_number_many_rows() {
        let tuples: Vec<SimpleTuple> = (0..1000)
            .map(|i| SimpleTuple { data: vec![i as u8] })
            .collect();
        let mock = Box::new(MockExecutor::new(tuples));
        let mut window = Window::new(mock, WindowFunction::RowNumber);

        window.open().unwrap();
        for i in 1..=1000 {
            let r = window.next().unwrap().unwrap();
            assert_eq!(&r.data[r.data.len() - 8..], &(i as i64).to_le_bytes());
        }
        window.close().unwrap();
    }

    #[test]
    fn test_rank_empty() {
        let mock = Box::new(MockExecutor::new(vec![]));
        let mut window = Window::new(mock, WindowFunction::Rank);

        window.open().unwrap();
        assert!(window.next().unwrap().is_none());
        window.close().unwrap();
    }

    #[test]
    fn test_rank_single() {
        let tuples = vec![SimpleTuple { data: vec![10] }];
        let mock = Box::new(MockExecutor::new(tuples));
        let mut window = Window::new(mock, WindowFunction::Rank);

        window.open().unwrap();
        let r = window.next().unwrap().unwrap();
        assert_eq!(&r.data[r.data.len() - 8..], &1i64.to_le_bytes());
        window.close().unwrap();
    }

    #[test]
    fn test_dense_rank_empty() {
        let mock = Box::new(MockExecutor::new(vec![]));
        let mut window = Window::new(mock, WindowFunction::DenseRank);

        window.open().unwrap();
        assert!(window.next().unwrap().is_none());
        window.close().unwrap();
    }

    #[test]
    fn test_dense_rank_multiple() {
        let tuples = vec![
            SimpleTuple { data: vec![1] },
            SimpleTuple { data: vec![2] },
            SimpleTuple { data: vec![3] },
        ];
        let mock = Box::new(MockExecutor::new(tuples));
        let mut window = Window::new(mock, WindowFunction::DenseRank);

        window.open().unwrap();
        for i in 1..=3 {
            let r = window.next().unwrap().unwrap();
            assert_eq!(&r.data[r.data.len() - 8..], &(i as i64).to_le_bytes());
        }
        window.close().unwrap();
    }

    #[test]
    fn test_window_reopen_multiple_times() {
        let tuples = vec![SimpleTuple { data: vec![1] }];
        let mock = Box::new(MockExecutor::new(tuples));
        let mut window = Window::new(mock, WindowFunction::RowNumber);

        for _ in 0..5 {
            window.open().unwrap();
            let r = window.next().unwrap().unwrap();
            assert_eq!(&r.data[r.data.len() - 8..], &1i64.to_le_bytes());
            window.close().unwrap();
        }
    }

    #[test]
    fn test_window_large_data_values() {
        let tuples = vec![SimpleTuple {
            data: vec![255; 1000],
        }];
        let mock = Box::new(MockExecutor::new(tuples));
        let mut window = Window::new(mock, WindowFunction::RowNumber);

        window.open().unwrap();
        let r = window.next().unwrap().unwrap();
        assert_eq!(r.data.len(), 1008);
        window.close().unwrap();
    }

    #[test]
    fn test_window_empty_data() {
        let tuples = vec![SimpleTuple { data: vec![] }];
        let mock = Box::new(MockExecutor::new(tuples));
        let mut window = Window::new(mock, WindowFunction::RowNumber);

        window.open().unwrap();
        let r = window.next().unwrap().unwrap();
        assert_eq!(&r.data[r.data.len() - 8..], &1i64.to_le_bytes());
        window.close().unwrap();
    }

    #[test]
    fn test_window_all_functions_same_input() {
        let tuples = vec![SimpleTuple { data: vec![1] }, SimpleTuple { data: vec![2] }];

        for func in [
            WindowFunction::RowNumber,
            WindowFunction::Rank,
            WindowFunction::DenseRank,
        ] {
            let mock = Box::new(MockExecutor::new(tuples.clone()));
            let mut window = Window::new(mock, func);

            window.open().unwrap();
            let r1 = window.next().unwrap().unwrap();
            assert_eq!(&r1.data[r1.data.len() - 8..], &1i64.to_le_bytes());
            let r2 = window.next().unwrap().unwrap();
            assert_eq!(&r2.data[r2.data.len() - 8..], &2i64.to_le_bytes());
            window.close().unwrap();
        }
    }

    #[test]
    fn test_window_max_rows() {
        let tuples: Vec<SimpleTuple> = (0..10000)
            .map(|_| SimpleTuple { data: vec![1] })
            .collect();
        let mock = Box::new(MockExecutor::new(tuples));
        let mut window = Window::new(mock, WindowFunction::RowNumber);

        window.open().unwrap();
        let mut count = 0;
        while window.next().unwrap().is_some() {
            count += 1;
        }
        assert_eq!(count, 10000);
        window.close().unwrap();
    }

    #[test]
    fn test_window_partial_read() {
        let tuples = vec![
            SimpleTuple { data: vec![1] },
            SimpleTuple { data: vec![2] },
            SimpleTuple { data: vec![3] },
        ];
        let mock = Box::new(MockExecutor::new(tuples));
        let mut window = Window::new(mock, WindowFunction::RowNumber);

        window.open().unwrap();
        window.next().unwrap();
        window.close().unwrap();

        window.open().unwrap();
        let r1 = window.next().unwrap().unwrap();
        assert_eq!(&r1.data[r1.data.len() - 8..], &1i64.to_le_bytes());
        window.close().unwrap();
    }
}
