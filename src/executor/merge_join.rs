use crate::executor::{ExecutorError, SimpleExecutor, SimpleTuple};

pub struct MergeJoin {
    left: Box<dyn SimpleExecutor>,
    right: Box<dyn SimpleExecutor>,
    left_current: Option<SimpleTuple>,
    right_current: Option<SimpleTuple>,
    right_buffer: Vec<SimpleTuple>,
    buffer_position: usize,
    left_value: Option<Vec<u8>>,
    finished: bool,
}

impl MergeJoin {
    pub fn new(left: Box<dyn SimpleExecutor>, right: Box<dyn SimpleExecutor>) -> Self {
        Self {
            left,
            right,
            left_current: None,
            right_current: None,
            right_buffer: Vec::new(),
            buffer_position: 0,
            left_value: None,
            finished: false,
        }
    }

    fn compare_tuples(left: &SimpleTuple, right: &SimpleTuple) -> std::cmp::Ordering {
        left.data.cmp(&right.data)
    }
}

impl SimpleExecutor for MergeJoin {
    fn open(&mut self) -> Result<(), ExecutorError> {
        self.left.open()?;
        self.right.open()?;
        self.left_current = self.left.next()?;
        self.right_current = self.right.next()?;
        self.finished = false;
        Ok(())
    }

    fn next(&mut self) -> Result<Option<SimpleTuple>, ExecutorError> {
        if self.finished {
            return Ok(None);
        }

        loop {
            if self.buffer_position < self.right_buffer.len() {
                let left = self.left_current.as_ref().unwrap();
                let right = &self.right_buffer[self.buffer_position];
                self.buffer_position += 1;

                let mut data = left.data.clone();
                data.extend_from_slice(&right.data);
                return Ok(Some(SimpleTuple { data }));
            }

            if self.buffer_position == self.right_buffer.len() && !self.right_buffer.is_empty() {
                self.left_current = self.left.next()?;
                if let Some(ref left) = self.left_current {
                    if Some(&left.data) == self.left_value.as_ref() {
                        self.buffer_position = 0;
                        continue;
                    }
                }
                self.right_buffer.clear();
                self.left_value = None;
            }

            if self.left_current.is_none() || self.right_current.is_none() {
                self.finished = true;
                return Ok(None);
            }

            let left = self.left_current.as_ref().unwrap();
            let right = self.right_current.as_ref().unwrap();

            match Self::compare_tuples(left, right) {
                std::cmp::Ordering::Less => {
                    self.left_current = self.left.next()?;
                }
                std::cmp::Ordering::Greater => {
                    self.right_current = self.right.next()?;
                }
                std::cmp::Ordering::Equal => {
                    self.right_buffer.clear();
                    self.right_buffer.push(right.clone());
                    self.left_value = Some(left.data.clone());

                    loop {
                        match self.right.next()? {
                            Some(r) => {
                                if Self::compare_tuples(left, &r) == std::cmp::Ordering::Equal {
                                    self.right_buffer.push(r);
                                } else {
                                    self.right_current = Some(r);
                                    break;
                                }
                            }
                            None => {
                                self.right_current = None;
                                break;
                            }
                        }
                    }

                    self.buffer_position = 0;
                }
            }
        }
    }

    fn close(&mut self) -> Result<(), ExecutorError> {
        self.left.close()?;
        self.right.close()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
    fn test_merge_join_basic() {
        let left = vec![
            SimpleTuple { data: vec![1] },
            SimpleTuple { data: vec![2] },
            SimpleTuple { data: vec![3] },
        ];
        let right = vec![
            SimpleTuple { data: vec![1] },
            SimpleTuple { data: vec![2] },
            SimpleTuple { data: vec![3] },
        ];

        let mut join =
            MergeJoin::new(Box::new(MockExecutor::new(left)), Box::new(MockExecutor::new(right)));

        join.open().unwrap();
        assert_eq!(join.next().unwrap().unwrap().data, vec![1, 1]);
        assert_eq!(join.next().unwrap().unwrap().data, vec![2, 2]);
        assert_eq!(join.next().unwrap().unwrap().data, vec![3, 3]);
        assert!(join.next().unwrap().is_none());
        join.close().unwrap();
    }

    #[test]
    fn test_merge_join_empty_left() {
        let left = vec![];
        let right = vec![SimpleTuple { data: vec![1] }];

        let mut join =
            MergeJoin::new(Box::new(MockExecutor::new(left)), Box::new(MockExecutor::new(right)));

        join.open().unwrap();
        assert!(join.next().unwrap().is_none());
        join.close().unwrap();
    }

    #[test]
    fn test_merge_join_empty_right() {
        let left = vec![SimpleTuple { data: vec![1] }];
        let right = vec![];

        let mut join =
            MergeJoin::new(Box::new(MockExecutor::new(left)), Box::new(MockExecutor::new(right)));

        join.open().unwrap();
        assert!(join.next().unwrap().is_none());
        join.close().unwrap();
    }

    #[test]
    fn test_merge_join_no_matches() {
        let left = vec![SimpleTuple { data: vec![1] }, SimpleTuple { data: vec![2] }];
        let right = vec![SimpleTuple { data: vec![3] }, SimpleTuple { data: vec![4] }];

        let mut join =
            MergeJoin::new(Box::new(MockExecutor::new(left)), Box::new(MockExecutor::new(right)));

        join.open().unwrap();
        assert!(join.next().unwrap().is_none());
        join.close().unwrap();
    }

    #[test]
    fn test_merge_join_duplicates() {
        let left = vec![SimpleTuple { data: vec![1] }, SimpleTuple { data: vec![1] }];
        let right = vec![SimpleTuple { data: vec![1] }, SimpleTuple { data: vec![1] }];

        let mut join =
            MergeJoin::new(Box::new(MockExecutor::new(left)), Box::new(MockExecutor::new(right)));

        join.open().unwrap();
        assert_eq!(join.next().unwrap().unwrap().data, vec![1, 1]);
        assert_eq!(join.next().unwrap().unwrap().data, vec![1, 1]);
        assert_eq!(join.next().unwrap().unwrap().data, vec![1, 1]);
        assert_eq!(join.next().unwrap().unwrap().data, vec![1, 1]);
        assert!(join.next().unwrap().is_none());
        join.close().unwrap();
    }

    #[test]
    fn test_merge_join_single_match() {
        let left = vec![SimpleTuple { data: vec![2] }];
        let right = vec![SimpleTuple { data: vec![2] }];

        let mut join =
            MergeJoin::new(Box::new(MockExecutor::new(left)), Box::new(MockExecutor::new(right)));

        join.open().unwrap();
        assert_eq!(join.next().unwrap().unwrap().data, vec![2, 2]);
        assert!(join.next().unwrap().is_none());
        join.close().unwrap();
    }

    #[test]
    fn test_merge_join_partial_overlap() {
        let left = vec![
            SimpleTuple { data: vec![1] },
            SimpleTuple { data: vec![2] },
            SimpleTuple { data: vec![3] },
        ];
        let right = vec![
            SimpleTuple { data: vec![2] },
            SimpleTuple { data: vec![3] },
            SimpleTuple { data: vec![4] },
        ];

        let mut join =
            MergeJoin::new(Box::new(MockExecutor::new(left)), Box::new(MockExecutor::new(right)));

        join.open().unwrap();
        assert_eq!(join.next().unwrap().unwrap().data, vec![2, 2]);
        assert_eq!(join.next().unwrap().unwrap().data, vec![3, 3]);
        assert!(join.next().unwrap().is_none());
        join.close().unwrap();
    }
}
