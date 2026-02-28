use super::{SimpleExecutor, SimpleTuple as Tuple, ExecutorError};

pub enum JoinType {
    Inner,
    Left,
}

pub struct Join {
    left: Box<dyn SimpleExecutor>,
    right: Box<dyn SimpleExecutor>,
    join_type: JoinType,
    condition: Box<dyn Fn(&Tuple, &Tuple) -> bool + Send>,
    left_tuple: Option<Tuple>,
    right_tuples: Vec<Tuple>,
    right_index: usize,
    right_loaded: bool,
    found_match: bool,
}

impl Join {
    pub fn new(
        left: Box<dyn SimpleExecutor>,
        right: Box<dyn SimpleExecutor>,
        join_type: JoinType,
        condition: Box<dyn Fn(&Tuple, &Tuple) -> bool + Send>,
    ) -> Self {
        Self {
            left,
            right,
            join_type,
            condition,
            left_tuple: None,
            right_tuples: Vec::new(),
            right_index: 0,
            right_loaded: false,
            found_match: false,
        }
    }

    fn load_right(&mut self) -> Result<(), ExecutorError> {
        if !self.right_loaded {
            while let Some(tuple) = self.right.next()? {
                self.right_tuples.push(tuple);
            }
            self.right_loaded = true;
        }
        Ok(())
    }

    fn merge_tuples(&self, left: &Tuple, right: &Tuple) -> Tuple {
        let mut data = left.data.clone();
        data.extend_from_slice(&right.data);
        Tuple { data }
    }
}

impl SimpleExecutor for Join {
    fn open(&mut self) -> Result<(), ExecutorError> {
        self.left.open()?;
        self.right.open()?;
        self.right_loaded = false;
        self.right_tuples.clear();
        self.right_index = 0;
        self.left_tuple = None;
        self.found_match = false;
        Ok(())
    }

    fn next(&mut self) -> Result<Option<Tuple>, ExecutorError> {
        self.load_right()?;

        loop {
            if self.left_tuple.is_none() {
                self.left_tuple = self.left.next()?;
                self.right_index = 0;
                self.found_match = false;
                
                if self.left_tuple.is_none() {
                    return Ok(None);
                }
            }

            let left = self.left_tuple.as_ref().unwrap();

            while self.right_index < self.right_tuples.len() {
                let right = &self.right_tuples[self.right_index];
                self.right_index += 1;

                if (self.condition)(left, right) {
                    self.found_match = true;
                    return Ok(Some(self.merge_tuples(left, right)));
                }
            }

            if matches!(self.join_type, JoinType::Left) && !self.found_match {
                let result = left.clone();
                self.left_tuple = None;
                return Ok(Some(result));
            }

            self.left_tuple = None;
        }
    }

    fn close(&mut self) -> Result<(), ExecutorError> {
        self.left.close()?;
        self.right.close()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::executor::mock::MockExecutor;

    #[test]
    fn test_inner_join_basic() {
        let left = MockExecutor::new(vec![
            Tuple { data: vec![1, 10] },
            Tuple { data: vec![2, 20] },
        ]);
        let right = MockExecutor::new(vec![
            Tuple { data: vec![1, 100] },
            Tuple { data: vec![3, 255] },
        ]);
        let mut join = Join::new(
            Box::new(left),
            Box::new(right),
            JoinType::Inner,
            Box::new(|l, r| l.data[0] == r.data[0]),
        );
        join.open().unwrap();

        let result = join.next().unwrap().unwrap();
        assert_eq!(result.data, vec![1, 10, 1, 100]);
        assert!(join.next().unwrap().is_none());
        join.close().unwrap();
    }

    #[test]
    fn test_inner_join_no_matches() {
        let left = MockExecutor::new(vec![Tuple { data: vec![1] }]);
        let right = MockExecutor::new(vec![Tuple { data: vec![2] }]);
        let mut join = Join::new(
            Box::new(left),
            Box::new(right),
            JoinType::Inner,
            Box::new(|l, r| l.data[0] == r.data[0]),
        );
        join.open().unwrap();
        assert!(join.next().unwrap().is_none());
        join.close().unwrap();
    }

    #[test]
    fn test_inner_join_multiple_matches() {
        let left = MockExecutor::new(vec![
            Tuple { data: vec![1] },
            Tuple { data: vec![1] },
        ]);
        let right = MockExecutor::new(vec![
            Tuple { data: vec![1] },
            Tuple { data: vec![1] },
        ]);
        let mut join = Join::new(
            Box::new(left),
            Box::new(right),
            JoinType::Inner,
            Box::new(|l, r| l.data[0] == r.data[0]),
        );
        join.open().unwrap();

        let mut count = 0;
        while join.next().unwrap().is_some() {
            count += 1;
        }
        assert_eq!(count, 4);
        join.close().unwrap();
    }

    #[test]
    fn test_left_join_basic() {
        let left = MockExecutor::new(vec![
            Tuple { data: vec![1] },
            Tuple { data: vec![2] },
        ]);
        let right = MockExecutor::new(vec![Tuple { data: vec![1] }]);
        let mut join = Join::new(
            Box::new(left),
            Box::new(right),
            JoinType::Left,
            Box::new(|l, r| l.data[0] == r.data[0]),
        );
        join.open().unwrap();

        let r1 = join.next().unwrap().unwrap();
        assert_eq!(r1.data, vec![1, 1]);
        let r2 = join.next().unwrap().unwrap();
        assert_eq!(r2.data, vec![2]);
        assert!(join.next().unwrap().is_none());
        join.close().unwrap();
    }

    #[test]
    fn test_join_empty_left() {
        let left = MockExecutor::new(vec![]);
        let right = MockExecutor::new(vec![Tuple { data: vec![1] }]);
        let mut join = Join::new(
            Box::new(left),
            Box::new(right),
            JoinType::Inner,
            Box::new(|l, r| l.data[0] == r.data[0]),
        );
        join.open().unwrap();
        assert!(join.next().unwrap().is_none());
        join.close().unwrap();
    }

    #[test]
    fn test_join_empty_right() {
        let left = MockExecutor::new(vec![Tuple { data: vec![1] }]);
        let right = MockExecutor::new(vec![]);
        let mut join = Join::new(
            Box::new(left),
            Box::new(right),
            JoinType::Inner,
            Box::new(|l, r| l.data[0] == r.data[0]),
        );
        join.open().unwrap();
        assert!(join.next().unwrap().is_none());
        join.close().unwrap();
    }
}
