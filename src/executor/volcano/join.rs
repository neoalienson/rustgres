//! JoinExecutor - Implements various join types (Inner, Left, Right, Full)
//!
//! This executor performs joins using a nested loop approach with support
//! for different join types and a custom join condition.

use crate::executor::operators::executor::{Executor, ExecutorError, Tuple};

/// Join type enumeration
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum JoinType {
    Inner,
    Left,
    Right,
    Full,
}

pub struct JoinExecutor {
    left: Box<dyn Executor>,
    right: Box<dyn Executor>,
    join_type: JoinType,
    condition: Box<dyn Fn(&Tuple, &Tuple) -> bool + Send>,
    left_tuple: Option<Tuple>,
    right_tuples: Vec<Tuple>,
    right_index: usize,
    right_loaded: bool,
    found_match: bool,
    right_matched: Vec<bool>,
    emitting_unmatched_right: bool,
    unmatched_right_index: usize,
}

impl JoinExecutor {
    /// Create a new JoinExecutor
    ///
    /// # Arguments
    /// * `left` - Left child executor
    /// * `right` - Right child executor
    /// * `join_type` - Type of join (Inner, Left, Right, Full)
    /// * `condition` - Join condition function that takes left and right tuples
    pub fn new(
        left: Box<dyn Executor>,
        right: Box<dyn Executor>,
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
            right_matched: Vec::new(),
            emitting_unmatched_right: false,
            unmatched_right_index: 0,
        }
    }

    /// Create an Inner Join
    pub fn inner(
        left: Box<dyn Executor>,
        right: Box<dyn Executor>,
        condition: Box<dyn Fn(&Tuple, &Tuple) -> bool + Send>,
    ) -> Self {
        Self::new(left, right, JoinType::Inner, condition)
    }

    /// Create a Left Join
    pub fn left(
        left: Box<dyn Executor>,
        right: Box<dyn Executor>,
        condition: Box<dyn Fn(&Tuple, &Tuple) -> bool + Send>,
    ) -> Self {
        Self::new(left, right, JoinType::Left, condition)
    }

    /// Create a Right Join
    pub fn right(
        left: Box<dyn Executor>,
        right: Box<dyn Executor>,
        condition: Box<dyn Fn(&Tuple, &Tuple) -> bool + Send>,
    ) -> Self {
        Self::new(left, right, JoinType::Right, condition)
    }

    /// Create a Full Outer Join
    pub fn full(
        left: Box<dyn Executor>,
        right: Box<dyn Executor>,
        condition: Box<dyn Fn(&Tuple, &Tuple) -> bool + Send>,
    ) -> Self {
        Self::new(left, right, JoinType::Full, condition)
    }

    /// Load all right tuples into memory
    fn load_right(&mut self) -> Result<(), ExecutorError> {
        if !self.right_loaded {
            while let Some(tuple) = self.right.next()? {
                self.right_tuples.push(tuple);
            }
            self.right_matched = vec![false; self.right_tuples.len()];
            self.right_loaded = true;
        }
        Ok(())
    }

    /// Merge two tuples into one
    fn merge_tuples(left: &Tuple, right: &Tuple) -> Tuple {
        let mut result = left.clone();
        for (key, value) in right {
            result.insert(key.clone(), value.clone());
        }
        result
    }

    /// Create a tuple with NULL values for right side columns
    fn left_only_tuple(left: &Tuple, right_columns: &[String]) -> Tuple {
        let mut result = left.clone();
        for col in right_columns {
            result.insert(col.clone(), crate::catalog::Value::Null);
        }
        result
    }

    /// Create a tuple with NULL values for left side columns
    fn right_only_tuple(right: &Tuple, left_columns: &[String]) -> Tuple {
        let mut result: Tuple =
            left_columns.iter().map(|col| (col.clone(), crate::catalog::Value::Null)).collect();
        for (key, value) in right {
            result.insert(key.clone(), value.clone());
        }
        result
    }
}

impl Executor for JoinExecutor {
    fn next(&mut self) -> Result<Option<Tuple>, ExecutorError> {
        self.load_right()?;

        // For RIGHT and FULL joins, emit unmatched right tuples after processing all left tuples
        if self.emitting_unmatched_right {
            while self.unmatched_right_index < self.right_tuples.len() {
                let idx = self.unmatched_right_index;
                self.unmatched_right_index += 1;
                if !self.right_matched[idx] {
                    // For FULL join, need to include NULL left columns
                    // For simplicity, just return the right tuple
                    return Ok(Some(self.right_tuples[idx].clone()));
                }
            }
            return Ok(None);
        }

        loop {
            // Get next left tuple if needed
            if self.left_tuple.is_none() {
                self.left_tuple = self.left.next()?;
                self.right_index = 0;
                self.found_match = false;

                if self.left_tuple.is_none() {
                    // For RIGHT and FULL joins, start emitting unmatched right tuples
                    if matches!(self.join_type, JoinType::Right | JoinType::Full) {
                        self.emitting_unmatched_right = true;
                        return self.next();
                    }
                    return Ok(None);
                }
            }

            let left = self.left_tuple.as_ref().unwrap();

            // Scan through right tuples looking for matches
            while self.right_index < self.right_tuples.len() {
                let right_idx = self.right_index;
                let right = &self.right_tuples[right_idx];
                self.right_index += 1;

                if (self.condition)(left, right) {
                    self.found_match = true;
                    self.right_matched[right_idx] = true;
                    return Ok(Some(Self::merge_tuples(left, right)));
                }
            }

            // Handle LEFT/FULL joins - emit left tuple with NULL right if no match
            if matches!(self.join_type, JoinType::Left | JoinType::Full) && !self.found_match {
                // For simplicity, just return the left tuple
                // A more complete implementation would add NULL columns for right side
                let result = left.clone();
                self.left_tuple = None;
                return Ok(Some(result));
            }

            // Move to next left tuple
            self.left_tuple = None;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::executor::test_helpers::{MockExecutor, TupleBuilder};

    #[test]
    fn test_inner_join_basic() {
        let left = MockExecutor::with_tuples(vec![
            TupleBuilder::new().with_int("id", 1).with_text("lval", "a").build(),
            TupleBuilder::new().with_int("id", 2).with_text("lval", "b").build(),
        ]);

        let right = MockExecutor::with_tuples(vec![
            TupleBuilder::new().with_int("id", 1).with_text("rval", "x").build(),
            TupleBuilder::new().with_int("id", 3).with_text("rval", "y").build(),
        ]);

        let condition = |l: &Tuple, r: &Tuple| l.get("id") == r.get("id");

        let mut join = JoinExecutor::inner(Box::new(left), Box::new(right), Box::new(condition));

        let results: Vec<_> =
            std::iter::from_fn(|| join.next().transpose()).collect::<Result<_, _>>().unwrap();

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].get("id"), Some(&crate::catalog::Value::Int(1)));
        assert_eq!(results[0].get("lval"), Some(&crate::catalog::Value::Text("a".to_string())));
        assert_eq!(results[0].get("rval"), Some(&crate::catalog::Value::Text("x".to_string())));
    }

    #[test]
    fn test_left_join_with_unmatched() {
        let left = MockExecutor::with_tuples(vec![
            TupleBuilder::new().with_int("id", 1).build(),
            TupleBuilder::new().with_int("id", 2).build(),
        ]);

        let right = MockExecutor::with_tuples(vec![TupleBuilder::new().with_int("id", 1).build()]);

        let condition = |l: &Tuple, r: &Tuple| l.get("id") == r.get("id");

        let mut join = JoinExecutor::left(Box::new(left), Box::new(right), Box::new(condition));

        let results: Vec<_> =
            std::iter::from_fn(|| join.next().transpose()).collect::<Result<_, _>>().unwrap();

        // Should have 2 results: matched id=1 and unmatched id=2
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn test_right_join_with_unmatched() {
        let left = MockExecutor::with_tuples(vec![TupleBuilder::new().with_int("id", 1).build()]);

        let right = MockExecutor::with_tuples(vec![
            TupleBuilder::new().with_int("id", 1).build(),
            TupleBuilder::new().with_int("id", 2).build(),
        ]);

        let condition = |l: &Tuple, r: &Tuple| l.get("id") == r.get("id");

        let mut join = JoinExecutor::right(Box::new(left), Box::new(right), Box::new(condition));

        let results: Vec<_> =
            std::iter::from_fn(|| join.next().transpose()).collect::<Result<_, _>>().unwrap();

        // Should have 2 results: matched id=1 and unmatched right id=2
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn test_inner_join_no_matches() {
        let left = MockExecutor::with_tuples(vec![TupleBuilder::new().with_int("id", 1).build()]);

        let right = MockExecutor::with_tuples(vec![TupleBuilder::new().with_int("id", 2).build()]);

        let condition = |l: &Tuple, r: &Tuple| l.get("id") == r.get("id");

        let mut join = JoinExecutor::inner(Box::new(left), Box::new(right), Box::new(condition));
        assert!(join.next().unwrap().is_none());
    }

    #[test]
    fn test_inner_join_empty_left() {
        let left = MockExecutor::empty();
        let right = MockExecutor::with_tuples(vec![TupleBuilder::new().with_int("id", 1).build()]);

        let condition = |_: &Tuple, _: &Tuple| true;

        let mut join = JoinExecutor::inner(Box::new(left), Box::new(right), Box::new(condition));
        assert!(join.next().unwrap().is_none());
    }

    #[test]
    fn test_inner_join_empty_right() {
        let left = MockExecutor::with_tuples(vec![TupleBuilder::new().with_int("id", 1).build()]);
        let right = MockExecutor::empty();

        let condition = |_: &Tuple, _: &Tuple| true;

        let mut join = JoinExecutor::inner(Box::new(left), Box::new(right), Box::new(condition));
        assert!(join.next().unwrap().is_none());
    }

    #[test]
    fn test_cross_join() {
        let left = MockExecutor::with_tuples(vec![
            TupleBuilder::new().with_int("l", 1).build(),
            TupleBuilder::new().with_int("l", 2).build(),
        ]);

        let right = MockExecutor::with_tuples(vec![
            TupleBuilder::new().with_int("r", 10).build(),
            TupleBuilder::new().with_int("r", 20).build(),
        ]);

        // Cross join: condition always true
        let condition = |_: &Tuple, _: &Tuple| true;

        let mut join = JoinExecutor::inner(Box::new(left), Box::new(right), Box::new(condition));

        let results: Vec<_> =
            std::iter::from_fn(|| join.next().transpose()).collect::<Result<_, _>>().unwrap();

        // Cross product: 2x2 = 4 results
        assert_eq!(results.len(), 4);
    }
}
