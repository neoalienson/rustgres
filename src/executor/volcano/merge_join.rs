//! MergeJoinExecutor - Implements merge join algorithm
//!
//! This executor performs a merge join by loading and sorting both inputs,
//! then merging them based on the sorted join keys.

use crate::catalog::Value;
use crate::executor::operators::executor::{Executor, ExecutorError, Tuple};

pub struct MergeJoinExecutor {
    left: Box<dyn Executor>,
    right: Box<dyn Executor>,
    left_buffer: Vec<Tuple>,
    right_buffer: Vec<Tuple>,
    left_idx: usize,
    right_idx: usize,
    result_buffer: Vec<Tuple>,
    result_idx: usize,
    initialized: bool,
    left_key: String,
    right_key: String,
}

impl MergeJoinExecutor {
    /// Create a new MergeJoinExecutor
    ///
    /// # Arguments
    /// * `left` - Left child executor (must be sortable by left_key)
    /// * `right` - Right child executor (must be sortable by right_key)
    /// * `left_key` - Column name for join key in left side
    /// * `right_key` - Column name for join key in right side
    pub fn new(
        left: Box<dyn Executor>,
        right: Box<dyn Executor>,
        left_key: String,
        right_key: String,
    ) -> Self {
        Self {
            left,
            right,
            left_buffer: Vec::new(),
            right_buffer: Vec::new(),
            left_idx: 0,
            right_idx: 0,
            result_buffer: Vec::new(),
            result_idx: 0,
            initialized: false,
            left_key,
            right_key,
        }
    }

    /// Create a new MergeJoinExecutor with same key name on both sides
    pub fn with_key(left: Box<dyn Executor>, right: Box<dyn Executor>, key: String) -> Self {
        Self::new(left, right, key.clone(), key)
    }

    /// Load all tuples from both sides and sort by join key
    fn load_and_sort(&mut self) -> Result<(), ExecutorError> {
        // Load left side
        while let Some(tuple) = self.left.next()? {
            self.left_buffer.push(tuple);
        }

        // Load right side
        while let Some(tuple) = self.right.next()? {
            self.right_buffer.push(tuple);
        }

        // Sort both sides by join key
        let left_key = self.left_key.clone();
        self.left_buffer.sort_by(|a, b| {
            let a_val = a.get(&left_key);
            let b_val = b.get(&left_key);
            Self::compare_values(a_val, b_val)
        });

        let right_key = self.right_key.clone();
        self.right_buffer.sort_by(|a, b| {
            let a_val = a.get(&right_key);
            let b_val = b.get(&right_key);
            Self::compare_values(a_val, b_val)
        });

        self.initialized = true;
        Ok(())
    }

    /// Compare two optional values for sorting
    fn compare_values(a: Option<&Value>, b: Option<&Value>) -> std::cmp::Ordering {
        match (a, b) {
            (Some(a), Some(b)) => a.cmp(b),
            (None, Some(_)) => std::cmp::Ordering::Less,
            (Some(_), None) => std::cmp::Ordering::Greater,
            (None, None) => std::cmp::Ordering::Equal,
        }
    }

    /// Extract the join key value from a tuple
    fn get_key_value<'a>(tuple: &'a Tuple, key: &str) -> Option<&'a Value> {
        tuple.get(key)
    }

    /// Perform the merge join
    fn merge(&mut self) -> Result<(), ExecutorError> {
        while self.left_idx < self.left_buffer.len() && self.right_idx < self.right_buffer.len() {
            let left_key_val =
                Self::get_key_value(&self.left_buffer[self.left_idx], &self.left_key);
            let right_key_val =
                Self::get_key_value(&self.right_buffer[self.right_idx], &self.right_key);

            match Self::compare_values(left_key_val, right_key_val) {
                std::cmp::Ordering::Less => {
                    // Left key is smaller, skip it
                    self.left_idx += 1;
                }
                std::cmp::Ordering::Greater => {
                    // Right key is smaller, skip it
                    self.right_idx += 1;
                }
                std::cmp::Ordering::Equal => {
                    // Keys match - find all tuples with this key on both sides
                    let left_key_end = self.find_group_end(
                        &self.left_buffer,
                        self.left_idx,
                        &self.left_key,
                        left_key_val,
                    );
                    let right_key_end = self.find_group_end(
                        &self.right_buffer,
                        self.right_idx,
                        &self.right_key,
                        right_key_val,
                    );

                    // Produce cross product of matching groups
                    for i in self.left_idx..left_key_end {
                        for j in self.right_idx..right_key_end {
                            let result =
                                Self::merge_tuples(&self.left_buffer[i], &self.right_buffer[j]);
                            self.result_buffer.push(result);
                        }
                    }

                    self.left_idx = left_key_end;
                    self.right_idx = right_key_end;
                }
            }
        }

        Ok(())
    }

    /// Find the end of a group with the same key value
    fn find_group_end(
        &self,
        buffer: &[Tuple],
        start: usize,
        key_column: &str,
        key_value: Option<&Value>,
    ) -> usize {
        let mut end = start;
        while end < buffer.len() {
            let current_val = Self::get_key_value(&buffer[end], key_column);
            if Self::compare_values(current_val, key_value) != std::cmp::Ordering::Equal {
                break;
            }
            end += 1;
        }
        end
    }

    /// Merge two tuples into one
    fn merge_tuples(left: &Tuple, right: &Tuple) -> Tuple {
        let mut result = left.clone();
        for (key, value) in right {
            result.insert(key.clone(), value.clone());
        }
        result
    }
}

impl Executor for MergeJoinExecutor {
    fn next(&mut self) -> Result<Option<Tuple>, ExecutorError> {
        // Initialize on first call
        if !self.initialized {
            self.load_and_sort()?;
            self.merge()?;
        }

        // Return next result from buffer
        if self.result_idx < self.result_buffer.len() {
            let result = self.result_buffer[self.result_idx].clone();
            self.result_idx += 1;
            Ok(Some(result))
        } else {
            Ok(None)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::executor::test_helpers::{MockExecutor, TupleBuilder};

    #[test]
    fn test_merge_join_basic() {
        // Note: Input doesn't need to be pre-sorted; executor sorts internally
        let left = MockExecutor::with_tuples(vec![
            TupleBuilder::new().with_int("id", 2).with_int("lval", 20).build(),
            TupleBuilder::new().with_int("id", 1).with_int("lval", 10).build(),
        ]);

        let right = MockExecutor::with_tuples(vec![
            TupleBuilder::new().with_int("id", 1).with_int("rval", 100).build(),
            TupleBuilder::new().with_int("id", 2).with_int("rval", 200).build(),
        ]);

        let mut join =
            MergeJoinExecutor::with_key(Box::new(left), Box::new(right), "id".to_string());

        let mut results = Vec::new();
        while let Some(tuple) = join.next().unwrap() {
            results.push(tuple);
        }

        assert_eq!(results.len(), 2);

        // Results should be in sorted order
        assert_eq!(results[0].get("id"), Some(&Value::Int(1)));
        assert_eq!(results[0].get("lval"), Some(&Value::Int(10)));
        assert_eq!(results[0].get("rval"), Some(&Value::Int(100)));

        assert_eq!(results[1].get("id"), Some(&Value::Int(2)));
        assert_eq!(results[1].get("lval"), Some(&Value::Int(20)));
        assert_eq!(results[1].get("rval"), Some(&Value::Int(200)));
    }

    #[test]
    fn test_merge_join_no_match() {
        let left = MockExecutor::with_tuples(vec![TupleBuilder::new().with_int("id", 1).build()]);

        let right = MockExecutor::with_tuples(vec![TupleBuilder::new().with_int("id", 2).build()]);

        let mut join =
            MergeJoinExecutor::with_key(Box::new(left), Box::new(right), "id".to_string());
        assert!(join.next().unwrap().is_none());
    }

    #[test]
    fn test_merge_join_multiple_matches() {
        // Multiple tuples with same key on both sides
        let left = MockExecutor::with_tuples(vec![
            TupleBuilder::new().with_int("id", 1).with_text("l", "a").build(),
            TupleBuilder::new().with_int("id", 1).with_text("l", "b").build(),
        ]);

        let right = MockExecutor::with_tuples(vec![
            TupleBuilder::new().with_int("id", 1).with_text("r", "x").build(),
            TupleBuilder::new().with_int("id", 1).with_text("r", "y").build(),
        ]);

        let mut join =
            MergeJoinExecutor::with_key(Box::new(left), Box::new(right), "id".to_string());

        let mut results = Vec::new();
        while let Some(tuple) = join.next().unwrap() {
            results.push(tuple);
        }

        // Cross product: 2x2 = 4 results
        assert_eq!(results.len(), 4);
    }

    #[test]
    fn test_merge_join_empty_left() {
        let left = MockExecutor::empty();
        let right = MockExecutor::with_tuples(vec![TupleBuilder::new().with_int("id", 1).build()]);

        let mut join =
            MergeJoinExecutor::with_key(Box::new(left), Box::new(right), "id".to_string());
        assert!(join.next().unwrap().is_none());
    }

    #[test]
    fn test_merge_join_empty_right() {
        let left = MockExecutor::with_tuples(vec![TupleBuilder::new().with_int("id", 1).build()]);
        let right = MockExecutor::empty();

        let mut join =
            MergeJoinExecutor::with_key(Box::new(left), Box::new(right), "id".to_string());
        assert!(join.next().unwrap().is_none());
    }

    #[test]
    fn test_merge_join_text_key() {
        let left = MockExecutor::with_tuples(vec![
            TupleBuilder::new().with_text("name", "alice").with_int("age", 30).build(),
            TupleBuilder::new().with_text("name", "bob").with_int("age", 25).build(),
        ]);

        let right = MockExecutor::with_tuples(vec![
            TupleBuilder::new().with_text("name", "alice").with_text("city", "NYC").build(),
            TupleBuilder::new().with_text("name", "charlie").with_text("city", "LA").build(),
        ]);

        let mut join =
            MergeJoinExecutor::with_key(Box::new(left), Box::new(right), "name".to_string());

        let results: Vec<_> =
            std::iter::from_fn(|| join.next().transpose()).collect::<Result<_, _>>().unwrap();

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].get("name"), Some(&Value::Text("alice".to_string())));
        assert_eq!(results[0].get("age"), Some(&Value::Int(30)));
        assert_eq!(results[0].get("city"), Some(&Value::Text("NYC".to_string())));
    }
}
