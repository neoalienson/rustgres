//! SubqueryExecutor - Executes subqueries and returns results
//!
//! This executor buffers subquery results and can be used for:
//! - Scalar subqueries (single value)
//! - IN subqueries (set of values)
//! - EXISTS subqueries (boolean check)

use crate::catalog::Value;
use crate::executor::operators::executor::{Executor, ExecutorError, Tuple};
use std::collections::HashSet;

pub struct SubqueryExecutor {
    child: Box<dyn Executor>,
    buffered: Vec<Tuple>,
    position: usize,
    executed: bool,
}

impl SubqueryExecutor {
    /// Create a new SubqueryExecutor
    ///
    /// # Arguments
    /// * `child` - Child executor representing the subquery
    pub fn new(child: Box<dyn Executor>) -> Self {
        Self { child, buffered: Vec::new(), position: 0, executed: false }
    }

    /// Execute the subquery and return a scalar value (first column of first row)
    ///
    /// Returns None if the subquery returns no rows
    pub fn execute_scalar(&mut self) -> Result<Option<Value>, ExecutorError> {
        self.ensure_executed()?;
        Ok(self.buffered.first().and_then(|t| t.values().next().cloned()))
    }

    /// Execute the subquery and return a set of values from the first column
    pub fn execute_set(&mut self) -> Result<HashSet<Value>, ExecutorError> {
        self.ensure_executed()?;
        Ok(self.buffered.iter().filter_map(|t| t.values().next().cloned()).collect())
    }

    /// Execute the subquery and return true if any rows are returned (EXISTS)
    pub fn execute_exists(&mut self) -> Result<bool, ExecutorError> {
        self.ensure_executed()?;
        Ok(!self.buffered.is_empty())
    }

    /// Ensure the subquery has been executed
    fn ensure_executed(&mut self) -> Result<(), ExecutorError> {
        if !self.executed {
            while let Some(tuple) = self.child.next()? {
                self.buffered.push(tuple);
            }
            self.executed = true;
            self.position = 0;
        }
        Ok(())
    }
}

impl Executor for SubqueryExecutor {
    fn next(&mut self) -> Result<Option<Tuple>, ExecutorError> {
        self.ensure_executed()?;

        if self.position < self.buffered.len() {
            let tuple = self.buffered[self.position].clone();
            self.position += 1;
            Ok(Some(tuple))
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
    fn test_subquery_scalar() {
        let input =
            MockExecutor::with_tuples(vec![TupleBuilder::new().with_int("val", 42).build()]);

        let mut subquery = SubqueryExecutor::new(Box::new(input));
        let result = subquery.execute_scalar().unwrap();

        assert_eq!(result, Some(Value::Int(42)));
    }

    #[test]
    fn test_subquery_scalar_empty() {
        let input = MockExecutor::empty();

        let mut subquery = SubqueryExecutor::new(Box::new(input));
        let result = subquery.execute_scalar().unwrap();

        assert_eq!(result, None);
    }

    #[test]
    fn test_subquery_set() {
        let input = MockExecutor::with_tuples(vec![
            TupleBuilder::new().with_int("val", 1).build(),
            TupleBuilder::new().with_int("val", 2).build(),
            TupleBuilder::new().with_int("val", 3).build(),
        ]);

        let mut subquery = SubqueryExecutor::new(Box::new(input));
        let result = subquery.execute_set().unwrap();

        assert!(result.contains(&Value::Int(1)));
        assert!(result.contains(&Value::Int(2)));
        assert!(result.contains(&Value::Int(3)));
        assert_eq!(result.len(), 3);
    }

    #[test]
    fn test_subquery_exists_true() {
        let input = MockExecutor::with_tuples(vec![TupleBuilder::new().with_int("val", 1).build()]);

        let mut subquery = SubqueryExecutor::new(Box::new(input));
        let result = subquery.execute_exists().unwrap();

        assert!(result);
    }

    #[test]
    fn test_subquery_exists_false() {
        let input = MockExecutor::empty();

        let mut subquery = SubqueryExecutor::new(Box::new(input));
        let result = subquery.execute_exists().unwrap();

        assert!(!result);
    }

    #[test]
    fn test_subquery_iterator() {
        let input = MockExecutor::with_tuples(vec![
            TupleBuilder::new().with_int("val", 1).build(),
            TupleBuilder::new().with_int("val", 2).build(),
        ]);

        let mut subquery = SubqueryExecutor::new(Box::new(input));

        let t1 = subquery.next().unwrap().unwrap();
        assert_eq!(t1.get("val"), Some(&Value::Int(1)));

        let t2 = subquery.next().unwrap().unwrap();
        assert_eq!(t2.get("val"), Some(&Value::Int(2)));

        assert!(subquery.next().unwrap().is_none());
    }

    #[test]
    fn test_subquery_multiple_executions() {
        let input = MockExecutor::with_tuples(vec![
            TupleBuilder::new().with_int("val", 1).build(),
            TupleBuilder::new().with_int("val", 2).build(),
        ]);

        let mut subquery = SubqueryExecutor::new(Box::new(input));

        // First execution via iterator
        let t1 = subquery.next().unwrap().unwrap();
        assert_eq!(t1.get("val"), Some(&Value::Int(1)));

        // Second execution via exists (should use buffered results)
        let exists = subquery.execute_exists().unwrap();
        assert!(exists);

        // Continue iteration
        let t2 = subquery.next().unwrap().unwrap();
        assert_eq!(t2.get("val"), Some(&Value::Int(2)));
    }
}
