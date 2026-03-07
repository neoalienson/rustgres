//! CaseExecutor - Implements CASE expression evaluation
//!
//! This executor evaluates CASE expressions for each input tuple.
//! The condition and result functions are provided as closures.

use crate::catalog::Value;
use crate::executor::operators::executor::{Executor, ExecutorError, Tuple};

pub struct CaseExecutor {
    child: Box<dyn Executor>,
    condition: Box<dyn Fn(&Tuple) -> bool + Send>,
    then_expr: Box<dyn Fn(&Tuple) -> Value + Send>,
    else_expr: Option<Box<dyn Fn(&Tuple) -> Value + Send>>,
    result_column: String,
}

impl CaseExecutor {
    /// Create a new CaseExecutor
    ///
    /// # Arguments
    /// * `child` - Child executor providing input tuples
    /// * `condition` - Condition function that returns true if THEN clause applies
    /// * `then_expr` - Expression to evaluate when condition is true
    /// * `else_expr` - Optional expression to evaluate when condition is false
    /// * `result_column` - Name of the result column to add
    pub fn new(
        child: Box<dyn Executor>,
        condition: Box<dyn Fn(&Tuple) -> bool + Send>,
        then_expr: Box<dyn Fn(&Tuple) -> Value + Send>,
        else_expr: Option<Box<dyn Fn(&Tuple) -> Value + Send>>,
        result_column: String,
    ) -> Self {
        Self { child, condition, then_expr, else_expr, result_column }
    }

    /// Create a CASE executor with NULL for ELSE clause
    pub fn with_null_else(
        child: Box<dyn Executor>,
        condition: Box<dyn Fn(&Tuple) -> bool + Send>,
        then_expr: Box<dyn Fn(&Tuple) -> Value + Send>,
        result_column: String,
    ) -> Self {
        Self::new(child, condition, then_expr, None, result_column)
    }
}

impl Executor for CaseExecutor {
    fn next(&mut self) -> Result<Option<Tuple>, ExecutorError> {
        match self.child.next()? {
            Some(input_tuple) => {
                let mut result_tuple = input_tuple.clone();

                let condition_result = (self.condition)(&input_tuple);
                let value = if condition_result {
                    (self.then_expr)(&input_tuple)
                } else if let Some(ref else_fn) = self.else_expr {
                    (else_fn)(&input_tuple)
                } else {
                    Value::Null
                };

                result_tuple.insert(self.result_column.clone(), value);
                Ok(Some(result_tuple))
            }
            None => Ok(None),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::executor::test_helpers::{MockExecutor, TupleBuilder};

    fn get_int(t: &Tuple, col: &str) -> i64 {
        match t.get(col) {
            Some(Value::Int(i)) => *i,
            _ => panic!("Expected Int"),
        }
    }

    #[test]
    fn test_case_basic() {
        let input = MockExecutor::with_tuples(vec![
            TupleBuilder::new().with_int("val", 1).build(),
            TupleBuilder::new().with_int("val", 2).build(),
            TupleBuilder::new().with_int("val", 3).build(),
        ]);

        let mut case = CaseExecutor::with_null_else(
            Box::new(input),
            Box::new(|t| get_int(t, "val") > 1),
            Box::new(|_| Value::Text("greater".to_string())),
            "category".to_string(),
        );

        let t1 = case.next().unwrap().unwrap();
        assert_eq!(t1.get("val"), Some(&Value::Int(1)));
        assert_eq!(t1.get("category"), Some(&Value::Null));

        let t2 = case.next().unwrap().unwrap();
        assert_eq!(t2.get("val"), Some(&Value::Int(2)));
        assert_eq!(t2.get("category"), Some(&Value::Text("greater".to_string())));

        let t3 = case.next().unwrap().unwrap();
        assert_eq!(t3.get("val"), Some(&Value::Int(3)));
        assert_eq!(t3.get("category"), Some(&Value::Text("greater".to_string())));

        assert!(case.next().unwrap().is_none());
    }

    #[test]
    fn test_case_with_else() {
        let input = MockExecutor::with_tuples(vec![
            TupleBuilder::new().with_int("val", 1).build(),
            TupleBuilder::new().with_int("val", 2).build(),
        ]);

        let mut case = CaseExecutor::new(
            Box::new(input),
            Box::new(|t| get_int(t, "val") > 1),
            Box::new(|_| Value::Text("big".to_string())),
            Some(Box::new(|_| Value::Text("small".to_string()))),
            "size".to_string(),
        );

        let t1 = case.next().unwrap().unwrap();
        assert_eq!(t1.get("size"), Some(&Value::Text("small".to_string())));

        let t2 = case.next().unwrap().unwrap();
        assert_eq!(t2.get("size"), Some(&Value::Text("big".to_string())));
    }

    #[test]
    fn test_case_empty_input() {
        let input = MockExecutor::empty();

        let mut case = CaseExecutor::with_null_else(
            Box::new(input),
            Box::new(|_| true),
            Box::new(|_| Value::Int(1)),
            "result".to_string(),
        );

        assert!(case.next().unwrap().is_none());
    }
}
