//! Test helpers for the Executor trait
//!
//! This module provides utilities for testing executors.

use crate::catalog::{TableSchema, Value};
use crate::executor::{Executor, ExecutorError, Tuple};
use crate::parser::ast::{ColumnDef, DataType};
use std::collections::HashMap;

/// Mock executor for the new Executor trait (Volcano model)
pub struct MockExecutor {
    pub tuples: Vec<Tuple>,
    pub position: usize,
}

impl MockExecutor {
    pub fn new(tuples: Vec<Tuple>) -> Self {
        Self { tuples, position: 0 }
    }

    /// Create an empty mock executor
    pub fn empty() -> Self {
        Self::new(Vec::new())
    }

    /// Create a mock executor with simple integer tuples
    pub fn from_int_values(values: Vec<i64>) -> Self {
        let tuples = values
            .into_iter()
            .map(|v| {
                let mut tuple = Tuple::new();
                tuple.insert("value".to_string(), Value::Int(v));
                tuple
            })
            .collect();
        Self::new(tuples)
    }

    /// Create a mock executor with text values
    pub fn from_text_values(values: Vec<&str>) -> Self {
        let tuples = values
            .into_iter()
            .map(|v| {
                let mut tuple = Tuple::new();
                tuple.insert("value".to_string(), Value::Text(v.to_string()));
                tuple
            })
            .collect();
        Self::new(tuples)
    }

    /// Create a mock executor that returns an error
    pub fn failing() -> Self {
        Self {
            tuples: vec![],
            position: usize::MAX, // Will cause error on access
        }
    }

    /// Create a mock executor with custom tuples
    pub fn with_tuples(tuples: Vec<Tuple>) -> Self {
        Self::new(tuples)
    }

    /// Get the number of remaining tuples
    pub fn remaining(&self) -> usize {
        self.tuples.len().saturating_sub(self.position)
    }

    /// Reset position to start
    pub fn reset(&mut self) {
        self.position = 0;
    }
}

impl Executor for MockExecutor {
    fn next(&mut self) -> Result<Option<Tuple>, ExecutorError> {
        if self.position >= self.tuples.len() {
            return Ok(None);
        }
        let tuple = self.tuples[self.position].clone();
        self.position += 1;
        Ok(Some(tuple))
    }
}

// ============================================================================
// Helper Functions
// ============================================================================

/// Count results from an executor
pub fn count_results<E: Executor>(executor: &mut E) -> Result<usize, ExecutorError> {
    let mut count = 0;
    while executor.next()?.is_some() {
        count += 1;
    }
    Ok(count)
}

/// Run an executor and collect all results
pub fn run_executor<E: Executor>(executor: &mut E) -> Result<Vec<Tuple>, ExecutorError> {
    let mut results = Vec::new();
    while let Some(tuple) = executor.next()? {
        results.push(tuple);
    }
    Ok(results)
}

/// Test executor lifecycle with expected count
pub fn test_executor_lifecycle<E: Executor>(
    executor: &mut E,
    expected_count: usize,
) -> Result<(), ExecutorError> {
    let count = count_results(executor)?;
    assert_eq!(count, expected_count);
    Ok(())
}

// ============================================================================
// Comparison Utilities
// ============================================================================

/// Compare two executors by running them and comparing results
pub fn compare_executors<E1: Executor, E2: Executor>(
    executor1: &mut E1,
    executor2: &mut E2,
) -> Result<bool, ExecutorError> {
    let results1 = run_executor(executor1)?;
    let results2 = run_executor(executor2)?;

    if results1.len() != results2.len() {
        return Ok(false);
    }

    for (t1, t2) in results1.iter().zip(results2.iter()) {
        if !tuples_equal(t1, t2) {
            return Ok(false);
        }
    }

    Ok(true)
}

/// Compare two tuples for equality
pub fn tuples_equal(a: &Tuple, b: &Tuple) -> bool {
    if a.len() != b.len() {
        return false;
    }

    for (key, value_a) in a {
        match b.get(key) {
            Some(value_b) if value_a == value_b => {}
            _ => return false,
        }
    }

    true
}

/// Compare two tuples approximately (for floating point)
pub fn tuples_approximately_equal(a: &Tuple, b: &Tuple, epsilon: f64) -> bool {
    if a.len() != b.len() {
        return false;
    }

    for (key, value_a) in a {
        match b.get(key) {
            Some(value_b) => {
                if !values_approximately_equal(value_a, value_b, epsilon) {
                    return false;
                }
            }
            _ => return false,
        }
    }

    true
}

fn values_approximately_equal(a: &Value, b: &Value, epsilon: f64) -> bool {
    match (a, b) {
        (Value::Float(a), Value::Float(b)) => (a - b).abs() < epsilon,
        _ => a == b,
    }
}

// ============================================================================
// Schema Helpers
// ============================================================================

/// Create a simple test schema with a single column
pub fn create_simple_schema(column_name: &str, data_type: DataType) -> TableSchema {
    TableSchema::new(
        "test".to_string(),
        vec![ColumnDef {
            name: column_name.to_string(),
            data_type,
            is_primary_key: false,
            is_unique: false,
            is_auto_increment: false,
            is_not_null: false,
            default_value: None,
            foreign_key: None,
        }],
    )
}

/// Create a test schema with multiple columns
pub fn create_multi_column_schema(table_name: &str, columns: Vec<(&str, DataType)>) -> TableSchema {
    TableSchema::new(
        table_name.to_string(),
        columns
            .into_iter()
            .map(|(name, data_type)| ColumnDef {
                name: name.to_string(),
                data_type,
                is_primary_key: false,
                is_unique: false,
                is_auto_increment: false,
                is_not_null: false,
                default_value: None,
                foreign_key: None,
            })
            .collect(),
    )
}

// ============================================================================
// Tuple Builders
// ============================================================================

/// Builder for creating test tuples
pub struct TupleBuilder {
    tuple: Tuple,
}

impl TupleBuilder {
    pub fn new() -> Self {
        Self { tuple: HashMap::new() }
    }

    pub fn with_int(mut self, key: &str, value: i64) -> Self {
        self.tuple.insert(key.to_string(), Value::Int(value));
        self
    }

    pub fn with_float(mut self, key: &str, value: f64) -> Self {
        self.tuple.insert(key.to_string(), Value::Float(value));
        self
    }

    pub fn with_text(mut self, key: &str, value: &str) -> Self {
        self.tuple.insert(key.to_string(), Value::Text(value.to_string()));
        self
    }

    pub fn with_bool(mut self, key: &str, value: bool) -> Self {
        self.tuple.insert(key.to_string(), Value::Bool(value));
        self
    }

    pub fn with_null(mut self, key: &str) -> Self {
        self.tuple.insert(key.to_string(), Value::Null);
        self
    }

    pub fn with_value(mut self, key: &str, value: Value) -> Self {
        self.tuple.insert(key.to_string(), value);
        self
    }

    pub fn build(self) -> Tuple {
        self.tuple
    }
}

impl Default for TupleBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// Create a tuple with a single value
pub fn tuple_with_value(key: &str, value: Value) -> Tuple {
    let mut tuple = HashMap::new();
    tuple.insert(key.to_string(), value);
    tuple
}

// ============================================================================
// Assertion Macros
// ============================================================================

/// Assert that two tuples are equal
#[macro_export]
macro_rules! assert_tuple_eq {
    ($a:expr, $b:expr) => {{
        let a = &$a;
        let b = &$b;
        if !$crate::executor::test_helpers::tuples_equal(a, b) {
            panic!("Tuples not equal:\n  left: {:?}\n right: {:?}", a, b);
        }
    }};
}

/// Assert that two tuples are approximately equal (for floating point)
#[macro_export]
macro_rules! assert_tuple_approx_eq {
    ($a:expr, $b:expr, $epsilon:expr) => {{
        let a = &$a;
        let b = &$b;
        if !$crate::executor::test_helpers::tuples_approximately_equal(a, b, $epsilon) {
            panic!(
                "Tuples not approximately equal (epsilon={}):\n  left: {:?}\n right: {:?}",
                $epsilon, a, b
            );
        }
    }};
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mock_executor_basic() {
        let mut executor = MockExecutor::from_int_values(vec![1, 2, 3]);
        assert_eq!(executor.remaining(), 3);

        let tuple = executor.next().unwrap().unwrap();
        assert_eq!(tuple.get("value"), Some(&Value::Int(1)));
        assert_eq!(executor.remaining(), 2);
    }

    #[test]
    fn test_mock_executor_empty() {
        let mut executor = MockExecutor::empty();
        assert!(executor.next().unwrap().is_none());
    }

    #[test]
    fn test_count_results() {
        let mut executor = MockExecutor::from_int_values(vec![1, 2, 3, 4, 5]);
        let count = count_results(&mut executor).unwrap();
        assert_eq!(count, 5);
    }

    #[test]
    fn test_run_executor() {
        let mut executor = MockExecutor::from_text_values(vec!["a", "b", "c"]);
        let results = run_executor(&mut executor).unwrap();
        assert_eq!(results.len(), 3);
    }

    #[test]
    fn test_compare_executors() {
        let mut executor1 = MockExecutor::from_int_values(vec![1, 2, 3]);
        let mut executor2 = MockExecutor::from_int_values(vec![1, 2, 3]);
        assert!(compare_executors(&mut executor1, &mut executor2).unwrap());
    }

    #[test]
    fn test_tuple_builder() {
        let tuple = TupleBuilder::new()
            .with_int("id", 42)
            .with_text("name", "test")
            .with_bool("active", true)
            .with_null("optional")
            .build();

        assert_eq!(tuple.get("id"), Some(&Value::Int(42)));
        assert_eq!(tuple.get("name"), Some(&Value::Text("test".to_string())));
        assert_eq!(tuple.get("active"), Some(&Value::Bool(true)));
        assert_eq!(tuple.get("optional"), Some(&Value::Null));
    }

    #[test]
    fn test_create_simple_schema() {
        let schema = create_simple_schema("value", DataType::Int);
        assert_eq!(schema.name, "test");
        assert_eq!(schema.columns.len(), 1);
        assert_eq!(schema.columns[0].name, "value");
    }

    #[test]
    fn test_create_multi_column_schema() {
        let schema = create_multi_column_schema(
            "users",
            vec![("id", DataType::Int), ("name", DataType::Text), ("active", DataType::Boolean)],
        );
        assert_eq!(schema.name, "users");
        assert_eq!(schema.columns.len(), 3);
    }
}
