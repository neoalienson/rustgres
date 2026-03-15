//! AggregateExecutor - Implements aggregate functions (COUNT, SUM, AVG, MIN, MAX)
//!
//! This executor computes aggregate functions over all input tuples.
//! It buffers all input and produces a single result tuple.

use crate::catalog::Value;
use crate::executor::operators::executor::{Executor, ExecutorError, Tuple};

/// Aggregate function types
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum AggregateFunction {
    Count,
    CountStar,
    Sum,
    Avg,
    Min,
    Max,
}

pub struct AggregateExecutor {
    child: Box<dyn Executor>,
    function: AggregateFunction,
    column: Option<String>,
    result: Option<Value>,
    computed: bool,
}

impl AggregateExecutor {
    /// Create a new AggregateExecutor
    ///
    /// # Arguments
    /// * `child` - Child executor providing input tuples
    /// * `function` - Aggregate function to compute
    /// * `column` - Column to aggregate (None for COUNT(*))
    pub fn new(
        child: Box<dyn Executor>,
        function: AggregateFunction,
        column: Option<String>,
    ) -> Self {
        Self { child, function, column, result: None, computed: false }
    }

    /// Create a COUNT(*) executor
    pub fn count_star(child: Box<dyn Executor>) -> Self {
        Self::new(child, AggregateFunction::CountStar, None)
    }

    /// Create a COUNT(column) executor
    pub fn count(child: Box<dyn Executor>, column: String) -> Self {
        Self::new(child, AggregateFunction::Count, Some(column))
    }

    /// Create a SUM(column) executor
    pub fn sum(child: Box<dyn Executor>, column: String) -> Self {
        Self::new(child, AggregateFunction::Sum, Some(column))
    }

    /// Create an AVG(column) executor
    pub fn avg(child: Box<dyn Executor>, column: String) -> Self {
        Self::new(child, AggregateFunction::Avg, Some(column))
    }

    /// Create a MIN(column) executor
    pub fn min(child: Box<dyn Executor>, column: String) -> Self {
        Self::new(child, AggregateFunction::Min, Some(column))
    }

    /// Create a MAX(column) executor
    pub fn max(child: Box<dyn Executor>, column: String) -> Self {
        Self::new(child, AggregateFunction::Max, Some(column))
    }

    /// Compute the aggregate result
    fn compute(&mut self) -> Result<(), ExecutorError> {
        let mut count: i64 = 0;
        let mut sum: i64 = 0;
        let mut min: Option<i64> = None;
        let mut max: Option<i64> = None;

        while let Some(tuple) = self.child.next()? {
            match self.function {
                AggregateFunction::CountStar => {
                    count += 1;
                }
                AggregateFunction::Count => {
                    if let Some(col) = &self.column
                        && let Some(val) = tuple.get(col)
                        && !matches!(val, Value::Null)
                    {
                        count += 1;
                    }
                }
                _ => {
                    if let Some(col) = &self.column
                        && let Some(&Value::Int(i)) = tuple.get(col)
                    {
                        sum += i;
                        count += 1;
                        min = Some(min.map_or(i, |m| m.min(i)));
                        max = Some(max.map_or(i, |m| m.max(i)));
                    }
                }
            }
        }

        self.result = Some(match self.function {
            AggregateFunction::CountStar | AggregateFunction::Count => Value::Int(count),
            AggregateFunction::Sum => Value::Int(sum),
            AggregateFunction::Avg => {
                if count > 0 {
                    Value::Int(sum / count)
                } else {
                    Value::Null
                }
            }
            AggregateFunction::Min => min.map(Value::Int).unwrap_or(Value::Null),
            AggregateFunction::Max => max.map(Value::Int).unwrap_or(Value::Null),
        });

        self.computed = true;
        Ok(())
    }

    /// Get the result column name
    fn result_column_name(&self) -> String {
        match self.function {
            AggregateFunction::CountStar => "count".to_string(),
            AggregateFunction::Count => format!("count_{}", self.column.as_deref().unwrap_or("")),
            AggregateFunction::Sum => format!("sum_{}", self.column.as_deref().unwrap_or("")),
            AggregateFunction::Avg => format!("avg_{}", self.column.as_deref().unwrap_or("")),
            AggregateFunction::Min => format!("min_{}", self.column.as_deref().unwrap_or("")),
            AggregateFunction::Max => format!("max_{}", self.column.as_deref().unwrap_or("")),
        }
    }
}

impl Executor for AggregateExecutor {
    fn next(&mut self) -> Result<Option<Tuple>, ExecutorError> {
        if !self.computed {
            self.compute()?;
        }

        if let Some(result) = self.result.take() {
            let mut tuple = Tuple::new();
            tuple.insert(self.result_column_name(), result);
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
    fn test_count_star() {
        let input = MockExecutor::with_tuples(vec![
            TupleBuilder::new().with_int("val", 1).build(),
            TupleBuilder::new().with_int("val", 2).build(),
            TupleBuilder::new().with_int("val", 3).build(),
        ]);

        let mut agg = AggregateExecutor::count_star(Box::new(input));
        let result = agg.next().unwrap().unwrap();

        assert_eq!(result.get("count"), Some(&Value::Int(3)));
        assert!(agg.next().unwrap().is_none());
    }

    #[test]
    fn test_count_column() {
        let input = MockExecutor::with_tuples(vec![
            TupleBuilder::new().with_int("val", 1).build(),
            TupleBuilder::new().with_int("val", 2).build(),
            TupleBuilder::new().with_null("val").build(),
        ]);

        let mut agg = AggregateExecutor::count(Box::new(input), "val".to_string());
        let result = agg.next().unwrap().unwrap();

        assert_eq!(result.get("count_val"), Some(&Value::Int(2)));
    }

    #[test]
    fn test_sum() {
        let input = MockExecutor::with_tuples(vec![
            TupleBuilder::new().with_int("val", 10).build(),
            TupleBuilder::new().with_int("val", 20).build(),
            TupleBuilder::new().with_int("val", 30).build(),
        ]);

        let mut agg = AggregateExecutor::sum(Box::new(input), "val".to_string());
        let result = agg.next().unwrap().unwrap();

        assert_eq!(result.get("sum_val"), Some(&Value::Int(60)));
    }

    #[test]
    fn test_avg() {
        let input = MockExecutor::with_tuples(vec![
            TupleBuilder::new().with_int("val", 10).build(),
            TupleBuilder::new().with_int("val", 20).build(),
            TupleBuilder::new().with_int("val", 30).build(),
        ]);

        let mut agg = AggregateExecutor::avg(Box::new(input), "val".to_string());
        let result = agg.next().unwrap().unwrap();

        assert_eq!(result.get("avg_val"), Some(&Value::Int(20)));
    }

    #[test]
    fn test_min() {
        let input = MockExecutor::with_tuples(vec![
            TupleBuilder::new().with_int("val", 30).build(),
            TupleBuilder::new().with_int("val", 10).build(),
            TupleBuilder::new().with_int("val", 20).build(),
        ]);

        let mut agg = AggregateExecutor::min(Box::new(input), "val".to_string());
        let result = agg.next().unwrap().unwrap();

        assert_eq!(result.get("min_val"), Some(&Value::Int(10)));
    }

    #[test]
    fn test_max() {
        let input = MockExecutor::with_tuples(vec![
            TupleBuilder::new().with_int("val", 10).build(),
            TupleBuilder::new().with_int("val", 30).build(),
            TupleBuilder::new().with_int("val", 20).build(),
        ]);

        let mut agg = AggregateExecutor::max(Box::new(input), "val".to_string());
        let result = agg.next().unwrap().unwrap();

        assert_eq!(result.get("max_val"), Some(&Value::Int(30)));
    }

    #[test]
    fn test_aggregate_empty_input() {
        let input = MockExecutor::empty();

        let mut agg = AggregateExecutor::count_star(Box::new(input));
        let result = agg.next().unwrap().unwrap();

        assert_eq!(result.get("count"), Some(&Value::Int(0)));
    }

    #[test]
    fn test_avg_empty_input() {
        let input = MockExecutor::empty();

        let mut agg = AggregateExecutor::avg(Box::new(input), "val".to_string());
        let result = agg.next().unwrap().unwrap();

        assert_eq!(result.get("avg_val"), Some(&Value::Null));
    }
}
