use super::{TableSchema, Tuple, Value};
use crate::catalog::predicate::PredicateEvaluator;
use crate::parser::ast::{DataType, Expr};
use crate::transaction::{Snapshot, TransactionManager, TupleHeader};
use std::sync::Arc;

pub struct UpdateDeleteExecutor;

impl UpdateDeleteExecutor {
    pub fn update(
        tuples: &mut [Tuple],
        assignments: &[(String, Expr)],
        where_clause: &Option<Expr>,
        schema: &TableSchema,
        snapshot: &Snapshot,
        txn_mgr: &Arc<TransactionManager>,
    ) -> Result<usize, String> {
        let mut updated = 0;
        for tuple in tuples.iter_mut() {
            if !tuple.header.is_visible(snapshot, txn_mgr) {
                continue;
            }

            if let Some(ref predicate) = where_clause {
                if !PredicateEvaluator::evaluate(predicate, &tuple.data, schema)? {
                    continue;
                }
            }

            Self::apply_assignments(tuple, assignments, schema)?;
            updated += 1;
        }
        Ok(updated)
    }

    fn apply_assignments(
        tuple: &mut Tuple,
        assignments: &[(String, Expr)],
        schema: &TableSchema,
    ) -> Result<(), String> {
        for (col_name, expr) in assignments {
            let idx = schema
                .columns
                .iter()
                .position(|c| &c.name == col_name)
                .ok_or_else(|| format!("Column '{}' not found", col_name))?;

            let value = match expr {
                Expr::Number(n) => Value::Int(*n),
                Expr::String(s) => Value::Text(s.clone()),
                _ => return Err("Invalid value expression".to_string()),
            };

            Self::validate_type(&schema.columns[idx].data_type, &value, col_name)?;
            tuple.data[idx] = value;
        }
        Ok(())
    }

    fn validate_type(data_type: &DataType, value: &Value, col_name: &str) -> Result<(), String> {
        match (data_type, value) {
            (DataType::Int, Value::Int(_))
            | (DataType::Text, Value::Text(_))
            | (DataType::Varchar(_), Value::Text(_)) => Ok(()),
            _ => Err(format!("Type mismatch for column '{}'", col_name)),
        }
    }

    pub fn delete(
        tuples: &mut [Tuple],
        where_clause: &Option<Expr>,
        schema: &TableSchema,
        snapshot: &Snapshot,
        txn_mgr: &Arc<TransactionManager>,
        xid: u64,
    ) -> Result<usize, String> {
        let mut deleted = 0;
        for tuple in tuples.iter_mut() {
            if !tuple.header.is_visible(snapshot, txn_mgr) {
                continue;
            }

            if let Some(ref predicate) = where_clause {
                if !PredicateEvaluator::evaluate(predicate, &tuple.data, schema)? {
                    continue;
                }
            }

            tuple.header.delete(xid);
            deleted += 1;
        }
        Ok(deleted)
    }
}
