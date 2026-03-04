use super::{TableSchema, Tuple, UniqueValidator, Value};
use crate::parser::ast::{ColumnDef, DataType, Expr};
use crate::transaction::{Snapshot, TransactionManager};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

pub struct InsertValidator;

impl InsertValidator {
    pub fn resolve_value(
        col: &ColumnDef,
        i: usize,
        values: &[Expr],
        table: &str,
        sequences: &Arc<RwLock<HashMap<String, i64>>>,
    ) -> Result<Value, String> {
        if i < values.len() {
            Self::parse_and_validate_value(&values[i], col)
        } else if col.is_auto_increment || col.data_type == DataType::Serial {
            Self::generate_sequence(table, &col.name, sequences)
        } else if let Some(ref default_expr) = col.default_value {
            Self::parse_value(default_expr)
        } else {
            Err(format!("Column '{}' has no default value", col.name))
        }
    }

    fn parse_value(expr: &Expr) -> Result<Value, String> {
        match expr {
            Expr::Number(n) => Ok(Value::Int(*n)),
            Expr::String(s) => Ok(Value::Text(s.clone())),
            _ => Err("Invalid value expression".to_string()),
        }
    }

    fn parse_and_validate_value(expr: &Expr, col: &ColumnDef) -> Result<Value, String> {
        let val = Self::parse_value(expr)?;
        match (&col.data_type, &val) {
            (DataType::Int, Value::Int(_))
            | (DataType::Serial, Value::Int(_))
            | (DataType::Text, Value::Text(_))
            | (DataType::Varchar(_), Value::Text(_)) => Ok(val),
            _ => Err(format!("Type mismatch for column '{}'", col.name)),
        }
    }

    fn generate_sequence(
        table: &str,
        col_name: &str,
        sequences: &Arc<RwLock<HashMap<String, i64>>>,
    ) -> Result<Value, String> {
        let seq_key = format!("{}_{}", table, col_name);
        let mut seqs = sequences.write().unwrap();
        let next_val = seqs.entry(seq_key).or_insert(0);
        *next_val += 1;
        Ok(Value::Int(*next_val))
    }

    pub fn validate_not_null(schema: &TableSchema, tuple_data: &[Value]) -> Result<(), String> {
        for (i, col) in schema.columns.iter().enumerate() {
            if (col.is_not_null || col.is_primary_key) && tuple_data[i] == Value::Null {
                return Err(format!("Column '{}' cannot be NULL", col.name));
            }
        }
        Ok(())
    }

    pub fn validate_primary_key(
        schema: &TableSchema,
        tuple_data: &[Value],
        table: &str,
        data: &HashMap<String, Vec<Tuple>>,
        txn_mgr: &TransactionManager,
    ) -> Result<(), String> {
        let Some(ref pk_cols) = schema.primary_key else { return Ok(()) };

        let pk_indices: Vec<usize> = pk_cols
            .iter()
            .map(|col| schema.columns.iter().position(|c| &c.name == col).unwrap())
            .collect();

        for &idx in &pk_indices {
            if tuple_data[idx] == Value::Null {
                return Err(format!(
                    "Primary key column '{}' cannot be NULL",
                    schema.columns[idx].name
                ));
            }
        }

        if let Some(tuples) = data.get(table) {
            let snapshot = txn_mgr.get_snapshot();
            for existing in tuples {
                if existing.header.is_visible(&snapshot, txn_mgr)
                    && pk_indices.iter().all(|&idx| existing.data[idx] == tuple_data[idx])
                {
                    return Err("Primary key violation: duplicate key value".to_string());
                }
            }
        }
        Ok(())
    }

    pub fn validate_foreign_keys(
        schema: &TableSchema,
        tuple_data: &[Value],
        data: &HashMap<String, Vec<Tuple>>,
        tables: &HashMap<String, TableSchema>,
        txn_mgr: &TransactionManager,
    ) -> Result<(), String> {
        for fk in &schema.foreign_keys {
            let fk_indices: Vec<usize> = fk
                .columns
                .iter()
                .map(|col| schema.columns.iter().position(|c| &c.name == col).unwrap())
                .collect();

            let fk_values: Vec<Value> =
                fk_indices.iter().map(|&idx| tuple_data[idx].clone()).collect();

            let ref_schema = tables
                .get(&fk.ref_table)
                .ok_or_else(|| format!("Referenced table '{}' does not exist", fk.ref_table))?;

            let ref_indices: Vec<usize> = fk
                .ref_columns
                .iter()
                .map(|col| ref_schema.columns.iter().position(|c| &c.name == col).unwrap())
                .collect();

            let ref_tuples = data
                .get(&fk.ref_table)
                .ok_or_else(|| format!("Referenced table '{}' has no data", fk.ref_table))?;

            let snapshot = txn_mgr.get_snapshot();
            let found = ref_tuples.iter().any(|ref_tuple| {
                ref_tuple.header.is_visible(&snapshot, txn_mgr)
                    && ref_indices
                        .iter()
                        .enumerate()
                        .all(|(i, &ref_idx)| ref_tuple.data[ref_idx] == fk_values[i])
            });

            if !found {
                return Err(format!(
                    "Foreign key violation: referenced row does not exist in table '{}'",
                    fk.ref_table
                ));
            }
        }
        Ok(())
    }

    pub fn validate_unique(
        schema: &TableSchema,
        tuple_data: &[Value],
        table: &str,
        data: &HashMap<String, Vec<Tuple>>,
        txn_mgr: &TransactionManager,
    ) -> Result<(), String> {
        let Some(tuples) = data.get(table) else { return Ok(()) };

        let snapshot = txn_mgr.get_snapshot();
        let visible_tuples: Vec<Tuple> =
            tuples.iter().filter(|t| t.header.is_visible(&snapshot, txn_mgr)).cloned().collect();

        for (i, col) in schema.columns.iter().enumerate() {
            if col.is_unique {
                let constraint = crate::parser::ast::UniqueConstraint {
                    name: Some(format!("{}_{}_unique", table, col.name)),
                    columns: vec![col.name.clone()],
                };
                UniqueValidator::validate(&constraint, tuple_data, &visible_tuples, &[i])?;
            }
        }

        for unique_constraint in &schema.unique_constraints {
            let indices: Vec<usize> = unique_constraint
                .columns
                .iter()
                .map(|col| schema.columns.iter().position(|c| &c.name == col).unwrap())
                .collect();
            UniqueValidator::validate(unique_constraint, tuple_data, &visible_tuples, &indices)?;
        }
        Ok(())
    }
}
