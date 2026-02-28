use crate::parser::ast::{ColumnDef, DataType, Expr, OrderByExpr, AggregateFunc};
use crate::transaction::{TransactionManager, TupleHeader};
use super::{Value, TableSchema, Tuple};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::fs::{File, create_dir_all};
use std::io::{Write, Read, BufWriter, BufReader};
use std::path::Path;

pub struct Catalog {
    tables: Arc<RwLock<HashMap<String, TableSchema>>>,
    data: Arc<RwLock<HashMap<String, Vec<Tuple>>>>,
    txn_mgr: Arc<TransactionManager>,
    data_dir: Option<String>,
}

impl Catalog {
    pub fn new() -> Self {
        Self {
            tables: Arc::new(RwLock::new(HashMap::new())),
            data: Arc::new(RwLock::new(HashMap::new())),
            txn_mgr: Arc::new(TransactionManager::new()),
            data_dir: None,
        }
    }
    
    pub fn new_with_data_dir(data_dir: &str) -> Self {
        let catalog = Self {
            tables: Arc::new(RwLock::new(HashMap::new())),
            data: Arc::new(RwLock::new(HashMap::new())),
            txn_mgr: Arc::new(TransactionManager::new()),
            data_dir: Some(data_dir.to_string()),
        };
        if let Err(e) = catalog.load_from_disk(data_dir) {
            log::error!("Failed to load catalog: {}", e);
        }
        catalog
    }
    
    fn auto_save(&self) {
        if let Some(ref dir) = self.data_dir {
            // Clone data while holding locks briefly
            let tables_clone = self.tables.read().unwrap().clone();
            let data_clone = self.data.read().unwrap().clone();
            
            // Save without holding locks
            if let Err(e) = Self::save_to_disk_static(dir, &tables_clone, &data_clone) {
                log::error!("Auto-save failed: {}", e);
            }
        }
    }
    
    pub fn create_table(&self, name: String, columns: Vec<ColumnDef>) -> Result<(), String> {
        let mut tables = self.tables.write().unwrap();
        
        if tables.contains_key(&name) {
            return Err(format!("Table '{}' already exists", name));
        }
        
        tables.insert(name.clone(), TableSchema { name: name.clone(), columns });
        drop(tables); // Release lock before auto_save
        
        let mut data = self.data.write().unwrap();
        data.insert(name, Vec::new());
        drop(data); // Release lock before auto_save
        
        self.auto_save();
        Ok(())
    }
    
    pub fn drop_table(&self, name: &str, if_exists: bool) -> Result<(), String> {
        let mut tables = self.tables.write().unwrap();
        
        if tables.remove(name).is_none() && !if_exists {
            return Err(format!("Table '{}' does not exist", name));
        }
        drop(tables); // Release lock
        
        let mut data = self.data.write().unwrap();
        data.remove(name);
        drop(data); // Release lock
        
        self.auto_save();
        Ok(())
    }
    
    pub fn get_table(&self, name: &str) -> Option<TableSchema> {
        let tables = self.tables.read().unwrap();
        tables.get(name).cloned()
    }
    
    pub fn list_tables(&self) -> Vec<String> {
        let tables = self.tables.read().unwrap();
        tables.keys().cloned().collect()
    }
    
    pub fn insert(&self, table: &str, values: Vec<Expr>) -> Result<(), String> {
        let schema = self.get_table(table)
            .ok_or_else(|| format!("Table '{}' does not exist", table))?;
        
        if values.len() != schema.columns.len() {
            return Err(format!("Expected {} values, got {}", schema.columns.len(), values.len()));
        }
        
        let txn = self.txn_mgr.begin();
        let header = TupleHeader::new(txn.xid);
        
        let mut tuple_data = Vec::new();
        for (i, expr) in values.iter().enumerate() {
            let value = match expr {
                Expr::Number(n) => Value::Int(*n),
                Expr::String(s) => Value::Text(s.clone()),
                _ => return Err("Invalid value expression".to_string()),
            };
            
            match (&schema.columns[i].data_type, &value) {
                (DataType::Int, Value::Int(_)) => {},
                (DataType::Text, Value::Text(_)) => {},
                (DataType::Varchar(_), Value::Text(_)) => {},
                _ => return Err(format!("Type mismatch for column '{}'", schema.columns[i].name)),
            }
            
            tuple_data.push(value);
        }
        
        let tuple = Tuple { header, data: tuple_data };
        
        let mut data = self.data.write().unwrap();
        data.get_mut(table).unwrap().push(tuple);
        drop(data); // Release lock before commit and auto_save
        
        self.txn_mgr.commit(txn.xid).map_err(|e| e.to_string())?;
        self.auto_save();
        Ok(())
    }
    
    pub fn row_count(&self, table: &str) -> usize {
        let data = self.data.read().unwrap();
        data.get(table).map(|rows| rows.len()).unwrap_or(0)
    }
    
    pub fn select(&self, table: &str, distinct: bool, columns: Vec<String>, where_clause: Option<Expr>, group_by: Option<Vec<String>>, having: Option<Expr>, order_by: Option<Vec<OrderByExpr>>, limit: Option<usize>, offset: Option<usize>) -> Result<Vec<Vec<Value>>, String> {
        let schema = self.get_table(table)
            .ok_or_else(|| format!("Table '{}' does not exist", table))?;
        
        // Check if this is an aggregate query
        if columns.len() == 1 && columns[0].starts_with("AGG:") {
            return self.execute_aggregate(table, &columns[0], where_clause);
        }
        
        let data = self.data.read().unwrap();
        let tuples = data.get(table).ok_or_else(|| format!("Table '{}' has no data", table))?;
        
        let snapshot = self.txn_mgr.get_snapshot();
        let mut results = Vec::new();
        
        for tuple in tuples {
            if tuple.header.is_visible(&snapshot, &self.txn_mgr) {
                // Evaluate WHERE clause
                if let Some(ref predicate) = where_clause {
                    if !self.evaluate_predicate(predicate, &tuple.data, &schema)? {
                        continue;
                    }
                }
                
                if columns.is_empty() || columns[0] == "*" {
                    results.push(tuple.data.clone());
                } else {
                    let mut row = Vec::new();
                    for col_name in &columns {
                        if let Some(idx) = schema.columns.iter().position(|c| &c.name == col_name) {
                            row.push(tuple.data[idx].clone());
                        } else {
                            return Err(format!("Column '{}' not found", col_name));
                        }
                    }
                    results.push(row);
                }
            }
        }
        
        // Apply GROUP BY if specified
        if let Some(group_cols) = group_by {
            results = self.apply_group_by(results, &group_cols, &columns, &schema)?;
            
            // Apply HAVING if specified
            if let Some(having_expr) = having {
                results.retain(|row| {
                    self.evaluate_having(&having_expr, row).unwrap_or(false)
                });
            }
        }
        
        // Apply ORDER BY if specified
        if let Some(order_by_exprs) = order_by {
            for order_expr in order_by_exprs.iter().rev() {
                let col_idx = schema.columns.iter().position(|c| c.name == order_expr.column)
                    .ok_or_else(|| format!("Column '{}' not found", order_expr.column))?;
                
                results.sort_by(|a, b| {
                    let cmp = a[col_idx].cmp(&b[col_idx]);
                    if order_expr.ascending { cmp } else { cmp.reverse() }
                });
            }
        }
        
        // Apply OFFSET and LIMIT
        let start = offset.unwrap_or(0);
        let end = limit.map(|l| start + l).unwrap_or(results.len());
        results = results.into_iter().skip(start).take(end - start).collect();
        
        // Apply DISTINCT if specified
        if distinct {
            let mut seen = std::collections::HashSet::new();
            results.retain(|row| seen.insert(row.clone()));
        }
        
        Ok(results)
    }
    
    
    fn apply_group_by(&self, rows: Vec<Vec<Value>>, group_cols: &[String], select_cols: &[String], schema: &TableSchema) -> Result<Vec<Vec<Value>>, String> {
        use std::collections::HashMap;
        
        let mut groups: HashMap<Vec<Value>, Vec<Vec<Value>>> = HashMap::new();
        
        for row in rows {
            let mut key = Vec::new();
            for col_name in group_cols {
                let idx = schema.columns.iter().position(|c| &c.name == col_name)
                    .ok_or_else(|| format!("Column '{}' not found", col_name))?;
                key.push(row[idx].clone());
            }
            groups.entry(key).or_insert_with(Vec::new).push(row);
        }
        
        let mut result = Vec::new();
        for (key, group_rows) in groups {
            let mut row = Vec::new();
            for col_name in select_cols {
                if group_cols.contains(col_name) {
                    let idx = group_cols.iter().position(|c| c == col_name).unwrap();
                    row.push(key[idx].clone());
                } else {
                    row.push(Value::Int(group_rows.len() as i64));
                }
            }
            result.push(row);
        }
        
        Ok(result)
    }
    
    
    fn evaluate_having(&self, expr: &Expr, row: &[Value]) -> Result<bool, String> {
        match expr {
            Expr::BinaryOp { left, op, right } => {
                let left_val = match **left {
                    Expr::Number(n) => Value::Int(n),
                    _ => row.get(0).cloned().unwrap_or(Value::Int(0)),
                };
                let right_val = match **right {
                    Expr::Number(n) => Value::Int(n),
                    _ => Value::Int(0),
                };
                
                use crate::parser::ast::BinaryOperator;
                match op {
                    BinaryOperator::GreaterThan => Ok(left_val > right_val),
                    BinaryOperator::GreaterThanOrEqual => Ok(left_val >= right_val),
                    BinaryOperator::LessThan => Ok(left_val < right_val),
                    BinaryOperator::LessThanOrEqual => Ok(left_val <= right_val),
                    BinaryOperator::Equals => Ok(left_val == right_val),
                    BinaryOperator::NotEquals => Ok(left_val != right_val),
                    _ => Ok(false),
                }
            }
            _ => Ok(false),
        }
    }
    
    fn evaluate_predicate(&self, expr: &Expr, tuple: &[Value], schema: &TableSchema) -> Result<bool, String> {
        use crate::parser::ast::BinaryOperator;
        
        match expr {
            Expr::BinaryOp { left, op, right } => {
                match op {
                    BinaryOperator::In => {
                        let left_val = self.evaluate_expr(left, tuple, schema)?;
                        if let Expr::List(values) = &**right {
                            for val_expr in values {
                                let val = self.evaluate_expr(val_expr, tuple, schema)?;
                                if left_val == val {
                                    return Ok(true);
                                }
                            }
                            return Ok(false);
                        }
                        Err("IN requires list of values".to_string())
                    },
                    BinaryOperator::Between => {
                        let left_val = self.evaluate_expr(left, tuple, schema)?;
                        if let Expr::List(values) = &**right {
                            if values.len() == 2 {
                                let lower = self.evaluate_expr(&values[0], tuple, schema)?;
                                let upper = self.evaluate_expr(&values[1], tuple, schema)?;
                                return Ok(left_val >= lower && left_val <= upper);
                            }
                        }
                        Err("BETWEEN requires two values".to_string())
                    },
                    BinaryOperator::And => {
                        let left_result = self.evaluate_predicate(left, tuple, schema)?;
                        let right_result = self.evaluate_predicate(right, tuple, schema)?;
                        Ok(left_result && right_result)
                    },
                    BinaryOperator::Or => {
                        let left_result = self.evaluate_predicate(left, tuple, schema)?;
                        let right_result = self.evaluate_predicate(right, tuple, schema)?;
                        Ok(left_result || right_result)
                    },
                    _ => {
                        let left_val = self.evaluate_expr(left, tuple, schema)?;
                        let right_val = self.evaluate_expr(right, tuple, schema)?;
                        
                        match op {
                            BinaryOperator::Equals => Ok(left_val == right_val),
                            BinaryOperator::NotEquals => Ok(left_val != right_val),
                            BinaryOperator::LessThan => match (&left_val, &right_val) {
                                (Value::Int(l), Value::Int(r)) => Ok(l < r),
                                (Value::Text(l), Value::Text(r)) => Ok(l < r),
                                _ => Err("Type mismatch in comparison".to_string()),
                            },
                            BinaryOperator::LessThanOrEqual => match (&left_val, &right_val) {
                                (Value::Int(l), Value::Int(r)) => Ok(l <= r),
                                (Value::Text(l), Value::Text(r)) => Ok(l <= r),
                                _ => Err("Type mismatch in comparison".to_string()),
                            },
                            BinaryOperator::GreaterThan => match (&left_val, &right_val) {
                                (Value::Int(l), Value::Int(r)) => Ok(l > r),
                                (Value::Text(l), Value::Text(r)) => Ok(l > r),
                                _ => Err("Type mismatch in comparison".to_string()),
                            },
                            BinaryOperator::GreaterThanOrEqual => match (&left_val, &right_val) {
                                (Value::Int(l), Value::Int(r)) => Ok(l >= r),
                                (Value::Text(l), Value::Text(r)) => Ok(l >= r),
                                _ => Err("Type mismatch in comparison".to_string()),
                            },
                            BinaryOperator::Like => match (&left_val, &right_val) {
                                (Value::Text(s), Value::Text(pattern)) => {
                                    Ok(s.contains(&pattern.replace('%', "")))
                                }
                                _ => Err("LIKE requires text values".to_string()),
                            },
                            BinaryOperator::In | BinaryOperator::Between => {
                                Err("IN/BETWEEN handled separately".to_string())
                            },
                            _ => unreachable!(),
                        }
                    }
                }
            }
            Expr::UnaryOp { op, expr } => {
                use crate::parser::ast::UnaryOperator;
                match op {
                    UnaryOperator::Not => {
                        let result = self.evaluate_predicate(expr, tuple, schema)?;
                        Ok(!result)
                    }
                    _ => Err("Unsupported unary operator".to_string()),
                }
            }
            Expr::IsNull(expr) => {
                let val = self.evaluate_expr(expr, tuple, schema)?;
                Ok(matches!(val, Value::Null))
            }
            Expr::IsNotNull(expr) => {
                let val = self.evaluate_expr(expr, tuple, schema)?;
                Ok(!matches!(val, Value::Null))
            }
            _ => Err("Unsupported predicate expression".to_string()),
        }
    }
    
    fn evaluate_expr(&self, expr: &Expr, tuple: &[Value], schema: &TableSchema) -> Result<Value, String> {
        match expr {
            Expr::Column(name) => {
                let idx = schema.columns.iter().position(|c| &c.name == name)
                    .ok_or_else(|| format!("Column '{}' not found", name))?;
                Ok(tuple[idx].clone())
            }
            Expr::Number(n) => Ok(Value::Int(*n)),
            Expr::String(s) => Ok(Value::Text(s.clone())),
            Expr::List(_) => Err("List not evaluable as value".to_string()),
            _ => Err("Unsupported expression".to_string()),
        }
    }
    
    pub fn update(&self, table: &str, assignments: Vec<(String, Expr)>, where_clause: Option<Expr>) -> Result<usize, String> {
        let schema = self.get_table(table)
            .ok_or_else(|| format!("Table '{}' does not exist", table))?;
        
        let txn = self.txn_mgr.begin();
        let snapshot = txn.snapshot.clone();
        
        let mut data = self.data.write().unwrap();
        let tuples = data.get_mut(table).ok_or_else(|| format!("Table '{}' has no data", table))?;
        
        let mut updated = 0;
        for tuple in tuples.iter_mut() {
            if tuple.header.is_visible(&snapshot, &self.txn_mgr) {
                // Evaluate WHERE clause
                if let Some(ref predicate) = where_clause {
                    if !self.evaluate_predicate(predicate, &tuple.data, &schema)? {
                        continue;
                    }
                }
                
                for (col_name, expr) in &assignments {
                    let idx = schema.columns.iter().position(|c| &c.name == col_name)
                        .ok_or_else(|| format!("Column '{}' not found", col_name))?;
                    
                    let value = match expr {
                        Expr::Number(n) => Value::Int(*n),
                        Expr::String(s) => Value::Text(s.clone()),
                        _ => return Err("Invalid value expression".to_string()),
                    };
                    
                    match (&schema.columns[idx].data_type, &value) {
                        (DataType::Int, Value::Int(_)) => {},
                        (DataType::Text, Value::Text(_)) => {},
                        (DataType::Varchar(_), Value::Text(_)) => {},
                        _ => return Err(format!("Type mismatch for column '{}'", col_name)),
                    }
                    
                    tuple.data[idx] = value;
                }
                updated += 1;
            }
        }
        
        self.txn_mgr.commit(txn.xid).map_err(|e| e.to_string())?;
        self.auto_save();
        Ok(updated)
    }
    
    pub fn delete(&self, table: &str, where_clause: Option<Expr>) -> Result<usize, String> {
        let schema = self.get_table(table)
            .ok_or_else(|| format!("Table '{}' does not exist", table))?;
        
        let txn = self.txn_mgr.begin();
        let snapshot = txn.snapshot.clone();
        
        let mut data = self.data.write().unwrap();
        let tuples = data.get_mut(table).ok_or_else(|| format!("Table '{}' has no data", table))?;
        
        let mut deleted = 0;
        for tuple in tuples.iter_mut() {
            if tuple.header.is_visible(&snapshot, &self.txn_mgr) {
                // Evaluate WHERE clause
                if let Some(ref predicate) = where_clause {
                    if !self.evaluate_predicate(predicate, &tuple.data, &schema)? {
                        continue;
                    }
                }
                
                tuple.header.delete(txn.xid);
                deleted += 1;
            }
        }
        
        self.txn_mgr.commit(txn.xid).map_err(|e| e.to_string())?;
        self.auto_save();
        Ok(deleted)
    }
    
    fn execute_aggregate(&self, table: &str, agg_spec: &str, where_clause: Option<Expr>) -> Result<Vec<Vec<Value>>, String> {
        let schema = self.get_table(table)
            .ok_or_else(|| format!("Table '{}' does not exist", table))?;
        
        let data = self.data.read().unwrap();
        let tuples = data.get(table).ok_or_else(|| format!("Table '{}' has no data", table))?;
        
        let snapshot = self.txn_mgr.get_snapshot();
        
        // Parse aggregate spec: "AGG:FUNC:COLUMN"
        let parts: Vec<&str> = agg_spec.split(':').collect();
        if parts.len() < 2 {
            return Err("Invalid aggregate specification".to_string());
        }
        
        let func = parts[1];
        let col_name = if parts.len() > 2 { Some(parts[2]) } else { None };
        
        let mut values = Vec::new();
        for tuple in tuples {
            if tuple.header.is_visible(&snapshot, &self.txn_mgr) {
                if let Some(ref predicate) = where_clause {
                    if !self.evaluate_predicate(predicate, &tuple.data, &schema)? {
                        continue;
                    }
                }
                
                if func == "COUNT" {
                    values.push(Value::Int(1));
                } else if let Some(col) = col_name {
                    let idx = schema.columns.iter().position(|c| c.name == col)
                        .ok_or_else(|| format!("Column '{}' not found", col))?;
                    values.push(tuple.data[idx].clone());
                }
            }
        }
        
        let result = match func {
            "COUNT" => Value::Int(values.len() as i64),
            "SUM" => {
                let sum: i64 = values.iter().filter_map(|v| {
                    if let Value::Int(n) = v { Some(*n) } else { None }
                }).sum();
                Value::Int(sum)
            }
            "AVG" => {
                let nums: Vec<i64> = values.iter().filter_map(|v| {
                    if let Value::Int(n) = v { Some(*n) } else { None }
                }).collect();
                if nums.is_empty() {
                    Value::Int(0)
                } else {
                    Value::Int(nums.iter().sum::<i64>() / nums.len() as i64)
                }
            }
            "MIN" => {
                values.iter().min().cloned().unwrap_or(Value::Int(0))
            }
            "MAX" => {
                values.iter().max().cloned().unwrap_or(Value::Int(0))
            }
            _ => return Err(format!("Unknown aggregate function: {}", func)),
        };
        
        Ok(vec![vec![result]])
    }
}

impl Default for Catalog {
    fn default() -> Self {
        Self::new()
    }
}

// Persistence implementation
impl Catalog {
    fn save_to_disk_static(data_dir: &str, tables: &HashMap<String, TableSchema>, data: &HashMap<String, Vec<Tuple>>) -> Result<(), String> {
        create_dir_all(data_dir).map_err(|e| format!("Failed to create data dir: {}", e))?;
        
        let catalog_path = format!("{}/catalog.bin", data_dir);
        let file = File::create(&catalog_path)
            .map_err(|e| format!("Failed to create catalog file: {}", e))?;
        let mut writer = BufWriter::new(file);
        
        // Write number of tables
        write_u32(&mut writer, tables.len() as u32)?;
        
        for (table_name, schema) in tables.iter() {
            // Write table name
            write_string(&mut writer, table_name)?;
            
            // Write schema
            write_u32(&mut writer, schema.columns.len() as u32)?;
            for col in &schema.columns {
                write_string(&mut writer, &col.name)?;
                write_data_type(&mut writer, &col.data_type)?;
            }
            
            // Write tuples
            let tuples = data.get(table_name).map(|t| t.as_slice()).unwrap_or(&[]);
            write_u32(&mut writer, tuples.len() as u32)?;
            
            for tuple in tuples {
                write_u32(&mut writer, tuple.data.len() as u32)?;
                for value in &tuple.data {
                    write_value(&mut writer, value)?;
                }
            }
        }
        
        writer.flush().map_err(|e| format!("Failed to flush: {}", e))?;
        log::info!("💾 Saved {} tables to {}", tables.len(), catalog_path);
        Ok(())
    }
    
    pub fn save_to_disk(&self, data_dir: &str) -> Result<(), String> {
        let tables = self.tables.read().unwrap();
        let data = self.data.read().unwrap();
        Self::save_to_disk_static(data_dir, &tables, &data)
    }
    
    pub fn load_from_disk(&self, data_dir: &str) -> Result<(), String> {
        let catalog_path = format!("{}/catalog.bin", data_dir);
        
        if !Path::new(&catalog_path).exists() {
            log::info!("📂 No existing catalog found, starting fresh");
            return Ok(());
        }
        
        let file = File::open(&catalog_path)
            .map_err(|e| format!("Failed to open catalog file: {}", e))?;
        let mut reader = BufReader::new(file);
        
        let num_tables = read_u32(&mut reader)?;
        
        let mut tables = self.tables.write().unwrap();
        let mut data = self.data.write().unwrap();
        
        for _ in 0..num_tables {
            // Read table name
            let table_name = read_string(&mut reader)?;
            
            // Read schema
            let num_columns = read_u32(&mut reader)?;
            let mut columns = Vec::new();
            
            for _ in 0..num_columns {
                let col_name = read_string(&mut reader)?;
                let data_type = read_data_type(&mut reader)?;
                columns.push(ColumnDef {
                    name: col_name,
                    data_type,
                });
            }
            
            let schema = TableSchema {
                name: table_name.clone(),
                columns,
            };
            
            // Read tuples
            let num_tuples = read_u32(&mut reader)?;
            let mut tuples = Vec::new();
            
            for _ in 0..num_tuples {
                let num_values = read_u32(&mut reader)?;
                let mut values = Vec::new();
                
                for _ in 0..num_values {
                    values.push(read_value(&mut reader)?);                }
                
                // Create tuple with default header (visible to all)
                let txn = self.txn_mgr.begin();
                let header = TupleHeader::new(txn.xid);
                self.txn_mgr.commit(txn.xid).map_err(|e| e.to_string())?;
                
                tuples.push(Tuple {
                    header,
                    data: values,
                });
            }
            
            tables.insert(table_name.clone(), schema);
            data.insert(table_name.clone(), tuples);
        }
        
        log::info!("📂 Loaded {} tables from {}", num_tables, catalog_path);
        Ok(())
    }
}

// Helper functions for binary serialization

fn write_u32<W: Write>(writer: &mut W, value: u32) -> Result<(), String> {
    writer.write_all(&value.to_le_bytes())
        .map_err(|e| format!("Write error: {}", e))
}

fn read_u32<R: Read>(reader: &mut R) -> Result<u32, String> {
    let mut buf = [0u8; 4];
    reader.read_exact(&mut buf)
        .map_err(|e| format!("Read error: {}", e))?;
    Ok(u32::from_le_bytes(buf))
}

fn write_string<W: Write>(writer: &mut W, s: &str) -> Result<(), String> {
    write_u32(writer, s.len() as u32)?;
    writer.write_all(s.as_bytes())
        .map_err(|e| format!("Write error: {}", e))
}

fn read_string<R: Read>(reader: &mut R) -> Result<String, String> {
    let len = read_u32(reader)?;
    let mut buf = vec![0u8; len as usize];
    reader.read_exact(&mut buf)
        .map_err(|e| format!("Read error: {}", e))?;
    String::from_utf8(buf)
        .map_err(|e| format!("UTF-8 error: {}", e))
}

fn write_data_type<W: Write>(writer: &mut W, dt: &DataType) -> Result<(), String> {
    match dt {
        DataType::Int => {
            writer.write_all(&[0]).map_err(|e| format!("Write error: {}", e))?;
        }
        DataType::Text => {
            writer.write_all(&[1]).map_err(|e| format!("Write error: {}", e))?;
        }
        DataType::Varchar(len) => {
            writer.write_all(&[2]).map_err(|e| format!("Write error: {}", e))?;
            write_u32(writer, *len)?;
        }
    }
    Ok(())
}

fn read_data_type<R: Read>(reader: &mut R) -> Result<DataType, String> {
    let mut buf = [0u8; 1];
    reader.read_exact(&mut buf)
        .map_err(|e| format!("Read error: {}", e))?;
    
    match buf[0] {
        0 => Ok(DataType::Int),
        1 => Ok(DataType::Text),
        2 => {
            let len = read_u32(reader)?;
            Ok(DataType::Varchar(len))
        }
        _ => Err(format!("Unknown data type: {}", buf[0])),
    }
}

fn write_value<W: Write>(writer: &mut W, value: &Value) -> Result<(), String> {
    match value {
        Value::Int(n) => {
            writer.write_all(&[0]).map_err(|e| format!("Write error: {}", e))?;
            writer.write_all(&n.to_le_bytes())
                .map_err(|e| format!("Write error: {}", e))?;
        }
        Value::Text(s) => {
            writer.write_all(&[1]).map_err(|e| format!("Write error: {}", e))?;
            write_string(writer, s)?;
        }
        Value::Null => {
            writer.write_all(&[2]).map_err(|e| format!("Write error: {}", e))?;
        }
    }
    Ok(())
}

fn read_value<R: Read>(reader: &mut R) -> Result<Value, String> {
    let mut buf = [0u8; 1];
    reader.read_exact(&mut buf)
        .map_err(|e| format!("Read error: {}", e))?;
    
    match buf[0] {
        0 => {
            let mut buf = [0u8; 8];
            reader.read_exact(&mut buf)
                .map_err(|e| format!("Read error: {}", e))?;
            Ok(Value::Int(i64::from_le_bytes(buf)))
        }
        1 => {
            let s = read_string(reader)?;
            Ok(Value::Text(s))
        }
        2 => Ok(Value::Null),
        _ => Err(format!("Unknown value type: {}", buf[0])),
    }
}
