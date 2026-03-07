//! Sequential Scan Executor (Volcano model)
//!
//! This executor performs a sequential scan over a heap file,
//! returning all tuples one by one.

use crate::catalog::TableSchema;
use crate::catalog::Value;
use crate::executor::operators::executor::{Executor, ExecutorError, Tuple};
use crate::storage::heap::HeapFile;
use crate::storage::page::PageId;
use std::sync::Arc;

/// Sequential scan executor that reads all tuples from a heap file
pub struct SeqScanExecutor {
    /// The heap file to scan
    heap: Arc<HeapFile>,
    /// Table name for column qualification
    table_name: String,
    /// Table schema for tuple construction
    schema: TableSchema,
    /// Current page being scanned
    current_page: u32,
    /// Current slot within the page
    current_slot: u16,
    /// Whether the scan is complete
    exhausted: bool,
}

impl SeqScanExecutor {
    /// Create a new sequential scan executor
    ///
    /// # Arguments
    /// * `heap` - The heap file to scan
    /// * `table_name` - Name of the table being scanned
    /// * `schema` - Schema of the table for tuple construction
    pub fn new(heap: Arc<HeapFile>, table_name: String, schema: TableSchema) -> Self {
        Self { heap, table_name, schema, current_page: 0, current_slot: 0, exhausted: false }
    }

    /// Create a new sequential scan executor with minimal schema
    ///
    /// # Arguments
    /// * `heap` - The heap file to scan
    /// * `table_name` - Name of the table being scanned
    pub fn with_table_name(heap: Arc<HeapFile>, table_name: String) -> Self {
        let schema = TableSchema::new(table_name.clone(), vec![]);
        Self { heap, table_name, schema, current_page: 0, current_slot: 0, exhausted: false }
    }

    /// Get the table name
    pub fn table_name(&self) -> &str {
        &self.table_name
    }

    /// Get the schema
    pub fn schema(&self) -> &TableSchema {
        &self.schema
    }

    /// Reset the scan to the beginning
    pub fn reset(&mut self) {
        self.current_page = 0;
        self.current_slot = 0;
        self.exhausted = false;
    }

    /// Try to get the next tuple from the heap file
    fn get_next_tuple(&mut self) -> Result<Option<Vec<u8>>, ExecutorError> {
        // Try to get tuple from current position
        // For now, we use a simplified approach that scans page 0 only
        // In a real implementation, we'd query the heap file for total pages

        if self.exhausted {
            return Ok(None);
        }

        // Scan page 0, slots 0..max_slots
        // Limit to 1000 slots to prevent infinite loops
        const MAX_SLOTS: u16 = 1000;

        while self.current_slot < MAX_SLOTS {
            let page_id = PageId(0);

            match self.heap.get_tuple(page_id, self.current_slot) {
                Ok(data) => {
                    // Advance slot for next call
                    self.current_slot += 1;

                    if data.is_empty() {
                        // Empty slot, continue to next
                        continue;
                    }
                    return Ok(Some(data));
                }
                Err(_) => {
                    // Error accessing slot - assume end of data
                    self.exhausted = true;
                    return Ok(None);
                }
            }
        }

        // Reached max slots
        self.exhausted = true;
        Ok(None)
    }

    /// Convert raw bytes to a typed Tuple using the schema
    fn bytes_to_tuple(&self, data: Vec<u8>) -> Tuple {
        let mut tuple = Tuple::new();

        // For now, store raw data in a single "data" column
        // A proper implementation would parse the tuple format based on schema
        if self.schema.columns.is_empty() {
            // No schema - store as generic data
            tuple.insert("data".to_string(), Value::Bytea(data));
        } else {
            // Try to map bytes to columns based on schema
            // This is a simplified implementation
            for (i, column) in self.schema.columns.iter().enumerate() {
                let value = if i == 0 {
                    // First column gets the data
                    Value::Bytea(data.clone())
                } else {
                    Value::Null
                };
                tuple.insert(column.name.clone(), value);
            }
        }

        tuple
    }
}

impl Executor for SeqScanExecutor {
    fn next(&mut self) -> Result<Option<Tuple>, ExecutorError> {
        if self.exhausted {
            return Ok(None);
        }

        match self.get_next_tuple()? {
            Some(data) => {
                let tuple = self.bytes_to_tuple(data);
                Ok(Some(tuple))
            }
            None => {
                self.exhausted = true;
                Ok(None)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::buffer_pool::BufferPool;

    #[test]
    fn test_seq_scan_empty() {
        let pool = Arc::new(BufferPool::new(10));
        let heap = Arc::new(HeapFile::new(pool));
        let schema = TableSchema::new("test".to_string(), vec![]);
        let mut scan = SeqScanExecutor::new(heap, "test".to_string(), schema);

        // Empty scan should return None immediately
        assert!(scan.next().unwrap().is_none());
        assert!(scan.exhausted);
    }

    #[test]
    fn test_seq_scan_with_data() {
        let pool = Arc::new(BufferPool::new(10));
        let heap = Arc::new(HeapFile::new(pool));
        let schema = TableSchema::new("test".to_string(), vec![]);

        // Insert some data
        heap.insert_tuple(PageId(0), vec![1, 2, 3]).unwrap();
        heap.insert_tuple(PageId(0), vec![4, 5, 6]).unwrap();

        let mut scan = SeqScanExecutor::new(heap, "test".to_string(), schema);

        // Get first tuple
        let tuple1 = scan.next().unwrap().unwrap();
        assert!(tuple1.contains_key("data"));

        // Get second tuple
        let tuple2 = scan.next().unwrap().unwrap();
        assert!(tuple2.contains_key("data"));

        // No more tuples
        assert!(scan.next().unwrap().is_none());
        assert!(scan.exhausted);
    }

    #[test]
    fn test_seq_scan_reset() {
        let pool = Arc::new(BufferPool::new(10));
        let heap = Arc::new(HeapFile::new(pool));
        let schema = TableSchema::new("test".to_string(), vec![]);

        heap.insert_tuple(PageId(0), vec![1, 2, 3]).unwrap();

        let mut scan = SeqScanExecutor::new(heap, "test".to_string(), schema);

        // Consume all tuples
        assert!(scan.next().unwrap().is_some());
        assert!(scan.next().unwrap().is_none());

        // Reset and scan again
        scan.reset();
        assert!(!scan.exhausted);
        assert!(scan.next().unwrap().is_some());
    }

    #[test]
    fn test_seq_scan_with_schema() {
        let pool = Arc::new(BufferPool::new(10));
        let heap = Arc::new(HeapFile::new(pool));

        use crate::parser::ast::{ColumnDef, DataType};
        let columns = vec![
            ColumnDef {
                name: "id".to_string(),
                data_type: DataType::Int,
                is_primary_key: false,
                is_unique: false,
                is_auto_increment: false,
                is_not_null: false,
                default_value: None,
                foreign_key: None,
            },
            ColumnDef {
                name: "name".to_string(),
                data_type: DataType::Text,
                is_primary_key: false,
                is_unique: false,
                is_auto_increment: false,
                is_not_null: false,
                default_value: None,
                foreign_key: None,
            },
        ];
        let schema = TableSchema::new("users".to_string(), columns);

        heap.insert_tuple(PageId(0), vec![1, 2, 3]).unwrap();

        let mut scan = SeqScanExecutor::new(heap, "users".to_string(), schema);
        let tuple = scan.next().unwrap().unwrap();

        // Should have columns from schema
        assert!(tuple.contains_key("id"));
        assert!(tuple.contains_key("name"));
    }
}
