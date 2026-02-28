//! Edge case tests for query executor

#[cfg(test)]
mod tests {
    use crate::executor::*;
    use std::collections::HashMap;

    struct EmptyExecutor;

    impl Executor for EmptyExecutor {
        fn open(&mut self) -> Result<(), ExecutorError> {
            Ok(())
        }

        fn next(&mut self) -> Result<Option<Tuple>, ExecutorError> {
            Ok(None)
        }

        fn close(&mut self) -> Result<(), ExecutorError> {
            Ok(())
        }
    }

    struct ErrorExecutor;

    impl Executor for ErrorExecutor {
        fn open(&mut self) -> Result<(), ExecutorError> {
            Err(ExecutorError::Storage("Open failed".to_string()))
        }

        fn next(&mut self) -> Result<Option<Tuple>, ExecutorError> {
            Err(ExecutorError::Storage("Next failed".to_string()))
        }

        fn close(&mut self) -> Result<(), ExecutorError> {
            Ok(())
        }
    }

    #[test]
    fn test_empty_executor() {
        let mut exec = EmptyExecutor;
        exec.open().unwrap();
        let result = exec.next().unwrap();
        assert!(result.is_none());
        exec.close().unwrap();
    }

    #[test]
    fn test_executor_open_error() {
        let mut exec = ErrorExecutor;
        assert!(exec.open().is_err());
    }

    #[test]
    fn test_executor_next_error() {
        let mut exec = ErrorExecutor;
        assert!(exec.next().is_err());
    }

    #[test]
    fn test_tuple_with_empty_columns() {
        let tuple: Tuple = HashMap::new();
        assert_eq!(tuple.len(), 0);
    }

    #[test]
    fn test_tuple_with_empty_values() {
        let mut tuple: Tuple = HashMap::new();
        tuple.insert("col1".to_string(), vec![]);
        assert_eq!(tuple.get("col1").unwrap().len(), 0);
    }

    #[test]
    fn test_tuple_with_large_values() {
        let mut tuple: Tuple = HashMap::new();
        tuple.insert("col1".to_string(), vec![0u8; 1_000_000]);
        assert_eq!(tuple.get("col1").unwrap().len(), 1_000_000);
    }

    #[test]
    fn test_simple_tuple_empty() {
        let tuple = SimpleTuple { data: vec![] };
        assert_eq!(tuple.data.len(), 0);
    }

    #[test]
    fn test_executor_error_display() {
        let err = ExecutorError::ColumnNotFound("test_col".to_string());
        let msg = format!("{}", err);
        assert!(msg.contains("test_col"));
    }

    #[test]
    fn test_executor_error_storage() {
        let err = ExecutorError::Storage("disk error".to_string());
        let msg = format!("{}", err);
        assert!(msg.contains("disk error"));
    }

    #[test]
    fn test_executor_error_type_mismatch() {
        let err = ExecutorError::TypeMismatch("expected int, got string".to_string());
        let msg = format!("{}", err);
        assert!(msg.contains("expected int"));
    }

    #[test]
    fn test_tuple_column_access_nonexistent() {
        let tuple: Tuple = HashMap::new();
        assert!(tuple.get("nonexistent").is_none());
    }

    #[test]
    fn test_multiple_close_calls() {
        let mut exec = EmptyExecutor;
        exec.open().unwrap();
        exec.close().unwrap();
        exec.close().unwrap();
        exec.close().unwrap();
    }
}
