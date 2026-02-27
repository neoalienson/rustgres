use super::executor::{Executor, ExecutorError, Tuple};

pub struct Project {
    child: Box<dyn Executor>,
    columns: Vec<String>,
}

impl Project {
    pub fn new(child: Box<dyn Executor>, columns: Vec<String>) -> Self {
        Self { child, columns }
    }
}

impl Executor for Project {
    fn open(&mut self) -> Result<(), ExecutorError> {
        self.child.open()
    }

    fn next(&mut self) -> Result<Option<Tuple>, ExecutorError> {
        match self.child.next()? {
            None => Ok(None),
            Some(tuple) => {
                let mut result = Tuple::new();
                for col in &self.columns {
                    if col == "*" {
                        return Ok(Some(tuple));
                    }
                    if let Some(val) = tuple.get(col) {
                        result.insert(col.clone(), val.clone());
                    } else {
                        return Err(ExecutorError::ColumnNotFound(col.clone()));
                    }
                }
                Ok(Some(result))
            }
        }
    }

    fn close(&mut self) -> Result<(), ExecutorError> {
        self.child.close()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    struct MockExecutor {
        tuples: Vec<Tuple>,
        index: usize,
    }

    impl MockExecutor {
        fn new(tuples: Vec<Tuple>) -> Self {
            Self { tuples, index: 0 }
        }
    }

    impl Executor for MockExecutor {
        fn open(&mut self) -> Result<(), ExecutorError> {
            self.index = 0;
            Ok(())
        }

        fn next(&mut self) -> Result<Option<Tuple>, ExecutorError> {
            if self.index < self.tuples.len() {
                let tuple = self.tuples[self.index].clone();
                self.index += 1;
                Ok(Some(tuple))
            } else {
                Ok(None)
            }
        }

        fn close(&mut self) -> Result<(), ExecutorError> {
            Ok(())
        }
    }

    #[test]
    fn test_project_single_column() {
        let mut tuple = HashMap::new();
        tuple.insert("id".to_string(), b"1".to_vec());
        tuple.insert("name".to_string(), b"test".to_vec());

        let mock = MockExecutor::new(vec![tuple]);
        let mut project = Project::new(Box::new(mock), vec!["id".to_string()]);
        
        project.open().unwrap();
        let result = project.next().unwrap().unwrap();
        
        assert_eq!(result.len(), 1);
        assert_eq!(result.get("id").unwrap(), b"1");
        assert!(result.get("name").is_none());
    }

    #[test]
    fn test_project_star() {
        let mut tuple = HashMap::new();
        tuple.insert("id".to_string(), b"1".to_vec());
        tuple.insert("name".to_string(), b"test".to_vec());

        let mock = MockExecutor::new(vec![tuple.clone()]);
        let mut project = Project::new(Box::new(mock), vec!["*".to_string()]);
        
        project.open().unwrap();
        let result = project.next().unwrap().unwrap();
        
        assert_eq!(result.len(), 2);
        assert_eq!(result, tuple);
    }
}
