use super::value::Value;
use crate::transaction::TupleHeader;
use std::collections::HashMap;

/// Tuple with MVCC header and data
#[derive(Debug, Clone)]
pub struct Tuple {
    pub header: TupleHeader,
    pub data: Vec<Value>,
    pub column_map: HashMap<String, usize>,
}

impl Tuple {
    pub fn new() -> Self {
        Self { header: TupleHeader::new(0), data: Vec::new(), column_map: HashMap::new() }
    }

    pub fn add_value(&mut self, name: String, value: Value) {
        let index = self.data.len();
        self.data.push(value);
        self.column_map.insert(name, index);
    }

    pub fn get_value(&self, name: &str) -> Option<Value> {
        self.column_map.get(name).and_then(|&idx| self.data.get(idx).cloned())
    }
}

impl Default for Tuple {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transaction::TupleHeader;

    #[test]
    fn test_new_tuple() {
        let tuple = Tuple::new();
        assert_eq!(tuple.header, TupleHeader::new(0));
        assert!(tuple.data.is_empty());
        assert!(tuple.column_map.is_empty());
    }

    #[test]
    fn test_add_single_value() {
        let mut tuple = Tuple::new();
        tuple.add_value("id".to_string(), Value::Int(1));

        assert_eq!(tuple.data.len(), 1);
        assert_eq!(tuple.data[0], Value::Int(1));
        assert_eq!(tuple.column_map.len(), 1);
        assert_eq!(tuple.column_map["id"], 0);
    }

    #[test]
    fn test_add_multiple_values() {
        let mut tuple = Tuple::new();
        tuple.add_value("id".to_string(), Value::Int(1));
        tuple.add_value("name".to_string(), Value::Text("test".to_string()));
        tuple.add_value("age".to_string(), Value::Int(25));

        assert_eq!(tuple.data.len(), 3);
        assert_eq!(tuple.data[0], Value::Int(1));
        assert_eq!(tuple.data[1], Value::Text("test".to_string()));
        assert_eq!(tuple.data[2], Value::Int(25));

        assert_eq!(tuple.column_map.len(), 3);
        assert_eq!(tuple.column_map["id"], 0);
        assert_eq!(tuple.column_map["name"], 1);
        assert_eq!(tuple.column_map["age"], 2);
    }

    #[test]
    fn test_add_value_with_existing_column_name() {
        let mut tuple = Tuple::new();
        tuple.add_value("id".to_string(), Value::Int(1));
        tuple.add_value("name".to_string(), Value::Text("old".to_string()));
        tuple.add_value("id".to_string(), Value::Int(2)); // Overwrites index for 'id'
        tuple.add_value("name".to_string(), Value::Text("new".to_string())); // Overwrites index for 'name'

        assert_eq!(tuple.data.len(), 4); // Data grows sequentially
        assert_eq!(tuple.data[0], Value::Int(1));
        assert_eq!(tuple.data[1], Value::Text("old".to_string()));
        assert_eq!(tuple.data[2], Value::Int(2));
        assert_eq!(tuple.data[3], Value::Text("new".to_string()));

        assert_eq!(tuple.column_map.len(), 2); // Map size remains 2, as keys are overwritten
        assert_eq!(tuple.column_map["id"], 2); // 'id' now points to index 2
        assert_eq!(tuple.column_map["name"], 3); // 'name' now points to index 3
    }

    #[test]
    fn test_get_existing_value() {
        let mut tuple = Tuple::new();
        tuple.add_value("id".to_string(), Value::Int(1));
        tuple.add_value("name".to_string(), Value::Text("test".to_string()));

        assert_eq!(tuple.get_value("id"), Some(Value::Int(1)));
        assert_eq!(tuple.get_value("name"), Some(Value::Text("test".to_string())));
    }

    #[test]
    fn test_get_non_existent_value() {
        let mut tuple = Tuple::new();
        tuple.add_value("id".to_string(), Value::Int(1));

        assert_eq!(tuple.get_value("non_existent"), None);
    }

    #[test]
    fn test_get_value_after_overwrite() {
        let mut tuple = Tuple::new();
        tuple.add_value("key".to_string(), Value::Int(10));
        tuple.add_value("key".to_string(), Value::Int(20)); // Overwrite

        assert_eq!(tuple.get_value("key"), Some(Value::Int(20)));
    }

    #[test]
    fn test_default_tuple() {
        let default_tuple = Tuple::default();
        let new_tuple = Tuple::new();
        assert_eq!(default_tuple.header, new_tuple.header);
        assert_eq!(default_tuple.data, new_tuple.data);
        assert_eq!(default_tuple.column_map, new_tuple.column_map);
    }
}
