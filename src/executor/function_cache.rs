use crate::catalog::Value;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

pub struct FunctionCache {
    cache: Arc<Mutex<HashMap<String, Value>>>,
}

impl FunctionCache {
    pub fn new() -> Self {
        Self { cache: Arc::new(Mutex::new(HashMap::new())) }
    }

    pub fn get(&self, key: &str) -> Option<Value> {
        self.cache.lock().unwrap().get(key).cloned()
    }

    pub fn set(&self, key: String, value: Value) {
        self.cache.lock().unwrap().insert(key, value);
    }

    pub fn clear(&self) {
        self.cache.lock().unwrap().clear();
    }

    pub fn make_key(func_name: &str, args: &[Value]) -> String {
        let mut key = func_name.to_string();
        for arg in args {
            key.push('|');
            key.push_str(&format!("{:?}", arg));
        }
        key
    }
}

impl Default for FunctionCache {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cache_set_get() {
        let cache = FunctionCache::new();
        let key = FunctionCache::make_key("add", &[Value::Int(1), Value::Int(2)]);
        cache.set(key.clone(), Value::Int(3));
        assert_eq!(cache.get(&key), Some(Value::Int(3)));
    }

    #[test]
    fn test_cache_miss() {
        let cache = FunctionCache::new();
        assert_eq!(cache.get("nonexistent"), None);
    }

    #[test]
    fn test_cache_clear() {
        let cache = FunctionCache::new();
        let key = FunctionCache::make_key("add", &[Value::Int(1), Value::Int(2)]);
        cache.set(key.clone(), Value::Int(3));
        cache.clear();
        assert_eq!(cache.get(&key), None);
    }

    #[test]
    fn test_make_key() {
        let key1 = FunctionCache::make_key("add", &[Value::Int(1), Value::Int(2)]);
        let key2 = FunctionCache::make_key("add", &[Value::Int(1), Value::Int(2)]);
        let key3 = FunctionCache::make_key("add", &[Value::Int(2), Value::Int(1)]);
        assert_eq!(key1, key2);
        assert_ne!(key1, key3);
    }

    #[test]
    fn test_cache_different_functions() {
        let cache = FunctionCache::new();
        let key1 = FunctionCache::make_key("add", &[Value::Int(1), Value::Int(2)]);
        let key2 = FunctionCache::make_key("mul", &[Value::Int(1), Value::Int(2)]);
        cache.set(key1.clone(), Value::Int(3));
        cache.set(key2.clone(), Value::Int(2));
        assert_eq!(cache.get(&key1), Some(Value::Int(3)));
        assert_eq!(cache.get(&key2), Some(Value::Int(2)));
    }
}
