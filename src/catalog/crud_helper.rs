use crate::parser::ast::{CreateIndexStmt, CreateTriggerStmt, SelectStmt};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

pub struct CrudHelper;

impl CrudHelper {
    pub fn create<T: Clone>(
        store: &Arc<RwLock<HashMap<String, T>>>,
        name: String,
        item: T,
        type_name: &str,
    ) -> Result<(), String> {
        let mut map = store.write().unwrap();
        if map.contains_key(&name) {
            return Err(format!("{} '{}' already exists", type_name, name));
        }
        map.insert(name, item);
        Ok(())
    }

    pub fn drop<T>(
        store: &Arc<RwLock<HashMap<String, T>>>,
        name: &str,
        if_exists: bool,
        type_name: &str,
    ) -> Result<(), String> {
        let mut map = store.write().unwrap();
        if map.remove(name).is_none() && !if_exists {
            return Err(format!("{} '{}' does not exist", type_name, name));
        }
        Ok(())
    }

    pub fn get<T: Clone>(store: &Arc<RwLock<HashMap<String, T>>>, name: &str) -> Option<T> {
        store.read().unwrap().get(name).cloned()
    }
}
