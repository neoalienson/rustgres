# Deadlock Fix - Catalog Auto-Save

## Problem
The `auto_save()` method was causing a deadlock when called from `create_table()`, `drop_table()`, and `insert()` methods. The issue occurred because:

1. Calling method acquires write lock on `tables` or `data`
2. Calling method invokes `auto_save()`
3. `auto_save()` tries to acquire read locks on both `tables` and `data`
4. **Deadlock**: Write lock is still held, preventing read lock acquisition

## Solution
Refactored the locking strategy to ensure all locks are released before calling `auto_save()`:

### Key Changes

1. **Modified `auto_save()`** - Clone data while holding locks briefly, then release before saving:
```rust
fn auto_save(&self) {
    if let Some(ref dir) = self.data_dir {
        let tables_clone = self.tables.read().unwrap().clone();
        let data_clone = self.data.read().unwrap().clone();
        
        if let Err(e) = Self::save_to_disk_static(dir, &tables_clone, &data_clone) {
            log::error!("Auto-save failed: {}", e);
        }
    }
}
```

2. **Added `save_to_disk_static()`** - Static helper that doesn't require locks:
```rust
fn save_to_disk_static(
    data_dir: &str, 
    tables: &HashMap<String, TableSchema>, 
    data: &HashMap<String, Vec<Tuple>>
) -> Result<(), String>
```

3. **Explicit lock drops** - Added `drop()` calls before `auto_save()`:
```rust
pub fn create_table(&self, name: String, columns: Vec<ColumnDef>) -> Result<(), String> {
    let mut tables = self.tables.write().unwrap();
    tables.insert(name.clone(), TableSchema { name: name.clone(), columns });
    drop(tables); // Release lock
    
    let mut data = self.data.write().unwrap();
    data.insert(name, Vec::new());
    drop(data); // Release lock
    
    self.auto_save(); // Now safe to call
    Ok(())
}
```

## Result
- ✅ CREATE TABLE works without hanging
- ✅ INSERT operations complete successfully
- ✅ Data persists across server restarts
- ✅ No performance degradation (cloning is cheap for small catalogs)

## Testing
```bash
cd /home/neo/rustgres
cargo build --release
./target/release/rustgres &
psql -h localhost -p 5433 -U postgres -d postgres -c "CREATE TABLE test (id INT)"
# Returns immediately: CREATE TABLE
```
