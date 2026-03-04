# Development Guidelines

## Code Quality Standards

### Code Formatting
- **Indentation**: 4 spaces (enforced by rustfmt)
- **Line Length**: Soft limit at 100 characters, hard limit at 120
- **Brace Style**: Opening braces on same line (K&R style)
- **Imports**: Group by std, external crates, internal modules
- **Trailing Commas**: Used in multi-line collections and function arguments

### Structural Conventions
- **Module Organization**: One primary type per file, related types grouped together
- **Visibility**: Default to private, expose only necessary public APIs
- **File Naming**: Snake_case matching module names (e.g., `join_order.rs`)
- **Test Organization**: Tests in dedicated `#[cfg(test)] mod tests` blocks or separate `*_tests.rs` files

### Naming Standards
- **Types**: PascalCase (e.g., `BTree`, `JoinOptimizer`, `TupleId`)
- **Functions/Methods**: snake_case (e.g., `insert`, `optimize_dp`, `estimate_selectivity`)
- **Constants**: SCREAMING_SNAKE_CASE (e.g., `PORT_COUNTER`)
- **Type Aliases**: PascalCase (e.g., `Key`, `TupleId`)
- **Lifetimes**: Single lowercase letter or descriptive (e.g., `'a`, `'static`)

### Documentation Standards
- **Module-Level Docs**: Use `//!` for module documentation at file top
- **Item Documentation**: Use `///` for public types, functions, and methods
- **Doc Comments**: Describe purpose, parameters, return values, and examples
- **Edge Cases**: Document special behavior, panics, and error conditions
- **Examples**: Include code examples in doc comments for complex APIs

## Practices Followed Throughout Codebase

### Error Handling
- **Error Types**: Each module defines its own error type using `thiserror` crate
- **Result Types**: All fallible operations return `Result<T, E>`
- **Error Propagation**: Use `?` operator for error propagation
- **Error Messages**: Descriptive error messages with context
- **Example Pattern**:
```rust
use thiserror::Error;

#[derive(Error, Debug)]
pub enum StorageError {
    #[error("Page {0} not found")]
    PageNotFound(PageId),
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
}

pub type Result<T> = std::result::Result<T, StorageError>;
```

### Testing Strategy
- **Unit Tests**: In-module tests for individual functions and methods
- **Integration Tests**: Cross-module tests in `tests/integration/` directory
- **E2E Tests**: End-to-end tests using actual server process and psql client
- **Edge Tests**: Dedicated `*_edge_tests.rs` files for boundary conditions
- **Test Naming**: Descriptive names starting with `test_` prefix
- **Test Organization**: Group related tests in same file, one assertion focus per test

### Test Patterns
```rust
#[test]
fn test_btree_insert_and_get() {
    let mut tree = BTree::new();
    let key = vec![1, 2, 3];
    let value = TupleId { page_id: PageId(1), slot: 0 };
    
    tree.insert(key.clone(), value).unwrap();
    assert_eq!(tree.get(&key), Some(value));
}

#[test]
fn test_btree_get_nonexistent() {
    let tree = BTree::new();
    let key = vec![1, 2, 3];
    assert_eq!(tree.get(&key), None);
}
```

### Concurrency Patterns
- **Synchronization**: Use `parking_lot` crate for mutexes and RwLocks
- **Concurrent Collections**: Use `dashmap` for concurrent hash maps
- **Atomic Operations**: Use `std::sync::atomic` for lock-free counters
- **Example**:
```rust
use std::sync::atomic::{AtomicU16, Ordering};
use parking_lot::RwLock;
use dashmap::DashMap;

static PORT_COUNTER: AtomicU16 = AtomicU16::new(15433);
let port = PORT_COUNTER.fetch_add(1, Ordering::SeqCst);
```

### Resource Management
- **RAII Pattern**: Use Drop trait for automatic cleanup
- **Temporary Resources**: Use `tempfile` crate for temporary directories in tests
- **Cleanup**: Implement Drop for resources requiring explicit cleanup
- **Example**:
```rust
impl Drop for TestServer {
    fn drop(&mut self) {
        let _ = self.process.kill();
        let _ = self.process.wait();
        thread::sleep(Duration::from_millis(100));
        // TempDir automatically cleans up
    }
}
```

### Logging Standards
- **Framework**: Use `log` crate for logging facade, `env_logger` for implementation
- **No Debug Prints**: Never use `println!`, `eprintln!`, or `dbg!` in production code
- **Log Levels**: Use appropriate log levels for different scenarios
  - `error!`: Critical errors that require immediate attention
  - `warn!`: Warning conditions that should be investigated
  - `info!`: Important informational messages (startup, shutdown, major operations)
  - `debug!`: Detailed information for debugging (function entry/exit, state changes)
  - `trace!`: Very detailed tracing information (loop iterations, expression evaluation)
- **Compile-Time Control**: Logging is controlled at runtime via `RUST_LOG` environment variable
- **Example**:
```rust
use log::{trace, debug, info, warn, error};

pub fn execute_query(query: &str) -> Result<Vec<Row>, Error> {
    info!("Executing query: {}", query);
    debug!("Query length: {} bytes", query.len());
    
    let parsed = parse(query)?;
    trace!("Parsed AST: {:?}", parsed);
    
    match optimize(&parsed) {
        Ok(plan) => {
            debug!("Optimized plan generated");
            execute_plan(plan)
        }
        Err(e) => {
            warn!("Optimization failed, using default plan: {}", e);
            execute_plan(default_plan(&parsed))
        }
    }
}
```

- **Runtime Configuration**: Set log level via environment variable
```bash
# Show all logs
RUST_LOG=trace cargo run

# Show only info and above
RUST_LOG=info cargo run

# Module-specific logging
RUST_LOG=vaultgres=debug,storage=trace cargo run

# In tests
RUST_LOG=debug cargo test -- --nocapture
```

- **Test Logging**: Initialize logger in tests that need it
```rust
#[test]
fn test_with_logging() {
    env_logger::builder()
        .filter_level(log::LevelFilter::Debug)
        .is_test(true)
        .try_init()
        .ok();
    
    // Test code with logging
}
```

## Semantic Patterns Overview

### Complexity Reduction Patterns

#### Function Table Pattern
- Extract repeated logic into reusable helper functions
- Reduces code duplication and improves maintainability
- **Example**:
```rust
// Before: Repeated parsing logic
fn parse_create_table() {
    if self.peek() == Token::Identifier {
        let name = self.consume_identifier()?;
        // ...
    }
    if self.peek() == Token::Comma {
        self.consume(Token::Comma)?;
        let name = self.consume_identifier()?;
        // ...
    }
}

// After: Helper function
fn parse_identifier_list(&mut self) -> Result<Vec<String>> {
    let mut names = vec![self.consume_identifier()?];
    while self.peek() == Token::Comma {
        self.consume(Token::Comma)?;
        names.push(self.consume_identifier()?);
    }
    Ok(names)
}

fn parse_create_table() {
    let names = self.parse_identifier_list()?;
    // ...
}
```

#### Match Consolidation Pattern
- Replace cascading if-else chains with single match statement
- Improves readability and reduces cyclomatic complexity
- **Example**:
```rust
// Before: Cascading if-else (complexity 10+)
if token == Token::Create { parse_create() }
else if token == Token::Select { parse_select() }
else if token == Token::Insert { parse_insert() }
else if token == Token::Update { parse_update() }
else if token == Token::Delete { parse_delete() }
// ...

// After: Single match (complexity 1)
match token {
    Token::Create => parse_create(),
    Token::Select => parse_select(),
    Token::Insert => parse_insert(),
    Token::Update => parse_update(),
    Token::Delete => parse_delete(),
    _ => Err("Unexpected token"),
}
```

#### Enum Dispatch Pattern
- Use enums to consolidate similar variants into single match
- Reduces complexity by handling each variant in focused match arms
- **Example**:
```rust
// Before: Multiple separate checks
if is_column { handle_column() }
if is_primary_key { handle_primary_key() }
if is_foreign_key { handle_foreign_key() }

// After: Enum dispatch
enum TableElement {
    Column(ColumnDef),
    PrimaryKey(Vec<String>),
    ForeignKey(ForeignKeyDef),
}

match element {
    TableElement::Column(col) => handle_column(col),
    TableElement::PrimaryKey(cols) => handle_primary_key(cols),
    TableElement::ForeignKey(fk) => handle_foreign_key(fk),
}
```

#### Module Extraction Pattern
- Extract cohesive functionality into separate modules
- Separates concerns and reduces file size
- **Example**:
```rust
// Before: 1000-line catalog.rs with CRUD, validation, execution
// src/catalog/catalog.rs (1000 lines)

// After: Extracted modules
// src/catalog/catalog.rs (600 lines) - main coordination
// src/catalog/crud_helper.rs - create/drop/get operations
// src/catalog/insert_validator.rs - validation logic
// src/catalog/select_executor.rs - query execution
// src/catalog/update_delete_executor.rs - update/delete operations
```

#### Helper Composition Pattern
- Build complex operations from simple, focused helpers
- Each helper does one thing well
- **Example**:
```rust
// Simple helpers
fn parse_type_name(&mut self) -> Result<String> { /* ... */ }
fn parse_param_mode(&mut self) -> Result<ParamMode> { /* ... */ }
fn parse_default_value(&mut self) -> Result<Option<Expr>> { /* ... */ }

// Composed into complex operation
fn parse_function_parameter(&mut self) -> Result<FunctionParam> {
    let mode = self.parse_param_mode()?;
    let name = self.consume_identifier()?;
    let type_name = self.parse_type_name()?;
    let default = self.parse_default_value()?;
    Ok(FunctionParam { mode, name, type_name, default })
}
```

#### Callback Pattern for Dependencies
- Pass dependencies as callbacks to avoid circular dependencies
- Enables testing and modularity
- **Example**:
```rust
// Before: Direct dependency (circular)
impl PredicateEvaluator {
    fn evaluate(expr: &Expr, catalog: &Catalog) -> Result<bool> {
        // Can't use Catalog here without circular dependency
    }
}

// After: Callback pattern
impl PredicateEvaluator {
    fn evaluate_with_subquery<F>(
        expr: &Expr,
        subquery_eval: &F,
    ) -> Result<bool>
    where
        F: Fn(&SelectStmt) -> Result<Value>,
    {
        match expr {
            Expr::Subquery(select) => subquery_eval(select),
            // ...
        }
    }
}

// Caller provides the dependency
let subquery_eval = |select| catalog.execute_subquery(select);
PredicateEvaluator::evaluate_with_subquery(expr, &subquery_eval)
```

### Complexity Targets
- **Maximum Cyclomatic Complexity**: 8 per function
- **File Size**: Prefer <1000 lines, extract modules if larger
- **Function Length**: Prefer <50 lines, extract helpers if longer
- **Parameter Count**: Prefer ≤8 parameters, use structs for more

### Builder Pattern for Complex Types
- Use builder methods for types with multiple configuration options
- Provide sensible defaults with `new()` and customization with `with_*()` methods
```rust
impl BTree {
    pub fn new() -> Self {
        Self::with_order(128)
    }
    
    pub fn with_order(order: usize) -> Self {
        Self { root: None, order }
    }
}
```

### Default Trait Implementation
- Implement `Default` trait for types with obvious default values
- Delegate to `new()` method when appropriate
```rust
impl Default for BTree {
    fn default() -> Self {
        Self::new()
    }
}

impl Default for JoinOptimizer {
    fn default() -> Self {
        Self::new()
    }
}
```

### Iterator Pattern
- Provide iterators for collection-like types
- Define custom iterator structs with lifetime parameters
- Implement `Iterator` trait with appropriate `Item` type
```rust
pub struct BTreeIterator<'a> {
    node: Option<&'a Node>,
    index: usize,
}

impl<'a> Iterator for BTreeIterator<'a> {
    type Item = (&'a Key, TupleId);
    
    fn next(&mut self) -> Option<Self::Item> {
        // Iterator implementation
    }
}
```

### Enum-Based Polymorphism
- Use enums for types with distinct variants
- Pattern match on enum variants for type-specific behavior
- Keep enum variants private when implementation detail
```rust
enum Node {
    Internal(InternalNode),
    Leaf(LeafNode),
}

match node {
    Node::Leaf(leaf) => { /* handle leaf */ },
    Node::Internal(internal) => { /* handle internal */ },
}
```

### Type Aliases for Domain Concepts
- Define type aliases for domain-specific concepts
- Improves code readability and maintainability
```rust
pub type Key = Vec<u8>;
pub type Result<T> = std::result::Result<T, StorageError>;
```

### Struct-Based Configuration
- Use structs to group related configuration or data
- Derive common traits (Debug, Clone, Copy, PartialEq, Eq)
```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TupleId {
    pub page_id: PageId,
    pub slot: u16,
}

#[derive(Debug, Clone)]
pub struct Relation {
    pub id: usize,
    pub name: String,
    pub row_count: u64,
}
```

## Internal API Usage and Patterns

### Storage Layer APIs
```rust
// B+Tree index operations
let mut tree = BTree::new();
tree.insert(key, value)?;
let result = tree.get(&key);
tree.delete(&key)?;

// Iterate over entries
for (key, value) in tree.iter() {
    // Process entries
}
```

### Optimizer APIs
```rust
// Cost model usage
let model = CostModel::new();
let seq_cost = model.estimate_seq_scan(&stats, selectivity)?;
let idx_cost = model.estimate_index_scan(&stats, selectivity)?;
let join_cost = model.estimate_nested_loop_join(&left, &right)?;

// Join optimization
let optimizer = JoinOptimizer::new();
let plan = optimizer.optimize(relations)?;
```

### Statistics APIs
```rust
// Histogram creation and usage
let mut hist = Histogram::new(10);
hist.build(values)?;
let selectivity = hist.estimate_selectivity(value);

// Selectivity estimation
let estimator = SelectivityEstimator::new();
let eq_sel = estimator.estimate_equality(&stats);
let and_sel = estimator.estimate_and(sel1, sel2);
let or_sel = estimator.estimate_or(sel1, sel2);
```

### Test Infrastructure APIs
```rust
// E2E test server setup
let server = TestServer::start();
let result = server.execute_sql("CREATE TABLE users (id INT, name TEXT)");
assert!(result.is_ok());

// Temporary directories for tests
let data_dir = TempDir::new()?;
let path = data_dir.path();
// Directory automatically cleaned up on drop
```

## Frequently Used Code Idioms

### Option and Result Handling
```rust
// Early return with ?
pub fn get(&self, key: &Key) -> Option<TupleId> {
    let root = self.root.as_ref()?;  // Return None if root is None
    // Continue processing
}

// Pattern matching on Result
match result {
    Ok(value) => { /* handle success */ },
    Err(e) => { /* handle error */ },
}

// Unwrap with expect for tests
tree.insert(key, value).expect("INSERT failed");
```

### Binary Search for Sorted Collections
```rust
// Find insertion position
let pos = leaf.keys.binary_search(&key).unwrap_or_else(|e| e);
leaf.keys.insert(pos, key);

// Check if key exists
if let Ok(idx) = leaf.keys.binary_search(key) {
    leaf.keys.remove(idx);
    return Ok(true);
}
```

### Cloning for Ownership Transfer
```rust
// Clone when moving into owned structure
tree.insert(key.clone(), value).unwrap();
assert_eq!(tree.get(&key), Some(value));

// Clone for recursive structures
best_plan = Some(JoinPlan {
    left: Some(Box::new(left_plan.clone())),
    right: Some(Box::new(right_plan.clone())),
    relation: None,
    cost: join_cost,
});
```

### Conditional Compilation for Tests
```rust
#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_something() {
        // Test code
    }
}

// Edge tests in separate files
#[cfg(test)]
mod config_edge_tests;
```

### Dynamic Programming with HashMap
```rust
let mut dp: HashMap<Vec<usize>, JoinPlan> = HashMap::new();

// Store subproblem solutions
dp.insert(vec![i], plan);

// Retrieve and use cached solutions
if let (Some(left_plan), Some(right_plan)) = (dp.get(&left_set), dp.get(&right_set)) {
    // Use cached plans
}
```

## Popular Annotations and Attributes

### Derive Macros
```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq)]  // Common for simple types
#[derive(Debug, Clone)]                        // For types with heap data
#[derive(Error, Debug)]                        // For error types (thiserror)
#[derive(Default)]                             // For types with obvious defaults
```

### Test Attributes
```rust
#[test]                    // Mark function as test
#[cfg(test)]              // Conditional compilation for tests
#[should_panic]           // Test should panic (rarely used)
```

### Conditional Compilation
```rust
#[cfg(test)]              // Only compile in test builds
#[cfg(feature = "...")]   // Feature-gated code
```

### Visibility and Warnings
```rust
pub                       // Public visibility
pub(crate)               // Crate-visible (used sparingly)
#[allow(dead_code)]      // Suppress unused code warnings (rare)
```

## Code Review Checklist

### Before Submitting Code
- [ ] All tests pass (`cargo test`)
- [ ] Code is formatted (`cargo fmt`)
- [ ] No clippy warnings (`cargo clippy`)
- [ ] Public APIs have documentation
- [ ] Error cases are handled with Result types
- [ ] Resources are properly cleaned up (Drop trait)
- [ ] **Unit tests added for all new functions/methods**
- [ ] **Edge case tests added in dedicated `*_edge_tests.rs` files**
- [ ] **Integration tests added for cross-module features**
- [ ] **E2E tests added for user-facing features**
- [ ] No unwrap() in production code (use ? or expect in tests)
- [ ] Concurrency primitives used correctly (parking_lot, dashmap)

### Testing Requirements (MANDATORY)

Every implementation MUST include:

#### 1. Unit Tests
- Test individual functions and methods in isolation
- Located in `#[cfg(test)] mod tests` blocks within the same file
- Cover happy path, error cases, and boundary conditions
- Minimum 3-5 tests per public function
- Example:
```rust
#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_basic_functionality() {
        // Test normal operation
    }
    
    #[test]
    fn test_error_handling() {
        // Test error cases
    }
    
    #[test]
    fn test_empty_input() {
        // Test boundary condition
    }
}
```

#### 2. Edge Case Tests
- Dedicated `*_edge_tests.rs` files for each module
- Test boundary conditions, extreme values, and corner cases
- Examples: empty input, single element, maximum values, overflow
- Minimum 10-15 edge case tests per module
- Example:
```rust
// src/executor/limit_edge_tests.rs
#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_limit_larger_than_data() { /* ... */ }
    
    #[test]
    fn test_offset_beyond_data() { /* ... */ }
    
    #[test]
    fn test_zero_limit() { /* ... */ }
    
    #[test]
    fn test_empty_input() { /* ... */ }
}
```

#### 3. Integration Tests
- Located in `tests/integration/` directory
- Test interaction between multiple modules
- Use real components, not mocks
- Cover complete workflows
- Example:
```rust
// tests/integration/executor_test.rs
#[test]
fn test_limit_with_filter() {
    // Test LIMIT operator with Filter operator
}

#[test]
fn test_aggregate_with_sort() {
    // Test Aggregate with Sort operator
}
```

#### 4. End-to-End Tests
- Located in `tests/e2e_tests.rs`
- Test complete user workflows via PostgreSQL protocol
- Use actual server process and psql client
- Cover SQL statements from parsing to execution
- Example:
```rust
#[test]
fn test_select_with_limit_offset() {
    let server = TestServer::start();
    server.execute_sql("CREATE TABLE t (id INT)").unwrap();
    server.execute_sql("INSERT INTO t VALUES (1), (2), (3)").unwrap();
    let result = server.execute_sql("SELECT * FROM t LIMIT 2 OFFSET 1");
    assert!(result.is_ok());
}
```

### Test Coverage Requirements

**Minimum Coverage per Feature:**
- Unit tests: 5+ tests
- Edge case tests: 10+ tests
- Integration tests: 2+ tests
- E2E tests: 1+ test

**Example: LIMIT/OFFSET Implementation**
- Unit tests: 5 tests (basic limit, offset, combinations)
- Edge tests: 11 tests (empty, large values, boundaries)
- Integration tests: 3 tests (with filter, sort, aggregate)
- E2E tests: 2 tests (SQL execution via psql)
- **Total: 21 tests**

### Test Organization

```
src/
├── executor/
│   ├── limit.rs              # Implementation + unit tests
│   ├── limit_edge_tests.rs   # Edge case tests
│   └── mod.rs
tests/
├── integration/
│   └── executor_test.rs      # Integration tests
└── e2e_tests.rs              # E2E tests
```

### Performance Considerations
- [ ] Avoid unnecessary clones (use references when possible)
- [ ] Use binary search for sorted collections
- [ ] Consider cache locality for hot paths
- [ ] Profile before optimizing (use criterion benchmarks)
- [ ] Use appropriate data structures (BTree vs HashMap)

### Security Considerations
- [ ] Input validation for external data
- [ ] Bounds checking for array/vector access
- [ ] Integer overflow checks for arithmetic
- [ ] Resource limits enforced (connections, memory)
- [ ] No unsafe code without thorough review
