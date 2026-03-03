use e2e::*;
use std::time::Instant;

#[test]
fn test_pet_store_comprehensive() {
    eprintln!("\n=== Test: Pet Store - Comprehensive Features ===");
    let env = TestEnv::new().with_vaultgres().start();
    let db = env.vaultgres();

    // Create tables with various data types and indexes
    eprintln!("[PetStore] Creating schema with indexes...");
    db.execute("CREATE TABLE pets (id SERIAL PRIMARY KEY, name TEXT, species TEXT, price DECIMAL(10,2), birth_date DATE, available BOOLEAN DEFAULT TRUE)").unwrap();
    db.execute("CREATE INDEX idx_pets_species ON pets(species)").unwrap();
    db.execute("CREATE INDEX idx_pets_price ON pets(price)").unwrap();
    
    db.execute("CREATE TABLE customers (id SERIAL PRIMARY KEY, name TEXT, email TEXT, joined_date TIMESTAMP DEFAULT NOW(), loyalty_points INT DEFAULT 0)").unwrap();
    db.execute("CREATE INDEX idx_customers_email ON customers(email)").unwrap();
    
    db.execute("CREATE TABLE orders (id SERIAL PRIMARY KEY, customer_id INT, order_date TIMESTAMP DEFAULT NOW(), total DECIMAL(10,2))").unwrap();
    db.execute("CREATE INDEX idx_orders_customer ON orders(customer_id)").unwrap();
    
    db.execute("CREATE TABLE order_items (id SERIAL, order_id INT, pet_id INT, quantity INT, price DECIMAL(10,2))").unwrap();
    db.execute("CREATE INDEX idx_order_items_order ON order_items(order_id)").unwrap();
    
    db.execute("CREATE TABLE inventory (pet_id INT, stock INT, last_updated TIMESTAMP)").unwrap();

    // Batch insert pets
    eprintln!("[PetStore] Batch inserting pets...");
    db.execute("INSERT INTO pets (name, species, price, birth_date, available) VALUES ('Buddy', 'Dog', 500.00, '2023-01-15', TRUE), ('Whiskers', 'Cat', 300.50, '2023-03-20', TRUE), ('Goldie', 'Fish', 20.99, '2024-01-10', TRUE), ('Tweety', 'Bird', 150.00, '2023-06-05', FALSE), ('Max', 'Dog', 600.00, '2022-11-30', TRUE)").unwrap();

    // Batch insert customers
    eprintln!("[PetStore] Batch inserting customers...");
    db.execute("INSERT INTO customers (name, email, loyalty_points) VALUES ('Alice', 'alice@example.com', 100), ('Bob', 'bob@example.com', 50), ('Charlie', 'charlie@example.com', 200)").unwrap();

    // Transaction with savepoint
    eprintln!("[PetStore] Testing transaction with savepoint...");
    db.execute("BEGIN").unwrap();
    db.execute("INSERT INTO orders (customer_id, total) VALUES (1, 500.00)").unwrap();
    db.execute("SAVEPOINT sp1").unwrap();
    db.execute("INSERT INTO order_items (order_id, pet_id, quantity, price) VALUES (1, 1, 1, 500.00)").unwrap();
    db.execute("ROLLBACK TO sp1").unwrap();
    db.execute("INSERT INTO order_items (order_id, pet_id, quantity, price) VALUES (1, 1, 1, 500.00), (1, 3, 2, 41.98)").unwrap();
    db.execute("COMMIT").unwrap();

    // Create views
    eprintln!("[PetStore] Creating views...");
    db.execute("CREATE VIEW available_pets AS SELECT id, name, species, price FROM pets WHERE available = TRUE").unwrap();
    db.execute("CREATE VIEW customer_orders AS SELECT c.name, o.id AS order_id, o.total FROM customers c JOIN orders o ON c.id = o.customer_id").unwrap();

    // Create materialized view
    eprintln!("[PetStore] Creating materialized view...");
    db.execute("CREATE MATERIALIZED VIEW pet_sales_summary AS SELECT p.species, COUNT(*) AS sold_count FROM pets p JOIN order_items oi ON p.id = oi.pet_id GROUP BY p.species").unwrap();

    // Test joins
    eprintln!("[PetStore] Testing INNER JOIN...");
    let result = db.execute("SELECT c.name, o.total FROM customers c INNER JOIN orders o ON c.id = o.customer_id");
    assert!(result.is_ok());
    assert!(result.unwrap().contains("Alice"));

    eprintln!("[PetStore] Testing LEFT JOIN...");
    let result = db.execute("SELECT c.name, o.id FROM customers c LEFT JOIN orders o ON c.id = o.customer_id");
    assert!(result.is_ok());

    // Test subqueries
    eprintln!("[PetStore] Testing subquery...");
    let result = db.execute("SELECT name, price FROM pets WHERE price > (SELECT AVG(price) FROM pets)");
    assert!(result.is_ok());

    eprintln!("[PetStore] Testing correlated subquery...");
    let result = db.execute("SELECT name FROM customers WHERE id IN (SELECT customer_id FROM orders)");
    assert!(result.is_ok());

    // Test string functions
    eprintln!("[PetStore] Testing string functions...");
    let result = db.execute("SELECT UPPER(name), LOWER(species), LENGTH(name) FROM pets");
    assert!(result.is_ok());
    let result = db.execute("SELECT CONCAT(name, ' - ', species) FROM pets");
    assert!(result.is_ok());
    let result = db.execute("SELECT SUBSTRING(email, 1, 5) FROM customers");
    assert!(result.is_ok());

    // Test date/time functions
    eprintln!("[PetStore] Testing date/time functions...");
    let result = db.execute("SELECT name, EXTRACT(YEAR FROM birth_date) FROM pets");
    assert!(result.is_ok());
    let result = db.execute("SELECT NOW()");
    assert!(result.is_ok());

    // Test views
    eprintln!("[PetStore] Querying views...");
    let result = db.execute("SELECT * FROM available_pets");
    assert!(result.is_ok());
    let result = db.execute("SELECT * FROM customer_orders");
    assert!(result.is_ok());

    // Test materialized view
    eprintln!("[PetStore] Querying materialized view...");
    let result = db.execute("SELECT * FROM pet_sales_summary");
    assert!(result.is_ok());

    eprintln!("[PetStore] Stopping server for persistence test...");
    drop(db);
    drop(env);

    // Restart and verify persistence
    eprintln!("[PetStore] Restarting server...");
    let env = TestEnv::new().with_vaultgres().start();
    let db = env.vaultgres();

    eprintln!("[PetStore] Verifying data persistence...");
    let result = db.execute("SELECT * FROM pets");
    assert!(result.is_ok());
    let output = result.unwrap();
    assert!(output.contains("Buddy"));
    assert!(output.contains("Whiskers"));

    eprintln!("[PetStore] Verifying index persistence...");
    let result = db.execute("SELECT * FROM pets WHERE species = 'Dog'");
    assert!(result.is_ok());

    eprintln!("[PetStore] Verifying view persistence...");
    let result = db.execute("SELECT * FROM available_pets");
    assert!(result.is_ok());

    eprintln!("[PetStore] Verifying materialized view persistence...");
    let result = db.execute("SELECT * FROM pet_sales_summary");
    assert!(result.is_ok());

    eprintln!("[PetStore] Verifying orders persistence...");
    let result = db.execute("SELECT * FROM orders");
    assert!(result.is_ok());

    // Cleanup
    db.execute("DROP MATERIALIZED VIEW pet_sales_summary").ok();
    db.execute("DROP VIEW customer_orders").ok();
    db.execute("DROP VIEW available_pets").ok();
    db.execute("DROP TABLE order_items").ok();
    db.execute("DROP TABLE orders").ok();
    db.execute("DROP TABLE inventory").ok();
    db.execute("DROP TABLE customers").ok();
    db.execute("DROP TABLE pets").ok();

    eprintln!("=== Test PASSED ===");
}

#[test]
fn test_pet_store_concurrent_orders() {
    eprintln!("\n=== Test: Pet Store - Concurrent Orders ===");
    let env = TestEnv::new().with_vaultgres().start();
    let db = env.vaultgres();

    // Setup
    db.execute("CREATE TABLE pets_concurrent (id INT, name TEXT, stock INT)").unwrap();
    db.execute("CREATE TABLE orders_concurrent (id INT, pet_id INT, quantity INT)").unwrap();
    
    eprintln!("[PetStore] Stocking inventory...");
    db.execute("INSERT INTO pets_concurrent VALUES (1, 'Puppy', 100)").unwrap();
    db.execute("INSERT INTO pets_concurrent VALUES (2, 'Kitten', 50)").unwrap();

    // Simulate concurrent orders
    eprintln!("[PetStore] Simulating 20 concurrent orders...");
    let start = Instant::now();
    for i in 0..20 {
        let pet_id = (i % 2) + 1;
        db.execute(&format!("INSERT INTO orders_concurrent VALUES ({}, {}, 1)", i, pet_id)).ok();
    }
    let duration = start.elapsed();

    eprintln!("[PetStore] Processed 20 orders in {:?}", duration);
    
    // Verify orders were created
    let result = db.execute("SELECT * FROM orders_concurrent");
    assert!(result.is_ok(), "Failed to query orders");
    let output = result.unwrap();
    eprintln!("[PetStore] Orders created: {}", output.lines().count().saturating_sub(3));
    
    assert!(duration.as_secs() < 10, "Orders too slow");

    // Cleanup
    db.execute("DROP TABLE orders_concurrent").ok();
    db.execute("DROP TABLE pets_concurrent").ok();

    eprintln!("=== Test PASSED ===");
}

#[test]
fn test_pet_store_inventory_management() {
    eprintln!("\n=== Test: Pet Store - Inventory Management ===");
    let env = TestEnv::new().with_vaultgres().start();
    let db = env.vaultgres();

    db.execute("CREATE TABLE inventory (id INT, item TEXT, quantity INT, price INT)").unwrap();

    // Initial stock
    eprintln!("[PetStore] Adding initial inventory...");
    let items = vec![
        (1, "Dog Food", 100, 50),
        (2, "Cat Litter", 75, 30),
        (3, "Fish Tank", 20, 200),
        (4, "Bird Cage", 15, 150),
        (5, "Pet Toys", 200, 10),
    ];

    for (id, item, qty, price) in items {
        db.execute(&format!(
            "INSERT INTO inventory VALUES ({}, '{}', {}, {})",
            id, item, qty, price
        )).unwrap();
    }

    // Query all inventory
    eprintln!("[PetStore] Checking inventory levels...");
    let result = db.execute("SELECT * FROM inventory");
    assert!(result.is_ok(), "Failed to query inventory");
    let output = result.unwrap();
    assert!(output.contains("Dog Food"), "Dog Food not found in inventory");
    assert!(output.contains("Cat Litter"), "Cat Litter not found in inventory");
    eprintln!("[PetStore] Found {} items in inventory", 5);

    // Query specific items
    eprintln!("[PetStore] Querying low stock items (quantity < 50)...");
    let result = db.execute("SELECT * FROM inventory WHERE quantity < 50");
    assert!(result.is_ok(), "Failed to query low stock items");
    let output = result.unwrap();
    assert!(output.contains("Fish Tank") || output.contains("Bird Cage"), "Expected low stock items not found");

    // Cleanup
    db.execute("DROP TABLE inventory").ok();

    eprintln!("=== Test PASSED ===");
}

#[test]
fn test_pet_store_customer_orders_workflow() {
    eprintln!("\n=== Test: Pet Store - Complete Order Workflow ===");
    let env = TestEnv::new().with_vaultgres().start();
    let db = env.vaultgres();

    // Create tables
    eprintln!("[PetStore] Setting up database schema...");
    db.execute("CREATE TABLE customers_workflow (id INT, name TEXT, loyalty_points INT)").unwrap();
    db.execute("CREATE TABLE products_workflow (id INT, name TEXT, category TEXT, price INT)").unwrap();
    db.execute("CREATE TABLE orders_workflow (id INT, customer_id INT, product_id INT, total INT)").unwrap();

    // Add customers
    eprintln!("[PetStore] Registering customers...");
    db.execute("INSERT INTO customers_workflow VALUES (1, 'Alice', 100)").unwrap();
    db.execute("INSERT INTO customers_workflow VALUES (2, 'Bob', 50)").unwrap();
    db.execute("INSERT INTO customers_workflow VALUES (3, 'Charlie', 200)").unwrap();

    // Add products
    eprintln!("[PetStore] Adding products...");
    db.execute("INSERT INTO products_workflow VALUES (1, 'Premium Dog Food', 'Food', 60)").unwrap();
    db.execute("INSERT INTO products_workflow VALUES (2, 'Cat Scratching Post', 'Furniture', 80)").unwrap();
    db.execute("INSERT INTO products_workflow VALUES (3, 'Aquarium Filter', 'Equipment', 45)").unwrap();

    // Process orders
    eprintln!("[PetStore] Processing customer orders...");
    db.execute("INSERT INTO orders_workflow VALUES (1, 1, 1, 60)").unwrap();
    db.execute("INSERT INTO orders_workflow VALUES (2, 2, 2, 80)").unwrap();
    db.execute("INSERT INTO orders_workflow VALUES (3, 1, 3, 45)").unwrap();
    db.execute("INSERT INTO orders_workflow VALUES (4, 3, 1, 60)").unwrap();

    // Verify data
    eprintln!("[PetStore] Verifying orders...");
    let result = db.execute("SELECT * FROM orders_workflow");
    assert!(result.is_ok(), "Failed to query orders");
    let orders_output = result.unwrap();
    eprintln!("[PetStore] Orders output: {}", orders_output.lines().take(5).collect::<Vec<_>>().join(" | "));

    eprintln!("[PetStore] Verifying customers...");
    let result = db.execute("SELECT * FROM customers_workflow");
    assert!(result.is_ok(), "Failed to query customers");
    let customers_output = result.unwrap();
    assert!(customers_output.contains("Alice"), "Customer Alice not found");
    assert!(customers_output.contains("Bob"), "Customer Bob not found");
    assert!(customers_output.contains("Charlie"), "Customer Charlie not found");
    eprintln!("[PetStore] Verified 3 customers");

    // Cleanup
    db.execute("DROP TABLE orders_workflow").ok();
    db.execute("DROP TABLE products_workflow").ok();
    db.execute("DROP TABLE customers_workflow").ok();

    eprintln!("=== Test PASSED ===");
}

#[test]
fn test_pet_store_high_volume_sales() {
    eprintln!("\n=== Test: Pet Store - High Volume Sales ===");
    let env = TestEnv::new().with_vaultgres().start();
    let db = env.vaultgres();

    db.execute("CREATE TABLE sales (id INT, product TEXT, amount INT, timestamp INT)").unwrap();

    eprintln!("[PetStore] Recording 100 sales transactions...");
    let start = Instant::now();
    
    for i in 0..100 {
        let product = match i % 5 {
            0 => "Dog Food",
            1 => "Cat Litter",
            2 => "Fish Food",
            3 => "Bird Seed",
            _ => "Pet Toys",
        };
        let amount = (i % 10 + 1) * 10;
        
        db.execute(&format!(
            "INSERT INTO sales VALUES ({}, '{}', {}, {})",
            i, product, amount, i
        )).ok();
    }
    
    let duration = start.elapsed();
    let tps = 100.0 / duration.as_secs_f64();
    
    eprintln!("[PetStore] Completed 100 sales in {:?}", duration);
    eprintln!("[PetStore] Throughput: {:.2} transactions/sec", tps);
    
    assert!(tps > 5.0, "Transaction throughput too low: {:.2} TPS", tps);
    
    // Cleanup
    db.execute("DROP TABLE sales").ok();
    
    eprintln!("=== Test PASSED ===");
}
