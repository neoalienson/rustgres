use vaultgres::catalog::Catalog;
use vaultgres::parser::ast::{ColumnDef, DataType, Expr};
use vaultgres::parser::{Parser, Statement};

#[test]
fn test_inner_join_basic() {
    let catalog = Catalog::new();

    catalog
        .create_table(
            "customers".to_string(),
            vec![
                ColumnDef::new("id".to_string(), DataType::Int),
                ColumnDef::new("name".to_string(), DataType::Text),
            ],
        )
        .unwrap();

    catalog
        .create_table(
            "orders".to_string(),
            vec![
                ColumnDef::new("id".to_string(), DataType::Int),
                ColumnDef::new("customer_id".to_string(), DataType::Int),
                ColumnDef::new("total".to_string(), DataType::Int),
            ],
        )
        .unwrap();

    catalog.insert("customers", vec![Expr::Number(1), Expr::String("Alice".to_string())]).unwrap();
    catalog.insert("customers", vec![Expr::Number(2), Expr::String("Bob".to_string())]).unwrap();
    catalog.insert("orders", vec![Expr::Number(1), Expr::Number(1), Expr::Number(100)]).unwrap();
    catalog.insert("orders", vec![Expr::Number(2), Expr::Number(1), Expr::Number(200)]).unwrap();

    let sql = "SELECT c.name, o.total FROM customers c INNER JOIN orders o ON c.id = o.customer_id";
    let mut parser = Parser::new(sql).unwrap();
    let stmt = parser.parse().unwrap();

    if let Statement::Select(select) = stmt {
        assert_eq!(select.joins.len(), 1);
        assert_eq!(select.from, "customers");
    } else {
        panic!("Expected SELECT");
    }
}

#[test]
fn test_join_no_matches() {
    let catalog = Catalog::new();

    catalog
        .create_table("a".to_string(), vec![ColumnDef::new("id".to_string(), DataType::Int)])
        .unwrap();

    catalog
        .create_table("b".to_string(), vec![ColumnDef::new("a_id".to_string(), DataType::Int)])
        .unwrap();

    catalog.insert("a", vec![Expr::Number(1)]).unwrap();
    catalog.insert("b", vec![Expr::Number(2)]).unwrap();

    let sql = "SELECT * FROM a JOIN b ON a.id = b.a_id";
    let mut parser = Parser::new(sql).unwrap();
    let stmt = parser.parse().unwrap();

    if let Statement::Select(select) = stmt {
        assert_eq!(select.joins.len(), 1);
    } else {
        panic!("Expected SELECT");
    }
}

#[test]
fn test_multiple_joins() {
    let catalog = Catalog::new();

    catalog
        .create_table("a".to_string(), vec![ColumnDef::new("id".to_string(), DataType::Int)])
        .unwrap();

    catalog
        .create_table(
            "b".to_string(),
            vec![
                ColumnDef::new("id".to_string(), DataType::Int),
                ColumnDef::new("a_id".to_string(), DataType::Int),
            ],
        )
        .unwrap();

    catalog
        .create_table("c".to_string(), vec![ColumnDef::new("b_id".to_string(), DataType::Int)])
        .unwrap();

    catalog.insert("a", vec![Expr::Number(1)]).unwrap();
    catalog.insert("b", vec![Expr::Number(1), Expr::Number(1)]).unwrap();
    catalog.insert("c", vec![Expr::Number(1)]).unwrap();

    let sql = "SELECT * FROM a JOIN b ON a.id = b.a_id JOIN c ON b.id = c.b_id";
    let mut parser = Parser::new(sql).unwrap();
    let stmt = parser.parse().unwrap();

    if let Statement::Select(select) = stmt {
        assert_eq!(select.joins.len(), 2);
    } else {
        panic!("Expected SELECT");
    }
}

#[test]
fn test_join_with_where_clause() {
    let catalog = Catalog::new();

    catalog
        .create_table(
            "users".to_string(),
            vec![
                ColumnDef::new("id".to_string(), DataType::Int),
                ColumnDef::new("status".to_string(), DataType::Int),
            ],
        )
        .unwrap();

    catalog
        .create_table(
            "orders".to_string(),
            vec![
                ColumnDef::new("user_id".to_string(), DataType::Int),
                ColumnDef::new("amount".to_string(), DataType::Int),
            ],
        )
        .unwrap();

    catalog.insert("users", vec![Expr::Number(1), Expr::Number(1)]).unwrap();
    catalog.insert("users", vec![Expr::Number(2), Expr::Number(0)]).unwrap();
    catalog.insert("orders", vec![Expr::Number(1), Expr::Number(100)]).unwrap();
    catalog.insert("orders", vec![Expr::Number(2), Expr::Number(200)]).unwrap();

    let sql = "SELECT * FROM users u JOIN orders o ON u.id = o.user_id WHERE u.status = 1";
    let mut parser = Parser::new(sql).unwrap();
    let stmt = parser.parse().unwrap();

    if let Statement::Select(select) = stmt {
        assert!(select.where_clause.is_some());
        assert_eq!(select.joins.len(), 1);
    } else {
        panic!("Expected SELECT");
    }
}

#[test]
fn test_join_empty_tables() {
    let catalog = Catalog::new();

    catalog
        .create_table("a".to_string(), vec![ColumnDef::new("id".to_string(), DataType::Int)])
        .unwrap();

    catalog
        .create_table("b".to_string(), vec![ColumnDef::new("a_id".to_string(), DataType::Int)])
        .unwrap();

    let sql = "SELECT * FROM a JOIN b ON a.id = b.a_id";
    let mut parser = Parser::new(sql).unwrap();
    let stmt = parser.parse().unwrap();

    if let Statement::Select(select) = stmt {
        assert_eq!(select.joins.len(), 1);
    } else {
        panic!("Expected SELECT");
    }
}

#[test]
fn test_join_with_qualified_columns_in_select() {
    let catalog = Catalog::new();

    catalog
        .create_table(
            "t1".to_string(),
            vec![
                ColumnDef::new("id".to_string(), DataType::Int),
                ColumnDef::new("name".to_string(), DataType::Text),
            ],
        )
        .unwrap();

    catalog
        .create_table(
            "t2".to_string(),
            vec![
                ColumnDef::new("t1_id".to_string(), DataType::Int),
                ColumnDef::new("value".to_string(), DataType::Int),
            ],
        )
        .unwrap();

    catalog.insert("t1", vec![Expr::Number(1), Expr::String("A".to_string())]).unwrap();
    catalog.insert("t2", vec![Expr::Number(1), Expr::Number(100)]).unwrap();

    let sql = "SELECT t1.name, t2.value FROM t1 JOIN t2 ON t1.id = t2.t1_id";
    let mut parser = Parser::new(sql).unwrap();
    let stmt = parser.parse().unwrap();

    if let Statement::Select(select) = stmt {
        assert_eq!(select.columns.len(), 2);
        assert!(matches!(select.columns[0], Expr::QualifiedColumn { .. }));
        assert!(matches!(select.columns[1], Expr::QualifiedColumn { .. }));
    } else {
        panic!("Expected SELECT");
    }
}

#[test]
fn test_join_same_column_names() {
    let catalog = Catalog::new();

    catalog
        .create_table(
            "t1".to_string(),
            vec![
                ColumnDef::new("id".to_string(), DataType::Int),
                ColumnDef::new("name".to_string(), DataType::Text),
            ],
        )
        .unwrap();

    catalog
        .create_table(
            "t2".to_string(),
            vec![
                ColumnDef::new("id".to_string(), DataType::Int),
                ColumnDef::new("name".to_string(), DataType::Text),
            ],
        )
        .unwrap();

    catalog.insert("t1", vec![Expr::Number(1), Expr::String("A".to_string())]).unwrap();
    catalog.insert("t2", vec![Expr::Number(1), Expr::String("B".to_string())]).unwrap();

    let sql = "SELECT t1.name, t2.name FROM t1 JOIN t2 ON t1.id = t2.id";
    let mut parser = Parser::new(sql).unwrap();
    let stmt = parser.parse().unwrap();

    if let Statement::Select(select) = stmt {
        assert_eq!(select.columns.len(), 2);
    } else {
        panic!("Expected SELECT");
    }
}

#[test]
fn test_join_with_order_by() {
    let catalog = Catalog::new();

    catalog
        .create_table(
            "a".to_string(),
            vec![
                ColumnDef::new("id".to_string(), DataType::Int),
                ColumnDef::new("name".to_string(), DataType::Text),
            ],
        )
        .unwrap();

    catalog
        .create_table("b".to_string(), vec![ColumnDef::new("a_id".to_string(), DataType::Int)])
        .unwrap();

    catalog.insert("a", vec![Expr::Number(2), Expr::String("B".to_string())]).unwrap();
    catalog.insert("a", vec![Expr::Number(1), Expr::String("A".to_string())]).unwrap();
    catalog.insert("b", vec![Expr::Number(1)]).unwrap();
    catalog.insert("b", vec![Expr::Number(2)]).unwrap();

    let sql = "SELECT a.name FROM a JOIN b ON a.id = b.a_id ORDER BY a.name";
    let mut parser = Parser::new(sql).unwrap();
    let stmt = parser.parse().unwrap();

    if let Statement::Select(select) = stmt {
        assert!(select.order_by.is_some());
    } else {
        panic!("Expected SELECT");
    }
}

#[test]
fn test_join_with_limit() {
    let catalog = Catalog::new();

    catalog
        .create_table("a".to_string(), vec![ColumnDef::new("id".to_string(), DataType::Int)])
        .unwrap();

    catalog
        .create_table("b".to_string(), vec![ColumnDef::new("a_id".to_string(), DataType::Int)])
        .unwrap();

    catalog.insert("a", vec![Expr::Number(1)]).unwrap();
    catalog.insert("a", vec![Expr::Number(2)]).unwrap();
    catalog.insert("b", vec![Expr::Number(1)]).unwrap();
    catalog.insert("b", vec![Expr::Number(2)]).unwrap();

    let sql = "SELECT * FROM a JOIN b ON a.id = b.a_id LIMIT 1";
    let mut parser = Parser::new(sql).unwrap();
    let stmt = parser.parse().unwrap();

    if let Statement::Select(select) = stmt {
        assert_eq!(select.limit, Some(1));
    } else {
        panic!("Expected SELECT");
    }
}

#[test]
fn test_join_all_types() {
    let test_cases = vec![
        ("SELECT * FROM a INNER JOIN b ON a.id = b.a_id", "INNER"),
        ("SELECT * FROM a LEFT JOIN b ON a.id = b.a_id", "LEFT"),
        ("SELECT * FROM a RIGHT JOIN b ON a.id = b.a_id", "RIGHT"),
        ("SELECT * FROM a FULL JOIN b ON a.id = b.a_id", "FULL"),
    ];

    for (sql, join_type) in test_cases {
        let mut parser = Parser::new(sql).unwrap();
        let stmt = parser.parse().unwrap();

        if let Statement::Select(select) = stmt {
            assert_eq!(select.joins.len(), 1, "Failed for {}", join_type);
        } else {
            panic!("Expected SELECT for {}", join_type);
        }
    }
}
