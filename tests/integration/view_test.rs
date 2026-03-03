use vaultgres::catalog::Catalog;
use vaultgres::parser::ast::{BinaryOperator, ColumnDef, DataType, Expr, SelectStmt};

#[test]
fn test_create_view() {
    let catalog = Catalog::new();
    let columns = vec![
        ColumnDef::new("id".to_string(), DataType::Int),
        ColumnDef::new("name".to_string(), DataType::Text),
    ];

    catalog.create_table("users".to_string(), columns).unwrap();

    let query = SelectStmt {
        distinct: false,
        columns: vec![Expr::Star],
        from: "users".to_string(),
        table_alias: None,
        joins: vec![],
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    catalog.create_view("user_view".to_string(), query).unwrap();
    assert!(catalog.get_view("user_view").is_some());
}

#[test]
fn test_drop_view() {
    let catalog = Catalog::new();
    let columns = vec![
        ColumnDef::new("id".to_string(), DataType::Int),
        ColumnDef::new("email".to_string(), DataType::Text),
    ];

    catalog.create_table("accounts".to_string(), columns).unwrap();

    let query = SelectStmt {
        distinct: false,
        columns: vec![Expr::Column("email".to_string())],
        from: "accounts".to_string(),
        table_alias: None,
        joins: vec![],
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    catalog.create_view("email_view".to_string(), query).unwrap();
    catalog.drop_view("email_view", false).unwrap();
    assert!(catalog.get_view("email_view").is_none());
}

#[test]
fn test_drop_view_if_exists() {
    let catalog = Catalog::new();

    // Should not error when view doesn't exist
    catalog.drop_view("nonexistent_view", true).unwrap();
}

#[test]
fn test_drop_view_not_exists_error() {
    let catalog = Catalog::new();

    // Should error when view doesn't exist and if_exists is false
    let result = catalog.drop_view("nonexistent_view", false);
    assert!(result.is_err());
}

#[test]
fn test_create_view_duplicate_error() {
    let catalog = Catalog::new();
    let columns = vec![ColumnDef::new("id".to_string(), DataType::Int)];

    catalog.create_table("items".to_string(), columns).unwrap();

    let query = SelectStmt {
        distinct: false,
        columns: vec![Expr::Star],
        from: "items".to_string(),
        table_alias: None,
        joins: vec![],
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    catalog.create_view("item_view".to_string(), query.clone()).unwrap();

    // Should error when creating duplicate view
    let result = catalog.create_view("item_view".to_string(), query);
    assert!(result.is_err());
}

#[test]
fn test_view_with_where_clause() {
    let catalog = Catalog::new();
    let columns = vec![
        ColumnDef::new("id".to_string(), DataType::Int),
        ColumnDef::new("status".to_string(), DataType::Text),
    ];

    catalog.create_table("orders".to_string(), columns).unwrap();

    let query = SelectStmt {
        distinct: false,
        columns: vec![Expr::Star],
        from: "orders".to_string(),
        table_alias: None,
        joins: vec![],
        where_clause: Some(Expr::BinaryOp {
            left: Box::new(Expr::Column("status".to_string())),
            op: BinaryOperator::Equals,
            right: Box::new(Expr::String("active".to_string())),
        }),
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    catalog.create_view("active_orders".to_string(), query).unwrap();
    let view = catalog.get_view("active_orders").unwrap();
    assert!(view.where_clause.is_some());
}
