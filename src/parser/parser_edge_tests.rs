use super::*;

#[test]
fn test_parse_empty_string() {
    let result = parse("");
    assert!(result.is_err());
}

#[test]
fn test_parse_whitespace_only() {
    let result = parse("   \t\n  ");
    assert!(result.is_err());
}

#[test]
fn test_parse_incomplete_select() {
    let result = parse("SELECT");
    assert!(result.is_err());
}

#[test]
fn test_parse_select_invalid_column_list() {
    let result = parse("SELECT , FROM users");
    assert!(result.is_err());
}

#[test]
fn test_parse_invalid_syntax() {
    let result = parse("INVALID SQL STATEMENT");
    assert!(result.is_err());
}

#[test]
fn test_parse_unclosed_string() {
    let result = parse("SELECT * FROM users WHERE name = 'unclosed");
    assert!(result.is_err());
}

#[test]
fn test_parse_missing_semicolon_ok() {
    let result = parse("SELECT * FROM users");
    assert!(result.is_ok());
}

#[test]
fn test_parse_extra_commas() {
    let result = parse("SELECT id,, name FROM users");
    assert!(result.is_err());
}

#[test]
fn test_parse_missing_table_name() {
    let result = parse("SELECT * FROM");
    assert!(result.is_err());
}

#[test]
fn test_parse_invalid_where_clause() {
    let result = parse("SELECT * FROM users WHERE");
    assert!(result.is_err());
}

#[test]
fn test_parse_insert_without_values() {
    let result = parse("INSERT INTO users");
    assert!(result.is_err());
}

#[test]
fn test_parse_update_without_set() {
    let result = parse("UPDATE users");
    assert!(result.is_err());
}

#[test]
fn test_parse_delete_without_from() {
    let result = parse("DELETE users");
    assert!(result.is_err());
}

#[test]
fn test_parse_create_table_without_columns() {
    let result = parse("CREATE TABLE users");
    assert!(result.is_err());
}

#[test]
fn test_parse_drop_table_nonexistent_ok() {
    let result = parse("DROP TABLE nonexistent");
    assert!(result.is_ok());
}
