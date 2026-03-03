#[cfg(test)]
mod tests {
    use crate::parser::ast::{Expr, JoinType};
    use crate::parser::{Parser, Statement};

    #[test]
    fn test_parse_inner_join_with_alias() {
        let sql = "SELECT * FROM customers c INNER JOIN orders o ON c.id = o.customer_id";
        let mut parser = Parser::new(sql).unwrap();
        let stmt = parser.parse().unwrap();

        if let Statement::Select(select) = stmt {
            assert_eq!(select.from, "customers");
            assert_eq!(select.table_alias, Some("c".to_string()));
            assert_eq!(select.joins.len(), 1);
            assert_eq!(select.joins[0].table, "orders");
            assert_eq!(select.joins[0].alias, Some("o".to_string()));
            assert_eq!(select.joins[0].join_type, JoinType::Inner);
        } else {
            panic!("Expected SELECT");
        }
    }

    #[test]
    fn test_parse_join_without_alias() {
        let sql = "SELECT * FROM customers JOIN orders ON customers.id = orders.customer_id";
        let mut parser = Parser::new(sql).unwrap();
        let stmt = parser.parse().unwrap();

        if let Statement::Select(select) = stmt {
            assert_eq!(select.from, "customers");
            assert_eq!(select.table_alias, None);
            assert_eq!(select.joins.len(), 1);
            assert_eq!(select.joins[0].table, "orders");
            assert_eq!(select.joins[0].alias, None);
        } else {
            panic!("Expected SELECT");
        }
    }

    #[test]
    fn test_parse_left_join() {
        let sql = "SELECT * FROM a LEFT JOIN b ON a.id = b.a_id";
        let mut parser = Parser::new(sql).unwrap();
        let stmt = parser.parse().unwrap();

        if let Statement::Select(select) = stmt {
            assert_eq!(select.joins[0].join_type, JoinType::Left);
        } else {
            panic!("Expected SELECT");
        }
    }

    #[test]
    fn test_parse_right_join() {
        let sql = "SELECT * FROM a RIGHT JOIN b ON a.id = b.a_id";
        let mut parser = Parser::new(sql).unwrap();
        let stmt = parser.parse().unwrap();

        if let Statement::Select(select) = stmt {
            assert_eq!(select.joins[0].join_type, JoinType::Right);
        } else {
            panic!("Expected SELECT");
        }
    }

    #[test]
    fn test_parse_full_join() {
        let sql = "SELECT * FROM a FULL JOIN b ON a.id = b.a_id";
        let mut parser = Parser::new(sql).unwrap();
        let stmt = parser.parse().unwrap();

        if let Statement::Select(select) = stmt {
            assert_eq!(select.joins[0].join_type, JoinType::Full);
        } else {
            panic!("Expected SELECT");
        }
    }

    #[test]
    fn test_parse_multiple_joins() {
        let sql = "SELECT * FROM a JOIN b ON a.id = b.a_id JOIN c ON b.id = c.b_id";
        let mut parser = Parser::new(sql).unwrap();
        let stmt = parser.parse().unwrap();

        if let Statement::Select(select) = stmt {
            assert_eq!(select.joins.len(), 2);
            assert_eq!(select.joins[0].table, "b");
            assert_eq!(select.joins[1].table, "c");
        } else {
            panic!("Expected SELECT");
        }
    }

    #[test]
    fn test_parse_join_with_qualified_columns() {
        let sql = "SELECT c.name, o.total FROM customers c JOIN orders o ON c.id = o.customer_id";
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
    fn test_parse_join_with_where() {
        let sql = "SELECT * FROM a JOIN b ON a.id = b.a_id WHERE a.status = 1";
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
    fn test_parse_join_with_as_keyword() {
        let sql = "SELECT * FROM customers AS c JOIN orders AS o ON c.id = o.customer_id";
        let mut parser = Parser::new(sql).unwrap();
        let stmt = parser.parse().unwrap();

        if let Statement::Select(select) = stmt {
            assert_eq!(select.table_alias, Some("c".to_string()));
            assert_eq!(select.joins[0].alias, Some("o".to_string()));
        } else {
            panic!("Expected SELECT");
        }
    }

    #[test]
    fn test_parse_join_with_order_by() {
        let sql = "SELECT * FROM a JOIN b ON a.id = b.a_id ORDER BY a.name";
        let mut parser = Parser::new(sql).unwrap();
        let stmt = parser.parse().unwrap();

        if let Statement::Select(select) = stmt {
            assert!(select.order_by.is_some());
            assert_eq!(select.joins.len(), 1);
        } else {
            panic!("Expected SELECT");
        }
    }
}
