#[cfg(test)]
mod tests {
    use crate::parser::ast::{Expr, JoinType, SelectStmt};
    use crate::parser::{Parser, Statement};

    fn parse_select(sql: &str) -> SelectStmt {
        let mut parser = Parser::new(sql).unwrap();
        match parser.parse().unwrap() {
            Statement::Select(s) => s,
            _ => panic!("Expected SELECT"),
        }
    }

    #[test]
    fn test_parse_inner_join_with_alias() {
        let select =
            parse_select("SELECT * FROM customers c INNER JOIN orders o ON c.id = o.customer_id");
        assert_eq!(select.from, "customers");
        assert_eq!(select.table_alias, Some("c".to_string()));
        assert_eq!(select.joins.len(), 1);
        assert_eq!(select.joins[0].table, "orders");
        assert_eq!(select.joins[0].alias, Some("o".to_string()));
        assert_eq!(select.joins[0].join_type, JoinType::Inner);
    }

    #[test]
    fn test_parse_join_without_alias() {
        let select = parse_select(
            "SELECT * FROM customers JOIN orders ON customers.id = orders.customer_id",
        );
        assert_eq!(select.from, "customers");
        assert_eq!(select.table_alias, None);
        assert_eq!(select.joins.len(), 1);
        assert_eq!(select.joins[0].table, "orders");
        assert_eq!(select.joins[0].alias, None);
    }

    #[test]
    fn test_parse_left_join() {
        let select = parse_select("SELECT * FROM a LEFT JOIN b ON a.id = b.a_id");
        assert_eq!(select.joins[0].join_type, JoinType::Left);
    }

    #[test]
    fn test_parse_right_join() {
        let select = parse_select("SELECT * FROM a RIGHT JOIN b ON a.id = b.a_id");
        assert_eq!(select.joins[0].join_type, JoinType::Right);
    }

    #[test]
    fn test_parse_full_join() {
        let select = parse_select("SELECT * FROM a FULL JOIN b ON a.id = b.a_id");
        assert_eq!(select.joins[0].join_type, JoinType::Full);
    }

    #[test]
    fn test_parse_multiple_joins() {
        let select =
            parse_select("SELECT * FROM a JOIN b ON a.id = b.a_id JOIN c ON b.id = c.b_id");
        assert_eq!(select.joins.len(), 2);
        assert_eq!(select.joins[0].table, "b");
        assert_eq!(select.joins[1].table, "c");
    }

    #[test]
    fn test_parse_join_with_qualified_columns() {
        let select = parse_select(
            "SELECT c.name, o.total FROM customers c JOIN orders o ON c.id = o.customer_id",
        );
        assert_eq!(select.columns.len(), 2);
        assert!(matches!(select.columns[0], Expr::QualifiedColumn { .. }));
        assert!(matches!(select.columns[1], Expr::QualifiedColumn { .. }));
    }

    #[test]
    fn test_parse_join_with_where() {
        let select = parse_select("SELECT * FROM a JOIN b ON a.id = b.a_id WHERE a.status = 1");
        assert!(select.where_clause.is_some());
        assert_eq!(select.joins.len(), 1);
    }

    #[test]
    fn test_parse_join_with_as_keyword() {
        let select =
            parse_select("SELECT * FROM customers AS c JOIN orders AS o ON c.id = o.customer_id");
        assert_eq!(select.table_alias, Some("c".to_string()));
        assert_eq!(select.joins[0].alias, Some("o".to_string()));
    }

    #[test]
    fn test_parse_join_with_order_by() {
        let select = parse_select("SELECT * FROM a JOIN b ON a.id = b.a_id ORDER BY a.name");
        assert!(select.order_by.is_some());
        assert_eq!(select.joins.len(), 1);
    }
}
