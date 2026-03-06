use crate::parser::ast::Expr;

#[derive(Debug, Clone)]
pub enum PlPgSqlStmt {
    Declare {
        name: String,
        data_type: String,
        default: Option<Expr>,
    },
    Assign {
        target: String,
        value: Expr,
    },
    If {
        condition: Expr,
        then_stmts: Vec<PlPgSqlStmt>,
        else_stmts: Vec<PlPgSqlStmt>,
    },
    While {
        condition: Expr,
        body: Vec<PlPgSqlStmt>,
    },
    For {
        var: String,
        start: Expr,
        end: Expr,
        body: Vec<PlPgSqlStmt>,
    },
    ForEach {
        var: String,
        array: Expr,
        body: Vec<PlPgSqlStmt>,
    },
    ForQuery {
        var: String,
        query: String,
        body: Vec<PlPgSqlStmt>,
    },
    Loop {
        body: Vec<PlPgSqlStmt>,
    },
    Exit,
    Continue,
    Case {
        expr: Expr,
        when_clauses: Vec<(Expr, Vec<PlPgSqlStmt>)>,
        else_stmts: Vec<PlPgSqlStmt>,
    },
    Return {
        value: Option<Expr>,
    },
    Execute {
        query: String,
    },
    Perform {
        query: String,
    },
    ExceptionBlock {
        try_stmts: Vec<PlPgSqlStmt>,
        exception_var: String,
        catch_stmts: Vec<PlPgSqlStmt>,
    },
    Raise {
        message: String,
    },
}

#[derive(Debug, Clone)]
pub struct PlPgSqlFunction {
    pub declarations: Vec<PlPgSqlStmt>,
    pub body: Vec<PlPgSqlStmt>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::ast::Expr;

    #[test]
    fn test_plpgsql_stmt_declare() {
        let stmt = PlPgSqlStmt::Declare {
            name: "my_var".to_string(),
            data_type: "INTEGER".to_string(),
            default: Some(Expr::Number(10)),
        };
        if let PlPgSqlStmt::Declare { name, data_type, default } = stmt {
            assert_eq!(name, "my_var");
            assert_eq!(data_type, "INTEGER");
            assert_eq!(default, Some(Expr::Number(10)));
        } else {
            panic!("Expected Declare statement");
        }

        let stmt_no_default = PlPgSqlStmt::Declare {
            name: "another_var".to_string(),
            data_type: "TEXT".to_string(),
            default: None,
        };
        if let PlPgSqlStmt::Declare { name, data_type, default } = stmt_no_default {
            assert_eq!(name, "another_var");
            assert_eq!(data_type, "TEXT");
            assert_eq!(default, None);
        } else {
            panic!("Expected Declare statement");
        }
    }

    #[test]
    fn test_plpgsql_stmt_assign() {
        let stmt = PlPgSqlStmt::Assign {
            target: "x".to_string(),
            value: Expr::BinaryOp {
                left: Box::new(Expr::Column("y".to_string())),
                op: crate::parser::ast::BinaryOperator::Add,
                right: Box::new(Expr::Number(5)),
            },
        };
        if let PlPgSqlStmt::Assign { target, value } = stmt {
            assert_eq!(target, "x");
            assert!(matches!(value, Expr::BinaryOp { .. }));
        } else {
            panic!("Expected Assign statement");
        }
    }

    #[test]
    fn test_plpgsql_stmt_if() {
        let stmt_with_else = PlPgSqlStmt::If {
            condition: Expr::BinaryOp {
                left: Box::new(Expr::Column("a".to_string())),
                op: crate::parser::ast::BinaryOperator::GreaterThan,
                right: Box::new(Expr::Number(0)),
            },
            then_stmts: vec![PlPgSqlStmt::Return { value: Some(Expr::Number(1)) }],
            else_stmts: vec![PlPgSqlStmt::Return { value: Some(Expr::Number(0)) }],
        };
        if let PlPgSqlStmt::If { condition, then_stmts, else_stmts } = stmt_with_else {
            assert!(matches!(condition, Expr::BinaryOp { .. }));
            assert_eq!(then_stmts.len(), 1);
            assert_eq!(else_stmts.len(), 1);
        } else {
            panic!("Expected If statement");
        }

        let stmt_no_else = PlPgSqlStmt::If {
            condition: Expr::BinaryOp {
                left: Box::new(Expr::Column("b".to_string())),
                op: crate::parser::ast::BinaryOperator::LessThan,
                right: Box::new(Expr::Number(10)),
            },
            then_stmts: vec![PlPgSqlStmt::Execute { query: "UPDATE t SET v = 1".to_string() }],
            else_stmts: vec![],
        };
        if let PlPgSqlStmt::If { condition, then_stmts, else_stmts } = stmt_no_else {
            assert!(matches!(condition, Expr::BinaryOp { .. }));
            assert_eq!(then_stmts.len(), 1);
            assert!(else_stmts.is_empty());
        } else {
            panic!("Expected If statement");
        }
    }

    #[test]
    fn test_plpgsql_stmt_while() {
        let stmt = PlPgSqlStmt::While {
            condition: Expr::BinaryOp {
                left: Box::new(Expr::Column("count".to_string())),
                op: crate::parser::ast::BinaryOperator::LessThan,
                right: Box::new(Expr::Number(5)),
            },
            body: vec![PlPgSqlStmt::Assign {
                target: "count".to_string(),
                value: Expr::BinaryOp {
                    left: Box::new(Expr::Column("count".to_string())),
                    op: crate::parser::ast::BinaryOperator::Add,
                    right: Box::new(Expr::Number(1)),
                },
            }],
        };
        if let PlPgSqlStmt::While { condition, body } = stmt {
            assert!(matches!(condition, Expr::BinaryOp { .. }));
            assert_eq!(body.len(), 1);
        } else {
            panic!("Expected While statement");
        }
    }

    #[test]
    fn test_plpgsql_stmt_for() {
        let stmt = PlPgSqlStmt::For {
            var: "i".to_string(),
            start: Expr::Number(1),
            end: Expr::Number(10),
            body: vec![PlPgSqlStmt::Execute { query: "INSERT INTO logs VALUES (i)".to_string() }],
        };
        if let PlPgSqlStmt::For { var, start, end, body } = stmt {
            assert_eq!(var, "i");
            assert_eq!(start, Expr::Number(1));
            assert_eq!(end, Expr::Number(10));
            assert_eq!(body.len(), 1);
        } else {
            panic!("Expected For statement");
        }
    }

    #[test]
    fn test_plpgsql_stmt_for_each() {
        let stmt = PlPgSqlStmt::ForEach {
            var: "element".to_string(),
            array: Expr::Column("my_array".to_string()),
            body: vec![PlPgSqlStmt::Perform {
                query: "SELECT process_element(element)".to_string(),
            }],
        };
        if let PlPgSqlStmt::ForEach { var, array, body } = stmt {
            assert_eq!(var, "element");
            assert_eq!(array, Expr::Column("my_array".to_string()));
            assert_eq!(body.len(), 1);
        } else {
            panic!("Expected ForEach statement");
        }
    }

    #[test]
    fn test_plpgsql_stmt_for_query() {
        let stmt = PlPgSqlStmt::ForQuery {
            var: "rec".to_string(),
            query: "SELECT * FROM users".to_string(),
            body: vec![PlPgSqlStmt::Execute { query: "RAISE NOTICE '%', rec.name".to_string() }],
        };
        if let PlPgSqlStmt::ForQuery { var, query, body } = stmt {
            assert_eq!(var, "rec");
            assert_eq!(query, "SELECT * FROM users");
            assert_eq!(body.len(), 1);
        } else {
            panic!("Expected ForQuery statement");
        }
    }

    #[test]
    fn test_plpgsql_stmt_loop() {
        let stmt = PlPgSqlStmt::Loop { body: vec![PlPgSqlStmt::Exit] };
        if let PlPgSqlStmt::Loop { body } = stmt {
            assert_eq!(body.len(), 1);
            assert!(matches!(body[0], PlPgSqlStmt::Exit));
        } else {
            panic!("Expected Loop statement");
        }

        let empty_loop = PlPgSqlStmt::Loop { body: vec![] };
        if let PlPgSqlStmt::Loop { body } = empty_loop {
            assert!(body.is_empty());
        } else {
            panic!("Expected Loop statement");
        }
    }

    #[test]
    fn test_plpgsql_stmt_exit() {
        let stmt = PlPgSqlStmt::Exit;
        assert!(matches!(stmt, PlPgSqlStmt::Exit));
    }

    #[test]
    fn test_plpgsql_stmt_continue() {
        let stmt = PlPgSqlStmt::Continue;
        assert!(matches!(stmt, PlPgSqlStmt::Continue));
    }

    #[test]
    fn test_plpgsql_stmt_case() {
        let stmt_with_else = PlPgSqlStmt::Case {
            expr: Expr::Column("status".to_string()),
            when_clauses: vec![
                (
                    Expr::String("active".to_string()),
                    vec![PlPgSqlStmt::Return { value: Some(Expr::String("Active".to_string())) }],
                ),
                (
                    Expr::String("inactive".to_string()),
                    vec![PlPgSqlStmt::Return { value: Some(Expr::String("Inactive".to_string())) }],
                ),
            ],
            else_stmts: vec![PlPgSqlStmt::Return {
                value: Some(Expr::String("Unknown".to_string())),
            }],
        };
        if let PlPgSqlStmt::Case { expr, when_clauses, else_stmts } = stmt_with_else {
            assert_eq!(expr, Expr::Column("status".to_string()));
            assert_eq!(when_clauses.len(), 2);
            assert_eq!(else_stmts.len(), 1);
        } else {
            panic!("Expected Case statement");
        }

        let stmt_no_else = PlPgSqlStmt::Case {
            expr: Expr::Column("status".to_string()),
            when_clauses: vec![(
                Expr::String("pending".to_string()),
                vec![PlPgSqlStmt::Raise { message: "Pending status".to_string() }],
            )],
            else_stmts: vec![],
        };
        if let PlPgSqlStmt::Case { expr, when_clauses, else_stmts } = stmt_no_else {
            assert_eq!(expr, Expr::Column("status".to_string()));
            assert_eq!(when_clauses.len(), 1);
            assert!(else_stmts.is_empty());
        } else {
            panic!("Expected Case statement");
        }
    }

    #[test]
    fn test_plpgsql_stmt_return() {
        let stmt_with_value = PlPgSqlStmt::Return { value: Some(Expr::Number(100)) };
        if let PlPgSqlStmt::Return { value } = stmt_with_value {
            assert_eq!(value, Some(Expr::Number(100)));
        } else {
            panic!("Expected Return statement");
        }

        let stmt_no_value = PlPgSqlStmt::Return { value: None };
        if let PlPgSqlStmt::Return { value } = stmt_no_value {
            assert_eq!(value, None);
        } else {
            panic!("Expected Return statement");
        }
    }

    #[test]
    fn test_plpgsql_stmt_execute() {
        let stmt = PlPgSqlStmt::Execute { query: "SELECT * FROM data".to_string() };
        if let PlPgSqlStmt::Execute { query } = stmt {
            assert_eq!(query, "SELECT * FROM data");
        } else {
            panic!("Expected Execute statement");
        }
    }

    #[test]
    fn test_plpgsql_stmt_perform() {
        let stmt = PlPgSqlStmt::Perform { query: "CALL my_proc()".to_string() };
        if let PlPgSqlStmt::Perform { query } = stmt {
            assert_eq!(query, "CALL my_proc()");
        } else {
            panic!("Expected Perform statement");
        }
    }

    #[test]
    fn test_plpgsql_stmt_exception_block() {
        let stmt = PlPgSqlStmt::ExceptionBlock {
            try_stmts: vec![PlPgSqlStmt::Execute { query: "PERFORM failing_func()".to_string() }],
            exception_var: "err_msg".to_string(),
            catch_stmts: vec![PlPgSqlStmt::Raise { message: "Error caught".to_string() }],
        };
        if let PlPgSqlStmt::ExceptionBlock { try_stmts, exception_var, catch_stmts } = stmt {
            assert_eq!(try_stmts.len(), 1);
            assert_eq!(exception_var, "err_msg");
            assert_eq!(catch_stmts.len(), 1);
        } else {
            panic!("Expected ExceptionBlock statement");
        }
    }

    #[test]
    fn test_plpgsql_stmt_raise() {
        let stmt = PlPgSqlStmt::Raise { message: "Something went wrong".to_string() };
        if let PlPgSqlStmt::Raise { message } = stmt {
            assert_eq!(message, "Something went wrong");
        } else {
            panic!("Expected Raise statement");
        }
    }

    #[test]
    fn test_plpgsql_function() {
        let func = PlPgSqlFunction {
            declarations: vec![PlPgSqlStmt::Declare {
                name: "counter".to_string(),
                data_type: "INTEGER".to_string(),
                default: Some(Expr::Number(0)),
            }],
            body: vec![PlPgSqlStmt::Loop { body: vec![PlPgSqlStmt::Exit] }],
        };
        assert_eq!(func.declarations.len(), 1);
        assert_eq!(func.body.len(), 1);

        let empty_func = PlPgSqlFunction { declarations: vec![], body: vec![] };
        assert!(empty_func.declarations.is_empty());
        assert!(empty_func.body.is_empty());
    }
}
