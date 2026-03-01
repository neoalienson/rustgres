#[cfg(test)]
mod tests {
    use crate::parser::ast::{TriggerEvent, TriggerTiming};
    use crate::parser::{Parser, Statement};

    #[test]
    fn test_trigger_all_timing_combinations() {
        let cases = vec![
            (
                "CREATE TRIGGER t BEFORE INSERT ON t FOR EACH ROW BEGIN END",
                TriggerTiming::Before,
                TriggerEvent::Insert,
            ),
            (
                "CREATE TRIGGER t BEFORE UPDATE ON t FOR EACH ROW BEGIN END",
                TriggerTiming::Before,
                TriggerEvent::Update,
            ),
            (
                "CREATE TRIGGER t BEFORE DELETE ON t FOR EACH ROW BEGIN END",
                TriggerTiming::Before,
                TriggerEvent::Delete,
            ),
            (
                "CREATE TRIGGER t AFTER INSERT ON t FOR EACH ROW BEGIN END",
                TriggerTiming::After,
                TriggerEvent::Insert,
            ),
            (
                "CREATE TRIGGER t AFTER UPDATE ON t FOR EACH ROW BEGIN END",
                TriggerTiming::After,
                TriggerEvent::Update,
            ),
            (
                "CREATE TRIGGER t AFTER DELETE ON t FOR EACH ROW BEGIN END",
                TriggerTiming::After,
                TriggerEvent::Delete,
            ),
        ];

        for (sql, timing, event) in cases {
            let mut parser = Parser::new(sql).unwrap();
            let stmt = parser.parse().unwrap();

            match stmt {
                Statement::CreateTrigger(t) => {
                    assert_eq!(t.timing, timing);
                    assert_eq!(t.event, event);
                }
                _ => panic!("Expected CREATE TRIGGER"),
            }
        }
    }

    #[test]
    fn test_trigger_name_variations() {
        let names = vec!["t1", "my_trigger", "trigger_123", "MixedCase"];

        for name in names {
            let sql = format!("CREATE TRIGGER {} BEFORE INSERT ON t FOR EACH ROW BEGIN END", name);
            let mut parser = Parser::new(&sql).unwrap();
            let stmt = parser.parse().unwrap();

            match stmt {
                Statement::CreateTrigger(t) => {
                    assert_eq!(t.name, name);
                }
                _ => panic!("Expected CREATE TRIGGER"),
            }
        }
    }

    #[test]
    fn test_trigger_table_variations() {
        let tables = vec!["users", "orders", "products_123"];

        for table in tables {
            let sql = format!("CREATE TRIGGER t BEFORE INSERT ON {} FOR EACH ROW BEGIN END", table);
            let mut parser = Parser::new(&sql).unwrap();
            let stmt = parser.parse().unwrap();

            match stmt {
                Statement::CreateTrigger(t) => {
                    assert_eq!(t.table, table);
                }
                _ => panic!("Expected CREATE TRIGGER"),
            }
        }
    }

    #[test]
    fn test_trigger_when_with_comparison() {
        let mut parser = Parser::new(
            "CREATE TRIGGER t BEFORE INSERT ON users FOR EACH ROW WHEN (age > 18) BEGIN END",
        )
        .unwrap();
        let stmt = parser.parse().unwrap();

        match stmt {
            Statement::CreateTrigger(t) => {
                assert!(t.when.is_some());
            }
            _ => panic!("Expected CREATE TRIGGER"),
        }
    }

    #[test]
    fn test_trigger_when_with_and() {
        let mut parser = Parser::new("CREATE TRIGGER t BEFORE INSERT ON users FOR EACH ROW WHEN (active = 1 AND age > 18) BEGIN END").unwrap();
        let stmt = parser.parse().unwrap();

        match stmt {
            Statement::CreateTrigger(t) => {
                assert!(t.when.is_some());
            }
            _ => panic!("Expected CREATE TRIGGER"),
        }
    }

    #[test]
    fn test_trigger_when_with_or() {
        let mut parser = Parser::new("CREATE TRIGGER t BEFORE INSERT ON users FOR EACH ROW WHEN (status = 1 OR status = 2) BEGIN END").unwrap();
        let stmt = parser.parse().unwrap();

        match stmt {
            Statement::CreateTrigger(t) => {
                assert!(t.when.is_some());
            }
            _ => panic!("Expected CREATE TRIGGER"),
        }
    }

    #[test]
    fn test_trigger_case_insensitive() {
        let mut parser =
            Parser::new("create trigger MyTrigger before insert on MyTable for each row begin end")
                .unwrap();
        let stmt = parser.parse().unwrap();

        match stmt {
            Statement::CreateTrigger(t) => {
                assert_eq!(t.name, "MyTrigger");
                assert_eq!(t.table, "MyTable");
            }
            _ => panic!("Expected CREATE TRIGGER"),
        }
    }

    #[test]
    fn test_drop_trigger_case_insensitive() {
        let mut parser = Parser::new("drop trigger MyTrigger").unwrap();
        let stmt = parser.parse().unwrap();

        match stmt {
            Statement::DropTrigger(d) => {
                assert_eq!(d.name, "MyTrigger");
            }
            _ => panic!("Expected DROP TRIGGER"),
        }
    }

    #[test]
    fn test_trigger_empty_body() {
        let mut parser =
            Parser::new("CREATE TRIGGER t BEFORE INSERT ON users FOR EACH ROW BEGIN END").unwrap();
        let stmt = parser.parse().unwrap();

        match stmt {
            Statement::CreateTrigger(t) => {
                assert_eq!(t.body.len(), 0);
            }
            _ => panic!("Expected CREATE TRIGGER"),
        }
    }

    #[test]
    fn test_trigger_with_single_statement_body() {
        let mut parser = Parser::new("CREATE TRIGGER t BEFORE INSERT ON users FOR EACH ROW BEGIN INSERT INTO log VALUES (1); END").unwrap();
        let stmt = parser.parse().unwrap();

        match stmt {
            Statement::CreateTrigger(t) => {
                assert_eq!(t.body.len(), 1);
            }
            _ => panic!("Expected CREATE TRIGGER"),
        }
    }

    #[test]
    fn test_trigger_with_multiple_statement_body() {
        let mut parser = Parser::new("CREATE TRIGGER t BEFORE INSERT ON users FOR EACH ROW BEGIN INSERT INTO log VALUES (1); UPDATE stats SET total = 10; END").unwrap();
        let stmt = parser.parse().unwrap();

        match stmt {
            Statement::CreateTrigger(t) => {
                assert_eq!(t.body.len(), 2);
            }
            _ => panic!("Expected CREATE TRIGGER"),
        }
    }

    #[test]
    fn test_trigger_all_comparison_operators() {
        let ops = vec!["=", "!=", "<", "<=", ">", ">="];

        for op in ops {
            let sql = format!(
                "CREATE TRIGGER t BEFORE INSERT ON users FOR EACH ROW WHEN (x {} 10) BEGIN END",
                op
            );
            let mut parser = Parser::new(&sql).unwrap();
            let stmt = parser.parse().unwrap();

            match stmt {
                Statement::CreateTrigger(t) => {
                    assert!(t.when.is_some());
                }
                _ => panic!("Expected CREATE TRIGGER"),
            }
        }
    }
}
