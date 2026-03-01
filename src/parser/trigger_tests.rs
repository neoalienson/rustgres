#[cfg(test)]
mod tests {
    use crate::parser::ast::{TriggerEvent, TriggerFor, TriggerTiming};
    use crate::parser::{Parser, Statement};

    #[test]
    fn test_create_trigger_before_insert() {
        let mut parser =
            Parser::new("CREATE TRIGGER t BEFORE INSERT ON users FOR EACH ROW BEGIN END").unwrap();
        let stmt = parser.parse().unwrap();

        match stmt {
            Statement::CreateTrigger(t) => {
                assert_eq!(t.name, "t");
                assert_eq!(t.timing, TriggerTiming::Before);
                assert_eq!(t.event, TriggerEvent::Insert);
                assert_eq!(t.table, "users");
                assert_eq!(t.for_each, TriggerFor::EachRow);
            }
            _ => panic!("Expected CREATE TRIGGER"),
        }
    }

    #[test]
    fn test_create_trigger_after_update() {
        let mut parser =
            Parser::new("CREATE TRIGGER t AFTER UPDATE ON users FOR EACH ROW BEGIN END").unwrap();
        let stmt = parser.parse().unwrap();

        match stmt {
            Statement::CreateTrigger(t) => {
                assert_eq!(t.timing, TriggerTiming::After);
                assert_eq!(t.event, TriggerEvent::Update);
            }
            _ => panic!("Expected CREATE TRIGGER"),
        }
    }

    #[test]
    fn test_create_trigger_before_delete() {
        let mut parser =
            Parser::new("CREATE TRIGGER t BEFORE DELETE ON users FOR EACH ROW BEGIN END").unwrap();
        let stmt = parser.parse().unwrap();

        match stmt {
            Statement::CreateTrigger(t) => {
                assert_eq!(t.event, TriggerEvent::Delete);
            }
            _ => panic!("Expected CREATE TRIGGER"),
        }
    }

    #[test]
    fn test_create_trigger_for_each_statement() {
        let mut parser =
            Parser::new("CREATE TRIGGER t AFTER INSERT ON users FOR EACH STATEMENT BEGIN END")
                .unwrap();
        let stmt = parser.parse().unwrap();

        match stmt {
            Statement::CreateTrigger(t) => {
                assert_eq!(t.for_each, TriggerFor::EachStatement);
            }
            _ => panic!("Expected CREATE TRIGGER"),
        }
    }

    #[test]
    fn test_create_trigger_with_when() {
        let mut parser = Parser::new(
            "CREATE TRIGGER t BEFORE INSERT ON users FOR EACH ROW WHEN (active = 1) BEGIN END",
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
    fn test_drop_trigger_basic() {
        let mut parser = Parser::new("DROP TRIGGER t").unwrap();
        let stmt = parser.parse().unwrap();

        match stmt {
            Statement::DropTrigger(d) => {
                assert_eq!(d.name, "t");
                assert!(!d.if_exists);
            }
            _ => panic!("Expected DROP TRIGGER"),
        }
    }

    #[test]
    fn test_drop_trigger_if_exists() {
        let mut parser = Parser::new("DROP TRIGGER IF EXISTS t").unwrap();
        let stmt = parser.parse().unwrap();

        match stmt {
            Statement::DropTrigger(d) => {
                assert_eq!(d.name, "t");
                assert!(d.if_exists);
            }
            _ => panic!("Expected DROP TRIGGER"),
        }
    }
}
