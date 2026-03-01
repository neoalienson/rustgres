use crate::parser::Expr;

#[derive(Debug, Clone)]
pub enum LogicalPlan {
    Scan { table: String, filter: Option<Expr>, columns: Vec<String> },
    Filter { input: Box<LogicalPlan>, predicate: Expr },
    Project { input: Box<LogicalPlan>, columns: Vec<String> },
    Join { left: Box<LogicalPlan>, right: Box<LogicalPlan>, condition: Option<Expr> },
}

impl LogicalPlan {
    pub fn scan(table: String) -> Self {
        Self::Scan { table, filter: None, columns: vec![] }
    }

    pub fn filter(input: LogicalPlan, predicate: Expr) -> Self {
        Self::Filter { input: Box::new(input), predicate }
    }

    pub fn project(input: LogicalPlan, columns: Vec<String>) -> Self {
        Self::Project { input: Box::new(input), columns }
    }

    pub fn join(left: LogicalPlan, right: LogicalPlan) -> Self {
        Self::Join { left: Box::new(left), right: Box::new(right), condition: None }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::ast::{BinaryOperator, Expr as AstExpr};

    #[test]
    fn test_scan_plan() {
        let plan = LogicalPlan::scan("users".to_string());
        match plan {
            LogicalPlan::Scan { table, .. } => assert_eq!(table, "users"),
            _ => panic!("Expected Scan plan"),
        }
    }

    #[test]
    fn test_filter_plan() {
        let scan = LogicalPlan::scan("users".to_string());
        let predicate = AstExpr::BinaryOp {
            left: Box::new(AstExpr::Column("id".to_string())),
            op: BinaryOperator::Equals,
            right: Box::new(AstExpr::Number(1)),
        };
        let plan = LogicalPlan::filter(scan, predicate);

        match plan {
            LogicalPlan::Filter { .. } => {}
            _ => panic!("Expected Filter plan"),
        }
    }

    #[test]
    fn test_project_plan() {
        let scan = LogicalPlan::scan("users".to_string());
        let plan = LogicalPlan::project(scan, vec!["id".to_string(), "name".to_string()]);

        match plan {
            LogicalPlan::Project { columns, .. } => assert_eq!(columns.len(), 2),
            _ => panic!("Expected Project plan"),
        }
    }

    #[test]
    fn test_join_plan() {
        let left = LogicalPlan::scan("users".to_string());
        let right = LogicalPlan::scan("orders".to_string());
        let plan = LogicalPlan::join(left, right);

        match plan {
            LogicalPlan::Join { .. } => {}
            _ => panic!("Expected Join plan"),
        }
    }
}
