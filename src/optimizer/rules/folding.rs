use super::{OptimizationRule, LogicalPlan};
use crate::parser::{Expr, BinaryOperator};

pub struct ConstantFolding;

impl ConstantFolding {
    fn fold_expr(&self, expr: Expr) -> Expr {
        match expr {
            Expr::BinaryOp { left, op, right } => {
                let left = self.fold_expr(*left);
                let right = self.fold_expr(*right);
                
                match (&left, &right, op) {
                    (Expr::Number(l), Expr::Number(r), BinaryOperator::Equals) => {
                        Expr::Number(if l == r { 1 } else { 0 })
                    }
                    _ => Expr::BinaryOp { left: Box::new(left), op, right: Box::new(right) }
                }
            }
            other => other,
        }
    }
}

impl OptimizationRule for ConstantFolding {
    fn apply(&self, plan: LogicalPlan) -> LogicalPlan {
        match plan {
            LogicalPlan::Filter { input, predicate } => {
                LogicalPlan::Filter {
                    input: Box::new(self.apply(*input)),
                    predicate: self.fold_expr(predicate),
                }
            }
            LogicalPlan::Project { input, columns } => {
                LogicalPlan::Project {
                    input: Box::new(self.apply(*input)),
                    columns,
                }
            }
            LogicalPlan::Join { left, right, condition } => {
                LogicalPlan::Join {
                    left: Box::new(self.apply(*left)),
                    right: Box::new(self.apply(*right)),
                    condition: condition.map(|c| self.fold_expr(c)),
                }
            }
            other => other,
        }
    }
    
    fn name(&self) -> &str {
        "ConstantFolding"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::BinaryOperator;
    
    #[test]
    fn test_fold_equality_true() {
        let rule = ConstantFolding;
        let expr = Expr::BinaryOp {
            left: Box::new(Expr::Number(5)),
            op: BinaryOperator::Equals,
            right: Box::new(Expr::Number(5)),
        };
        
        let folded = rule.fold_expr(expr);
        assert_eq!(folded, Expr::Number(1));
    }
    
    #[test]
    fn test_fold_equality_false() {
        let rule = ConstantFolding;
        let expr = Expr::BinaryOp {
            left: Box::new(Expr::Number(5)),
            op: BinaryOperator::Equals,
            right: Box::new(Expr::Number(10)),
        };
        
        let folded = rule.fold_expr(expr);
        assert_eq!(folded, Expr::Number(0));
    }
    
    #[test]
    fn test_fold_non_constant() {
        let rule = ConstantFolding;
        let expr = Expr::BinaryOp {
            left: Box::new(Expr::Column("id".to_string())),
            op: BinaryOperator::Equals,
            right: Box::new(Expr::Number(5)),
        };
        
        let folded = rule.fold_expr(expr.clone());
        assert_eq!(folded, expr);
    }
    
    #[test]
    fn test_apply_filter() {
        let rule = ConstantFolding;
        let scan = LogicalPlan::scan("users".to_string());
        let filter = LogicalPlan::filter(scan, Expr::BinaryOp {
            left: Box::new(Expr::Number(1)),
            op: BinaryOperator::Equals,
            right: Box::new(Expr::Number(1)),
        });
        
        let optimized = rule.apply(filter);
        match optimized {
            LogicalPlan::Filter { predicate, .. } => {
                assert_eq!(predicate, Expr::Number(1));
            }
            _ => panic!("Expected filter"),
        }
    }
    
    #[test]
    fn test_apply_project() {
        let rule = ConstantFolding;
        let scan = LogicalPlan::scan("users".to_string());
        let proj = LogicalPlan::project(scan, vec!["id".to_string()]);
        
        let optimized = rule.apply(proj);
        match optimized {
            LogicalPlan::Project { .. } => {},
            _ => panic!("Expected project"),
        }
    }
    
    #[test]
    fn test_apply_join() {
        let rule = ConstantFolding;
        let left = LogicalPlan::scan("users".to_string());
        let right = LogicalPlan::scan("orders".to_string());
        let join = LogicalPlan::Join {
            left: Box::new(left),
            right: Box::new(right),
            condition: Some(Expr::BinaryOp {
                left: Box::new(Expr::Number(1)),
                op: BinaryOperator::Equals,
                right: Box::new(Expr::Number(1)),
            }),
        };
        
        let optimized = rule.apply(join);
        match optimized {
            LogicalPlan::Join { condition, .. } => {
                assert_eq!(condition, Some(Expr::Number(1)));
            }
            _ => panic!("Expected join"),
        }
    }
    
    #[test]
    fn test_name() {
        let rule = ConstantFolding;
        assert_eq!(rule.name(), "ConstantFolding");
    }
}
