use super::{OptimizationRule, LogicalPlan};

pub struct ProjectionPruning;

impl OptimizationRule for ProjectionPruning {
    fn apply(&self, plan: LogicalPlan) -> LogicalPlan {
        match plan {
            LogicalPlan::Project { input, columns } => {
                match *input {
                    LogicalPlan::Project { input: inner, columns: inner_cols } => {
                        let pruned: Vec<String> = inner_cols.into_iter()
                            .filter(|c| columns.contains(c))
                            .collect();
                        LogicalPlan::Project {
                            input: Box::new(self.apply(*inner)),
                            columns: if pruned.is_empty() { columns } else { pruned },
                        }
                    }
                    other => LogicalPlan::Project {
                        input: Box::new(self.apply(other)),
                        columns,
                    }
                }
            }
            LogicalPlan::Filter { input, predicate } => {
                LogicalPlan::Filter {
                    input: Box::new(self.apply(*input)),
                    predicate,
                }
            }
            LogicalPlan::Join { left, right, condition } => {
                LogicalPlan::Join {
                    left: Box::new(self.apply(*left)),
                    right: Box::new(self.apply(*right)),
                    condition,
                }
            }
            other => other,
        }
    }
    
    fn name(&self) -> &str {
        "ProjectionPruning"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_prune_nested_projections() {
        let rule = ProjectionPruning;
        let scan = LogicalPlan::scan("users".to_string());
        let proj1 = LogicalPlan::project(scan, vec!["a".to_string(), "b".to_string()]);
        let proj2 = LogicalPlan::project(proj1, vec!["a".to_string()]);
        
        let optimized = rule.apply(proj2);
        match optimized {
            LogicalPlan::Project { columns, .. } => assert_eq!(columns.len(), 1),
            _ => panic!("Expected project"),
        }
    }
    
    #[test]
    fn test_no_pruning_needed() {
        let rule = ProjectionPruning;
        let scan = LogicalPlan::scan("users".to_string());
        let proj = LogicalPlan::project(scan, vec!["a".to_string()]);
        
        let optimized = rule.apply(proj);
        match optimized {
            LogicalPlan::Project { columns, .. } => assert_eq!(columns.len(), 1),
            _ => panic!("Expected project"),
        }
    }
    
    #[test]
    fn test_apply_filter() {
        let rule = ProjectionPruning;
        let scan = LogicalPlan::scan("users".to_string());
        let filter = LogicalPlan::filter(scan, crate::parser::Expr::Column("id".to_string()));
        
        let optimized = rule.apply(filter);
        match optimized {
            LogicalPlan::Filter { .. } => {},
            _ => panic!("Expected filter"),
        }
    }
    
    #[test]
    fn test_apply_join() {
        let rule = ProjectionPruning;
        let left = LogicalPlan::scan("users".to_string());
        let right = LogicalPlan::scan("orders".to_string());
        let join = LogicalPlan::join(left, right);
        
        let optimized = rule.apply(join);
        match optimized {
            LogicalPlan::Join { .. } => {},
            _ => panic!("Expected join"),
        }
    }
    
    #[test]
    fn test_apply_scan() {
        let rule = ProjectionPruning;
        let scan = LogicalPlan::scan("users".to_string());
        
        let optimized = rule.apply(scan.clone());
        match optimized {
            LogicalPlan::Scan { .. } => {},
            _ => panic!("Expected scan"),
        }
    }
    
    #[test]
    fn test_name() {
        let rule = ProjectionPruning;
        assert_eq!(rule.name(), "ProjectionPruning");
    }
}
