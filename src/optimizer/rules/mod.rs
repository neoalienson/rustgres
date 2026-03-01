use super::plan::LogicalPlan;

pub mod folding;
pub mod pruning;
pub mod pushdown;

pub use folding::ConstantFolding;
pub use pruning::ProjectionPruning;
pub use pushdown::PredicatePushdown;

pub trait OptimizationRule {
    fn apply(&self, plan: LogicalPlan) -> LogicalPlan;
    fn name(&self) -> &str;
}

pub struct RuleOptimizer {
    rules: Vec<Box<dyn OptimizationRule>>,
}

impl RuleOptimizer {
    pub fn new() -> Self {
        Self { rules: vec![] }
    }

    pub fn add_rule(&mut self, rule: Box<dyn OptimizationRule>) {
        self.rules.push(rule);
    }

    pub fn optimize(&self, mut plan: LogicalPlan) -> LogicalPlan {
        for rule in &self.rules {
            plan = rule.apply(plan);
        }
        plan
    }
}

impl Default for RuleOptimizer {
    fn default() -> Self {
        let mut optimizer = Self::new();
        optimizer.add_rule(Box::new(PredicatePushdown));
        optimizer.add_rule(Box::new(ProjectionPruning));
        optimizer.add_rule(Box::new(ConstantFolding));
        optimizer
    }
}
