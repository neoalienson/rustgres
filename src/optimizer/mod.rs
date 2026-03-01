pub mod cost;
pub mod error;
pub mod join_order;
pub mod plan;
pub mod rules;
pub mod selectivity;

#[cfg(test)]
mod edge_tests;

pub use cost::{Cost, CostModel};
pub use error::{OptimizerError, Result};
pub use join_order::{JoinOptimizer, JoinPlan, Relation};
pub use plan::LogicalPlan;
pub use rules::{
    ConstantFolding, OptimizationRule, PredicatePushdown, ProjectionPruning, RuleOptimizer,
};
pub use selectivity::SelectivityEstimator;
