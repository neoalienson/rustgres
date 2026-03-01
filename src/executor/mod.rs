#![allow(clippy::module_inception)]

mod aggregate;
mod case;
mod cte;
mod distinct;
mod except;
mod executor;
mod filter;
mod group_by;
mod hash_agg;
mod hash_join;
mod having;
mod intersect;
mod join;
mod limit;
mod merge_join;
mod mock;
mod nested_loop;
mod project;
mod seq_scan;
mod sort;
mod subquery;
mod union;
mod window;

#[cfg(test)]
mod aggregate_edge_tests;
#[cfg(test)]
mod case_edge_tests;
#[cfg(test)]
mod cte_edge_tests;
#[cfg(test)]
mod distinct_edge_tests;
#[cfg(test)]
mod edge_tests;
#[cfg(test)]
mod except_edge_tests;
#[cfg(test)]
mod group_by_edge_tests;
#[cfg(test)]
mod having_edge_tests;
#[cfg(test)]
mod intersect_edge_tests;
#[cfg(test)]
mod join_edge_tests;
#[cfg(test)]
mod limit_edge_tests;
#[cfg(test)]
mod merge_join_edge_tests;
#[cfg(test)]
mod subquery_edge_tests;
#[cfg(test)]
mod union_edge_tests;
#[cfg(test)]
mod window_edge_tests;

pub use aggregate::{Aggregate, AggregateFunction};
pub use case::Case;
pub use cte::CTE;
pub use distinct::Distinct;
pub use except::Except;
pub use executor::{Executor, ExecutorError, SimpleExecutor, SimpleTuple, Tuple, Value};
pub use filter::Filter;
pub use group_by::GroupBy;
pub use hash_agg::HashAgg;
pub use hash_join::HashJoin;
pub use having::Having;
pub use intersect::Intersect;
pub use join::{Join, JoinType};
pub use limit::Limit;
pub use merge_join::MergeJoin;
pub use mock::MockExecutor;
pub use nested_loop::NestedLoopJoin;
pub use project::Project;
pub use seq_scan::SeqScan;
pub use sort::Sort;
pub use subquery::Subquery;
pub use union::Union;
pub use window::{Window, WindowFunction};
