pub mod collector;
pub mod error;
pub mod histogram;

#[cfg(test)]
mod edge_tests;

pub use collector::{Analyzer, ColumnStats, TableStats};
pub use error::{Result, StatisticsError};
pub use histogram::Histogram;
