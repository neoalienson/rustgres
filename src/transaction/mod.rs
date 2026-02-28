//! Transaction management with MVCC.
//!
//! This module provides:
//! - Transaction ID generation
//! - MVCC tuple visibility
//! - Snapshot isolation
//! - Basic lock manager

pub mod error;
pub mod manager;
pub mod snapshot;
pub mod mvcc;
pub mod lock;

pub use error::{TransactionError, Result};
pub use manager::{TransactionManager, Transaction, TransactionId, TransactionState};
pub use snapshot::Snapshot;
pub use mvcc::TupleHeader;
pub use lock::{LockManager, LockMode, LockKey};

#[cfg(test)]
mod edge_tests;
