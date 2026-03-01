use super::value::Value;
use crate::transaction::TupleHeader;

/// Tuple with MVCC header and data
#[derive(Debug, Clone)]
pub struct Tuple {
    pub header: TupleHeader,
    pub data: Vec<Value>,
}
