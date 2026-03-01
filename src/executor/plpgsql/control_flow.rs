use crate::catalog::Value;

pub enum ControlFlow {
    None,
    Exit,
    Continue,
    Return(Value),
}
