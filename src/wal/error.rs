use thiserror::Error;

#[derive(Error, Debug, PartialEq)]
pub enum WALError {
    #[error("WAL write failed: {0}")]
    WriteFailed(String),
    
    #[error("WAL flush failed: {0}")]
    FlushFailed(String),
    
    #[error("invalid LSN: {0}")]
    InvalidLSN(u64),
    
    #[error("recovery failed: {0}")]
    RecoveryFailed(String),
}

pub type Result<T> = std::result::Result<T, WALError>;
