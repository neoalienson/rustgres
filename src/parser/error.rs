use thiserror::Error;

#[derive(Error, Debug, PartialEq)]
pub enum ParseError {
    #[error("unexpected token: {0}")]
    UnexpectedToken(String),

    #[error("unexpected end of input")]
    UnexpectedEOF,

    #[error("invalid syntax: {0}")]
    InvalidSyntax(String),

    #[error("unsupported feature: {0}")]
    Unsupported(String),
}

pub type Result<T> = std::result::Result<T, ParseError>;
