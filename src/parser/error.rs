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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_unexpected_token_error() {
        let err = ParseError::UnexpectedToken("IDENTIFIER".to_string());
        assert_eq!(format!("{}", err), "unexpected token: IDENTIFIER");
        assert_eq!(err, ParseError::UnexpectedToken("IDENTIFIER".to_string()));
    }

    #[test]
    fn test_unexpected_eof_error() {
        let err = ParseError::UnexpectedEOF;
        assert_eq!(format!("{}", err), "unexpected end of input");
        assert_eq!(err, ParseError::UnexpectedEOF);
    }

    #[test]
    fn test_invalid_syntax_error() {
        let err = ParseError::InvalidSyntax("missing semicolon".to_string());
        assert_eq!(format!("{}", err), "invalid syntax: missing semicolon");
        assert_eq!(err, ParseError::InvalidSyntax("missing semicolon".to_string()));
    }

    #[test]
    fn test_unsupported_error() {
        let err = ParseError::Unsupported("feature X".to_string());
        assert_eq!(format!("{}", err), "unsupported feature: feature X");
        assert_eq!(err, ParseError::Unsupported("feature X".to_string()));
    }

    #[test]
    fn test_error_debug_format() {
        let err = ParseError::UnexpectedToken("KEYWORD".to_string());
        let debug_str = format!("{:?}", err);
        assert!(debug_str.contains("UnexpectedToken"));
        assert!(debug_str.contains("KEYWORD"));
    }
}
