use std::io::Write;

#[derive(Debug, Clone, PartialEq)]
pub enum Message {
    Startup { user: String, database: String },
    Query { sql: String },
    Terminate,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Response {
    AuthenticationOk,
    ReadyForQuery,
    RowDescription { columns: Vec<String> },
    DataRow { values: Vec<Vec<u8>> },
    CommandComplete { tag: String },
    ErrorResponse { message: String },
}

impl Message {
    pub fn parse(tag: u8, data: &[u8]) -> Result<Self, ProtocolError> {
        match tag {
            b'Q' => Ok(Message::Query {
                sql: String::from_utf8_lossy(data).trim_end_matches('\0').to_string(),
            }),
            b'X' => Ok(Message::Terminate),
            0 => {
                let s = String::from_utf8_lossy(data);
                let mut user = String::new();
                let mut database = String::new();
                for part in s.split('\0') {
                    if part.starts_with("user") {
                        user = part.split('=').nth(1).unwrap_or("").to_string();
                    }
                    if part.starts_with("database") {
                        database = part.split('=').nth(1).unwrap_or("").to_string();
                    }
                }
                Ok(Message::Startup { user, database })
            }
            _ => Err(ProtocolError::UnknownMessage(tag)),
        }
    }
}

impl Response {
    pub fn write<W: Write>(&self, writer: &mut W) -> Result<(), ProtocolError> {
        match self {
            Response::AuthenticationOk => {
                writer.write_all(b"R")?;
                writer.write_all(&8i32.to_be_bytes())?;
                writer.write_all(&0i32.to_be_bytes())?;
            }
            Response::ReadyForQuery => {
                writer.write_all(b"Z")?;
                writer.write_all(&5i32.to_be_bytes())?;
                writer.write_all(b"I")?;
            }
            Response::CommandComplete { tag } => {
                writer.write_all(b"C")?;
                let len = 4 + tag.len() + 1;
                writer.write_all(&(len as i32).to_be_bytes())?;
                writer.write_all(tag.as_bytes())?;
                writer.write_all(b"\0")?;
            }
            Response::ErrorResponse { message } => {
                writer.write_all(b"E")?;
                // Calculate correct length: 4 (length itself) + field codes + strings + nulls
                let severity = b"SERROR\0";
                let msg_field = b"M";
                let msg_bytes = message.as_bytes();
                let len = 4 + severity.len() + msg_field.len() + msg_bytes.len() + 1 + 1;
                writer.write_all(&(len as i32).to_be_bytes())?;
                writer.write_all(severity)?;
                writer.write_all(msg_field)?;
                writer.write_all(msg_bytes)?;
                writer.write_all(b"\0\0")?;
            }
            _ => {}
        }
        Ok(())
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ProtocolError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Unknown message type: {0}")]
    UnknownMessage(u8),
    #[error("Invalid message format")]
    InvalidFormat,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_query_message() {
        let data = b"SELECT 1\0";
        let msg = Message::parse(b'Q', data).unwrap();
        assert_eq!(msg, Message::Query { sql: "SELECT 1".to_string() });
    }

    #[test]
    fn test_terminate_message() {
        let msg = Message::parse(b'X', &[]).unwrap();
        assert_eq!(msg, Message::Terminate);
    }

    #[test]
    fn test_auth_ok_response() {
        let mut buf = Vec::new();
        Response::AuthenticationOk.write(&mut buf).unwrap();
        assert_eq!(&buf[0..1], b"R");
    }
}
