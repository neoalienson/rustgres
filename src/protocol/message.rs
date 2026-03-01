use super::result_set::ColumnMetadata;
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
    // New wire protocol messages
    RowDescriptionDetailed { columns: Vec<ColumnMetadata> },
    DataRowDetailed { fields: Vec<Option<Vec<u8>>> },
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
            Response::RowDescriptionDetailed { columns } => {
                writer.write_all(b"T")?;
                
                // Calculate length
                let mut length = 4 + 2; // length field + field count
                for col in columns {
                    length += col.name.len() + 1; // name + null terminator
                    length += 4 + 2 + 4 + 2 + 4 + 2; // oids and sizes
                }
                
                writer.write_all(&(length as i32).to_be_bytes())?;
                writer.write_all(&(columns.len() as i16).to_be_bytes())?;
                
                for col in columns {
                    writer.write_all(col.name.as_bytes())?;
                    writer.write_all(b"\0")?;
                    writer.write_all(&col.table_oid.to_be_bytes())?;
                    writer.write_all(&col.column_attr_number.to_be_bytes())?;
                    writer.write_all(&col.type_oid.to_be_bytes())?;
                    writer.write_all(&col.type_size.to_be_bytes())?;
                    writer.write_all(&col.type_modifier.to_be_bytes())?;
                    writer.write_all(&col.format_code.to_be_bytes())?;
                }
            }
            Response::DataRowDetailed { fields } => {
                writer.write_all(b"D")?;
                
                // Calculate length
                let mut length = 4 + 2; // length field + field count
                for field in fields {
                    length += 4; // field length
                    if let Some(data) = field {
                        length += data.len();
                    }
                }
                
                writer.write_all(&(length as i32).to_be_bytes())?;
                writer.write_all(&(fields.len() as i16).to_be_bytes())?;
                
                for field in fields {
                    match field {
                        None => {
                            writer.write_all(&(-1i32).to_be_bytes())?;
                        }
                        Some(data) => {
                            writer.write_all(&(data.len() as i32).to_be_bytes())?;
                            writer.write_all(data)?;
                        }
                    }
                }
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

    #[test]
    fn test_row_description_detailed() {
        let columns = vec![ColumnMetadata {
            name: "id".to_string(),
            table_oid: 0,
            column_attr_number: 0,
            type_oid: 23,
            type_size: 4,
            type_modifier: -1,
            format_code: 0,
        }];

        let mut buf = Vec::new();
        Response::RowDescriptionDetailed { columns }.write(&mut buf).unwrap();

        assert_eq!(buf[0], b'T');
        // Verify field count
        let field_count = i16::from_be_bytes([buf[5], buf[6]]);
        assert_eq!(field_count, 1);
    }

    #[test]
    fn test_data_row_detailed() {
        let fields = vec![Some(b"42".to_vec()), Some(b"Alice".to_vec())];

        let mut buf = Vec::new();
        Response::DataRowDetailed { fields }.write(&mut buf).unwrap();

        assert_eq!(buf[0], b'D');
        // Verify field count
        let field_count = i16::from_be_bytes([buf[5], buf[6]]);
        assert_eq!(field_count, 2);
    }

    #[test]
    fn test_data_row_with_null() {
        let fields = vec![Some(b"1".to_vec()), None, Some(b"test".to_vec())];

        let mut buf = Vec::new();
        Response::DataRowDetailed { fields }.write(&mut buf).unwrap();

        assert_eq!(buf[0], b'D');
        let field_count = i16::from_be_bytes([buf[5], buf[6]]);
        assert_eq!(field_count, 3);
    }
}
