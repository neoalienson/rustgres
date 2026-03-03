use crate::catalog::Value;

pub struct StringFunctions;

impl StringFunctions {
    pub fn concat(values: Vec<Value>) -> Result<Value, String> {
        let mut result = String::new();
        for val in values {
            match val {
                Value::Text(s) => result.push_str(&s),
                Value::Int(i) => result.push_str(&i.to_string()),
                Value::Null => continue,
                _ => return Err("CONCAT requires text or numeric values".to_string()),
            }
        }
        Ok(Value::Text(result))
    }

    pub fn substring(text: Value, start: Value, length: Option<Value>) -> Result<Value, String> {
        let s = match text {
            Value::Text(s) => s,
            _ => return Err("SUBSTRING requires text value".to_string()),
        };

        let start_pos = match start {
            Value::Int(i) => i.max(1) as usize - 1,
            _ => return Err("SUBSTRING start must be integer".to_string()),
        };

        if let Some(len_val) = length {
            let len = match len_val {
                Value::Int(i) => i.max(0) as usize,
                _ => return Err("SUBSTRING length must be integer".to_string()),
            };
            Ok(Value::Text(s.chars().skip(start_pos).take(len).collect()))
        } else {
            Ok(Value::Text(s.chars().skip(start_pos).collect()))
        }
    }

    pub fn upper(text: Value) -> Result<Value, String> {
        match text {
            Value::Text(s) => Ok(Value::Text(s.to_uppercase())),
            Value::Null => Ok(Value::Null),
            _ => Err("UPPER requires text value".to_string()),
        }
    }

    pub fn lower(text: Value) -> Result<Value, String> {
        match text {
            Value::Text(s) => Ok(Value::Text(s.to_lowercase())),
            Value::Null => Ok(Value::Null),
            _ => Err("LOWER requires text value".to_string()),
        }
    }

    pub fn length(text: Value) -> Result<Value, String> {
        match text {
            Value::Text(s) => Ok(Value::Int(s.len() as i64)),
            Value::Null => Ok(Value::Null),
            _ => Err("LENGTH requires text value".to_string()),
        }
    }

    pub fn trim(text: Value) -> Result<Value, String> {
        match text {
            Value::Text(s) => Ok(Value::Text(s.trim().to_string())),
            Value::Null => Ok(Value::Null),
            _ => Err("TRIM requires text value".to_string()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_concat() {
        let result = StringFunctions::concat(vec![
            Value::Text("Hello".to_string()),
            Value::Text(" ".to_string()),
            Value::Text("World".to_string()),
        ])
        .unwrap();
        assert_eq!(result, Value::Text("Hello World".to_string()));
    }

    #[test]
    fn test_substring() {
        let result = StringFunctions::substring(
            Value::Text("Hello World".to_string()),
            Value::Int(1),
            Some(Value::Int(5)),
        )
        .unwrap();
        assert_eq!(result, Value::Text("Hello".to_string()));
    }

    #[test]
    fn test_upper() {
        let result = StringFunctions::upper(Value::Text("hello".to_string())).unwrap();
        assert_eq!(result, Value::Text("HELLO".to_string()));
    }

    #[test]
    fn test_lower() {
        let result = StringFunctions::lower(Value::Text("HELLO".to_string())).unwrap();
        assert_eq!(result, Value::Text("hello".to_string()));
    }

    #[test]
    fn test_length() {
        let result = StringFunctions::length(Value::Text("Hello".to_string())).unwrap();
        assert_eq!(result, Value::Int(5));
    }

    #[test]
    fn test_trim() {
        let result = StringFunctions::trim(Value::Text("  hello  ".to_string())).unwrap();
        assert_eq!(result, Value::Text("hello".to_string()));
    }
}
