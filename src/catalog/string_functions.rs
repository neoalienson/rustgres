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

    // New tests for CONCAT
    #[test]
    fn test_concat_with_null() {
        let result = StringFunctions::concat(vec![
            Value::Text("Hello".to_string()),
            Value::Null,
            Value::Text("World".to_string()),
        ])
        .unwrap();
        assert_eq!(result, Value::Text("HelloWorld".to_string()));
    }

    #[test]
    fn test_concat_empty_vec() {
        let result = StringFunctions::concat(vec![]).unwrap();
        assert_eq!(result, Value::Text("".to_string()));
    }

    #[test]
    fn test_concat_mixed_types() {
        let result = StringFunctions::concat(vec![
            Value::Text("Number: ".to_string()),
            Value::Int(123),
            Value::Text(" and Text".to_string()),
        ])
        .unwrap();
        assert_eq!(result, Value::Text("Number: 123 and Text".to_string()));
    }

    #[test]
    fn test_concat_invalid_type() {
        let result = StringFunctions::concat(vec![Value::Bool(true)]);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "CONCAT requires text or numeric values");
    }

    // New tests for SUBSTRING
    #[test]
    fn test_substring_start_out_of_bounds() {
        let result = StringFunctions::substring(
            Value::Text("Hello".to_string()),
            Value::Int(10), // start pos is 9
            Some(Value::Int(2)),
        )
        .unwrap();
        assert_eq!(result, Value::Text("".to_string()));
    }

    #[test]
    fn test_substring_length_out_of_bounds() {
        let result = StringFunctions::substring(
            Value::Text("Hello".to_string()),
            Value::Int(1),
            Some(Value::Int(10)),
        )
        .unwrap();
        assert_eq!(result, Value::Text("Hello".to_string()));
    }

    #[test]
    fn test_substring_no_length() {
        let result = StringFunctions::substring(
            Value::Text("Hello World".to_string()),
            Value::Int(7), // start pos is 6
            None,
        )
        .unwrap();
        assert_eq!(result, Value::Text("World".to_string()));
    }

    #[test]
    fn test_substring_empty_string() {
        let result = StringFunctions::substring(
            Value::Text("".to_string()),
            Value::Int(1),
            Some(Value::Int(5)),
        )
        .unwrap();
        assert_eq!(result, Value::Text("".to_string()));
    }

    #[test]
    fn test_substring_start_zero() {
        let result = StringFunctions::substring(
            Value::Text("Hello".to_string()),
            Value::Int(0), // should be treated as 1
            Some(Value::Int(2)),
        )
        .unwrap();
        assert_eq!(result, Value::Text("He".to_string()));
    }

    #[test]
    fn test_substring_length_zero() {
        let result = StringFunctions::substring(
            Value::Text("Hello".to_string()),
            Value::Int(1),
            Some(Value::Int(0)),
        )
        .unwrap();
        assert_eq!(result, Value::Text("".to_string()));
    }

    #[test]
    fn test_substring_invalid_text_type() {
        let result =
            StringFunctions::substring(Value::Int(123), Value::Int(1), Some(Value::Int(2)));
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "SUBSTRING requires text value");
    }

    #[test]
    fn test_substring_invalid_start_type() {
        let result = StringFunctions::substring(
            Value::Text("Hello".to_string()),
            Value::Text("1".to_string()),
            Some(Value::Int(2)),
        );
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "SUBSTRING start must be integer");
    }

    #[test]
    fn test_substring_invalid_length_type() {
        let result = StringFunctions::substring(
            Value::Text("Hello".to_string()),
            Value::Int(1),
            Some(Value::Text("2".to_string())),
        );
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "SUBSTRING length must be integer");
    }

    // New tests for UPPER, LOWER, LENGTH, TRIM with edge cases
    #[test]
    fn test_upper_empty_string() {
        let result = StringFunctions::upper(Value::Text("".to_string())).unwrap();
        assert_eq!(result, Value::Text("".to_string()));
    }

    #[test]
    fn test_upper_invalid_type() {
        let result = StringFunctions::upper(Value::Int(123));
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "UPPER requires text value");
    }

    #[test]
    fn test_upper_null_input() {
        let result = StringFunctions::upper(Value::Null).unwrap();
        assert_eq!(result, Value::Null);
    }

    #[test]
    fn test_lower_empty_string() {
        let result = StringFunctions::lower(Value::Text("".to_string())).unwrap();
        assert_eq!(result, Value::Text("".to_string()));
    }

    #[test]
    fn test_lower_invalid_type() {
        let result = StringFunctions::lower(Value::Int(123));
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "LOWER requires text value");
    }

    #[test]
    fn test_lower_null_input() {
        let result = StringFunctions::lower(Value::Null).unwrap();
        assert_eq!(result, Value::Null);
    }

    #[test]
    fn test_length_empty_string() {
        let result = StringFunctions::length(Value::Text("".to_string())).unwrap();
        assert_eq!(result, Value::Int(0));
    }

    #[test]
    fn test_length_invalid_type() {
        let result = StringFunctions::length(Value::Int(123));
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "LENGTH requires text value");
    }

    #[test]
    fn test_length_null_input() {
        let result = StringFunctions::length(Value::Null).unwrap();
        assert_eq!(result, Value::Null);
    }

    #[test]
    fn test_trim_empty_string() {
        let result = StringFunctions::trim(Value::Text("".to_string())).unwrap();
        assert_eq!(result, Value::Text("".to_string()));
    }

    #[test]
    fn test_trim_whitespace_only_string() {
        let result = StringFunctions::trim(Value::Text("   ".to_string())).unwrap();
        assert_eq!(result, Value::Text("".to_string()));
    }

    #[test]
    fn test_trim_invalid_type() {
        let result = StringFunctions::trim(Value::Int(123));
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "TRIM requires text value");
    }

    #[test]
    fn test_trim_null_input() {
        let result = StringFunctions::trim(Value::Null).unwrap();
        assert_eq!(result, Value::Null);
    }
}
