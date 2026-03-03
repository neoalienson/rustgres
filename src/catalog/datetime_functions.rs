use crate::catalog::Value;
use std::time::{SystemTime, UNIX_EPOCH};

pub struct DateTimeFunctions;

impl DateTimeFunctions {
    pub fn now() -> Result<Value, String> {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|e| format!("System time error: {}", e))?
            .as_secs() as i64;
        Ok(Value::Int(timestamp))
    }

    pub fn current_timestamp() -> Result<Value, String> {
        Self::now()
    }

    pub fn current_date() -> Result<Value, String> {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|e| format!("System time error: {}", e))?
            .as_secs() as i64;
        // Return days since epoch
        Ok(Value::Int(timestamp / 86400))
    }

    pub fn extract(field: &str, timestamp: Value) -> Result<Value, String> {
        let ts = match timestamp {
            Value::Int(i) => i,
            _ => return Err("EXTRACT requires integer timestamp".to_string()),
        };

        match field.to_uppercase().as_str() {
            "EPOCH" => Ok(Value::Int(ts)),
            "YEAR" => {
                let year = 1970 + (ts / 31536000);
                Ok(Value::Int(year))
            }
            "MONTH" => {
                let days = ts / 86400;
                let month = ((days % 365) / 30) + 1;
                Ok(Value::Int(month.min(12)))
            }
            "DAY" => {
                let day = (ts / 86400) % 365;
                Ok(Value::Int(day))
            }
            "HOUR" => {
                let hour = (ts / 3600) % 24;
                Ok(Value::Int(hour))
            }
            "MINUTE" => {
                let minute = (ts / 60) % 60;
                Ok(Value::Int(minute))
            }
            "SECOND" => {
                let second = ts % 60;
                Ok(Value::Int(second))
            }
            _ => Err(format!("Unknown EXTRACT field: {}", field)),
        }
    }

    pub fn date_trunc(precision: &str, timestamp: Value) -> Result<Value, String> {
        let ts = match timestamp {
            Value::Int(i) => i,
            _ => return Err("DATE_TRUNC requires integer timestamp".to_string()),
        };

        match precision.to_uppercase().as_str() {
            "YEAR" => {
                let years = ts / 31536000;
                Ok(Value::Int(years * 31536000))
            }
            "MONTH" => {
                let months = ts / 2592000;
                Ok(Value::Int(months * 2592000))
            }
            "DAY" => {
                let days = ts / 86400;
                Ok(Value::Int(days * 86400))
            }
            "HOUR" => {
                let hours = ts / 3600;
                Ok(Value::Int(hours * 3600))
            }
            "MINUTE" => {
                let minutes = ts / 60;
                Ok(Value::Int(minutes * 60))
            }
            _ => Err(format!("Unknown DATE_TRUNC precision: {}", precision)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_now() {
        let result = DateTimeFunctions::now().unwrap();
        match result {
            Value::Int(ts) => assert!(ts > 0),
            _ => panic!("Expected Int"),
        }
    }

    #[test]
    fn test_extract_epoch() {
        let result = DateTimeFunctions::extract("EPOCH", Value::Int(1000000)).unwrap();
        assert_eq!(result, Value::Int(1000000));
    }

    #[test]
    fn test_extract_hour() {
        let result = DateTimeFunctions::extract("HOUR", Value::Int(3661)).unwrap();
        assert_eq!(result, Value::Int(1));
    }

    #[test]
    fn test_date_trunc_day() {
        let result = DateTimeFunctions::date_trunc("DAY", Value::Int(90000)).unwrap();
        assert_eq!(result, Value::Int(86400));
    }

    #[test]
    fn test_current_date() {
        let result = DateTimeFunctions::current_date().unwrap();
        match result {
            Value::Int(days) => assert!(days > 0),
            _ => panic!("Expected Int"),
        }
    }
}
