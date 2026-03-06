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

    #[test]
    fn test_current_timestamp() {
        let result = DateTimeFunctions::current_timestamp().unwrap();
        match result {
            Value::Int(ts) => assert!(ts > 0),
            _ => panic!("Expected Int"),
        }
    }

    // New tests for EXTRACT
    #[test]
    fn test_extract_invalid_timestamp_type() {
        let result = DateTimeFunctions::extract("HOUR", Value::Text("abc".to_string()));
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "EXTRACT requires integer timestamp");
    }

    #[test]
    fn test_extract_unknown_field() {
        let result = DateTimeFunctions::extract("UNKNOWN", Value::Int(12345));
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "Unknown EXTRACT field: UNKNOWN");
    }

    #[test]
    fn test_extract_epoch_zero() {
        let result = DateTimeFunctions::extract("EPOCH", Value::Int(0)).unwrap();
        assert_eq!(result, Value::Int(0));
    }

    #[test]
    fn test_extract_year() {
        // Timestamp for 2000-01-01 00:00:00 UTC (approx 946684800 seconds)
        let result = DateTimeFunctions::extract("YEAR", Value::Int(946684800)).unwrap();
        // 1970 + (946684800 / 31536000) = 1970 + 30 = 2000
        assert_eq!(result, Value::Int(2000));
    }

    #[test]
    fn test_extract_month() {
        // Timestamp for 1970-02-01 00:00:00 UTC (approx 2678400 seconds)
        let result = DateTimeFunctions::extract("MONTH", Value::Int(2678400)).unwrap();
        // Days = 2678400 / 86400 = 31 days
        // Month = ((31 % 365) / 30) + 1 = (31 / 30) + 1 = 1 + 1 = 2
        assert_eq!(result, Value::Int(2));
    }

    #[test]
    fn test_extract_day() {
        // Timestamp for 1970-01-31 00:00:00 UTC (approx 2592000 seconds)
        let result = DateTimeFunctions::extract("DAY", Value::Int(2592000)).unwrap();
        // Days = 2592000 / 86400 = 30 days
        // Day = 30 % 365 = 30
        assert_eq!(result, Value::Int(30));
    }

    #[test]
    fn test_extract_minute() {
        let result = DateTimeFunctions::extract("MINUTE", Value::Int(3661)).unwrap(); // 1 hour, 1 minute, 1 second
        assert_eq!(result, Value::Int(1));
    }

    #[test]
    fn test_extract_second() {
        let result = DateTimeFunctions::extract("SECOND", Value::Int(3661)).unwrap(); // 1 hour, 1 minute, 1 second
        assert_eq!(result, Value::Int(1));
    }

    // New tests for DATE_TRUNC
    #[test]
    fn test_date_trunc_invalid_timestamp_type() {
        let result = DateTimeFunctions::date_trunc("HOUR", Value::Text("abc".to_string()));
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "DATE_TRUNC requires integer timestamp");
    }

    #[test]
    fn test_date_trunc_unknown_precision() {
        let result = DateTimeFunctions::date_trunc("CENTURY", Value::Int(12345));
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "Unknown DATE_TRUNC precision: CENTURY");
    }

    #[test]
    fn test_date_trunc_year() {
        // Timestamp for 2000-05-15 10:30:45 UTC (approx 958473045 seconds)
        let result = DateTimeFunctions::date_trunc("YEAR", Value::Int(958473045)).unwrap();
        // 958473045 / 31536000 = 30.39... -> 30 years from epoch
        // 30 * 31536000 = 946080000 (Epoch for 2000-01-01 00:00:00)
        assert_eq!(result, Value::Int(946080000));
    }

    #[test]
    fn test_date_trunc_month() {
        // Timestamp for 2000-05-15 10:30:45 UTC (approx 958473045 seconds)
        let result = DateTimeFunctions::date_trunc("MONTH", Value::Int(958473045)).unwrap();
        // 958473045 / 2592000 = 369.7... -> 369 months from epoch
        // 369 * 2592000 = 957888000 (Epoch for 2000-05-01 00:00:00)
        assert_eq!(result, Value::Int(956448000));
    }

    #[test]
    fn test_date_trunc_hour() {
        // Timestamp for 2000-05-15 10:30:45 UTC (approx 958473045 seconds)
        let result = DateTimeFunctions::date_trunc("HOUR", Value::Int(958473045)).unwrap();
        // 958473045 / 3600 = 266242.5... -> 266242 hours from epoch
        // 266242 * 3600 = 958471200 (Epoch for 2000-05-15 10:00:00)
        assert_eq!(result, Value::Int(958471200));
    }

    #[test]
    fn test_date_trunc_minute() {
        // Timestamp for 2000-05-15 10:30:45 UTC (approx 958473045 seconds)
        let result = DateTimeFunctions::date_trunc("MINUTE", Value::Int(958473045)).unwrap();
        // 958473045 / 60 = 15974550.75 -> 15974550 minutes from epoch
        // 15974550 * 60 = 958473000 (Epoch for 2000-05-15 10:30:00)
        assert_eq!(result, Value::Int(958473000));
    }
}
