#[cfg(test)]
mod tests {
    use crate::catalog::value::Value;
    use crate::parser::ast::DataType;

    #[test]
    fn test_boolean_value() {
        let v1 = Value::Bool(true);
        let v2 = Value::Bool(false);
        assert_ne!(v1, v2);
        assert!(v1 > v2);
    }

    #[test]
    fn test_date_value() {
        let v1 = Value::Date(18000);
        let v2 = Value::Date(18001);
        assert!(v1 < v2);
    }

    #[test]
    fn test_time_value() {
        let v1 = Value::Time(3600_000_000);
        let v2 = Value::Time(7200_000_000);
        assert!(v1 < v2);
    }

    #[test]
    fn test_timestamp_value() {
        let v1 = Value::Timestamp(1609459200_000_000);
        let v2 = Value::Timestamp(1609545600_000_000);
        assert!(v1 < v2);
    }

    #[test]
    fn test_decimal_value() {
        let v1 = Value::Decimal(12345, 2);
        let v2 = Value::Decimal(12346, 2);
        assert!(v1 < v2);
    }

    #[test]
    fn test_bytea_value() {
        let v1 = Value::Bytea(vec![1, 2, 3]);
        let v2 = Value::Bytea(vec![1, 2, 4]);
        assert!(v1 < v2);
    }

    #[test]
    fn test_datatype_boolean() {
        let dt = DataType::Boolean;
        assert_eq!(dt, DataType::Boolean);
    }

    #[test]
    fn test_datatype_date() {
        let dt = DataType::Date;
        assert_eq!(dt, DataType::Date);
    }

    #[test]
    fn test_datatype_time() {
        let dt = DataType::Time;
        assert_eq!(dt, DataType::Time);
    }

    #[test]
    fn test_datatype_timestamp() {
        let dt = DataType::Timestamp;
        assert_eq!(dt, DataType::Timestamp);
    }

    #[test]
    fn test_datatype_decimal() {
        let dt = DataType::Decimal(10, 2);
        assert_eq!(dt, DataType::Decimal(10, 2));
    }

    #[test]
    fn test_datatype_bytea() {
        let dt = DataType::Bytea;
        assert_eq!(dt, DataType::Bytea);
    }
}
