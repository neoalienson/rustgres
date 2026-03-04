// Helper macro for registering builtin functions with common patterns
#[macro_export]
macro_rules! register_builtin {
    ($registry:expr, $name:expr, $params:expr, $return_type:expr, $volatility:expr) => {
        $registry
            .register($crate::catalog::Function {
                name: $name.to_string(),
                parameters: $params,
                return_type: $return_type.to_string(),
                language: $crate::catalog::FunctionLanguage::Sql,
                body: format!("BUILTIN:{}", $name),
                is_variadic: false,
                cost: 100.0,
                rows: 1,
                volatility: $volatility,
            })
            .unwrap();
    };
}

// Helper to create a parameter
#[macro_export]
macro_rules! param {
    ($name:expr, $type:expr) => {
        $crate::catalog::Parameter {
            name: $name.to_string(),
            data_type: $type.to_string(),
            default: None,
        }
    };
}
