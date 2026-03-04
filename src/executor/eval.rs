use super::executor::{ExecutorError, Tuple};
use crate::catalog::{string_functions, Value};
use crate::parser::ast::Expr;

pub struct Eval;

impl Eval {
    pub fn eval_expr(expr: &Expr, tuple: &Tuple) -> Result<Value, ExecutorError> {
        match expr {
            Expr::Column(name) => {
                let bytes = tuple
                    .get(name)
                    .cloned()
                    .ok_or_else(|| ExecutorError::ColumnNotFound(name.clone()))?;
                // This is a big simplification, we are assuming all columns are text
                Ok(Value::Text(String::from_utf8_lossy(&bytes).to_string()))
            }
            Expr::Number(n) => Ok(Value::Int(*n)),
            Expr::String(s) => Ok(Value::Text(s.clone())),
            Expr::FunctionCall { name, args } => {
                let mut evaluated_args = Vec::new();
                for arg in args {
                    evaluated_args.push(Self::eval_expr(arg, tuple)?);
                }
                Self::eval_function(name, evaluated_args)
            }
            _ => Err(ExecutorError::TypeMismatch("Unsupported expression".to_string())),
        }
    }

    fn eval_function(name: &str, args: Vec<Value>) -> Result<Value, ExecutorError> {
        match name.to_uppercase().as_str() {
            "UPPER" => {
                if args.len() != 1 {
                    return Err(ExecutorError::TypeMismatch(
                        "UPPER takes one argument".to_string(),
                    ));
                }
                string_functions::StringFunctions::upper(args[0].clone())
                    .map_err(|e| ExecutorError::TypeMismatch(e))
            }
            "LOWER" => {
                if args.len() != 1 {
                    return Err(ExecutorError::TypeMismatch(
                        "LOWER takes one argument".to_string(),
                    ));
                }
                string_functions::StringFunctions::lower(args[0].clone())
                    .map_err(|e| ExecutorError::TypeMismatch(e))
            }
            "LENGTH" => {
                if args.len() != 1 {
                    return Err(ExecutorError::TypeMismatch(
                        "LENGTH takes one argument".to_string(),
                    ));
                }
                string_functions::StringFunctions::length(args[0].clone())
                    .map_err(|e| ExecutorError::TypeMismatch(e))
            }
            _ => Err(ExecutorError::ColumnNotFound(name.to_string())),
        }
    }
}
