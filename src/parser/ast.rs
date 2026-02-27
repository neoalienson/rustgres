/// SQL Statement
#[derive(Debug, Clone, PartialEq)]
pub enum Statement {
    Select(SelectStmt),
    Insert(InsertStmt),
    Update(UpdateStmt),
    Delete(DeleteStmt),
}

/// SELECT statement
#[derive(Debug, Clone, PartialEq)]
pub struct SelectStmt {
    pub columns: Vec<Expr>,
    pub from: String,
    pub where_clause: Option<Expr>,
}

/// INSERT statement
#[derive(Debug, Clone, PartialEq)]
pub struct InsertStmt {
    pub table: String,
    pub values: Vec<Expr>,
}

/// UPDATE statement
#[derive(Debug, Clone, PartialEq)]
pub struct UpdateStmt {
    pub table: String,
    pub assignments: Vec<(String, Expr)>,
    pub where_clause: Option<Expr>,
}

/// DELETE statement
#[derive(Debug, Clone, PartialEq)]
pub struct DeleteStmt {
    pub table: String,
    pub where_clause: Option<Expr>,
}

/// Expression
#[derive(Debug, Clone, PartialEq)]
pub enum Expr {
    Column(String),
    Number(i64),
    String(String),
    Star,
    BinaryOp {
        left: Box<Expr>,
        op: BinaryOperator,
        right: Box<Expr>,
    },
}

/// Binary operator
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum BinaryOperator {
    Equals,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_select_stmt_creation() {
        let stmt = SelectStmt {
            columns: vec![Expr::Star],
            from: "users".to_string(),
            where_clause: None,
        };
        
        assert_eq!(stmt.from, "users");
        assert_eq!(stmt.columns.len(), 1);
    }
    
    #[test]
    fn test_insert_stmt_creation() {
        let stmt = InsertStmt {
            table: "users".to_string(),
            values: vec![Expr::Number(1), Expr::String("Alice".to_string())],
        };
        
        assert_eq!(stmt.table, "users");
        assert_eq!(stmt.values.len(), 2);
    }
}
