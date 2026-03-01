/// Represents a query result set
#[derive(Debug, Clone)]
pub struct ResultSet {
    pub columns: Vec<ColumnMetadata>,
    pub rows: Vec<Row>,
}

/// Column metadata for RowDescription message
#[derive(Debug, Clone, PartialEq)]
pub struct ColumnMetadata {
    pub name: String,
    pub table_oid: i32,
    pub column_attr_number: i16,
    pub type_oid: i32,
    pub type_size: i16,
    pub type_modifier: i32,
    pub format_code: i16,
}

/// Single row of data
#[derive(Debug, Clone)]
pub struct Row {
    pub fields: Vec<Option<Vec<u8>>>,
}

impl ResultSet {
    pub fn new(columns: Vec<ColumnMetadata>) -> Self {
        Self {
            columns,
            rows: Vec::new(),
        }
    }

    pub fn add_row(&mut self, row: Row) {
        self.rows.push(row);
    }

    pub fn row_count(&self) -> usize {
        self.rows.len()
    }

    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }
}

impl Row {
    pub fn new(fields: Vec<Option<Vec<u8>>>) -> Self {
        Self { fields }
    }

    pub fn field_count(&self) -> usize {
        self.fields.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_result_set_creation() {
        let columns = vec![ColumnMetadata {
            name: "id".to_string(),
            table_oid: 0,
            column_attr_number: 0,
            type_oid: 23,
            type_size: 4,
            type_modifier: -1,
            format_code: 0,
        }];
        let rs = ResultSet::new(columns);
        assert_eq!(rs.row_count(), 0);
        assert!(rs.is_empty());
    }

    #[test]
    fn test_add_row() {
        let columns = vec![];
        let mut rs = ResultSet::new(columns);

        let row = Row::new(vec![Some(b"test".to_vec())]);
        rs.add_row(row);

        assert_eq!(rs.row_count(), 1);
        assert!(!rs.is_empty());
    }

    #[test]
    fn test_multiple_rows() {
        let columns = vec![];
        let mut rs = ResultSet::new(columns);

        rs.add_row(Row::new(vec![Some(b"1".to_vec())]));
        rs.add_row(Row::new(vec![Some(b"2".to_vec())]));
        rs.add_row(Row::new(vec![Some(b"3".to_vec())]));

        assert_eq!(rs.row_count(), 3);
    }

    #[test]
    fn test_row_field_count() {
        let row = Row::new(vec![Some(b"a".to_vec()), Some(b"b".to_vec()), None]);
        assert_eq!(row.field_count(), 3);
    }
}
