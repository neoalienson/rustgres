use super::{cost::CostModel, error::Result};
use crate::parser::ast::{BinaryOperator, Expr};
use crate::statistics::TableStats;

#[derive(Debug, Clone)]
pub struct IndexInfo {
    pub name: String,
    pub columns: Vec<String>,
    pub unique: bool,
}

impl IndexInfo {
    pub fn new(name: String, columns: Vec<String>) -> Self {
        Self { name, columns, unique: false }
    }

    pub fn with_unique(name: String, columns: Vec<String>, unique: bool) -> Self {
        Self { name, columns, unique }
    }
}

pub struct IndexSelector {
    cost_model: CostModel,
}

impl IndexSelector {
    pub fn new() -> Self {
        Self { cost_model: CostModel::new() }
    }

    pub fn select_index(
        &self,
        stats: &TableStats,
        filter_columns: &[String],
        available_indexes: &[IndexInfo],
        selectivity: f64,
    ) -> Result<Option<String>> {
        let seq_cost = self.cost_model.estimate_seq_scan(stats, selectivity)?;
        let mut best_cost = seq_cost.total;
        let mut best_index = None;

        for index in available_indexes {
            let match_quality = self.index_match_quality(index, filter_columns);
            if match_quality > 0 {
                let mut adjusted_selectivity = selectivity;
                if index.unique && match_quality == index.columns.len() {
                    adjusted_selectivity = (1.0 / stats.row_count as f64).max(0.0001);
                }

                let index_cost =
                    self.cost_model.estimate_index_scan(stats, adjusted_selectivity)?;
                let total_cost = index_cost.total * (1.0 / match_quality as f64);

                if total_cost < best_cost {
                    best_cost = total_cost;
                    best_index = Some(index.name.clone());
                }
            }
        }

        Ok(best_index)
    }

    pub fn select_index_for_predicate(
        &self,
        stats: &TableStats,
        predicate: &Expr,
        available_indexes: &[IndexInfo],
        selectivity: f64,
    ) -> Result<Option<String>> {
        let filter_columns = self.extract_filter_columns(predicate);
        self.select_index(stats, &filter_columns, available_indexes, selectivity)
    }

    fn extract_filter_columns(&self, expr: &Expr) -> Vec<String> {
        match expr {
            Expr::BinaryOp { left, op, right } => {
                let mut cols = Vec::new();
                if matches!(
                    op,
                    BinaryOperator::Equals
                        | BinaryOperator::LessThan
                        | BinaryOperator::GreaterThan
                        | BinaryOperator::LessThanOrEqual
                        | BinaryOperator::GreaterThanOrEqual
                ) {
                    if let Expr::Column(col) = left.as_ref() {
                        cols.push(col.clone());
                    }
                    if let Expr::Column(col) = right.as_ref() {
                        cols.push(col.clone());
                    }
                }
                if matches!(op, BinaryOperator::And) {
                    cols.extend(self.extract_filter_columns(left));
                    cols.extend(self.extract_filter_columns(right));
                }
                cols
            }
            Expr::Column(col) => vec![col.clone()],
            _ => Vec::new(),
        }
    }

    fn index_match_quality(&self, index: &IndexInfo, filter_columns: &[String]) -> usize {
        let mut quality = 0;
        for (i, idx_col) in index.columns.iter().enumerate() {
            if filter_columns.contains(idx_col) {
                quality += index.columns.len() - i;
            }
        }
        quality
    }
}

impl Default for IndexSelector {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::ast::{BinaryOperator, Expr as AstExpr};

    #[test]
    fn test_index_selector_creation() {
        let selector = IndexSelector::new();
        assert!(selector.cost_model.estimate_seq_scan(&TableStats::default(), 1.0).is_ok());
    }

    #[test]
    fn test_select_index_with_matching_index() {
        let selector = IndexSelector::new();
        let stats = TableStats { row_count: 10000, page_count: 100, avg_row_size: 100 };
        let indexes = vec![IndexInfo::new("idx_user_id".to_string(), vec!["user_id".to_string()])];

        let result = selector.select_index(&stats, &["user_id".to_string()], &indexes, 0.01);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Some("idx_user_id".to_string()));
    }

    #[test]
    fn test_select_index_no_matching_index() {
        let selector = IndexSelector::new();
        let stats = TableStats { row_count: 1000, page_count: 10, avg_row_size: 100 };
        let indexes = vec![IndexInfo::new("idx_email".to_string(), vec!["email".to_string()])];

        let result = selector.select_index(&stats, &["user_id".to_string()], &indexes, 0.5);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), None);
    }

    #[test]
    fn test_select_index_seq_scan_cheaper() {
        let selector = IndexSelector::new();
        let stats = TableStats { row_count: 100, page_count: 1, avg_row_size: 100 };
        let indexes = vec![IndexInfo::new("idx_user_id".to_string(), vec!["user_id".to_string()])];

        let result = selector.select_index(&stats, &["user_id".to_string()], &indexes, 0.9);
        assert!(result.is_ok());
    }

    #[test]
    fn test_select_best_index_from_multiple() {
        let selector = IndexSelector::new();
        let stats = TableStats { row_count: 10000, page_count: 100, avg_row_size: 100 };
        let indexes = vec![
            IndexInfo::new("idx_user_id".to_string(), vec!["user_id".to_string()]),
            IndexInfo::new("idx_email".to_string(), vec!["email".to_string()]),
        ];

        let result = selector.select_index(&stats, &["user_id".to_string()], &indexes, 0.01);
        assert!(result.is_ok());
        assert!(result.unwrap().is_some());
    }

    #[test]
    fn test_unique_index_selectivity() {
        let selector = IndexSelector::new();
        let stats = TableStats { row_count: 10000, page_count: 100, avg_row_size: 100 };
        let indexes = vec![IndexInfo::with_unique(
            "idx_user_id".to_string(),
            vec!["user_id".to_string()],
            true,
        )];

        let result = selector.select_index(&stats, &["user_id".to_string()], &indexes, 0.5);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Some("idx_user_id".to_string()));
    }

    #[test]
    fn test_multi_column_index_quality() {
        let selector = IndexSelector::new();
        let index = IndexInfo::new(
            "idx_user".to_string(),
            vec!["user_id".to_string(), "email".to_string()],
        );

        assert_eq!(selector.index_match_quality(&index, &["user_id".to_string()]), 2);
        assert_eq!(selector.index_match_quality(&index, &["email".to_string()]), 1);
        assert_eq!(
            selector.index_match_quality(&index, &["user_id".to_string(), "email".to_string()]),
            3
        );
        assert_eq!(selector.index_match_quality(&index, &["name".to_string()]), 0);
    }

    #[test]
    fn test_extract_filter_columns() {
        let selector = IndexSelector::new();

        let expr = AstExpr::BinaryOp {
            left: Box::new(AstExpr::Column("user_id".to_string())),
            op: BinaryOperator::Equals,
            right: Box::new(AstExpr::Number(1)),
        };
        let cols = selector.extract_filter_columns(&expr);
        assert_eq!(cols, vec!["user_id".to_string()]);

        let expr = AstExpr::BinaryOp {
            left: Box::new(AstExpr::BinaryOp {
                left: Box::new(AstExpr::Column("user_id".to_string())),
                op: BinaryOperator::Equals,
                right: Box::new(AstExpr::Number(1)),
            }),
            op: BinaryOperator::And,
            right: Box::new(AstExpr::BinaryOp {
                left: Box::new(AstExpr::Column("email".to_string())),
                op: BinaryOperator::Equals,
                right: Box::new(AstExpr::String("test@example.com".to_string())),
            }),
        };
        let cols = selector.extract_filter_columns(&expr);
        assert_eq!(cols, vec!["user_id".to_string(), "email".to_string()]);
    }

    #[test]
    fn test_select_index_for_predicate() {
        let selector = IndexSelector::new();
        let stats = TableStats { row_count: 10000, page_count: 100, avg_row_size: 100 };
        let indexes = vec![IndexInfo::new("idx_user_id".to_string(), vec!["user_id".to_string()])];

        let predicate = AstExpr::BinaryOp {
            left: Box::new(AstExpr::Column("user_id".to_string())),
            op: BinaryOperator::Equals,
            right: Box::new(AstExpr::Number(1)),
        };

        let result = selector.select_index_for_predicate(&stats, &predicate, &indexes, 0.01);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Some("idx_user_id".to_string()));
    }
}

#[cfg(test)]
mod edge_tests {
    use super::*;

    #[test]
    fn test_empty_indexes() {
        let selector = IndexSelector::new();
        let stats = TableStats { row_count: 1000, page_count: 10, avg_row_size: 100 };
        let result = selector.select_index(&stats, &["user_id".to_string()], &[], 0.1);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), None);
    }

    #[test]
    fn test_empty_filter_columns() {
        let selector = IndexSelector::new();
        let stats = TableStats { row_count: 1000, page_count: 10, avg_row_size: 100 };
        let indexes = vec![IndexInfo::new("idx_user_id".to_string(), vec!["user_id".to_string()])];
        let result = selector.select_index(&stats, &[], &indexes, 0.1);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), None);
    }

    #[test]
    fn test_very_high_selectivity() {
        let selector = IndexSelector::new();
        let stats = TableStats { row_count: 1000, page_count: 10, avg_row_size: 100 };
        let indexes = vec![IndexInfo::new("idx_user_id".to_string(), vec!["user_id".to_string()])];
        let result = selector.select_index(&stats, &["user_id".to_string()], &indexes, 0.99);
        assert!(result.is_ok());
    }

    #[test]
    fn test_very_low_selectivity() {
        let selector = IndexSelector::new();
        let stats = TableStats { row_count: 10000, page_count: 100, avg_row_size: 100 };
        let indexes = vec![IndexInfo::new("idx_user_id".to_string(), vec!["user_id".to_string()])];
        let result = selector.select_index(&stats, &["user_id".to_string()], &indexes, 0.0001);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Some("idx_user_id".to_string()));
    }

    #[test]
    fn test_single_row_table() {
        let selector = IndexSelector::new();
        let stats = TableStats { row_count: 1, page_count: 1, avg_row_size: 100 };
        let indexes = vec![IndexInfo::new("idx_user_id".to_string(), vec!["user_id".to_string()])];
        let result = selector.select_index(&stats, &["user_id".to_string()], &indexes, 1.0);
        assert!(result.is_ok());
    }

    #[test]
    fn test_large_table() {
        let selector = IndexSelector::new();
        let stats = TableStats { row_count: 1_000_000, page_count: 10_000, avg_row_size: 100 };
        let indexes = vec![IndexInfo::new("idx_user_id".to_string(), vec!["user_id".to_string()])];
        let result = selector.select_index(&stats, &["user_id".to_string()], &indexes, 0.001);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Some("idx_user_id".to_string()));
    }
}
