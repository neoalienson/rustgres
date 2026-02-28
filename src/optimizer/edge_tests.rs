//! Edge case tests for query optimizer

#[cfg(test)]
mod tests {
    use crate::optimizer::*;
    use crate::statistics::TableStats;

    #[test]
    fn test_zero_cost() {
        let cost = Cost::zero();
        assert_eq!(cost.startup, 0.0);
        assert_eq!(cost.total, 0.0);
        assert_eq!(cost.rows, 0.0);
    }

    #[test]
    fn test_seq_scan_empty_table() {
        let model = CostModel::new();
        let stats = TableStats {
            row_count: 0,
            page_count: 0,
            avg_row_size: 0,
        };
        let cost = model.estimate_seq_scan(&stats, 1.0).unwrap();
        assert_eq!(cost.rows, 0.0);
    }

    #[test]
    fn test_seq_scan_zero_selectivity() {
        let model = CostModel::new();
        let stats = TableStats {
            row_count: 1000,
            page_count: 10,
            avg_row_size: 100,
        };
        let cost = model.estimate_seq_scan(&stats, 0.0).unwrap();
        assert_eq!(cost.rows, 0.0);
    }

    #[test]
    fn test_index_scan_zero_selectivity() {
        let model = CostModel::new();
        let stats = TableStats {
            row_count: 1000,
            page_count: 10,
            avg_row_size: 100,
        };
        let cost = model.estimate_index_scan(&stats, 0.0).unwrap();
        assert!(cost.total > 0.0); // Still has minimum page cost
    }

    #[test]
    fn test_index_scan_full_selectivity() {
        let model = CostModel::new();
        let stats = TableStats {
            row_count: 1000,
            page_count: 10,
            avg_row_size: 100,
        };
        let cost = model.estimate_index_scan(&stats, 1.0).unwrap();
        assert_eq!(cost.rows, 1000.0);
    }

    #[test]
    fn test_nested_loop_join_empty_left() {
        let model = CostModel::new();
        let left = Cost::new(0.0, 10.0, 0.0);
        let right = Cost::new(0.0, 5.0, 100.0);
        let cost = model.estimate_nested_loop_join(&left, &right).unwrap();
        assert_eq!(cost.rows, 0.0);
    }

    #[test]
    fn test_nested_loop_join_empty_right() {
        let model = CostModel::new();
        let left = Cost::new(0.0, 10.0, 100.0);
        let right = Cost::new(0.0, 5.0, 0.0);
        let cost = model.estimate_nested_loop_join(&left, &right).unwrap();
        assert_eq!(cost.rows, 0.0);
    }

    #[test]
    fn test_hash_join_empty_tables() {
        let model = CostModel::new();
        let left = Cost::zero();
        let right = Cost::zero();
        let cost = model.estimate_hash_join(&left, &right).unwrap();
        assert_eq!(cost.rows, 0.0);
    }

    #[test]
    fn test_nested_loop_large_tables() {
        let model = CostModel::new();
        let left = Cost::new(0.0, 100.0, 10000.0);
        let right = Cost::new(0.0, 100.0, 10000.0);
        let cost = model.estimate_nested_loop_join(&left, &right).unwrap();
        assert_eq!(cost.rows, 100_000_000.0);
    }

    #[test]
    fn test_hash_join_asymmetric_tables() {
        let model = CostModel::new();
        let left = Cost::new(0.0, 100.0, 10000.0);
        let right = Cost::new(0.0, 10.0, 100.0);
        let cost = model.estimate_hash_join(&left, &right).unwrap();
        assert!(cost.total > 0.0);
    }

    #[test]
    fn test_seq_scan_single_page() {
        let model = CostModel::new();
        let stats = TableStats {
            row_count: 10,
            page_count: 1,
            avg_row_size: 100,
        };
        let cost = model.estimate_seq_scan(&stats, 1.0).unwrap();
        assert!(cost.total > 0.0);
    }

    #[test]
    fn test_index_scan_single_row() {
        let model = CostModel::new();
        let stats = TableStats {
            row_count: 1,
            page_count: 1,
            avg_row_size: 100,
        };
        let cost = model.estimate_index_scan(&stats, 1.0).unwrap();
        assert_eq!(cost.rows, 1.0);
    }

    #[test]
    fn test_cost_comparison() {
        let cost1 = Cost::new(1.0, 10.0, 100.0);
        let cost2 = Cost::new(2.0, 20.0, 200.0);
        assert!(cost1.total < cost2.total);
        assert!(cost1.rows < cost2.rows);
    }

    #[test]
    fn test_negative_selectivity_handling() {
        let model = CostModel::new();
        let stats = TableStats {
            row_count: 1000,
            page_count: 10,
            avg_row_size: 100,
        };
        // Negative selectivity should still work (treated as 0)
        let cost = model.estimate_seq_scan(&stats, -0.1).unwrap();
        assert!(cost.rows <= 0.0);
    }

    #[test]
    fn test_selectivity_greater_than_one() {
        let model = CostModel::new();
        let stats = TableStats {
            row_count: 1000,
            page_count: 10,
            avg_row_size: 100,
        };
        let cost = model.estimate_seq_scan(&stats, 1.5).unwrap();
        assert_eq!(cost.rows, 1500.0);
    }
}
