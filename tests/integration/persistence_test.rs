use tempfile::TempDir;
use vaultgres::catalog::Catalog;
use vaultgres::parser::{Parser, Statement};

fn with_temp_catalog<F>(test: F)
where
    F: FnOnce(&str, &Catalog) + FnOnce(&str, &Catalog),
{
    let temp_dir = TempDir::new().unwrap();
    let data_dir = temp_dir.path().to_str().unwrap();
    {
        let catalog = Catalog::new_with_data_dir(data_dir);
        test(data_dir, &catalog);
    }
}

fn execute_sql(catalog: &Catalog, sql: &str) {
    let mut parser = Parser::new(sql).unwrap();
    let stmt = parser.parse().unwrap();
    match stmt {
        Statement::CreateView(create) => {
            catalog.create_view(create.name.clone(), *create.query).unwrap();
        }
        Statement::CreateTrigger(create) => {
            catalog.create_trigger(create).unwrap();
        }
        _ => panic!("Unexpected statement type"),
    }
}

#[test]
fn test_view_persistence() {
    let temp_dir = TempDir::new().unwrap();
    let data_dir = temp_dir.path().to_str().unwrap();

    {
        let catalog = Catalog::new_with_data_dir(data_dir);
        execute_sql(&catalog, "CREATE VIEW v AS SELECT * FROM t");
    }

    {
        let catalog = Catalog::new_with_data_dir(data_dir);
        assert!(catalog.get_view("v").is_some());
    }
}

#[test]
fn test_trigger_persistence() {
    let temp_dir = TempDir::new().unwrap();
    let data_dir = temp_dir.path().to_str().unwrap();

    {
        let catalog = Catalog::new_with_data_dir(data_dir);
        execute_sql(&catalog, "CREATE TRIGGER t BEFORE INSERT ON users FOR EACH ROW BEGIN END");
    }

    {
        let catalog = Catalog::new_with_data_dir(data_dir);
        assert!(catalog.get_trigger("t").is_some());
    }
}

#[test]
fn test_multiple_views_persistence() {
    let temp_dir = TempDir::new().unwrap();
    let data_dir = temp_dir.path().to_str().unwrap();

    {
        let catalog = Catalog::new_with_data_dir(data_dir);
        for i in 0..5 {
            execute_sql(&catalog, &format!("CREATE VIEW v{} AS SELECT * FROM t{}", i, i));
        }
    }

    {
        let catalog = Catalog::new_with_data_dir(data_dir);
        for i in 0..5 {
            assert!(catalog.get_view(&format!("v{}", i)).is_some());
        }
    }
}

#[test]
fn test_multiple_triggers_persistence() {
    let temp_dir = TempDir::new().unwrap();
    let data_dir = temp_dir.path().to_str().unwrap();

    {
        let catalog = Catalog::new_with_data_dir(data_dir);
        for i in 0..5 {
            execute_sql(
                &catalog,
                &format!("CREATE TRIGGER t{} BEFORE INSERT ON users FOR EACH ROW BEGIN END", i),
            );
        }
    }

    {
        let catalog = Catalog::new_with_data_dir(data_dir);
        for i in 0..5 {
            assert!(catalog.get_trigger(&format!("t{}", i)).is_some());
        }
    }
}

#[test]
fn test_view_drop_persistence() {
    let temp_dir = TempDir::new().unwrap();
    let data_dir = temp_dir.path().to_str().unwrap();

    {
        let catalog = Catalog::new_with_data_dir(data_dir);
        execute_sql(&catalog, "CREATE VIEW v AS SELECT * FROM t");
        catalog.drop_view("v", false).unwrap();
    }

    {
        let catalog = Catalog::new_with_data_dir(data_dir);
        assert!(catalog.get_view("v").is_none());
    }
}

#[test]
fn test_trigger_drop_persistence() {
    let temp_dir = TempDir::new().unwrap();
    let data_dir = temp_dir.path().to_str().unwrap();

    {
        let catalog = Catalog::new_with_data_dir(data_dir);
        execute_sql(&catalog, "CREATE TRIGGER t BEFORE INSERT ON users FOR EACH ROW BEGIN END");
        catalog.drop_trigger("t", false).unwrap();
    }

    {
        let catalog = Catalog::new_with_data_dir(data_dir);
        assert!(catalog.get_trigger("t").is_none());
    }
}
