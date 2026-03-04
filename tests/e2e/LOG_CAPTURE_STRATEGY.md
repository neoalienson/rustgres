# E2E Test Server Log Capture Strategy

## Problem
E2E tests spawn a separate server process (in Docker), making server logs inaccessible to the test process for debugging. This makes it difficult to diagnose failures.

## Solution: Docker Logs Capture

### Implementation

1. **Capture logs on test failure**:
   - After each test, check if it failed
   - If failed, capture server logs using `docker logs <container>`
   - Write logs to test output or file

2. **Enable via environment variable**:
   - `VAULTGRES_CAPTURE_LOGS=1` - capture logs on failure
   - `VAULTGRES_CAPTURE_LOGS=always` - always capture logs
   - `VAULTGRES_LOG_FILE=/tmp/vaultgres.log` - write to specific file

3. **Implementation in lib.rs**:

```rust
impl TestEnv {
    fn capture_server_logs(&self, test_name: &str) -> Result<String, String> {
        let output = Command::new("docker")
            .args(&["logs", &format!("{}-vaultgres-1", self.compose_project)])
            .output()
            .map_err(|e| format!("Failed to capture logs: {}", e))?;
        
        let logs = String::from_utf8_lossy(&output.stdout).to_string();
        
        if let Ok(log_file) = std::env::var("VAULTGRES_LOG_FILE") {
            std::fs::write(&log_file, &logs)
                .map_err(|e| format!("Failed to write logs: {}", e))?;
            eprintln!("[LOGS] Server logs written to: {}", log_file);
        } else {
            eprintln!("[LOGS] Server logs for {}:\n{}", test_name, logs);
        }
        
        Ok(logs)
    }
}
```

4. **Usage in tests**:

```rust
#[test]
fn test_something() {
    let env = TestEnv::new().with_vaultgres().start();
    
    match run_test(&env) {
        Ok(_) => {
            if std::env::var("VAULTGRES_CAPTURE_LOGS").as_deref() == Ok("always") {
                env.capture_server_logs("test_something").ok();
            }
        }
        Err(e) => {
            env.capture_server_logs("test_something").ok();
            panic!("Test failed: {}", e);
        }
    }
}
```

### Usage

```bash
# Capture logs only on failure (default)
cargo test

# Always capture logs
VAULTGRES_CAPTURE_LOGS=always cargo test

# Write logs to file
VAULTGRES_LOG_FILE=/tmp/vaultgres.log cargo test

# Combine with log level
RUST_LOG=debug VAULTGRES_CAPTURE_LOGS=always cargo test
```

### Benefits

- Minimal code changes
- No performance impact when not enabled
- Works with existing Docker setup
- Easy to enable/disable per test run
- Logs available for post-mortem analysis
