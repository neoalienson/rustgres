# E2E Testing - Quick Reference

## Setup (One-time)

```bash
# 1. Build RustGres Docker image
docker build -f docker/Dockerfile -t rustgres:latest .

# 2. Validate E2E framework
cd e2e && ./validate.sh
```

## Running Tests

```bash
cd e2e

# Quick smoke test (5 min) - Run this first!
./run_all.sh quick

# Compare with PostgreSQL
./run_all.sh compare

# Load tests (30 min)
./run_all.sh load

# Soak tests (24h+)
./run_all.sh soak

# Full suite (1h)
./run_all.sh full

# Start monitoring stack
./run_all.sh monitor
```

## Test Structure

```
scenarios/     → Real-world workloads (OLTP, crash recovery)
load/          → Performance tests (ramp-up, spike)
soak/          → Stability tests (24h memory leak detection)
comparison/    → RustGres vs PostgreSQL benchmarks
```

## Monitoring

After running `./run_all.sh monitor`:
- **Grafana**: http://localhost:3000 (admin/admin)
- **Prometheus**: http://localhost:9090
- **cAdvisor**: http://localhost:8080

## Test Patterns

### Stateless (fresh start each test)
```rust
let env = TestEnv::new().with_rustgres().start();
```

### Stateful (data persists across restarts)
```rust
let env = TestEnv::new()
    .with_rustgres()
    .with_persistence()
    .start();
```

### Comparison (RustGres vs PostgreSQL)
```rust
let env = TestEnv::new()
    .with_rustgres()
    .with_postgres()
    .start();
```

### Monitoring (track memory/CPU)
```rust
let env = TestEnv::new()
    .with_rustgres()
    .with_monitoring()
    .start();
```

## Adding New Tests

1. Create file in appropriate directory:
   - `scenarios/my_test.rs` for functional tests
   - `load/my_test.rs` for performance tests
   - `soak/my_test.rs` for stability tests
   - `comparison/my_test.rs` for benchmarks

2. Add to `Cargo.toml`:
```toml
[[test]]
name = "my_test"
path = "scenarios/my_test.rs"
```

3. Use the framework:
```rust
use e2e::*;

#[test]
fn test_my_scenario() {
    let env = TestEnv::new().with_rustgres().start();
    let db = env.rustgres();
    // Your test logic...
}
```

## Troubleshooting

```bash
# Check container logs
docker-compose logs rustgres
docker-compose logs postgres

# Clean up everything
docker-compose down -v
docker system prune -f

# Run single test with output
cargo test --package e2e --test scenarios test_oltp_simple_transactions -- --nocapture
```

## CI/CD

### PR Checks (Fast)
```yaml
- run: cd e2e && ./run_all.sh quick
```

### Nightly (Comprehensive)
```yaml
- run: cd e2e && ./run_all.sh full
```

### Weekly (Exhaustive)
```yaml
- run: cd e2e && ./run_all.sh soak
```
