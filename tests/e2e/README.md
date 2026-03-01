# End-to-End (E2E) Tests

E2E tests for RustGres using Docker containers.

## Overview

The E2E tests verify that:
- Docker image builds successfully
- Container starts and runs properly
- Server initializes and listens on the correct port
- Resource usage is minimal
- Logs show proper startup sequence

## Running E2E Tests

### Prerequisites

- Docker installed and running
- Bash shell

### Run Tests

```bash
# From project root
./tests/e2e/docker_test.sh
```

## Test Coverage

### Test 1: Container Status
Verifies the container is running after startup.

### Test 2: Startup Logs
Checks that the server logs show "Ready for connections".

### Test 3: Port Listening
Verifies the PostgreSQL port (5432) is listening.

### Test 4: Log Retrieval
Ensures logs can be retrieved from the container.

### Test 5: Resource Usage
Monitors CPU and memory usage (should be minimal).

## Test Results

```
✓ Container is running
✓ Server started successfully
✓ Port 15432 is listening
✓ Logs retrieved
✓ Resource check passed

Image size: 11.1MB
Memory usage: ~450KB
CPU usage: 0.00%
```

## Manual Testing

### Start Container

```bash
docker run -d \
  --name rustgres-test \
  -p 5432:5432 \
  rustgres:latest
```

### Check Logs

```bash
docker logs rustgres-test
```

### Connect with psql

```bash
psql -h localhost -p 5432 -U postgres -d testdb
```

### Stop Container

```bash
docker stop rustgres-test
docker rm rustgres-test
```

## CI/CD Integration

Add to your CI pipeline:

```yaml
# GitHub Actions example
- name: Run E2E Tests
  run: ./tests/e2e/docker_test.sh
```

## Troubleshooting

### Container Won't Start

```bash
# Check logs
docker logs rustgres-test

# Check if port is in use
lsof -i :5432
```

### Tests Fail

```bash
# Run with verbose output
bash -x ./tests/e2e/docker_test.sh

# Check Docker status
docker ps -a
docker inspect rustgres-test
```

## Future Enhancements

- [ ] SQL query execution tests
- [ ] Connection pooling tests
- [ ] Performance benchmarks
- [ ] Multi-container tests (replication)
- [ ] Backup/restore tests
