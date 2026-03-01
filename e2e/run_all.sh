#!/bin/bash
set -e

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

echo -e "${GREEN}RustGres E2E Test Suite${NC}"
echo "================================"

cd "$(dirname "$0")"

cleanup() {
    echo -e "${YELLOW}Cleaning up...${NC}"
    docker compose down -v 2>/dev/null || true
}
trap cleanup EXIT

MODE=${1:-quick}

case $MODE in
    quick)
        echo -e "${GREEN}Running quick tests (smoke tests)${NC}"
        cargo test --package e2e --test smoke -- --test-threads=1 --nocapture
        ;;
    
    full)
        echo -e "${GREEN}Running full test suite${NC}"
        cargo test --package e2e --test scenarios -- --nocapture
        cargo test --package e2e --test comparison -- --nocapture
        cargo test --package e2e --test load -- --ignored --test-threads=1 --nocapture
        ;;
    
    load)
        echo -e "${GREEN}Running load tests${NC}"
        cargo test --package e2e --test load -- --ignored --test-threads=1 --nocapture
        ;;
    
    soak)
        echo -e "${GREEN}Running soak tests (long duration)${NC}"
        cargo test --package e2e --test soak -- --ignored --test-threads=1 --nocapture
        ;;
    
    compare)
        echo -e "${GREEN}Running comparison benchmarks${NC}"
        docker compose up -d rustgres postgres
        sleep 10
        cargo test --package e2e --test comparison -- --nocapture
        ;;
    
    monitor)
        echo -e "${GREEN}Starting monitoring stack${NC}"
        docker compose up -d
        echo -e "${YELLOW}Grafana: http://localhost:3000 (admin/admin)${NC}"
        echo -e "${YELLOW}Prometheus: http://localhost:9090${NC}"
        echo -e "${YELLOW}cAdvisor: http://localhost:8080${NC}"
        echo -e "${GREEN}Press Ctrl+C to stop${NC}"
        docker compose logs -f
        ;;
    
    *)
        echo -e "${RED}Unknown mode: $MODE${NC}"
        echo "Usage: $0 [quick|full|load|soak|compare|monitor]"
        exit 1
        ;;
esac

echo -e "${GREEN}Tests completed!${NC}"
