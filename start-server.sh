#!/bin/bash

# RustGres Server Startup Script

echo "🦀 RustGres v0.1.0 - PostgreSQL-compatible RDBMS"
echo "================================================"
echo ""

# Build if needed
if [ ! -f "target/release/rustgres" ]; then
    echo "📦 Building RustGres..."
    cargo build --release
    echo ""
fi

# Start server
echo "🚀 Starting server..."
./target/release/rustgres
