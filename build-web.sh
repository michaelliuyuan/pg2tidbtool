#!/bin/bash
set -e

echo "=== Building frontend ==="
cd web/frontend
npm ci
npm run build
cd ../..

echo "=== Copying frontend to cmd/static ==="
rm -rf cmd/static/assets cmd/static/favicon.svg cmd/static/icons.svg
cp -r web/dist/* cmd/static/

echo "=== Building Go binary ==="
CGO_ENABLED=0 go build -ldflags="-s -w" -o build/pg2tidb .

echo "=== Done: build/pg2tidb ==="
./build/pg2tidb web --help
