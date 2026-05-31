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

# Check if tidb-lightning binary is provided for embedding
LIGHTNING_BIN="${LIGHTNING_BIN:-}"
if [ -n "$LIGHTNING_BIN" ] && [ -f "$LIGHTNING_BIN" ]; then
    echo "=== Embedding tidb-lightning from $LIGHTNING_BIN ==="
    cp "$LIGHTNING_BIN" internal/lightning/tidb-lightning
    TRAP_CMD="echo 'placeholder' > internal/lightning/tidb-lightning"
    trap "$TRAP_CMD" EXIT
else
    echo "=== No LIGHTNING_BIN specified, building without embedded tidb-lightning ==="
    echo "=== To embed: LIGHTNING_BIN=/path/to/tidb-lightning bash build-web.sh ==="
fi

echo "=== Building Go binary ==="
CGO_ENABLED=0 go build -ldflags="-s -w" -o build/pg2tidb .

if [ -n "$LIGHTNING_BIN" ]; then
    echo "placeholder" > internal/lightning/tidb-lightning
fi

echo "=== Done: build/pg2tidb ==="
./build/pg2tidb web --help
