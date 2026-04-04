#!/usr/bin/env bash
set -euo pipefail

# ============================================================
# Tooldeck Engine Sync Script
#
# Builds the WASM engine, validates output, and optionally
# copies artifacts to the frontend project.
#
# Usage:
#   ./scripts/sync-engine.sh              # full build + copy
#   ./scripts/sync-engine.sh --if-stale   # skip if WASM is up-to-date
#
# Environment:
#   TOOLDECK_UI_PATH  — path to the frontend project (optional)
#                       If set, copies .wasm + .js to $TOOLDECK_UI_PATH/public/wasm/
#                       If not set, builds and validates only (no copy)
#
# Load from .env.local if it exists:
#   echo 'TOOLDECK_UI_PATH=../tooldeck-ui' > .env.local
# ============================================================

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
PKG_DIR="$ROOT_DIR/tooldeck-engine/pkg"
WASM_FILE="$PKG_DIR/tooldeck_engine_bg.wasm"
JS_FILE="$PKG_DIR/tooldeck_engine.js"

# Colors
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[0;33m'
CYAN='\033[0;36m'
NC='\033[0m'

step() { echo -e "${CYAN}[sync]${NC} $1"; }
ok()   { echo -e "${GREEN}  OK${NC} $1"; }
fail() { echo -e "${RED}  FAIL${NC} $1"; exit 1; }
warn() { echo -e "${YELLOW}  SKIP${NC} $1"; }

cd "$ROOT_DIR"

# Load .env.local if it exists
if [ -f ".env.local" ]; then
    set -a
    source .env.local
    set +a
fi

# --if-stale: skip if no .rs files are newer than the WASM binary
if [ "${1:-}" = "--if-stale" ] && [ -f "$WASM_FILE" ]; then
    NEWEST_RS=$(find tooldeck-*/src -name '*.rs' -newer "$WASM_FILE" 2>/dev/null | head -1)
    if [ -z "$NEWEST_RS" ]; then
        warn "WASM is up-to-date, skipping build"
        exit 0
    fi
    step "Rust sources changed since last build, rebuilding..."
fi

# 1. Run tests
step "Running cargo test..."
if cargo test --quiet 2>&1; then
    ok "All tests passed"
else
    fail "Tests failed — aborting"
fi

# 2. Run clippy
step "Running cargo clippy..."
if cargo clippy --target wasm32-unknown-unknown -p tooldeck-engine --quiet -- -D warnings 2>&1; then
    ok "No clippy warnings"
else
    fail "Clippy found issues — aborting"
fi

# 3. Build WASM
step "Building WASM with wasm-pack..."
if wasm-pack build tooldeck-engine --target web --release --quiet 2>&1; then
    ok "WASM build complete"
else
    fail "wasm-pack build failed"
fi

# 4. Validate output
step "Validating build artifacts..."
[ -f "$WASM_FILE" ] || fail "$WASM_FILE not found"
[ -f "$JS_FILE" ] || fail "$JS_FILE not found"
[ -s "$WASM_FILE" ] || fail "$WASM_FILE is empty"
[ -s "$JS_FILE" ] || fail "$JS_FILE is empty"
WASM_SIZE=$(wc -c < "$WASM_FILE" | tr -d ' ')
JS_SIZE=$(wc -c < "$JS_FILE" | tr -d ' ')
ok "tooldeck_engine_bg.wasm ($(echo "$WASM_SIZE" | awk '{printf "%.1f KB", $1/1024}'))"
ok "tooldeck_engine.js ($(echo "$JS_SIZE" | awk '{printf "%.1f KB", $1/1024}'))"

# 5. Copy to frontend (if path is set)
if [ -n "${TOOLDECK_UI_PATH:-}" ]; then
    DEST="$TOOLDECK_UI_PATH/public/wasm"
    if [ -d "$TOOLDECK_UI_PATH" ]; then
        step "Copying to $DEST..."
        mkdir -p "$DEST"
        cp "$WASM_FILE" "$DEST/"
        cp "$JS_FILE" "$DEST/"
        ok "Artifacts synced to frontend"
    else
        fail "TOOLDECK_UI_PATH=$TOOLDECK_UI_PATH does not exist"
    fi
else
    warn "TOOLDECK_UI_PATH not set — built and validated, but not copied"
    warn "Set it in .env.local: TOOLDECK_UI_PATH=../tooldeck-ui"
fi

# 6. Summary
echo ""
echo -e "${GREEN}Engine sync complete${NC} $(date '+%H:%M:%S')"
