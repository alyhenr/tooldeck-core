# Tooldeck Core Architecture

## Crate Structure

```
tooldeck-core/
├── tooldeck-registry/     The foundation: traits, types, execution context
├── tooldeck-json/         JSON tool implementations
├── tooldeck-engine/       WASM entry point + pipeline orchestrator
└── scripts/
    └── sync-engine.sh     Build, validate, and sync WASM to frontend
```

**Dependency graph:**
```
tooldeck-registry  (depends on: arrow, serde, serde_json)
       ↑
tooldeck-json      (depends on: tooldeck-registry, arrow, arrow-json)
       ↑
tooldeck-engine    (depends on: tooldeck-registry, tooldeck-json, wasm-bindgen)
```

Tool crates depend on the registry. The engine depends on all tool crates. No circular dependencies.

## Adding a New Tool

### Step 1: Create the tool crate

```bash
cargo new tooldeck-csv --lib
```

Add to workspace `Cargo.toml`:
```toml
members = ["tooldeck-registry", "tooldeck-json", "tooldeck-csv", "tooldeck-engine"]
```

Add dependencies to `tooldeck-csv/Cargo.toml`:
```toml
[dependencies]
tooldeck-registry = { path = "../tooldeck-registry" }
arrow = { version = "58.1", default-features = false }
```

### Step 2: Implement the ToolHandler trait

```rust
// tooldeck-csv/src/lib.rs
use tooldeck_registry::{
    ExecutionContext, ToolHandler, ToolRegistry, ToolSpec,
    port, string_param, bool_param,
};

pub struct CsvIngest;

impl ToolHandler for CsvIngest {
    fn spec(&self) -> ToolSpec {
        ToolSpec {
            id: "csv_ingest".into(),
            label: "CSV Ingest".into(),
            description: "Parse CSV text into a table".into(),
            category: "ingest".into(),
            icon: "FileSpreadsheet".into(),
            inputs: vec![port("raw", "Text")],
            outputs: vec![port("data", "Text")],
            params: vec![
                string_param("delimiter", "Delimiter"),
                bool_param("has_header", "Has Header Row"),
            ],
        }
    }

    fn execute(&self, ctx: &mut ExecutionContext) -> Result<(), String> {
        let text = ctx.input_text("raw")?;
        let delimiter = ctx.param_str("delimiter").unwrap_or(",");
        // ... parse CSV to Arrow RecordBatch ...
        // ctx.set_output_arrow("data", batch);
        Ok(())
    }
}

pub fn register(registry: &mut ToolRegistry) {
    registry.register(Box::new(CsvIngest));
}
```

### Step 3: Register in the engine

In `tooldeck-engine/Cargo.toml`, add:
```toml
tooldeck-csv = { path = "../tooldeck-csv" }
```

In `tooldeck-engine/src/lib.rs`, add one line:
```rust
fn build_registry() -> ToolRegistry {
    let mut registry = ToolRegistry::new();
    tooldeck_json::register(&mut registry);
    tooldeck_csv::register(&mut registry);  // ← this line
    registry
}
```

### Step 4: Build and sync

```bash
./scripts/sync-engine.sh
```

The frontend discovers the new tool automatically. **The only frontend change needed is for new icons:** if your tool uses a Lucide icon that isn't already in the frontend's icon registry, add one entry to `tooldeck-ui/src/lib/icon-map.ts` (one import + one map entry). All existing icons work out of the box.

## Key Abstractions

### DataPayload

The internal data bus. Tools produce and consume data as either `Text` (strings) or `Arrow` (RecordBatch). The `ExecutionContext` auto-converts between them:

- `ctx.input_arrow("port")` — Returns Arrow. If upstream produced Text, auto-parses as NDJSON.
- `ctx.input_text("port")` — Returns Text. If upstream produced Arrow, auto-serializes to NDJSON.
- `ctx.set_output_arrow("port", batch)` — Store Arrow output (zero-copy to next tool if it also uses Arrow).
- `ctx.set_output_text("port", text)` — Store Text output.

### Port Types (Frontend)

Only two: `Text` and `Bytes`. The frontend validates connections using `can_connect_to` rules. `Text` can connect to both `Text` and `Bytes` inputs. `Bytes` can only connect to `Bytes`.

Arrow RecordBatch is an internal engine optimization — invisible to the frontend.

### Progress Reporting

The engine reports progress via a callback function passed from JavaScript:

```
node_started  → { "event": "node_started", "node_id": "..." }
node_completed → { "event": "node_completed", "node_id": "...", "result": { ... } }
```

The frontend updates per-node status in real-time as each node executes.

## Build Script

```bash
# Full build: test → clippy → wasm-pack → validate → copy to frontend
./scripts/sync-engine.sh

# Only rebuild if Rust sources changed
./scripts/sync-engine.sh --if-stale
```

Set `TOOLDECK_UI_PATH` in `.env.local` to auto-copy WASM artifacts to the frontend:
```
TOOLDECK_UI_PATH=../tooldeck-ui
```

## Tool Development Guidelines

1. **Never panic.** Return `Result::Err(String)` with a descriptive message.
2. **Use Arrow compute kernels** for tabular operations. Never iterate row-by-row.
3. **Accept input via `ExecutionContext`** — don't parse raw strings directly. The context handles format conversion.
4. **Write tests** with inline `#[cfg(test)]` blocks using mock Arrow data.
5. **Register all tools** in a single `pub fn register()` function in your crate root.
