# Tooldeck Core: Tool Development Guidelines

Welcome to the `tooldeck-core` engine. This repository houses the high-performance Rust utilities that are compiled to WebAssembly (WASM) for the Tooldeck web application (in-progress).

To maintain blazing-fast execution times and zero-copy memory efficiency, all new tools MUST adhere to the following architectural standards.

## 1. The Separation of Concerns
* **The Tools (`tooldeck-json`, `tooldeck-csv`, etc.):** These crates contain PURE Rust logic. They know nothing about WebAssembly, the browser, or JavaScript. Do not import `wasm-bindgen` here.
* **The Engine (`tooldeck-engine`):** This is the ONLY crate that compiles to WASM. It imports the pure tools and handles the parsing of raw JS strings into Rust memory.

## 2. The Zero-Copy Standard (CRITICAL)
Tools process data within a Directed Acyclic Graph (DAG) pipeline. To prevent memory spikes when processing 500MB+ files, tools must **never** clone the entire dataset or return new memory allocations.

* **Accept Mutable References:** Tools must accept `&mut serde_json::Value` (for json tools, for example) (or equivalent structs for other tools).
* **Mutate In-Place:** Use methods like `.retain()`, `.take()`, or swap values directly in memory.
* **Return Signatures:** Tools must return `Result<(), String>`. They mutate the data lock and return success or a descriptive error string.

**Bad (Allocates new memory):**
```rust
pub fn bad_tool(data: Value) -> Result<Value, String> { ... }
pub fn good_tool(data: &mut Value) -> Result<(), String> { ... }
```

## 3. Error Handling
Never use .unwrap() or .expect() inside a tool unless in a test block. If a tool encounters an unexpected data structure (e.g., expecting an array but receiving an object), it must return a clear Err(String). This string will be bubbled up and displayed to the user in the React UI.

## 4. Unit Testing
Every single tool MUST have an inline #[cfg(test)] block.
Use the serde_json::json! macro to create mock input data and expected output data.
Pass the mutable reference to your tool, and use assert_eq! to verify the final memory state matches your expected output.

## 5. Adding a Tool to the Pipeline
Once a tool is written and tested in its respective crate (e.g., tooldeck-json):
Export it in the crate's lib.rs.
Import it into tooldeck-engine/src/lib.rs.
Integrate it into the run_pipeline DAG execution flow, handling its Result.