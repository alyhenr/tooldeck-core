# Tooldeck Core: Tool Development Guidelines

Welcome to the `tooldeck-core` engine. This repository houses high-performance Rust utilities compiled to WebAssembly (WASM).

## 1. The Universal Data Bus (Apache Arrow)
Tooldeck does NOT pass raw strings or `serde_json` objects between tools. We use the **Apache Arrow `RecordBatch`** as our Universal Data Bus.
* **The Edges:** Only specific crates (like `tooldeck-json` or `tooldeck-csv`) contain "Ingest" and "Export" functions. They convert raw text into a `RecordBatch` and vice versa.
* **The Tools:** ALL data transformation tools MUST accept a `&RecordBatch` and return a `Result<RecordBatch, String>`. 

## 2. Immutability and Compute Kernels
Arrow arrays are strictly immutable. We achieve zero-copy performance by relying on Arrow's compute kernels.
* Do not iterate row-by-row unless absolutely necessary.
* Use `arrow::compute` functions (like `filter_record_batch`, `cast`, `concat`) to generate new arrays. 
* Untouched columns will automatically have their underlying memory buffers shared with the new `RecordBatch`, saving massive amounts of RAM.

## 3. Tool Signature Standard
```rust
pub fn my_tool(
    batch: &RecordBatch,
    param1: &str,
) -> Result<RecordBatch, String> { ... }
```

## 4. Error Handling & Testing
Never panic. Return descriptive Result::Err(String) so the Next.js frontend can display the error to the user.
Write inline #[cfg(test)] blocks creating mock Arrow arrays to validate tool logic.