use wasm_bindgen::prelude::*;
use tooldeck_json::{filter_by_key_value_mut, retain_keys_mut};

#[wasm_bindgen]
pub fn run_pipeline(input: &str) -> Result<String, JsValue> {
    // 1. Ingest
    let mut data: serde_json::Value = serde_json::from_str(input)
        .map_err(|e| JsValue::from_str(&format!("Parse error: {}", e)))?;

    // 2. Node 1: Filter
    filter_by_key_value_mut(&mut data, "tier", "pro")
        .map_err(|e| JsValue::from_str(&e))?;

    // 3. Node 2: Strip sensitive keys
    retain_keys_mut(&mut data, &["name", "email"])
        .map_err(|e| JsValue::from_str(&e))?;

    // 4. Export
    serde_json::to_string_pretty(&data)
        .map_err(|e| JsValue::from_str(&format!("Serialize error: {}", e)))
}