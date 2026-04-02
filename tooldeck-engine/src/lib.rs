use wasm_bindgen::prelude::*;
use tooldeck_json::{json_to_arrow, filter_by_string_col, arrow_to_json};

#[wasm_bindgen]
pub fn run_pipeline(input_ndjson: &str) -> Result<String, JsValue> {
    
    // 1. Ingest (The Edge)
    let batch = json_to_arrow(input_ndjson)
        .map_err(|e| JsValue::from_str(&e))?;

    // 2. Process (The Tool) - Filter for tier == "pro"
    let filtered_batch = filter_by_string_col(&batch, "tier", "pro")
        .map_err(|e| JsValue::from_str(&e))?;

    // 3. Export (The Edge)
    arrow_to_json(&filtered_batch)
        .map_err(|e| JsValue::from_str(&e))
}