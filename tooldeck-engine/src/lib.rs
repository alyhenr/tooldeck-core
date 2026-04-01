use wasm_bindgen::prelude::*;
use tooldeck_json::filter_by_key_value;

// This macro exposes the function to JavaScript
#[wasm_bindgen]
pub fn run_json_filter(input: &str, key: &str, value: &str) -> Result<String, JsValue> {
    // Call our pure Rust logic
    match filter_by_key_value(input, key, value) {
        Ok(result) => Ok(result),
        Err(e) => Err(JsValue::from_str(&e)), // Convert Rust strings to JS Errors
    }
}