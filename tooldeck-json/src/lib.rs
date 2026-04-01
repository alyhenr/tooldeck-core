use serde_json::Value;

/// Filters a JSON array of objects by a specific key-value pair.
/// Returns a pretty-printed JSON string of the filtered array.
pub fn filter_by_key_value(
    input_json: &str,
    target_key: &str,
    target_value: &str,
) -> Result<String, String> {
    
    // 1. Parse the raw string into a serde_json::Value
    let parsed_data: Value = serde_json::from_str(input_json)
        .map_err(|e| format!("Failed to parse JSON: {}", e))?;

    // 2. Ensure the root structure is actually an array
    let array = parsed_data
        .as_array()
        .ok_or_else(|| "Input JSON must be an array of objects".to_string())?;

    // 3. Filter the array
    let filtered_array: Vec<Value> = array
        .iter()
        .filter(|item| {
            // Ensure the item is an object
            if let Some(obj) = item.as_object() {
                // Check if the key exists
                if let Some(val) = obj.get(target_key) {
                    // Check if the value matches (handling it as a string for this MVP)
                    if let Some(val_str) = val.as_str() {
                        return val_str == target_value;
                    }
                }
            }
            false
        })
        .cloned() // Clone the matched items into the new Vec
        .collect();

    // 4. Serialize the filtered Vec back into a pretty JSON string
    serde_json::to_string_pretty(&Value::Array(filtered_array))
        .map_err(|e| format!("Failed to serialize result: {}", e))
}

// --- UNIT TESTS ---

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_filter_active_users() {
        // Hardcoded mock JSON
        let mock_input = r#"
        [
            {"id": 1, "name": "Alice", "status": "active", "role": "admin"},
            {"id": 2, "name": "Bob", "status": "inactive", "role": "user"},
            {"id": 3, "name": "Charlie", "status": "active", "role": "user"},
            {"id": 4, "name": "Dave", "status": "pending", "role": "user"}
        ]
        "#;

        // Run the engine
        let result = filter_by_key_value(mock_input, "status", "active").unwrap();

        // The expected output (parsed to Value for safe comparison ignoring whitespace)
        let expected_json = r#"
        [
            {"id": 1, "name": "Alice", "status": "active", "role": "admin"},
            {"id": 3, "name": "Charlie", "status": "active", "role": "user"}
        ]
        "#;
        
        let actual_val: Value = serde_json::from_str(&result).unwrap();
        let expected_val: Value = serde_json::from_str(expected_json).unwrap();

        assert_eq!(actual_val, expected_val, "The filtered JSON did not match expected output.");
    }

    #[test]
    fn test_invalid_json_returns_error() {
        let bad_input = r#"[ {"id": 1, "name": "missing quotes } ]"#;
        let result = filter_by_key_value(bad_input, "status", "active");
        assert!(result.is_err());
    }
}