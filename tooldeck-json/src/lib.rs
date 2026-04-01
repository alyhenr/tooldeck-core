use serde_json::Value;

/// Node 1: Filters an array of objects IN-PLACE based on a key-value match.
pub fn filter_by_key_value_mut(
    data: &mut Value,
    target_key: &str,
    target_value: &str,
) -> Result<(), String> {
    let array = data.as_array_mut().ok_or("Input must be an array of objects")?;

    array.retain(|item| {
        item.as_object()
            .and_then(|obj| obj.get(target_key))
            .and_then(|val| val.as_str())
            .map_or(false, |v| v == target_value)
    });

    Ok(())
}

/// Node 2: Strips unwanted keys from every object in an array IN-PLACE.
pub fn retain_keys_mut(
    data: &mut Value, 
    keys_to_keep: &[&str]
) -> Result<(), String> {
    let array = data.as_array_mut().ok_or("Input must be an array of objects")?;

    for item in array {
        if let Some(obj) = item.as_object_mut() {
            // retain() keeps only the key-value pairs where the closure returns true
            obj.retain(|key, _| keys_to_keep.contains(&key.as_str()));
        }
    }

    Ok(())
}

// --- UNIT TESTS ---

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_filter_by_key_value_mut() {
        let mut data = json!([
            {"id": 1, "tier": "pro", "name": "Alice"},
            {"id": 2, "tier": "free", "name": "Bob"},
            {"id": 3, "tier": "pro", "name": "Charlie"}
        ]);

        filter_by_key_value_mut(&mut data, "tier", "pro").unwrap();

        let expected = json!([
            {"id": 1, "tier": "pro", "name": "Alice"},
            {"id": 3, "tier": "pro", "name": "Charlie"}
        ]);

        assert_eq!(data, expected);
    }

    #[test]
    fn test_retain_keys_mut() {
        let mut data = json!([
            {"id": "A1", "name": "Alice", "secret": "xyz", "email": "a@a.com"},
            {"id": "B2", "name": "Bob", "secret": "abc", "email": "b@b.com"}
        ]);

        retain_keys_mut(&mut data, &["name", "email"]).unwrap();

        let expected = json!([
            {"name": "Alice", "email": "a@a.com"},
            {"name": "Bob", "email": "b@b.com"}
        ]);

        assert_eq!(data, expected);
    }
}