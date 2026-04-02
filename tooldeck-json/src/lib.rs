use arrow::array::{BooleanArray, StringArray};
use arrow::compute::filter_record_batch;
use arrow::record_batch::RecordBatch;
use arrow_json::{reader::infer_json_schema, ReaderBuilder, writer::LineDelimitedWriter};
use std::io::Cursor;
use std::sync::Arc;

// ==========================================
// 1. THE INGEST EDGE (NDJSON -> Arrow)
// ==========================================
pub fn json_to_arrow(ndjson_str: &str) -> Result<RecordBatch, String> {
    let mut cursor = Cursor::new(ndjson_str.as_bytes());

    let (schema, _) = infer_json_schema(&mut cursor, None)
        .map_err(|e| format!("Failed to infer Arrow schema: {}", e))?;

    cursor.set_position(0);

    let mut reader = ReaderBuilder::new(Arc::new(schema))
        .build(cursor)
        .map_err(|e| format!("Failed to build Arrow JSON reader: {}", e))?;

    match reader.next() {
        Some(Ok(batch)) => Ok(batch),
        Some(Err(e)) => Err(format!("Failed to read RecordBatch: {}", e)),
        None => Err("JSON file was empty.".to_string()),
    }
}

// ==========================================
// 2. THE TOOL (Arrow -> Arrow)
// ==========================================
/// Filters an Arrow RecordBatch where a specific string column matches a value.
pub fn filter_by_string_col(
    batch: &RecordBatch,
    col_name: &str,
    target_value: &str,
) -> Result<RecordBatch, String> {
    
    // 1. Find the column index
    let col_idx = batch.schema().index_of(col_name)
        .map_err(|_| format!("Column '{}' not found in schema", col_name))?;

    // 2. Extract the column and downcast it to a String Array
    let col = batch.column(col_idx);
    let string_col = col.as_any().downcast_ref::<StringArray>()
        .ok_or_else(|| format!("Column '{}' is not a string type", col_name))?;

    // 3. Build a Boolean Mask (True if it matches, False if it doesn't)
    // This is blazing fast because it only scans the contiguous string block.
    let mask: BooleanArray = string_col.iter().map(|val| {
        val == Some(target_value)
    }).collect();

    // 4. Use Arrow's highly optimized compute kernel to apply the mask to the whole table
    filter_record_batch(batch, &mask)
        .map_err(|e| format!("Failed to apply compute filter: {}", e))
}

// ==========================================
// 3. THE EXPORT EDGE (Arrow -> NDJSON)
// ==========================================
pub fn arrow_to_json(batch: &RecordBatch) -> Result<String, String> {
    let mut buf = Vec::new();
    
    // We use a block here to drop the writer so it flushes to the buffer
    {
        let mut writer = LineDelimitedWriter::new(&mut buf);
        writer.write(batch).map_err(|e| format!("Write error: {}", e))?;
        writer.finish().map_err(|e| format!("Finish error: {}", e))?;
    }

    String::from_utf8(buf).map_err(|e| format!("UTF8 conversion error: {}", e))
}

// --- UNIT TESTS ---

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_json_to_arrow_ingest() {
        // We use NDJSON (JSONLines) - the industry standard for streaming big data.
        // No commas between objects, no outer array brackets. Just newlines.
        let raw_json = r#"{"id": 1, "name": "Alice", "active": true}
{"id": 2, "name": "Bob", "active": false}"#;

        let batch = json_to_arrow(raw_json).expect("Failed to ingest JSON");

        assert_eq!(batch.num_columns(), 3, "Should have 3 columns");
        assert_eq!(batch.num_rows(), 2, "Should have 2 rows");

        println!("Inferred Schema:\n{:#?}", batch.schema());
    }
}