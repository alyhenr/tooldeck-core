use arrow::array::{BooleanArray, StringArray};
use arrow::compute::filter_record_batch;
use arrow::record_batch::RecordBatch;
use arrow_json::{reader::infer_json_schema, ReaderBuilder, writer::LineDelimitedWriter};
use std::io::Cursor;
use std::sync::Arc;
use tooldeck_registry::{
    ExecutionContext, ToolHandler, ToolRegistry, ToolSpec,
    port, string_param,
};

// ============================================================
// PURE ARROW FUNCTIONS — the core logic, reusable
// ============================================================

pub fn json_to_arrow(ndjson_str: &str) -> Result<RecordBatch, String> {
    let mut cursor = Cursor::new(ndjson_str.as_bytes());
    let (schema, _) = infer_json_schema(&mut cursor, None)
        .map_err(|e| format!("Failed to infer Arrow schema: {e}"))?;
    cursor.set_position(0);
    let mut reader = ReaderBuilder::new(Arc::new(schema))
        .build(cursor)
        .map_err(|e| format!("Failed to build Arrow JSON reader: {e}"))?;
    match reader.next() {
        Some(Ok(batch)) => Ok(batch),
        Some(Err(e)) => Err(format!("Failed to read RecordBatch: {e}")),
        None => Err("JSON file was empty.".to_string()),
    }
}

pub fn filter_by_string_col(
    batch: &RecordBatch,
    col_name: &str,
    target_value: &str,
) -> Result<RecordBatch, String> {
    let col_idx = batch.schema().index_of(col_name)
        .map_err(|_| format!("Column '{col_name}' not found in schema"))?;
    let col = batch.column(col_idx);
    let string_col = col.as_any().downcast_ref::<StringArray>()
        .ok_or_else(|| format!("Column '{col_name}' is not a string type"))?;
    let mask: BooleanArray = string_col.iter().map(|val| val == Some(target_value)).collect();
    filter_record_batch(batch, &mask)
        .map_err(|e| format!("Failed to apply compute filter: {e}"))
}

pub fn arrow_to_json(batch: &RecordBatch) -> Result<String, String> {
    let mut buf = Vec::new();
    {
        let mut writer = LineDelimitedWriter::new(&mut buf);
        writer.write(batch).map_err(|e| format!("Write error: {e}"))?;
        writer.finish().map_err(|e| format!("Finish error: {e}"))?;
    }
    String::from_utf8(buf).map_err(|e| format!("UTF8 conversion error: {e}"))
}

// ============================================================
// TOOL HANDLERS — implementing the ToolHandler trait
// ============================================================

pub struct JsonIngest;

impl ToolHandler for JsonIngest {
    fn spec(&self) -> ToolSpec {
        ToolSpec {
            id: "json_ingest".into(),
            label: "JSON Ingest".into(),
            description: "Parse JSON or NDJSON into a table".into(),
            category: "ingest".into(),
            icon: "FileJson".into(),
            inputs: vec![port("raw", "Text")],
            outputs: vec![port("data", "Text")],
            params: vec![],
        }
    }

    fn execute(&self, ctx: &mut ExecutionContext) -> Result<(), String> {
        let text = ctx.input_text("raw")?;
        let batch = json_to_arrow(&text)?;
        ctx.set_output_arrow("data", batch);
        Ok(())
    }
}

pub struct FilterByStringCol;

impl ToolHandler for FilterByStringCol {
    fn spec(&self) -> ToolSpec {
        ToolSpec {
            id: "filter_by_string_col".into(),
            label: "Filter Rows".into(),
            description: "Keep rows where a column matches a value".into(),
            category: "transform".into(),
            icon: "Filter".into(),
            inputs: vec![port("data", "Text")],
            outputs: vec![port("filtered", "Text")],
            params: vec![
                string_param("column", "Column"),
                string_param("value", "Value"),
            ],
        }
    }

    fn execute(&self, ctx: &mut ExecutionContext) -> Result<(), String> {
        let batch = ctx.input_arrow("data")?;
        let column = ctx.param_str("column")?;
        let value = ctx.param_str("value")?;
        let filtered = filter_by_string_col(&batch, column, value)?;
        ctx.set_output_arrow("filtered", filtered);
        Ok(())
    }
}

pub struct JsonExport;

impl ToolHandler for JsonExport {
    fn spec(&self) -> ToolSpec {
        ToolSpec {
            id: "json_export".into(),
            label: "JSON Export".into(),
            description: "Convert table to JSON or NDJSON".into(),
            category: "export".into(),
            icon: "FileOutput".into(),
            inputs: vec![port("data", "Text")],
            outputs: vec![port("text", "Text")],
            params: vec![],
        }
    }

    fn execute(&self, ctx: &mut ExecutionContext) -> Result<(), String> {
        let batch = ctx.input_arrow("data")?;
        let text = arrow_to_json(&batch)?;
        ctx.set_output_text("text", text);
        Ok(())
    }
}

// ============================================================
// REGISTRATION — one function to register all JSON tools
// ============================================================

pub fn register(registry: &mut ToolRegistry) {
    registry.register(Box::new(JsonIngest));
    registry.register(Box::new(FilterByStringCol));
    registry.register(Box::new(JsonExport));
}

// ============================================================
// TESTS
// ============================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_json_to_arrow_ingest() {
        let raw_json = r#"{"id": 1, "name": "Alice", "active": true}
{"id": 2, "name": "Bob", "active": false}"#;

        let batch = json_to_arrow(raw_json).expect("Failed to ingest JSON");

        assert_eq!(batch.num_columns(), 3, "Should have 3 columns");
        assert_eq!(batch.num_rows(), 2, "Should have 2 rows");
    }
}
