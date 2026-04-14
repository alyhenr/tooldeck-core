use arrow::array::BooleanArray;
use arrow::compute::filter_record_batch;
use arrow::record_batch::RecordBatch;
use tooldeck_registry::{
    cell_to_string, ExecutionContext, ToolHandler, ToolRegistry, ToolSpec,
    port_with_format, string_param,
};

// ============================================================
// PURE ARROW FUNCTIONS
// ============================================================

/// Filter rows where a column's value (of any type) matches the target string.
pub fn filter_rows(
    batch: &RecordBatch,
    col_name: &str,
    target_value: &str,
) -> Result<RecordBatch, String> {
    let col_idx = batch.schema().index_of(col_name)
        .map_err(|_| format!("Column '{col_name}' not found in schema"))?;
    let col = batch.column(col_idx);

    // Compare each cell's string representation against the target
    let mask: BooleanArray = (0..batch.num_rows())
        .map(|row| Some(cell_to_string(col.as_ref(), row) == target_value))
        .collect();

    filter_record_batch(batch, &mask)
        .map_err(|e| format!("Failed to apply filter: {e}"))
}

// ============================================================
// TOOL HANDLERS
// ============================================================

pub struct FilterRows;

impl ToolHandler for FilterRows {
    fn spec(&self) -> ToolSpec {
        ToolSpec {
            id: "filter_rows".into(),
            label: "Filter Rows".into(),
            description: "Keep rows where a column matches a value".into(),
            category: "data".into(),
            icon: "Filter".into(),
            inputs: vec![port_with_format("data", "Text", "tabular")],
            outputs: vec![port_with_format("result", "Text", "tabular")],
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
        let filtered = filter_rows(&batch, column, value)?;
        ctx.set_output_arrow("result", filtered);
        Ok(())
    }
}

// ============================================================
// REGISTRATION
// ============================================================

pub fn register(registry: &mut ToolRegistry) {
    registry.register(Box::new(FilterRows));
}

// ============================================================
// TESTS
// ============================================================

#[cfg(test)]
mod tests {
    use super::*;
    use tooldeck_registry::json_to_arrow;

    #[test]
    fn test_filter_by_string() {
        let raw = r#"{"name":"Alice","tier":"pro"}
{"name":"Bob","tier":"free"}
{"name":"Charlie","tier":"pro"}"#;
        let batch = json_to_arrow(raw).unwrap();
        let filtered = filter_rows(&batch, "tier", "pro").unwrap();
        assert_eq!(filtered.num_rows(), 2);
    }

    #[test]
    fn test_filter_by_integer() {
        let raw = r#"{"name":"Alice","age":30}
{"name":"Bob","age":25}
{"name":"Charlie","age":30}"#;
        let batch = json_to_arrow(raw).unwrap();
        let filtered = filter_rows(&batch, "age", "30").unwrap();
        assert_eq!(filtered.num_rows(), 2);
    }

    #[test]
    fn test_filter_by_boolean() {
        let raw = r#"{"name":"Alice","active":true}
{"name":"Bob","active":false}
{"name":"Charlie","active":true}"#;
        let batch = json_to_arrow(raw).unwrap();
        let filtered = filter_rows(&batch, "active", "true").unwrap();
        assert_eq!(filtered.num_rows(), 2);
    }
}
