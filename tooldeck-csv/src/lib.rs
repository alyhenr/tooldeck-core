use arrow::record_batch::RecordBatch;
use arrow_csv::reader::Format;
use arrow_csv::{ReaderBuilder, WriterBuilder};
use std::io::Cursor;
use tooldeck_registry::{
    ExecutionContext, ToolHandler, ToolRegistry, ToolSpec,
    port, string_param, bool_param,
};

// ============================================================
// PURE ARROW FUNCTIONS
// ============================================================

pub fn csv_to_arrow(text: &str, delimiter: u8, has_header: bool) -> Result<RecordBatch, String> {
    let cursor = Cursor::new(text.as_bytes());
    let format = Format::default()
        .with_delimiter(delimiter)
        .with_header(has_header);

    let (schema, _) = format
        .infer_schema(Cursor::new(text.as_bytes()), Some(100))
        .map_err(|e| format!("Failed to infer CSV schema: {e}"))?;

    let reader = ReaderBuilder::new(std::sync::Arc::new(schema))
        .with_format(format)
        .build(cursor)
        .map_err(|e| format!("Failed to build CSV reader: {e}"))?;

    let batches: Result<Vec<RecordBatch>, _> = reader.collect();
    let batches = batches.map_err(|e| format!("Failed to read CSV: {e}"))?;

    if batches.is_empty() {
        return Err("CSV file was empty".into());
    }

    // Concatenate all batches into one
    if batches.len() == 1 {
        return Ok(batches.into_iter().next().unwrap());
    }

    arrow::compute::concat_batches(&batches[0].schema(), &batches)
        .map_err(|e| format!("Failed to concat CSV batches: {e}"))
}

pub fn arrow_to_csv(batch: &RecordBatch, has_header: bool) -> Result<String, String> {
    let mut buf = Vec::new();
    {
        let mut writer = WriterBuilder::new()
            .with_header(has_header)
            .build(&mut buf);
        writer.write(batch).map_err(|e| format!("CSV write error: {e}"))?;
    }
    String::from_utf8(buf).map_err(|e| format!("UTF8 error: {e}"))
}

// ============================================================
// TOOL HANDLERS
// ============================================================

pub struct CsvIngest;

impl ToolHandler for CsvIngest {
    fn spec(&self) -> ToolSpec {
        ToolSpec {
            id: "csv_ingest".into(),
            label: "CSV Ingest".into(),
            description: "Parse CSV text into a table".into(),
            category: "ingest".into(),
            icon: "FileSpreadsheet".into(),
            inputs: vec![port("raw", "Text")],
            outputs: vec![port("data", "Text")],
            params: vec![
                string_param("delimiter", "Delimiter"),
                bool_param("has_header", "Has Header Row"),
            ],
        }
    }

    fn execute(&self, ctx: &mut ExecutionContext) -> Result<(), String> {
        let text = ctx.input_text("raw")?;
        let delimiter = ctx.param_str("delimiter").unwrap_or(",");
        let has_header = ctx.param_bool("has_header").unwrap_or(true);

        let delim_byte = delimiter.as_bytes().first().copied().unwrap_or(b',');
        let batch = csv_to_arrow(&text, delim_byte, has_header)?;
        ctx.set_output_arrow("data", batch);
        Ok(())
    }
}

pub struct CsvExport;

impl ToolHandler for CsvExport {
    fn spec(&self) -> ToolSpec {
        ToolSpec {
            id: "csv_export".into(),
            label: "CSV Export".into(),
            description: "Convert table to CSV text".into(),
            category: "export".into(),
            icon: "FileSpreadsheet".into(),
            inputs: vec![port("data", "Text")],
            outputs: vec![port("text", "Text")],
            params: vec![
                bool_param("include_header", "Include Header Row"),
            ],
        }
    }

    fn execute(&self, ctx: &mut ExecutionContext) -> Result<(), String> {
        let batch = ctx.input_arrow("data")?;
        let include_header = ctx.param_bool("include_header").unwrap_or(true);
        let text = arrow_to_csv(&batch, include_header)?;
        ctx.set_output_text("text", text);
        Ok(())
    }
}

// ============================================================
// REGISTRATION
// ============================================================

pub fn register(registry: &mut ToolRegistry) {
    registry.register(Box::new(CsvIngest));
    registry.register(Box::new(CsvExport));
}

// ============================================================
// TESTS
// ============================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_csv_roundtrip() {
        let csv = "name,age,city\nAlice,30,NYC\nBob,25,LA\n";
        let batch = csv_to_arrow(csv, b',', true).expect("Failed to parse CSV");
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 3);

        let exported = arrow_to_csv(&batch, true).expect("Failed to export CSV");
        assert!(exported.contains("Alice"));
        assert!(exported.contains("Bob"));
    }
}
