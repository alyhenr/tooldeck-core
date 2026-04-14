use tooldeck_registry::{
    ExecutionContext, DataFormat, ToolHandler, ToolRegistry, ToolSpec,
    port_with_format, arrow_to_csv, arrow_to_json,
};

// ============================================================
// CONVERTER TOOLS — explicit format change
// ============================================================

pub struct JsonToCsv;

impl ToolHandler for JsonToCsv {
    fn spec(&self) -> ToolSpec {
        ToolSpec {
            id: "json_to_csv".into(),
            label: "JSON → CSV".into(),
            description: "Convert JSON or NDJSON data to CSV format".into(),
            category: "convert".into(),
            icon: "ArrowRightLeft".into(),
            inputs: vec![port_with_format("data", "Text", "tabular")],
            outputs: vec![port_with_format("result", "Text", "csv")],
            params: vec![],
        }
    }

    fn execute(&self, ctx: &mut ExecutionContext) -> Result<(), String> {
        let batch = ctx.input_arrow("data")?;
        let csv_text = arrow_to_csv(&batch)?;
        ctx.set_output_text("result", csv_text, DataFormat::Csv);
        Ok(())
    }
}

pub struct CsvToJson;

impl ToolHandler for CsvToJson {
    fn spec(&self) -> ToolSpec {
        ToolSpec {
            id: "csv_to_json".into(),
            label: "CSV → JSON".into(),
            description: "Convert CSV data to JSON (NDJSON) format".into(),
            category: "convert".into(),
            icon: "ArrowRightLeft".into(),
            inputs: vec![port_with_format("data", "Text", "tabular")],
            outputs: vec![port_with_format("result", "Text", "json")],
            params: vec![],
        }
    }

    fn execute(&self, ctx: &mut ExecutionContext) -> Result<(), String> {
        let batch = ctx.input_arrow("data")?;
        let json_text = arrow_to_json(&batch)?;
        ctx.set_output_text("result", json_text, DataFormat::Ndjson);
        Ok(())
    }
}

// ============================================================
// REGISTRATION
// ============================================================

pub fn register(registry: &mut ToolRegistry) {
    registry.register(Box::new(JsonToCsv));
    registry.register(Box::new(CsvToJson));
}

// ============================================================
// TESTS
// ============================================================

#[cfg(test)]
mod tests {
    use super::*;
    use tooldeck_registry::json_to_arrow;

    #[test]
    fn test_json_to_csv() {
        let json = r#"{"name":"Alice","age":"30"}
{"name":"Bob","age":"25"}"#;
        let batch = json_to_arrow(json).unwrap();
        let csv = arrow_to_csv(&batch).unwrap();
        assert!(csv.contains("Alice"));
        assert!(csv.contains("Bob"));
    }
}
