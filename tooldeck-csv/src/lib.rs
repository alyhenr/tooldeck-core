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
    use tooldeck_registry::{csv_to_arrow, json_to_arrow};

    #[test]
    fn test_json_to_csv() {
        let json = r#"{"name":"Alice","age":"30"}
{"name":"Bob","age":"25"}"#;
        let batch = json_to_arrow(json).unwrap();
        let csv = arrow_to_csv(&batch).unwrap();
        assert!(csv.contains("Alice"));
        assert!(csv.contains("Bob"));
        // Header row should include the column names
        let first_line = csv.lines().next().unwrap();
        assert!(first_line.contains("name"));
        assert!(first_line.contains("age"));
    }

    #[test]
    fn test_csv_to_json_basic() {
        let csv = "name,age\nAlice,30\nBob,25\n";
        let batch = csv_to_arrow(csv).unwrap();
        let json = arrow_to_json(&batch).unwrap();
        assert!(json.contains("Alice"));
        assert!(json.contains("Bob"));
        assert!(json.contains("\"name\""));
        assert!(json.contains("\"age\""));
    }

    #[test]
    fn test_round_trip_json_csv_json() {
        // JSON → CSV → JSON should preserve the data
        let original_json = r#"{"name":"Alice","city":"NYC"}
{"name":"Bob","city":"LA"}"#;
        let batch1 = json_to_arrow(original_json).unwrap();
        let csv = arrow_to_csv(&batch1).unwrap();
        let batch2 = csv_to_arrow(&csv).unwrap();
        let json_again = arrow_to_json(&batch2).unwrap();
        assert!(json_again.contains("Alice"));
        assert!(json_again.contains("NYC"));
        assert!(json_again.contains("Bob"));
        assert!(json_again.contains("LA"));
        assert_eq!(batch1.num_rows(), batch2.num_rows());
        assert_eq!(batch1.num_columns(), batch2.num_columns());
    }

    #[test]
    fn test_ndjson_to_csv() {
        // Each line is a JSON object (NDJSON)
        let ndjson = r#"{"a":"1","b":"one"}
{"a":"2","b":"two"}
{"a":"3","b":"three"}"#;
        let batch = json_to_arrow(ndjson).unwrap();
        let csv = arrow_to_csv(&batch).unwrap();
        assert_eq!(batch.num_rows(), 3);
        assert!(csv.contains("one"));
        assert!(csv.contains("three"));
    }
}
