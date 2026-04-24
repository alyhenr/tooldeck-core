use arrow::array::{Array, AsArray};
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use arrow_csv::reader::Format as CsvFormat;
use arrow_csv::{ReaderBuilder as CsvReaderBuilder, WriterBuilder as CsvWriterBuilder};
use arrow_json::{reader::infer_json_schema, ReaderBuilder as JsonReaderBuilder, writer::LineDelimitedWriter};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::io::Cursor;
use std::sync::Arc;

// ============================================================
// DATA FORMAT — tracks what format the data is in
// ============================================================

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DataFormat {
    Json,
    Ndjson,
    Csv,
    #[serde(rename = "text")]
    PlainText,
    #[serde(rename = "auto")]
    Unknown,
}

impl DataFormat {
    /// Detect format from text content using simple heuristics.
    pub fn detect(text: &str) -> Self {
        let trimmed = text.trim();
        if trimmed.is_empty() {
            return DataFormat::Unknown;
        }

        // Check JSON: starts with { or [
        if trimmed.starts_with('[') {
            return DataFormat::Json;
        }
        if trimmed.starts_with('{') {
            let lines: Vec<&str> = trimmed.lines().filter(|l| !l.trim().is_empty()).collect();
            if lines.len() > 1 && lines.iter().all(|l| l.trim().starts_with('{')) {
                return DataFormat::Ndjson;
            }
            return DataFormat::Json;
        }

        // Check CSV: multiple lines with consistent comma count
        let lines: Vec<&str> = trimmed.lines().filter(|l| !l.trim().is_empty()).collect();
        if lines.len() > 1 {
            let first_commas = lines[0].matches(',').count();
            if first_commas > 0 {
                let consistent = lines[1..].iter().all(|l| {
                    let c = l.matches(',').count();
                    c == first_commas || c == first_commas + 1 || c + 1 == first_commas
                });
                if consistent {
                    return DataFormat::Csv;
                }
            }
        }

        DataFormat::PlainText
    }

    /// Infer format from file extension.
    pub fn from_extension(filename: &str) -> Self {
        let ext = filename.rsplit('.').next().unwrap_or("").to_lowercase();
        match ext.as_str() {
            "json" => DataFormat::Json,
            "ndjson" | "jsonl" => DataFormat::Ndjson,
            "csv" => DataFormat::Csv,
            "tsv" => DataFormat::Csv, // treat TSV as CSV variant
            "txt" => DataFormat::PlainText,
            _ => DataFormat::Unknown,
        }
    }

    pub fn is_tabular(&self) -> bool {
        matches!(self, DataFormat::Json | DataFormat::Ndjson | DataFormat::Csv)
    }
}

// ============================================================
// DATA PAYLOAD — the internal data bus with format tracking
// ============================================================

pub enum DataPayload {
    Text { content: String, format: DataFormat },
    Arrow { batch: RecordBatch, source_format: DataFormat },
    Bytes { data: Vec<u8>, mime_type: String },
}

impl DataPayload {
    /// Create a text payload, auto-detecting format if unknown.
    pub fn text(content: String, format: DataFormat) -> Self {
        let resolved = if format == DataFormat::Unknown {
            DataFormat::detect(&content)
        } else {
            format
        };
        DataPayload::Text { content, format: resolved }
    }

    /// Get as Arrow RecordBatch. Uses format tag to choose the right parser.
    pub fn as_arrow(&self) -> Result<RecordBatch, String> {
        match self {
            DataPayload::Arrow { batch, .. } => Ok(batch.clone()),
            DataPayload::Text { content, format } => match format {
                DataFormat::Csv => csv_to_arrow(content),
                DataFormat::Json | DataFormat::Ndjson => json_to_arrow(content),
                _ => json_to_arrow(content).or_else(|_| csv_to_arrow(content)),
            },
            DataPayload::Bytes { .. } => Err("Cannot convert binary data to Arrow table".into()),
        }
    }

    /// Get as text. Serializes Arrow back to the source format.
    pub fn as_text(&self) -> Result<String, String> {
        match self {
            DataPayload::Text { content, .. } => Ok(content.clone()),
            DataPayload::Arrow { batch, source_format } => match source_format {
                DataFormat::Csv => arrow_to_csv(batch),
                _ => arrow_to_json(batch),
            },
            DataPayload::Bytes { .. } => Err("Cannot convert binary data to text".into()),
        }
    }

    /// Get as raw bytes.
    pub fn as_bytes(&self) -> Result<&[u8], String> {
        match self {
            DataPayload::Bytes { data, .. } => Ok(data),
            DataPayload::Text { content, .. } => Ok(content.as_bytes()),
            DataPayload::Arrow { .. } => Err("Cannot convert Arrow table to raw bytes".into()),
        }
    }

    /// Get the MIME type (only for Bytes payloads).
    pub fn mime_type(&self) -> Option<&str> {
        match self {
            DataPayload::Bytes { mime_type, .. } => Some(mime_type),
            _ => None,
        }
    }

    /// Get the format tag.
    pub fn format(&self) -> DataFormat {
        match self {
            DataPayload::Text { format, .. } => *format,
            DataPayload::Arrow { source_format, .. } => *source_format,
            DataPayload::Bytes { .. } => DataFormat::Unknown,
        }
    }

    /// Generate a bounded preview for the frontend.
    pub fn preview(&self, max_rows: usize) -> NodePreview {
        match self {
            DataPayload::Arrow { batch, .. } => batch_to_preview(batch, max_rows),
            DataPayload::Text { content, .. } => {
                if let Ok(batch) = self.as_arrow() {
                    batch_to_preview(&batch, max_rows)
                } else {
                    text_to_preview(content)
                }
            }
            DataPayload::Bytes { data, mime_type } => {
                let size_str = if data.len() < 1024 {
                    format!("{} B", data.len())
                } else if data.len() < 1048576 {
                    format!("{:.1} KB", data.len() as f64 / 1024.0)
                } else {
                    format!("{:.1} MB", data.len() as f64 / 1048576.0)
                };
                NodePreview::Text {
                    excerpt: format!("[{mime_type}] {size_str}"),
                    total_bytes: data.len(),
                }
            }
        }
    }

    /// Row count (only for Arrow data, or parseable text).
    pub fn row_count(&self) -> Option<usize> {
        match self {
            DataPayload::Arrow { batch, .. } => Some(batch.num_rows()),
            DataPayload::Text { .. } => self.as_arrow().ok().map(|b| b.num_rows()),
            DataPayload::Bytes { .. } => None,
        }
    }
}

// ============================================================
// EXECUTION CONTEXT
// ============================================================

pub struct ExecutionContext {
    inputs: HashMap<String, DataPayload>,
    params: HashMap<String, serde_json::Value>,
    outputs: HashMap<String, DataPayload>,
}

impl ExecutionContext {
    pub fn new(
        inputs: HashMap<String, DataPayload>,
        params: HashMap<String, serde_json::Value>,
    ) -> Self {
        Self { inputs, params, outputs: HashMap::new() }
    }

    // --- Input accessors ---

    pub fn input_arrow(&self, port: &str) -> Result<RecordBatch, String> {
        self.inputs
            .get(port)
            .ok_or_else(|| format!("Input '{port}' not connected"))?
            .as_arrow()
    }

    pub fn input_text(&self, port: &str) -> Result<String, String> {
        self.inputs
            .get(port)
            .ok_or_else(|| format!("Input '{port}' not connected"))?
            .as_text()
    }

    /// Get input as raw bytes.
    pub fn input_bytes(&self, port: &str) -> Result<Vec<u8>, String> {
        self.inputs
            .get(port)
            .ok_or_else(|| format!("Input '{port}' not connected"))?
            .as_bytes()
            .map(|b| b.to_vec())
    }

    /// Get multiple byte inputs for a multi-port (e.g., merge tools).
    /// Looks for "port", "port:0", "port:1", etc.
    pub fn input_bytes_multi(&self, port: &str) -> Result<Vec<Vec<u8>>, String> {
        let mut results = Vec::new();

        // Check for single key first
        if let Some(payload) = self.inputs.get(port) {
            results.push(payload.as_bytes()?.to_vec());
        }

        // Check for indexed keys: "port:0", "port:1", ...
        let mut idx = 0;
        loop {
            let key = format!("{port}:{idx}");
            if let Some(payload) = self.inputs.get(&key) {
                results.push(payload.as_bytes()?.to_vec());
                idx += 1;
            } else {
                break;
            }
        }

        if results.is_empty() {
            return Err(format!("Input '{port}' not connected"));
        }
        Ok(results)
    }

    /// Get the MIME type of a bytes input.
    pub fn input_mime_type(&self, port: &str) -> Option<String> {
        self.inputs.get(port).and_then(|p| p.mime_type().map(|s| s.to_string()))
    }

    /// Get the format of an input port's data.
    pub fn input_format(&self, port: &str) -> DataFormat {
        self.inputs
            .get(port)
            .map(|p| p.format())
            .unwrap_or(DataFormat::Unknown)
    }

    // --- Param accessors ---

    pub fn param_str(&self, name: &str) -> Result<&str, String> {
        self.params.get(name).and_then(|v| v.as_str())
            .ok_or_else(|| format!("Missing param '{name}'"))
    }

    pub fn param_str_array(&self, name: &str) -> Result<Vec<&str>, String> {
        self.params.get(name).and_then(|v| v.as_array())
            .ok_or_else(|| format!("Missing param '{name}'"))?
            .iter()
            .map(|v| v.as_str().ok_or_else(|| format!("Param '{name}' has non-string element")))
            .collect()
    }

    pub fn param_f64(&self, name: &str) -> Result<f64, String> {
        self.params.get(name).and_then(|v| v.as_f64())
            .ok_or_else(|| format!("Missing param '{name}'"))
    }

    pub fn param_bool(&self, name: &str) -> Result<bool, String> {
        self.params.get(name).and_then(|v| v.as_bool())
            .ok_or_else(|| format!("Missing param '{name}'"))
    }

    // --- Output setters ---

    /// Set Arrow output, inheriting format from the specified input port.
    pub fn set_output_arrow(&mut self, port: &str, batch: RecordBatch) {
        // Inherit source_format from the first input
        let source_format = self.inputs.values().next()
            .map(|p| p.format())
            .unwrap_or(DataFormat::Ndjson);
        self.outputs.insert(port.to_string(), DataPayload::Arrow { batch, source_format });
    }

    /// Set Arrow output with an explicit format (used by converter tools).
    pub fn set_output_arrow_as(&mut self, port: &str, batch: RecordBatch, format: DataFormat) {
        self.outputs.insert(port.to_string(), DataPayload::Arrow { batch, source_format: format });
    }

    /// Set text output with a format tag.
    pub fn set_output_text(&mut self, port: &str, text: String, format: DataFormat) {
        self.outputs.insert(port.to_string(), DataPayload::Text { content: text, format });
    }

    /// Set binary output with a MIME type.
    pub fn set_output_bytes(&mut self, port: &str, data: Vec<u8>, mime_type: &str) {
        self.outputs.insert(port.to_string(), DataPayload::Bytes {
            data,
            mime_type: mime_type.to_string(),
        });
    }

    pub fn into_outputs(self) -> HashMap<String, DataPayload> {
        self.outputs
    }
}

// ============================================================
// TOOL HANDLER TRAIT
// ============================================================

pub trait ToolHandler: Send + Sync {
    fn spec(&self) -> ToolSpec;
    fn execute(&self, ctx: &mut ExecutionContext) -> Result<(), String>;
}

// ============================================================
// TOOL REGISTRY
// ============================================================

pub struct ToolRegistry {
    handlers: HashMap<String, Box<dyn ToolHandler>>,
}

impl ToolRegistry {
    pub fn new() -> Self { Self { handlers: HashMap::new() } }

    pub fn register(&mut self, handler: Box<dyn ToolHandler>) {
        let id = handler.spec().id.clone();
        self.handlers.insert(id, handler);
    }

    pub fn get_handler(&self, tool_id: &str) -> Option<&dyn ToolHandler> {
        self.handlers.get(tool_id).map(|h| h.as_ref())
    }

    pub fn manifest(&self) -> ToolManifest {
        ToolManifest {
            port_types: HashMap::from([
                ("Bytes".into(), PortTypeSpec {
                    label: "Bytes".into(), color: "#94a3b8".into(),
                    can_connect_to: vec!["Bytes".into()],
                }),
                ("Text".into(), PortTypeSpec {
                    label: "Text".into(), color: "#3b82f6".into(),
                    can_connect_to: vec!["Bytes".into(), "Text".into()],
                }),
            ]),
            tools: self.handlers.values().map(|h| h.spec()).collect(),
        }
    }
}

impl Default for ToolRegistry {
    fn default() -> Self { Self::new() }
}

// ============================================================
// SPEC TYPES
// ============================================================

#[derive(Serialize, Deserialize, Clone)]
pub struct ToolManifest {
    pub port_types: HashMap<String, PortTypeSpec>,
    pub tools: Vec<ToolSpec>,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct PortTypeSpec {
    pub label: String,
    pub color: String,
    pub can_connect_to: Vec<String>,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct ToolSpec {
    pub id: String,
    pub label: String,
    pub description: String,
    pub category: String,
    pub icon: String,
    pub inputs: Vec<PortSpec>,
    pub outputs: Vec<PortSpec>,
    pub params: Vec<ParamSpec>,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct PortSpec {
    pub name: String,
    #[serde(rename = "type")]
    pub port_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub format: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub multiple: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub min: Option<u32>,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct ParamSpec {
    pub name: String,
    #[serde(rename = "type")]
    pub param_type: String,
    pub label: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub options: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub default: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ui_hint: Option<String>,
}

// ============================================================
// PIPELINE TYPES
// ============================================================

#[derive(Serialize, Deserialize)]
pub struct PipelineDescription {
    pub nodes: HashMap<String, PipelineNode>,
    pub edges: Vec<PipelineEdge>,
    pub provided_inputs: HashMap<String, ProvidedInput>,
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "kind")]
pub enum ProvidedInput {
    #[serde(rename = "text")]
    Text { content: String, format: DataFormat },
    #[serde(rename = "binary")]
    Binary { mime_type: String },
}

#[derive(Serialize, Deserialize)]
pub struct PipelineNode {
    pub tool_id: String,
    pub params: HashMap<String, serde_json::Value>,
}

#[derive(Serialize, Deserialize)]
pub struct PipelineEdge {
    pub from: PortRef,
    pub to: PortRef,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub order: Option<u32>,
}

#[derive(Serialize, Deserialize)]
pub struct PortRef { pub node: String, pub port: String }

#[derive(Serialize)]
pub struct PipelineResult {
    pub status: String,
    pub node_results: HashMap<String, NodeResult>,
    pub terminal_outputs: HashMap<String, String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub failed_node: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[derive(Serialize)]
pub struct NodeResult {
    pub status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub duration_ms: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub output_rows: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub preview: Option<NodePreview>,
}

#[derive(Serialize)]
#[serde(tag = "type")]
pub enum NodePreview {
    #[serde(rename = "tabular")]
    Tabular { columns: Vec<String>, sample_rows: Vec<Vec<serde_json::Value>>, total_rows: usize },
    #[serde(rename = "text")]
    Text { excerpt: String, total_bytes: usize },
}

// ============================================================
// HELPER BUILDERS
// ============================================================

pub fn port(name: &str, port_type: &str) -> PortSpec {
    PortSpec { name: name.into(), port_type: port_type.into(), format: None, multiple: None, min: None }
}

pub fn port_with_format(name: &str, port_type: &str, format: &str) -> PortSpec {
    PortSpec { name: name.into(), port_type: port_type.into(), format: Some(format.into()), multiple: None, min: None }
}

pub fn multi_port(name: &str, port_type: &str, min: u32) -> PortSpec {
    PortSpec { name: name.into(), port_type: port_type.into(), format: None, multiple: Some(true), min: Some(min) }
}

pub fn string_param(name: &str, label: &str) -> ParamSpec {
    ParamSpec { name: name.into(), param_type: "string".into(), label: label.into(), options: None, default: None, ui_hint: None }
}

pub fn string_array_param(name: &str, label: &str) -> ParamSpec {
    ParamSpec { name: name.into(), param_type: "string[]".into(), label: label.into(), options: None, default: None, ui_hint: None }
}

pub fn number_param(name: &str, label: &str) -> ParamSpec {
    ParamSpec { name: name.into(), param_type: "number".into(), label: label.into(), options: None, default: None, ui_hint: None }
}

pub fn bool_param(name: &str, label: &str) -> ParamSpec {
    ParamSpec { name: name.into(), param_type: "boolean".into(), label: label.into(), options: None, default: None, ui_hint: None }
}

pub fn select_param(name: &str, label: &str, options: &[&str]) -> ParamSpec {
    ParamSpec {
        name: name.into(), param_type: "select".into(), label: label.into(),
        options: Some(options.iter().map(|s| s.to_string()).collect()),
        default: None, ui_hint: None,
    }
}

// ============================================================
// FORMAT PARSERS — Text ↔ Arrow
// ============================================================

pub fn json_to_arrow(text: &str) -> Result<RecordBatch, String> {
    let ndjson = normalize_to_ndjson(text)?;
    let mut cursor = Cursor::new(ndjson.as_bytes());
    let (schema, _) = infer_json_schema(&mut cursor, None)
        .map_err(|e| format!("Failed to infer JSON schema: {e}"))?;
    cursor.set_position(0);
    let mut reader = JsonReaderBuilder::new(Arc::new(schema))
        .build(cursor)
        .map_err(|e| format!("Failed to build JSON reader: {e}"))?;
    match reader.next() {
        Some(Ok(batch)) => Ok(batch),
        Some(Err(e)) => Err(format!("Failed to read JSON: {e}")),
        None => Err("JSON input was empty".into()),
    }
}

/// Normalize any valid JSON into NDJSON (one JSON object per line).
/// Handles: single objects, pretty-printed objects, JSON arrays, and NDJSON.
fn normalize_to_ndjson(text: &str) -> Result<String, String> {
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return Err("JSON input is empty".into());
    }

    // Try parsing as a single JSON value first
    match serde_json::from_str::<serde_json::Value>(trimmed) {
        Ok(serde_json::Value::Array(arr)) => {
            // JSON array of objects → each element becomes one NDJSON line
            if arr.is_empty() {
                return Err("JSON array is empty".into());
            }
            let lines: Vec<String> = arr.iter()
                .map(|v| serde_json::to_string(v).unwrap_or_default())
                .collect();
            Ok(lines.join("\n"))
        }
        Ok(serde_json::Value::Object(_)) => {
            // Single JSON object → one NDJSON line
            Ok(serde_json::to_string(&serde_json::from_str::<serde_json::Value>(trimmed).unwrap())
                .map_err(|e| format!("Failed to serialize JSON: {e}"))?)
        }
        Ok(_) => {
            Err("JSON input must be an object or array of objects".into())
        }
        Err(_) => {
            // Not valid as a single JSON value — might already be NDJSON
            // Verify each non-empty line is valid JSON
            let lines: Vec<&str> = trimmed.lines()
                .map(|l| l.trim())
                .filter(|l| !l.is_empty())
                .collect();

            if lines.is_empty() {
                return Err("JSON input is empty".into());
            }

            for (i, line) in lines.iter().enumerate() {
                if serde_json::from_str::<serde_json::Value>(line).is_err() {
                    return Err(format!(
                        "Invalid JSON at line {}: {}",
                        i + 1,
                        if line.len() > 50 { &line[..50] } else { line }
                    ));
                }
            }

            Ok(lines.join("\n"))
        }
    }
}

pub fn arrow_to_json(batch: &RecordBatch) -> Result<String, String> {
    let mut buf = Vec::new();
    {
        let mut writer = LineDelimitedWriter::new(&mut buf);
        writer.write(batch).map_err(|e| format!("JSON write error: {e}"))?;
        writer.finish().map_err(|e| format!("JSON finish error: {e}"))?;
    }
    String::from_utf8(buf).map_err(|e| format!("UTF8 error: {e}"))
}

pub fn csv_to_arrow(text: &str) -> Result<RecordBatch, String> {
    let cursor = Cursor::new(text.as_bytes());
    let format = CsvFormat::default().with_delimiter(b',').with_header(true);

    let (schema, _) = format
        .infer_schema(Cursor::new(text.as_bytes()), Some(100))
        .map_err(|e| format!("Failed to infer CSV schema: {e}"))?;

    let reader = CsvReaderBuilder::new(Arc::new(schema))
        .with_format(format)
        .build(cursor)
        .map_err(|e| format!("Failed to build CSV reader: {e}"))?;

    let batches: Result<Vec<RecordBatch>, _> = reader.collect();
    let batches = batches.map_err(|e| format!("Failed to read CSV: {e}"))?;

    if batches.is_empty() {
        return Err("CSV input was empty".into());
    }
    if batches.len() == 1 {
        return Ok(batches.into_iter().next().unwrap());
    }
    arrow::compute::concat_batches(&batches[0].schema(), &batches)
        .map_err(|e| format!("Failed to concat CSV batches: {e}"))
}

pub fn arrow_to_csv(batch: &RecordBatch) -> Result<String, String> {
    let mut buf = Vec::new();
    {
        let mut writer = CsvWriterBuilder::new().with_header(true).build(&mut buf);
        writer.write(batch).map_err(|e| format!("CSV write error: {e}"))?;
    }
    String::from_utf8(buf).map_err(|e| format!("UTF8 error: {e}"))
}

// ============================================================
// PREVIEW GENERATION
// ============================================================

fn batch_to_preview(batch: &RecordBatch, max_rows: usize) -> NodePreview {
    let schema = batch.schema();
    let columns: Vec<String> = schema.fields().iter().map(|f| f.name().clone()).collect();
    let num_rows = batch.num_rows().min(max_rows);
    let total_rows = batch.num_rows();

    let mut sample_rows = Vec::with_capacity(num_rows);
    for row_idx in 0..num_rows {
        let mut row = Vec::with_capacity(columns.len());
        for col_idx in 0..batch.num_columns() {
            row.push(arrow_cell_to_json(batch.column(col_idx).as_ref(), row_idx));
        }
        sample_rows.push(row);
    }
    NodePreview::Tabular { columns, sample_rows, total_rows }
}

fn text_to_preview(text: &str) -> NodePreview {
    let total_bytes = text.len();
    let excerpt = if text.len() > 500 { format!("{}...", &text[..500]) } else { text.to_string() };
    NodePreview::Text { excerpt, total_bytes }
}

/// Convert any Arrow cell to its string representation.
/// Used by tools for filtering, deduplication, and other comparisons.
/// Produces clean output for all types including lists and nested objects.
pub fn cell_to_string(col: &dyn Array, row: usize) -> String {
    use arrow::datatypes::{
        Float32Type, Float64Type, Int16Type, Int32Type, Int64Type, Int8Type,
        UInt16Type, UInt32Type, UInt64Type, UInt8Type,
    };
    if col.is_null(row) { return String::new(); }
    match col.data_type() {
        // Fast path for common primitives — avoid JSON overhead.
        // `as_primitive::<T>` requires T to match the array's actual element type
        // EXACTLY, so each integer / float width needs its own branch.
        DataType::Utf8 => col.as_string::<i32>().value(row).to_string(),
        DataType::LargeUtf8 => col.as_string::<i64>().value(row).to_string(),
        DataType::Int8 => col.as_primitive::<Int8Type>().value(row).to_string(),
        DataType::Int16 => col.as_primitive::<Int16Type>().value(row).to_string(),
        DataType::Int32 => col.as_primitive::<Int32Type>().value(row).to_string(),
        DataType::Int64 => col.as_primitive::<Int64Type>().value(row).to_string(),
        DataType::UInt8 => col.as_primitive::<UInt8Type>().value(row).to_string(),
        DataType::UInt16 => col.as_primitive::<UInt16Type>().value(row).to_string(),
        DataType::UInt32 => col.as_primitive::<UInt32Type>().value(row).to_string(),
        DataType::UInt64 => col.as_primitive::<UInt64Type>().value(row).to_string(),
        DataType::Float32 => col.as_primitive::<Float32Type>().value(row).to_string(),
        DataType::Float64 => col.as_primitive::<Float64Type>().value(row).to_string(),
        DataType::Boolean => col.as_boolean().value(row).to_string(),
        // Complex types (List, Struct, etc.) — serialize as JSON string
        _ => {
            let json_val = arrow_cell_to_json(col, row);
            serde_json::to_string(&json_val).unwrap_or_default()
        }
    }
}

fn arrow_cell_to_json(col: &dyn Array, row: usize) -> serde_json::Value {
    use arrow::datatypes::{
        Float32Type, Float64Type, Int16Type, Int32Type, Int64Type, Int8Type,
        UInt16Type, UInt32Type, UInt64Type, UInt8Type,
    };
    if col.is_null(row) { return serde_json::Value::Null; }
    match col.data_type() {
        DataType::Utf8 => serde_json::Value::String(col.as_string::<i32>().value(row).to_string()),
        DataType::LargeUtf8 => serde_json::Value::String(col.as_string::<i64>().value(row).to_string()),
        DataType::Int8 => serde_json::json!(col.as_primitive::<Int8Type>().value(row)),
        DataType::Int16 => serde_json::json!(col.as_primitive::<Int16Type>().value(row)),
        DataType::Int32 => serde_json::json!(col.as_primitive::<Int32Type>().value(row)),
        DataType::Int64 => serde_json::json!(col.as_primitive::<Int64Type>().value(row)),
        DataType::UInt8 => serde_json::json!(col.as_primitive::<UInt8Type>().value(row)),
        DataType::UInt16 => serde_json::json!(col.as_primitive::<UInt16Type>().value(row)),
        DataType::UInt32 => serde_json::json!(col.as_primitive::<UInt32Type>().value(row)),
        DataType::UInt64 => serde_json::json!(col.as_primitive::<UInt64Type>().value(row)),
        DataType::Float32 => serde_json::json!(col.as_primitive::<Float32Type>().value(row)),
        DataType::Float64 => serde_json::json!(col.as_primitive::<Float64Type>().value(row)),
        DataType::Boolean => serde_json::Value::Bool(col.as_boolean().value(row)),
        _ => serde_json::Value::String(format!("{:?}", col.slice(row, 1))),
    }
}

// ============================================================
// TESTS
// ============================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_json_single_object() {
        let input = r#"{"name": "John", "age": 30}"#;
        let batch = json_to_arrow(input).unwrap();
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.num_columns(), 2);
    }

    #[test]
    fn test_json_single_object_pretty() {
        let input = r#"{
  "name": "John Doe",
  "age": 30,
  "isStudent": false
}"#;
        let batch = json_to_arrow(input).unwrap();
        assert_eq!(batch.num_rows(), 1);
    }

    #[test]
    fn test_json_array() {
        let input = r#"[
  {"name": "John", "age": 30},
  {"name": "Jane", "age": 25}
]"#;
        let batch = json_to_arrow(input).unwrap();
        assert_eq!(batch.num_rows(), 2);
    }

    #[test]
    fn test_ndjson() {
        let input = r#"{"name":"John","age":30}
{"name":"Jane","age":25}"#;
        let batch = json_to_arrow(input).unwrap();
        assert_eq!(batch.num_rows(), 2);
    }

    #[test]
    fn test_json_with_nested() {
        let input = r#"{"name": "John", "hobbies": ["reading", "coding"], "address": {"city": "NYC"}}"#;
        let batch = json_to_arrow(input).unwrap();
        assert_eq!(batch.num_rows(), 1);
    }

    #[test]
    fn test_json_empty_returns_error() {
        assert!(json_to_arrow("").is_err());
        assert!(json_to_arrow("  ").is_err());
    }

    #[test]
    fn test_json_invalid_returns_error() {
        assert!(json_to_arrow("not json at all").is_err());
    }

    #[test]
    fn test_csv_basic() {
        let input = "Name,Email,Age\nJohn,john@test.com,30\nJane,jane@test.com,25\n";
        let batch = csv_to_arrow(input).unwrap();
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 3);
    }

    #[test]
    fn test_format_detect_json_object() {
        assert_eq!(DataFormat::detect(r#"{"name": "John"}"#), DataFormat::Json);
    }

    #[test]
    fn test_format_detect_json_array() {
        assert_eq!(DataFormat::detect(r#"[{"a":1}]"#), DataFormat::Json);
    }

    #[test]
    fn test_format_detect_ndjson() {
        assert_eq!(DataFormat::detect("{\"a\":1}\n{\"a\":2}"), DataFormat::Ndjson);
    }

    #[test]
    fn test_format_detect_csv() {
        assert_eq!(DataFormat::detect("a,b,c\n1,2,3\n4,5,6"), DataFormat::Csv);
    }

    #[test]
    fn test_format_detect_plain() {
        assert_eq!(DataFormat::detect("hello world"), DataFormat::PlainText);
    }
}
