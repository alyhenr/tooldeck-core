use arrow::array::{Array, AsArray};
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use arrow_json::{reader::infer_json_schema, ReaderBuilder, writer::LineDelimitedWriter};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::io::Cursor;
use std::sync::Arc;

// ============================================================
// DATA PAYLOAD — the internal data bus
// ============================================================

/// Data flowing between tools. The port type (Text/Bytes) is the frontend
/// concept. Internally, tools can produce Arrow for performance. The
/// execution infrastructure auto-converts at boundaries.
pub enum DataPayload {
    Text(String),
    Arrow(RecordBatch),
}

impl DataPayload {
    /// Get as Arrow RecordBatch. If Text, attempts NDJSON parse (best-effort).
    pub fn as_arrow(&self) -> Result<RecordBatch, String> {
        match self {
            DataPayload::Arrow(batch) => Ok(batch.clone()),
            DataPayload::Text(text) => text_to_arrow(text),
        }
    }

    /// Get as text. If Arrow, serializes to NDJSON.
    pub fn as_text(&self) -> Result<String, String> {
        match self {
            DataPayload::Text(text) => Ok(text.clone()),
            DataPayload::Arrow(batch) => arrow_to_text(batch),
        }
    }

    /// Generate a bounded preview for the frontend.
    pub fn preview(&self, max_rows: usize) -> NodePreview {
        match self {
            DataPayload::Arrow(batch) => batch_to_preview(batch, max_rows),
            DataPayload::Text(text) => text_to_preview(text),
        }
    }

    /// Row count (only for Arrow data).
    pub fn row_count(&self) -> Option<usize> {
        match self {
            DataPayload::Arrow(batch) => Some(batch.num_rows()),
            DataPayload::Text(_) => None,
        }
    }
}

// ============================================================
// EXECUTION CONTEXT — what tools receive
// ============================================================

/// The context passed to a tool during execution. Tools read inputs,
/// read params, and write outputs through this interface.
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
        Self {
            inputs,
            params,
            outputs: HashMap::new(),
        }
    }

    // --- Input accessors ---

    /// Get input as Arrow RecordBatch. Auto-parses Text→Arrow if needed.
    pub fn input_arrow(&self, port: &str) -> Result<RecordBatch, String> {
        self.inputs
            .get(port)
            .ok_or_else(|| format!("Input '{port}' not connected"))?
            .as_arrow()
    }

    /// Get input as text. Auto-serializes Arrow→Text if needed.
    pub fn input_text(&self, port: &str) -> Result<String, String> {
        self.inputs
            .get(port)
            .ok_or_else(|| format!("Input '{port}' not connected"))?
            .as_text()
    }

    // --- Param accessors ---

    pub fn param_str(&self, name: &str) -> Result<&str, String> {
        self.params
            .get(name)
            .and_then(|v| v.as_str())
            .ok_or_else(|| format!("Missing param '{name}'"))
    }

    pub fn param_str_array(&self, name: &str) -> Result<Vec<&str>, String> {
        self.params
            .get(name)
            .and_then(|v| v.as_array())
            .ok_or_else(|| format!("Missing param '{name}'"))?
            .iter()
            .map(|v| v.as_str().ok_or_else(|| format!("Param '{name}' has non-string element")))
            .collect()
    }

    pub fn param_f64(&self, name: &str) -> Result<f64, String> {
        self.params
            .get(name)
            .and_then(|v| v.as_f64())
            .ok_or_else(|| format!("Missing param '{name}'"))
    }

    pub fn param_bool(&self, name: &str) -> Result<bool, String> {
        self.params
            .get(name)
            .and_then(|v| v.as_bool())
            .ok_or_else(|| format!("Missing param '{name}'"))
    }

    // --- Output setters ---

    pub fn set_output_arrow(&mut self, port: &str, batch: RecordBatch) {
        self.outputs.insert(port.to_string(), DataPayload::Arrow(batch));
    }

    pub fn set_output_text(&mut self, port: &str, text: String) {
        self.outputs.insert(port.to_string(), DataPayload::Text(text));
    }

    /// Consume the context and return the outputs.
    pub fn into_outputs(self) -> HashMap<String, DataPayload> {
        self.outputs
    }
}

// ============================================================
// TOOL HANDLER TRAIT — what tools implement
// ============================================================

pub trait ToolHandler: Send + Sync {
    /// Declare the tool's metadata (id, ports, params).
    fn spec(&self) -> ToolSpec;

    /// Execute the tool. Read inputs and params from ctx, write outputs to ctx.
    fn execute(&self, ctx: &mut ExecutionContext) -> Result<(), String>;
}

// ============================================================
// TOOL REGISTRY — collects handlers, generates manifest
// ============================================================

pub struct ToolRegistry {
    handlers: HashMap<String, Box<dyn ToolHandler>>,
}

impl ToolRegistry {
    pub fn new() -> Self {
        Self {
            handlers: HashMap::new(),
        }
    }

    pub fn register(&mut self, handler: Box<dyn ToolHandler>) {
        let id = handler.spec().id.clone();
        self.handlers.insert(id, handler);
    }

    pub fn get_handler(&self, tool_id: &str) -> Option<&dyn ToolHandler> {
        self.handlers.get(tool_id).map(|h| h.as_ref())
    }

    /// Generate the full manifest for the frontend.
    pub fn manifest(&self) -> ToolManifest {
        ToolManifest {
            port_types: HashMap::from([
                ("Bytes".into(), PortTypeSpec {
                    label: "Bytes".into(),
                    color: "#94a3b8".into(),
                    can_connect_to: vec!["Bytes".into()],
                }),
                ("Text".into(), PortTypeSpec {
                    label: "Text".into(),
                    color: "#3b82f6".into(),
                    can_connect_to: vec!["Bytes".into(), "Text".into()],
                }),
            ]),
            tools: self.handlers.values().map(|h| h.spec()).collect(),
        }
    }
}

impl Default for ToolRegistry {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================
// SPEC TYPES — shared between engine and tools
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
// PIPELINE TYPES — shared between engine and frontend
// ============================================================

#[derive(Serialize, Deserialize)]
pub struct PipelineDescription {
    pub nodes: HashMap<String, PipelineNode>,
    pub edges: Vec<PipelineEdge>,
    pub provided_inputs: HashMap<String, String>,
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
pub struct PortRef {
    pub node: String,
    pub port: String,
}

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
    Tabular {
        columns: Vec<String>,
        sample_rows: Vec<Vec<serde_json::Value>>,
        total_rows: usize,
    },
    #[serde(rename = "text")]
    Text {
        excerpt: String,
        total_bytes: usize,
    },
}

// ============================================================
// HELPER BUILDERS — for clean tool registration
// ============================================================

pub fn port(name: &str, port_type: &str) -> PortSpec {
    PortSpec {
        name: name.into(),
        port_type: port_type.into(),
        multiple: None,
        min: None,
    }
}

pub fn multi_port(name: &str, port_type: &str, min: u32) -> PortSpec {
    PortSpec {
        name: name.into(),
        port_type: port_type.into(),
        multiple: Some(true),
        min: Some(min),
    }
}

pub fn string_param(name: &str, label: &str) -> ParamSpec {
    ParamSpec {
        name: name.into(),
        param_type: "string".into(),
        label: label.into(),
        options: None,
        default: None,
        ui_hint: None,
    }
}

pub fn string_array_param(name: &str, label: &str) -> ParamSpec {
    ParamSpec {
        name: name.into(),
        param_type: "string[]".into(),
        label: label.into(),
        options: None,
        default: None,
        ui_hint: None,
    }
}

pub fn number_param(name: &str, label: &str) -> ParamSpec {
    ParamSpec {
        name: name.into(),
        param_type: "number".into(),
        label: label.into(),
        options: None,
        default: None,
        ui_hint: None,
    }
}

pub fn bool_param(name: &str, label: &str) -> ParamSpec {
    ParamSpec {
        name: name.into(),
        param_type: "boolean".into(),
        label: label.into(),
        options: None,
        default: None,
        ui_hint: None,
    }
}

// ============================================================
// INTERNAL CONVERTERS — Text ↔ Arrow
// ============================================================

fn text_to_arrow(ndjson: &str) -> Result<RecordBatch, String> {
    let mut cursor = Cursor::new(ndjson.as_bytes());
    let (schema, _) = infer_json_schema(&mut cursor, None)
        .map_err(|e| format!("Failed to infer schema: {e}"))?;
    cursor.set_position(0);
    let mut reader = ReaderBuilder::new(Arc::new(schema))
        .build(cursor)
        .map_err(|e| format!("Failed to build reader: {e}"))?;
    match reader.next() {
        Some(Ok(batch)) => Ok(batch),
        Some(Err(e)) => Err(format!("Failed to read batch: {e}")),
        None => Err("Input was empty".into()),
    }
}

fn arrow_to_text(batch: &RecordBatch) -> Result<String, String> {
    let mut buf = Vec::new();
    {
        let mut writer = LineDelimitedWriter::new(&mut buf);
        writer.write(batch).map_err(|e| format!("Write error: {e}"))?;
        writer.finish().map_err(|e| format!("Finish error: {e}"))?;
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
    let excerpt = if text.len() > 500 {
        format!("{}...", &text[..500])
    } else {
        text.to_string()
    };
    NodePreview::Text { excerpt, total_bytes }
}

fn arrow_cell_to_json(col: &dyn Array, row: usize) -> serde_json::Value {
    if col.is_null(row) {
        return serde_json::Value::Null;
    }
    match col.data_type() {
        DataType::Utf8 => {
            serde_json::Value::String(col.as_string::<i32>().value(row).to_string())
        }
        DataType::LargeUtf8 => {
            serde_json::Value::String(col.as_string::<i64>().value(row).to_string())
        }
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => {
            serde_json::json!(col.as_primitive::<arrow::datatypes::Int64Type>().value(row))
        }
        DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 => {
            serde_json::json!(col.as_primitive::<arrow::datatypes::UInt64Type>().value(row))
        }
        DataType::Float32 | DataType::Float64 => {
            serde_json::json!(col.as_primitive::<arrow::datatypes::Float64Type>().value(row))
        }
        DataType::Boolean => {
            serde_json::Value::Bool(col.as_boolean().value(row))
        }
        _ => serde_json::Value::String(format!("{:?}", col.slice(row, 1))),
    }
}
