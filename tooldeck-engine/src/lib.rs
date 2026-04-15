use wasm_bindgen::prelude::*;
use std::cell::RefCell;
use std::collections::HashMap;

use tooldeck_registry::{
    DataPayload, ExecutionContext, ToolRegistry,
    PipelineDescription, PipelineResult, NodeResult,
};

// ============================================================
// GLOBAL REGISTRY + BINARY STORE
// ============================================================

fn build_registry() -> ToolRegistry {
    let mut registry = ToolRegistry::new();
    tooldeck_json::register(&mut registry);
    tooldeck_csv::register(&mut registry);
    tooldeck_transforms::register(&mut registry);
    tooldeck_image::register(&mut registry);
    tooldeck_pdf::register(&mut registry);
    registry
}

thread_local! {
    static REGISTRY: ToolRegistry = build_registry();
    /// Binary store: holds raw bytes keyed by "nodeId:portName".
    /// Worker calls set_binary_input before run_pipeline, and
    /// get_binary_output after. No base64 encoding needed.
    static BINARY_STORE: RefCell<HashMap<String, Vec<u8>>> = RefCell::new(HashMap::new());
    /// Mime types for stored binaries.
    static BINARY_MIME: RefCell<HashMap<String, String>> = RefCell::new(HashMap::new());
}

// ============================================================
// WASM EXPORTS
// ============================================================

#[wasm_bindgen]
pub fn get_tool_registry() -> String {
    REGISTRY.with(|r| serde_json::to_string(&r.manifest()).expect("Failed to serialize manifest"))
}

/// Store raw binary data before pipeline execution.
/// Called by the worker for each binary provided input.
#[wasm_bindgen]
pub fn set_binary_input(key: &str, data: &[u8], mime_type: &str) {
    BINARY_STORE.with(|store| {
        store.borrow_mut().insert(key.to_string(), data.to_vec());
    });
    BINARY_MIME.with(|mime| {
        mime.borrow_mut().insert(key.to_string(), mime_type.to_string());
    });
}

/// Retrieve binary output after pipeline execution.
/// Returns the raw bytes for the given key.
#[wasm_bindgen]
pub fn get_binary_output(key: &str) -> Option<Vec<u8>> {
    BINARY_STORE.with(|store| {
        store.borrow().get(key).cloned()
    })
}

/// Get the mime type of a binary output.
#[wasm_bindgen]
pub fn get_binary_output_mime(key: &str) -> Option<String> {
    BINARY_MIME.with(|mime| {
        mime.borrow().get(key).cloned()
    })
}

/// Get all binary output keys (JSON array of strings).
#[wasm_bindgen]
pub fn get_binary_output_keys() -> String {
    BINARY_STORE.with(|store| {
        let binding = store.borrow();
        let keys: Vec<&String> = binding.keys().collect();
        serde_json::to_string(&keys).unwrap_or_else(|_| "[]".to_string())
    })
}

/// Clear all binary data after pipeline execution.
#[wasm_bindgen]
pub fn clear_binary_store() {
    BINARY_STORE.with(|store| store.borrow_mut().clear());
    BINARY_MIME.with(|mime| mime.borrow_mut().clear());
}

/// Execute a pipeline with real-time progress reporting.
/// `progress_callback` receives a JSON string for each node event:
///   { "event": "node_started", "node_id": "..." }
///   { "event": "node_completed", "node_id": "...", "result": { ... } }
#[wasm_bindgen]
pub fn run_pipeline(pipeline_json: &str, progress_callback: &js_sys::Function) -> Result<String, JsValue> {
    let pipeline: PipelineDescription = serde_json::from_str(pipeline_json)
        .map_err(|e| JsValue::from_str(&format!("Pipeline parse error: {e}")))?;

    let result = REGISTRY.with(|registry| {
        execute_pipeline(&pipeline, registry, progress_callback)
    });

    match result {
        Ok(r) => serde_json::to_string(&r)
            .map_err(|e| JsValue::from_str(&format!("Serialize error: {e}"))),
        Err(e) => Err(JsValue::from_str(&e)),
    }
}

// ============================================================
// PIPELINE EXECUTOR
// ============================================================

fn emit_progress(callback: &js_sys::Function, json: &str) {
    let _ = callback.call1(&JsValue::NULL, &JsValue::from_str(json));
}

fn execute_pipeline(
    pipeline: &PipelineDescription,
    registry: &ToolRegistry,
    progress: &js_sys::Function,
) -> Result<PipelineResult, String> {
    let sorted = topological_sort(pipeline)?;

    let mut data_store: HashMap<String, DataPayload> = HashMap::new();
    let mut node_results: HashMap<String, NodeResult> = HashMap::new();

    for node_id in &sorted {
        let node = pipeline.nodes.get(node_id)
            .ok_or_else(|| format!("Node '{node_id}' not found"))?;

        let tool_id = &node.tool_id;
        let handler = registry.get_handler(tool_id)
            .ok_or_else(|| format!("Unknown tool: {tool_id}"))?;

        // Report: node started
        emit_progress(progress, &format!(
            r#"{{"event":"node_started","node_id":"{node_id}"}}"#
        ));

        let start = js_sys::Date::now();
        let inputs = resolve_node_inputs(node_id, handler, pipeline, &data_store)?;
        let mut ctx = ExecutionContext::new(inputs, node.params.clone());

        match handler.execute(&mut ctx) {
            Ok(()) => {
                let duration_ms = js_sys::Date::now() - start;
                let outputs = ctx.into_outputs();

                let mut preview = None;
                let mut output_rows = None;
                if let Some((_, payload)) = outputs.iter().next() {
                    preview = Some(payload.preview(5));
                    output_rows = payload.row_count();
                }

                for (port_name, payload) in outputs {
                    data_store.insert(format!("{node_id}:{port_name}"), payload);
                }

                let node_result = NodeResult {
                    status: "success".into(),
                    duration_ms: Some(duration_ms),
                    output_rows,
                    error: None,
                    preview,
                };

                // Report: node completed
                if let Ok(result_json) = serde_json::to_string(&node_result) {
                    emit_progress(progress, &format!(
                        r#"{{"event":"node_completed","node_id":"{node_id}","result":{result_json}}}"#
                    ));
                }

                node_results.insert(node_id.clone(), node_result);
            }
            Err(e) => {
                let duration_ms = js_sys::Date::now() - start;

                let node_result = NodeResult {
                    status: "error".into(),
                    duration_ms: Some(duration_ms),
                    output_rows: None,
                    error: Some(e.clone()),
                    preview: None,
                };

                // Report: node failed
                if let Ok(result_json) = serde_json::to_string(&node_result) {
                    emit_progress(progress, &format!(
                        r#"{{"event":"node_completed","node_id":"{node_id}","result":{result_json}}}"#
                    ));
                }

                node_results.insert(node_id.clone(), node_result);

                let failed_idx = sorted.iter().position(|id| id == node_id).unwrap();
                for remaining_id in sorted.iter().skip(failed_idx + 1) {
                    node_results.insert(remaining_id.clone(), NodeResult {
                        status: "skipped".into(),
                        duration_ms: None,
                        output_rows: None,
                        error: None,
                        preview: None,
                    });
                }

                return Ok(PipelineResult {
                    status: "error".into(),
                    node_results,
                    terminal_outputs: HashMap::new(),
                    failed_node: Some(node_id.clone()),
                    error: Some(e),
                });
            }
        }
    }

    // Collect terminal outputs
    let nodes_with_outgoing: std::collections::HashSet<String> =
        pipeline.edges.iter().map(|e| e.from.node.clone()).collect();

    let mut terminal_outputs: HashMap<String, String> = HashMap::new();
    let terminal_keys: Vec<String> = data_store.keys()
        .filter(|key| {
            let nid = key.split(':').next().unwrap();
            !nodes_with_outgoing.contains(nid)
        })
        .cloned()
        .collect();

    for key in terminal_keys {
        if let Some(payload) = data_store.remove(&key) {
            match payload {
                DataPayload::Bytes { data, mime_type } => {
                    // Store binary output in the binary store for the worker to retrieve
                    // via get_binary_output(). No base64 encoding needed.
                    let output_key = format!("__output__:{key}");
                    BINARY_STORE.with(|store| {
                        store.borrow_mut().insert(output_key.clone(), data);
                    });
                    BINARY_MIME.with(|mime| {
                        mime.borrow_mut().insert(output_key.clone(), mime_type);
                    });
                    // Mark as binary in terminal_outputs (worker checks for this prefix)
                    terminal_outputs.insert(key, "__binary__".to_string());
                }
                _ => {
                    if let Ok(text) = payload.as_text() {
                        terminal_outputs.insert(key, text);
                    }
                }
            }
        }
    }

    Ok(PipelineResult {
        status: "success".into(),
        node_results,
        terminal_outputs,
        failed_node: None,
        error: None,
    })
}

// ============================================================
// INPUT RESOLUTION
// ============================================================

fn resolve_node_inputs(
    node_id: &str,
    handler: &dyn tooldeck_registry::ToolHandler,
    pipeline: &PipelineDescription,
    data_store: &HashMap<String, DataPayload>,
) -> Result<HashMap<String, DataPayload>, String> {
    let spec = handler.spec();
    let mut inputs: HashMap<String, DataPayload> = HashMap::new();

    for input_port in &spec.inputs {
        let incoming = pipeline.edges.iter().find(|e| {
            e.to.node == node_id && e.to.port == input_port.name
        });

        if let Some(edge) = incoming {
            let from_node = &edge.from.node;
            let from_port = &edge.from.port;
            let port_name = &input_port.name;
            let key = format!("{from_node}:{from_port}");
            let payload = data_store.get(&key)
                .ok_or_else(|| format!("No data at {key} for input {node_id}:{port_name}"))?;

            let cloned = match payload {
                DataPayload::Text { content, format } => DataPayload::Text { content: content.clone(), format: *format },
                DataPayload::Arrow { batch, source_format } => DataPayload::Arrow { batch: batch.clone(), source_format: *source_format },
                DataPayload::Bytes { data, mime_type } => DataPayload::Bytes { data: data.clone(), mime_type: mime_type.clone() },
            };
            inputs.insert(input_port.name.clone(), cloned);
        } else {
            let port_name = &input_port.name;

            // Check for single provided input (JSON or binary store)
            let single_key = format!("{node_id}:{port_name}");
            let has_binary = BINARY_STORE.with(|s| s.borrow().contains_key(&single_key));

            if has_binary {
                // Read directly from binary store
                let data = BINARY_STORE.with(|s| s.borrow().get(&single_key).cloned().unwrap());
                let mime = BINARY_MIME.with(|m| {
                    m.borrow().get(&single_key).cloned()
                        .unwrap_or_else(|| "application/octet-stream".to_string())
                });
                inputs.insert(input_port.name.clone(), DataPayload::Bytes { data, mime_type: mime });
            } else if let Some(input) = pipeline.provided_inputs.get(&single_key) {
                let payload = resolve_provided_input(input, node_id, port_name)?;
                inputs.insert(input_port.name.clone(), payload);
            }

            // Check for indexed inputs (multi-file: "nodeId:port:0", "nodeId:port:1", ...)
            let mut idx = 0;
            loop {
                let indexed_key = format!("{node_id}:{port_name}:{idx}");
                let has_indexed_binary = BINARY_STORE.with(|s| s.borrow().contains_key(&indexed_key));

                if has_indexed_binary {
                    let data = BINARY_STORE.with(|s| s.borrow().get(&indexed_key).cloned().unwrap());
                    let mime = BINARY_MIME.with(|m| {
                        m.borrow().get(&indexed_key).cloned()
                            .unwrap_or_else(|| "application/octet-stream".to_string())
                    });
                    inputs.insert(format!("{}:{}", input_port.name, idx), DataPayload::Bytes { data, mime_type: mime });
                    idx += 1;
                } else if let Some(input) = pipeline.provided_inputs.get(&indexed_key) {
                    let payload = resolve_provided_input(input, node_id, port_name)?;
                    inputs.insert(format!("{}:{}", input_port.name, idx), payload);
                    idx += 1;
                } else {
                    break;
                }
            }
        }
    }

    Ok(inputs)
}

fn resolve_provided_input(
    input: &tooldeck_registry::ProvidedInput,
    node_id: &str,
    port_name: &str,
) -> Result<DataPayload, String> {
    match input {
        tooldeck_registry::ProvidedInput::Text { content, format } => {
            Ok(DataPayload::text(content.clone(), *format))
        }
        tooldeck_registry::ProvidedInput::Binary { mime_type } => {
            // Binary data is in the binary store (set by the worker before execution)
            let store_key = format!("{node_id}:{port_name}");
            let data = BINARY_STORE.with(|s| s.borrow().get(&store_key).cloned())
                .ok_or_else(|| format!("Binary data not found for {node_id}:{port_name}"))?;
            Ok(DataPayload::Bytes { data, mime_type: mime_type.clone() })
        }
    }
}

// ============================================================
// TOPOLOGICAL SORT
// ============================================================

fn topological_sort(pipeline: &PipelineDescription) -> Result<Vec<String>, String> {
    let mut in_degree: HashMap<String, usize> = HashMap::new();
    let mut adjacency: HashMap<String, Vec<String>> = HashMap::new();

    for node_id in pipeline.nodes.keys() {
        in_degree.insert(node_id.clone(), 0);
        adjacency.insert(node_id.clone(), vec![]);
    }

    for edge in &pipeline.edges {
        let src = &edge.from.node;
        let tgt = &edge.to.node;
        adjacency.get_mut(src)
            .ok_or_else(|| format!("Unknown source node: {src}"))?
            .push(tgt.clone());
        *in_degree.get_mut(tgt)
            .ok_or_else(|| format!("Unknown target node: {tgt}"))? += 1;
    }

    let mut queue: Vec<String> = in_degree.iter()
        .filter(|(_, &deg)| deg == 0)
        .map(|(id, _)| id.clone())
        .collect();

    let mut sorted: Vec<String> = vec![];

    while let Some(id) = queue.pop() {
        sorted.push(id.clone());
        for next in adjacency.get(&id).unwrap_or(&vec![]) {
            let deg = in_degree.get_mut(next).unwrap();
            *deg -= 1;
            if *deg == 0 {
                queue.push(next.clone());
            }
        }
    }

    if sorted.len() != pipeline.nodes.len() {
        let cycle_nodes: Vec<String> = pipeline.nodes.keys()
            .filter(|id| !sorted.contains(id))
            .cloned()
            .collect();
        return Err(format!("Cycle detected: {}", cycle_nodes.join(", ")));
    }

    Ok(sorted)
}
