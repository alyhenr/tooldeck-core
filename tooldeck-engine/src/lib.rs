use wasm_bindgen::prelude::*;
use std::collections::HashMap;
use tooldeck_registry::{
    DataPayload, ExecutionContext, ToolRegistry,
    PipelineDescription, PipelineResult, NodeResult,
};

// ============================================================
// GLOBAL REGISTRY — initialized once, used for all calls
// ============================================================

fn build_registry() -> ToolRegistry {
    let mut registry = ToolRegistry::new();
    // Register all tool crates. Adding a new crate = one line here.
    tooldeck_json::register(&mut registry);
    registry
}

thread_local! {
    static REGISTRY: ToolRegistry = build_registry();
}

// ============================================================
// WASM EXPORTS
// ============================================================

#[wasm_bindgen]
pub fn get_tool_registry() -> String {
    REGISTRY.with(|r| serde_json::to_string(&r.manifest()).unwrap())
}

#[wasm_bindgen]
pub fn run_pipeline(pipeline_json: &str) -> Result<String, JsValue> {
    let pipeline: PipelineDescription = serde_json::from_str(pipeline_json)
        .map_err(|e| JsValue::from_str(&format!("Pipeline parse error: {e}")))?;

    let result = REGISTRY.with(|registry| {
        execute_pipeline(&pipeline, registry)
    });

    match result {
        Ok(r) => serde_json::to_string(&r)
            .map_err(|e| JsValue::from_str(&format!("Serialize error: {e}"))),
        Err(e) => Err(JsValue::from_str(&e)),
    }
}

// ============================================================
// PIPELINE EXECUTOR — generic, no tool-specific code
// ============================================================

fn execute_pipeline(
    pipeline: &PipelineDescription,
    registry: &ToolRegistry,
) -> Result<PipelineResult, String> {
    let sorted = topological_sort(pipeline)?;

    // Data store: node_id:port_name → DataPayload
    let mut data_store: HashMap<String, DataPayload> = HashMap::new();
    let mut node_results: HashMap<String, NodeResult> = HashMap::new();

    for node_id in &sorted {
        let node = pipeline.nodes.get(node_id)
            .ok_or_else(|| format!("Node '{node_id}' not found"))?;

        let tool_id = &node.tool_id;
        let handler = registry.get_handler(tool_id)
            .ok_or_else(|| format!("Unknown tool: {tool_id}"))?;

        let start = js_sys::Date::now();

        // Resolve inputs for this node
        let inputs = resolve_node_inputs(node_id, handler, pipeline, &data_store)?;

        // Build execution context
        let mut ctx = ExecutionContext::new(inputs, node.params.clone());

        // Execute the tool
        match handler.execute(&mut ctx) {
            Ok(()) => {
                let duration_ms = js_sys::Date::now() - start;
                let outputs = ctx.into_outputs();

                // Get preview + row count from first output
                let mut preview = None;
                let mut output_rows = None;
                if let Some((_, payload)) = outputs.iter().next() {
                    preview = Some(payload.preview(5));
                    output_rows = payload.row_count();
                }

                // Store all outputs in the data store
                for (port_name, payload) in outputs {
                    data_store.insert(format!("{node_id}:{port_name}"), payload);
                }

                node_results.insert(node_id.clone(), NodeResult {
                    status: "success".into(),
                    duration_ms: Some(duration_ms),
                    output_rows,
                    error: None,
                    preview,
                });
            }
            Err(e) => {
                let duration_ms = js_sys::Date::now() - start;
                node_results.insert(node_id.clone(), NodeResult {
                    status: "error".into(),
                    duration_ms: Some(duration_ms),
                    output_rows: None,
                    error: Some(e.clone()),
                    preview: None,
                });

                // Mark remaining nodes as skipped
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

    // Collect terminal outputs — nodes whose ports have no outgoing edge
    let nodes_with_outgoing: std::collections::HashSet<String> =
        pipeline.edges.iter().map(|e| e.from.node.clone()).collect();

    let mut terminal_outputs: HashMap<String, String> = HashMap::new();
    let terminal_keys: Vec<String> = data_store.keys()
        .filter(|key| {
            let node_id = key.split(':').next().unwrap();
            !nodes_with_outgoing.contains(node_id)
        })
        .cloned()
        .collect();

    for key in terminal_keys {
        if let Some(payload) = data_store.remove(&key) {
            if let Ok(text) = payload.as_text() {
                terminal_outputs.insert(key, text);
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
// INPUT RESOLUTION — generic, follows edges
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
        // Find the edge connecting to this port
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

            // Clone the payload for the tool to consume
            let cloned = match payload {
                DataPayload::Text(t) => DataPayload::Text(t.clone()),
                DataPayload::Arrow(b) => DataPayload::Arrow(b.clone()),
            };
            inputs.insert(input_port.name.clone(), cloned);
        } else {
            let port_name = &input_port.name;
            let key = format!("{node_id}:{port_name}");
            if let Some(text) = pipeline.provided_inputs.get(&key) {
                inputs.insert(input_port.name.clone(), DataPayload::Text(text.clone()));
            }
            // If no data at all, the tool will get an error when it tries to read the input
        }
    }

    Ok(inputs)
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
