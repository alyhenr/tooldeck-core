use arrow::array::{ArrayRef, BooleanArray};
use arrow::compute;
use arrow::record_batch::RecordBatch;
use arrow_schema::{Field, Schema};
use std::collections::HashSet;
use std::sync::Arc;
use tooldeck_registry::{
    cell_to_string, ExecutionContext, ToolHandler, ToolRegistry, ToolSpec,
    port_with_format, string_param, string_array_param, select_param,
};

// ============================================================
// SELECT COLUMNS — keep only specified columns
// ============================================================

pub fn select_columns(batch: &RecordBatch, columns: &[&str]) -> Result<RecordBatch, String> {
    let indices: Vec<usize> = columns
        .iter()
        .map(|name| {
            batch.schema().index_of(name)
                .map_err(|_| format!("Column '{name}' not found. Available: {}",
                    batch.schema().fields().iter().map(|f| f.name().as_str()).collect::<Vec<_>>().join(", ")))
        })
        .collect::<Result<Vec<_>, _>>()?;

    batch.project(&indices)
        .map_err(|e| format!("Failed to project columns: {e}"))
}

pub struct SelectColumns;

impl ToolHandler for SelectColumns {
    fn spec(&self) -> ToolSpec {
        ToolSpec {
            id: "select_columns".into(),
            label: "Select Columns".into(),
            description: "Keep only specified columns".into(),
            category: "data".into(),
            icon: "Columns3".into(),
            inputs: vec![port_with_format("data", "Text", "tabular")],
            outputs: vec![port_with_format("result", "Text", "tabular")],
            params: vec![string_array_param("columns", "Columns to keep")],
        }
    }

    fn execute(&self, ctx: &mut ExecutionContext) -> Result<(), String> {
        let batch = ctx.input_arrow("data")?;
        let columns = ctx.param_str_array("columns")?;
        let result = select_columns(&batch, &columns)?;
        ctx.set_output_arrow("result", result);
        Ok(())
    }
}

// ============================================================
// RENAME COLUMNS — rename columns by mapping
// ============================================================

pub fn rename_columns(
    batch: &RecordBatch,
    from: &str,
    to: &str,
) -> Result<RecordBatch, String> {
    let schema = batch.schema();
    let new_fields: Vec<Arc<Field>> = schema
        .fields()
        .iter()
        .map(|f| {
            if f.name() == from {
                Arc::new(Field::new(to, f.data_type().clone(), f.is_nullable()))
            } else {
                f.clone()
            }
        })
        .collect();

    let new_schema = Arc::new(Schema::new(new_fields));
    RecordBatch::try_new(new_schema, batch.columns().to_vec())
        .map_err(|e| format!("Failed to rename column: {e}"))
}

pub struct RenameColumn;

impl ToolHandler for RenameColumn {
    fn spec(&self) -> ToolSpec {
        ToolSpec {
            id: "rename_column".into(),
            label: "Rename Column".into(),
            description: "Rename a column in the table".into(),
            category: "data".into(),
            icon: "TextCursorInput".into(),
            inputs: vec![port_with_format("data", "Text", "tabular")],
            outputs: vec![port_with_format("result", "Text", "tabular")],
            params: vec![
                string_param("from", "Current Name"),
                string_param("to", "New Name"),
            ],
        }
    }

    fn execute(&self, ctx: &mut ExecutionContext) -> Result<(), String> {
        let batch = ctx.input_arrow("data")?;
        let from = ctx.param_str("from")?;
        let to = ctx.param_str("to")?;
        let result = rename_columns(&batch, from, to)?;
        ctx.set_output_arrow("result", result);
        Ok(())
    }
}

// ============================================================
// SORT ROWS — sort by a column
// ============================================================

pub fn sort_rows(
    batch: &RecordBatch,
    col_name: &str,
    ascending: bool,
) -> Result<RecordBatch, String> {
    let col_idx = batch.schema().index_of(col_name)
        .map_err(|_| format!("Column '{col_name}' not found"))?;

    let sort_options = arrow::compute::SortOptions {
        descending: !ascending,
        nulls_first: false,
    };

    let indices = compute::sort_to_indices(
        batch.column(col_idx),
        Some(sort_options),
        None,
    ).map_err(|e| format!("Failed to sort: {e}"))?;

    let columns: Vec<ArrayRef> = batch
        .columns()
        .iter()
        .map(|col| compute::take(col.as_ref(), &indices, None))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| format!("Failed to reorder rows: {e}"))?;

    RecordBatch::try_new(batch.schema(), columns)
        .map_err(|e| format!("Failed to rebuild batch: {e}"))
}

pub struct SortRows;

impl ToolHandler for SortRows {
    fn spec(&self) -> ToolSpec {
        ToolSpec {
            id: "sort_rows".into(),
            label: "Sort Rows".into(),
            description: "Sort rows by a column".into(),
            category: "data".into(),
            icon: "ArrowUpDown".into(),
            inputs: vec![port_with_format("data", "Text", "tabular")],
            outputs: vec![port_with_format("result", "Text", "tabular")],
            params: vec![
                string_param("column", "Sort by Column"),
                string_param("direction", "Direction (asc/desc)"),
            ],
        }
    }

    fn execute(&self, ctx: &mut ExecutionContext) -> Result<(), String> {
        let batch = ctx.input_arrow("data")?;
        let column = ctx.param_str("column")?;
        let direction = ctx.param_str("direction").unwrap_or("asc");
        let ascending = direction != "desc";
        let result = sort_rows(&batch, column, ascending)?;
        ctx.set_output_arrow("result", result);
        Ok(())
    }
}

// ============================================================
// DEDUPLICATE — remove duplicate rows based on key columns
// ============================================================

pub fn deduplicate(
    batch: &RecordBatch,
    key_columns: &[&str],
) -> Result<RecordBatch, String> {
    if batch.num_rows() == 0 {
        return Ok(batch.clone());
    }

    // Build a composite key for each row by concatenating string representations
    let key_indices: Vec<usize> = key_columns
        .iter()
        .map(|name| {
            batch.schema().index_of(name)
                .map_err(|_| format!("Column '{name}' not found"))
        })
        .collect::<Result<Vec<_>, _>>()?;

    let mut seen = HashSet::new();
    let mut keep = Vec::with_capacity(batch.num_rows());

    for row in 0..batch.num_rows() {
        let mut key = String::new();
        for &col_idx in &key_indices {
            let col = batch.column(col_idx);
            key.push_str(&cell_to_string(col.as_ref(), row));
            key.push('\0');
        }

        if seen.insert(key) {
            keep.push(true);
        } else {
            keep.push(false);
        }
    }

    let mask = BooleanArray::from(keep);
    compute::filter_record_batch(batch, &mask)
        .map_err(|e| format!("Failed to filter duplicates: {e}"))
}

pub struct Deduplicate;

impl ToolHandler for Deduplicate {
    fn spec(&self) -> ToolSpec {
        ToolSpec {
            id: "deduplicate".into(),
            label: "Deduplicate".into(),
            description: "Remove duplicate rows by key columns".into(),
            category: "data".into(),
            icon: "CopyMinus".into(),
            inputs: vec![port_with_format("data", "Text", "tabular")],
            outputs: vec![port_with_format("result", "Text", "tabular")],
            params: vec![string_array_param("key_columns", "Key Columns")],
        }
    }

    fn execute(&self, ctx: &mut ExecutionContext) -> Result<(), String> {
        let batch = ctx.input_arrow("data")?;
        let keys = ctx.param_str_array("key_columns")?;
        let result = deduplicate(&batch, &keys)?;
        ctx.set_output_arrow("result", result);
        Ok(())
    }
}

// ============================================================
// GROUP BY — aggregate rows by key columns
// ============================================================

pub fn group_by(
    batch: &RecordBatch,
    group_cols: &[&str],
    aggregations: &[&str],
) -> Result<RecordBatch, String> {
    if batch.num_rows() == 0 {
        return Ok(batch.clone());
    }

    let group_indices: Vec<usize> = group_cols
        .iter()
        .map(|name| batch.schema().index_of(name).map_err(|_| format!("Column '{name}' not found")))
        .collect::<Result<Vec<_>, _>>()?;

    // Parse aggregations: "column:operation"
    let parsed_aggs: Vec<(&str, &str, usize)> = aggregations
        .iter()
        .map(|s| {
            let parts: Vec<&str> = s.splitn(2, ':').collect();
            if parts.len() != 2 {
                return Err(format!("Invalid aggregation '{s}'. Use format 'column:operation' (e.g. 'amount:sum')"));
            }
            let col_idx = batch.schema().index_of(parts[0])
                .map_err(|_| format!("Aggregation column '{}' not found", parts[0]))?;
            Ok((parts[0], parts[1], col_idx))
        })
        .collect::<Result<Vec<_>, _>>()?;

    // Build groups using composite key
    let mut groups: Vec<(String, Vec<usize>)> = Vec::new();
    let mut group_map: std::collections::HashMap<String, usize> = std::collections::HashMap::new();

    for row in 0..batch.num_rows() {
        let mut key = String::new();
        for &col_idx in &group_indices {
            key.push_str(&cell_to_string(batch.column(col_idx).as_ref(), row));
            key.push('\0');
        }
        if let Some(&idx) = group_map.get(&key) {
            groups[idx].1.push(row);
        } else {
            let idx = groups.len();
            group_map.insert(key.clone(), idx);
            groups.push((key, vec![row]));
        }
    }

    // Build output schema: group columns + aggregation columns
    let mut fields: Vec<Arc<Field>> = group_indices
        .iter()
        .map(|&i| batch.schema().field(i).clone().into())
        .collect();
    for &(col_name, op, _) in &parsed_aggs {
        fields.push(Arc::new(Field::new(format!("{col_name}_{op}"), arrow_schema::DataType::Utf8, true)));
    }
    let out_schema = Arc::new(Schema::new(fields));

    // Build output columns
    let mut columns: Vec<ArrayRef> = Vec::new();

    // Group key columns — take first row of each group
    for &col_idx in &group_indices {
        let col = batch.column(col_idx);
        let mut builder = arrow::array::StringBuilder::new();
        for (_, rows) in &groups {
            builder.append_value(cell_to_string(col.as_ref(), rows[0]));
        }
        columns.push(Arc::new(builder.finish()));
    }

    // Aggregation columns
    for &(_, op, col_idx) in &parsed_aggs {
        let col = batch.column(col_idx);
        let mut builder = arrow::array::StringBuilder::new();

        for (_, rows) in &groups {
            let values: Vec<f64> = rows
                .iter()
                .filter_map(|&r| cell_to_string(col.as_ref(), r).parse::<f64>().ok())
                .collect();

            let result = match op {
                "count" => rows.len() as f64,
                "sum" => values.iter().sum(),
                "avg" => {
                    if values.is_empty() { 0.0 }
                    else { values.iter().sum::<f64>() / values.len() as f64 }
                }
                "min" => values.iter().cloned().fold(f64::INFINITY, f64::min),
                "max" => values.iter().cloned().fold(f64::NEG_INFINITY, f64::max),
                _ => return Err(format!("Unknown operation '{op}'. Use: count, sum, avg, min, max")),
            };

            if op == "count" || result == result.floor() {
                builder.append_value(format!("{}", result as i64));
            } else {
                builder.append_value(format!("{result:.2}"));
            }
        }
        columns.push(Arc::new(builder.finish()));
    }

    RecordBatch::try_new(out_schema, columns)
        .map_err(|e| format!("Failed to build grouped result: {e}"))
}

pub struct GroupBy;

impl ToolHandler for GroupBy {
    fn spec(&self) -> ToolSpec {
        ToolSpec {
            id: "group_by".into(),
            label: "Group By".into(),
            description: "Aggregate rows by key columns (sum, count, avg, min, max)".into(),
            category: "data".into(),
            icon: "Group".into(),
            inputs: vec![port_with_format("data", "Text", "tabular")],
            outputs: vec![port_with_format("result", "Text", "tabular")],
            params: vec![
                string_array_param("group_by", "Group by Columns"),
                string_array_param("aggregations", "Aggregations (column:operation)"),
            ],
        }
    }

    fn execute(&self, ctx: &mut ExecutionContext) -> Result<(), String> {
        let batch = ctx.input_arrow("data")?;
        let group_cols = ctx.param_str_array("group_by")?;
        let aggregations = ctx.param_str_array("aggregations")?;
        let result = group_by(&batch, &group_cols, &aggregations)?;
        ctx.set_output_arrow("result", result);
        Ok(())
    }
}

// ============================================================
// ADD COLUMN — compute a new column from an expression
// ============================================================

pub fn add_column(
    batch: &RecordBatch,
    column_name: &str,
    expression: &str,
) -> Result<RecordBatch, String> {
    if batch.num_rows() == 0 {
        let mut fields: Vec<Arc<Field>> = batch.schema().fields().iter().cloned().collect();
        fields.push(Arc::new(Field::new(column_name, arrow_schema::DataType::Utf8, true)));
        let schema = Arc::new(Schema::new(fields));
        return Ok(RecordBatch::new_empty(schema));
    }

    // Evaluate expression for each row
    let mut results = arrow::array::StringBuilder::new();
    for row in 0..batch.num_rows() {
        let value = eval_expression(batch, row, expression)?;
        results.append_value(value);
    }

    let mut columns: Vec<ArrayRef> = batch.columns().to_vec();
    columns.push(Arc::new(results.finish()));

    let mut fields: Vec<Arc<Field>> = batch.schema().fields().iter().cloned().collect();
    fields.push(Arc::new(Field::new(column_name, arrow_schema::DataType::Utf8, true)));
    let schema = Arc::new(Schema::new(fields));

    RecordBatch::try_new(schema, columns)
        .map_err(|e| format!("Failed to add column: {e}"))
}

/// Simple expression evaluator supporting column references, arithmetic, and string concat.
/// Examples: "price * quantity", "first_name + ' ' + last_name", "amount / 100"
fn eval_expression(batch: &RecordBatch, row: usize, expr: &str) -> Result<String, String> {
    let tokens = tokenize_expression(expr);
    if tokens.is_empty() {
        return Err("Empty expression".into());
    }

    // Try numeric evaluation first
    let mut numeric = true;
    let mut values: Vec<f64> = Vec::new();
    let mut ops: Vec<char> = Vec::new();

    for token in &tokens {
        match token {
            Token::Op(c) => ops.push(*c),
            Token::Literal(s) => {
                if let Ok(n) = s.parse::<f64>() {
                    values.push(n);
                } else {
                    numeric = false;
                    break;
                }
            }
            Token::Column(name) => {
                let val = get_cell_value(batch, row, name)?;
                if let Ok(n) = val.parse::<f64>() {
                    values.push(n);
                } else {
                    numeric = false;
                    break;
                }
            }
        }
    }

    if numeric && !values.is_empty() {
        // Evaluate left to right with precedence: * / before + -
        let mut result = values[0];
        for (i, &op) in ops.iter().enumerate() {
            let rhs = values.get(i + 1).copied().unwrap_or(0.0);
            match op {
                '+' => result += rhs,
                '-' => result -= rhs,
                '*' => result *= rhs,
                '/' => {
                    if rhs == 0.0 { return Ok("0".into()); }
                    result /= rhs;
                }
                _ => return Err(format!("Unknown operator '{op}'")),
            }
        }
        if result == result.floor() && result.abs() < 1e15 {
            return Ok(format!("{}", result as i64));
        }
        return Ok(format!("{result:.2}"));
    }

    // Fall back to string concatenation
    let mut result = String::new();
    for token in &tokens {
        match token {
            Token::Op('+') => {} // string concat is implicit
            Token::Op(c) => return Err(format!("Cannot use '{c}' on text values")),
            Token::Literal(s) => result.push_str(s),
            Token::Column(name) => {
                let val = get_cell_value(batch, row, name)?;
                result.push_str(&val);
            }
        }
    }
    Ok(result)
}

#[derive(Debug)]
enum Token {
    Column(String),
    Literal(String),
    Op(char),
}

fn tokenize_expression(expr: &str) -> Vec<Token> {
    let mut tokens = Vec::new();
    let mut chars = expr.chars().peekable();

    while let Some(&c) = chars.peek() {
        if c.is_whitespace() {
            chars.next();
            continue;
        }
        if "+-*/".contains(c) {
            tokens.push(Token::Op(c));
            chars.next();
        } else if c == '\'' || c == '"' {
            chars.next(); // skip opening quote
            let mut s = String::new();
            while let Some(&ch) = chars.peek() {
                if ch == c { chars.next(); break; }
                s.push(ch);
                chars.next();
            }
            tokens.push(Token::Literal(s));
        } else {
            let mut word = String::new();
            while let Some(&ch) = chars.peek() {
                if ch.is_whitespace() || "+-*/".contains(ch) { break; }
                word.push(ch);
                chars.next();
            }
            if word.parse::<f64>().is_ok() {
                tokens.push(Token::Literal(word));
            } else {
                tokens.push(Token::Column(word));
            }
        }
    }
    tokens
}

fn get_cell_value(batch: &RecordBatch, row: usize, col_name: &str) -> Result<String, String> {
    let col_idx = batch.schema().index_of(col_name)
        .map_err(|_| format!("Column '{col_name}' not found in expression"))?;
    Ok(cell_to_string(batch.column(col_idx).as_ref(), row))
}

pub struct AddColumn;

impl ToolHandler for AddColumn {
    fn spec(&self) -> ToolSpec {
        ToolSpec {
            id: "add_column".into(),
            label: "Add Column".into(),
            description: "Compute a new column from an expression".into(),
            category: "data".into(),
            icon: "Plus".into(),
            inputs: vec![port_with_format("data", "Text", "tabular")],
            outputs: vec![port_with_format("result", "Text", "tabular")],
            params: vec![
                string_param("column_name", "New Column Name"),
                string_param("expression", "Expression"),
            ],
        }
    }

    fn execute(&self, ctx: &mut ExecutionContext) -> Result<(), String> {
        let batch = ctx.input_arrow("data")?;
        let column_name = ctx.param_str("column_name")?;
        let expression = ctx.param_str("expression")?;
        let result = add_column(&batch, column_name, expression)?;
        ctx.set_output_arrow("result", result);
        Ok(())
    }
}

// ============================================================
// TEXT REPLACE — find and replace within column values
// ============================================================

pub fn text_replace(
    batch: &RecordBatch,
    col_name: &str,
    find: &str,
    replace: &str,
) -> Result<RecordBatch, String> {
    let col_idx = batch.schema().index_of(col_name)
        .map_err(|_| format!("Column '{col_name}' not found"))?;

    let col = batch.column(col_idx);
    let mut builder = arrow::array::StringBuilder::new();
    for row in 0..batch.num_rows() {
        let val = cell_to_string(col.as_ref(), row);
        builder.append_value(val.replace(find, replace));
    }

    let new_col: ArrayRef = Arc::new(builder.finish());
    let mut columns: Vec<ArrayRef> = batch.columns().to_vec();
    columns[col_idx] = new_col;

    // Update schema to ensure the column is Utf8
    let mut fields: Vec<Arc<Field>> = batch.schema().fields().iter().cloned().collect();
    fields[col_idx] = Arc::new(Field::new(col_name, arrow_schema::DataType::Utf8, true));
    let schema = Arc::new(Schema::new(fields));

    RecordBatch::try_new(schema, columns)
        .map_err(|e| format!("Failed to replace text: {e}"))
}

pub struct TextReplace;

impl ToolHandler for TextReplace {
    fn spec(&self) -> ToolSpec {
        ToolSpec {
            id: "text_replace".into(),
            label: "Text Replace".into(),
            description: "Find and replace text within a column".into(),
            category: "data".into(),
            icon: "Replace".into(),
            inputs: vec![port_with_format("data", "Text", "tabular")],
            outputs: vec![port_with_format("result", "Text", "tabular")],
            params: vec![
                string_param("column", "Column"),
                string_param("find", "Find"),
                string_param("replace", "Replace with"),
            ],
        }
    }

    fn execute(&self, ctx: &mut ExecutionContext) -> Result<(), String> {
        let batch = ctx.input_arrow("data")?;
        let column = ctx.param_str("column")?;
        let find = ctx.param_str("find")?;
        let replace = ctx.param_str("replace").unwrap_or("");
        let result = text_replace(&batch, column, find, replace)?;
        ctx.set_output_arrow("result", result);
        Ok(())
    }
}

// ============================================================
// JOIN TABLES — SQL-style join on key columns (Pro)
// ============================================================

pub fn join_tables(
    left: &RecordBatch,
    right: &RecordBatch,
    left_key: &str,
    right_key: &str,
    join_type: &str,
) -> Result<RecordBatch, String> {
    let left_key_idx = left.schema().index_of(left_key)
        .map_err(|_| format!("Left key column '{left_key}' not found"))?;
    let right_key_idx = right.schema().index_of(right_key)
        .map_err(|_| format!("Right key column '{right_key}' not found"))?;

    // Build index of right table by key value
    let mut right_index: std::collections::HashMap<String, Vec<usize>> = std::collections::HashMap::new();
    for row in 0..right.num_rows() {
        let key = cell_to_string(right.column(right_key_idx).as_ref(), row);
        right_index.entry(key).or_default().push(row);
    }

    // Determine which right columns to include (all except the join key to avoid duplication)
    let right_col_indices: Vec<usize> = (0..right.num_columns())
        .filter(|&i| i != right_key_idx)
        .collect();

    // Build output schema: all left columns + right columns (minus right key)
    // All columns become Utf8 since we use string builders for the join output
    let right_schema = right.schema();
    let left_schema = left.schema();
    let mut fields: Vec<Arc<Field>> = left_schema.fields().iter()
        .map(|f| Arc::new(Field::new(f.name(), arrow_schema::DataType::Utf8, true)))
        .collect();
    for &i in &right_col_indices {
        let f = right_schema.field(i);
        let name = if left_schema.field_with_name(f.name()).is_ok() {
            format!("{}_right", f.name())
        } else {
            f.name().to_string()
        };
        fields.push(Arc::new(Field::new(name, arrow_schema::DataType::Utf8, true)));
    }
    let out_schema = Arc::new(Schema::new(fields));

    // Build output rows
    let left_num_cols = left.num_columns();
    let total_cols = left_num_cols + right_col_indices.len();
    let mut builders: Vec<arrow::array::StringBuilder> = (0..total_cols)
        .map(|_| arrow::array::StringBuilder::new())
        .collect();

    for left_row in 0..left.num_rows() {
        let key = cell_to_string(left.column(left_key_idx).as_ref(), left_row);
        let right_rows = right_index.get(&key);

        match right_rows {
            Some(rows) => {
                for &right_row in rows {
                    for (col, builder) in builders.iter_mut().enumerate().take(left_num_cols) {
                        builder.append_value(cell_to_string(left.column(col).as_ref(), left_row));
                    }
                    for (i, &right_col) in right_col_indices.iter().enumerate() {
                        builders[left_num_cols + i].append_value(
                            cell_to_string(right.column(right_col).as_ref(), right_row),
                        );
                    }
                }
            }
            None if join_type == "left" => {
                for (col, builder) in builders.iter_mut().enumerate().take(left_num_cols) {
                    builder.append_value(cell_to_string(left.column(col).as_ref(), left_row));
                }
                for builder in builders.iter_mut().skip(left_num_cols) {
                    builder.append_value("");
                }
            }
            _ => {} // inner join: skip rows with no match
        }
    }

    let columns: Vec<ArrayRef> = builders.into_iter().map(|mut b| Arc::new(b.finish()) as ArrayRef).collect();

    RecordBatch::try_new(out_schema, columns)
        .map_err(|e| format!("Failed to build join result: {e}"))
}

pub struct JoinTables;

impl ToolHandler for JoinTables {
    fn spec(&self) -> ToolSpec {
        ToolSpec {
            id: "join_tables".into(),
            label: "Join Tables".into(),
            description: "Merge two datasets on a key column".into(),
            category: "data".into(),
            icon: "Merge".into(),
            inputs: vec![
                port_with_format("left", "Text", "tabular"),
                port_with_format("right", "Text", "tabular"),
            ],
            outputs: vec![port_with_format("result", "Text", "tabular")],
            params: vec![
                string_param("left_key", "Left Key Column"),
                string_param("right_key", "Right Key Column"),
                select_param("join_type", "Join Type", &["inner", "left"]),
            ],
        }
    }

    fn execute(&self, ctx: &mut ExecutionContext) -> Result<(), String> {
        let left = ctx.input_arrow("left")?;
        let right = ctx.input_arrow("right")?;
        let left_key = ctx.param_str("left_key")?;
        let right_key = ctx.param_str("right_key")?;
        let join_type = ctx.param_str("join_type").unwrap_or("inner");
        let result = join_tables(&left, &right, left_key, right_key, join_type)?;
        ctx.set_output_arrow("result", result);
        Ok(())
    }
}

// ============================================================
// REGISTRATION
// ============================================================

pub fn register(registry: &mut ToolRegistry) {
    registry.register(Box::new(SelectColumns));
    registry.register(Box::new(RenameColumn));
    registry.register(Box::new(SortRows));
    registry.register(Box::new(Deduplicate));
    registry.register(Box::new(GroupBy));
    registry.register(Box::new(AddColumn));
    registry.register(Box::new(TextReplace));
    registry.register(Box::new(JoinTables));
}

// ============================================================
// TESTS
// ============================================================

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, StringArray};
    use arrow_schema::{DataType, Field, Schema};

    fn sample_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("age", DataType::Int32, false),
            Field::new("city", DataType::Utf8, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["Alice", "Bob", "Alice", "Charlie"])),
                Arc::new(Int32Array::from(vec![30, 25, 30, 35])),
                Arc::new(StringArray::from(vec!["NYC", "LA", "NYC", "SF"])),
            ],
        )
        .unwrap()
    }

    #[test]
    fn test_select_columns() {
        let batch = sample_batch();
        let result = select_columns(&batch, &["name", "city"]).unwrap();
        assert_eq!(result.num_columns(), 2);
        assert_eq!(result.num_rows(), 4);
    }

    #[test]
    fn test_rename_column() {
        let batch = sample_batch();
        let result = rename_columns(&batch, "name", "full_name").unwrap();
        assert!(result.schema().field_with_name("full_name").is_ok());
    }

    #[test]
    fn test_sort_rows() {
        let batch = sample_batch();
        let result = sort_rows(&batch, "age", true).unwrap();
        assert_eq!(result.num_rows(), 4);
        let ages = result.column(1).as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(ages.value(0), 25); // Bob first (youngest)
    }

    #[test]
    fn test_deduplicate() {
        let batch = sample_batch();
        let result = deduplicate(&batch, &["name"]).unwrap();
        assert_eq!(result.num_rows(), 3); // Alice appears once
    }

    // ─── GroupBy ────────────────────────────────────────────────

    fn cell_str(batch: &RecordBatch, row: usize, col: &str) -> String {
        let idx = batch.schema().index_of(col).unwrap();
        cell_to_string(batch.column(idx).as_ref(), row)
    }

    #[test]
    fn test_group_by_count() {
        let batch = sample_batch();
        let result = group_by(&batch, &["city"], &["name:count"]).unwrap();
        // 3 distinct cities: NYC (x2), LA (x1), SF (x1)
        assert_eq!(result.num_rows(), 3);
        assert!(result.schema().field_with_name("name_count").is_ok());

        let mut counts_by_city: std::collections::HashMap<String, String> =
            std::collections::HashMap::new();
        for row in 0..result.num_rows() {
            counts_by_city.insert(
                cell_str(&result, row, "city"),
                cell_str(&result, row, "name_count"),
            );
        }
        assert_eq!(counts_by_city.get("NYC").unwrap(), "2");
        assert_eq!(counts_by_city.get("LA").unwrap(), "1");
        assert_eq!(counts_by_city.get("SF").unwrap(), "1");
    }

    #[test]
    fn test_group_by_sum_avg() {
        let batch = sample_batch();
        let result = group_by(&batch, &["city"], &["age:sum", "age:avg"]).unwrap();
        assert_eq!(result.num_rows(), 3);

        let mut by_city: std::collections::HashMap<String, (String, String)> =
            std::collections::HashMap::new();
        for row in 0..result.num_rows() {
            by_city.insert(
                cell_str(&result, row, "city"),
                (cell_str(&result, row, "age_sum"), cell_str(&result, row, "age_avg")),
            );
        }
        // NYC: Alice 30 + Alice 30 = 60 sum, 30 avg
        assert_eq!(by_city.get("NYC").unwrap().0, "60");
        assert_eq!(by_city.get("NYC").unwrap().1, "30");
        // LA: Bob 25
        assert_eq!(by_city.get("LA").unwrap().0, "25");
    }

    #[test]
    fn test_group_by_rejects_malformed_aggregation() {
        let batch = sample_batch();
        let err = group_by(&batch, &["city"], &["badformat"]).unwrap_err();
        assert!(err.contains("Invalid aggregation"), "got: {err}");
    }

    // ─── AddColumn ──────────────────────────────────────────────

    #[test]
    fn test_add_column_arithmetic() {
        let batch = sample_batch();
        let result = add_column(&batch, "age_double", "age * 2").unwrap();
        assert!(result.schema().field_with_name("age_double").is_ok());
        assert_eq!(result.num_rows(), 4);

        // Row 0: Alice age 30 -> 60
        assert_eq!(cell_str(&result, 0, "age_double"), "60");
        // Row 1: Bob age 25 -> 50
        assert_eq!(cell_str(&result, 1, "age_double"), "50");
    }

    #[test]
    fn test_add_column_string_concat() {
        let batch = sample_batch();
        let result = add_column(&batch, "label", "name + ' from ' + city").unwrap();
        assert_eq!(cell_str(&result, 0, "label"), "Alice from NYC");
        assert_eq!(cell_str(&result, 1, "label"), "Bob from LA");
    }

    #[test]
    fn test_add_column_division_by_zero_safe() {
        let batch = sample_batch();
        // Any row divided by 0 should return 0, not panic
        let result = add_column(&batch, "weird", "age / 0").unwrap();
        assert_eq!(cell_str(&result, 0, "weird"), "0");
    }

    // ─── TextReplace ────────────────────────────────────────────

    #[test]
    fn test_text_replace_basic() {
        let batch = sample_batch();
        let result = text_replace(&batch, "city", "NYC", "New York").unwrap();
        assert_eq!(cell_str(&result, 0, "city"), "New York");
        assert_eq!(cell_str(&result, 1, "city"), "LA"); // untouched
        assert_eq!(cell_str(&result, 2, "city"), "New York");
    }

    #[test]
    fn test_text_replace_empty_replacement_strips_text() {
        let batch = sample_batch();
        let result = text_replace(&batch, "name", "Alice", "").unwrap();
        assert_eq!(cell_str(&result, 0, "name"), "");
        assert_eq!(cell_str(&result, 1, "name"), "Bob");
    }

    #[test]
    fn test_text_replace_missing_column_errors() {
        let batch = sample_batch();
        let err = text_replace(&batch, "nonexistent", "a", "b").unwrap_err();
        assert!(err.contains("not found"), "got: {err}");
    }

    // ─── JoinTables ─────────────────────────────────────────────

    fn orders_batch() -> RecordBatch {
        // "customer_id" joins to sample_batch's "name"
        let schema = Arc::new(Schema::new(vec![
            Field::new("customer_id", DataType::Utf8, false),
            Field::new("amount", DataType::Int32, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["Alice", "Bob", "Alice", "Zelda"])),
                Arc::new(Int32Array::from(vec![100, 250, 50, 999])),
            ],
        )
        .unwrap()
    }

    #[test]
    fn test_join_tables_inner() {
        let customers = sample_batch();
        let orders = orders_batch();
        let result = join_tables(&customers, &orders, "name", "customer_id", "inner").unwrap();

        // Alice appears twice in customers AND twice in orders -> 4 matches
        // Bob: 1 × 1 = 1 match
        // Charlie: no match, skipped
        // Zelda: not in customers, skipped
        // Expected total: 5 rows
        assert_eq!(result.num_rows(), 5);
        // Join key column "customer_id" should NOT appear (avoided duplication)
        assert!(result.schema().field_with_name("customer_id").is_err());
        assert!(result.schema().field_with_name("amount").is_ok());
    }

    #[test]
    fn test_join_tables_left_preserves_unmatched() {
        let customers = sample_batch();
        let orders = orders_batch();
        let result = join_tables(&customers, &orders, "name", "customer_id", "left").unwrap();

        // Left join: all 4 customer rows preserved.
        // Alice (x2 rows) each match 2 orders -> 4 rows. Bob -> 1 row. Charlie has no match -> 1 row with blanks.
        // Total: 4 + 1 + 1 = 6 rows
        assert_eq!(result.num_rows(), 6);
        // Charlie's row should have empty amount (unmatched)
        let mut found_charlie_blank = false;
        for row in 0..result.num_rows() {
            if cell_str(&result, row, "name") == "Charlie"
                && cell_str(&result, row, "amount").is_empty()
            {
                found_charlie_blank = true;
            }
        }
        assert!(found_charlie_blank, "left join should preserve Charlie with blank amount");
    }

    #[test]
    fn test_join_tables_missing_key_column_errors() {
        let customers = sample_batch();
        let orders = orders_batch();
        let err = join_tables(&customers, &orders, "wrong_key", "customer_id", "inner")
            .unwrap_err();
        assert!(err.contains("not found"), "got: {err}");
    }
}
