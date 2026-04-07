use arrow::array::{Array, ArrayRef, StringArray, BooleanArray};
use arrow::compute;
use arrow::record_batch::RecordBatch;
use arrow_schema::{Field, Schema};
use std::collections::HashSet;
use std::sync::Arc;
use tooldeck_registry::{
    ExecutionContext, ToolHandler, ToolRegistry, ToolSpec,
    port, string_param, string_array_param,
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
            category: "transform".into(),
            icon: "Columns3".into(),
            inputs: vec![port("data", "Text")],
            outputs: vec![port("selected", "Text")],
            params: vec![string_array_param("columns", "Columns to keep")],
        }
    }

    fn execute(&self, ctx: &mut ExecutionContext) -> Result<(), String> {
        let batch = ctx.input_arrow("data")?;
        let columns = ctx.param_str_array("columns")?;
        let result = select_columns(&batch, &columns)?;
        ctx.set_output_arrow("selected", result);
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
            category: "transform".into(),
            icon: "TextCursorInput".into(),
            inputs: vec![port("data", "Text")],
            outputs: vec![port("renamed", "Text")],
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
        ctx.set_output_arrow("renamed", result);
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
            category: "transform".into(),
            icon: "ArrowUpDown".into(),
            inputs: vec![port("data", "Text")],
            outputs: vec![port("sorted", "Text")],
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
        ctx.set_output_arrow("sorted", result);
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
            // Use the string representation of each cell as part of the key
            if col.is_null(row) {
                key.push_str("__NULL__");
            } else if let Some(s) = col.as_any().downcast_ref::<StringArray>() {
                key.push_str(s.value(row));
            } else {
                // Fallback: use debug format for non-string columns
                key.push_str(&format!("{:?}", col.slice(row, 1)));
            }
            key.push('\0'); // separator
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
            category: "transform".into(),
            icon: "CopyMinus".into(),
            inputs: vec![port("data", "Text")],
            outputs: vec![port("unique", "Text")],
            params: vec![string_array_param("key_columns", "Key Columns")],
        }
    }

    fn execute(&self, ctx: &mut ExecutionContext) -> Result<(), String> {
        let batch = ctx.input_arrow("data")?;
        let keys = ctx.param_str_array("key_columns")?;
        let result = deduplicate(&batch, &keys)?;
        ctx.set_output_arrow("unique", result);
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
}
