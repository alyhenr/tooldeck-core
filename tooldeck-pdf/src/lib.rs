use lopdf::{dictionary, Document, Object, ObjectId};
use std::collections::{BTreeMap, HashSet};
use tooldeck_registry::{
    ExecutionContext, ToolHandler, ToolRegistry, ToolSpec,
    port_with_format, number_param,
};

// ============================================================
// MERGE PDFs — combines multiple PDFs into one
// ============================================================

/// Merge two or more PDF byte blobs into a single PDF.
/// Returns the bytes of the merged PDF.
pub fn merge_pdfs_bytes(pdf_files: &[&[u8]]) -> Result<Vec<u8>, String> {
    if pdf_files.len() < 2 {
        return Err("At least 2 PDF files are required to merge".into());
    }

    let documents: Vec<Document> = pdf_files
        .iter()
        .enumerate()
        .map(|(i, bytes)| {
            Document::load_mem(bytes).map_err(|e| {
                format!(
                    "Failed to load PDF {}. The file may be encrypted, corrupted, or use an unsupported PDF version. Details: {e}",
                    i + 1
                )
            })
        })
        .collect::<Result<Vec<_>, _>>()?;

    let mut merged = merge_pdf_documents(documents)?;

    let mut output = Vec::new();
    merged
        .save_to(&mut output)
        .map_err(|e| format!("Failed to save merged PDF: {e}"))?;

    Ok(output)
}

pub struct MergePdfs;

impl ToolHandler for MergePdfs {
    fn spec(&self) -> ToolSpec {
        ToolSpec {
            id: "merge_pdfs".into(),
            label: "Merge PDFs".into(),
            description: "Combine multiple PDF files into one".into(),
            category: "pdf".into(),
            icon: "Files".into(),
            inputs: vec![
                tooldeck_registry::PortSpec {
                    name: "pdfs".into(),
                    port_type: "Bytes".into(),
                    format: Some("pdf".into()),
                    multiple: Some(true),
                    min: Some(2),
                },
            ],
            outputs: vec![port_with_format("result", "Bytes", "pdf")],
            params: vec![],
        }
    }

    fn execute(&self, ctx: &mut ExecutionContext) -> Result<(), String> {
        let pdf_files = ctx.input_bytes_multi("pdfs")?;
        let refs: Vec<&[u8]> = pdf_files.iter().map(|v| v.as_slice()).collect();
        let output = merge_pdfs_bytes(&refs)?;
        ctx.set_output_bytes("result", output, "application/pdf");
        Ok(())
    }
}

fn merge_pdf_documents(documents: Vec<Document>) -> Result<Document, String> {
    let mut max_id = 1;
    let mut all_pages = Vec::new();
    let mut all_objects: BTreeMap<ObjectId, Object> = BTreeMap::new();

    for mut doc in documents {
        doc.renumber_objects_with(max_id);
        max_id = doc.max_id + 1;

        // Collect all pages from this document
        let pages: Vec<ObjectId> = doc.get_pages().into_values().collect();
        all_pages.extend(pages);

        // Collect all objects
        for (id, object) in doc.objects {
            all_objects.insert(id, object);
        }
    }

    // Create a new document with all the objects
    let mut merged = Document::with_version("1.5");
    merged.objects = all_objects;
    merged.max_id = max_id;

    // Create a new Pages object that references all collected pages
    let pages_id = merged.new_object_id();
    let page_refs: Vec<Object> = all_pages.iter()
        .map(|id| Object::Reference(*id))
        .collect();

    let pages_dict = lopdf::dictionary! {
        "Type" => "Pages",
        "Count" => all_pages.len() as i64,
        "Kids" => page_refs,
    };
    merged.objects.insert(pages_id, Object::Dictionary(pages_dict));

    // Update each page's Parent reference
    for page_id in &all_pages {
        if let Ok(Object::Dictionary(ref mut dict)) = merged.objects.get_mut(page_id)
            .ok_or("Page not found") {
            dict.set("Parent", Object::Reference(pages_id));
        }
    }

    // Create the Catalog
    let catalog_id = merged.new_object_id();
    let catalog = lopdf::dictionary! {
        "Type" => "Catalog",
        "Pages" => Object::Reference(pages_id),
    };
    merged.objects.insert(catalog_id, Object::Dictionary(catalog));
    merged.trailer.set("Root", Object::Reference(catalog_id));

    // Compress before saving
    merged.compress();

    Ok(merged)
}

// ============================================================
// SPLIT PDF — extract a range of pages
// ============================================================

/// Extract pages `start..=end` (1-indexed, inclusive) from a PDF.
/// Returns the bytes of the resulting single-range PDF.
///
/// Implementation note: we build a fresh Pages tree containing only the kept
/// page ObjectIds, then rely on `prune_objects` to drop everything unreachable
/// (old Pages tree, deleted pages, their content streams and private fonts).
/// This is O(kept pages) + one graph traversal — MUCH faster than
/// `delete_pages` which calls `delete_object` per removed page, and each
/// `delete_object` walks the entire object graph → O(deleted pages × objects).
pub fn split_pdf_bytes(bytes: &[u8], start: u32, end: u32) -> Result<Vec<u8>, String> {
    if start == 0 || end == 0 {
        return Err("Page numbers start at 1".into());
    }
    if start > end {
        return Err(format!("Start page ({start}) must be <= end page ({end})"));
    }

    let mut doc = Document::load_mem(bytes).map_err(|e| {
        format!("Failed to load PDF. The file may be encrypted, corrupted, or use an unsupported PDF version. Details: {e}")
    })?;

    let all_pages = doc.get_pages();
    let total_pages = all_pages.len() as u32;
    if start > total_pages {
        return Err(format!("Start page ({start}) exceeds total pages ({total_pages})"));
    }

    // Split pages into kept vs removed sets.
    let end_clamped = end.min(total_pages);
    let mut kept_page_ids: Vec<ObjectId> = Vec::new();
    let mut removed_page_ids: HashSet<ObjectId> = HashSet::new();
    for (page_num, id) in all_pages {
        if page_num >= start && page_num <= end_clamped {
            kept_page_ids.push(id);
        } else {
            removed_page_ids.insert(id);
        }
    }

    // Delete the non-kept page objects directly from the objects map.
    // Unlike lopdf's `delete_pages` (which does a full graph traversal per
    // deleted page to clean up inbound references, → O(pages × objects)),
    // this is O(pages). Any lingering references to removed pages (e.g. from
    // annotations, bookmarks, named destinations) will point to non-existent
    // ObjectIds — prune_objects handles that gracefully: it adds the ID to
    // the visited set but skips traversal when the object is missing, so the
    // removed pages' content streams / fonts / images stay unreachable.
    for id in &removed_page_ids {
        doc.objects.remove(id);
    }

    // Build a fresh Pages dict containing only the kept page references.
    let pages_id = doc.new_object_id();
    let page_refs: Vec<Object> = kept_page_ids.iter().map(|id| Object::Reference(*id)).collect();
    let pages_dict = dictionary! {
        "Type" => "Pages",
        "Count" => kept_page_ids.len() as i64,
        "Kids" => page_refs,
    };
    doc.objects.insert(pages_id, Object::Dictionary(pages_dict));

    // Re-parent kept pages so prune won't consider them orphans via their old
    // Parent pointer (which referenced the original Pages tree).
    for page_id in &kept_page_ids {
        if let Some(Object::Dictionary(ref mut dict)) = doc.objects.get_mut(page_id) {
            dict.set("Parent", Object::Reference(pages_id));
        }
    }

    // Fresh Catalog pointing at the new Pages tree.
    let catalog_id = doc.new_object_id();
    let catalog = dictionary! {
        "Type" => "Catalog",
        "Pages" => Object::Reference(pages_id),
    };
    doc.objects.insert(catalog_id, Object::Dictionary(catalog));

    // Replace the trailer wholesale. Keeping the old trailer around can keep
    // stale objects reachable (e.g. /Info pointing into the old metadata tree,
    // or the old /Root still being traversed by some codepath). Starting from
    // a minimal trailer makes prune's reachability analysis unambiguous.
    let mut new_trailer = lopdf::Dictionary::new();
    new_trailer.set("Root", Object::Reference(catalog_id));
    doc.trailer = new_trailer;

    // Drop all unreachable objects: the old catalog, old Pages tree, and any
    // content streams / fonts / images only the removed pages referenced.
    doc.prune_objects();

    let mut output = Vec::new();
    doc.save_to(&mut output)
        .map_err(|e| format!("Failed to save PDF: {e}"))?;

    Ok(output)
}

pub struct SplitPdf;

impl ToolHandler for SplitPdf {
    fn spec(&self) -> ToolSpec {
        ToolSpec {
            id: "split_pdf".into(),
            label: "Split PDF".into(),
            description: "Extract a range of pages from a PDF".into(),
            category: "pdf".into(),
            icon: "Scissors".into(),
            inputs: vec![port_with_format("pdf", "Bytes", "pdf")],
            outputs: vec![port_with_format("result", "Bytes", "pdf")],
            params: vec![
                number_param("start_page", "Start Page"),
                number_param("end_page", "End Page"),
            ],
        }
    }

    fn execute(&self, ctx: &mut ExecutionContext) -> Result<(), String> {
        let bytes = ctx.input_bytes("pdf")?;
        let start = ctx.param_f64("start_page").unwrap_or(1.0) as u32;
        let end = ctx.param_f64("end_page").unwrap_or(1.0) as u32;
        let output = split_pdf_bytes(&bytes, start, end)?;
        ctx.set_output_bytes("result", output, "application/pdf");
        Ok(())
    }
}

// ============================================================
// REGISTRATION
// ============================================================

pub fn register(registry: &mut ToolRegistry) {
    registry.register(Box::new(MergePdfs));
    registry.register(Box::new(SplitPdf));
}
