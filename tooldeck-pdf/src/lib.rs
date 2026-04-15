use lopdf::{dictionary, Document, Object, ObjectId};
use std::collections::BTreeMap;
use tooldeck_registry::{
    ExecutionContext, ToolHandler, ToolRegistry, ToolSpec,
    port_with_format, number_param,
};

// ============================================================
// MERGE PDFs — combines multiple PDFs into one
// ============================================================

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

        if pdf_files.len() < 2 {
            return Err("At least 2 PDF files are required to merge".into());
        }

        // Load all documents
        let documents: Vec<Document> = pdf_files.iter().enumerate()
            .map(|(i, bytes)| {
                Document::load_mem(bytes)
                    .map_err(|e| format!("Failed to load PDF {}. The file may be encrypted, corrupted, or use an unsupported PDF version. Details: {e}", i + 1))
            })
            .collect::<Result<Vec<_>, _>>()?;

        // Merge using lopdf's low-level API
        let mut merged = merge_pdf_documents(documents)?;

        let mut output = Vec::new();
        merged.save_to(&mut output)
            .map_err(|e| format!("Failed to save merged PDF: {e}"))?;

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

        if start == 0 || end == 0 {
            return Err("Page numbers start at 1".into());
        }
        if start > end {
            return Err(format!("Start page ({start}) must be <= end page ({end})"));
        }

        let doc = Document::load_mem(&bytes)
            .map_err(|e| format!("Failed to load PDF. The file may be encrypted, corrupted, or use an unsupported PDF version. Details: {e}"))?;

        let total_pages = doc.get_pages().len() as u32;
        if start > total_pages {
            return Err(format!("Start page ({start}) exceeds total pages ({total_pages})"));
        }

        let pages_to_delete: Vec<u32> = (1..=total_pages)
            .filter(|&p| p < start || p > end.min(total_pages))
            .collect();

        let mut new_doc = doc;
        new_doc.delete_pages(&pages_to_delete);

        let mut output = Vec::new();
        new_doc.save_to(&mut output)
            .map_err(|e| format!("Failed to save PDF: {e}"))?;

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
