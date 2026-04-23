//! Integration tests for the PDF tools.
//!
//! Correctness tests run by default (`cargo test -p tooldeck-pdf`).
//! Performance benchmarks are marked `#[ignore]` — run explicitly with:
//!
//!     cargo test -p tooldeck-pdf --release -- --ignored --nocapture
//!
//! The `--release` flag matters a lot: debug builds of lopdf are 10-50x slower.

use std::time::Instant;
use tooldeck_pdf::{merge_pdfs_bytes, split_pdf_bytes};

/// Load the reference sample PDF from the workspace-level sample_files/ directory.
fn sample_pdf() -> Vec<u8> {
    let path = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../sample_files/CSAPP_2016.pdf"
    );
    std::fs::read(path).unwrap_or_else(|_| {
        panic!(
            "sample PDF not found at {path}. Expected sample_files/CSAPP_2016.pdf at the repo root."
        )
    })
}

// ─── Correctness ──────────────────────────────────────────────

#[test]
fn split_first_three_pages_succeeds() {
    let pdf = sample_pdf();
    let out = split_pdf_bytes(&pdf, 1, 3).expect("split should succeed");
    assert!(!out.is_empty(), "output must not be empty");
    assert!(out.starts_with(b"%PDF"), "output must be a valid PDF header");
    // After prune_objects(), 3 pages from a 1000+ page PDF should produce output
    // drastically smaller than the input — at most 20% of the source size.
    assert!(
        out.len() < pdf.len() / 5,
        "split output too large: {} bytes from {} bytes (prune_objects may not be pruning)",
        out.len(),
        pdf.len()
    );
}

#[test]
fn split_rejects_zero_page() {
    let pdf = sample_pdf();
    let err = split_pdf_bytes(&pdf, 0, 3).unwrap_err();
    assert!(err.contains("start at 1"), "got: {err}");
}

#[test]
fn split_rejects_reversed_range() {
    let pdf = sample_pdf();
    let err = split_pdf_bytes(&pdf, 10, 5).unwrap_err();
    assert!(err.contains("must be <= end"), "got: {err}");
}

#[test]
fn split_rejects_out_of_range_start() {
    let pdf = sample_pdf();
    let err = split_pdf_bytes(&pdf, 99_999, 99_999).unwrap_err();
    assert!(err.contains("exceeds total pages"), "got: {err}");
}

#[test]
fn merge_requires_at_least_two_pdfs() {
    let pdf = sample_pdf();
    let err = merge_pdfs_bytes(&[&pdf]).unwrap_err();
    assert!(err.contains("At least 2"), "got: {err}");
}

#[test]
fn merge_two_small_pdfs_produces_valid_output() {
    let pdf = sample_pdf();
    // First split to a small chunk so correctness tests stay fast
    let chunk = split_pdf_bytes(&pdf, 1, 3).unwrap();
    let out = merge_pdfs_bytes(&[&chunk, &chunk]).expect("merge should succeed");
    assert!(out.starts_with(b"%PDF"));
    assert!(out.len() > chunk.len(), "merged output should be larger than one chunk");
}

// ─── Performance benchmarks (run explicitly with --release --ignored) ──

#[test]
#[ignore]
fn bench_split_first_10_pages() {
    let pdf = sample_pdf();
    let size_mb = pdf.len() as f64 / 1024.0 / 1024.0;

    let start = Instant::now();
    let out = split_pdf_bytes(&pdf, 1, 10).expect("split should succeed");
    let elapsed = start.elapsed();

    let out_mb = out.len() as f64 / 1024.0 / 1024.0;
    eprintln!(
        "[bench][split-10] {size_mb:.2}MB input -> {out_mb:.2}MB (pages 1-10): {elapsed:?}"
    );
}

#[test]
#[ignore]
fn bench_split_first_100_pages() {
    let pdf = sample_pdf();
    let size_mb = pdf.len() as f64 / 1024.0 / 1024.0;

    let start = Instant::now();
    let out = split_pdf_bytes(&pdf, 1, 100).expect("split should succeed");
    let elapsed = start.elapsed();

    let out_mb = out.len() as f64 / 1024.0 / 1024.0;
    eprintln!(
        "[bench][split-100] {size_mb:.2}MB input -> {out_mb:.2}MB (pages 1-100): {elapsed:?}"
    );
}

#[test]
#[ignore]
fn bench_merge_two_small_chunks() {
    let pdf = sample_pdf();
    let chunk = split_pdf_bytes(&pdf, 1, 3).unwrap();
    let chunk_mb = chunk.len() as f64 / 1024.0 / 1024.0;

    let start = Instant::now();
    let out = merge_pdfs_bytes(&[&chunk, &chunk]).expect("merge should succeed");
    let elapsed = start.elapsed();

    let out_mb = out.len() as f64 / 1024.0 / 1024.0;
    eprintln!(
        "[bench][merge-small] 2 x {chunk_mb:.2}MB -> {out_mb:.2}MB: {elapsed:?}"
    );
}

#[test]
#[ignore]
fn bench_merge_two_full_pdfs() {
    let pdf = sample_pdf();
    let size_mb = pdf.len() as f64 / 1024.0 / 1024.0;

    let start = Instant::now();
    let out = merge_pdfs_bytes(&[&pdf, &pdf]).expect("merge should succeed");
    let elapsed = start.elapsed();

    let out_mb = out.len() as f64 / 1024.0 / 1024.0;
    eprintln!(
        "[bench][merge-full] 2 x {size_mb:.2}MB -> {out_mb:.2}MB: {elapsed:?}"
    );
}
