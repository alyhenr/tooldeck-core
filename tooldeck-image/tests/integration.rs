//! Integration tests for image tools.
//! Uses sample_files/banner.png (1.1 MB) as the main fixture.

use tooldeck_image::{convert_image_bytes, remove_background_bytes, resize_image_bytes};

fn sample_png() -> Vec<u8> {
    let path = concat!(env!("CARGO_MANIFEST_DIR"), "/../../sample_files/banner.png");
    std::fs::read(path).unwrap_or_else(|_| panic!("sample banner.png not found at {path}"))
}

// ─── Resize ────────────────────────────────────────────────

#[test]
fn resize_aspect_preserved_fits_within_box() {
    let png = sample_png();
    let (out, mime) = resize_image_bytes(&png, 200, 200, true).unwrap();
    assert_eq!(mime, "image/png");
    // Decode and verify dimensions <= 200 in both directions.
    let decoded = image::load_from_memory(&out).unwrap();
    let (w, h) = (decoded.width(), decoded.height());
    assert!(w <= 200 && h <= 200, "expected <=200x200, got {w}x{h}");
    // At least one dimension should hit the target (fitting within box).
    assert!(
        w == 200 || h == 200,
        "aspect-preserve should hit one dimension, got {w}x{h}"
    );
}

#[test]
fn resize_exact_ignores_aspect_when_disabled() {
    let png = sample_png();
    let (out, _) = resize_image_bytes(&png, 300, 100, false).unwrap();
    let decoded = image::load_from_memory(&out).unwrap();
    assert_eq!(decoded.width(), 300);
    assert_eq!(decoded.height(), 100);
}

#[test]
fn resize_single_dimension_infers_other() {
    let png = sample_png();
    // Width-only resize with aspect preserved
    let (out, _) = resize_image_bytes(&png, 100, 0, true).unwrap();
    let decoded = image::load_from_memory(&out).unwrap();
    assert_eq!(decoded.width(), 100);
    assert!(decoded.height() > 0);
}

#[test]
fn resize_rejects_zero_dimensions() {
    let png = sample_png();
    let err = resize_image_bytes(&png, 0, 0, true).unwrap_err();
    assert!(err.contains("at least width or height"));
}

// ─── Convert ──────────────────────────────────────────────

#[test]
fn convert_png_to_webp() {
    let png = sample_png();
    let (out, mime) = convert_image_bytes(&png, "webp").unwrap();
    assert_eq!(mime, "image/webp");
    // WebP files start with "RIFF....WEBP"
    assert!(out.len() > 12, "output too small");
    assert_eq!(&out[0..4], b"RIFF", "missing WebP RIFF header");
    assert_eq!(&out[8..12], b"WEBP", "missing WEBP marker");
}

#[test]
fn convert_png_to_jpeg() {
    let png = sample_png();
    let (out, mime) = convert_image_bytes(&png, "jpg").unwrap();
    assert_eq!(mime, "image/jpeg");
    // JPEG starts with FF D8
    assert!(out.starts_with(&[0xFF, 0xD8]), "not a JPEG");
}

#[test]
fn convert_unknown_format_falls_back_to_png() {
    let png = sample_png();
    let (out, mime) = convert_image_bytes(&png, "gibberish").unwrap();
    // Unknown names fall back to PNG per `mime_to_format` defaults.
    assert_eq!(mime, "image/png");
    assert!(out.starts_with(&[0x89, b'P', b'N', b'G']));
}

// ─── Remove Background ────────────────────────────────────

#[test]
fn remove_background_outputs_valid_png() {
    let png = sample_png();
    let out = remove_background_bytes(&png, 20).unwrap();
    // Always produces PNG (for transparency support)
    assert!(out.starts_with(&[0x89, b'P', b'N', b'G']), "not a PNG");
    // Output should decode cleanly
    let decoded = image::load_from_memory(&out).unwrap();
    // Same dimensions as input
    let orig = image::load_from_memory(&png).unwrap();
    assert_eq!(decoded.width(), orig.width());
    assert_eq!(decoded.height(), orig.height());
}

#[test]
fn image_tools_reject_garbage_bytes() {
    let garbage = b"this is not an image";
    let err_resize = resize_image_bytes(garbage, 100, 100, true).unwrap_err();
    let err_convert = convert_image_bytes(garbage, "png").unwrap_err();
    let err_bg = remove_background_bytes(garbage, 20).unwrap_err();
    // All three should fail cleanly with a "cannot detect" / "decode" message.
    for err in [&err_resize, &err_convert, &err_bg] {
        assert!(
            err.contains("detect") || err.contains("decode"),
            "unexpected error: {err}"
        );
    }
}
