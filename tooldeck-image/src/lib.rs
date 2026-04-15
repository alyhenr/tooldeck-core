use image::{DynamicImage, GenericImageView, ImageFormat, Rgba};
use std::io::Cursor;
use tooldeck_registry::{
    ExecutionContext, ToolHandler, ToolRegistry, ToolSpec,
    port_with_format, number_param, bool_param, select_param,
};

// ============================================================
// HELPERS
// ============================================================

fn decode_image(bytes: &[u8]) -> Result<(DynamicImage, ImageFormat), String> {
    let format = image::guess_format(bytes)
        .map_err(|e| format!("Cannot detect image format: {e}"))?;
    let img = image::load_from_memory_with_format(bytes, format)
        .map_err(|e| format!("Failed to decode image: {e}"))?;
    Ok((img, format))
}

fn encode_image(img: &DynamicImage, format: ImageFormat) -> Result<Vec<u8>, String> {
    let mut buf = Vec::new();
    img.write_to(&mut Cursor::new(&mut buf), format)
        .map_err(|e| format!("Failed to encode image: {e}"))?;
    Ok(buf)
}

fn format_to_mime(format: ImageFormat) -> &'static str {
    match format {
        ImageFormat::Png => "image/png",
        ImageFormat::Jpeg => "image/jpeg",
        ImageFormat::WebP => "image/webp",
        _ => "application/octet-stream",
    }
}

fn mime_to_format(name: &str) -> ImageFormat {
    match name.to_lowercase().as_str() {
        "png" => ImageFormat::Png,
        "jpg" | "jpeg" => ImageFormat::Jpeg,
        "webp" => ImageFormat::WebP,
        _ => ImageFormat::Png,
    }
}

// ============================================================
// REMOVE BACKGROUND
// ============================================================

pub struct RemoveBackground;

impl ToolHandler for RemoveBackground {
    fn spec(&self) -> ToolSpec {
        ToolSpec {
            id: "remove_background".into(),
            label: "Remove Background".into(),
            description: "Make the background of an image transparent".into(),
            category: "image".into(),
            icon: "ImageMinus".into(),
            inputs: vec![port_with_format("image", "Bytes", "image")],
            outputs: vec![port_with_format("result", "Bytes", "image")],
            params: vec![
                number_param("tolerance", "Color Tolerance (0-100)"),
            ],
        }
    }

    fn execute(&self, ctx: &mut ExecutionContext) -> Result<(), String> {
        let bytes = ctx.input_bytes("image")?;
        let tolerance = ctx.param_f64("tolerance").unwrap_or(20.0) as u8;

        let (img, _) = decode_image(&bytes)?;
        let mut rgba = img.to_rgba8();
        let (width, height) = rgba.dimensions();

        // Sample background color from the corners
        let corners = [
            rgba.get_pixel(0, 0),
            rgba.get_pixel(width - 1, 0),
            rgba.get_pixel(0, height - 1),
            rgba.get_pixel(width - 1, height - 1),
        ];

        // Use the most common corner color as the background
        let bg = most_common_color(&corners);

        // Make pixels similar to bg color transparent
        for y in 0..height {
            for x in 0..width {
                let pixel = rgba.get_pixel(x, y);
                if color_distance(pixel, &bg) <= tolerance as u32 {
                    rgba.put_pixel(x, y, Rgba([0, 0, 0, 0]));
                }
            }
        }

        // Always output PNG (supports transparency)
        let result = DynamicImage::ImageRgba8(rgba);
        let output = encode_image(&result, ImageFormat::Png)?;
        ctx.set_output_bytes("result", output, "image/png");
        Ok(())
    }
}

fn most_common_color(colors: &[&Rgba<u8>]) -> Rgba<u8> {
    // Simple: just use the first corner
    *colors[0]
}

fn color_distance(a: &Rgba<u8>, b: &Rgba<u8>) -> u32 {
    let dr = (a[0] as i32 - b[0] as i32).unsigned_abs();
    let dg = (a[1] as i32 - b[1] as i32).unsigned_abs();
    let db = (a[2] as i32 - b[2] as i32).unsigned_abs();
    (dr + dg + db) / 3
}

// ============================================================
// RESIZE IMAGE
// ============================================================

pub struct ResizeImage;

impl ToolHandler for ResizeImage {
    fn spec(&self) -> ToolSpec {
        ToolSpec {
            id: "resize_image".into(),
            label: "Resize Image".into(),
            description: "Scale an image to target dimensions".into(),
            category: "image".into(),
            icon: "ImageUp".into(),
            inputs: vec![port_with_format("image", "Bytes", "image")],
            outputs: vec![port_with_format("result", "Bytes", "image")],
            params: vec![
                number_param("width", "Width (px)"),
                number_param("height", "Height (px)"),
                bool_param("maintain_aspect", "Maintain Aspect Ratio"),
            ],
        }
    }

    fn execute(&self, ctx: &mut ExecutionContext) -> Result<(), String> {
        let bytes = ctx.input_bytes("image")?;
        let target_width = ctx.param_f64("width").unwrap_or(0.0) as u32;
        let target_height = ctx.param_f64("height").unwrap_or(0.0) as u32;
        let maintain_aspect = ctx.param_bool("maintain_aspect").unwrap_or(true);

        if target_width == 0 && target_height == 0 {
            return Err("Specify at least width or height".into());
        }

        let (img, format) = decode_image(&bytes)?;
        let (orig_w, orig_h) = img.dimensions();

        let (new_w, new_h) = if maintain_aspect {
            if target_width > 0 && target_height > 0 {
                // Fit within the box
                let ratio_w = target_width as f64 / orig_w as f64;
                let ratio_h = target_height as f64 / orig_h as f64;
                let ratio = ratio_w.min(ratio_h);
                ((orig_w as f64 * ratio) as u32, (orig_h as f64 * ratio) as u32)
            } else if target_width > 0 {
                let ratio = target_width as f64 / orig_w as f64;
                (target_width, (orig_h as f64 * ratio) as u32)
            } else {
                let ratio = target_height as f64 / orig_h as f64;
                ((orig_w as f64 * ratio) as u32, target_height)
            }
        } else {
            (
                if target_width > 0 { target_width } else { orig_w },
                if target_height > 0 { target_height } else { orig_h },
            )
        };

        let resized = img.resize_exact(new_w, new_h, image::imageops::FilterType::Lanczos3);
        let output = encode_image(&resized, format)?;
        ctx.set_output_bytes("result", output, format_to_mime(format));
        Ok(())
    }
}

// ============================================================
// CONVERT IMAGE FORMAT
// ============================================================

pub struct ConvertImage;

impl ToolHandler for ConvertImage {
    fn spec(&self) -> ToolSpec {
        ToolSpec {
            id: "convert_image".into(),
            label: "Convert Image".into(),
            description: "Change image format (PNG, JPG, WebP)".into(),
            category: "image".into(),
            icon: "FileImage".into(),
            inputs: vec![port_with_format("image", "Bytes", "image")],
            outputs: vec![port_with_format("result", "Bytes", "image")],
            params: vec![
                select_param("output_format", "Output Format", &["PNG", "JPG", "WebP"]),
            ],
        }
    }

    fn execute(&self, ctx: &mut ExecutionContext) -> Result<(), String> {
        let bytes = ctx.input_bytes("image")?;
        let output_format_name = ctx.param_str("output_format").unwrap_or("PNG");

        let (img, _) = decode_image(&bytes)?;
        let target_format = mime_to_format(output_format_name);
        let output = encode_image(&img, target_format)?;
        ctx.set_output_bytes("result", output, format_to_mime(target_format));
        Ok(())
    }
}

// ============================================================
// REGISTRATION
// ============================================================

pub fn register(registry: &mut ToolRegistry) {
    registry.register(Box::new(RemoveBackground));
    registry.register(Box::new(ResizeImage));
    registry.register(Box::new(ConvertImage));
}
