"""Enhance Confluent lineage screenshots: 2x upscale, sharpen, panorama + legend."""
from __future__ import annotations

from pathlib import Path

from PIL import Image, ImageDraw, ImageEnhance, ImageFilter, ImageFont

ASSETS = Path(
    r"C:\Users\ymittal\.cursor\projects"
    r"\c-Users-ymittal-OneDrive-Burns-McDonnell-Desktop-confluent-metrics\assets"
)
OUT = Path(__file__).resolve().parents[1] / "docs" / "gridsentinel-lineage-enhanced.png"

LEFT = ASSETS / (
    "c__Users_ymittal_AppData_Roaming_Cursor_User_workspaceStorage_empty-window_"
    "images_image-28eb8e13-8d5b-44d9-b427-89a1a1fc98d2.png"
)
RIGHT = ASSETS / (
    "c__Users_ymittal_AppData_Roaming_Cursor_User_workspaceStorage_empty-window_"
    "images_image-5edb4a69-fd01-4c62-a6ef-88f42adf4a61.png"
)

CROP_TOP = 118
SCALE = 2
BG = (14, 14, 16)


def _fonts() -> tuple[ImageFont.FreeTypeFont | ImageFont.ImageFont, ...]:
    try:
        return (
            ImageFont.truetype("segoeui.ttf", 22),
            ImageFont.truetype("segoeui.ttf", 15),
            ImageFont.truetype("segoeui.ttf", 13),
            ImageFont.truetype("segoeuib.ttf", 14),
        )
    except OSError:
        d = ImageFont.load_default()
        return d, d, d, d


def crop_graph(path: Path, bottom_trim: int) -> Image.Image:
    im = Image.open(path).convert("RGB")
    w, h = im.size
    return im.crop((0, CROP_TOP, w, h - bottom_trim))


def enhance_panel(im: Image.Image) -> Image.Image:
    w, h = im.size
    up = im.resize((w * SCALE, h * SCALE), Image.Resampling.LANCZOS)
    up = ImageEnhance.Contrast(up).enhance(1.12)
    up = ImageEnhance.Sharpness(up).enhance(1.35)
    return up.filter(ImageFilter.UnsharpMask(radius=1.2, percent=130, threshold=2))


def draw_legend(draw: ImageDraw.ImageDraw, x: int, y: int, w: int, fonts) -> int:
    _, small, tiny, bold = fonts
    stages = [
        ("① Ingest", "telemetry · scenario · sim-scenario · dashboard"),
        ("② Score & route", "telemetry.enriched · incidents (warning/critical)"),
        ("③ RAG diagnose", "diagnosis_query → Atlas vector search → diagnosis_retrieved → Gemini"),
        ("④ Plan & act", "actions.planned → actions.executed · work_orders → MongoDB"),
        ("⑤ Verify", "incidents.resolved"),
    ]
    col_w = w // len(stages)
    for i, (title, detail) in enumerate(stages):
        cx = x + i * col_w + 10
        draw.text((cx, y), title, fill=(120, 180, 255), font=bold)
        draw.text((cx, y + 20), detail, fill=(190, 190, 190), font=tiny)
    return y + 52


def main() -> None:
    title_font, sub_font, tiny_font, _ = _fonts()

    left = enhance_panel(crop_graph(LEFT, 6))
    right = enhance_panel(crop_graph(RIGHT, 42))

    panels = [left, right]
    labels = ["Ingestion · Simulation · Actions", "Diagnosis · Vector RAG · Downstream"]

    header_h = 52
    sub_h = 30
    legend_h = 78
    pad = 14
    max_h = max(p.height for p in panels)
    graph_w = sum(p.width for p in panels)
    total_w = graph_w + pad * 2
    total_h = header_h + sub_h + max_h + legend_h + pad * 2

    canvas = Image.new("RGB", (total_w, total_h), BG)
    draw = ImageDraw.Draw(canvas)

    draw.text(
        (pad, 12),
        "GridSentinel — Confluent Cloud Stream Lineage",
        fill=(245, 245, 245),
        font=title_font,
    )
    draw.text(
        (pad, 34),
        "confluent-ai-day · cluster_0  ·  end-to-end: telemetry → incidents → RAG diagnosis → actions → recovery",
        fill=(140, 140, 145),
        font=tiny_font,
    )

    y_graph = header_h + sub_h
    x = pad
    for panel, label in zip(panels, labels):
        draw.text((x, y_graph - 22), label, fill=(100, 160, 220), font=sub_font)
        y_off = y_graph + (max_h - panel.height) // 2
        canvas.paste(panel, (x, y_off))
        x += panel.width

    legend_y = y_graph + max_h + 8
    draw.line([(pad, legend_y), (total_w - pad, legend_y)], fill=(50, 50, 55), width=1)
    draw_legend(draw, pad, legend_y + 10, total_w - pad * 2, (title_font, sub_font, tiny_font, _))

    # Icon key (from partial screenshots: pink=topic, blue=Flink, green=custom app)
    key_y = total_h - 22
    draw.text((pad, key_y), "■ topic", fill=(220, 120, 160), font=tiny_font)
    draw.text((pad + 72, key_y), "● Flink statement", fill=(90, 150, 230), font=tiny_font)
    draw.text((pad + 220, key_y), "◆ custom app (sim / dashboard)", fill=(80, 190, 140), font=tiny_font)

    OUT.parent.mkdir(parents=True, exist_ok=True)
    canvas.save(OUT, format="PNG", compress_level=1)
    print(f"Saved {OUT}")
    print(f"Size: {canvas.size[0]}x{canvas.size[1]} px ({SCALE}x graph upscale, lossless PNG)")


if __name__ == "__main__":
    main()
