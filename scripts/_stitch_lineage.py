"""Stitch Confluent Stream Lineage screenshots into one horizontal panorama."""
from __future__ import annotations

from pathlib import Path

from PIL import Image, ImageDraw, ImageFont

ASSETS = Path(
    r"C:\Users\ymittal\.cursor\projects"
    r"\c-Users-ymittal-OneDrive-Burns-McDonnell-Desktop-confluent-metrics\assets"
)
OUT = Path(__file__).resolve().parents[1] / "docs" / "gridsentinel-lineage-panorama.png"

# Left = ingestion / simulation / actions  |  Right = diagnosis / RAG
LEFT = ASSETS / (
    "c__Users_ymittal_AppData_Roaming_Cursor_User_workspaceStorage_empty-window_"
    "images_image-28eb8e13-8d5b-44d9-b427-89a1a1fc98d2.png"
)
RIGHT = ASSETS / (
    "c__Users_ymittal_AppData_Roaming_Cursor_User_workspaceStorage_empty-window_"
    "images_image-5edb4a69-fd01-4c62-a6ef-88f42adf4a61.png"
)

# Confluent lineage graph area starts below the filter bar (full pixel crop, no resize).
CROP_TOP = 118
CROP_BOTTOM_LEFT = 6
CROP_BOTTOM_RIGHT = 42  # trim Windows taskbar on right capture


def crop_graph(im: Image.Image, bottom_trim: int) -> Image.Image:
    w, h = im.size
    return im.crop((0, CROP_TOP, w, h - bottom_trim))


def main() -> None:
    panels = [
        crop_graph(Image.open(LEFT).convert("RGB"), CROP_BOTTOM_LEFT),
        crop_graph(Image.open(RIGHT).convert("RGB"), CROP_BOTTOM_RIGHT),
    ]
    labels = ["Ingestion · Simulation · Actions", "Diagnosis · Vector RAG · Downstream"]

    gap = 0
    label_h = 36
    header_h = 44
    bg = (18, 18, 18)

    max_h = max(p.height for p in panels)
    total_w = sum(p.width for p in panels) + gap * (len(panels) - 1)
    canvas_h = header_h + label_h + max_h + 16

    canvas = Image.new("RGB", (total_w, canvas_h), bg)
    draw = ImageDraw.Draw(canvas)

    try:
        title_font = ImageFont.truetype("segoeui.ttf", 20)
        label_font = ImageFont.truetype("segoeui.ttf", 14)
    except OSError:
        title_font = ImageFont.load_default()
        label_font = title_font

    draw.text(
        (16, 10),
        "GridSentinel — Confluent Cloud Stream Lineage (confluent-ai-day · cluster_0)",
        fill=(230, 230, 230),
        font=title_font,
    )

    x = 0
    y_base = header_h + label_h
    for i, (panel, label) in enumerate(zip(panels, labels)):
        draw.text((x + 8, header_h + 8), label, fill=(160, 160, 160), font=label_font)
        y_off = y_base + (max_h - panel.height) // 2
        canvas.paste(panel, (x, y_off))
        x += panel.width
        if gap:
            x += gap

    OUT.parent.mkdir(parents=True, exist_ok=True)
    canvas.save(OUT, format="PNG", compress_level=1)
    print(f"Saved {OUT}")
    print(f"Size: {canvas.size[0]}x{canvas.size[1]} px (no downscaling)")


if __name__ == "__main__":
    main()
