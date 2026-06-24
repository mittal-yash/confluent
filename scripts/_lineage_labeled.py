"""Readable labeled reference for the Confluent Stream Lineage (full zoom-out decode)."""
from __future__ import annotations

from pathlib import Path

from PIL import Image, ImageDraw, ImageFont

OUT = Path(__file__).resolve().parents[1] / "docs" / "gridsentinel-lineage-labeled.png"

W, H = 2400, 1100
BG = (16, 16, 20)
PINK = (235, 120, 170)
BLUE = (90, 155, 235)
GREEN = (80, 195, 140)
WHITE = (240, 240, 242)
GRAY = (150, 150, 155)
ARROW = (110, 110, 180)


def fonts():
    try:
        return (
            ImageFont.truetype("segoeuib.ttf", 26),
            ImageFont.truetype("segoeui.ttf", 17),
            ImageFont.truetype("segoeui.ttf", 14),
            ImageFont.truetype("segoeui.ttf", 12),
        )
    except OSError:
        d = ImageFont.load_default()
        return d, d, d, d


def box(draw, x, y, w, h, color, title, sub="", kind=""):
    draw.rounded_rectangle((x, y, x + w, y + h), radius=8, fill=(28, 28, 34), outline=color, width=2)
    draw.ellipse((x + 10, y + 12, x + 28, y + 30), fill=color)
    draw.text((x + 36, y + 8), title, fill=WHITE, font=fonts()[1])
    if sub:
        draw.text((x + 36, y + 28), sub, fill=GRAY, font=fonts()[3])
    if kind:
        draw.text((x + 36, y + h - 20), kind, fill=color, font=fonts()[3])


def arrow(draw, x1, y1, x2, y2):
    draw.line((x1, y1, x2, y2), fill=ARROW, width=2)
    if x2 > x1:
        draw.polygon([(x2, y2), (x2 - 10, y2 - 5), (x2 - 10, y2 + 5)], fill=ARROW)


def main() -> None:
    title_f, _, section_f, _ = fonts()
    img = Image.new("RGB", (W, H), BG)
    d = ImageDraw.Draw(img)

    d.text((40, 24), "GridSentinel — Stream Lineage (readable decode)", fill=WHITE, font=title_f)
    d.text(
        (40, 58),
        "Pink = Kafka topic  ·  Blue = Flink statement/job  ·  Green = custom app (simulator / dashboard)",
        fill=GRAY,
        font=section_f,
    )

    # Row Y positions
    y_ingest, y_score, y_diag, y_act, y_verify = 130, 280, 430, 620, 800
    bw, bh = 220, 52

    # --- LEFT: ingestion ---
    d.text((40, y_ingest - 28), "① Ingestion (far left of zoom-out)", fill=PINK, font=section_f)
    box(d, 40, y_ingest, bw, bh, PINK, "gridsentinel.telemetry", "raw SCADA stream", "TOPIC")
    box(d, 40, y_ingest + 70, bw, bh, PINK, "gridsentinel.scenario", "fault injection", "TOPIC")
    box(d, 40, y_ingest + 140, bw, bh, GREEN, "sim-scenario", "Python simulator", "CUSTOM APP")
    box(d, 40, y_ingest + 210, bw, bh, GREEN, "dashboard-*", "Streamlit UI", "CUSTOM APP")
    box(d, 300, y_ingest + 70, bw, bh, PINK, "gridsentinel.weather", "ambient context", "TOPIC")
    box(d, 300, y_ingest + 140, bw, bh, PINK, "gridsentinel.crew.locations", "crew GPS", "TOPIC")

    arrow(d, 260, y_ingest + 26, 300, y_ingest + 96)
    arrow(d, 260, y_ingest + 96, 300, y_ingest + 166)

    # --- scoring / enrich ---
    d.text((560, y_score - 28), "② Score & route", fill=BLUE, font=section_f)
    box(d, 560, y_score, 200, bh, BLUE, "telemetry_anomaly", "z-score window", "VIEW")
    box(d, 560, y_score + 70, 200, bh, BLUE, "telemetry_scored", "temporal join specs", "VIEW")
    box(d, 560, y_score + 140, 200, bh, BLUE, "telemetry_graded", "logistic fault_score", "VIEW")
    box(d, 800, y_score + 35, 240, bh, PINK, "gridsentinel.telemetry.enriched", "all assets + severity", "TOPIC")
    box(d, 800, y_score + 120, 200, bh, PINK, "gridsentinel.incidents", "warning / critical", "TOPIC")
    box(d, 800, y_score + 200, 200, bh, PINK, "asset_specs_ref", "Mongo ref (upsert)", "TABLE")
    arrow(d, 760, y_score + 26, 800, y_score + 61)
    arrow(d, 760, y_score + 96, 800, y_score + 61)
    arrow(d, 1000, y_score + 87, 1080, y_score + 150)

    # --- diagnosis RAG (center-right cluster) ---
    d.text((1080, y_diag - 28), "③ Diagnosis + RAG (dense middle of zoom-out)", fill=BLUE, font=section_f)
    box(d, 1080, y_diag, 200, bh, BLUE, "023b INSERT job", "AI_EMBEDDING", "FLINK JOB")
    box(d, 1080, y_diag + 80, 200, bh, PINK, "diagnosis_query", "incident + qvec", "TOPIC")
    box(d, 1320, y_diag + 20, 200, bh, PINK, "manual_chunks_vec", "Atlas vector index", "MONGO TABLE")
    box(d, 1320, y_diag + 100, 200, bh, BLUE, "024b INSERT job", "VECTOR_SEARCH_AGG", "FLINK JOB")
    box(d, 1560, y_diag + 60, 220, bh, PINK, "diagnosis_retrieved", "manual context", "TOPIC")
    box(d, 1820, y_diag + 60, 200, bh, BLUE, "025 INSERT job", "ML_PREDICT Gemini", "FLINK JOB")
    box(d, 2060, y_diag + 60, 240, bh, PINK, "gridsentinel.incidents.diagnosed", "dx_* JSON fields", "TOPIC")
    arrow(d, 1280, y_diag + 26, 1320, y_diag + 46)
    arrow(d, 1280, y_diag + 106, 1320, y_diag + 126)
    arrow(d, 1520, y_diag + 126, 1560, y_diag + 86)
    arrow(d, 1780, y_diag + 86, 1820, y_diag + 86)
    arrow(d, 2020, y_diag + 86, 2060, y_diag + 86)
    d.line((1320, y_diag + 46, 1320, y_diag + 126), fill=ARROW, width=2)

    # --- plan & act ---
    d.text((560, y_act - 28), "④ Plan & act (right cluster)", fill=BLUE, font=section_f)
    box(d, 560, y_act, 200, bh, BLUE, "plan_base / plan_ranked", "crew + tie_assets", "VIEW")
    box(d, 800, y_act, 220, bh, PINK, "gridsentinel.actions.planned", "throttle / reroute / isolate", "TOPIC")
    box(d, 1080, y_act, 200, bh, BLUE, "030–032 INSERT jobs", "plan · control · execute", "FLINK JOB")
    box(d, 1320, y_act, 220, bh, PINK, "gridsentinel.actions.executed", "commands sent", "TOPIC")
    box(d, 1580, y_act, 200, bh, PINK, "gridsentinel.control", "load throttle signals", "TOPIC")
    box(d, 1580, y_act + 80, 200, bh, PINK, "gridsentinel.work.orders", "→ MongoDB sink", "TOPIC")
    arrow(d, 760, y_act + 26, 800, y_act + 26)
    arrow(d, 1020, y_act + 26, 1080, y_act + 26)
    arrow(d, 1280, y_act + 26, 1320, y_act + 26)

    # --- verify + loop back ---
    d.text((1820, y_verify - 28), "⑤ Verify (far right + loop back)", fill=PINK, font=section_f)
    box(d, 1820, y_verify, 200, bh, BLUE, "034 recovery_candidates", "telemetry normal?", "VIEW")
    box(d, 2060, y_verify, 240, bh, PINK, "gridsentinel.incidents.resolved", "closed incidents", "TOPIC")
    arrow(d, 2020, y_verify + 26, 2060, y_verify + 26)
    # feedback loop line
    d.arc((60, 900, 2300, 1040), start=10, end=170, fill=ARROW, width=2)
    d.text((900, 1020), "← feedback: resolved / telemetry loop back to ingestion", fill=GRAY, font=section_f)

    # Mongo ref branch (top)
    d.text((560, 100), "Reference data (MongoDB Source Connector)", fill=GREEN, font=section_f)
    box(d, 560, 130, 230, bh, PINK, "gridsentinel.mongo…asset_specs", "thresholds", "TOPIC")
    box(d, 820, 130, 210, bh, PINK, "gridsentinel.mongo…assets", "tie_assets, GPS", "TOPIC")

    OUT.parent.mkdir(parents=True, exist_ok=True)
    img.save(OUT, format="PNG", compress_level=1)
    print(f"Saved {OUT} ({W}x{H})")


if __name__ == "__main__":
    main()
