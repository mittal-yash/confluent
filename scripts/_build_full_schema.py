"""Build gridsentinel_full_schema.md — all topic/collection schemas in one file."""
from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SCHEMAS = ROOT / "schemas"
FLINK = ROOT / "flink" / "gridsentinel_full_pipeline.sql"
OUT = SCHEMAS / "gridsentinel_full_schema.md"

HEADER = """# GridSentinel — Full Schema Reference

All Kafka topic contracts (Avro + Flink DDL) and MongoDB collection shapes.
Schema Registry subjects use pattern: `{topic}-value`.

---

"""

MONGO_SECTION = """## MongoDB Atlas collections (`gridsentinel` database)

### `assets` (MongoDB Source Connector → `gridsentinel.mongo.gridsentinel.assets`)
```json
{
  "asset_id": "string",
  "name": "string",
  "asset_type": "transformer | transmission_line | substation | water_pump",
  "region": "string",
  "substation": "string",
  "voltage_kv": "int",
  "criticality": "critical | high | medium",
  "install_year": "int",
  "lat": "double",
  "lon": "double",
  "rated_load_mw": "double",
  "nominal_temp_c": "double",
  "warning_temp_c": "double",
  "critical_temp_c": "double",
  "nominal_vibration_mm_s": "double",
  "warning_vibration_mm_s": "double",
  "nominal_oil_pressure_kpa": "double",
  "feeds": ["string"],
  "tie_assets": ["string"],
  "spec_sheet_id": "string"
}
```

### `asset_specs` (Source Connector → `gridsentinel.mongo.gridsentinel.asset_specs`)
```json
{
  "spec_sheet_id": "string",
  "asset_id": "string",
  "asset_type": "string",
  "name": "string",
  "voltage_kv": "int",
  "rated_load_mw": "double",
  "nominal_temp_c": "double",
  "warning_temp_c": "double",
  "critical_temp_c": "double",
  "warning_vibration_mm_s": "double",
  "nominal_vibration_mm_s": "double",
  "nominal_oil_pressure_kpa": "double",
  "install_year": "int",
  "criticality": "string",
  "tie_assets": ["string"],
  "reference_guide": "string",
  "spec_text": "string"
}
```

### `crew` (static roster; live positions on `gridsentinel.crew.location` Kafka topic)
```json
{
  "crew_id": "string",
  "name": "string",
  "region": "string",
  "lat": "double",
  "lon": "double",
  "status": "available | enroute | on_site | off_shift",
  "skills": ["transformer", "line", "pump", "general"]
}
```

### `manual_chunks` (RAG corpus; queried by Flink `VECTOR_SEARCH_AGG`, not connector)
```json
{
  "text": "string",
  "source": "string",
  "chunk": "int",
  "asset_id": "string",
  "asset_type": "string",
  "embedding": ["float (3072 dimensions, gemini-embedding-001)"]
}
```
Vector index: `manuals_vector_index` on field `embedding`, 3072 dims, cosine.

### `work_orders` (MongoDB Sink Connector ← `gridsentinel.work.orders`)
```json
{
  "work_order_id": "string",
  "incident_id": "string",
  "asset_id": "string",
  "ts": "timestamp",
  "priority": "P1 | P2 | P3",
  "crew_id": "string",
  "sla_hours": "int",
  "description": "string",
  "status": "open | closed"
}
```

### `maintenance_history` (reference only, not in Flink pipeline)
```json
{
  "asset_id": "string",
  "ts": "long (epoch ms)",
  "note": "string",
  "technician": "string",
  "result": "ok | follow_up"
}
```

---

"""

SCENARIO_SECTION = """## Kafka: `gridsentinel.scenario` (JSON, simulator control topic)

```json
{
  "scenario": "heatwave | transformer_overload | bearing_failure | pump_cavitation | clear",
  "asset_id": "string",
  "region": "string",
  "intensity": "double",
  "ramp_seconds": "double",
  "ts": "long (epoch ms)"
}
```

---

"""


def _extract_create_tables(sql: str) -> list[str]:
    blocks: list[str] = []
    lines = sql.splitlines()
    i = 0
    while i < len(lines):
        line = lines[i]
        if line.strip().startswith("CREATE TABLE"):
            block = [line]
            i += 1
            while i < len(lines):
                block.append(lines[i])
                if lines[i].strip().endswith(";"):
                    break
                i += 1
            blocks.append("\n".join(block))
        i += 1
    return blocks


def main() -> None:
    parts = [HEADER]

    parts.append("## Kafka input topics — Avro (Schema Registry)\n\n")
    for name in ("telemetry.avsc", "crew_location.avsc", "weather.avsc"):
        body = (SCHEMAS / name).read_text(encoding="utf-8").strip()
        topic = {
            "telemetry.avsc": "gridsentinel.telemetry",
            "crew_location.avsc": "gridsentinel.crew.location",
            "weather.avsc": "gridsentinel.weather",
        }[name]
        parts.append(f"### `{topic}`\n\n```json\n{body}\n```\n\n")

    parts.append(SCENARIO_SECTION)
    parts.append("## Kafka / Flink tables — Flink SQL DDL\n\n")
    parts.append(
        "Flink-produced topics use `value.format = avro-registry`. "
        "Intermediate Flink tables (`asset_specs_ref`, `assets_ref`, "
        "`diagnosis_query`, `diagnosis_retrieved`) are Kafka-backed.\n\n"
    )
    parts.append("```sql\n")
    for block in _extract_create_tables(FLINK.read_text(encoding="utf-8")):
        parts.append(block.strip())
        parts.append("\n\n")
    parts.append("```\n\n")

    parts.append(MONGO_SECTION)

    parts.append("## Diagnosis model output (Gemini JSON in `dx_*` fields)\n\n")
    parts.append(
        "```json\n"
        "{\n"
        '  "root_cause": "string",\n'
        '  "recommended_action": "monitor | throttle_load | reroute_load | dispatch_crew | isolate",\n'
        '  "rationale": "string",\n'
        '  "confidence": "number 0..1",\n'
        '  "citation": "string"\n'
        "}\n"
        "```\n"
    )

    OUT.write_text("".join(parts), encoding="utf-8")
    print(f"Wrote {OUT} ({OUT.stat().st_size} bytes)")


if __name__ == "__main__":
    main()
