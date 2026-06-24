from pathlib import Path

ROOT = Path(__file__).resolve().parents[1] / "flink" / "steps"
OUT = Path(__file__).resolve().parents[1] / "flink" / "gridsentinel_full_pipeline.sql"

ORDER = [
    "001_table_telemetry_enriched.sql",
    "002_table_incidents.sql",
    "003_table_incidents_diagnosed.sql",
    "004_table_actions_planned.sql",
    "005_table_actions_executed.sql",
    "006_table_control.sql",
    "007_table_work_orders.sql",
    "008_table_incidents_resolved.sql",
    "009_table_asset_specs_ref.sql",
    "009b_job_insert_asset_specs_ref.sql",
    "010_table_assets_ref.sql",
    "010b_job_insert_assets_ref.sql",
    "011_view_telemetry_anomaly.sql",
    "012_view_telemetry_scored.sql",
    "013_view_telemetry_graded.sql",
    "014_view_telemetry_severity.sql",
    "015_job_insert_enriched.sql",
    "016_job_insert_incidents.sql",
    "017_connection_gemini.sql",
    "018_model_diagnosis.sql",
    "019_connection_gemini_embed.sql",
    "020_model_gemini_embed.sql",
    "021_connection_mongodb.sql",
    "022_table_manual_chunks_vec.sql",
    "023_table_diagnosis_query.sql",
    "023b_job_insert_diagnosis_query.sql",
    "024_table_diagnosis_retrieved.sql",
    "024b_job_insert_diagnosis_retrieved.sql",
    "025_job_insert_diagnosed_rag.sql",
    "026_job_insert_diagnosed_gated.sql",
    "027_view_crew_latest.sql",
    "028_view_plan_base.sql",
    "029_view_plan_ranked.sql",
    "030_job_insert_planned.sql",
    "031_job_insert_executed.sql",
    "032_job_insert_control.sql",
    "033_job_insert_work_orders.sql",
    "034_view_recovery_candidates.sql",
    "035_job_insert_resolved.sql",
]

HEADER = """-- =============================================================================
-- GridSentinel — full Flink SQL pipeline (run order)
-- =============================================================================
-- Confluent Cloud runs ONE statement at a time. Execute each block separately,
-- in order. Leave *_job_* INSERT statements running (streaming jobs).
--
-- Before steps 015/016 (scoring INSERT jobs), run once:
--   SET 'sql.tables.scan.idle-timeout' = '15s';
--
-- Prerequisite topics (simulator / connectors):
--   gridsentinel.telemetry
--   gridsentinel.crew.location
--   gridsentinel.mongo.gridsentinel.asset_specs
--   gridsentinel.mongo.gridsentinel.assets
-- =============================================================================

"""


def main() -> None:
    parts = [HEADER]
    for name in ORDER:
        body = (ROOT / name).read_text(encoding="utf-8").strip()
        step = name.replace(".sql", "")
        parts.append("-- -----------------------------------------------------------------------------")
        parts.append(f"-- STEP {step}")
        parts.append("-- -----------------------------------------------------------------------------")
        parts.append(body)
        parts.append("")
    OUT.write_text("\n".join(parts) + "\n", encoding="utf-8")
    print(f"Wrote {OUT} ({len(ORDER)} steps, {OUT.stat().st_size} bytes)")


if __name__ == "__main__":
    main()
