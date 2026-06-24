INSERT INTO `gridsentinel.telemetry.enriched`
SELECT
  asset_id, asset_type, event_time, region, substation, load_pct, temp_c,
  vibration_mm_s, oil_pressure_kpa, fault_score_calc, severity,
  warning_temp_c, critical_temp_c
FROM telemetry_severity;
