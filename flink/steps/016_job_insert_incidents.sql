INSERT INTO `gridsentinel.incidents`
SELECT
  asset_id,
  CAST(UUID() AS STRING) AS incident_id,
  asset_type, event_time, region, substation,
  CASE
    WHEN temp_c >= warning_temp_c THEN 'temp_c'
    WHEN warning_vibration_mm_s > 0 AND vibration_mm_s >= warning_vibration_mm_s THEN 'vibration_mm_s'
    WHEN load_pct >= 102 THEN 'load_pct'
    ELSE 'oil_pressure_kpa'
  END AS signal,
  temp_c AS `value`,
  warning_temp_c AS threshold,
  temp_zscore AS zscore,
  fault_score_calc AS fault_score,
  severity,
  warning_temp_c,
  critical_temp_c,
  warning_vibration_mm_s,
  nominal_oil_pressure_kpa,
  rated_load_mw,
  tie_assets,
  (severity = 'critical' OR fault_score_calc >= 0.5 OR ABS(temp_zscore) >= 3.0) AS reasoning_needed
FROM telemetry_severity
WHERE severity IN ('warning', 'critical');
