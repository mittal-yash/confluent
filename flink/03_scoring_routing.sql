-- =====================================================================
-- 03 - Scoring + routing (deterministic, no external model needed here).
-- Joins the z-scored stream to the governed specs, computes an interpretable
-- logistic fault_score INLINE (identical math to flink/scoring.py), grades
-- severity, writes the enriched stream for the dashboard, and routes only
-- warning/critical readings onward as incidents (the cost gate).
-- =====================================================================

-- Join telemetry+anomaly to the latest specs/registry, normalize features.
CREATE VIEW telemetry_scored AS
SELECT
  a.asset_id,
  a.asset_type,
  a.event_time,
  a.region,
  a.substation,
  a.load_pct,
  a.temp_c,
  a.vibration_mm_s,
  a.oil_pressure_kpa,
  a.temp_zscore,
  COALESCE(s.warning_temp_c, 85.0)            AS warning_temp_c,
  COALESCE(s.critical_temp_c, 95.0)           AS critical_temp_c,
  COALESCE(s.warning_vibration_mm_s, 0.0)     AS warning_vibration_mm_s,
  COALESCE(s.nominal_oil_pressure_kpa, 0.0)   AS nominal_oil_pressure_kpa,
  COALESCE(s.rated_load_mw, 0.0)              AS rated_load_mw,
  COALESCE(ARRAY_JOIN(r.tie_assets, ','), '') AS tie_assets,
  -- normalized features (mirror scoring.py)
  GREATEST(0.0, (a.temp_c - COALESCE(s.warning_temp_c, 85.0))
        / NULLIF(COALESCE(s.critical_temp_c, 95.0) - COALESCE(s.warning_temp_c, 85.0), 0)) AS temp_ratio,
  GREATEST(0.0, (a.load_pct - 100.0) / 20.0) AS load_over,
  CASE WHEN COALESCE(s.warning_vibration_mm_s, 0.0) > 0
       THEN GREATEST(0.0, (a.vibration_mm_s - s.warning_vibration_mm_s) / s.warning_vibration_mm_s)
       ELSE 0.0 END AS vib_ratio,
  CASE WHEN COALESCE(s.nominal_oil_pressure_kpa, 0.0) > 0
       THEN GREATEST(0.0, (s.nominal_oil_pressure_kpa - a.oil_pressure_kpa) / s.nominal_oil_pressure_kpa)
       ELSE 0.0 END AS pressure_drop
FROM telemetry_anomaly AS a
LEFT JOIN `gridsentinel.mongo.asset_specs` FOR SYSTEM_TIME AS OF a.`$rowtime` AS s
  ON a.asset_id = s.asset_id
LEFT JOIN `gridsentinel.mongo.assets` FOR SYSTEM_TIME AS OF a.`$rowtime` AS r
  ON a.asset_id = r.asset_id;

-- Logistic fault_score + severity grade.
CREATE VIEW telemetry_graded AS
SELECT
  *,
  1.0 / (1.0 + EXP(-( -3.6
        + 4.2 * temp_ratio
        + 3.1 * load_over
        + 2.6 * vib_ratio
        + 2.4 * pressure_drop ))) AS fault_score_calc
FROM telemetry_scored;

CREATE VIEW telemetry_severity AS
SELECT
  *,
  CASE
    WHEN temp_c >= critical_temp_c
      OR (warning_vibration_mm_s > 0 AND vibration_mm_s >= warning_vibration_mm_s * 1.5)
      OR (nominal_oil_pressure_kpa > 0 AND (nominal_oil_pressure_kpa - oil_pressure_kpa) / nominal_oil_pressure_kpa >= 0.30)
      OR load_pct >= 115
      OR fault_score_calc >= 0.80
      THEN 'critical'
    WHEN temp_c >= warning_temp_c
      OR (warning_vibration_mm_s > 0 AND vibration_mm_s >= warning_vibration_mm_s)
      OR (nominal_oil_pressure_kpa > 0 AND (nominal_oil_pressure_kpa - oil_pressure_kpa) / nominal_oil_pressure_kpa >= 0.15)
      OR load_pct >= 102
      OR fault_score_calc >= 0.50
      THEN 'warning'
    ELSE 'info'
  END AS severity
FROM telemetry_graded;

-- Enriched stream for the dashboard.
INSERT INTO `gridsentinel.telemetry.enriched`
SELECT
  asset_id, asset_type, event_time, region, substation, load_pct, temp_c,
  vibration_mm_s, oil_pressure_kpa, fault_score_calc, severity,
  warning_temp_c, critical_temp_c
FROM telemetry_severity;

-- Route incidents (warning/critical only) onward to Gemini diagnosis.
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
  -- cost gate: only genuinely worrying incidents get the (paid) Gemini call
  (severity = 'critical' OR fault_score_calc >= 0.5 OR ABS(temp_zscore) >= 3.0) AS reasoning_needed
FROM telemetry_severity
WHERE severity IN ('warning', 'critical');
