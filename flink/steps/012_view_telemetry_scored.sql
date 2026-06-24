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

LEFT JOIN asset_specs_ref FOR SYSTEM_TIME AS OF a.`$rowtime` AS s

  ON a.asset_id = s.asset_id

LEFT JOIN assets_ref FOR SYSTEM_TIME AS OF a.`$rowtime` AS r

  ON a.asset_id = r.asset_id;

