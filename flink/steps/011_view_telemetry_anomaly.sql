CREATE VIEW telemetry_anomaly AS
SELECT
  asset_id,
  asset_type,
  `$rowtime`,
  `$rowtime` AS event_time,
  region,
  substation,
  load_pct,
  temp_c,
  ambient_c,
  vibration_mm_s,
  oil_pressure_kpa,
  CASE
    WHEN STDDEV_POP(temp_c) OVER w > 0
    THEN (temp_c - AVG(temp_c) OVER w) / STDDEV_POP(temp_c) OVER w
    ELSE 0.0
  END AS temp_zscore
FROM `gridsentinel.telemetry`
WINDOW w AS (
  PARTITION BY asset_id
  ORDER BY `$rowtime`
  ROWS BETWEEN 30 PRECEDING AND CURRENT ROW
);
