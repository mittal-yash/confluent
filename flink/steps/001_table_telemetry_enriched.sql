CREATE TABLE `gridsentinel.telemetry.enriched` (
  asset_id         STRING,
  asset_type       STRING,
  `ts`             TIMESTAMP_LTZ(3),
  region           STRING,
  substation       STRING,
  load_pct         DOUBLE,
  temp_c           DOUBLE,
  vibration_mm_s   DOUBLE,
  oil_pressure_kpa DOUBLE,
  fault_score      DOUBLE,
  severity         STRING,
  warning_temp_c   DOUBLE,
  critical_temp_c  DOUBLE
) DISTRIBUTED BY (asset_id) INTO 6 BUCKETS
WITH ('changelog.mode' = 'append', 'value.format' = 'avro-registry');
