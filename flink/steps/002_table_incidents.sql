CREATE TABLE `gridsentinel.incidents` (
  asset_id         STRING,
  incident_id      STRING,
  asset_type       STRING,
  `ts`             TIMESTAMP_LTZ(3),
  region           STRING,
  substation       STRING,
  signal           STRING,
  `value`          DOUBLE,
  threshold        DOUBLE,
  zscore           DOUBLE,
  fault_score      DOUBLE,
  severity         STRING,
  warning_temp_c   DOUBLE,
  critical_temp_c  DOUBLE,
  warning_vibration_mm_s DOUBLE,
  nominal_oil_pressure_kpa DOUBLE,
  rated_load_mw    DOUBLE,
  tie_assets       STRING,
  reasoning_needed BOOLEAN
) DISTRIBUTED BY (asset_id) INTO 6 BUCKETS
WITH ('changelog.mode' = 'append', 'value.format' = 'avro-registry');
