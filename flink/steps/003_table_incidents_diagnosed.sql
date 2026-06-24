CREATE TABLE `gridsentinel.incidents.diagnosed` (
  asset_id             STRING,
  incident_id          STRING,
  asset_type           STRING,
  region               STRING,
  severity             STRING,
  signal               STRING,
  `value`              DOUBLE,
  fault_score          DOUBLE,
  zscore               DOUBLE,
  tie_assets           STRING,
  dx_root_cause        STRING,
  dx_recommended_action STRING,
  dx_rationale         STRING,
  dx_confidence        DOUBLE,
  dx_citations         ARRAY<ROW<`source` STRING, snippet STRING>>,
  dx_method            STRING,
  diagnosed_ts         TIMESTAMP_LTZ(3)
) DISTRIBUTED BY (asset_id) INTO 6 BUCKETS
WITH ('changelog.mode' = 'append', 'value.format' = 'avro-registry');
