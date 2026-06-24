-- Re-keyed, versioned reference table for the temporal join.
-- WHY: the MongoDB Source Connector topic
--   `gridsentinel.mongo.gridsentinel.asset_specs`  (topic.prefix + db + collection)
-- is an auto-inferred APPEND table with NO primary key, so it can't be the
-- build side of an event-time temporal join. We re-key it by asset_id into an
-- UPSERT table (Kafka key = asset_id) that keeps the latest spec per asset.
CREATE TABLE asset_specs_ref (
  asset_id                 STRING,
  asset_type               STRING,
  warning_temp_c           DOUBLE,
  critical_temp_c          DOUBLE,
  warning_vibration_mm_s   DOUBLE,
  nominal_oil_pressure_kpa DOUBLE,
  rated_load_mw            DOUBLE,
  PRIMARY KEY (asset_id) NOT ENFORCED
) DISTRIBUTED BY (asset_id) INTO 6 BUCKETS
WITH ('changelog.mode' = 'upsert', 'value.format' = 'avro-registry');
