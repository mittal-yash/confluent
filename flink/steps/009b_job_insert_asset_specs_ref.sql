-- Long-running job: re-key the connector's asset_specs topic by asset_id.
-- NOTE the doubled `gridsentinel` in the source topic name — that is what the
-- MongoDB Source Connector actually produces (topic.prefix=gridsentinel.mongo,
-- database=gridsentinel, collection=asset_specs). Verify with SHOW TABLES LIKE
-- '%asset_specs%' and adjust if your connector uses a different prefix.
INSERT INTO asset_specs_ref
SELECT
  asset_id,
  asset_type,
  warning_temp_c,
  critical_temp_c,
  warning_vibration_mm_s,
  nominal_oil_pressure_kpa,
  rated_load_mw
FROM `gridsentinel.mongo.gridsentinel.asset_specs`;
