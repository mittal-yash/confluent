-- Re-keyed, versioned asset-registry table (lat/lon + tie routes for planning).
-- Same reasoning as 009: re-key the connector topic
--   `gridsentinel.mongo.gridsentinel.assets`
-- into an UPSERT table keyed by asset_id so it can drive temporal joins.
CREATE TABLE assets_ref (
  asset_id    STRING,
  lat         DOUBLE,
  lon         DOUBLE,
  tie_assets  ARRAY<STRING>,
  PRIMARY KEY (asset_id) NOT ENFORCED
) DISTRIBUTED BY (asset_id) INTO 6 BUCKETS
WITH ('changelog.mode' = 'upsert', 'value.format' = 'avro-registry');
