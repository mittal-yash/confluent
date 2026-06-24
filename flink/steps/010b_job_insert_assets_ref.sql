-- Long-running job: re-key the connector's assets topic by asset_id.
INSERT INTO assets_ref
SELECT
  asset_id,
  lat,
  lon,
  tie_assets
FROM `gridsentinel.mongo.gridsentinel.assets`;
