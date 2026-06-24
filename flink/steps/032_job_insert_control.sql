INSERT INTO `gridsentinel.control`
SELECT
  incident_id,
  asset_id,
  'CTL-' || SUBSTRING(incident_id FROM 1 FOR 8) AS action_id,
  CAST(CURRENT_TIMESTAMP AS TIMESTAMP_LTZ(3)) AS `ts`,
  action_type
FROM `gridsentinel.actions.planned`
WHERE action_type IN ('throttle_load','reroute_load','isolate','dispatch_crew');
