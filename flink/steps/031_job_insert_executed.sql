INSERT INTO `gridsentinel.actions.executed`
SELECT
  incident_id,
  asset_id,
  'ACT-' || SUBSTRING(incident_id FROM 1 FOR 8) AS action_id,
  CAST(CURRENT_TIMESTAMP AS TIMESTAMP_LTZ(3)) AS `ts`,
  action_type, rationale,
  CASE WHEN action_type = 'monitor' THEN 'completed' ELSE 'issued' END AS status,
  'flink-action' AS issued_by
FROM `gridsentinel.actions.planned`;
