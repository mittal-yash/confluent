INSERT INTO `gridsentinel.work.orders`
SELECT
  incident_id,
  'WO-' || SUBSTRING(incident_id FROM 1 FOR 8) AS work_order_id,
  asset_id,
  CAST(CURRENT_TIMESTAMP AS TIMESTAMP_LTZ(3)) AS `ts`,
  priority, crew_id, sla_hours,
  SUBSTRING(action_type || ' on ' || asset_id || '. Root cause: ' || COALESCE(root_cause,'') FROM 1 FOR 500) AS description,
  'open' AS status
FROM `gridsentinel.actions.planned`
WHERE need_work_order = TRUE;
