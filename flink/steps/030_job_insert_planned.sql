INSERT INTO `gridsentinel.actions.planned`
SELECT
  incident_id,
  asset_id,
  'PLAN-' || SUBSTRING(incident_id FROM 1 FOR 8) AS plan_id,
  asset_type, region,
  CAST(CURRENT_TIMESTAMP AS TIMESTAMP_LTZ(3)) AS `ts`,
  action_type,
  CASE WHEN action_type = 'throttle_load' THEN '52' ELSE '' END AS target_load_pct,
  tie_assets,
  (action_type IN ('dispatch_crew','isolate') OR severity = 'critical') AS need_crew,
  CASE WHEN (action_type IN ('dispatch_crew','isolate') OR severity = 'critical')
       THEN cand_crew_id ELSE CAST(NULL AS STRING) END AS crew_id,
  (action_type <> 'monitor') AS need_work_order,
  CASE severity WHEN 'critical' THEN 'P1' WHEN 'warning' THEN 'P2' ELSE 'P3' END AS priority,
  CASE severity WHEN 'critical' THEN 2 WHEN 'warning' THEN 8 ELSE 24 END AS sla_hours,
  severity, dx_root_cause AS root_cause, dx_rationale AS rationale, dx_confidence AS confidence
FROM plan_ranked
WHERE rn = 1;
