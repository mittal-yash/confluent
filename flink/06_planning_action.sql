-- =====================================================================
-- 06 - Planning + action, in Flink.
-- Validates Gemini's action against the constrained set, maps severity to
-- priority/SLA, selects the nearest qualified AVAILABLE crew from live crew
-- GPS, then ACTS: writes the control command (the laptop simulator obeys it
-- and the asset physically recovers), a durable work order, and an audit row.
-- =====================================================================

-- Latest position/status per crew (from live crew GPS on the laptop).
CREATE VIEW crew_latest AS
SELECT crew_id, region, lat, lon, status, skills FROM (
  SELECT crew_id, region, lat, lon, status, skills,
    ROW_NUMBER() OVER (PARTITION BY crew_id ORDER BY `$rowtime` DESC) AS rn
  FROM `gridsentinel.crew.location`
) WHERE rn = 1;

-- Diagnosis + validated action + required crew skill + asset location.
CREATE VIEW plan_base AS
SELECT
  d.incident_id, d.asset_id, d.asset_type, d.region, d.severity, d.tie_assets,
  d.dx_root_cause, d.dx_rationale, d.dx_confidence,
  CASE WHEN d.dx_recommended_action IN
        ('monitor','throttle_load','reroute_load','dispatch_crew','isolate')
       THEN d.dx_recommended_action ELSE 'monitor' END AS action_type,
  CASE d.asset_type
       WHEN 'transformer' THEN 'transformer'
       WHEN 'transmission_line' THEN 'line'
       WHEN 'water_pump' THEN 'pump'
       ELSE 'general' END AS req_skill,
  CAST(r.lat AS DOUBLE) AS asset_lat,
  CAST(r.lon AS DOUBLE) AS asset_lon
FROM `gridsentinel.incidents.diagnosed` AS d
LEFT JOIN `gridsentinel.mongo.assets` FOR SYSTEM_TIME AS OF d.`$rowtime` AS r
  ON d.asset_id = r.asset_id;

-- Rank qualified available crews: same-region first, then nearest (cheap
-- squared-distance proxy is fine for ranking).
CREATE VIEW plan_ranked AS
SELECT *, ROW_NUMBER() OVER (
    PARTITION BY incident_id
    ORDER BY region_match DESC, dist ASC
  ) AS rn
FROM (
  SELECT
    b.*,
    c.crew_id AS cand_crew_id,
    CASE WHEN c.region = b.region THEN 1 ELSE 0 END AS region_match,
    CASE WHEN c.crew_id IS NULL THEN 1e18
         ELSE (b.asset_lat - c.lat) * (b.asset_lat - c.lat)
            + (b.asset_lon - c.lon) * (b.asset_lon - c.lon) END AS dist
  FROM plan_base AS b
  LEFT JOIN crew_latest AS c
    ON c.status = 'available'
   AND (ARRAY_CONTAINS(c.skills, b.req_skill) OR ARRAY_CONTAINS(c.skills, 'general'))
);

-- One validated plan per incident.
INSERT INTO `gridsentinel.actions.planned`
SELECT
  asset_id,
  'PLAN-' || SUBSTRING(incident_id FROM 1 FOR 8) AS plan_id,
  incident_id, asset_type, region,
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

-- ACT 1/3: audit every executed action (dashboard ACT feed + verifier input).
INSERT INTO `gridsentinel.actions.executed`
SELECT
  asset_id,
  CAST(UUID() AS STRING) AS action_id,
  incident_id,
  CAST(CURRENT_TIMESTAMP AS TIMESTAMP_LTZ(3)) AS `ts`,
  action_type, rationale,
  CASE WHEN action_type = 'monitor' THEN 'completed' ELSE 'issued' END AS status,
  'flink-action' AS issued_by
FROM `gridsentinel.actions.planned`;

-- ACT 2/3: control command the laptop simulator obeys -> physical recovery.
INSERT INTO `gridsentinel.control`
SELECT
  asset_id,
  CAST(UUID() AS STRING) AS action_id,
  incident_id,
  CAST(CURRENT_TIMESTAMP AS TIMESTAMP_LTZ(3)) AS `ts`,
  action_type
FROM `gridsentinel.actions.planned`
WHERE action_type IN ('throttle_load','reroute_load','isolate','dispatch_crew');

-- ACT 3/3: durable work order -> MongoDB via the sink connector.
INSERT INTO `gridsentinel.work.orders`
SELECT
  'WO-' || SUBSTRING(REPLACE(CAST(UUID() AS STRING), '-', '') FROM 1 FOR 8) AS work_order_id,
  incident_id, asset_id,
  CAST(CURRENT_TIMESTAMP AS TIMESTAMP_LTZ(3)) AS `ts`,
  priority, crew_id, sla_hours,
  SUBSTRING(action_type || ' on ' || asset_id || '. Root cause: ' || COALESCE(root_cause,'') FROM 1 FOR 500) AS description,
  'open' AS status
FROM `gridsentinel.actions.planned`
WHERE need_work_order = TRUE;
