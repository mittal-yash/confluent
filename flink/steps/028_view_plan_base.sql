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

LEFT JOIN assets_ref FOR SYSTEM_TIME AS OF d.`$rowtime` AS r

  ON d.asset_id = r.asset_id;

