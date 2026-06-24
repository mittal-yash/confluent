-- actions.executed is an UPSERT changelog (it reads the upsert actions.planned),
-- and an interval join (BETWEEN two $rowtime time-attributes) only accepts
-- append-only inputs. Casting $rowtime to a plain TIMESTAMP(3) removes the
-- time-attribute property, so Flink uses a REGULAR join, which consumes the
-- changelog. The 10-minute recovery window is applied as an ordinary predicate.
CREATE VIEW recovery_candidates AS
SELECT
  a.incident_id,
  a.asset_id,
  a.action_type,
  a.act_time,
  e.rec_time
FROM (
  SELECT incident_id, asset_id, action_type,
         CAST(`$rowtime` AS TIMESTAMP(3)) AS act_time
  FROM `gridsentinel.actions.executed`
  WHERE action_type IN ('throttle_load','reroute_load','isolate','dispatch_crew')
) AS a
JOIN (
  SELECT asset_id, CAST(`$rowtime` AS TIMESTAMP(3)) AS rec_time
  FROM `gridsentinel.telemetry.enriched`
  WHERE severity = 'info'
) AS e
  ON a.asset_id = e.asset_id
 AND e.rec_time BETWEEN a.act_time AND a.act_time + INTERVAL '10' MINUTE;
