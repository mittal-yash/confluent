-- =====================================================================
-- 07 - Verification: close the loop, in Flink.
-- For every control action, find the FIRST subsequent healthy ('info') reading
-- for that asset within a recovery window, and emit a resolved event with the
-- measured recovery time. This is GridSentinel's headline sense->act->verify
-- closure - now fully server-side.
--
-- NOTE: a "did NOT recover within SLA -> escalate" branch is intentionally
-- omitted here (it needs MATCH_RECOGNIZE / timers and is easy to get wrong).
-- The first-recovery path below is what drives the live demo.
-- =====================================================================

-- actions.executed is an UPSERT changelog (reads upsert actions.planned); an
-- interval join (BETWEEN two $rowtime time-attributes) only accepts append-only
-- inputs. Casting $rowtime to plain TIMESTAMP(3) forces a REGULAR join, which
-- consumes the changelog. The 10-minute window becomes an ordinary predicate.
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

INSERT INTO `gridsentinel.incidents.resolved`
SELECT
  incident_id,
  asset_id,
  'resolved' AS status,
  action_type,
  CAST(TIMESTAMPDIFF(SECOND, act_time, rec_time) AS DOUBLE) AS recovery_seconds,
  CAST(rec_time AS TIMESTAMP_LTZ(3)) AS resolved_ts
FROM (
  SELECT *, ROW_NUMBER() OVER (
      PARTITION BY incident_id ORDER BY rec_time ASC
    ) AS rn
  FROM recovery_candidates
) WHERE rn = 1;
