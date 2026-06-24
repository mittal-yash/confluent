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
