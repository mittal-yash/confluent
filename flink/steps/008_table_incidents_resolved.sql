CREATE TABLE `gridsentinel.incidents.resolved` (
  incident_id      STRING,
  asset_id         STRING,
  status           STRING,
  action_type      STRING,
  recovery_seconds DOUBLE,
  resolved_ts      TIMESTAMP_LTZ(3),
  PRIMARY KEY (incident_id) NOT ENFORCED
) DISTRIBUTED BY (incident_id) INTO 6 BUCKETS
-- upsert: 035 reads the upsert actions.executed changelog (via recovery_candidates)
-- and dedups Top-1 per incident. One resolution row per incident.
WITH ('changelog.mode' = 'upsert', 'value.format' = 'avro-registry');
