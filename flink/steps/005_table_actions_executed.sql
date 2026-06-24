CREATE TABLE `gridsentinel.actions.executed` (
  incident_id  STRING,
  asset_id     STRING,
  action_id    STRING,
  `ts`         TIMESTAMP_LTZ(3),
  action_type  STRING,
  rationale    STRING,
  status       STRING,
  issued_by    STRING,
  PRIMARY KEY (incident_id) NOT ENFORCED
) DISTRIBUTED BY (incident_id) INTO 6 BUCKETS
-- upsert: reads the upsert actions.planned changelog. One current action/incident.
WITH ('changelog.mode' = 'upsert', 'value.format' = 'avro-registry');
