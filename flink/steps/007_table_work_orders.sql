CREATE TABLE `gridsentinel.work.orders` (
  incident_id   STRING,
  work_order_id STRING,
  asset_id      STRING,
  `ts`          TIMESTAMP_LTZ(3),
  priority      STRING,
  crew_id       STRING,
  sla_hours     INT,
  description   STRING,
  status        STRING,
  PRIMARY KEY (incident_id) NOT ENFORCED
) DISTRIBUTED BY (incident_id) INTO 6 BUCKETS
-- upsert keyed by incident_id (one current work order per incident). work_order_id
-- is derived deterministically from incident_id so upserts stay idempotent, and the
-- MongoDB sink connector upserts cleanly by key.
WITH ('changelog.mode' = 'upsert', 'value.format' = 'avro-registry');
