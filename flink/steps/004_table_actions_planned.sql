CREATE TABLE `gridsentinel.actions.planned` (
  incident_id      STRING,
  asset_id         STRING,
  plan_id          STRING,
  asset_type       STRING,
  region           STRING,
  `ts`             TIMESTAMP_LTZ(3),
  action_type      STRING,
  target_load_pct  STRING,
  tie_assets       STRING,
  need_crew        BOOLEAN,
  crew_id          STRING,
  need_work_order  BOOLEAN,
  priority         STRING,
  sla_hours        INT,
  severity         STRING,
  root_cause       STRING,
  rationale        STRING,
  confidence       DOUBLE,
  PRIMARY KEY (incident_id) NOT ENFORCED
) DISTRIBUTED BY (incident_id) INTO 6 BUCKETS
-- upsert keyed by incident_id: plan_ranked (Top-1 nearest crew) is an updating
-- stream, so an append sink would fail with "doesn't support consuming update
-- and delete changes". One current plan per incident.
WITH ('changelog.mode' = 'upsert', 'value.format' = 'avro-registry');
