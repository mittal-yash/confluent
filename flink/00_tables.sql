-- =====================================================================
-- GridSentinel - Flink table definitions (REFERENCE — do NOT paste whole file)
-- Confluent Flink: ONE statement per Run. Use flink/steps/001-008 instead.
-- =====================================================================

-- Enriched, scored telemetry -> drives the live dashboard.
CREATE TABLE `gridsentinel.telemetry.enriched` (
  asset_id         STRING,
  asset_type       STRING,
  `ts`             TIMESTAMP_LTZ(3),
  region           STRING,
  substation       STRING,
  load_pct         DOUBLE,
  temp_c           DOUBLE,
  vibration_mm_s   DOUBLE,
  oil_pressure_kpa DOUBLE,
  fault_score      DOUBLE,
  severity         STRING,
  warning_temp_c   DOUBLE,
  critical_temp_c  DOUBLE
) DISTRIBUTED BY (asset_id) INTO 6 BUCKETS
WITH ('changelog.mode' = 'append', 'value.format' = 'avro-registry');

-- Incidents (only warning/critical rows) routed to the diagnosis stage.
CREATE TABLE `gridsentinel.incidents` (
  asset_id         STRING,
  incident_id      STRING,
  asset_type       STRING,
  `ts`             TIMESTAMP_LTZ(3),
  region           STRING,
  substation       STRING,
  signal           STRING,
  `value`          DOUBLE,
  threshold        DOUBLE,
  zscore           DOUBLE,
  fault_score      DOUBLE,
  severity         STRING,
  warning_temp_c   DOUBLE,
  critical_temp_c  DOUBLE,
  warning_vibration_mm_s DOUBLE,
  nominal_oil_pressure_kpa DOUBLE,
  rated_load_mw    DOUBLE,
  tie_assets       STRING,
  reasoning_needed BOOLEAN
) DISTRIBUTED BY (asset_id) INTO 6 BUCKETS
WITH ('changelog.mode' = 'append', 'value.format' = 'avro-registry');

-- Gemini-grounded diagnosis (dx_* fields the dashboard reads).
CREATE TABLE `gridsentinel.incidents.diagnosed` (
  asset_id             STRING,
  incident_id          STRING,
  asset_type           STRING,
  region               STRING,
  severity             STRING,
  signal               STRING,
  `value`              DOUBLE,
  fault_score          DOUBLE,
  zscore               DOUBLE,
  tie_assets           STRING,
  dx_root_cause        STRING,
  dx_recommended_action STRING,
  dx_rationale         STRING,
  dx_confidence        DOUBLE,
  dx_citations         ARRAY<ROW<`source` STRING, snippet STRING>>,
  dx_method            STRING,
  diagnosed_ts         TIMESTAMP_LTZ(3)
) DISTRIBUTED BY (asset_id) INTO 6 BUCKETS
WITH ('changelog.mode' = 'append', 'value.format' = 'avro-registry');

-- Validated action plan (constrained action + selected crew + priority/SLA).
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
-- upsert: plan_ranked (Top-1 nearest crew) is an updating stream; an append sink
-- would fail with "doesn't support consuming update and delete changes".
WITH ('changelog.mode' = 'upsert', 'value.format' = 'avro-registry');

-- Executed actions (audit + verifier + dashboard ACT feed).
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
WITH ('changelog.mode' = 'upsert', 'value.format' = 'avro-registry');

-- Control commands the LAPTOP simulator obeys -> physical recovery of the asset.
CREATE TABLE `gridsentinel.control` (
  incident_id  STRING,
  asset_id     STRING,
  action_id    STRING,
  `ts`         TIMESTAMP_LTZ(3),
  action_type  STRING,
  PRIMARY KEY (incident_id) NOT ENFORCED
) DISTRIBUTED BY (incident_id) INTO 6 BUCKETS
WITH ('changelog.mode' = 'upsert', 'value.format' = 'avro-registry');

-- Durable work orders -> MongoDB via the sink connector.
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
WITH ('changelog.mode' = 'upsert', 'value.format' = 'avro-registry');

-- Closed-loop verification results.
CREATE TABLE `gridsentinel.incidents.resolved` (
  incident_id      STRING,
  asset_id         STRING,
  status           STRING,
  action_type      STRING,
  recovery_seconds DOUBLE,
  resolved_ts      TIMESTAMP_LTZ(3),
  PRIMARY KEY (incident_id) NOT ENFORCED
) DISTRIBUTED BY (incident_id) INTO 6 BUCKETS
WITH ('changelog.mode' = 'upsert', 'value.format' = 'avro-registry');
