-- diagnosis_query is a TABLE (not a VIEW): Confluent Flink can throw
-- "Internal error occurred" when a CREATE VIEW wraps an ML table function like
-- AI_EMBEDDING. Materializing to a Kafka-backed table avoids that and lets the
-- embedding run as its own streaming job (023b). Run 023 (this) then 023b.
CREATE TABLE diagnosis_query (
  incident_id     STRING,
  asset_id        STRING,
  asset_type      STRING,
  region          STRING,
  severity        STRING,
  signal          STRING,
  `value`         DOUBLE,
  fault_score     DOUBLE,
  zscore          DOUBLE,
  tie_assets      STRING,
  warning_temp_c  DOUBLE,
  critical_temp_c DOUBLE,
  rated_load_mw   DOUBLE,
  qvec            ARRAY<FLOAT>,
  PRIMARY KEY (incident_id) NOT ENFORCED
) DISTRIBUTED BY (incident_id) INTO 6 BUCKETS
WITH ('changelog.mode' = 'append', 'value.format' = 'avro-registry');
