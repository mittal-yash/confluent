-- diagnosis_retrieved is a TABLE + INSERT job (024b), not a VIEW: a CREATE VIEW
-- that wraps VECTOR_SEARCH_AGG throws "Internal error occurred" on Confluent
-- Flink (same as AI_EMBEDDING in 023). Materialize, then run the search job.
CREATE TABLE diagnosis_retrieved (
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
  context_text    STRING,
  context_source  STRING,
  PRIMARY KEY (incident_id) NOT ENFORCED
) DISTRIBUTED BY (incident_id) INTO 6 BUCKETS
WITH ('changelog.mode' = 'append', 'value.format' = 'avro-registry');
