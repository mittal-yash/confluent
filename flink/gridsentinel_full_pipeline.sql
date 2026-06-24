-- =============================================================================
-- GridSentinel — full Flink SQL pipeline (run order)
-- =============================================================================
-- Confluent Cloud runs ONE statement at a time. Execute each block separately,
-- in order. Leave *_job_* INSERT statements running (streaming jobs).
--
-- Before steps 015/016 (scoring INSERT jobs), run once:
--   SET 'sql.tables.scan.idle-timeout' = '15s';
--
-- Prerequisite topics (simulator / connectors):
--   gridsentinel.telemetry
--   gridsentinel.crew.location
--   gridsentinel.mongo.gridsentinel.asset_specs
--   gridsentinel.mongo.gridsentinel.assets
-- =============================================================================


-- -----------------------------------------------------------------------------
-- STEP 001_table_telemetry_enriched
-- -----------------------------------------------------------------------------
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

-- -----------------------------------------------------------------------------
-- STEP 002_table_incidents
-- -----------------------------------------------------------------------------
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

-- -----------------------------------------------------------------------------
-- STEP 003_table_incidents_diagnosed
-- -----------------------------------------------------------------------------
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

-- -----------------------------------------------------------------------------
-- STEP 004_table_actions_planned
-- -----------------------------------------------------------------------------
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

-- -----------------------------------------------------------------------------
-- STEP 005_table_actions_executed
-- -----------------------------------------------------------------------------
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

-- -----------------------------------------------------------------------------
-- STEP 006_table_control
-- -----------------------------------------------------------------------------
CREATE TABLE `gridsentinel.control` (
  incident_id  STRING,
  asset_id     STRING,
  action_id    STRING,
  `ts`         TIMESTAMP_LTZ(3),
  action_type  STRING,
  PRIMARY KEY (incident_id) NOT ENFORCED
) DISTRIBUTED BY (incident_id) INTO 6 BUCKETS
-- upsert: reads the upsert actions.planned changelog. One current control/incident.
WITH ('changelog.mode' = 'upsert', 'value.format' = 'avro-registry');

-- -----------------------------------------------------------------------------
-- STEP 007_table_work_orders
-- -----------------------------------------------------------------------------
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

-- -----------------------------------------------------------------------------
-- STEP 008_table_incidents_resolved
-- -----------------------------------------------------------------------------
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

-- -----------------------------------------------------------------------------
-- STEP 009_table_asset_specs_ref
-- -----------------------------------------------------------------------------
-- Re-keyed, versioned reference table for the temporal join.
-- WHY: the MongoDB Source Connector topic
--   `gridsentinel.mongo.gridsentinel.asset_specs`  (topic.prefix + db + collection)
-- is an auto-inferred APPEND table with NO primary key, so it can't be the
-- build side of an event-time temporal join. We re-key it by asset_id into an
-- UPSERT table (Kafka key = asset_id) that keeps the latest spec per asset.
CREATE TABLE asset_specs_ref (
  asset_id                 STRING,
  asset_type               STRING,
  warning_temp_c           DOUBLE,
  critical_temp_c          DOUBLE,
  warning_vibration_mm_s   DOUBLE,
  nominal_oil_pressure_kpa DOUBLE,
  rated_load_mw            DOUBLE,
  PRIMARY KEY (asset_id) NOT ENFORCED
) DISTRIBUTED BY (asset_id) INTO 6 BUCKETS
WITH ('changelog.mode' = 'upsert', 'value.format' = 'avro-registry');

-- -----------------------------------------------------------------------------
-- STEP 009b_job_insert_asset_specs_ref
-- -----------------------------------------------------------------------------
-- Long-running job: re-key the connector's asset_specs topic by asset_id.
-- NOTE the doubled `gridsentinel` in the source topic name — that is what the
-- MongoDB Source Connector actually produces (topic.prefix=gridsentinel.mongo,
-- database=gridsentinel, collection=asset_specs). Verify with SHOW TABLES LIKE
-- '%asset_specs%' and adjust if your connector uses a different prefix.
INSERT INTO asset_specs_ref
SELECT
  asset_id,
  asset_type,
  warning_temp_c,
  critical_temp_c,
  warning_vibration_mm_s,
  nominal_oil_pressure_kpa,
  rated_load_mw
FROM `gridsentinel.mongo.gridsentinel.asset_specs`;

-- -----------------------------------------------------------------------------
-- STEP 010_table_assets_ref
-- -----------------------------------------------------------------------------
-- Re-keyed, versioned asset-registry table (lat/lon + tie routes for planning).
-- Same reasoning as 009: re-key the connector topic
--   `gridsentinel.mongo.gridsentinel.assets`
-- into an UPSERT table keyed by asset_id so it can drive temporal joins.
CREATE TABLE assets_ref (
  asset_id    STRING,
  lat         DOUBLE,
  lon         DOUBLE,
  tie_assets  ARRAY<STRING>,
  PRIMARY KEY (asset_id) NOT ENFORCED
) DISTRIBUTED BY (asset_id) INTO 6 BUCKETS
WITH ('changelog.mode' = 'upsert', 'value.format' = 'avro-registry');

-- -----------------------------------------------------------------------------
-- STEP 010b_job_insert_assets_ref
-- -----------------------------------------------------------------------------
-- Long-running job: re-key the connector's assets topic by asset_id.
INSERT INTO assets_ref
SELECT
  asset_id,
  lat,
  lon,
  tie_assets
FROM `gridsentinel.mongo.gridsentinel.assets`;

-- -----------------------------------------------------------------------------
-- STEP 011_view_telemetry_anomaly
-- -----------------------------------------------------------------------------
CREATE VIEW telemetry_anomaly AS
SELECT
  asset_id,
  asset_type,
  `$rowtime`,
  `$rowtime` AS event_time,
  region,
  substation,
  load_pct,
  temp_c,
  ambient_c,
  vibration_mm_s,
  oil_pressure_kpa,
  CASE
    WHEN STDDEV_POP(temp_c) OVER w > 0
    THEN (temp_c - AVG(temp_c) OVER w) / STDDEV_POP(temp_c) OVER w
    ELSE 0.0
  END AS temp_zscore
FROM `gridsentinel.telemetry`
WINDOW w AS (
  PARTITION BY asset_id
  ORDER BY `$rowtime`
  ROWS BETWEEN 30 PRECEDING AND CURRENT ROW
);

-- -----------------------------------------------------------------------------
-- STEP 012_view_telemetry_scored
-- -----------------------------------------------------------------------------
CREATE VIEW telemetry_scored AS

SELECT

  a.asset_id,

  a.asset_type,

  a.event_time,

  a.region,

  a.substation,

  a.load_pct,

  a.temp_c,

  a.vibration_mm_s,

  a.oil_pressure_kpa,

  a.temp_zscore,

  COALESCE(s.warning_temp_c, 85.0)            AS warning_temp_c,

  COALESCE(s.critical_temp_c, 95.0)           AS critical_temp_c,

  COALESCE(s.warning_vibration_mm_s, 0.0)     AS warning_vibration_mm_s,

  COALESCE(s.nominal_oil_pressure_kpa, 0.0)   AS nominal_oil_pressure_kpa,

  COALESCE(s.rated_load_mw, 0.0)              AS rated_load_mw,

  COALESCE(ARRAY_JOIN(r.tie_assets, ','), '') AS tie_assets,

  GREATEST(0.0, (a.temp_c - COALESCE(s.warning_temp_c, 85.0))

        / NULLIF(COALESCE(s.critical_temp_c, 95.0) - COALESCE(s.warning_temp_c, 85.0), 0)) AS temp_ratio,

  GREATEST(0.0, (a.load_pct - 100.0) / 20.0) AS load_over,

  CASE WHEN COALESCE(s.warning_vibration_mm_s, 0.0) > 0

       THEN GREATEST(0.0, (a.vibration_mm_s - s.warning_vibration_mm_s) / s.warning_vibration_mm_s)

       ELSE 0.0 END AS vib_ratio,

  CASE WHEN COALESCE(s.nominal_oil_pressure_kpa, 0.0) > 0

       THEN GREATEST(0.0, (s.nominal_oil_pressure_kpa - a.oil_pressure_kpa) / s.nominal_oil_pressure_kpa)

       ELSE 0.0 END AS pressure_drop

FROM telemetry_anomaly AS a

LEFT JOIN asset_specs_ref FOR SYSTEM_TIME AS OF a.`$rowtime` AS s

  ON a.asset_id = s.asset_id

LEFT JOIN assets_ref FOR SYSTEM_TIME AS OF a.`$rowtime` AS r

  ON a.asset_id = r.asset_id;

-- -----------------------------------------------------------------------------
-- STEP 013_view_telemetry_graded
-- -----------------------------------------------------------------------------
CREATE VIEW telemetry_graded AS
SELECT
  *,
  1.0 / (1.0 + EXP(-( -3.6
        + 4.2 * temp_ratio
        + 3.1 * load_over
        + 2.6 * vib_ratio
        + 2.4 * pressure_drop ))) AS fault_score_calc
FROM telemetry_scored;

-- -----------------------------------------------------------------------------
-- STEP 014_view_telemetry_severity
-- -----------------------------------------------------------------------------
CREATE VIEW telemetry_severity AS
SELECT
  *,
  CASE
    WHEN temp_c >= critical_temp_c
      OR (warning_vibration_mm_s > 0 AND vibration_mm_s >= warning_vibration_mm_s * 1.5)
      OR (nominal_oil_pressure_kpa > 0 AND (nominal_oil_pressure_kpa - oil_pressure_kpa) / nominal_oil_pressure_kpa >= 0.30)
      OR load_pct >= 115
      OR fault_score_calc >= 0.80
      THEN 'critical'
    WHEN temp_c >= warning_temp_c
      OR (warning_vibration_mm_s > 0 AND vibration_mm_s >= warning_vibration_mm_s)
      OR (nominal_oil_pressure_kpa > 0 AND (nominal_oil_pressure_kpa - oil_pressure_kpa) / nominal_oil_pressure_kpa >= 0.15)
      OR load_pct >= 102
      OR fault_score_calc >= 0.50
      THEN 'warning'
    ELSE 'info'
  END AS severity
FROM telemetry_graded;

-- -----------------------------------------------------------------------------
-- STEP 015_job_insert_enriched
-- -----------------------------------------------------------------------------
INSERT INTO `gridsentinel.telemetry.enriched`
SELECT
  asset_id, asset_type, event_time, region, substation, load_pct, temp_c,
  vibration_mm_s, oil_pressure_kpa, fault_score_calc, severity,
  warning_temp_c, critical_temp_c
FROM telemetry_severity;

-- -----------------------------------------------------------------------------
-- STEP 016_job_insert_incidents
-- -----------------------------------------------------------------------------
INSERT INTO `gridsentinel.incidents`
SELECT
  asset_id,
  CAST(UUID() AS STRING) AS incident_id,
  asset_type, event_time, region, substation,
  CASE
    WHEN temp_c >= warning_temp_c THEN 'temp_c'
    WHEN warning_vibration_mm_s > 0 AND vibration_mm_s >= warning_vibration_mm_s THEN 'vibration_mm_s'
    WHEN load_pct >= 102 THEN 'load_pct'
    ELSE 'oil_pressure_kpa'
  END AS signal,
  temp_c AS `value`,
  warning_temp_c AS threshold,
  temp_zscore AS zscore,
  fault_score_calc AS fault_score,
  severity,
  warning_temp_c,
  critical_temp_c,
  warning_vibration_mm_s,
  nominal_oil_pressure_kpa,
  rated_load_mw,
  tie_assets,
  (severity = 'critical' OR fault_score_calc >= 0.5 OR ABS(temp_zscore) >= 3.0) AS reasoning_needed
FROM telemetry_severity
WHERE severity IN ('warning', 'critical');

-- -----------------------------------------------------------------------------
-- STEP 017_connection_gemini
-- -----------------------------------------------------------------------------
CREATE CONNECTION gemini_connection
  WITH (
    'type'        = 'vertexai',
    'endpoint'    = 'https://asia-south1-aiplatform.googleapis.com/v1/projects/<YOUR_GCP_PROJECT>/locations/asia-south1/publishers/google/models/gemini-2.5-flash',
    'service-key' = '<YOUR_VERTEX_SERVICE_ACCOUNT_JSON>'
  );

-- -----------------------------------------------------------------------------
-- STEP 018_model_diagnosis
-- -----------------------------------------------------------------------------
-- Prerequisite: Flink workspace Catalog + Database dropdowns must match your
-- environment and Kafka cluster (e.g. confluent-ai-day / cluster_0), OR use:
-- CREATE MODEL `your_env`.`your_cluster`.diagnosis_model ...
CREATE MODEL diagnosis_model
INPUT  (`input` STRING)
OUTPUT (`output` STRING)
WITH (
  'provider'              = 'vertexai',
  'vertexai.connection'   = 'gemini_connection',
  'vertexai.input_format' = 'GEMINI-GENERATE',
  'task'                  = 'text_generation',
  'vertexai.system_prompt' =
    'You are a grid reliability engineer for a power utility. Diagnose the incident using ONLY the provided CONTEXT (equipment manuals + asset spec). Choose recommended_action from EXACTLY this set: ["monitor","throttle_load","reroute_load","dispatch_crew","isolate"]. Prefer the least disruptive action that removes the hazard (reroute/throttle before isolate); use reroute_load only if tie_assets are listed. If context is insufficient, recommend dispatch_crew. Respond with STRICT MINIFIED JSON ONLY (no markdown, no prose) with keys: root_cause (string), recommended_action (string from the set), rationale (string, cite the manual by name), confidence (number 0..1), citation (string = the manual/source name you used).'
);

-- -----------------------------------------------------------------------------
-- STEP 019_connection_gemini_embed
-- -----------------------------------------------------------------------------
CREATE CONNECTION gemini_embed_connection
  WITH (
    'type'        = 'vertexai',
    'endpoint'    = 'https://asia-south1-aiplatform.googleapis.com/v1/projects/<YOUR_GCP_PROJECT>/locations/asia-south1/publishers/google/models/gemini-embedding-001:predict',
    'service-key' = '<YOUR_VERTEX_SERVICE_ACCOUNT_JSON>'
  );

-- -----------------------------------------------------------------------------
-- STEP 020_model_gemini_embed
-- -----------------------------------------------------------------------------
CREATE MODEL gemini_embed
INPUT  (input STRING)
OUTPUT (embedding ARRAY<FLOAT>)
WITH (
  'provider'              = 'vertexai',
  'vertexai.connection'   = 'gemini_embed_connection',
  'vertexai.input_format' = 'VERTEX-EMBED',
  'task'                  = 'embedding'
);

-- -----------------------------------------------------------------------------
-- STEP 021_connection_mongodb
-- -----------------------------------------------------------------------------
CREATE CONNECTION mongodb_connection
  WITH (
    'type'     = 'mongodb',
    'endpoint' = '<atlas_srv_endpoint>',
    'username' = '<atlas_user>',
    'password' = '<atlas_password>'
  );

-- -----------------------------------------------------------------------------
-- STEP 022_table_manual_chunks_vec
-- -----------------------------------------------------------------------------
CREATE TABLE manual_chunks_vec (
  `text`      STRING,
  `source`    STRING,
  asset_type  STRING,
  embedding   ARRAY<FLOAT>
) WITH (
  'connector'           = 'mongodb',
  'mongodb.connection'  = 'mongodb_connection',
  'mongodb.database'    = 'gridsentinel',
  'mongodb.collection'  = 'manual_chunks',
  'mongodb.index'       = 'manuals_vector_index',
  'mongodb.numcandidates' = '100'
);

-- -----------------------------------------------------------------------------
-- STEP 023_table_diagnosis_query
-- -----------------------------------------------------------------------------
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

-- -----------------------------------------------------------------------------
-- STEP 023b_job_insert_diagnosis_query
-- -----------------------------------------------------------------------------
-- Long-running job: embed each gated incident as a retrieval query (Gemini),
-- writing the query vector into the diagnosis_query table created in 023.
INSERT INTO diagnosis_query
SELECT
  i.incident_id, i.asset_id, i.asset_type, i.region, i.severity, i.signal,
  i.`value`, i.fault_score, i.zscore, i.tie_assets,
  i.warning_temp_c, i.critical_temp_c, i.rated_load_mw,
  q.embedding AS qvec
FROM `gridsentinel.incidents` AS i,
  LATERAL TABLE(AI_EMBEDDING('gemini_embed',
    i.asset_type || ' ' || i.signal
      || ' anomaly (severity ' || i.severity
      || '). Root cause and recommended response action.')) AS q
WHERE i.reasoning_needed = TRUE;

-- -----------------------------------------------------------------------------
-- STEP 024_table_diagnosis_retrieved
-- -----------------------------------------------------------------------------
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

-- -----------------------------------------------------------------------------
-- STEP 024b_job_insert_diagnosis_retrieved
-- -----------------------------------------------------------------------------
-- Long-running job: semantic top-3 retrieval from the Atlas manual_chunks vector
-- index. Concatenate the returned array by index (guarded by CARDINALITY) rather
-- than UNNEST+GROUP BY, so it stays one row / one Gemini call per incident.
INSERT INTO diagnosis_retrieved
SELECT
  d.incident_id, d.asset_id, d.asset_type, d.region, d.severity, d.signal,
  d.`value`, d.fault_score, d.zscore, d.tie_assets,
  d.warning_temp_c, d.critical_temp_c, d.rated_load_mw,
  search_results[1].`text`
    || CASE WHEN CARDINALITY(search_results) >= 2 THEN ' --- ' || search_results[2].`text` ELSE '' END
    || CASE WHEN CARDINALITY(search_results) >= 3 THEN ' --- ' || search_results[3].`text` ELSE '' END
    AS context_text,
  search_results[1].`source`
    || CASE WHEN CARDINALITY(search_results) >= 2 THEN ', ' || search_results[2].`source` ELSE '' END
    || CASE WHEN CARDINALITY(search_results) >= 3 THEN ', ' || search_results[3].`source` ELSE '' END
    AS context_source
FROM diagnosis_query AS d,
  LATERAL TABLE(VECTOR_SEARCH_AGG(manual_chunks_vec, DESCRIPTOR(embedding), d.qvec, 3));

-- -----------------------------------------------------------------------------
-- STEP 025_job_insert_diagnosed_rag
-- -----------------------------------------------------------------------------
INSERT INTO `gridsentinel.incidents.diagnosed`
SELECT
  d.asset_id, d.incident_id, d.asset_type, d.region, d.severity, d.signal,
  d.`value`, d.fault_score, d.zscore, d.tie_assets,
  JSON_VALUE(p.`output`, '$.root_cause')                       AS dx_root_cause,
  COALESCE(JSON_VALUE(p.`output`, '$.recommended_action'), 'dispatch_crew') AS dx_recommended_action,
  JSON_VALUE(p.`output`, '$.rationale')                        AS dx_rationale,
  CAST(JSON_VALUE(p.`output`, '$.confidence') AS DOUBLE)       AS dx_confidence,
  ARRAY[ROW(d.context_source, SUBSTRING(d.context_text FROM 1 FOR 200))] AS dx_citations,
  'gemini+vector-rag'                                          AS dx_method,
  CAST(CURRENT_TIMESTAMP AS TIMESTAMP_LTZ(3))                  AS diagnosed_ts
FROM diagnosis_retrieved AS d,
  LATERAL TABLE(ML_PREDICT('diagnosis_model',
    'INCIDENT: asset=' || d.asset_id
      || ' type=' || d.asset_type
      || ' signal=' || d.signal
      || ' value=' || CAST(d.`value` AS STRING)
      || ' severity=' || d.severity
      || ' fault_score=' || CAST(d.fault_score AS STRING)
      || ' tie_assets=[' || d.tie_assets || ']'
      || '  ENVELOPE: warning_temp_c=' || CAST(d.warning_temp_c AS STRING)
      || ' critical_temp_c=' || CAST(d.critical_temp_c AS STRING)
      || ' rated_load_mw=' || CAST(d.rated_load_mw AS STRING)
      || '  CONTEXT (' || d.context_source || '): ' || d.context_text)) AS p;

-- -----------------------------------------------------------------------------
-- STEP 026_job_insert_diagnosed_gated
-- -----------------------------------------------------------------------------
INSERT INTO `gridsentinel.incidents.diagnosed`
SELECT
  i.asset_id, i.incident_id, i.asset_type, i.region, i.severity, i.signal,
  i.`value`, i.fault_score, i.zscore, i.tie_assets,
  'Stable warning within tolerance; no immediate hazard.' AS dx_root_cause,
  'monitor'                                               AS dx_recommended_action,
  'Cost gate: low fault score and no abnormal deviation.' AS dx_rationale,
  CAST(0.55 AS DOUBLE)                                    AS dx_confidence,
  CAST(NULL AS ARRAY<ROW<`source` STRING, snippet STRING>>) AS dx_citations,
  'gated-skip'                                            AS dx_method,
  CAST(CURRENT_TIMESTAMP AS TIMESTAMP_LTZ(3))             AS diagnosed_ts
FROM `gridsentinel.incidents` AS i
WHERE i.reasoning_needed = FALSE;

-- -----------------------------------------------------------------------------
-- STEP 027_view_crew_latest
-- -----------------------------------------------------------------------------
CREATE VIEW crew_latest AS
SELECT crew_id, region, lat, lon, status, skills FROM (
  SELECT crew_id, region, lat, lon, status, skills,
    ROW_NUMBER() OVER (PARTITION BY crew_id ORDER BY `$rowtime` DESC) AS rn
  FROM `gridsentinel.crew.location`
) WHERE rn = 1;

-- -----------------------------------------------------------------------------
-- STEP 028_view_plan_base
-- -----------------------------------------------------------------------------
CREATE VIEW plan_base AS

SELECT

  d.incident_id, d.asset_id, d.asset_type, d.region, d.severity, d.tie_assets,

  d.dx_root_cause, d.dx_rationale, d.dx_confidence,

  CASE WHEN d.dx_recommended_action IN

        ('monitor','throttle_load','reroute_load','dispatch_crew','isolate')

       THEN d.dx_recommended_action ELSE 'monitor' END AS action_type,

  CASE d.asset_type

       WHEN 'transformer' THEN 'transformer'

       WHEN 'transmission_line' THEN 'line'

       WHEN 'water_pump' THEN 'pump'

       ELSE 'general' END AS req_skill,

  CAST(r.lat AS DOUBLE) AS asset_lat,

  CAST(r.lon AS DOUBLE) AS asset_lon

FROM `gridsentinel.incidents.diagnosed` AS d

LEFT JOIN assets_ref FOR SYSTEM_TIME AS OF d.`$rowtime` AS r

  ON d.asset_id = r.asset_id;

-- -----------------------------------------------------------------------------
-- STEP 029_view_plan_ranked
-- -----------------------------------------------------------------------------
CREATE VIEW plan_ranked AS
SELECT *, ROW_NUMBER() OVER (
    PARTITION BY incident_id
    ORDER BY region_match DESC, dist ASC
  ) AS rn
FROM (
  SELECT
    b.*,
    c.crew_id AS cand_crew_id,
    CASE WHEN c.region = b.region THEN 1 ELSE 0 END AS region_match,
    CASE WHEN c.crew_id IS NULL THEN 1e18
         ELSE (b.asset_lat - c.lat) * (b.asset_lat - c.lat)
            + (b.asset_lon - c.lon) * (b.asset_lon - c.lon) END AS dist
  FROM plan_base AS b
  LEFT JOIN crew_latest AS c
    ON c.status = 'available'
   AND (ARRAY_CONTAINS(c.skills, b.req_skill) OR ARRAY_CONTAINS(c.skills, 'general'))
);

-- -----------------------------------------------------------------------------
-- STEP 030_job_insert_planned
-- -----------------------------------------------------------------------------
INSERT INTO `gridsentinel.actions.planned`
SELECT
  incident_id,
  asset_id,
  'PLAN-' || SUBSTRING(incident_id FROM 1 FOR 8) AS plan_id,
  asset_type, region,
  CAST(CURRENT_TIMESTAMP AS TIMESTAMP_LTZ(3)) AS `ts`,
  action_type,
  CASE WHEN action_type = 'throttle_load' THEN '52' ELSE '' END AS target_load_pct,
  tie_assets,
  (action_type IN ('dispatch_crew','isolate') OR severity = 'critical') AS need_crew,
  CASE WHEN (action_type IN ('dispatch_crew','isolate') OR severity = 'critical')
       THEN cand_crew_id ELSE CAST(NULL AS STRING) END AS crew_id,
  (action_type <> 'monitor') AS need_work_order,
  CASE severity WHEN 'critical' THEN 'P1' WHEN 'warning' THEN 'P2' ELSE 'P3' END AS priority,
  CASE severity WHEN 'critical' THEN 2 WHEN 'warning' THEN 8 ELSE 24 END AS sla_hours,
  severity, dx_root_cause AS root_cause, dx_rationale AS rationale, dx_confidence AS confidence
FROM plan_ranked
WHERE rn = 1;

-- -----------------------------------------------------------------------------
-- STEP 031_job_insert_executed
-- -----------------------------------------------------------------------------
INSERT INTO `gridsentinel.actions.executed`
SELECT
  incident_id,
  asset_id,
  'ACT-' || SUBSTRING(incident_id FROM 1 FOR 8) AS action_id,
  CAST(CURRENT_TIMESTAMP AS TIMESTAMP_LTZ(3)) AS `ts`,
  action_type, rationale,
  CASE WHEN action_type = 'monitor' THEN 'completed' ELSE 'issued' END AS status,
  'flink-action' AS issued_by
FROM `gridsentinel.actions.planned`;

-- -----------------------------------------------------------------------------
-- STEP 032_job_insert_control
-- -----------------------------------------------------------------------------
INSERT INTO `gridsentinel.control`
SELECT
  incident_id,
  asset_id,
  'CTL-' || SUBSTRING(incident_id FROM 1 FOR 8) AS action_id,
  CAST(CURRENT_TIMESTAMP AS TIMESTAMP_LTZ(3)) AS `ts`,
  action_type
FROM `gridsentinel.actions.planned`
WHERE action_type IN ('throttle_load','reroute_load','isolate','dispatch_crew');

-- -----------------------------------------------------------------------------
-- STEP 033_job_insert_work_orders
-- -----------------------------------------------------------------------------
INSERT INTO `gridsentinel.work.orders`
SELECT
  incident_id,
  'WO-' || SUBSTRING(incident_id FROM 1 FOR 8) AS work_order_id,
  asset_id,
  CAST(CURRENT_TIMESTAMP AS TIMESTAMP_LTZ(3)) AS `ts`,
  priority, crew_id, sla_hours,
  SUBSTRING(action_type || ' on ' || asset_id || '. Root cause: ' || COALESCE(root_cause,'') FROM 1 FOR 500) AS description,
  'open' AS status
FROM `gridsentinel.actions.planned`
WHERE need_work_order = TRUE;

-- -----------------------------------------------------------------------------
-- STEP 034_view_recovery_candidates
-- -----------------------------------------------------------------------------
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

-- -----------------------------------------------------------------------------
-- STEP 035_job_insert_resolved
-- -----------------------------------------------------------------------------
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

