-- =====================================================================
-- 05 - Diagnosis: TRUE semantic RAG + Gemini, entirely in Flink.
--   incident -> AI_EMBEDDING (Gemini gemini-embedding-001, 3072-dim)
--           -> VECTOR_SEARCH_AGG over MongoDB Atlas (manual_chunks)
--           -> ML_PREDICT (Gemini gemini-2.5-flash) grounded on the retrieved
--              passage -> strict-JSON parsed into the dx_* columns.
--
-- PREREQUISITES:
--   * manual_chunks embedded with the SAME model (gemini-embedding-001, 3072-dim):
--       set VERTEX_* in .env, then `python -m data.embed_manuals`
--   * an Atlas Vector Search index `manuals_vector_index` on manual_chunks.embedding
--     with numDimensions=3072 (Flink's CREATE MODEL cannot reduce the output dim,
--     so it emits the native 3072-d vector; see infra/mongodb_atlas_setup.md)
-- =====================================================================

-- Embedding model: Vertex AI gemini-embedding-001 (must match embed_manuals on laptop).
CREATE CONNECTION gemini_embed_connection
  WITH (
    'type'        = 'vertexai',
    'endpoint'    = 'https://asia-south1-aiplatform.googleapis.com/v1/projects/<YOUR_GCP_PROJECT>/locations/asia-south1/publishers/google/models/gemini-embedding-001:predict',
    'service-key' = '<YOUR_VERTEX_SERVICE_ACCOUNT_JSON>'
  );

CREATE MODEL gemini_embed
INPUT  (input STRING)
OUTPUT (embedding ARRAY<FLOAT>)
WITH (
  'provider'              = 'vertexai',
  'vertexai.connection'   = 'gemini_embed_connection',
  'vertexai.input_format' = 'VERTEX-EMBED',
  'task'                  = 'embedding'
);

-- MongoDB Atlas connection + read-only external table over the embedded corpus.
CREATE CONNECTION mongodb_connection
  WITH (
    'type'     = 'mongodb',
    'endpoint' = '<atlas_srv_endpoint>',     -- e.g. mongodb+srv://cluster0.xxxx.mongodb.net
    'username' = '<atlas_user>',
    'password' = '<atlas_password>'
  );

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

-- 1) Embed the incident as a retrieval query (Gemini), for incidents that
--    warrant the paid reasoning path (cost gate).
--    diagnosis_query is a TABLE + INSERT job, not a VIEW: Confluent Flink can
--    throw "Internal error occurred" when a CREATE VIEW wraps an ML table
--    function (AI_EMBEDDING). See steps/023_table_diagnosis_query.sql +
--    steps/023b_job_insert_diagnosis_query.sql.
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

-- 2) Semantic top-3 retrieval from Atlas. We keep the returned array and
--    concatenate its elements by index (guarded by CARDINALITY) rather than
--    UNNEST+GROUP BY - that keeps it one row / one Gemini call per incident
--    (a streaming GROUP BY would be an updating aggregate that re-fires the LLM).
-- diagnosis_retrieved is a TABLE + INSERT job, not a VIEW (CREATE VIEW over
-- VECTOR_SEARCH_AGG throws "Internal error occurred"). See
-- steps/024_table_diagnosis_retrieved.sql + 024b_job_insert_diagnosis_retrieved.sql.
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

-- 3) Gemini diagnosis grounded ONLY on the retrieved passage; parse strict JSON.
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

-- Incidents that did NOT warrant the paid model: deterministic 'monitor'.
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
