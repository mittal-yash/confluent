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
