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
