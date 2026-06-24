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
