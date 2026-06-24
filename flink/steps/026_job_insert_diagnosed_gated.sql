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
