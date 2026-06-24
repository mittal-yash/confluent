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
