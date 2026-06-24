CREATE VIEW telemetry_graded AS
SELECT
  *,
  1.0 / (1.0 + EXP(-( -3.6
        + 4.2 * temp_ratio
        + 3.1 * load_over
        + 2.6 * vib_ratio
        + 2.4 * pressure_drop ))) AS fault_score_calc
FROM telemetry_scored;
