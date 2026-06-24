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
