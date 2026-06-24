CREATE VIEW crew_latest AS
SELECT crew_id, region, lat, lon, status, skills FROM (
  SELECT crew_id, region, lat, lon, status, skills,
    ROW_NUMBER() OVER (PARTITION BY crew_id ORDER BY `$rowtime` DESC) AS rn
  FROM `gridsentinel.crew.location`
) WHERE rn = 1;
