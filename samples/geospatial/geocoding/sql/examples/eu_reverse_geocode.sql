-- ============================================================================
-- Reverse geocoding check for Germany, France and Italy
-- ============================================================================
-- Needs the --eu install (uses OVERTURE_ADDRESS_EU only to sample test points;
-- REVERSE_GEOCODE_TABLE itself works on the core install).
-- 30 real addresses per country, each point moved about 13 m off the address,
-- so the nearest address is not always the source one. A correct result is
-- never farther from the input than the source address is.
-- ============================================================================

CREATE OR REPLACE TABLE GEOCODING.PUBLIC.TEST_EU_REV_INPUT COMMENT = 'oss-geocoding' AS
SELECT country || '-' || LPAD(ROW_NUMBER() OVER (PARTITION BY country ORDER BY HASH(id))::STRING, 3, '0') AS id,
       country,
       id AS overture_id,
       ST_Y(geog) + 0.0001 AS lat,
       ST_X(geog) + 0.0001 AS lon
FROM GEOCODING.PUBLIC.OVERTURE_ADDRESS_EU
WHERE MOD(ABS(HASH(id, 11)), 10000) = 0  -- stable across index rebuilds
  AND country IN ('DE', 'FR', 'IT') AND number IS NOT NULL AND street IS NOT NULL
QUALIFY ROW_NUMBER() OVER (PARTITION BY country ORDER BY HASH(id)) <= 30;

CALL GEOCODING.PUBLIC.REVERSE_GEOCODE_TABLE(
    'GEOCODING.PUBLIC.TEST_EU_REV_INPUT', 'LAT', 'LON', 'ID', 200,
    'GEOCODING.PUBLIC.TEST_EU_REV_OUT');
ALTER TABLE GEOCODING.PUBLIC.TEST_EU_REV_OUT SET COMMENT = 'oss-geocoding';

-- Per country: how many returned the source address exactly, how many are
-- at least as close as the source address, and field coverage.
SELECT i.country,
       COUNT(*)                                                          AS points,
       COUNT(o.input_id)                                                 AS matched,
       COUNT_IF(ST_DISTANCE(o.result_geog, a.geog) < 1)                  AS source_address_returned,
       COUNT_IF(o.distance_m <= ST_DISTANCE(ST_POINT(i.lon, i.lat), a.geog) + 0.1) AS not_farther_than_source,
       COUNT(o.result_city)                                              AS with_city,
       COUNT(o.result_zip)                                               AS with_postcode,
       ROUND(AVG(o.distance_m), 1)                                       AS avg_distance_m
FROM GEOCODING.PUBLIC.TEST_EU_REV_INPUT i
LEFT JOIN GEOCODING.PUBLIC.TEST_EU_REV_OUT o ON o.input_id = i.id
JOIN GEOCODING.PUBLIC.OVERTURE_ADDRESS_EU a ON a.id = i.overture_id
GROUP BY 1 ORDER BY 1;

SELECT input_id, full_address, result_city, result_zip, distance_m
FROM GEOCODING.PUBLIC.TEST_EU_REV_OUT ORDER BY input_id LIMIT 15;
