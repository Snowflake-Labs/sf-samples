-- ============================================================================
-- Reverse geocode — usage examples & smoke tests
-- ============================================================================
USE DATABASE GEOCODING;
USE SCHEMA   PUBLIC;

-- ---------------------------------------------------------------------------
-- 1. Ad-hoc single-point reverse geocode (Salt Lake City)
-- ---------------------------------------------------------------------------
SELECT
    a.ID, a.NUMBER, a.STREET, a.UNIT, a.POSTCODE, a.POSTAL_CITY,
    ST_Y(a.GEOMETRY) AS result_lat,
    ST_X(a.GEOMETRY) AS result_lon,
    ROUND(ST_DISTANCE(a.GEOMETRY, TO_GEOGRAPHY('POINT(-111.890913 40.760681)')), 1) AS distance_m
FROM GEOCODING.PUBLIC.OVERTURE_ADDRESS a
WHERE ST_DWITHIN(a.GEOMETRY, TO_GEOGRAPHY('POINT(-111.890913 40.760681)'), 200)
  AND a.COUNTRY = 'US'
ORDER BY distance_m
LIMIT 5;

-- ---------------------------------------------------------------------------
-- 2. Batch reverse geocode via the stored procedure
-- ---------------------------------------------------------------------------
CREATE OR REPLACE TEMPORARY TABLE _demo_points (label STRING, lat FLOAT, lon FLOAT);
INSERT INTO _demo_points VALUES
    ('SLC_Office',     40.760681, -111.890913),
    ('Austin_Capitol', 30.274670,  -97.740349),
    ('NYC_ESB',        40.748817,  -73.985428);

-- US only:
CALL REVERSE_GEOCODE_TABLE(
    '_demo_points', 'lat', 'lon', 'label', 200,
    '_demo_points_addressed', 'US');

SELECT * FROM _demo_points_addressed ORDER BY input_id;

-- Worldwide (omit or pass NULL/'ALL' for COUNTRY):
-- CALL REVERSE_GEOCODE_TABLE(
--     '_demo_points', 'lat', 'lon', 'label', 200, '_demo_points_addressed');
