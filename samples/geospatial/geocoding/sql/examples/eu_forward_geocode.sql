-- ============================================================================
-- European forward geocoding — run + accuracy evaluation
-- ============================================================================
-- Needs the --eu install (libpostal service READY + BUILD_EU_INDEX tables) and
-- the test set from eu_test_set.sql.
-- ============================================================================
USE DATABASE GEOCODING;
USE SCHEMA   PUBLIC;

-- 1. Single-country call (COUNTRY parameter)
CREATE OR REPLACE TEMPORARY TABLE _demo_eu (id INT, address STRING);
INSERT INTO _demo_eu VALUES
    (1, 'Hauptstr. 12, 10827 Berlin'),
    (2, 'Waldstrasse 1, Hamburg'),
    (3, 'Karl-Marx-Allee 90, Berlin');
CALL GEOCODING.PUBLIC.FORWARD_GEOCODE_TABLE(
    '_demo_eu', 'address', 'id', '_demo_eu_geocoded', 'DE');
ALTER TABLE _demo_eu_geocoded SET COMMENT = 'oss-geocoding';
SELECT input_id, raw_address, result_street, result_number, result_zip,
       match_level, match_score, result_geog
FROM _demo_eu_geocoded ORDER BY input_id;

-- 2. Mixed countries via a country column (US rows keep the US path; GB has no
--    Overture addresses and returns MATCH_LEVEL = 'NONE').
CREATE OR REPLACE TEMPORARY TABLE _demo_mixed (id INT, address STRING, cc STRING);
INSERT INTO _demo_mixed VALUES
    (1, '350 5th Ave, New York, NY 10118',            'US'),
    (2, '8 Rue de la Poste, 80430 Liomer',            'FR'),
    (3, 'Via Silvio Pellico 8, Milano',               'IT'),
    (4, '10 Downing St, London SW1A 2AA',             'GB');
CALL GEOCODING.PUBLIC.FORWARD_GEOCODE_TABLE(
    '_demo_mixed', 'address', 'id', '_demo_mixed_geocoded', 'US', 'cc');
ALTER TABLE _demo_mixed_geocoded SET COMMENT = 'oss-geocoding';
SELECT input_id, result_country, raw_address, result_street, result_number,
       match_level, match_score
FROM _demo_mixed_geocoded ORDER BY input_id;

-- 3. Accuracy on the EU test set (360 rows, 60 per country)
CALL GEOCODING.PUBLIC.FORWARD_GEOCODE_TABLE(
    'GEOCODING.PUBLIC.TEST_EU_INPUT', 'ADDRESS', 'TEST_ID',
    'GEOCODING.PUBLIC.TEST_EU_RESULT', 'US', 'COUNTRY');
ALTER TABLE GEOCODING.PUBLIC.TEST_EU_RESULT SET COMMENT = 'oss-geocoding';

CREATE OR REPLACE TABLE GEOCODING.PUBLIC.TEST_EU_EVAL COMMENT = 'oss-geocoding' AS
SELECT t.test_id, t.country, t.noise, r.match_level, r.match_score,
       t.actual_geog, r.result_geog,
       ST_DISTANCE(t.actual_geog, r.result_geog) AS dist_m
FROM GEOCODING.PUBLIC.TEST_EU_INPUT t
JOIN GEOCODING.PUBLIC.TEST_EU_RESULT r ON r.input_id = t.test_id;

-- Per country
CALL GEOCODING.PUBLIC.EVALUATE_GEOCODE(
    'GEOCODING.PUBLIC.TEST_EU_EVAL', 'ACTUAL_GEOG', 'RESULT_GEOG', 200, 'COUNTRY');
-- Per match level
CALL GEOCODING.PUBLIC.EVALUATE_GEOCODE(
    'GEOCODING.PUBLIC.TEST_EU_EVAL', 'ACTUAL_GEOG', 'RESULT_GEOG', 200, 'MATCH_LEVEL');
-- Per country x match level
SELECT country, match_level, COUNT(*) AS n,
       ROUND(100 * COUNT_IF(dist_m <= 100) / COUNT(*), 1) AS within_100m_pct,
       ROUND(100 * COUNT_IF(dist_m <= 500) / COUNT(*), 1) AS within_500m_pct,
       ROUND(AVG(dist_m), 1) AS avg_dist_m
FROM GEOCODING.PUBLIC.TEST_EU_EVAL
GROUP BY 1, 2 ORDER BY 1, 2;
-- Per noise type
SELECT noise, COUNT(*) AS n, COUNT_IF(match_level = 'ADDRESS') AS address_hits,
       ROUND(100 * COUNT_IF(dist_m <= 100) / COUNT(*), 1) AS within_100m_pct
FROM GEOCODING.PUBLIC.TEST_EU_EVAL GROUP BY 1 ORDER BY 1;
