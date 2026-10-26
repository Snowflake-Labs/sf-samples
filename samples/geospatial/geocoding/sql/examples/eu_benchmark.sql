-- ============================================================================
-- EU forward geocoding — 10,000-row batch benchmark (DE and FR)
-- ============================================================================
-- Needs the --eu install. Samples 10,000 real Overture addresses per country,
-- renders them in local format (no added noise), geocodes each country as one
-- batch and reports runtime per pipeline step plus accuracy.
--
-- Run with the warehouse you want to measure, e.g.
--   snow sql -c <conn> --warehouse XSMALL -f sql/examples/eu_benchmark.sql
-- ============================================================================

CREATE OR REPLACE TABLE GEOCODING.PUBLIC.TEST_EU_10K COMMENT = 'oss-geocoding' AS
WITH s AS (
    SELECT a.*
    FROM GEOCODING.PUBLIC.OVERTURE_ADDRESS_EU a SAMPLE BERNOULLI (0.2) SEED (7)
    WHERE a.country IN ('DE', 'FR')
      AND a.number IS NOT NULL AND a.street IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY a.country ORDER BY HASH(a.id)) <= 10000
),
c AS (
    SELECT s.id, l.name AS city
    FROM s
    JOIN GEOCODING.PUBLIC.OVERTURE_ADDRESS_EU_LOC k ON k.country = s.country AND k.id = s.id
    JOIN GEOCODING.PUBLIC.EU_LOCALITY l ON l.locality_id = k.locality_id
    QUALIFY ROW_NUMBER() OVER (PARTITION BY s.id ORDER BY
        IFF(l.place_class IS NULL, 1, 0),
        IFF(REGEXP_LIKE(l.name, '^(VVG|GVV|vVG|Verwaltungs|Gemeindeverwaltungs|Amt |Samtgemeinde|Verbandsgemeinde|Mittelbereich).*'), 1, 0),
        ST_AREA(l.geog) DESC) = 1
)
SELECT s.country || '-' || LPAD(ROW_NUMBER() OVER (PARTITION BY s.country ORDER BY HASH(s.id))::STRING, 5, '0') AS test_id,
       s.country,
       s.geog AS actual_geog,
       CASE s.country WHEN 'FR' THEN s.number || ' ' || s.street
                      ELSE s.street || ' ' || s.number END
       || CASE WHEN s.postcode IS NOT NULL OR c.city IS NOT NULL THEN ', ' ELSE '' END
       || ARRAY_TO_STRING(ARRAY_CONSTRUCT_COMPACT(s.postcode, c.city), ' ') AS address
FROM s LEFT JOIN c ON c.id = s.id;

-- One batch per country, tagged so the step timings can be read back.
ALTER SESSION SET QUERY_TAG = 'oss-geocoding-bench-DE';
CREATE OR REPLACE TEMPORARY TABLE GEOCODING.PUBLIC._BENCH_IN AS
    SELECT * FROM GEOCODING.PUBLIC.TEST_EU_10K WHERE country = 'DE';
CALL GEOCODING.PUBLIC.FORWARD_GEOCODE_TABLE(
    'GEOCODING.PUBLIC._BENCH_IN', 'ADDRESS', 'TEST_ID', 'GEOCODING.PUBLIC.TEST_EU_10K_OUT_DE', 'DE');

ALTER SESSION SET QUERY_TAG = 'oss-geocoding-bench-FR';
CREATE OR REPLACE TEMPORARY TABLE GEOCODING.PUBLIC._BENCH_IN AS
    SELECT * FROM GEOCODING.PUBLIC.TEST_EU_10K WHERE country = 'FR';
CALL GEOCODING.PUBLIC.FORWARD_GEOCODE_TABLE(
    'GEOCODING.PUBLIC._BENCH_IN', 'ADDRESS', 'TEST_ID', 'GEOCODING.PUBLIC.TEST_EU_10K_OUT_FR', 'FR');

ALTER SESSION SET QUERY_TAG = 'oss-geocoding';

-- Runtime per step (the CALL row is the end-to-end time).
SELECT query_tag, warehouse_size,
       CASE WHEN query_text ILIKE 'CALL%'                 THEN '0 total'
            WHEN query_text ILIKE '%_FGC_EU_PARSED AS%'   THEN '1 libpostal parse + expand'
            WHEN query_text ILIKE '%_FGC_EU_KEYS AS%'     THEN '2 street keys'
            WHEN query_text ILIKE '%_FGC_EU_LOC AS%'      THEN '3 locality lookup'
            WHEN query_text ILIKE '%_FGC_EU_CAND AS%'     THEN '4 candidate join'
            WHEN query_text ILIKE '%_FGC_EU_RES%'         THEN '5 ranking + fallbacks'
            ELSE '6 staging + output' END AS step,
       ROUND(SUM(total_elapsed_time) / 1000, 1) AS seconds
FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY_BY_SESSION(RESULT_LIMIT => 1000))
WHERE query_tag LIKE 'oss-geocoding-bench-%'
GROUP BY 1, 2, 3
ORDER BY 1, 3;

-- Accuracy by country and match level.
SELECT o.result_country AS country, o.match_level, COUNT(*) AS n,
       ROUND(100 * COUNT_IF(ST_DISTANCE(t.actual_geog, o.result_geog) <= 100) / COUNT(*), 1) AS within_100m_pct,
       ROUND(100 * COUNT_IF(ST_DISTANCE(t.actual_geog, o.result_geog) <= 500) / COUNT(*), 1) AS within_500m_pct,
       ROUND(AVG(ST_DISTANCE(t.actual_geog, o.result_geog))) AS avg_distance_m
FROM (SELECT * FROM GEOCODING.PUBLIC.TEST_EU_10K_OUT_DE
      UNION ALL SELECT * FROM GEOCODING.PUBLIC.TEST_EU_10K_OUT_FR) o
JOIN GEOCODING.PUBLIC.TEST_EU_10K t ON t.test_id = o.input_id
GROUP BY GROUPING SETS ((1), (1, 2))
ORDER BY 1, GROUPING(o.match_level) DESC, 2;
