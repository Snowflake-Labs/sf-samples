-- ============================================================================
-- REVERSE_GEOCODE_TABLE — batch lat/lon -> nearest address
-- ============================================================================
-- Reads a source table of points, finds the closest Overture address for each,
-- and writes one row per matched input into OUTPUT_TABLE.
--
-- Params:
--   SOURCE_TABLE    fully-qualified source table
--   LAT_COLUMN      column holding latitude  (FLOAT)
--   LON_COLUMN      column holding longitude (FLOAT)
--   ID_COLUMN       column that uniquely identifies each input row
--   RADIUS_METERS   MAX search radius (e.g. 200). The search starts small and
--                   expands up to this cap — see "Match strategy" below.
--   OUTPUT_TABLE    fully-qualified table to (re)create with results
--   COUNTRY         optional ISO country filter (e.g. 'US'). NULL / '' / 'ALL'
--                   searches worldwide (Overture is global).
--
-- Match strategy (iterative expanding-radius):
--   Rather than one wide ST_DWITHIN scan for every point, the search starts at
--   a small radius (10 m) and doubles each pass (10, 20, 40, ... up to
--   RADIUS_METERS). Each pass only processes points not yet matched. Points in
--   dense areas resolve in the first cheap pass; only sparse/rural points
--   escalate to a wider search. This is both faster (small candidate sets for
--   most points) and higher-coverage (rural points still get found at the cap).
--   Within each pass, ST_DISTANCE ranks candidates and ROW_NUMBER keeps the
--   single closest address per point.
--
-- Output columns: input_id, input_geog (GEOGRAPHY), full_address, result_city,
--   result_zip, result_country, result_geog (GEOGRAPHY), distance_m
--   input_geog and result_geog are GEOGRAPHY points (built with ST_POINT for the
--   input, the Overture GEOMETRY for the result). Use ST_X()/ST_Y() if you need
--   lon/lat back.
--
-- Example (US only):
--   CALL GEOCODING.PUBLIC.REVERSE_GEOCODE_TABLE(
--     'MY_DB.PUBLIC.PINGS', 'LAT', 'LON', 'DEVICE_ID', 200,
--     'MY_DB.PUBLIC.PINGS_ADDRESSED', 'US');
-- ============================================================================

CREATE OR REPLACE PROCEDURE GEOCODING.PUBLIC.REVERSE_GEOCODE_TABLE(
    SOURCE_TABLE  VARCHAR,
    LAT_COLUMN    VARCHAR,
    LON_COLUMN    VARCHAR,
    ID_COLUMN     VARCHAR,
    RADIUS_METERS FLOAT,
    OUTPUT_TABLE  VARCHAR,
    COUNTRY       VARCHAR DEFAULT NULL)
RETURNS VARCHAR
LANGUAGE SQL
COMMENT = 'oss-geocoding'
EXECUTE AS OWNER
AS
$$
DECLARE
    n            INTEGER;
    country_pred VARCHAR;
    radius       FLOAT DEFAULT 10.0;   -- starting radius; doubles each pass
    eff          FLOAT;                -- effective radius for the current pass
    passes       INTEGER DEFAULT 0;
BEGIN
    IF (COUNTRY IS NULL OR TRIM(COUNTRY) = '' OR UPPER(TRIM(COUNTRY)) = 'ALL') THEN
        country_pred := '';
    ELSE
        country_pred := ' AND a.COUNTRY = ''' || UPPER(TRIM(COUNTRY)) || '''';
    END IF;

    -- Create the output table with the correct schema but no data (LIMIT 0).
    -- Points are GEOGRAPHY: input_geog is built once via ST_POINT(lon, lat),
    -- result_geog is the Overture GEOMETRY.
    EXECUTE IMMEDIATE '
        CREATE OR REPLACE TABLE ' || OUTPUT_TABLE || ' AS
        SELECT
            t1.' || ID_COLUMN || '        AS input_id,
            ST_POINT(t1.' || LON_COLUMN || '::FLOAT, t1.' || LAT_COLUMN || '::FLOAT) AS input_geog,
            CONCAT_WS('' '', a.NUMBER, a.STREET) AS full_address,
            a.POSTAL_CITY                  AS result_city,
            a.POSTCODE                     AS result_zip,
            a.COUNTRY                      AS result_country,
            a.GEOMETRY                     AS result_geog,
            0.0::FLOAT                     AS distance_m
        FROM ' || SOURCE_TABLE || ' t1, GEOCODING.PUBLIC.OVERTURE_ADDRESS a
        LIMIT 0';

    -- Expanding-radius passes. Cap the effective radius at RADIUS_METERS and
    -- guarantee one final pass exactly at the cap.
    FOR i IN 1 TO 40 DO
        eff := LEAST(radius, RADIUS_METERS);
        passes := passes + 1;

        EXECUTE IMMEDIATE '
            INSERT INTO ' || OUTPUT_TABLE || '
            WITH remaining AS (
                SELECT s.' || ID_COLUMN || '  AS input_id,
                       ST_POINT(s.' || LON_COLUMN || '::FLOAT, s.' || LAT_COLUMN || '::FLOAT) AS input_geog
                FROM ' || SOURCE_TABLE || ' s
                WHERE NOT EXISTS (
                    SELECT 1 FROM ' || OUTPUT_TABLE || ' o
                    WHERE o.input_id = s.' || ID_COLUMN || '
                )
            ),
            ranked AS (
                SELECT
                    r.input_id,
                    r.input_geog,
                    CONCAT_WS('' '', a.NUMBER, a.STREET) AS full_address,
                    a.POSTAL_CITY AS result_city,
                    a.POSTCODE    AS result_zip,
                    a.COUNTRY     AS result_country,
                    a.GEOMETRY    AS result_geog,
                    ROUND(ST_DISTANCE(a.GEOMETRY, r.input_geog), 1) AS distance_m,
                    ROW_NUMBER() OVER (
                        PARTITION BY r.input_id
                        ORDER BY ST_DISTANCE(a.GEOMETRY, r.input_geog)
                    ) AS rn
                FROM remaining r
                JOIN GEOCODING.PUBLIC.OVERTURE_ADDRESS a
                  ON ST_DWITHIN(a.GEOMETRY, r.input_geog, ' || eff || ')'
                  || country_pred || '
            )
            SELECT input_id, input_geog, full_address, result_city, result_zip,
                   result_country, result_geog, distance_m
            FROM ranked
            WHERE rn = 1';

        IF (radius >= RADIUS_METERS) THEN
            BREAK;
        END IF;
        radius := radius * 2;
    END FOR;

    EXECUTE IMMEDIATE 'SELECT COUNT(*) FROM ' || OUTPUT_TABLE;
    n := (SELECT $1 FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())));
    RETURN 'Reverse geocode complete. ' || n || ' points matched into ' || OUTPUT_TABLE
           || ' (' || passes || ' radius pass(es), max ' || RADIUS_METERS || 'm)'
           || IFF(country_pred = '', ' (worldwide)', country_pred);
END;
$$;
