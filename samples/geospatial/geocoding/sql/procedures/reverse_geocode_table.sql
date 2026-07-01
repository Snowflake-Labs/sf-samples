-- ============================================================================
-- REVERSE_GEOCODE_TABLE — batch lat/lon -> nearest address
-- ============================================================================
-- Reads a source table of points, finds the closest Overture address within
-- RADIUS_METERS for each, and writes one row per input into OUTPUT_TABLE.
--
-- Params:
--   SOURCE_TABLE    fully-qualified source table
--   LAT_COLUMN      column holding latitude  (FLOAT)
--   LON_COLUMN      column holding longitude (FLOAT)
--   ID_COLUMN       column that uniquely identifies each input row
--   RADIUS_METERS   search radius for ST_DWITHIN (e.g. 200)
--   OUTPUT_TABLE    fully-qualified table to (re)create with results
--   COUNTRY         optional ISO country filter (e.g. 'US'). NULL / '' / 'ALL'
--                   searches worldwide (Overture is global).
--
-- Match strategy:
--   ST_DWITHIN(GEOMETRY, point, radius) to filter candidates,
--   ST_DISTANCE to rank, ROW_NUMBER dedup to the single closest address.
--
-- Output columns: input_id, input_lat, input_lon, full_address, result_city,
--   result_zip, result_country, result_lat, result_lon, distance_m
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
    n INTEGER;
    country_pred VARCHAR;
BEGIN
    IF (COUNTRY IS NULL OR TRIM(COUNTRY) = '' OR UPPER(TRIM(COUNTRY)) = 'ALL') THEN
        country_pred := '';
    ELSE
        country_pred := ' AND a.COUNTRY = ''' || UPPER(TRIM(COUNTRY)) || '''';
    END IF;

    EXECUTE IMMEDIATE '
        CREATE OR REPLACE TABLE ' || OUTPUT_TABLE || ' AS
        WITH src AS (
            SELECT ' || ID_COLUMN || '  AS input_id,
                   ' || LAT_COLUMN || '::FLOAT AS input_lat,
                   ' || LON_COLUMN || '::FLOAT AS input_lon
            FROM ' || SOURCE_TABLE || '
        ),
        ranked AS (
            SELECT
                s.input_id,
                s.input_lat,
                s.input_lon,
                a.NUMBER || '' '' || a.STREET AS full_address,
                a.POSTAL_CITY AS result_city,
                a.POSTCODE    AS result_zip,
                a.COUNTRY     AS result_country,
                ST_Y(a.GEOMETRY) AS result_lat,
                ST_X(a.GEOMETRY) AS result_lon,
                ROUND(ST_DISTANCE(a.GEOMETRY,
                    TO_GEOGRAPHY(''POINT('' || s.input_lon || '' '' || s.input_lat || '')'')), 1) AS distance_m,
                ROW_NUMBER() OVER (
                    PARTITION BY s.input_id
                    ORDER BY ST_DISTANCE(a.GEOMETRY,
                        TO_GEOGRAPHY(''POINT('' || s.input_lon || '' '' || s.input_lat || '')''))
                ) AS rn
            FROM src s
            JOIN GEOCODING.PUBLIC.OVERTURE_ADDRESS a
              ON ST_DWITHIN(a.GEOMETRY,
                    TO_GEOGRAPHY(''POINT('' || s.input_lon || '' '' || s.input_lat || '')''), ' || RADIUS_METERS || ')'
              || country_pred || '
        )
        SELECT input_id, input_lat, input_lon, full_address, result_city, result_zip,
               result_country, result_lat, result_lon, distance_m
        FROM ranked
        WHERE rn = 1';

    EXECUTE IMMEDIATE 'SELECT COUNT(*) FROM ' || OUTPUT_TABLE;
    n := (SELECT $1 FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())));
    RETURN 'Reverse geocode complete. ' || n || ' points matched into ' || OUTPUT_TABLE
           || IFF(country_pred = '', ' (worldwide)', country_pred);
END;
$$;
