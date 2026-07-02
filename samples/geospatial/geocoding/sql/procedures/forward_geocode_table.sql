-- ============================================================================
-- FORWARD_GEOCODE_TABLE — batch free-text address -> lat/lon
-- ============================================================================
-- Reads a source table of free-text US addresses, runs the parse +
-- standardize + JOIN pipeline against Overture, and writes one best-match
-- row per input into OUTPUT_TABLE.
--
-- Params:
--   SOURCE_TABLE    fully-qualified source table (e.g. MY_DB.PUBLIC.PERMITS)
--   ADDRESS_COLUMN  column holding the free-text address
--   ID_COLUMN       column that uniquely identifies each input row
--   OUTPUT_TABLE    fully-qualified table to (re)create with results
--
-- Match strategy (US only):
--   COUNTRY = 'US' AND NUMBER exact
--   AND (POSTCODE match OR POSTAL_CITY match)   -- ~38% of US rows have NULL city
--   AND ( STREET LIKE %standardized_street%
--         OR JAROWINKLER_SIMILARITY(STREET, standardized_street) >= 92 )
--   ranked by JAROWINKLER_SIMILARITY DESC (normalized 0-100, prefix-weighted),
--   with EDITDISTANCE as a tiebreak. Jaro-Winkler is a better street ranker
--   than raw edit distance, which unfairly penalizes long street names, and it
--   also rescues near-misses that the substring LIKE filter would drop.
--
-- Output columns: input_id, raw_address, result_number, result_street,
--   result_city, result_zip, result_geog (GEOGRAPHY), street_sim, edit_dist
--   result_geog is the matched Overture GEOMETRY point. Use ST_X()/ST_Y() if you
--   need lon/lat back; pass result_geog straight into EVALUATE_GEOCODE.
--
-- Example:
--   CALL GEOCODING.PUBLIC.FORWARD_GEOCODE_TABLE(
--     'MY_DB.PUBLIC.PERMITS', 'ADDRESS_TEXT', 'PERMIT_ID',
--     'MY_DB.PUBLIC.PERMITS_GEOCODED');
-- ============================================================================

CREATE OR REPLACE PROCEDURE GEOCODING.PUBLIC.FORWARD_GEOCODE_TABLE(
    SOURCE_TABLE   VARCHAR,
    ADDRESS_COLUMN VARCHAR,
    ID_COLUMN      VARCHAR,
    OUTPUT_TABLE   VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
COMMENT = 'oss-geocoding'
EXECUTE AS OWNER
AS
$$
DECLARE
    n INTEGER;
BEGIN
    EXECUTE IMMEDIATE '
        CREATE OR REPLACE TABLE ' || OUTPUT_TABLE || ' AS
        WITH src AS (
            SELECT ' || ID_COLUMN || ' AS input_id,
                   ' || ADDRESS_COLUMN || '::STRING AS raw_address
            FROM ' || SOURCE_TABLE || '
        ),
        parsed AS (
            SELECT
                input_id,
                raw_address,
                GEOCODING.PUBLIC.PARSE_ADDRESS(raw_address):number::STRING AS number,
                GEOCODING.PUBLIC.STANDARDIZE_STREET(
                    GEOCODING.PUBLIC.PARSE_ADDRESS(raw_address):street::STRING) AS street_std,
                GEOCODING.PUBLIC.PARSE_ADDRESS(raw_address):city::STRING AS city,
                GEOCODING.PUBLIC.PARSE_ADDRESS(raw_address):zip::STRING  AS zip
            FROM src
        ),
        matched AS (
            SELECT
                p.input_id,
                p.raw_address,
                a.NUMBER      AS result_number,
                a.STREET      AS result_street,
                a.POSTAL_CITY AS result_city,
                a.POSTCODE    AS result_zip,
                a.GEOMETRY    AS result_geog,
                JAROWINKLER_SIMILARITY(UPPER(a.STREET), UPPER(p.street_std)) AS street_sim,
                EDITDISTANCE(UPPER(a.STREET), UPPER(p.street_std)) AS edit_dist,
                ROW_NUMBER() OVER (
                    PARTITION BY p.input_id
                    ORDER BY JAROWINKLER_SIMILARITY(UPPER(a.STREET), UPPER(p.street_std)) DESC,
                             EDITDISTANCE(UPPER(a.STREET), UPPER(p.street_std)) ASC
                ) AS rn
            FROM parsed p
            JOIN GEOCODING.PUBLIC.OVERTURE_ADDRESS a
              ON a.COUNTRY = ''US''
             AND a.NUMBER = p.number
             AND (a.POSTCODE = p.zip OR UPPER(a.POSTAL_CITY) = UPPER(p.city))
             AND (UPPER(a.STREET) LIKE ''%'' || UPPER(p.street_std) || ''%''
                  OR JAROWINKLER_SIMILARITY(UPPER(a.STREET), UPPER(p.street_std)) >= 92)
            WHERE p.number IS NOT NULL
              AND p.street_std IS NOT NULL
              AND (p.zip IS NOT NULL OR p.city IS NOT NULL)
        )
        SELECT input_id, raw_address, result_number, result_street, result_city,
               result_zip, result_geog, street_sim, edit_dist
        FROM matched
        WHERE rn = 1';

    EXECUTE IMMEDIATE 'SELECT COUNT(*) FROM ' || OUTPUT_TABLE;
    n := (SELECT $1 FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())));
    RETURN 'Forward geocode complete. ' || n || ' addresses matched into ' || OUTPUT_TABLE;
END;
$$;
