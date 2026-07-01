-- ============================================================================
-- Forward geocode — usage examples & smoke tests
-- ============================================================================
USE DATABASE GEOCODING;
USE SCHEMA   PUBLIC;

-- ---------------------------------------------------------------------------
-- 1. Test the parser
-- ---------------------------------------------------------------------------
SELECT
    addr,
    PARSE_ADDRESS(addr):number::STRING AS number,
    PARSE_ADDRESS(addr):street::STRING AS street,
    PARSE_ADDRESS(addr):city::STRING   AS city,
    PARSE_ADDRESS(addr):state::STRING  AS state,
    PARSE_ADDRESS(addr):zip::STRING    AS zip
FROM (SELECT * FROM VALUES
    ('125 Constitution Dr, Menlo Park, CA 94025'),
    ('1600 Pennsylvania Ave NW, Washington, DC 20500'),
    ('350 5th Ave, New York, NY 10118'),
    ('8601 W Cross Dr, Littleton, CO 80123')
    AS t(addr));

-- ---------------------------------------------------------------------------
-- 2. Test street standardization
-- ---------------------------------------------------------------------------
SELECT input, STANDARDIZE_STREET(input) AS standardized
FROM (SELECT * FROM VALUES
    ('E 38TH ST'), ('JACK RABBIT TRL'), ('W ANDERSON LN'), ('CR 250'),
    ('N MAIN ST'), ('CONGRESS AVE'), ('FM 1325'), ('S LAMAR BLVD')
    AS t(input));

-- ---------------------------------------------------------------------------
-- 3. Ad-hoc single-address forward geocode (inline, no procedure)
-- ---------------------------------------------------------------------------
WITH parsed AS (
    SELECT
        '175 E 400 S, Salt Lake City, UT 84111' AS raw_address,
        PARSE_ADDRESS('175 E 400 S, Salt Lake City, UT 84111') AS addr
),
fields AS (
    SELECT
        raw_address,
        addr:number::STRING AS number,
        STANDARDIZE_STREET(addr:street::STRING) AS street_std,
        addr:city::STRING AS city,
        addr:zip::STRING  AS zip
    FROM parsed
)
SELECT
    f.raw_address, f.street_std AS input_street_standardized,
    a.NUMBER AS result_number, a.STREET AS result_street,
    a.POSTAL_CITY AS result_city, a.POSTCODE AS result_zip,
    a.GEOMETRY AS result_geog, ST_ASWKT(a.GEOMETRY) AS result_wkt,
    EDITDISTANCE(UPPER(a.STREET), UPPER(f.street_std)) AS edit_dist
FROM fields f
JOIN GEOCODING.PUBLIC.OVERTURE_ADDRESS a
  ON a.COUNTRY = 'US'
 AND a.NUMBER = f.number
 AND (a.POSTCODE = f.zip OR UPPER(a.POSTAL_CITY) = UPPER(f.city))
 AND UPPER(a.STREET) LIKE '%' || UPPER(f.street_std) || '%'
ORDER BY edit_dist
LIMIT 5;

-- ---------------------------------------------------------------------------
-- 4. Batch forward geocode via the stored procedure
-- ---------------------------------------------------------------------------
CREATE OR REPLACE TEMPORARY TABLE _demo_addresses (id INT, address STRING);
INSERT INTO _demo_addresses VALUES
    (1, '175 E 400 S, Salt Lake City, UT 84111'),
    (2, '1600 Pennsylvania Ave NW, Washington, DC 20500'),
    (3, '8601 W Cross Dr, Littleton, CO 80123'),
    (4, '350 5th Ave, New York, NY 10118'),
    (5, '100 Congress Ave, Austin, TX 78701');

CALL FORWARD_GEOCODE_TABLE(
    '_demo_addresses', 'address', 'id', '_demo_addresses_geocoded');

-- result_geog is a GEOGRAPHY point; ST_X/ST_Y recover lon/lat for display.
SELECT
    input_id, raw_address, result_number, result_street, result_city, result_zip,
    result_geog,
    ST_Y(result_geog) AS result_lat,
    ST_X(result_geog) AS result_lon,
    street_sim, edit_dist
FROM _demo_addresses_geocoded
ORDER BY input_id;
