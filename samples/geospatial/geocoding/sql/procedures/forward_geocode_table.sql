-- ============================================================================
-- FORWARD_GEOCODE_TABLE — batch free-text address -> GEOGRAPHY point
-- ============================================================================
-- Reads a source table of free-text addresses, geocodes them against Overture,
-- and writes one row per input into OUTPUT_TABLE.
--
-- Params:
--   SOURCE_TABLE    fully-qualified source table (e.g. MY_DB.PUBLIC.PERMITS)
--   ADDRESS_COLUMN  column holding the free-text address
--   ID_COLUMN       column that uniquely identifies each input row
--   OUTPUT_TABLE    fully-qualified table to (re)create with results
--   COUNTRY         ISO-2 country for every row (default 'US')
--   COUNTRY_COLUMN  optional column holding a per-row ISO-2 country; overrides
--                   COUNTRY where it is not NULL
--
-- US rows (unchanged behaviour):
--   usaddress parse + USPS street standardization, then
--   COUNTRY = 'US' AND NUMBER exact
--   AND (POSTCODE match OR POSTAL_CITY match)   -- ~38% of US rows have NULL city
--   AND ( STREET LIKE %standardized_street%
--         OR JAROWINKLER_SIMILARITY(STREET, standardized_street) >= 92 )
--   ranked by JAROWINKLER_SIMILARITY DESC, EDITDISTANCE ASC; ties prefer the
--   candidate whose postcode matches, then the lowest Overture ID, so repeated
--   runs return the same row.
--   When every row is US the output table has exactly the original 9 columns
--   and only matched rows, as before.
--
-- European rows (FR ES PL BE DK CH AT NL NO FI PT DE IT CZ), needs the --eu
-- install (libpostal service + BUILD_EU_INDEX tables):
--   1. PARSE_ADDRESS_INTL splits number / road / postcode / city.
--      EXPAND_STREET_INTL expands the road; every variant and the Overture
--      street are reduced with NORMALIZE_STREET_EU to one comparison key.
--   2. Candidates are only ever fetched by equi-join on
--        country + postcode + house number        (OVERTURE_ADDRESS_EU)
--        country + locality polygon + house number (OVERTURE_ADDRESS_EU_LOC)
--      The locality comes from matching the parsed city to EU_LOCALITY; the
--      address-to-polygon assignment (ST_INTERSECTS) was done at build time.
--      No fuzzy comparison runs before that equi-join.
--   3. Best candidate: Jaro-Winkler on the street key, then exact number+unit,
--      then locality and postcode agreement, then edit distance.
--   4. Fallbacks when no house-number match: best street in the postcode /
--      locality (centroid of its addresses), then postcode centroid, then
--      locality centroid.
--   GB / UK / IE and any other unsupported country return MATCH_LEVEL 'NONE'
--   (this Overture share has no addresses there).
--
-- Output columns: input_id, raw_address, result_number, result_street,
--   result_city, result_zip, result_geog (GEOGRAPHY), street_sim, edit_dist
-- plus, when any input row is non-US:
--   result_country, match_level (ADDRESS | STREET | POSTCODE | LOCALITY | NONE),
--   match_score (0-100: ADDRESS = street similarity, STREET = 0.8 x similarity,
--   POSTCODE = 50, LOCALITY = 30, NONE = 0). Every input row is returned.
--
-- Example:
--   CALL GEOCODING.PUBLIC.FORWARD_GEOCODE_TABLE(
--     'MY_DB.PUBLIC.PERMITS', 'ADDRESS_TEXT', 'PERMIT_ID',
--     'MY_DB.PUBLIC.PERMITS_GEOCODED');                          -- US
--   CALL GEOCODING.PUBLIC.FORWARD_GEOCODE_TABLE(
--     'MY_DB.PUBLIC.CUSTOMERS', 'ADDRESS', 'ID',
--     'MY_DB.PUBLIC.CUSTOMERS_GEOCODED', 'US', 'COUNTRY_CODE');  -- mixed
-- ============================================================================

-- The 4-argument signature is replaced by the 6-argument one with defaults;
-- leaving both would make 4-argument calls ambiguous.
DROP PROCEDURE IF EXISTS GEOCODING.PUBLIC.FORWARD_GEOCODE_TABLE(VARCHAR, VARCHAR, VARCHAR, VARCHAR);

CREATE OR REPLACE PROCEDURE GEOCODING.PUBLIC.FORWARD_GEOCODE_TABLE(
    SOURCE_TABLE   VARCHAR,
    ADDRESS_COLUMN VARCHAR,
    ID_COLUMN      VARCHAR,
    OUTPUT_TABLE   VARCHAR,
    COUNTRY        VARCHAR DEFAULT 'US',
    COUNTRY_COLUMN VARCHAR DEFAULT NULL)
RETURNS VARCHAR
LANGUAGE SQL
COMMENT = 'oss-geocoding'
EXECUTE AS OWNER
AS
$$
DECLARE
    n            INTEGER;
    n_hit        INTEGER;
    n_non_us     INTEGER;
    n_eu         INTEGER;
    has_intl     INTEGER;
    country_expr VARCHAR;
    us_src       VARCHAR;
    us_sql       VARCHAR;
    bad_arg EXCEPTION (-20001, 'Invalid argument: table/column names must be plain or double-quoted identifiers; country must be a 2-letter code.');
BEGIN
    IF (NOT REGEXP_LIKE(SOURCE_TABLE, '^([A-Za-z_][A-Za-z0-9_$]*|"[^"]+")([.]([A-Za-z_][A-Za-z0-9_$]*|"[^"]+")){0,2}$')) THEN
        RAISE bad_arg;
    END IF;
    IF (NOT REGEXP_LIKE(OUTPUT_TABLE, '^([A-Za-z_][A-Za-z0-9_$]*|"[^"]+")([.]([A-Za-z_][A-Za-z0-9_$]*|"[^"]+")){0,2}$')) THEN
        RAISE bad_arg;
    END IF;
    IF (NOT REGEXP_LIKE(ADDRESS_COLUMN, '^([A-Za-z_][A-Za-z0-9_$]*|"[^"]+")$')) THEN
        RAISE bad_arg;
    END IF;
    IF (NOT REGEXP_LIKE(ID_COLUMN, '^([A-Za-z_][A-Za-z0-9_$]*|"[^"]+")$')) THEN
        RAISE bad_arg;
    END IF;
    IF (COUNTRY_COLUMN IS NOT NULL AND TRIM(COUNTRY_COLUMN) <> '' AND NOT REGEXP_LIKE(COUNTRY_COLUMN, '^([A-Za-z_][A-Za-z0-9_$]*|"[^"]+")$')) THEN
        RAISE bad_arg;
    END IF;
    IF (COUNTRY IS NOT NULL AND NOT REGEXP_LIKE(TRIM(COUNTRY), '^[A-Za-z]{2}$')) THEN
        RAISE bad_arg;
    END IF;

    country_expr := '''' || UPPER(TRIM(COALESCE(COUNTRY, 'US'))) || '''';
    IF (COUNTRY_COLUMN IS NOT NULL AND TRIM(COUNTRY_COLUMN) <> '') THEN
        country_expr := 'COALESCE(NULLIF(UPPER(TRIM(' || COUNTRY_COLUMN || '::STRING)), ''''), '
                        || country_expr || ')';
    END IF;

    -- -----------------------------------------------------------------------
    -- Pure-US input: run the original pipeline over the source table as is.
    -- -----------------------------------------------------------------------
    EXECUTE IMMEDIATE 'SELECT COUNT_IF(' || country_expr || ' <> ''US'') FROM ' || SOURCE_TABLE;
    n_non_us := (SELECT $1 FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())));

    IF (n_non_us = 0) THEN
        us_src := 'SELECT ' || ID_COLUMN || ' AS input_id,
                   ' || ADDRESS_COLUMN || '::STRING AS raw_address
            FROM ' || SOURCE_TABLE;
    ELSE
        -- Stage every input row once with its resolved country.
        EXECUTE IMMEDIATE '
            CREATE OR REPLACE TEMPORARY TABLE GEOCODING.PUBLIC._FGC_IN AS
            SELECT ' || ID_COLUMN || ' AS input_id,
                   ' || ADDRESS_COLUMN || '::STRING AS raw_address,
                   ' || country_expr || ' AS country
            FROM ' || SOURCE_TABLE;
        us_src := 'SELECT input_id, raw_address FROM GEOCODING.PUBLIC._FGC_IN WHERE country = ''US''';
    END IF;

    us_sql := '
        WITH src AS (
            ' || us_src || '
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
                             EDITDISTANCE(UPPER(a.STREET), UPPER(p.street_std)) ASC,
                             IFF(a.POSTCODE = p.zip, 0, 1) ASC,
                             a.ID
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

    IF (n_non_us = 0) THEN
        EXECUTE IMMEDIATE 'CREATE OR REPLACE TABLE ' || OUTPUT_TABLE || ' AS ' || us_sql;
        EXECUTE IMMEDIATE 'SELECT COUNT(*) FROM ' || OUTPUT_TABLE;
        n := (SELECT $1 FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())));
        RETURN 'Forward geocode complete. ' || n || ' addresses matched into ' || OUTPUT_TABLE;
    END IF;

    -- -----------------------------------------------------------------------
    -- Mixed / non-US input.
    -- -----------------------------------------------------------------------
    EXECUTE IMMEDIATE 'CREATE OR REPLACE TEMPORARY TABLE GEOCODING.PUBLIC._FGC_US AS ' || us_sql;

    CREATE OR REPLACE TEMPORARY TABLE GEOCODING.PUBLIC._FGC_EU_ADDR AS
    SELECT DISTINCT country, raw_address
    FROM GEOCODING.PUBLIC._FGC_IN
    WHERE country IN ('FR','ES','PL','BE','DK','CH','AT','NL','NO','FI','PT','DE','IT','CZ')
      AND raw_address IS NOT NULL;

    SELECT COUNT(*) INTO :n_eu FROM GEOCODING.PUBLIC._FGC_EU_ADDR;

    IF (n_eu > 0) THEN
        SELECT COUNT(*) INTO :has_intl
        FROM GEOCODING.INFORMATION_SCHEMA.FUNCTIONS
        WHERE FUNCTION_SCHEMA = 'PUBLIC'
          AND FUNCTION_NAME IN ('PARSE_ADDRESS_INTL', 'EXPAND_STREET_INTL');
        IF (has_intl < 2) THEN
            RETURN 'ERROR: ' || n_eu || ' European address(es) need the libpostal service. '
                || 'Re-run the installer with --eu, then retry.';
        END IF;
    ELSE
        -- Only US / unsupported countries: no European work, empty result set.
        CREATE OR REPLACE TEMPORARY TABLE GEOCODING.PUBLIC._FGC_EU_RES (
            country VARCHAR, raw_address VARCHAR, result_number VARCHAR,
            result_street VARCHAR, result_city VARCHAR, result_zip VARCHAR,
            result_geog GEOGRAPHY, street_sim NUMBER, edit_dist NUMBER,
            match_level VARCHAR, match_score FLOAT);
    END IF;

    IF (n_eu > 0) THEN

    -- 1. Parse each distinct European address once with libpostal.
    CREATE OR REPLACE TEMPORARY TABLE GEOCODING.PUBLIC._FGC_EU_PARSED AS
    WITH p AS (
        SELECT country, raw_address,
               GEOCODING.PUBLIC.PARSE_ADDRESS_INTL(raw_address) AS j
        FROM GEOCODING.PUBLIC._FGC_EU_ADDR
    ),
    f AS (
        SELECT country, raw_address,
               j:house_number::STRING AS number,
               j:road::STRING         AS road,
               j:postcode::STRING     AS postcode,
               -- libpostal often labels the town people write in Europe as
               -- suburb / city_district ("Mengede", "lanciano"), so every
               -- place-like label is a locality candidate; city wins for display.
               COALESCE(j:city::STRING, j:suburb::STRING, j:city_district::STRING,
                        j:island::STRING, j:state_district::STRING) AS city,
               ARRAY_CONSTRUCT_COMPACT(j:city::STRING, j:suburb::STRING,
                        j:city_district::STRING, j:island::STRING,
                        j:state_district::STRING,
                        -- libpostal sometimes labels a lower-case town as
                        -- nothing at all; the text after the last comma, minus
                        -- any postcode, is tried as an exact locality key too.
                        IFF(CONTAINS(raw_address, ','),
                            NULLIF(TRIM(REGEXP_REPLACE(REGEXP_SUBSTR(raw_address, '[^,]+$'),
                                                       '[0-9]{2,}[- ]?[0-9]*[A-Za-z]{0,2}\\b', '')), ''),
                            NULL)) AS city_names
        FROM p
    )
    SELECT country, raw_address, number,
           REGEXP_SUBSTR(number, '^[0-9]+')                                 AS num_base,
           LOWER(REGEXP_REPLACE(number, '[^0-9A-Za-z]', ''))                AS num_full,
           road,
           NULLIF(UPPER(REGEXP_REPLACE(postcode, '[^0-9A-Za-z]', '')), '')  AS postcode_norm,
           city,
           GEOCODING.PUBLIC.NORMALIZE_NAME_EU(city)                         AS city_key,
           city_names,
           IFF(road IS NULL, NULL, GEOCODING.PUBLIC.EXPAND_STREET_INTL(road)) AS expansions
    FROM f;

    -- 2. Street comparison keys: the raw road plus every libpostal expansion,
    --    plus the first comma segment minus the house number. libpostal reads
    --    French lieu-dits without a street type ("5 La Maçonnais") as a city
    --    and abbreviations such as "r. saint brice" as road "r.", so the raw
    --    segment is the only usable street text for those.
    CREATE OR REPLACE TEMPORARY TABLE GEOCODING.PUBLIC._FGC_EU_KEYS AS
    SELECT DISTINCT p.country, p.raw_address,
           GEOCODING.PUBLIC.NORMALIZE_STREET_EU(v.value::STRING, p.country) AS street_key
    FROM GEOCODING.PUBLIC._FGC_EU_PARSED p,
         LATERAL FLATTEN(input => ARRAY_CAT(COALESCE(p.expansions, ARRAY_CONSTRUCT()),
                                            ARRAY_CONSTRUCT_COMPACT(p.road,
                                                NULLIF(TRIM(REGEXP_REPLACE(REGEXP_SUBSTR(p.raw_address, '^[^,]+'),
                                                       '(^[0-9]+[A-Za-z]?\\b|\\b[0-9]+[A-Za-z]?$)', '')), '')))) v
    WHERE v.value IS NOT NULL;
    DELETE FROM GEOCODING.PUBLIC._FGC_EU_KEYS WHERE street_key IS NULL;

    -- 3. Candidate localities: exact key on any place label against any name
    --    alias ("Pabillonis" matches "Pabillois/Pabillonis"), else fuzzy on the
    --    main city key within the country (only against aliases sharing the
    --    first two key letters).
    CREATE OR REPLACE TEMPORARY TABLE GEOCODING.PUBLIC._FGC_EU_LOC AS
    WITH keys AS (
        SELECT DISTINCT p.country, p.raw_address,
               GEOCODING.PUBLIC.NORMALIZE_NAME_EU(c.value::STRING) AS k
        FROM GEOCODING.PUBLIC._FGC_EU_PARSED p,
             LATERAL FLATTEN(input => p.city_names) c
    ),
    ex AS (
        SELECT DISTINCT k.country, k.raw_address, l.locality_id, l.name, 100 AS sim
        FROM keys k
        JOIN GEOCODING.PUBLIC.EU_LOCALITY_ALIAS a
          ON a.country = k.country AND a.alias_key = k.k
        JOIN GEOCODING.PUBLIC.EU_LOCALITY l ON l.locality_id = a.locality_id
    ),
    fz AS (
        SELECT p.country, p.raw_address, l.locality_id, l.name,
               MAX(JAROWINKLER_SIMILARITY(a.alias_key, p.city_key)) AS sim
        FROM GEOCODING.PUBLIC._FGC_EU_PARSED p
        JOIN GEOCODING.PUBLIC.EU_LOCALITY_ALIAS a
          ON a.country = p.country AND LEFT(a.alias_key, 2) = LEFT(p.city_key, 2)
        JOIN GEOCODING.PUBLIC.EU_LOCALITY l ON l.locality_id = a.locality_id
        WHERE p.city_key IS NOT NULL
          AND NOT EXISTS (SELECT 1 FROM ex
                          WHERE ex.country = p.country AND ex.raw_address = p.raw_address)
          AND JAROWINKLER_SIMILARITY(a.alias_key, p.city_key) >= 92
        GROUP BY p.country, p.raw_address, l.locality_id, l.name
    )
    SELECT * FROM ex
    UNION ALL
    SELECT * FROM fz;

    -- 4. House-number candidates via the two equi-join paths, scored on the
    --    street key. One row per (input address, Overture address).
    CREATE OR REPLACE TEMPORARY TABLE GEOCODING.PUBLIC._FGC_EU_CAND AS
    WITH pc AS (
        SELECT p.country, p.raw_address, a.id, a.number, a.num_full, a.street,
               a.street_key, a.postcode, a.postal_city, a.geog, 1 AS pc_hit, 0 AS loc_hit,
               NULL AS loc_name
        FROM GEOCODING.PUBLIC._FGC_EU_PARSED p
        JOIN GEOCODING.PUBLIC.OVERTURE_ADDRESS_EU a
          ON a.country = p.country
         AND a.postcode_norm = p.postcode_norm
         AND a.num_base = p.num_base
    ),
    lc AS (
        SELECT p.country, p.raw_address, a.id, a.number, a.num_full, a.street,
               a.street_key, a.postcode, NULL AS postal_city, a.geog,
               IFF(UPPER(REGEXP_REPLACE(a.postcode, '[^0-9A-Za-z]', '')) = p.postcode_norm, 1, 0) AS pc_hit,
               1 AS loc_hit, l.name AS loc_name
        FROM GEOCODING.PUBLIC._FGC_EU_PARSED p
        JOIN GEOCODING.PUBLIC._FGC_EU_LOC l
          ON l.country = p.country AND l.raw_address = p.raw_address
        JOIN GEOCODING.PUBLIC.OVERTURE_ADDRESS_EU_LOC a
          ON a.country = l.country
         AND a.locality_id = l.locality_id
         AND a.num_base = p.num_base
    ),
    u AS (SELECT * FROM pc UNION ALL SELECT * FROM lc),
    scored AS (
        SELECT u.country, u.raw_address, u.id,
               ANY_VALUE(u.number) AS number, ANY_VALUE(u.num_full) AS num_full,
               ANY_VALUE(u.street) AS street, ANY_VALUE(u.street_key) AS street_key,
               ANY_VALUE(u.postcode) AS postcode, MAX(u.postal_city) AS postal_city,
               MAX(u.loc_name) AS loc_name, ANY_VALUE(u.geog) AS geog,
               MAX(u.pc_hit) AS pc_hit, MAX(u.loc_hit) AS loc_hit,
               MAX(JAROWINKLER_SIMILARITY(u.street_key, k.street_key)) AS sim,
               MIN(EDITDISTANCE(u.street_key, k.street_key)) AS ed
        FROM u
        LEFT JOIN GEOCODING.PUBLIC._FGC_EU_KEYS k
          ON k.country = u.country AND k.raw_address = u.raw_address
        GROUP BY u.country, u.raw_address, u.id
    )
    SELECT s.*, p.num_full AS in_num_full, p.road AS in_road, p.postcode_norm AS in_pc
    FROM scored s
    JOIN GEOCODING.PUBLIC._FGC_EU_PARSED p
      ON p.country = s.country AND p.raw_address = s.raw_address;

    -- 5. ADDRESS level. A street mismatch rejects the candidate. Overture rows
    --    with no street (Polish / Czech village addresses) are accepted on the
    --    number plus postcode or locality at a lower score, but when the input
    --    has a postcode it must agree: a street-less row has nothing else to
    --    confirm it, and same-named villages exist across the country. An input with no
    --    parsed road only matches such street-less rows; it is never matched
    --    to a named street it did not mention.
    --    Ranking: street similarity, then exact number+unit (3a over 3), then
    --    agreement with the input locality (one Polish postcode covers several
    --    villages that reuse street names), then postcode agreement.
    CREATE OR REPLACE TEMPORARY TABLE GEOCODING.PUBLIC._FGC_EU_RES AS
    SELECT country, raw_address, number AS result_number, street AS result_street,
           COALESCE(postal_city, loc_name) AS result_city, postcode AS result_zip,
           geog AS result_geog, sim AS street_sim, ed AS edit_dist,
           'ADDRESS'::VARCHAR AS match_level,
           COALESCE(sim, IFF(street_key IS NULL AND in_road IS NOT NULL, 70, 60))::FLOAT AS match_score
    FROM GEOCODING.PUBLIC._FGC_EU_CAND
    WHERE sim >= 85 OR (street_key IS NULL AND (pc_hit = 1 OR in_pc IS NULL))
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY country, raw_address
        ORDER BY COALESCE(sim, 0) DESC, IFF(num_full = in_num_full, 1, 0) DESC,
                 loc_hit DESC, pc_hit DESC, COALESCE(ed, 999) ASC, id) = 1;

    -- 6. STREET level: best street in the input's postcode / locality.
    INSERT INTO GEOCODING.PUBLIC._FGC_EU_RES
    WITH un AS (
        SELECT p.* FROM GEOCODING.PUBLIC._FGC_EU_PARSED p
        WHERE p.road IS NOT NULL
          AND NOT EXISTS (SELECT 1 FROM GEOCODING.PUBLIC._FGC_EU_RES r
                          WHERE r.country = p.country AND r.raw_address = p.raw_address)
    ),
    area AS (
        SELECT un.country, un.raw_address, a.street_key, a.street, a.postcode, a.geog
        FROM un JOIN GEOCODING.PUBLIC.OVERTURE_ADDRESS_EU a
          ON a.country = un.country AND a.postcode_norm = un.postcode_norm
        UNION ALL
        SELECT un.country, un.raw_address, a.street_key, a.street, a.postcode, a.geog
        FROM un
        JOIN GEOCODING.PUBLIC._FGC_EU_LOC l
          ON l.country = un.country AND l.raw_address = un.raw_address
        JOIN GEOCODING.PUBLIC.OVERTURE_ADDRESS_EU_LOC a
          ON a.country = l.country AND a.locality_id = l.locality_id
    ),
    streets AS (
        SELECT DISTINCT country, raw_address, street_key FROM area WHERE street_key IS NOT NULL
    ),
    best AS (
        SELECT s.country, s.raw_address, s.street_key,
               MAX(JAROWINKLER_SIMILARITY(s.street_key, k.street_key)) AS sim,
               MIN(EDITDISTANCE(s.street_key, k.street_key)) AS ed
        FROM streets s
        JOIN GEOCODING.PUBLIC._FGC_EU_KEYS k
          ON k.country = s.country AND k.raw_address = s.raw_address
        GROUP BY s.country, s.raw_address, s.street_key
        HAVING MAX(JAROWINKLER_SIMILARITY(s.street_key, k.street_key)) >= 85
        QUALIFY ROW_NUMBER() OVER (PARTITION BY s.country, s.raw_address
                                   ORDER BY sim DESC, ed ASC, s.street_key) = 1
    )
    SELECT b.country, b.raw_address, NULL, ANY_VALUE(a.street), NULL, ANY_VALUE(a.postcode),
           ST_CENTROID(ST_COLLECT(a.geog)), ANY_VALUE(b.sim), ANY_VALUE(b.ed),
           'STREET', 0.8 * ANY_VALUE(b.sim)
    FROM best b
    JOIN area a
      ON a.country = b.country AND a.raw_address = b.raw_address AND a.street_key = b.street_key
    GROUP BY b.country, b.raw_address;

    -- 7. POSTCODE level: centroid of the postcode's Overture addresses.
    INSERT INTO GEOCODING.PUBLIC._FGC_EU_RES
    SELECT p.country, p.raw_address, NULL, NULL, NULL, ANY_VALUE(a.postcode),
           ST_CENTROID(ST_COLLECT(a.geog)), NULL, NULL, 'POSTCODE', 50
    FROM GEOCODING.PUBLIC._FGC_EU_PARSED p
    JOIN GEOCODING.PUBLIC.OVERTURE_ADDRESS_EU a
      ON a.country = p.country AND a.postcode_norm = p.postcode_norm
    WHERE NOT EXISTS (SELECT 1 FROM GEOCODING.PUBLIC._FGC_EU_RES r
                      WHERE r.country = p.country AND r.raw_address = p.raw_address)
    GROUP BY p.country, p.raw_address;

    -- 8. LOCALITY level: best-named matching locality polygon, preferring real
    --    settlements over groupings (comarcas), then the largest.
    INSERT INTO GEOCODING.PUBLIC._FGC_EU_RES
    SELECT l.country, l.raw_address, NULL, NULL, e.name, NULL, e.centroid,
           NULL, NULL, 'LOCALITY', 30
    FROM GEOCODING.PUBLIC._FGC_EU_LOC l
    JOIN GEOCODING.PUBLIC.EU_LOCALITY e ON e.locality_id = l.locality_id
    WHERE NOT EXISTS (SELECT 1 FROM GEOCODING.PUBLIC._FGC_EU_RES r
                      WHERE r.country = l.country AND r.raw_address = l.raw_address)
    QUALIFY ROW_NUMBER() OVER (PARTITION BY l.country, l.raw_address
                               ORDER BY l.sim DESC, IFF(e.place_class IS NULL, 1, 0),
                                        ST_AREA(e.geog) DESC, e.locality_id) = 1;
    END IF;

    -- -----------------------------------------------------------------------
    -- Assemble: every input row, NONE where nothing matched.
    -- -----------------------------------------------------------------------
    EXECUTE IMMEDIATE '
        CREATE OR REPLACE TABLE ' || OUTPUT_TABLE || ' AS
        SELECT i.input_id, i.raw_address,
               COALESCE(u.result_number, e.result_number) AS result_number,
               COALESCE(u.result_street, e.result_street) AS result_street,
               COALESCE(u.result_city,   e.result_city)   AS result_city,
               COALESCE(u.result_zip,    e.result_zip)    AS result_zip,
               IFF(u.input_id IS NOT NULL, u.result_geog, e.result_geog) AS result_geog,
               COALESCE(u.street_sim, e.street_sim)       AS street_sim,
               COALESCE(u.edit_dist,  e.edit_dist)        AS edit_dist,
               i.country                                  AS result_country,
               CASE WHEN u.input_id IS NOT NULL THEN ''ADDRESS''
                    ELSE COALESCE(e.match_level, ''NONE'') END AS match_level,
               CASE WHEN u.input_id IS NOT NULL THEN u.street_sim::FLOAT
                    ELSE COALESCE(e.match_score, 0) END       AS match_score
        FROM GEOCODING.PUBLIC._FGC_IN i
        LEFT JOIN GEOCODING.PUBLIC._FGC_US u
          ON i.country = ''US'' AND u.input_id = i.input_id
        LEFT JOIN GEOCODING.PUBLIC._FGC_EU_RES e
          ON e.country = i.country AND e.raw_address = i.raw_address';

    EXECUTE IMMEDIATE 'SELECT COUNT(*) FROM ' || OUTPUT_TABLE;
    n := (SELECT $1 FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())));
    EXECUTE IMMEDIATE 'SELECT COUNT_IF(match_level <> ''NONE'') FROM ' || OUTPUT_TABLE;
    n_hit := (SELECT $1 FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())));
    RETURN 'Forward geocode complete. ' || n_hit || ' of ' || n
        || ' addresses matched into ' || OUTPUT_TABLE || ' (see MATCH_LEVEL).';
END;
$$;
