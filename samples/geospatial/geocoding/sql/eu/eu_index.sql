-- ============================================================================
-- BUILD_EU_INDEX — materialized candidate tables for European forward geocoding
-- ============================================================================
-- European forward matching cannot run fuzzy string comparisons against the
-- 130M+ raw Overture rows. This procedure pre-computes, once, everything the
-- query path needs so each input row only ever compares against the few
-- hundred addresses that share its country + postcode, or country + locality:
--
--   EU_LOCALITY            one row per Overture locality polygon
--                          (OVERTURE_MAPS__DIVISIONS ... DIVISION_AREA, subtype
--                          'locality'), with a normalized name key and the
--                          division's place class (city / town / village /
--                          hamlet; NULL for groupings such as Spanish comarcas).
--   EU_LOCALITY_ALIAS      one row per locality name key, including each part
--                          of bilingual names ("Pabillois/Pabillonis").
--   OVERTURE_ADDRESS_EU    Overture addresses for the supported countries with
--                          normalized number / street / postcode keys.
--                          Clustered by (COUNTRY, POSTCODE_NORM).
--   OVERTURE_ADDRESS_EU_LOC  the same addresses keyed by the locality polygon(s)
--                          they fall in (ST_INTERSECTS, computed here once).
--                          Clustered by (COUNTRY, LOCALITY_ID). Used when the
--                          input or the Overture row has no usable postcode
--                          (all of IT, ~79% of DE).
--
-- Params:
--   REBUILD  FALSE (default): build only the tables that do not exist yet, so
--            re-running the installer is a no-op. TRUE: rebuild all three
--            (use after the Overture share refreshes).
--
-- Storage: see README.md ("European coverage") for measured sizes.
-- Needs GEOCODING.PUBLIC.NORMALIZE_STREET_EU / NORMALIZE_NAME_EU.
-- ============================================================================

CREATE OR REPLACE PROCEDURE GEOCODING.PUBLIC.BUILD_EU_INDEX(
    REBUILD BOOLEAN DEFAULT FALSE)
RETURNS VARCHAR
LANGUAGE SQL
COMMENT = 'oss-geocoding'
EXECUTE AS CALLER
AS
$$
DECLARE
    countries VARCHAR DEFAULT '''FR'',''ES'',''PL'',''BE'',''DK'',''CH'',''AT'',''NL'',''NO'',''FI'',''PT'',''DE'',''IT'',''CZ''';
    built     VARCHAR DEFAULT '';
    has_loc   INTEGER;
    has_addr  INTEGER;
    has_link  INTEGER;
    has_cls   INTEGER;
    has_alias INTEGER;
BEGIN
    -- Empty tables count as missing, so a build that read an empty source
    -- (for example during a Marketplace share refresh) is redone on re-run.
    SELECT COUNT_IF(TABLE_NAME = 'EU_LOCALITY' AND ROW_COUNT > 0),
           COUNT_IF(TABLE_NAME = 'OVERTURE_ADDRESS_EU' AND ROW_COUNT > 0),
           COUNT_IF(TABLE_NAME = 'OVERTURE_ADDRESS_EU_LOC' AND ROW_COUNT > 0)
      INTO :has_loc, :has_addr, :has_link
      FROM GEOCODING.INFORMATION_SCHEMA.TABLES
     WHERE TABLE_SCHEMA = 'PUBLIC';

    -- ---------------------------------------------------------------------
    -- 1. Locality polygons
    -- ---------------------------------------------------------------------
    IF (REBUILD OR has_loc = 0) THEN
        EXECUTE IMMEDIATE '
            CREATE OR REPLACE TABLE GEOCODING.PUBLIC.EU_LOCALITY
              COMMENT = ''oss-geocoding''
            AS
            SELECT a.DIVISION_ID::VARCHAR                              AS locality_id,
                   a.COUNTRY::VARCHAR                                  AS country,
                   a.REGION::VARCHAR                                   AS region,
                   a.NAMES:primary::VARCHAR                            AS name,
                   GEOCODING.PUBLIC.NORMALIZE_NAME_EU(a.NAMES:primary::VARCHAR) AS name_key,
                   a.GEOMETRY                                          AS geog,
                   ST_CENTROID(a.GEOMETRY)                             AS centroid,
                   NULLIF(d.CLASS::VARCHAR, '''')                      AS place_class
            FROM OVERTURE_MAPS__DIVISIONS.CARTO.DIVISION_AREA a
            LEFT JOIN OVERTURE_MAPS__DIVISIONS.CARTO.DIVISION d ON d.ID = a.DIVISION_ID
            WHERE a.SUBTYPE = ''locality''
              AND a.COUNTRY IN (' || countries || ')
            UNION ALL
            -- Many large cities (Berlin, Hamburg, Wien, Praha, most German
            -- kreisfreie Staedte) are locality divisions without a locality
            -- polygon; their area is stored as the same-named localadmin,
            -- county or region. Use the smallest such area that contains the
            -- locality point, skipping state-sized ones.
            SELECT d.ID::VARCHAR, d.COUNTRY::VARCHAR, d.REGION::VARCHAR,
                   d.NAMES:primary::VARCHAR,
                   GEOCODING.PUBLIC.NORMALIZE_NAME_EU(d.NAMES:primary::VARCHAR),
                   a.GEOMETRY, ST_CENTROID(a.GEOMETRY), NULLIF(d.CLASS::VARCHAR, '''')
            FROM OVERTURE_MAPS__DIVISIONS.CARTO.DIVISION d
            JOIN OVERTURE_MAPS__DIVISIONS.CARTO.DIVISION_AREA a
              ON a.COUNTRY = d.COUNTRY
             AND a.NAMES:primary::VARCHAR = d.NAMES:primary::VARCHAR
             AND a.SUBTYPE IN (''localadmin'', ''county'', ''region'')
             AND ST_CONTAINS(a.GEOMETRY, d.GEOMETRY)
            WHERE d.SUBTYPE = ''locality''
              AND d.COUNTRY IN (' || countries || ')
              AND ST_AREA(a.GEOMETRY) <= 1.5e9
              AND NOT EXISTS (SELECT 1 FROM OVERTURE_MAPS__DIVISIONS.CARTO.DIVISION_AREA x
                              WHERE x.DIVISION_ID = d.ID)
            QUALIFY ROW_NUMBER() OVER (PARTITION BY d.ID ORDER BY ST_AREA(a.GEOMETRY)) = 1';
        built := built || 'EU_LOCALITY ';
    END IF;

    -- Older installs built EU_LOCALITY without place_class. Add and backfill it
    -- in place so the large address -> locality link table is not rebuilt.
    SELECT COUNT(*) INTO :has_cls
      FROM GEOCODING.INFORMATION_SCHEMA.COLUMNS
     WHERE TABLE_SCHEMA = 'PUBLIC' AND TABLE_NAME = 'EU_LOCALITY'
       AND COLUMN_NAME = 'PLACE_CLASS';
    IF (has_cls = 0) THEN
        ALTER TABLE GEOCODING.PUBLIC.EU_LOCALITY ADD COLUMN place_class VARCHAR;
        UPDATE GEOCODING.PUBLIC.EU_LOCALITY l
           SET place_class = NULLIF(d.CLASS::VARCHAR, '')
          FROM OVERTURE_MAPS__DIVISIONS.CARTO.DIVISION d
         WHERE d.ID = l.locality_id;
        built := built || 'EU_LOCALITY.place_class ';
    END IF;

    -- ---------------------------------------------------------------------
    -- 1b. Locality name aliases. Bilingual names are stored joined
    --     ("Pabillois/Pabillonis", "Radibor - Radwor"); people write one part,
    --     so each part plus the full name gets its own key.
    -- ---------------------------------------------------------------------
    SELECT COUNT(*) INTO :has_alias
      FROM GEOCODING.INFORMATION_SCHEMA.TABLES
     WHERE TABLE_SCHEMA = 'PUBLIC' AND TABLE_NAME = 'EU_LOCALITY_ALIAS' AND ROW_COUNT > 0;
    IF (REBUILD OR has_alias = 0 OR CONTAINS(built, 'EU_LOCALITY ')) THEN
        CREATE OR REPLACE TABLE GEOCODING.PUBLIC.EU_LOCALITY_ALIAS
          CLUSTER BY (country, alias_key)
          COMMENT = 'oss-geocoding'
        AS
        SELECT DISTINCT locality_id, country, alias_key
        FROM (
            SELECT locality_id, country, name_key AS alias_key
            FROM GEOCODING.PUBLIC.EU_LOCALITY
            UNION ALL
            SELECT l.locality_id, l.country,
                   GEOCODING.PUBLIC.NORMALIZE_NAME_EU(TRIM(s.value::STRING))
            FROM GEOCODING.PUBLIC.EU_LOCALITY l,
                 LATERAL FLATTEN(input => SPLIT(REGEXP_REPLACE(l.name, '\\s+-\\s+|/', '|'), '|')) s
            WHERE REGEXP_LIKE(l.name, '.*(/|\\s-\\s).*')
        )
        WHERE alias_key IS NOT NULL AND alias_key <> '';
        built := built || 'EU_LOCALITY_ALIAS ';
    END IF;

    -- ---------------------------------------------------------------------
    -- 2. Normalized address table. The street UDF runs once per DISTINCT
    --    (country, street), not once per address.
    -- ---------------------------------------------------------------------
    IF (REBUILD OR has_addr = 0) THEN
        EXECUTE IMMEDIATE '
            CREATE OR REPLACE TABLE GEOCODING.PUBLIC.OVERTURE_ADDRESS_EU
              CLUSTER BY (country, postcode_norm)
              COMMENT = ''oss-geocoding''
            AS
            WITH a AS (
                SELECT * FROM GEOCODING.PUBLIC.OVERTURE_ADDRESS
                WHERE COUNTRY IN (' || countries || ')
            ),
            streets AS (
                SELECT COUNTRY, STREET,
                       GEOCODING.PUBLIC.NORMALIZE_STREET_EU(STREET, COUNTRY) AS street_key
                FROM (SELECT DISTINCT COUNTRY, STREET FROM a WHERE STREET IS NOT NULL)
            )
            SELECT a.ID                                                          AS id,
                   a.COUNTRY                                                     AS country,
                   a.NUMBER                                                      AS number,
                   a.UNIT                                                        AS unit,
                   REGEXP_SUBSTR(a.NUMBER, ''^[0-9]+'')                          AS num_base,
                   LOWER(REGEXP_REPLACE(COALESCE(a.NUMBER, '''') || COALESCE(a.UNIT, ''''),
                                        ''[^0-9A-Za-z]'', ''''))                 AS num_full,
                   a.STREET                                                      AS street,
                   s.street_key                                                  AS street_key,
                   a.POSTCODE                                                    AS postcode,
                   NULLIF(UPPER(REGEXP_REPLACE(a.POSTCODE, ''[^0-9A-Za-z]'', '''')), '''') AS postcode_norm,
                   a.POSTAL_CITY                                                 AS postal_city,
                   a.GEOMETRY                                                    AS geog
            FROM a
            LEFT JOIN streets s ON s.COUNTRY = a.COUNTRY AND s.STREET = a.STREET
            ORDER BY country, postcode_norm';
        built := built || 'OVERTURE_ADDRESS_EU ';
    END IF;

    -- ---------------------------------------------------------------------
    -- 3. Address -> locality link (one spatial join, done here, never at
    --    query time). Carries the match columns so the query path never has
    --    to join back to OVERTURE_ADDRESS_EU.
    -- ---------------------------------------------------------------------
    IF (REBUILD OR has_link = 0 OR has_addr = 0 OR has_loc = 0) THEN
        EXECUTE IMMEDIATE '
            CREATE OR REPLACE TABLE GEOCODING.PUBLIC.OVERTURE_ADDRESS_EU_LOC
              CLUSTER BY (country, locality_id)
              COMMENT = ''oss-geocoding''
            AS
            SELECT l.locality_id, a.id, a.country, a.number, a.num_base, a.num_full,
                   a.street, a.street_key, a.postcode, a.geog
            FROM GEOCODING.PUBLIC.OVERTURE_ADDRESS_EU a
            JOIN GEOCODING.PUBLIC.EU_LOCALITY l
              ON l.country = a.country
             AND ST_INTERSECTS(l.geog, a.geog)
            ORDER BY a.country, l.locality_id';
        built := built || 'OVERTURE_ADDRESS_EU_LOC ';
    END IF;

    -- Never report success over an empty index.
    SELECT COUNT_IF(ROW_COUNT > 0) INTO :has_loc
      FROM GEOCODING.INFORMATION_SCHEMA.TABLES
     WHERE TABLE_SCHEMA = 'PUBLIC'
       AND TABLE_NAME IN ('EU_LOCALITY', 'EU_LOCALITY_ALIAS',
                          'OVERTURE_ADDRESS_EU', 'OVERTURE_ADDRESS_EU_LOC');
    IF (has_loc < 4) THEN
        RETURN 'ERROR: EU index has empty tables. The Overture shares returned no '
            || 'European rows (a share refresh may be in progress). Re-run in a few '
            || 'minutes; empty tables are rebuilt.';
    END IF;

    RETURN IFF(built = '', 'EU index already present (pass TRUE to rebuild).',
               'EU index built: ' || built);
END;
$$;
