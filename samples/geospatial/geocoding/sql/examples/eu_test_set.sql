-- ============================================================================
-- EU test set — real Overture addresses rendered back to text, with noise
-- ============================================================================
-- 60 addresses per country for DE, FR, IT, NL, ES, PL, sampled from
-- GEOCODING.PUBLIC.OVERTURE_ADDRESS_EU (needs the --eu install). Each row keeps
-- its source point as ground truth (ACTUAL_GEOG), so the forward result can
-- be scored with EVALUATE_GEOCODE.
--
-- Rendering follows each country's local order:
--   DE NL PL IT ES : <street> <number>, <postcode> <city>
--   FR             : <number> <street>, <postcode> <city>
-- The city is the largest settlement-class locality polygon the address falls
-- in (the municipality people write, not the sub-district or a comarca). IT has no postcodes in
-- Overture and ~79% of DE has none, so those rows are rendered without one.
--
-- Noise, by row (NOISE column):
--   clean     as rendered
--   abbrev    local street-type abbreviation (Str., r., av., V., C/, ul., str.)
--   no_pc     postcode removed
--   typo      one character deleted from the street name
--   lower     everything lower case
--   abbr_low  abbreviation + lower case
-- ============================================================================

CREATE OR REPLACE TABLE GEOCODING.PUBLIC.TEST_EU_INPUT COMMENT = '{"origin":"sf_sit-is-geocoding","name":"oss-geocoding","version":{"major":1,"minor":0},"attributes":{"is_quickstart":1,"source":"sql"}}' AS
WITH s AS (
    SELECT a.*
    -- Hash of the stable Overture ID instead of SAMPLE, so a rebuilt index
    -- yields the same test rows (SAMPLE depends on the physical layout).
    FROM GEOCODING.PUBLIC.OVERTURE_ADDRESS_EU a
    WHERE MOD(ABS(HASH(a.id, 42)), 10000) = 0
      AND a.country IN ('DE','FR','IT','NL','ES','PL')
      AND a.number IS NOT NULL AND a.street IS NOT NULL
      AND LENGTH(a.street) >= 5
    QUALIFY ROW_NUMBER() OVER (PARTITION BY a.country ORDER BY HASH(a.id)) <= 60
),
c AS (
    SELECT s.id, l.name AS city
    FROM s
    JOIN GEOCODING.PUBLIC.OVERTURE_ADDRESS_EU_LOC k ON k.country = s.country AND k.id = s.id
    JOIN GEOCODING.PUBLIC.EU_LOCALITY l ON l.locality_id = k.locality_id
    -- Prefer real settlements (Overture place class city/town/village/hamlet)
    -- over groupings such as Spanish comarcas, and skip German administrative
    -- unions (VVG / GVV / Amt / Verbandsgemeinde ...), which nobody writes as a city.
    QUALIFY ROW_NUMBER() OVER (PARTITION BY s.id ORDER BY
        IFF(l.place_class IS NULL, 1, 0),
        IFF(REGEXP_LIKE(l.name, '^(VVG|GVV|vVG|Verwaltungs|Gemeindeverwaltungs|Amt |Samtgemeinde|Verbandsgemeinde|Mittelbereich).*'), 1, 0),
        ST_AREA(l.geog) DESC) = 1
),
base AS (
    SELECT s.country, s.id AS overture_id, s.number, s.street, s.postcode,
           c.city, s.geog AS actual_geog,
           ROW_NUMBER() OVER (PARTITION BY s.country ORDER BY HASH(s.id)) AS rn
    FROM s LEFT JOIN c ON c.id = s.id
),
noised AS (
    SELECT b.*,
           DECODE(MOD(rn, 6), 0, 'clean', 1, 'abbrev', 2, 'no_pc', 3, 'typo', 4, 'lower', 'abbr_low') AS noise
    FROM base b
),
street_n AS (
    SELECT n.*,
        CASE
          WHEN noise IN ('abbrev', 'abbr_low') THEN
            CASE country
              WHEN 'DE' THEN REGEXP_REPLACE(street, 'stra(ss|ß)e', 'str.', 1, 0, 'i')
              WHEN 'NL' THEN REGEXP_REPLACE(street, 'straat$', 'str.', 1, 0, 'i')
              WHEN 'FR' THEN REGEXP_REPLACE(REGEXP_REPLACE(street, '^rue ', 'r. ', 1, 0, 'i'), '^avenue ', 'av. ', 1, 0, 'i')
              WHEN 'IT' THEN REGEXP_REPLACE(REGEXP_REPLACE(street, '^via ', 'V. ', 1, 0, 'i'), '^piazza ', 'P.zza ', 1, 0, 'i')
              WHEN 'ES' THEN REGEXP_REPLACE(REGEXP_REPLACE(street, '^calle ', 'C/ ', 1, 0, 'i'), '^avenida ', 'Avda. ', 1, 0, 'i')
              WHEN 'PL' THEN 'ul. ' || street
              ELSE street END
          WHEN noise = 'typo' THEN
            INSERT(street, GREATEST(3, FLOOR(LENGTH(street) / 2)), 1, '')
          ELSE street
        END AS street_txt,
        IFF(noise = 'no_pc', NULL, postcode) AS pc_txt
    FROM noised n
)
SELECT country || '-' || LPAD(rn::STRING, 3, '0')       AS test_id,
       country,
       noise,
       overture_id,
       actual_geog,
       IFF(noise IN ('lower', 'abbr_low'), LOWER(t), t) AS address
FROM (
    SELECT *,
           CASE country
             WHEN 'FR' THEN number || ' ' || street_txt
             ELSE street_txt || ' ' || number
           END
           || CASE WHEN pc_txt IS NOT NULL OR city IS NOT NULL THEN ', ' ELSE '' END
           || ARRAY_TO_STRING(ARRAY_CONSTRUCT_COMPACT(pc_txt, city), ' ') AS t
    FROM street_n
);

-- Quick look
SELECT country, noise, COUNT(*) AS n FROM GEOCODING.PUBLIC.TEST_EU_INPUT GROUP BY 1, 2 ORDER BY 1, 2;
SELECT country, ANY_VALUE(address) AS example FROM GEOCODING.PUBLIC.TEST_EU_INPUT GROUP BY 1;
