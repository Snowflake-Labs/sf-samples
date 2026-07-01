-- ============================================================================
-- Geocoding service — one-time setup
-- ============================================================================
-- Snowflake-native geocoding built on the Overture Maps Marketplace share.
-- No external geocoding APIs are used.
--
-- Prerequisites
--   1. The Overture Maps "Addresses" Marketplace listing (Carto). This script
--      AUTO-ACQUIRES it from the Marketplace (free, auto-fulfilled) into the
--      database OVERTURE_MAPS__ADDRESSES. If your account already has it under
--      either OVERTURE_MAPS__ADDRESSES or DS_OVERTURE_MAPS__ADDRESSES, that copy
--      is reused. Either way a stable view GEOCODING.PUBLIC.OVERTURE_ADDRESS is
--      created over whichever share resolves, so the UDFs/procedures never
--      hard-code the share name.
--      Table columns: ID, STREET, NUMBER, UNIT, POSTCODE, POSTAL_CITY,
--                     COUNTRY, GEOMETRY (GEOGRAPHY)
--   2. A warehouse with enough compute (MEDIUM recommended for batch jobs).
--
-- External access integrations: NONE. All geocoding runs on Snowflake data +
-- the shared PyPI repo (usaddress). The optional libpostal SPCS service bakes
-- its models into the image at build time and makes no outbound calls.
--
-- Run order:
--   00_setup.sql                       (this file)
--   udfs/parse_address.sql
--   udfs/standardize_street.sql
--   procedures/forward_geocode_table.sql
--   procedures/reverse_geocode_table.sql
--   examples/*                         (optional smoke tests)
-- ============================================================================

CREATE DATABASE IF NOT EXISTS GEOCODING
  COMMENT = 'oss-geocoding';
CREATE SCHEMA   IF NOT EXISTS GEOCODING.PUBLIC
  COMMENT = 'oss-geocoding';

USE DATABASE GEOCODING;
USE SCHEMA   PUBLIC;

-- ---------------------------------------------------------------------------
-- Acquire the Overture Maps "Addresses" listing (best-effort, idempotent)
-- ---------------------------------------------------------------------------
-- Free CARTO/Overture listing, auto-fulfilled in most regions. No-op if the
-- database already exists. Wrapped so a region where the listing is not
-- auto-fulfillable does not abort setup — the detection block below then
-- reports clear guidance if no share is reachable.
EXECUTE IMMEDIATE $$
BEGIN
    CALL SYSTEM$ACCEPT_LEGAL_TERMS('DATA_EXCHANGE_LISTING', 'GZT0Z4CM1E9NQ');
    EXECUTE IMMEDIATE
        'CREATE DATABASE IF NOT EXISTS OVERTURE_MAPS__ADDRESSES FROM LISTING GZT0Z4CM1E9NQ';
    RETURN 'Overture Addresses listing acquired (or already present).';
EXCEPTION
    WHEN OTHER THEN
        RETURN 'Could not auto-acquire the Overture Addresses listing '
            || '(' || SQLERRM || '). If a copy is already installed it will '
            || 'still be used; otherwise acquire "Overture Maps - Addresses" '
            || '(Carto) from the Marketplace and re-run.';
END;
$$;

-- ---------------------------------------------------------------------------
-- Overture share indirection
-- ---------------------------------------------------------------------------
-- Detect whichever Overture Addresses share is installed and expose it through
-- a single stable view. Every UDF/procedure reads GEOCODING.PUBLIC.OVERTURE_ADDRESS,
-- so no downstream file has to know the physical share name.
EXECUTE IMMEDIATE $$
DECLARE
    candidates ARRAY DEFAULT ARRAY_CONSTRUCT(
        'OVERTURE_MAPS__ADDRESSES.CARTO.ADDRESS',
        'DS_OVERTURE_MAPS__ADDRESSES.CARTO.ADDRESS');
    resolved  STRING DEFAULT NULL;
    candidate STRING;
    i INTEGER DEFAULT 0;
BEGIN
    FOR i IN 0 TO ARRAY_SIZE(candidates) - 1 DO
        candidate := GET(candidates, :i)::STRING;
        BEGIN
            EXECUTE IMMEDIATE
                'CREATE OR REPLACE VIEW GEOCODING.PUBLIC.OVERTURE_ADDRESS
                   COMMENT = ''oss-geocoding''
                 AS SELECT * FROM ' || :candidate;
            resolved := :candidate;
            BREAK;
        EXCEPTION
            WHEN OTHER THEN
                CONTINUE;
        END;
    END FOR;

    IF (resolved IS NULL) THEN
        RETURN 'ERROR: Overture Maps Addresses share not found. Acquire the '
            || '"Overture Maps - Addresses" (Carto) listing from the Snowflake '
            || 'Marketplace, then re-run 00_setup.sql. Tried: '
            || ARRAY_TO_STRING(candidates, ', ');
    END IF;

    RETURN 'OVERTURE_ADDRESS view created over ' || resolved;
END;
$$;

-- Sanity check: confirm the resolved share is reachable through the view.
SELECT COUNT(*) AS overture_us_addresses
FROM GEOCODING.PUBLIC.OVERTURE_ADDRESS
WHERE COUNTRY = 'US'
LIMIT 1;
