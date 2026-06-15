-- ============================================================================
-- IDR_CORE_ENTITY_IDENTIFIERS: Deduplicated identifier values
-- Each unique (identifier_type, identifier_value_normalized) appears once
-- Source records link via IDR_CORE_IDENTIFIER_LINK table
-- ============================================================================

USE ROLE SYSADMIN;
USE DATABASE IDR_DEMO;
USE SCHEMA SILVER;

-- Create table (idempotent — uses IF NOT EXISTS on fresh)
CREATE TABLE IF NOT EXISTS IDR_DEMO.SILVER.IDR_CORE_ENTITY_IDENTIFIERS (
    identifier_id              VARCHAR DEFAULT UUID_STRING() PRIMARY KEY,
    identifier_type            VARCHAR NOT NULL,
    identifier_value           VARCHAR,
    identifier_value_normalized VARCHAR NOT NULL,
    is_active                  BOOLEAN DEFAULT TRUE,
    created_at                 TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    updated_at                 TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    
    UNIQUE (identifier_type, identifier_value_normalized)
);

COMMENT ON TABLE IDR_DEMO.SILVER.IDR_CORE_ENTITY_IDENTIFIERS IS 
    'Deduplicated identifier values. Each unique (type, normalized_value) pair appears once.';

COMMENT ON COLUMN IDR_DEMO.SILVER.IDR_CORE_ENTITY_IDENTIFIERS.identifier_type IS 
    'Semantic type: EMAIL, PHONE, LOYALTY_ID, NAME_FIRST, NAME_LAST, DOB, etc.';

COMMENT ON COLUMN IDR_DEMO.SILVER.IDR_CORE_ENTITY_IDENTIFIERS.identifier_value IS 
    'Original value as provided by source';

COMMENT ON COLUMN IDR_DEMO.SILVER.IDR_CORE_ENTITY_IDENTIFIERS.identifier_value_normalized IS 
    'Standardized value for matching (lowercase, trimmed, etc.)';
