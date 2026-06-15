-- ============================================================================
-- IDR_CORE_IDENTIFIER_LINK: Connects source records to identifiers
-- This is the edge table in the identity graph model
-- ============================================================================

USE ROLE SYSADMIN;
USE DATABASE IDR_DEMO;
USE SCHEMA SILVER;

CREATE TABLE IF NOT EXISTS IDR_DEMO.SILVER.IDR_CORE_IDENTIFIER_LINK (
    link_id           VARCHAR DEFAULT UUID_STRING() PRIMARY KEY,
    source_record_id  VARCHAR NOT NULL,
    source_type       VARCHAR NOT NULL,
    identifier_id     VARCHAR NOT NULL,
    is_active         BOOLEAN DEFAULT TRUE,
    created_at        TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    
    UNIQUE (source_record_id, source_type, identifier_id)
);

COMMENT ON TABLE IDR_DEMO.SILVER.IDR_CORE_IDENTIFIER_LINK IS 
    'Links source records to deduplicated identifiers. Multiple source records can share the same identifier.';

COMMENT ON COLUMN IDR_DEMO.SILVER.IDR_CORE_IDENTIFIER_LINK.source_record_id IS 
    'Primary key from the source table (e.g., loyalty_id, booking_id)';

COMMENT ON COLUMN IDR_DEMO.SILVER.IDR_CORE_IDENTIFIER_LINK.source_type IS 
    'Source table name (e.g., CUSTOMER_BASE, BOOKING)';

COMMENT ON COLUMN IDR_DEMO.SILVER.IDR_CORE_IDENTIFIER_LINK.identifier_id IS 
    'Foreign key to IDR_CORE_ENTITY_IDENTIFIERS.identifier_id';
