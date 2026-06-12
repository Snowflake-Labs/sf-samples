USE DATABASE IDR_DEMO;
USE SCHEMA SILVER;

-- =====================================================
-- IDR TAG DEFINITIONS
-- These tags enable dynamic discovery of tables and columns
-- for the Identity Resolution pipeline.
--
-- BEST PRACTICE:
-- - Boolean flags (IDR_INCLUDE): Use ALLOWED_VALUES to prevent typos
-- - Semantic types (IDR_IDENTIFIER): NO ALLOWED_VALUES for extensibility
--   across industries (travel, healthcare, retail, etc.)
-- =====================================================

-- =====================================================
-- TABLE-LEVEL TAGS
-- =====================================================

CREATE TAG IF NOT EXISTS IDR_INCLUDE 
    ALLOWED_VALUES 'TRUE', 'FALSE'
    COMMENT = 'Whether table participates in IDR pipeline';

CREATE TAG IF NOT EXISTS IDR_SOURCE_ROLE 
    COMMENT = 'BASE=identity anchor (customer), USE_CASE=vertical data (booking, order, visit)';

CREATE TAG IF NOT EXISTS IDR_PRIORITY 
    COMMENT = 'Integer priority for golden record (1=highest). Lower number = higher priority';

CREATE TAG IF NOT EXISTS IDR_PRIMARY_KEY 
    COMMENT = 'Column name used as source_record_id in IDR_CORE_IDENTIFIER_LINK';

-- =====================================================
-- COLUMN-LEVEL TAGS
-- =====================================================

-- Unified tag for all identifier/attribute types
CREATE TAG IF NOT EXISTS IDR_IDENTIFIER 
    COMMENT = 'Semantic type for column. Examples: EMAIL, PHONE, LOYALTY_ID, PNR, NAME_FIRST, NAME_LAST, DOB, GENDER';

CREATE TAG IF NOT EXISTS IDR_EXTERNAL_KEY 
    COMMENT = 'Target table name for FK relationship to BASE entity';

-- =====================================================
-- DROP OLD TAGS (replaced by IDR_IDENTIFIER)
-- Note: Must unset from all objects before dropping
-- =====================================================
-- Run after migrating tag values:
-- DROP TAG IF EXISTS IDR_IDENTIFIER_TYPE;
-- DROP TAG IF EXISTS IDR_ATTRIBUTE;
