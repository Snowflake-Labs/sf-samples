-- ============================================================================
-- IDR_CORE_CLUSTER_MEMBERSHIP: Denormalized source_record_id -> cluster_id lookup
-- Enables incremental clustering by replacing LATERAL FLATTEN over all clusters
-- with indexed lookups.
-- ============================================================================

USE ROLE SYSADMIN;
USE DATABASE IDR_DEMO;
USE SCHEMA SILVER;

CREATE TABLE IF NOT EXISTS IDR_DEMO.SILVER.IDR_CORE_CLUSTER_MEMBERSHIP (
    source_record_id  VARCHAR NOT NULL PRIMARY KEY,
    cluster_id        VARCHAR NOT NULL,
    updated_at        TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

COMMENT ON TABLE IDR_DEMO.SILVER.IDR_CORE_CLUSTER_MEMBERSHIP IS
    'Denormalized source_record_id -> cluster_id lookup, maintained by SP_UPDATE_CLUSTERS. Enables incremental clustering.';

ALTER TABLE IDR_DEMO.SILVER.IDR_CORE_CLUSTER_MEMBERSHIP CLUSTER BY (CLUSTER_ID, SOURCE_RECORD_ID);
