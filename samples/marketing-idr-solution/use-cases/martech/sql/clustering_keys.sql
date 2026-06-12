-- ============================================================================
-- Martech: Clustering keys for query performance optimization
-- Applied to the Silver-layer tables whose key columns are actually used as
-- equality filters / join keys by the UI/API (verified against backend queries).
-- These guide Snowflake's auto-clustering to co-locate related rows.
-- Must run AFTER all idr_core / silver tables exist.
--
-- Scoped intentionally:
--   * IDR_CORE_CLUSTER         -> key already declared in its CREATE TABLE DDL.
--   * IDR_CORE_MATCH_RESULTS   -> queries wrap key cols in LEAST()/GREATEST()/OR,
--                                 so clustering can't prune; omitted.
--   * IDR_CORE_MATCH_LOG       -> never read by the app (write-only); omitted.
-- ============================================================================

USE ROLE ACCOUNTADMIN;
USE DATABASE IDENTIFIER('&{deployment_db}');
USE SCHEMA SILVER;

-- Cluster membership: WHERE CLUSTER_ID + JOIN on SOURCE_RECORD_ID (hot path,
-- replaces LATERAL FLATTEN scans across all identity-detail queries).
ALTER TABLE IDENTIFIER('&{deployment_db}.SILVER.IDR_CORE_CLUSTER_MEMBERSHIP') CLUSTER BY (CLUSTER_ID, SOURCE_RECORD_ID);

-- Cluster log: resolution-history queries filter by CLUSTER_ID.
ALTER TABLE IDENTIFIER('&{deployment_db}.SILVER.IDR_CORE_CLUSTER_LOG') CLUSTER BY (CLUSTER_ID);

