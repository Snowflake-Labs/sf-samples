-- ============================================================================
-- Martech: SILVER.IDR_CORE_CLUSTER_LOG
-- Audit log for cluster lifecycle decisions (create / merge / split).
-- Written by engine SP_UPDATE_CLUSTERS.
-- ============================================================================

CREATE TABLE IF NOT EXISTS IDENTIFIER('&{deployment_db}.SILVER.IDR_CORE_CLUSTER_LOG') (
    LOG_ID                      VARCHAR DEFAULT UUID_STRING(),
    CLUSTER_ID                  VARCHAR,
    ACTION                      VARCHAR,
    EVENT_TYPE                  VARCHAR,
    DETAILS                     VARIANT,
    EVENT_DETAILS               VARIANT,
    MATCH_DETAILS               VARIANT,
    SOURCE_RECORD_IDS           VARIANT,
    PREVIOUS_SOURCE_RECORD_IDS  VARIANT,
    NEW_SOURCE_RECORD_IDS       VARIANT,
    MERGED_FROM                 VARIANT,
    MERGED_FROM_CLUSTERS        VARIANT,
    OLD_CLUSTER_IDS             VARIANT,
    NEW_CLUSTER_ID              VARCHAR,
    NEW_MATCH_ID                VARCHAR,
    MEMBERS_MERGED              INT,
    MATCH_ID                    VARCHAR,
    TOTAL_RECORDS               INT,
    PROCESSING_TIME_MS          INT,
    CREATED_AT                  TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    RUN_ID                      VARCHAR,
    PIPELINE_RUN_ID             VARCHAR
);
