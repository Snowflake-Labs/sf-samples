-- ============================================================================
-- Martech: GOLD.DT_CUSTOMER_PROFILE
-- Golden customer record dynamic table aggregating per CLUSTER_ID across
-- all 4 sources. Refreshes on TARGET_LAG = 5 minutes.
--
-- Design notes:
--   1. UNION ALL across the 4 STD_* tables to avoid the cross-product
--      fan-out that LEFT JOIN of all 4 tables produces.
--   2. Each unioned row carries a SOURCE_PRIORITY rank (1..4) used to pick
--      the "best" non-NULL value per field via COALESCE-of-priority-MAX.
--      Loyalty wins for email/phone/name/address; POS wins for spend; Web
--      contributes device IDs; Shopify falls in between for online PII.
--   3. CONFIDENCE_SCORE is the AVG(MATCH_SCORE) of in-cluster edges in
--      IDR_CORE_MATCH_RESULTS — real rule-derived signal, not a synthetic
--      bucket. Singletons fall back to 0.5 (no edges = no evidence).
--   4. Per-source spend stays partitioned (POS / Shopify) so totals roll
--      up cleanly.
-- ============================================================================

CREATE OR REPLACE DYNAMIC TABLE IDENTIFIER('&{deployment_db}.GOLD.DT_CUSTOMER_PROFILE')
    TARGET_LAG = '5 minutes'
    WAREHOUSE = MARTECH_WH
AS
WITH cluster_members AS (
    SELECT m.CLUSTER_ID, m.SOURCE_RECORD_ID
    FROM IDENTIFIER('&{deployment_db}.SILVER.IDR_CORE_CLUSTER_MEMBERSHIP') m
),
member_rows AS (
    -- LOYALTY (priority 1: canonical customer dimension)
    SELECT
        cm.CLUSTER_ID,
        1                          AS PRIORITY_RANK,
        'LOYALTY'                  AS SOURCE_TAG,
        l.EMAIL_STD                AS EMAIL,
        l.EMAIL_HEM                AS EMAIL_HEM,
        l.PHONE_E164               AS PHONE,
        l.FIRST_NAME_STD           AS FIRST_NAME,
        l.LAST_NAME_STD            AS LAST_NAME,
        l.FIRST_NAME_CANONICAL     AS FIRST_NAME_CANONICAL,
        l.MEMBER_ID_STD            AS LOYALTY_MEMBER_ID,
        l.TIER                     AS LOYALTY_TIER,
        l.POINTS                   AS LOYALTY_POINTS,
        l.STREET_ADDRESS_STD       AS BILLING_STREET,
        l.CITY_STD                 AS BILLING_CITY,
        l.STATE_STD                AS BILLING_STATE,
        l.POSTAL_CODE_STD          AS BILLING_POSTAL,
        NULL                       AS SHIPPING_STREET,
        NULL                       AS SHIPPING_CITY,
        NULL                       AS SHIPPING_STATE,
        NULL                       AS SHIPPING_POSTAL,
        NULL                       AS DEVICE_ID,
        NULL                       AS UID2,
        NULL                       AS RAMPID,
        NULL                       AS POS_AMOUNT,
        NULL                       AS SHOPIFY_AMOUNT,
        l.ENROLLED_AT              AS SOURCE_TS
    FROM cluster_members cm
    JOIN IDENTIFIER('&{deployment_db}.SILVER.STD_LOYALTY_MEMBER_RAW') l
      ON l.MEMBER_ID = cm.SOURCE_RECORD_ID

    UNION ALL

    -- SHOPIFY (priority 2: online order with email + billing address)
    SELECT
        cm.CLUSTER_ID,
        2,
        'SHOPIFY',
        s.EMAIL_STD,
        s.EMAIL_HEM,
        s.PHONE_E164,
        s.FIRST_NAME_STD,
        s.LAST_NAME_STD,
        s.FIRST_NAME_CANONICAL,
        s.LOYALTY_MEMBER_NUMBER_STD,
        NULL, NULL,
        s.BILLING_STREET_ADDRESS,
        s.BILLING_CITY,
        s.BILLING_STATE,
        s.BILLING_POSTAL_CODE,
        s.SHIPPING_STREET_ADDRESS,
        s.SHIPPING_CITY,
        s.SHIPPING_STATE,
        s.SHIPPING_POSTAL_CODE,
        NULL, NULL, NULL,
        NULL,
        s.TOTAL_PRICE,
        s.CREATED_AT_SRC
    FROM cluster_members cm
    JOIN IDENTIFIER('&{deployment_db}.SILVER.STD_SHOPIFY_ORDER_RAW') s
      ON s.ORDER_ID = cm.SOURCE_RECORD_ID

    UNION ALL

    -- POS (priority 3: in-store transaction; loyalty-confirmed when scanned)
    SELECT
        cm.CLUSTER_ID,
        3,
        'POS',
        p.EMAIL_STD,
        p.EMAIL_HEM,
        p.PHONE_E164,
        p.FIRST_NAME_STD,
        p.LAST_NAME_STD,
        p.FIRST_NAME_CANONICAL,
        p.LOYALTY_MEMBER_NUMBER_STD,
        NULL, NULL,
        NULL, NULL, NULL, NULL,
        NULL, NULL, NULL, NULL,
        NULL, NULL, NULL,
        p.TOTAL,
        NULL,
        p.TS
    FROM cluster_members cm
    JOIN IDENTIFIER('&{deployment_db}.SILVER.STD_POS_TRANSACTION_RAW') p
      ON p.TXN_ID = cm.SOURCE_RECORD_ID

    UNION ALL

    -- WEB (priority 4: behavioral, mostly anonymous)
    SELECT
        cm.CLUSTER_ID,
        4,
        'WEB',
        w.EMAIL_STD,
        w.EMAIL_HEM,
        w.PHONE_E164,
        NULL, NULL, NULL,
        w.LOGGED_IN_MEMBER_ID_STD,
        NULL, NULL,
        NULL, NULL, NULL, NULL,
        NULL, NULL, NULL, NULL,
        w.DEVICE_ID_STD,
        w.UID2_STD,
        w.RAMPID_STD,
        NULL,
        NULL,
        w.TS
    FROM cluster_members cm
    JOIN IDENTIFIER('&{deployment_db}.SILVER.STD_WEB_CLICKSTREAM_RAW') w
      ON w.EVENT_ID = cm.SOURCE_RECORD_ID
),
in_cluster_match_scores AS (
    -- Average MATCH_SCORE over edges where BOTH endpoints belong to the
    -- same cluster. Cross-cluster edges (one endpoint outside) are not
    -- evidence for this identity. Singletons get NULL → defaulted later.
    SELECT cm_a.CLUSTER_ID,
           AVG(m.MATCH_SCORE) AS AVG_MATCH_SCORE,
           COUNT(*)           AS IN_CLUSTER_EDGE_COUNT
    FROM IDENTIFIER('&{deployment_db}.SILVER.IDR_CORE_MATCH_RESULTS') m
    JOIN cluster_members cm_a ON cm_a.SOURCE_RECORD_ID = m.NEW_SOURCE_RECORD_ID
    JOIN cluster_members cm_b ON cm_b.SOURCE_RECORD_ID = m.MATCHED_SOURCE_RECORD_ID
                              AND cm_b.CLUSTER_ID     = cm_a.CLUSTER_ID
    WHERE m.IS_CURRENT = TRUE
      AND m.NEW_SOURCE_RECORD_ID != m.MATCHED_SOURCE_RECORD_ID
    GROUP BY cm_a.CLUSTER_ID
),
agg AS (
    SELECT
        CLUSTER_ID,
        -- Priority-ranked picks: take the value from the highest-priority
        -- source (lowest PRIORITY_RANK) that has a non-NULL value.
        COALESCE(MAX(CASE WHEN PRIORITY_RANK = 1 THEN EMAIL                END),
                 MAX(CASE WHEN PRIORITY_RANK = 2 THEN EMAIL                END),
                 MAX(CASE WHEN PRIORITY_RANK = 3 THEN EMAIL                END),
                 MAX(CASE WHEN PRIORITY_RANK = 4 THEN EMAIL                END))  AS PRIMARY_EMAIL,
        COALESCE(MAX(CASE WHEN PRIORITY_RANK = 1 THEN EMAIL_HEM            END),
                 MAX(CASE WHEN PRIORITY_RANK = 2 THEN EMAIL_HEM            END),
                 MAX(CASE WHEN PRIORITY_RANK = 3 THEN EMAIL_HEM            END),
                 MAX(CASE WHEN PRIORITY_RANK = 4 THEN EMAIL_HEM            END))  AS PRIMARY_EMAIL_HEM,
        COALESCE(MAX(CASE WHEN PRIORITY_RANK = 1 THEN PHONE                END),
                 MAX(CASE WHEN PRIORITY_RANK = 2 THEN PHONE                END),
                 MAX(CASE WHEN PRIORITY_RANK = 3 THEN PHONE                END),
                 MAX(CASE WHEN PRIORITY_RANK = 4 THEN PHONE                END))  AS PRIMARY_PHONE,
        COALESCE(MAX(CASE WHEN PRIORITY_RANK = 1 THEN FIRST_NAME           END),
                 MAX(CASE WHEN PRIORITY_RANK = 2 THEN FIRST_NAME           END),
                 MAX(CASE WHEN PRIORITY_RANK = 3 THEN FIRST_NAME           END))  AS PRIMARY_FIRST_NAME,
        COALESCE(MAX(CASE WHEN PRIORITY_RANK = 1 THEN LAST_NAME            END),
                 MAX(CASE WHEN PRIORITY_RANK = 2 THEN LAST_NAME            END),
                 MAX(CASE WHEN PRIORITY_RANK = 3 THEN LAST_NAME            END))  AS PRIMARY_LAST_NAME,
        COALESCE(MAX(CASE WHEN PRIORITY_RANK = 1 THEN FIRST_NAME_CANONICAL END),
                 MAX(CASE WHEN PRIORITY_RANK = 2 THEN FIRST_NAME_CANONICAL END),
                 MAX(CASE WHEN PRIORITY_RANK = 3 THEN FIRST_NAME_CANONICAL END))  AS FIRST_NAME_CANONICAL,
        -- Loyalty fields only meaningful from LOYALTY rows (PRIORITY_RANK=1).
        MAX(CASE WHEN PRIORITY_RANK = 1 THEN LOYALTY_MEMBER_ID END)               AS LOYALTY_MEMBER_ID,
        MAX(CASE WHEN PRIORITY_RANK = 1 THEN LOYALTY_TIER     END)                AS LOYALTY_TIER,
        MAX(CASE WHEN PRIORITY_RANK = 1 THEN LOYALTY_POINTS   END)                AS LOYALTY_POINTS,
        -- Address: loyalty wins, shopify is fallback.
        COALESCE(MAX(CASE WHEN PRIORITY_RANK = 1 THEN BILLING_STREET END),
                 MAX(CASE WHEN PRIORITY_RANK = 2 THEN BILLING_STREET END))        AS BILLING_STREET,
        COALESCE(MAX(CASE WHEN PRIORITY_RANK = 1 THEN BILLING_CITY   END),
                 MAX(CASE WHEN PRIORITY_RANK = 2 THEN BILLING_CITY   END))        AS BILLING_CITY,
        COALESCE(MAX(CASE WHEN PRIORITY_RANK = 1 THEN BILLING_STATE  END),
                 MAX(CASE WHEN PRIORITY_RANK = 2 THEN BILLING_STATE  END))        AS BILLING_STATE,
        COALESCE(MAX(CASE WHEN PRIORITY_RANK = 1 THEN BILLING_POSTAL END),
                 MAX(CASE WHEN PRIORITY_RANK = 2 THEN BILLING_POSTAL END))        AS BILLING_POSTAL,
        -- Shipping is shopify-only.
        MAX(SHIPPING_STREET)            AS SHIPPING_STREET,
        MAX(SHIPPING_CITY)              AS SHIPPING_CITY,
        MAX(SHIPPING_STATE)             AS SHIPPING_STATE,
        MAX(SHIPPING_POSTAL)            AS SHIPPING_POSTAL,
        -- Multi-valued device / addressability arrays (web-sourced).
        ARRAY_DISTINCT(ARRAY_AGG(DEVICE_ID) WITHIN GROUP (ORDER BY DEVICE_ID))    AS DEVICE_IDS,
        ARRAY_DISTINCT(ARRAY_AGG(UID2)      WITHIN GROUP (ORDER BY UID2))         AS UID2_IDS,
        ARRAY_DISTINCT(ARRAY_AGG(RAMPID)    WITHIN GROUP (ORDER BY RAMPID))       AS RAMPID_IDS,
        -- Per-source spend totals (no fan-out under UNION ALL).
        COALESCE(SUM(POS_AMOUNT),     0) AS LIFETIME_POS_SPEND,
        COALESCE(SUM(SHOPIFY_AMOUNT), 0) AS LIFETIME_SHOPIFY_SPEND,
        COALESCE(SUM(POS_AMOUNT),     0) +
        COALESCE(SUM(SHOPIFY_AMOUNT), 0) AS LIFETIME_TOTAL_SPEND,
        MIN(SOURCE_TS) AS FIRST_SEEN_TS,
        MAX(SOURCE_TS) AS LAST_SEEN_TS,
        ARRAY_DISTINCT(ARRAY_AGG(SOURCE_TAG) WITHIN GROUP (ORDER BY SOURCE_TAG)) AS SOURCE_SET
    FROM member_rows
    GROUP BY CLUSTER_ID
)
SELECT
    a.*,
    -- Real confidence: average of MATCH_SCORE across in-cluster edges from
    -- IDR_CORE_MATCH_RESULTS. Singletons (no edges) default to 0.5 so they
    -- don't appear "high confidence" but also don't disappear from sorted
    -- views. This mirrors SSP's match_confidence CTE pattern.
    COALESCE(s.AVG_MATCH_SCORE, 0.5) AS CONFIDENCE_SCORE,
    COALESCE(s.IN_CLUSTER_EDGE_COUNT, 0) AS IN_CLUSTER_EDGE_COUNT
FROM agg a
LEFT JOIN in_cluster_match_scores s ON s.CLUSTER_ID = a.CLUSTER_ID;
