-- ============================================================================
-- Martech: APP.SP_CUSTOM_STANDARDIZE
-- Use-case-specific standardize hook invoked by engine SP_STANDARDIZE_DATA
-- (because config.yaml has custom_standardize: true).
--
-- Responsibilities:
--   1. Drain each bronze stream into its STD_* table
--   2. Compute *_STD canonical forms (email lowercase+trim, HEM SHA-256,
--      phone E.164, names UPPER, address UPPER, FIRST_NAME_CANONICAL via
--      nickname map join, Shopify JSON flatten)
--   3. Set STD_PROCESSED_AT / STD_BATCH_ID
--   4. Mark IDR_PROCESSED = FALSE so the engine's identifier extractor
--      picks up the new rows
--
-- NFR-1: NO hard-coded DB literal. DB qualifier resolved via
--        SELECT CURRENT_DATABASE() at runtime.
-- ============================================================================

CREATE OR REPLACE PROCEDURE IDENTIFIER('&{deployment_db}.APP.SP_CUSTOM_STANDARDIZE')()
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
    var dbResult = snowflake.execute({sqlText: "SELECT CURRENT_DATABASE() AS DB"});
    dbResult.next();
    var DB = dbResult.getColumnValue('DB');

    var batchResult = snowflake.execute({sqlText: "SELECT UUID_STRING() AS BID"});
    batchResult.next();
    var batchId = batchResult.getColumnValue('BID');

    var results = { pos: 0, loyalty: 0, web: 0, shopify: 0 };
    var errors = [];

    function exec(sql) {
        try { return snowflake.execute({sqlText: sql}); }
        catch (e) { errors.push({sql: sql.substring(0, 200), error: e.message}); return null; }
    }

    // ─── POS Transactions ────────────────────────────────────────────────
    var posSql =
        "INSERT INTO " + DB + ".SILVER.STD_POS_TRANSACTION_RAW (" +
        "  TXN_ID, STORE_ID, TS, TENDER_TYPE, TOTAL, LOYALTY_MEMBER_NUMBER, " +
        "  CUSTOMER_EMAIL, CUSTOMER_PHONE, CUSTOMER_FIRST_NAME, CUSTOMER_LAST_NAME, " +
        "  LINE_ITEMS_JSON, SOURCE_FILE, INGESTED_AT, " +
        "  EMAIL_STD, EMAIL_HEM, PHONE_E164, LOYALTY_MEMBER_NUMBER_STD, " +
        "  FIRST_NAME_STD, LAST_NAME_STD, FIRST_NAME_CANONICAL, " +
        "  CUSTOMER_EMAIL_STD, CUSTOMER_PHONE_STD, CUSTOMER_FIRST_NAME_STD, CUSTOMER_LAST_NAME_STD, " +
        "  STD_PROCESSED_AT, STD_BATCH_ID, IDR_PROCESSED" +
        ") " +
        "SELECT " +
        "  s.TXN_ID, s.STORE_ID, s.TS, s.TENDER_TYPE, s.TOTAL, s.LOYALTY_MEMBER_NUMBER, " +
        "  s.CUSTOMER_EMAIL, s.CUSTOMER_PHONE, s.CUSTOMER_FIRST_NAME, s.CUSTOMER_LAST_NAME, " +
        "  s.LINE_ITEMS_JSON, s.SOURCE_FILE, s.INGESTED_AT, " +
        "  LOWER(TRIM(s.CUSTOMER_EMAIL))," +
        "  CASE WHEN s.CUSTOMER_EMAIL IS NOT NULL THEN SHA2_HEX(LOWER(TRIM(s.CUSTOMER_EMAIL)), 256) END," +
        "  CASE WHEN LENGTH(REGEXP_REPLACE(s.CUSTOMER_PHONE, '[^0-9]', '')) >= 10 " +
        "       THEN '+1' || RIGHT(REGEXP_REPLACE(s.CUSTOMER_PHONE, '[^0-9]', ''), 10) END," +
        "  UPPER(TRIM(s.LOYALTY_MEMBER_NUMBER))," +
        "  UPPER(REGEXP_REPLACE(s.CUSTOMER_FIRST_NAME, '[^A-Za-z]', ''))," +
        "  UPPER(REGEXP_REPLACE(s.CUSTOMER_LAST_NAME, '[^A-Za-z]', ''))," +
        "  COALESCE(nm.canonical_name, UPPER(REGEXP_REPLACE(s.CUSTOMER_FIRST_NAME, '[^A-Za-z]', '')))," +
        // Prefixed columns: engine extractor probes <bronze_col>_STD, so populate
        // CUSTOMER_EMAIL_STD etc. with the same normalized values. PHONE uses
        // digits-only (no '+') to match engine's default PHONE normalization
        // applied to loyalty's bronze column (which has no _STD shadow).
        "  LOWER(TRIM(s.CUSTOMER_EMAIL))," +
        "  REGEXP_REPLACE(s.CUSTOMER_PHONE, '[^0-9]', '')," +
        "  UPPER(REGEXP_REPLACE(s.CUSTOMER_FIRST_NAME, '[^A-Za-z]', ''))," +
        "  UPPER(REGEXP_REPLACE(s.CUSTOMER_LAST_NAME, '[^A-Za-z]', ''))," +
        "  CURRENT_TIMESTAMP(), '" + batchId + "', FALSE " +
        "FROM " + DB + ".BRONZE.POS_TRANSACTION_STREAM s " +
        "LEFT JOIN " + DB + ".CONFIG.IDR_CORE_NICKNAME_MAP nm " +
        "  ON nm.nickname = UPPER(REGEXP_REPLACE(s.CUSTOMER_FIRST_NAME, '[^A-Za-z]', ''))";
    var r = exec(posSql); if (r) { r.next(); results.pos = r.getColumnValue(1); }

    // ─── Loyalty Members ─────────────────────────────────────────────────
    var loySql =
        "INSERT INTO " + DB + ".SILVER.STD_LOYALTY_MEMBER_RAW (" +
        "  MEMBER_ID, EMAIL, PHONE, FIRST_NAME, LAST_NAME, STREET_ADDRESS, CITY, STATE, POSTAL_CODE, " +
        "  COUNTRY, TIER, POINTS, ENROLLED_AT, SOURCE_FILE, INGESTED_AT, " +
        "  EMAIL_STD, EMAIL_HEM, PHONE_E164, MEMBER_ID_STD, FIRST_NAME_STD, LAST_NAME_STD, " +
        "  FIRST_NAME_CANONICAL, STREET_ADDRESS_STD, CITY_STD, STATE_STD, POSTAL_CODE_STD, " +
        "  STD_PROCESSED_AT, STD_BATCH_ID, IDR_PROCESSED" +
        ") " +
        "SELECT " +
        "  s.MEMBER_ID, s.EMAIL, s.PHONE, s.FIRST_NAME, s.LAST_NAME, s.STREET_ADDRESS, s.CITY, s.STATE, s.POSTAL_CODE, " +
        "  s.COUNTRY, s.TIER, s.POINTS, s.ENROLLED_AT, s.SOURCE_FILE, s.INGESTED_AT, " +
        "  LOWER(TRIM(s.EMAIL))," +
        "  CASE WHEN s.EMAIL IS NOT NULL THEN SHA2_HEX(LOWER(TRIM(s.EMAIL)), 256) END," +
        "  CASE WHEN LENGTH(REGEXP_REPLACE(s.PHONE, '[^0-9]', '')) >= 10 " +
        "       THEN '+1' || RIGHT(REGEXP_REPLACE(s.PHONE, '[^0-9]', ''), 10) END," +
        "  UPPER(TRIM(s.MEMBER_ID))," +
        "  UPPER(REGEXP_REPLACE(s.FIRST_NAME, '[^A-Za-z]', ''))," +
        "  UPPER(REGEXP_REPLACE(s.LAST_NAME, '[^A-Za-z]', ''))," +
        "  COALESCE(nm.canonical_name, UPPER(REGEXP_REPLACE(s.FIRST_NAME, '[^A-Za-z]', '')))," +
        "  UPPER(TRIM(s.STREET_ADDRESS))," +
        "  UPPER(TRIM(s.CITY))," +
        "  UPPER(TRIM(s.STATE))," +
        "  UPPER(REGEXP_REPLACE(s.POSTAL_CODE, '[^A-Za-z0-9]', ''))," +
        "  CURRENT_TIMESTAMP(), '" + batchId + "', FALSE " +
        "FROM " + DB + ".BRONZE.LOYALTY_MEMBER_STREAM s " +
        "LEFT JOIN " + DB + ".CONFIG.IDR_CORE_NICKNAME_MAP nm " +
        "  ON nm.nickname = UPPER(REGEXP_REPLACE(s.FIRST_NAME, '[^A-Za-z]', ''))";
    r = exec(loySql); if (r) { r.next(); results.loyalty = r.getColumnValue(1); }

    // ─── Web Clickstream ─────────────────────────────────────────────────
    var webSql =
        "INSERT INTO " + DB + ".SILVER.STD_WEB_CLICKSTREAM_RAW (" +
        "  EVENT_ID, TS, ANONYMOUS_ID, DEVICE_ID, UID2, RAMPID, SESSION_ID, USER_AGENT, IP, " +
        "  EVENT_NAME, PAGE_URL, LOGGED_IN_EMAIL, LOGGED_IN_PHONE, LOGGED_IN_MEMBER_ID, " +
        "  EVENT_PROPERTIES_JSON, SOURCE_FILE, INGESTED_AT, " +
        "  ANONYMOUS_ID_STD, DEVICE_ID_STD, UID2_STD, RAMPID_STD, EMAIL_STD, EMAIL_HEM, " +
        "  PHONE_E164, LOGGED_IN_MEMBER_ID_STD, USER_AGENT_STD, IP_STD, " +
        "  LOGGED_IN_EMAIL_STD, LOGGED_IN_PHONE_STD, " +
        "  STD_PROCESSED_AT, STD_BATCH_ID, IDR_PROCESSED" +
        ") " +
        "SELECT " +
        "  s.EVENT_ID, s.TS, s.ANONYMOUS_ID, s.DEVICE_ID, s.UID2, s.RAMPID, s.SESSION_ID, s.USER_AGENT, s.IP, " +
        "  s.EVENT_NAME, s.PAGE_URL, s.LOGGED_IN_EMAIL, s.LOGGED_IN_PHONE, s.LOGGED_IN_MEMBER_ID, " +
        "  s.EVENT_PROPERTIES_JSON, s.SOURCE_FILE, s.INGESTED_AT, " +
        "  TRIM(s.ANONYMOUS_ID)," +
        "  TRIM(s.DEVICE_ID)," +
        "  TRIM(s.UID2)," +
        "  TRIM(s.RAMPID)," +
        "  LOWER(TRIM(s.LOGGED_IN_EMAIL))," +
        "  CASE WHEN s.LOGGED_IN_EMAIL IS NOT NULL THEN SHA2_HEX(LOWER(TRIM(s.LOGGED_IN_EMAIL)), 256) END," +
        "  CASE WHEN LENGTH(REGEXP_REPLACE(s.LOGGED_IN_PHONE, '[^0-9]', '')) >= 10 " +
        "       THEN '+1' || RIGHT(REGEXP_REPLACE(s.LOGGED_IN_PHONE, '[^0-9]', ''), 10) END," +
        "  UPPER(TRIM(s.LOGGED_IN_MEMBER_ID))," +
        "  REGEXP_REPLACE(COALESCE(s.USER_AGENT, ''), '[^a-zA-Z0-9]', '')," +
        "  TRIM(s.IP)," +
        // Engine extractor probes <bronze_col>_STD, so populate
        // LOGGED_IN_EMAIL_STD / LOGGED_IN_PHONE_STD with the same normalized
        // values. PHONE uses digits-only (no '+') to match engine's default
        // PHONE normalization applied to loyalty bronze (no _STD shadow there).
        "  LOWER(TRIM(s.LOGGED_IN_EMAIL))," +
        "  REGEXP_REPLACE(s.LOGGED_IN_PHONE, '[^0-9]', '')," +
        "  CURRENT_TIMESTAMP(), '" + batchId + "', FALSE " +
        "FROM " + DB + ".BRONZE.WEB_CLICKSTREAM_STREAM s";
    r = exec(webSql); if (r) { r.next(); results.web = r.getColumnValue(1); }

    // ─── Shopify Orders (flatten RAW_PAYLOAD) ────────────────────────────
    var shopifySql =
        "INSERT INTO " + DB + ".SILVER.STD_SHOPIFY_ORDER_RAW (" +
        "  ORDER_ID, ORDER_NUMBER, CREATED_AT_SRC, TOTAL_PRICE, FINANCIAL_STATUS, RAW_PAYLOAD, SOURCE_FILE, INGESTED_AT, " +
        "  EMAIL, EMAIL_STD, EMAIL_HEM, PHONE, PHONE_E164, SHOPIFY_CUSTOMER_ID, " +
        "  LOYALTY_MEMBER_NUMBER, LOYALTY_MEMBER_NUMBER_STD, " +
        "  FIRST_NAME, LAST_NAME, FIRST_NAME_STD, LAST_NAME_STD, FIRST_NAME_CANONICAL, " +
        "  BILLING_STREET_ADDRESS, BILLING_CITY, BILLING_STATE, BILLING_POSTAL_CODE, BILLING_COUNTRY, " +
        "  SHIPPING_STREET_ADDRESS, SHIPPING_CITY, SHIPPING_STATE, SHIPPING_POSTAL_CODE, SHIPPING_COUNTRY, " +
        "  BROWSER_IP, USER_AGENT, SESSION_HASH, GCLID, FBCLID, TTCLID, LANDING_SITE, " +
        "  STD_PROCESSED_AT, STD_BATCH_ID, IDR_PROCESSED" +
        ") " +
        "SELECT " +
        "  s.ORDER_ID, s.ORDER_NUMBER, s.CREATED_AT_SRC, s.TOTAL_PRICE, s.FINANCIAL_STATUS, s.RAW_PAYLOAD, s.SOURCE_FILE, s.INGESTED_AT, " +
        "  s.RAW_PAYLOAD:customer.email::VARCHAR, " +
        "  LOWER(TRIM(s.RAW_PAYLOAD:customer.email::VARCHAR)), " +
        "  CASE WHEN s.RAW_PAYLOAD:customer.email IS NOT NULL THEN SHA2_HEX(LOWER(TRIM(s.RAW_PAYLOAD:customer.email::VARCHAR)), 256) END, " +
        "  s.RAW_PAYLOAD:customer.phone::VARCHAR, " +
        "  CASE WHEN LENGTH(REGEXP_REPLACE(s.RAW_PAYLOAD:customer.phone::VARCHAR, '[^0-9]', '')) >= 10 " +
        "       THEN '+1' || RIGHT(REGEXP_REPLACE(s.RAW_PAYLOAD:customer.phone::VARCHAR, '[^0-9]', ''), 10) END, " +
        "  s.RAW_PAYLOAD:customer.id::VARCHAR, " +
        // LOYALTY_MEMBER_NUMBER from note_attributes[name='loyalty_member_number'].value
        // Uses FILTER lambda + GET — pure inline expression, no subquery / no
        // LATERAL FLATTEN. Earlier scalar (SELECT MAX ... FROM LATERAL FLATTEN(s.RAW_PAYLOAD:note_attributes))
        // patterns trip Snowflake with "Unsupported subquery type" because
        // scalar subqueries cannot correlate to a VARIANT column for FLATTEN.
        "  GET(FILTER(s.RAW_PAYLOAD:note_attributes, x -> x:name::VARCHAR='loyalty_member_number')[0], 'value')::VARCHAR, " +
        "  UPPER(TRIM(GET(FILTER(s.RAW_PAYLOAD:note_attributes, x -> x:name::VARCHAR='loyalty_member_number')[0], 'value')::VARCHAR)), " +
        "  s.RAW_PAYLOAD:customer.first_name::VARCHAR, " +
        "  s.RAW_PAYLOAD:customer.last_name::VARCHAR, " +
        "  UPPER(REGEXP_REPLACE(s.RAW_PAYLOAD:customer.first_name::VARCHAR, '[^A-Za-z]', '')), " +
        "  UPPER(REGEXP_REPLACE(s.RAW_PAYLOAD:customer.last_name::VARCHAR, '[^A-Za-z]', '')), " +
        "  COALESCE(nm.canonical_name, UPPER(REGEXP_REPLACE(s.RAW_PAYLOAD:customer.first_name::VARCHAR, '[^A-Za-z]', ''))), " +
        "  UPPER(TRIM(s.RAW_PAYLOAD:billing_address.address1::VARCHAR)), " +
        "  UPPER(TRIM(s.RAW_PAYLOAD:billing_address.city::VARCHAR)), " +
        "  UPPER(TRIM(s.RAW_PAYLOAD:billing_address.province::VARCHAR)), " +
        "  UPPER(REGEXP_REPLACE(s.RAW_PAYLOAD:billing_address.zip::VARCHAR, '[^A-Za-z0-9]', '')), " +
        "  UPPER(TRIM(s.RAW_PAYLOAD:billing_address.country::VARCHAR)), " +
        "  UPPER(TRIM(s.RAW_PAYLOAD:shipping_address.address1::VARCHAR)), " +
        "  UPPER(TRIM(s.RAW_PAYLOAD:shipping_address.city::VARCHAR)), " +
        "  UPPER(TRIM(s.RAW_PAYLOAD:shipping_address.province::VARCHAR)), " +
        "  UPPER(REGEXP_REPLACE(s.RAW_PAYLOAD:shipping_address.zip::VARCHAR, '[^A-Za-z0-9]', '')), " +
        "  UPPER(TRIM(s.RAW_PAYLOAD:shipping_address.country::VARCHAR)), " +
        "  s.RAW_PAYLOAD:client_details.browser_ip::VARCHAR, " +
        "  s.RAW_PAYLOAD:client_details.user_agent::VARCHAR, " +
        "  s.RAW_PAYLOAD:client_details.session_hash::VARCHAR, " +
        "  REGEXP_SUBSTR(s.RAW_PAYLOAD:landing_site::VARCHAR, 'gclid=([^&]+)', 1, 1, 'e', 1), " +
        "  REGEXP_SUBSTR(s.RAW_PAYLOAD:landing_site::VARCHAR, 'fbclid=([^&]+)', 1, 1, 'e', 1), " +
        "  REGEXP_SUBSTR(s.RAW_PAYLOAD:landing_site::VARCHAR, 'ttclid=([^&]+)', 1, 1, 'e', 1), " +
        "  s.RAW_PAYLOAD:landing_site::VARCHAR, " +
        "  CURRENT_TIMESTAMP(), '" + batchId + "', FALSE " +
        "FROM " + DB + ".BRONZE.SHOPIFY_ORDER_STREAM s " +
        "LEFT JOIN " + DB + ".CONFIG.IDR_CORE_NICKNAME_MAP nm " +
        "  ON nm.nickname = UPPER(REGEXP_REPLACE(s.RAW_PAYLOAD:customer.first_name::VARCHAR, '[^A-Za-z]', '')) " +
        "QUALIFY ROW_NUMBER() OVER (PARTITION BY s.ORDER_ID ORDER BY s.INGESTED_AT) = 1";
    r = exec(shopifySql); if (r) { r.next(); results.shopify = r.getColumnValue(1); }

    // ─── v1: ML blocking attributes + stop-list application ─────────────
    // Computed AFTER each STD insert by UPDATE keyed on STD_BATCH_ID. Drives
    // U4 incremental candidate generation. Stoplist consulted from
    // CONFIG.IDR_ML_INDIVIDUAL_BLOCKING_STOPLIST (table created by U3 — must
    // be deployed before this proc runs).

    function blockingUpdate(table, lastNameCol, rawPhoneCol) {
        // lastNameCol may be NULL (e.g., WEB has no LAST_NAME_STD)
        var lnExpr = lastNameCol ? "SOUNDEX(" + lastNameCol + ")" : "NULL";
        var fqTable = DB + "." + table;

        // PHONE_LAST7 derivation. Prefer the full E.164 tail; otherwise fall
        // back to the trailing 7 digits of the raw phone so a "local" number
        // captured without an area code (7-9 digits, PHONE_E164 = NULL) still
        // yields a legitimate last-7 signal. PHONE_LAST10 / PHONE_AREA_CODE stay
        // E.164-gated (local numbers have no area code -> correctly NULL).
        var last7Expr = rawPhoneCol
            ? "CASE WHEN PHONE_E164 IS NOT NULL AND LENGTH(PHONE_E164) >= 11 THEN RIGHT(PHONE_E164, 7) " +
              "     WHEN LENGTH(REGEXP_REPLACE(" + rawPhoneCol + ", '[^0-9]', '')) >= 7 " +
              "          THEN RIGHT(REGEXP_REPLACE(" + rawPhoneCol + ", '[^0-9]', ''), 7) END"
            : "CASE WHEN PHONE_E164 IS NOT NULL AND LENGTH(PHONE_E164) >= 11 THEN RIGHT(PHONE_E164, 7) END";

        // ─── Pass A: simple derivations (no subqueries) ─────────────────────
        // EMAIL_HANDLE_NORMALIZED, PHONE_LAST10/7/AREA_CODE, SOUNDEX_LNAME
        // are computed once here so Pass B/C can reference the column
        // instead of re-deriving it inside correlated subqueries (Snowflake
        // disallows "Unsupported subquery type" when multiple correlated
        // EXISTS recompute the same outer expression in a single UPDATE).
        var passA =
            "UPDATE " + fqTable + " SET " +
            "  EMAIL_HANDLE_NORMALIZED = NULLIF(" +
            "    REGEXP_REPLACE(" +
            "      CASE WHEN LOWER(SPLIT_PART(EMAIL_STD, '@', 2)) IN ('gmail.com','googlemail.com')" +
            "           THEN REGEXP_REPLACE(SPLIT_PART(SPLIT_PART(EMAIL_STD, '@', 1), '+', 1), '\\\\.', '')" +
            "           ELSE LOWER(SPLIT_PART(SPLIT_PART(EMAIL_STD, '@', 1), '+', 1))" +
            "      END, '[^a-z]', ''), '')," +
            "  PHONE_LAST10 = CASE WHEN PHONE_E164 IS NOT NULL AND LENGTH(PHONE_E164) >= 11 " +
            "                       THEN RIGHT(PHONE_E164, 10) END, " +
            "  PHONE_LAST7  = " + last7Expr + ", " +
            "  PHONE_AREA_CODE = CASE WHEN PHONE_E164 IS NOT NULL AND LENGTH(PHONE_E164) >= 11 " +
            "                          THEN SUBSTR(RIGHT(PHONE_E164, 10), 1, 3) END, " +
            "  SOUNDEX_LNAME = " + lnExpr + ", " +
            "  ML_BLOCKING_PROCESSED = FALSE, " +
            "  ML_BLOCKING_PROCESSED_AT = NULL " +
            "WHERE STD_BATCH_ID = '" + batchId + "'";
        var rA = exec(passA);

        // ─── Pass B: IS_BLOCKABLE_EMAIL via LEFT JOIN to stoplist ───────────
        // Snowflake disallows correlated EXISTS with multiple ORed predicates
        // in UPDATE SET ("Unsupported subquery type"). The LEFT JOIN +
        // COUNT_IF aggregate variant is universally supported.
        var passB =
            "UPDATE " + fqTable + " t " +
            "SET IS_BLOCKABLE_EMAIL = CASE WHEN m.has_hit > 0 OR t.EMAIL_HANDLE_NORMALIZED IS NULL THEN FALSE ELSE TRUE END " +
            "FROM (" +
            "  SELECT s.STD_BATCH_ID, s.EMAIL_HANDLE_NORMALIZED, " +
            "         COUNT_IF(sl.IS_ACTIVE = TRUE AND sl.ATTRIBUTE = 'EMAIL_HANDLE' AND " +
            "                  ((sl.PATTERN_TYPE = 'EXACT' AND sl.PATTERN = s.EMAIL_HANDLE_NORMALIZED)" +
            "                   OR (sl.PATTERN_TYPE = 'STARTS_WITH' AND STARTSWITH(s.EMAIL_HANDLE_NORMALIZED, sl.PATTERN))" +
            "                   OR (sl.PATTERN_TYPE = 'CONTAINS' AND POSITION(sl.PATTERN, s.EMAIL_HANDLE_NORMALIZED) > 0))" +
            "         ) AS has_hit " +
            "  FROM " + fqTable + " s " +
            "  LEFT JOIN " + DB + ".CONFIG.IDR_ML_INDIVIDUAL_BLOCKING_STOPLIST sl ON 1=1 " +
            "  WHERE s.STD_BATCH_ID = '" + batchId + "' " +
            "  GROUP BY s.STD_BATCH_ID, s.EMAIL_HANDLE_NORMALIZED" +
            ") m " +
            "WHERE t.STD_BATCH_ID = m.STD_BATCH_ID " +
            "  AND COALESCE(t.EMAIL_HANDLE_NORMALIZED, '__NULL__') = COALESCE(m.EMAIL_HANDLE_NORMALIZED, '__NULL__')";
        exec(passB);

        // ─── Pass C: IS_BLOCKABLE_PHONE via LEFT JOIN to stoplist ───────────
        var passC =
            "UPDATE " + fqTable + " t " +
            "SET IS_BLOCKABLE_PHONE = CASE WHEN t.PHONE_LAST7 IS NULL THEN FALSE " +
            "                              WHEN m.has_hit > 0 THEN FALSE " +
            "                              WHEN t.PHONE_LAST7 IN ('0123456','1234567','2345678','3456789','4567890','9876543','8765432','7654321','6543210','5432109') AND m.has_seq > 0 THEN FALSE " +
            "                              WHEN LENGTH(REGEXP_REPLACE(t.PHONE_LAST7, SUBSTR(t.PHONE_LAST7,1,1), '')) = 0 AND m.has_allsame > 0 THEN FALSE " +
            "                              ELSE TRUE END " +
            "FROM (" +
            "  SELECT s.STD_BATCH_ID, s.PHONE_LAST7, s.PHONE_AREA_CODE, " +
            "         COUNT_IF(sl.IS_ACTIVE = TRUE AND sl.ATTRIBUTE = 'PHONE_AREA_CODE' AND sl.PATTERN_TYPE = 'EXACT' AND sl.PATTERN = s.PHONE_AREA_CODE) " +
            "         + COUNT_IF(sl.IS_ACTIVE = TRUE AND sl.ATTRIBUTE = 'PHONE_LAST7' AND " +
            "                    ((sl.PATTERN_TYPE = 'EXACT' AND sl.PATTERN = s.PHONE_LAST7)" +
            "                     OR (sl.PATTERN_TYPE = 'STARTS_WITH' AND STARTSWITH(s.PHONE_LAST7, sl.PATTERN)))) AS has_hit, " +
            "         COUNT_IF(sl.IS_ACTIVE = TRUE AND sl.ATTRIBUTE = 'PHONE_LAST7' AND sl.PATTERN_TYPE = 'SEQUENTIAL') AS has_seq, " +
            "         COUNT_IF(sl.IS_ACTIVE = TRUE AND sl.ATTRIBUTE = 'PHONE_LAST7' AND sl.PATTERN_TYPE = 'ALL_SAME') AS has_allsame " +
            "  FROM " + fqTable + " s " +
            "  LEFT JOIN " + DB + ".CONFIG.IDR_ML_INDIVIDUAL_BLOCKING_STOPLIST sl ON 1=1 " +
            "  WHERE s.STD_BATCH_ID = '" + batchId + "' " +
            "  GROUP BY s.STD_BATCH_ID, s.PHONE_LAST7, s.PHONE_AREA_CODE" +
            ") m " +
            "WHERE t.STD_BATCH_ID = m.STD_BATCH_ID " +
            "  AND COALESCE(t.PHONE_LAST7, '__N__') = COALESCE(m.PHONE_LAST7, '__N__') " +
            "  AND COALESCE(t.PHONE_AREA_CODE, '__N__') = COALESCE(m.PHONE_AREA_CODE, '__N__')";
        exec(passC);
        return rA;
    }

    blockingUpdate('SILVER.STD_POS_TRANSACTION_RAW',  'LAST_NAME_STD', 'CUSTOMER_PHONE');
    blockingUpdate('SILVER.STD_LOYALTY_MEMBER_RAW',   'LAST_NAME_STD', 'PHONE');
    blockingUpdate('SILVER.STD_WEB_CLICKSTREAM_RAW',  null, 'LOGGED_IN_PHONE');  // no LAST_NAME_STD on WEB
    blockingUpdate('SILVER.STD_SHOPIFY_ORDER_RAW',    'LAST_NAME_STD', 'PHONE');

    return JSON.stringify({ batch_id: batchId, rows_in: results, errors: errors });
$$;
