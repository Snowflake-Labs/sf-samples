-- ============================================================================
-- Martech: APP.SP_GENERATE_ML_CANDIDATES (v1 — config-driven multi-strategy)
-- Reads active strategies from CONFIG.IDR_ML_INDIVIDUAL_BLOCKING_STRATEGIES
-- and emits candidate pairs into SILVER.ML_PAIR_FEATURES.
--
-- Incremental: drives off STD_*_RAW.ML_BLOCKING_PROCESSED flag.
-- Dedup-with-array-append: pair existing in ML_PAIR_FEATURES gets its
--   BLOCKING_KEYS_HIT array extended with the new strategy_id.
-- Hot-block cap: per-strategy MAX_BLOCK_SIZE skips oversized blocks.
--
-- NFR-1: Reads CURRENT_DATABASE() — no hard-coded literal.
-- ============================================================================

CREATE OR REPLACE PROCEDURE IDENTIFIER('&{deployment_db}.APP.SP_GENERATE_ML_CANDIDATES')()
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
    var dbResult = snowflake.execute({sqlText: "SELECT CURRENT_DATABASE() AS DB"});
    dbResult.next();
    var DB = dbResult.getColumnValue('DB') || 'MARTECH';

    // Ensure temp tables resolve to a database (required for CREATE OR REPLACE TEMPORARY TABLE).
    snowflake.execute({sqlText: "USE DATABASE " + DB});
    snowflake.execute({sqlText: "USE SCHEMA SILVER"});

    var totals = { strategies_run: 0, pairs_inserted: 0, pairs_appended: 0, records_processed: 0 };
    var perStrategy = [];
    var errors = [];

    function exec(sql) {
        try { return snowflake.execute({sqlText: sql}); }
        catch (e) { errors.push({sql: sql.substring(0, 200), error: e.message}); return null; }
    }

    // ─── Step 1: Build unified candidate-row view across all 4 STD sources ─────
    // Includes ALL columns needed by ANY strategy. Records with no LAST_NAME_STD
    // (e.g., WEB) still flow through — strategy PREREQUISITE_PREDICATE filters
    // them out at strategy level.
    exec(
        "CREATE OR REPLACE TEMPORARY TABLE _all_records AS " +
        "SELECT TXN_ID AS SRC_ID, 'POS_TRANSACTION_RAW' AS SRC_TYPE, " +
        "       EMAIL_STD, EMAIL_HEM, PHONE_E164, " +
        "       LOYALTY_MEMBER_NUMBER_STD AS LOYALTY_ID_STD, NULL AS DEVICE_ID_STD, " +
        "       FIRST_NAME_STD, LAST_NAME_STD, FIRST_NAME_CANONICAL, " +
        "       NULL AS STREET_STD, NULL AS CITY_STD, NULL AS STATE_STD, NULL AS POSTAL_CODE_STD, " +
        "       EMAIL_HANDLE_NORMALIZED, SOUNDEX_LNAME, PHONE_LAST7, " +
        "       IS_BLOCKABLE_EMAIL, IS_BLOCKABLE_PHONE, ML_BLOCKING_PROCESSED " +
        "FROM " + DB + ".SILVER.STD_POS_TRANSACTION_RAW " +
        "UNION ALL " +
        "SELECT MEMBER_ID AS SRC_ID, 'LOYALTY_MEMBER_RAW', " +
        "       EMAIL_STD, EMAIL_HEM, PHONE_E164, MEMBER_ID_STD, NULL, " +
        "       FIRST_NAME_STD, LAST_NAME_STD, FIRST_NAME_CANONICAL, " +
        "       STREET_ADDRESS_STD, CITY_STD, STATE_STD, POSTAL_CODE_STD, " +
        "       EMAIL_HANDLE_NORMALIZED, SOUNDEX_LNAME, PHONE_LAST7, " +
        "       IS_BLOCKABLE_EMAIL, IS_BLOCKABLE_PHONE, ML_BLOCKING_PROCESSED " +
        "FROM " + DB + ".SILVER.STD_LOYALTY_MEMBER_RAW " +
        "UNION ALL " +
        "SELECT EVENT_ID AS SRC_ID, 'WEB_CLICKSTREAM_RAW', " +
        "       EMAIL_STD, EMAIL_HEM, PHONE_E164, LOGGED_IN_MEMBER_ID_STD, DEVICE_ID_STD, " +
        "       NULL, NULL, NULL, NULL, NULL, NULL, NULL, " +
        "       EMAIL_HANDLE_NORMALIZED, SOUNDEX_LNAME, PHONE_LAST7, " +
        "       IS_BLOCKABLE_EMAIL, IS_BLOCKABLE_PHONE, ML_BLOCKING_PROCESSED " +
        "FROM " + DB + ".SILVER.STD_WEB_CLICKSTREAM_RAW " +
        "UNION ALL " +
        "SELECT ORDER_ID AS SRC_ID, 'SHOPIFY_ORDER_RAW', " +
        "       EMAIL_STD, EMAIL_HEM, PHONE_E164, LOYALTY_MEMBER_NUMBER_STD, NULL, " +
        "       FIRST_NAME_STD, LAST_NAME_STD, FIRST_NAME_CANONICAL, " +
        "       BILLING_STREET_ADDRESS, BILLING_CITY, BILLING_STATE, BILLING_POSTAL_CODE, " +
        "       EMAIL_HANDLE_NORMALIZED, SOUNDEX_LNAME, PHONE_LAST7, " +
        "       IS_BLOCKABLE_EMAIL, IS_BLOCKABLE_PHONE, ML_BLOCKING_PROCESSED " +
        "FROM " + DB + ".SILVER.STD_SHOPIFY_ORDER_RAW"
    );

    // Track which records we'll mark as processed at the end.
    exec(
        "CREATE OR REPLACE TEMPORARY TABLE _new_record_ids AS " +
        "SELECT SRC_ID FROM _all_records WHERE ML_BLOCKING_PROCESSED = FALSE"
    );

    var newRecCount = 0;
    var rc = exec("SELECT COUNT(*) FROM _new_record_ids");
    if (rc) { rc.next(); newRecCount = rc.getColumnValue(1); }
    totals.records_processed = newRecCount;

    // Early exit if no new records
    if (newRecCount === 0) {
        return JSON.stringify({ totals: totals, per_strategy: perStrategy, errors: errors, skipped: 'NO_NEW_RECORDS' });
    }

    // ─── Step 2: Load active strategies ────────────────────────────────────────
    var strategies = [];
    var sr = exec(
        "SELECT STRATEGY_ID, STRATEGY_NAME, BLOCK_KEY_EXPR, PREREQUISITE_PREDICATE, MAX_BLOCK_SIZE " +
        "FROM " + DB + ".CONFIG.IDR_ML_INDIVIDUAL_BLOCKING_STRATEGIES " +
        "WHERE IS_ACTIVE = TRUE " +
        "ORDER BY PRIORITY"
    );
    while (sr && sr.next()) {
        strategies.push({
            id:       sr.getColumnValue(1),
            name:     sr.getColumnValue(2),
            keyExpr:  sr.getColumnValue(3),
            predicate:sr.getColumnValue(4),
            maxBlock: sr.getColumnValue(5)
        });
    }

    // ─── Step 3: For each strategy, generate candidate pairs ──────────────────
    for (var i = 0; i < strategies.length; i++) {
        var s = strategies[i];
        var stratStats = { strategy_id: s.id, pairs_inserted: 0, pairs_appended: 0, max_block_size: 0 };

        // Build per-strategy candidate set (only records satisfying predicate)
        // The predicate uses alias 'r' — caller's SQL fragment must reference r.<col>.
        // The block_key_expr likewise references r.<col>.
        // Block keys for new records → only consider blocks where at least one new record participates.
        exec(
            "CREATE OR REPLACE TEMPORARY TABLE _strat_eligible AS " +
            "SELECT SRC_ID, SRC_TYPE, ML_BLOCKING_PROCESSED, " +
            "       (" + s.keyExpr + ") AS BLOCK_KEY " +
            "FROM _all_records r " +
            "WHERE " + s.predicate
        );

        // Compute block sizes; cap at MAX_BLOCK_SIZE
        exec(
            "CREATE OR REPLACE TEMPORARY TABLE _strat_block_sizes AS " +
            "SELECT BLOCK_KEY, COUNT(*) AS BLOCK_SZ " +
            "FROM _strat_eligible " +
            "GROUP BY BLOCK_KEY"
        );

        exec(
            "CREATE OR REPLACE TEMPORARY TABLE _strat_eligible_capped AS " +
            "SELECT e.* FROM _strat_eligible e " +
            "JOIN _strat_block_sizes b ON e.BLOCK_KEY = b.BLOCK_KEY " +
            "WHERE b.BLOCK_SZ <= " + s.maxBlock
        );

        // Generate candidate pairs: (new × any) within the same block, dedup with LEAST/GREATEST
        // Source pair where at least one side is a NEW record (ML_BLOCKING_PROCESSED=FALSE).
        exec(
            "CREATE OR REPLACE TEMPORARY TABLE _strat_pairs AS " +
            "SELECT DISTINCT " +
            "    LEAST(a.SRC_ID, b.SRC_ID) AS SOURCE_RECORD_ID_A, " +
            "    GREATEST(a.SRC_ID, b.SRC_ID) AS SOURCE_RECORD_ID_B, " +
            "    CASE WHEN a.SRC_ID < b.SRC_ID THEN a.SRC_TYPE ELSE b.SRC_TYPE END AS SOURCE_TYPE_A, " +
            "    CASE WHEN a.SRC_ID < b.SRC_ID THEN b.SRC_TYPE ELSE a.SRC_TYPE END AS SOURCE_TYPE_B, " +
            "    a.BLOCK_KEY, " +
            "    bs.BLOCK_SZ " +
            "FROM _strat_eligible_capped a " +
            "JOIN _strat_eligible_capped b " +
            "  ON a.BLOCK_KEY = b.BLOCK_KEY AND a.SRC_ID < b.SRC_ID " +
            "JOIN _strat_block_sizes bs ON a.BLOCK_KEY = bs.BLOCK_KEY " +
            "WHERE (a.ML_BLOCKING_PROCESSED = FALSE OR b.ML_BLOCKING_PROCESSED = FALSE)"
        );

        // (3a) UPDATE: append strategy_id to BLOCKING_KEYS_HIT for pairs that already exist
        var updSql =
            "UPDATE " + DB + ".SILVER.ML_PAIR_FEATURES f " +
            "SET BLOCKING_KEYS_HIT = ARRAY_APPEND(COALESCE(f.BLOCKING_KEYS_HIT, ARRAY_CONSTRUCT()), '" + s.id + "'), " +
            "    UPDATED_AT = CURRENT_TIMESTAMP() " +
            "FROM _strat_pairs sp " +
            "WHERE LEAST(f.SOURCE_RECORD_ID_A, f.SOURCE_RECORD_ID_B) = sp.SOURCE_RECORD_ID_A " +
            "  AND GREATEST(f.SOURCE_RECORD_ID_A, f.SOURCE_RECORD_ID_B) = sp.SOURCE_RECORD_ID_B " +
            "  AND NOT ARRAY_CONTAINS('" + s.id + "'::VARIANT, COALESCE(f.BLOCKING_KEYS_HIT, ARRAY_CONSTRUCT()))";
        var ur = exec(updSql);
        if (ur) { ur.next(); stratStats.pairs_appended = ur.getColumnValue(1); totals.pairs_appended += stratStats.pairs_appended; }

        // (3b) INSERT: pairs not yet in ML_PAIR_FEATURES — compute features inline
        // Features are looked up against _all_records (joined twice).
        var insSql =
            "INSERT INTO " + DB + ".SILVER.ML_PAIR_FEATURES (" +
            "  SOURCE_RECORD_ID_A, SOURCE_TYPE_A, SOURCE_RECORD_ID_B, SOURCE_TYPE_B, BLOCKING_KEY, " +
            "  EMAIL_EQ, HEM_EQ, PHONE_EQ, LOYALTY_ID_EQ, DEVICE_ID_EQ, " +
            "  JW_FIRST_NAME, JW_LAST_NAME, JW_STREET, JW_CITY, " +
            "  POSTAL_EQ, STATE_EQ, NICKNAME_FIRST_EQ, " +
            "  EMAIL_HANDLE_EQ, PHONE_LAST7_EQ, " +
            "  EMAIL_LEN_DIFF, PHONE_LEN_DIFF, STATUS, " +
            "  BLOCKING_KEYS_HIT, BLOCK_SIZE_AT_GEN" +
            ") " +
            "SELECT sp.SOURCE_RECORD_ID_A, sp.SOURCE_TYPE_A, sp.SOURCE_RECORD_ID_B, sp.SOURCE_TYPE_B, sp.BLOCK_KEY, " +
            "       (a.EMAIL_STD = b.EMAIL_STD) AS EMAIL_EQ, " +
            "       (a.EMAIL_HEM = b.EMAIL_HEM) AS HEM_EQ, " +
            "       (a.PHONE_E164 = b.PHONE_E164) AS PHONE_EQ, " +
            "       (a.LOYALTY_ID_STD = b.LOYALTY_ID_STD) AS LOYALTY_ID_EQ, " +
            "       (a.DEVICE_ID_STD = b.DEVICE_ID_STD) AS DEVICE_ID_EQ, " +
            "       " + DB + ".SILVER.JARO_WINKLER_SIMILARITY(a.FIRST_NAME_STD, b.FIRST_NAME_STD), " +
            "       " + DB + ".SILVER.JARO_WINKLER_SIMILARITY(a.LAST_NAME_STD,  b.LAST_NAME_STD), " +
            "       " + DB + ".SILVER.JARO_WINKLER_SIMILARITY(a.STREET_STD,     b.STREET_STD), " +
            "       " + DB + ".SILVER.JARO_WINKLER_SIMILARITY(a.CITY_STD,       b.CITY_STD), " +
            "       (a.POSTAL_CODE_STD = b.POSTAL_CODE_STD) AS POSTAL_EQ, " +
            "       (a.STATE_STD = b.STATE_STD) AS STATE_EQ, " +
            "       (a.FIRST_NAME_CANONICAL = b.FIRST_NAME_CANONICAL " +
            "        AND a.FIRST_NAME_CANONICAL IS NOT NULL " +
            "        AND a.FIRST_NAME_STD <> b.FIRST_NAME_STD) AS NICKNAME_FIRST_EQ, " +
            "       (SPLIT_PART(COALESCE(a.EMAIL_STD,''), '@', 1) = SPLIT_PART(COALESCE(b.EMAIL_STD,''), '@', 1) " +
            "        AND a.EMAIL_STD IS NOT NULL AND b.EMAIL_STD IS NOT NULL " +
            "        AND SPLIT_PART(a.EMAIL_STD, '@', 1) <> '') AS EMAIL_HANDLE_EQ, " +
            "       (a.PHONE_LAST7 = b.PHONE_LAST7 " +
            "        AND a.PHONE_LAST7 IS NOT NULL AND b.PHONE_LAST7 IS NOT NULL) AS PHONE_LAST7_EQ, " +
            "       ABS(LENGTH(COALESCE(a.EMAIL_STD,'')) - LENGTH(COALESCE(b.EMAIL_STD,''))), " +
            "       ABS(LENGTH(COALESCE(a.PHONE_E164,'')) - LENGTH(COALESCE(b.PHONE_E164,''))), " +
            "       'PENDING', " +
            "       ARRAY_CONSTRUCT('" + s.id + "'), " +
            "       sp.BLOCK_SZ " +
            "FROM _strat_pairs sp " +
            "JOIN _all_records a ON a.SRC_ID = sp.SOURCE_RECORD_ID_A " +
            "JOIN _all_records b ON b.SRC_ID = sp.SOURCE_RECORD_ID_B " +
            "WHERE NOT EXISTS (" +
            "    SELECT 1 FROM " + DB + ".SILVER.ML_PAIR_FEATURES f " +
            "    WHERE LEAST(f.SOURCE_RECORD_ID_A, f.SOURCE_RECORD_ID_B) = sp.SOURCE_RECORD_ID_A " +
            "      AND GREATEST(f.SOURCE_RECORD_ID_A, f.SOURCE_RECORD_ID_B) = sp.SOURCE_RECORD_ID_B" +
            ") " +
            "AND NOT EXISTS (" +
            "    SELECT 1 FROM " + DB + ".SILVER.IDR_CORE_MATCH_RESULTS mr " +
            "    WHERE mr.IS_CURRENT = TRUE " +
            "      AND mr.RULE_ID NOT IN ('MARTECH_R16','MARTECH_R17') " +
            "      AND LEAST(mr.NEW_SOURCE_RECORD_ID, mr.MATCHED_SOURCE_RECORD_ID) = sp.SOURCE_RECORD_ID_A " +
            "      AND GREATEST(mr.NEW_SOURCE_RECORD_ID, mr.MATCHED_SOURCE_RECORD_ID) = sp.SOURCE_RECORD_ID_B" +
            ")";
        var ir = exec(insSql);
        if (ir) { ir.next(); stratStats.pairs_inserted = ir.getColumnValue(1); totals.pairs_inserted += stratStats.pairs_inserted; }

        // Capture max block size seen for monitoring
        var mbr = exec("SELECT COALESCE(MAX(BLOCK_SZ), 0) FROM _strat_block_sizes");
        if (mbr) { mbr.next(); stratStats.max_block_size = mbr.getColumnValue(1); }

        perStrategy.push(stratStats);
        totals.strategies_run += 1;
    }

    // ─── Step 4: Mark NEW records as processed (ML_BLOCKING_PROCESSED=TRUE) ───
    var stdTables = [
        'STD_POS_TRANSACTION_RAW',
        'STD_LOYALTY_MEMBER_RAW',
        'STD_WEB_CLICKSTREAM_RAW',
        'STD_SHOPIFY_ORDER_RAW'
    ];
    for (var j = 0; j < stdTables.length; j++) {
        exec(
            "UPDATE " + DB + ".SILVER." + stdTables[j] + " " +
            "SET ML_BLOCKING_PROCESSED = TRUE, " +
            "    ML_BLOCKING_PROCESSED_AT = CURRENT_TIMESTAMP() " +
            "WHERE ML_BLOCKING_PROCESSED = FALSE"
        );
    }

    return JSON.stringify({ totals: totals, per_strategy: perStrategy, errors: errors, strategies_loaded: strategies.length });
$$;
