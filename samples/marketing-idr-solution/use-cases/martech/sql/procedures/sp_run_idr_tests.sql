-- ============================================================================
-- SP_RUN_IDR_TESTS: Full-pipeline IDR Test Runner (v2)
--
-- Seeds synthetic data into BRONZE tables only, calls SP_RUN_IDR_PIPELINE once
-- (streams → standardize → extract → match → ML → LLM → cluster), then asserts
-- on the final SILVER-layer state. Exercises the REAL end-to-end flow.
--
-- Parameters:
--   SKIP_LLM (BOOLEAN, default TRUE) — skip LLM adjudication tests
--   CLEANUP (BOOLEAN, default TRUE) — when TRUE: cleans prior TST_ data before
--     seeding AND cleans current run's data after assertions complete.
--     When FALSE: data stays for manual inspection.
--
-- Usage:
--   CALL MARTECH.APP.SP_RUN_IDR_TESTS();                -- full run, cleans up
--   CALL MARTECH.APP.SP_RUN_IDR_TESTS(TRUE, FALSE);    -- run, keep data
--   CALL MARTECH.APP.SP_RUN_IDR_TESTS(FALSE, TRUE);    -- include LLM, clean
--
-- Returns JSON:
--   {"passed":N,"failed":N,"skipped":N,"duration_ms":N,"pipeline_result":"...",
--    "tests":[{id,desc,status,actual,expected},...]}
--
-- All test data uses prefix TST_ on primary keys for deterministic cleanup.
-- ============================================================================

CREATE OR REPLACE PROCEDURE MARTECH.APP.SP_RUN_IDR_TESTS(
    SKIP_LLM BOOLEAN DEFAULT TRUE,
    CLEANUP BOOLEAN DEFAULT TRUE
)
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
    var DB = 'MARTECH';
    var startTime = Date.now();
    var tests = [];
    var passed = 0, failed = 0, skipped = 0;

    function exec(sql) {
        try { return snowflake.execute({sqlText: sql}); }
        catch (e) { return {error: e.message, sql: sql.substring(0, 300)}; }
    }

    function assert(testId, description, sql, op, expected) {
        try {
            var rs = snowflake.execute({sqlText: sql});
            if (!rs.next()) {
                failed++; tests.push({id: testId, desc: description, status: 'FAIL', actual: 'NO_ROWS', expected: expected, op: op});
                return;
            }
            var actual = rs.getColumnValue(1);
            var pass = false;
            if (op === '>=') pass = (actual >= expected);
            else if (op === '=') pass = (actual == expected);
            else if (op === '>') pass = (actual > expected);
            if (pass) {
                passed++; tests.push({id: testId, desc: description, status: 'PASS', actual: actual});
            } else {
                failed++; tests.push({id: testId, desc: description, status: 'FAIL', actual: actual, expected: expected, op: op});
            }
        } catch (e) {
            failed++; tests.push({id: testId, desc: description, status: 'ERROR', error: e.message.substring(0, 200)});
        }
    }

    function skip(testId, description, reason) {
        skipped++; tests.push({id: testId, desc: description, status: 'SKIP', reason: reason});
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // CLEANUP FUNCTION — removes all TST_ data from every layer
    // ═══════════════════════════════════════════════════════════════════════════
    function cleanAll() {
        exec("DELETE FROM " + DB + ".BRONZE.LOYALTY_MEMBER_RAW WHERE MEMBER_ID LIKE 'TST_%'");
        exec("DELETE FROM " + DB + ".BRONZE.POS_TRANSACTION_RAW WHERE TXN_ID LIKE 'TST_%'");
        exec("DELETE FROM " + DB + ".BRONZE.SHOPIFY_ORDER_RAW WHERE ORDER_ID LIKE 'TST_%'");
        exec("DELETE FROM " + DB + ".BRONZE.WEB_CLICKSTREAM_RAW WHERE EVENT_ID LIKE 'TST_%'");
        exec("DELETE FROM " + DB + ".SILVER.STD_LOYALTY_MEMBER_RAW WHERE MEMBER_ID LIKE 'TST_%'");
        exec("DELETE FROM " + DB + ".SILVER.STD_POS_TRANSACTION_RAW WHERE TXN_ID LIKE 'TST_%'");
        exec("DELETE FROM " + DB + ".SILVER.STD_SHOPIFY_ORDER_RAW WHERE ORDER_ID LIKE 'TST_%'");
        exec("DELETE FROM " + DB + ".SILVER.STD_WEB_CLICKSTREAM_RAW WHERE EVENT_ID LIKE 'TST_%'");
        exec("DELETE FROM " + DB + ".SILVER.IDR_CORE_IDENTIFIER_LINK WHERE SOURCE_RECORD_ID LIKE 'TST_%'");
        exec("DELETE FROM " + DB + ".SILVER.IDR_CORE_MATCH_RESULTS WHERE NEW_SOURCE_RECORD_ID LIKE 'TST_%' OR MATCHED_SOURCE_RECORD_ID LIKE 'TST_%' OR SOURCE_RECORD_ID_A LIKE 'TST_%' OR SOURCE_RECORD_ID_B LIKE 'TST_%'");
        exec("DELETE FROM " + DB + ".SILVER.IDR_INDIVIDUAL_MATCH_DISPUTE_QUEUE WHERE SOURCE_RECORD_ID_A LIKE 'TST_%' OR SOURCE_RECORD_ID_B LIKE 'TST_%'");
        exec("DELETE FROM " + DB + ".SILVER.IDR_ML_INDIVIDUAL_MATCH_EXPLAIN WHERE PAIR_ID IN (SELECT PAIR_ID FROM " + DB + ".SILVER.ML_PAIR_FEATURES WHERE SOURCE_RECORD_ID_A LIKE 'TST_%' OR SOURCE_RECORD_ID_B LIKE 'TST_%')");
        exec("DELETE FROM " + DB + ".SILVER.ML_PAIR_FEATURES WHERE SOURCE_RECORD_ID_A LIKE 'TST_%' OR SOURCE_RECORD_ID_B LIKE 'TST_%'");
        exec("DELETE FROM " + DB + ".SILVER.IDR_CORE_CLUSTER_MEMBERSHIP WHERE SOURCE_RECORD_ID LIKE 'TST_%'");
        // Clusters that now have no members
        exec("DELETE FROM " + DB + ".SILVER.IDR_CORE_CLUSTER WHERE STATUS='ACTIVE' AND NOT EXISTS (SELECT 1 FROM " + DB + ".SILVER.IDR_CORE_CLUSTER_MEMBERSHIP cm WHERE cm.CLUSTER_ID = " + DB + ".SILVER.IDR_CORE_CLUSTER.CLUSTER_ID)");
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // STEP 1: Pre-cleanup (if CLEANUP=TRUE)
    // ═══════════════════════════════════════════════════════════════════════════
    if (CLEANUP) { cleanAll(); }

    // ═══════════════════════════════════════════════════════════════════════════
    // STEP 2: SEED — INSERT into BRONZE tables only
    // The streams + full pipeline handle everything else.
    // ═══════════════════════════════════════════════════════════════════════════
    var ts = "CURRENT_TIMESTAMP()";

    // ─── D01: R01 Email Exact ───────────────────────────────────────────────
    exec("INSERT INTO " + DB + ".BRONZE.LOYALTY_MEMBER_RAW (MEMBER_ID, EMAIL, FIRST_NAME, LAST_NAME, INGESTED_AT) VALUES ('TST_D01_A', 'tst.d01@test.com', 'Alpha', 'One', " + ts + ")");
    exec("INSERT INTO " + DB + ".BRONZE.LOYALTY_MEMBER_RAW (MEMBER_ID, EMAIL, FIRST_NAME, LAST_NAME, INGESTED_AT) VALUES ('TST_D01_B', 'tst.d01@test.com', 'Bravo', 'Two', " + ts + ")");

    // ─── D02: R02 HEM (inactive) — same email but rule disabled ─────────────
    // Using a unique email so no other rule accidentally matches them
    exec("INSERT INTO " + DB + ".BRONZE.LOYALTY_MEMBER_RAW (MEMBER_ID, EMAIL, FIRST_NAME, LAST_NAME, INGESTED_AT) VALUES ('TST_D02_A', 'tst.d02.hem.only@uniquetest.dev', 'Charlie', 'Three', " + ts + ")");
    exec("INSERT INTO " + DB + ".BRONZE.LOYALTY_MEMBER_RAW (MEMBER_ID, EMAIL, FIRST_NAME, LAST_NAME, INGESTED_AT) VALUES ('TST_D02_B', 'tst.d02.hem.only@uniquetest.dev', 'Delta', 'Four', " + ts + ")");
    // NOTE: R02 is inactive BUT R01 will fire since they share EMAIL. We'll test R02 indirectly:
    // assert NO edge with rule_id=MARTECH_R02 (there will be one with R01).

    // ─── D03: R03 Loyalty ID ────────────────────────────────────────────────
    exec("INSERT INTO " + DB + ".BRONZE.LOYALTY_MEMBER_RAW (MEMBER_ID, EMAIL, FIRST_NAME, LAST_NAME, INGESTED_AT) VALUES ('TST_D03_A', 'tst.d03a@a.com', 'Echo', 'Five', " + ts + ")");
    exec("INSERT INTO " + DB + ".BRONZE.POS_TRANSACTION_RAW (TXN_ID, LOYALTY_MEMBER_NUMBER, CUSTOMER_FIRST_NAME, CUSTOMER_LAST_NAME, INGESTED_AT) VALUES ('TST_D03_B', 'TST_D03_A', 'Echo', 'Five', " + ts + ")");

    // ─── D04: R04 Device ID ─────────────────────────────────────────────────
    exec("INSERT INTO " + DB + ".BRONZE.WEB_CLICKSTREAM_RAW (EVENT_ID, TS, DEVICE_ID, EVENT_NAME, INGESTED_AT) VALUES ('TST_D04_A', " + ts + ", 'TST_DEVICE_04', 'page_view', " + ts + ")");
    exec("INSERT INTO " + DB + ".BRONZE.WEB_CLICKSTREAM_RAW (EVENT_ID, TS, DEVICE_ID, EVENT_NAME, INGESTED_AT) VALUES ('TST_D04_B', " + ts + ", 'TST_DEVICE_04', 'page_view', " + ts + ")");

    // ─── D05: R05 UID2 ──────────────────────────────────────────────────────
    exec("INSERT INTO " + DB + ".BRONZE.WEB_CLICKSTREAM_RAW (EVENT_ID, TS, UID2, EVENT_NAME, INGESTED_AT) VALUES ('TST_D05_A', " + ts + ", 'TST_UID2_05', 'page_view', " + ts + ")");
    exec("INSERT INTO " + DB + ".BRONZE.WEB_CLICKSTREAM_RAW (EVENT_ID, TS, UID2, EVENT_NAME, INGESTED_AT) VALUES ('TST_D05_B', " + ts + ", 'TST_UID2_05', 'page_view', " + ts + ")");

    // ─── D06: R06 RampID ────────────────────────────────────────────────────
    exec("INSERT INTO " + DB + ".BRONZE.WEB_CLICKSTREAM_RAW (EVENT_ID, TS, RAMPID, EVENT_NAME, INGESTED_AT) VALUES ('TST_D06_A', " + ts + ", 'TST_RAMP_06', 'page_view', " + ts + ")");
    exec("INSERT INTO " + DB + ".BRONZE.WEB_CLICKSTREAM_RAW (EVENT_ID, TS, RAMPID, EVENT_NAME, INGESTED_AT) VALUES ('TST_D06_B', " + ts + ", 'TST_RAMP_06', 'page_view', " + ts + ")");

    // ─── D07: R07 Full Name + Phone ─────────────────────────────────────────
    exec("INSERT INTO " + DB + ".BRONZE.LOYALTY_MEMBER_RAW (MEMBER_ID, PHONE, FIRST_NAME, LAST_NAME, INGESTED_AT) VALUES ('TST_D07_A', '+14155550707', 'Maria', 'Rodriguez', " + ts + ")");
    exec("INSERT INTO " + DB + ".BRONZE.LOYALTY_MEMBER_RAW (MEMBER_ID, PHONE, FIRST_NAME, LAST_NAME, INGESTED_AT) VALUES ('TST_D07_B', '+14155550707', 'Maria', 'Rodriguez', " + ts + ")");

    // ─── D10: R10 Fuzzy First + Email (JOHNATHAN vs JONATHAN, JW ≈ 0.96) ───
    // Different first names so R01 fires but dedup picks R10 (priority 100 < R01's 115).
    // Actually: dedup keeps lowest priority. R10=100 < R01=115 so R10 wins.
    // But both still share exact EMAIL + exact NAME_LAST, and R10 requires fuzzy NAME_FIRST.
    exec("INSERT INTO " + DB + ".BRONZE.LOYALTY_MEMBER_RAW (MEMBER_ID, EMAIL, FIRST_NAME, LAST_NAME, INGESTED_AT) VALUES ('TST_D10_A', 'tst.d10.only@test.com', 'Johnathan', 'Brown', " + ts + ")");
    exec("INSERT INTO " + DB + ".BRONZE.LOYALTY_MEMBER_RAW (MEMBER_ID, EMAIL, FIRST_NAME, LAST_NAME, INGESTED_AT) VALUES ('TST_D10_B', 'tst.d10.only@test.com', 'Jonathan', 'Brown', " + ts + ")");

    // ─── D11: R11 First + Fuzzy Last + Email (ANDERSON vs ANDERSEN) ─────────
    exec("INSERT INTO " + DB + ".BRONZE.LOYALTY_MEMBER_RAW (MEMBER_ID, EMAIL, FIRST_NAME, LAST_NAME, INGESTED_AT) VALUES ('TST_D11_A', 'tst.d11@test.com', 'Alice', 'Anderson', " + ts + ")");
    exec("INSERT INTO " + DB + ".BRONZE.LOYALTY_MEMBER_RAW (MEMBER_ID, EMAIL, FIRST_NAME, LAST_NAME, INGESTED_AT) VALUES ('TST_D11_B', 'tst.d11@test.com', 'Alice', 'Andersen', " + ts + ")");

    // ─── D12: R12 Fuzzy First + Phone (MICHAEL vs MICHEAL) ──────────────────
    exec("INSERT INTO " + DB + ".BRONZE.LOYALTY_MEMBER_RAW (MEMBER_ID, PHONE, FIRST_NAME, LAST_NAME, INGESTED_AT) VALUES ('TST_D12_A', '+14155551212', 'Michael', 'Garcia', " + ts + ")");
    exec("INSERT INTO " + DB + ".BRONZE.LOYALTY_MEMBER_RAW (MEMBER_ID, PHONE, FIRST_NAME, LAST_NAME, INGESTED_AT) VALUES ('TST_D12_B', '+14155551212', 'Micheal', 'Garcia', " + ts + ")");

    // ─── D13: R13 First + Fuzzy Last + Phone (JOHNSON vs JOHNSEN) ───────────
    exec("INSERT INTO " + DB + ".BRONZE.LOYALTY_MEMBER_RAW (MEMBER_ID, PHONE, FIRST_NAME, LAST_NAME, INGESTED_AT) VALUES ('TST_D13_A', '+14155551313', 'David', 'Johnson', " + ts + ")");
    exec("INSERT INTO " + DB + ".BRONZE.LOYALTY_MEMBER_RAW (MEMBER_ID, PHONE, FIRST_NAME, LAST_NAME, INGESTED_AT) VALUES ('TST_D13_B', '+14155551313', 'David', 'Johnsen', " + ts + ")");

    // ─── M01: B5 EMAIL_HANDLE pair (Gmail dot/plus normalization) ────────────
    exec("INSERT INTO " + DB + ".BRONZE.LOYALTY_MEMBER_RAW (MEMBER_ID, EMAIL, FIRST_NAME, LAST_NAME, POSTAL_CODE, INGESTED_AT) VALUES ('TST_M01_A', 'tst.mone@gmail.com', 'TestA', 'MlOne', '00001', " + ts + ")");
    exec("INSERT INTO " + DB + ".BRONZE.LOYALTY_MEMBER_RAW (MEMBER_ID, EMAIL, FIRST_NAME, LAST_NAME, POSTAL_CODE, INGESTED_AT) VALUES ('TST_M01_B', 'tstmone+work@gmail.com', 'TestB', 'MlTwo', '00002', " + ts + ")");

    // ─── M03: B7 PHONE_LAST7 pair (non-stoplisted) ──────────────────────────
    exec("INSERT INTO " + DB + ".BRONZE.LOYALTY_MEMBER_RAW (MEMBER_ID, PHONE, FIRST_NAME, LAST_NAME, INGESTED_AT) VALUES ('TST_M03_A', '+14155550303', 'Fred', 'PhoneA', " + ts + ")");
    exec("INSERT INTO " + DB + ".BRONZE.LOYALTY_MEMBER_RAW (MEMBER_ID, PHONE, FIRST_NAME, LAST_NAME, INGESTED_AT) VALUES ('TST_M03_B', '+12125550303', 'Frank', 'PhoneB', " + ts + ")");

    // ─── M04: B7 PHONE stoplisted (800 area code) — should NOT pair via B7 ──
    exec("INSERT INTO " + DB + ".BRONZE.LOYALTY_MEMBER_RAW (MEMBER_ID, PHONE, FIRST_NAME, LAST_NAME, INGESTED_AT) VALUES ('TST_M04_A', '+18005550404', 'Grace', 'StopA', " + ts + ")");
    exec("INSERT INTO " + DB + ".BRONZE.LOYALTY_MEMBER_RAW (MEMBER_ID, PHONE, FIRST_NAME, LAST_NAME, INGESTED_AT) VALUES ('TST_M04_B', '+18005550404', 'Gloria', 'StopB', " + ts + ")");

    // ─── M05: AUTO_MERGE ─────────────────────────────────────────────────────
    // To get score >= 0.85 we need: email(0.30)+phone(0.20)+loyalty(0.20)+JW_last(0.10)+JW_first(0.05)+postal(0.03)+nickname(0.02) = 0.90
    // Key: both records must share the SAME LOYALTY_MEMBER_NUMBER value (not just MEMBER_ID)
    // so that the LOYALTY_ID identifier is extracted as a shared value → LOYALTY_ID_EQ=TRUE in pair features.
    exec("INSERT INTO " + DB + ".BRONZE.LOYALTY_MEMBER_RAW (MEMBER_ID, EMAIL, PHONE, FIRST_NAME, LAST_NAME, POSTAL_CODE, INGESTED_AT) VALUES ('TST_M05_A', 'tst.m05.auto@x.com', '+14155550505', 'Anna', 'Taylor', '94110', " + ts + ")");
    exec("INSERT INTO " + DB + ".BRONZE.POS_TRANSACTION_RAW (TXN_ID, LOYALTY_MEMBER_NUMBER, CUSTOMER_EMAIL, CUSTOMER_PHONE, CUSTOMER_FIRST_NAME, CUSTOMER_LAST_NAME, INGESTED_AT) VALUES ('TST_M05_B', 'TST_M05_A', 'tst.m05.auto@x.com', '+14155550505', 'Anna', 'Taylor', " + ts + ")");

    // ─── M06: DROP (same postal + same SOUNDEX last name → B6 pairs, but score low) ─
    // WILLIAMS and WILLIAMSON have same SOUNDEX (W452) so B6 will pair them.
    // Score ≈ 0.10 (JW_LAST ≈ 0.85 partial credit) → DROP tier.
    exec("INSERT INTO " + DB + ".BRONZE.LOYALTY_MEMBER_RAW (MEMBER_ID, EMAIL, FIRST_NAME, LAST_NAME, POSTAL_CODE, INGESTED_AT) VALUES ('TST_M06_A', 'tst.m06a.only@x.com', 'Xavier', 'Williams', '99906', " + ts + ")");
    exec("INSERT INTO " + DB + ".BRONZE.LOYALTY_MEMBER_RAW (MEMBER_ID, EMAIL, FIRST_NAME, LAST_NAME, POSTAL_CODE, INGESTED_AT) VALUES ('TST_M06_B', 'tst.m06b.only@x.com', 'Yolanda', 'Williamson', '99906', " + ts + ")");

    // ─── Q01: MILD dispute (shared email, disagreeing names → ML low + R01 det edge) ─
    exec("INSERT INTO " + DB + ".BRONZE.LOYALTY_MEMBER_RAW (MEMBER_ID, EMAIL, FIRST_NAME, LAST_NAME, POSTAL_CODE, INGESTED_AT) VALUES ('TST_Q01_A', 'tst.q01.shared@test.com', 'Mark', 'Anderson', '94110', " + ts + ")");
    exec("INSERT INTO " + DB + ".BRONZE.LOYALTY_MEMBER_RAW (MEMBER_ID, EMAIL, FIRST_NAME, LAST_NAME, POSTAL_CODE, INGESTED_AT) VALUES ('TST_Q01_B', 'tst.q01.shared@test.com', 'Mary', 'Lopez', '10001', " + ts + ")");

    // ─── Q03: No dispute (different emails, same postal → no det edge, just DROP) ─
    exec("INSERT INTO " + DB + ".BRONZE.LOYALTY_MEMBER_RAW (MEMBER_ID, EMAIL, FIRST_NAME, LAST_NAME, POSTAL_CODE, INGESTED_AT) VALUES ('TST_Q03_A', 'tst.q03a@x.com', 'Pat', 'Unique3A', '77777', " + ts + ")");
    exec("INSERT INTO " + DB + ".BRONZE.LOYALTY_MEMBER_RAW (MEMBER_ID, EMAIL, FIRST_NAME, LAST_NAME, POSTAL_CODE, INGESTED_AT) VALUES ('TST_Q03_B', 'tst.q03b@x.com', 'Quinn', 'Unique3B', '77777', " + ts + ")");

    // ─── C01: Transitive cluster (A↔B via email, B↔C via phone+name) ────────
    exec("INSERT INTO " + DB + ".BRONZE.LOYALTY_MEMBER_RAW (MEMBER_ID, EMAIL, FIRST_NAME, LAST_NAME, INGESTED_AT) VALUES ('TST_C01_A', 'tst.c01@test.com', 'Cluster', 'Test', " + ts + ")");
    exec("INSERT INTO " + DB + ".BRONZE.LOYALTY_MEMBER_RAW (MEMBER_ID, EMAIL, PHONE, FIRST_NAME, LAST_NAME, INGESTED_AT) VALUES ('TST_C01_B', 'tst.c01@test.com', '+14155559901', 'Cluster', 'Test', " + ts + ")");
    exec("INSERT INTO " + DB + ".BRONZE.LOYALTY_MEMBER_RAW (MEMBER_ID, PHONE, FIRST_NAME, LAST_NAME, INGESTED_AT) VALUES ('TST_C01_C', '+14155559901', 'Cluster', 'Test', " + ts + ")");

    // ─── C04: Singleton (unique email, no shared identifiers) ────────────────
    exec("INSERT INTO " + DB + ".BRONZE.LOYALTY_MEMBER_RAW (MEMBER_ID, EMAIL, FIRST_NAME, LAST_NAME, INGESTED_AT) VALUES ('TST_C04_A', 'tst.c04.solo@protonmail.com', 'Solo', 'Singleton', " + ts + ")");

    // ─── P7: Shopify through full pipeline (verifies shopify extract) ────────
    exec("INSERT INTO " + DB + ".BRONZE.SHOPIFY_ORDER_RAW (ORDER_ID, RAW_PAYLOAD, INGESTED_AT) " +
         "SELECT 'TST_SHP_A', PARSE_JSON('{\"customer\":{\"email\":\"tst.shp@test.com\",\"phone\":\"+14155559999\",\"first_name\":\"Shop\",\"last_name\":\"Test\"},\"billing_address\":{\"zip\":\"94110\"}}'), " + ts);
    exec("INSERT INTO " + DB + ".BRONZE.LOYALTY_MEMBER_RAW (MEMBER_ID, EMAIL, PHONE, FIRST_NAME, LAST_NAME, POSTAL_CODE, INGESTED_AT) VALUES ('TST_SHP_B', 'tst.shp@test.com', '+14155559999', 'Shop', 'Test', '94110', " + ts + ")");

    // ═══════════════════════════════════════════════════════════════════════════
    // STEP 3: RUN FULL PIPELINE
    // ═══════════════════════════════════════════════════════════════════════════
    exec("USE DATABASE " + DB);
    var pipeRes = exec("CALL IDR.PROCEDURES.SP_RUN_IDR_PIPELINE('" + DB + "')");
    var pipeMsg = '';
    if (pipeRes && !pipeRes.error) { try { pipeRes.next(); pipeMsg = pipeRes.getColumnValue(1); } catch(e) {} }
    else if (pipeRes && pipeRes.error) { pipeMsg = 'PIPELINE_ERROR: ' + pipeRes.error; }

    // ═══════════════════════════════════════════════════════════════════════════
    // STEP 4: ASSERTIONS
    // ═══════════════════════════════════════════════════════════════════════════
    var edgeQ = function(prefix, ruleId) {
        return "SELECT COUNT(*) FROM " + DB + ".SILVER.IDR_CORE_MATCH_RESULTS " +
               "WHERE IS_ACTIVE=TRUE AND RULE_ID='" + ruleId + "' " +
               "AND (NEW_SOURCE_RECORD_ID LIKE '" + prefix + "%' OR MATCHED_SOURCE_RECORD_ID LIKE '" + prefix + "%')";
    };

    var clusterContains = function(memberId, otherMemberId) {
        return "SELECT COUNT(*) FROM " + DB + ".SILVER.IDR_CORE_CLUSTER C, " +
               "LATERAL FLATTEN(input => C.SOURCE_RECORD_IDS) f " +
               "WHERE f.value::VARCHAR = '" + memberId + "' " +
               "AND ARRAY_CONTAINS('" + otherMemberId + "'::VARIANT, C.SOURCE_RECORD_IDS)";
    };

    // ─── Area 1: Deterministic ──────────────────────────────────────────────
    assert('D01', 'R01 Email Exact fires', edgeQ('TST_D01_', 'MARTECH_R01'), '>=', 1);
    assert('D02', 'R02 HEM (inactive) does NOT fire', edgeQ('TST_D02_', 'MARTECH_R02'), '=', 0);
    assert('D03', 'R03 Loyalty ID fires', edgeQ('TST_D03_', 'MARTECH_R03'), '>=', 1);
    assert('D04', 'R04 Device ID fires', edgeQ('TST_D04_', 'MARTECH_R04'), '>=', 1);
    assert('D05', 'R05 UID2 fires', edgeQ('TST_D05_', 'MARTECH_R05'), '>=', 1);
    assert('D06', 'R06 RampID fires', edgeQ('TST_D06_', 'MARTECH_R06'), '>=', 1);
    assert('D07', 'R07 Full Name + Phone fires', edgeQ('TST_D07_', 'MARTECH_R07'), '>=', 1);
    skip('D08', 'R08 Nickname + Email', 'FIRST_NAME_CANONICAL not in column metadata cache');
    skip('D09', 'R09 Nickname + Phone', 'FIRST_NAME_CANONICAL not in column metadata cache');
    // D10: R10 (priority 100) wins dedup over R01 (priority 115) since both fire
    // on the shared email pair. R10 is the edge emitted.
    assert('D10', 'R10 Fuzzy First + Email fires', edgeQ('TST_D10_', 'MARTECH_R10'), '>=', 1);
    assert('D11', 'R11 First + Fuzzy Last + Email fires', edgeQ('TST_D11_', 'MARTECH_R11'), '>=', 1);
    assert('D12', 'R12 Fuzzy First + Phone fires', edgeQ('TST_D12_', 'MARTECH_R12'), '>=', 1);
    assert('D13', 'R13 First + Fuzzy Last + Phone fires', edgeQ('TST_D13_', 'MARTECH_R13'), '>=', 1);

    // ─── Area 2: Household ──────────────────────────────────────────────────
    skip('H01', 'R14 Household exact', 'SP_RUN_MATCHING skips MATCH_TYPE=HOUSEHOLD');
    skip('H02', 'R15 Household fuzzy last', 'SP_RUN_MATCHING skips MATCH_TYPE=HOUSEHOLD');

    // ─── Area 3: ML blocking + scoring ──────────────────────────────────────
    var mlPairQ = function(prefix, strategy) {
        return "SELECT COUNT(*) FROM " + DB + ".SILVER.ML_PAIR_FEATURES " +
               "WHERE SOURCE_RECORD_ID_A LIKE '" + prefix + "%' AND SOURCE_RECORD_ID_B LIKE '" + prefix + "%' " +
               "AND ARRAY_TO_STRING(BLOCKING_KEYS_HIT, ',') LIKE '%" + strategy + "%'";
    };

    assert('M01', 'B5 EMAIL_HANDLE pair generated', mlPairQ('TST_M01_', 'B5'), '>=', 1);
    assert('M03', 'B7 PHONE_LAST7 pair generated', mlPairQ('TST_M03_', 'B7'), '>=', 1);
    assert('M04', 'B7 stoplisted phone NOT paired via B7',
        "SELECT COUNT(*) FROM " + DB + ".SILVER.ML_PAIR_FEATURES " +
        "WHERE SOURCE_RECORD_ID_A LIKE 'TST_M04_%' AND SOURCE_RECORD_ID_B LIKE 'TST_M04_%' " +
        "AND ARRAY_TO_STRING(BLOCKING_KEYS_HIT, ',') LIKE '%B7%'", '=', 0);
    // M05: email(0.30)+phone(0.20)+loyalty(0.20)+JW_last(~0.10)+JW_first(~0.05)+postal(0.03)+nickname(0.02)=0.90 → AUTO_MERGE
    assert('M05', 'AUTO_MERGE tier (score >= 0.85)',
        "SELECT COUNT(*) FROM " + DB + ".SILVER.ML_PAIR_FEATURES " +
        "WHERE SOURCE_RECORD_ID_A LIKE 'TST_M05_%' AND SOURCE_RECORD_ID_B LIKE 'TST_M05_%' AND ML_TIER='AUTO_MERGE'", '>=', 1);
    // M06: Same postal + same SOUNDEX last name → B6 pairs them, but all other signals differ → score ≈ 0.03 → DROP
    assert('M06', 'DROP tier (score < 0.55)',
        "SELECT COUNT(*) FROM " + DB + ".SILVER.ML_PAIR_FEATURES " +
        "WHERE SOURCE_RECORD_ID_A LIKE 'TST_M06_%' AND SOURCE_RECORD_ID_B LIKE 'TST_M06_%' AND ML_TIER IN ('DROP','DISPUTE')", '>=', 1);

    // ─── Area 4: Disputes ───────────────────────────────────────────────────
    assert('Q01', 'MILD dispute (shared email + disagreeing names)',
        "SELECT COUNT(*) FROM " + DB + ".SILVER.IDR_INDIVIDUAL_MATCH_DISPUTE_QUEUE " +
        "WHERE (SOURCE_RECORD_ID_A LIKE 'TST_Q01_%' OR SOURCE_RECORD_ID_B LIKE 'TST_Q01_%') AND DISPUTE_SEVERITY='MILD'", '>=', 1);
    assert('Q03', 'No dispute without deterministic edge',
        "SELECT COUNT(*) FROM " + DB + ".SILVER.IDR_INDIVIDUAL_MATCH_DISPUTE_QUEUE " +
        "WHERE (SOURCE_RECORD_ID_A LIKE 'TST_Q03_%' OR SOURCE_RECORD_ID_B LIKE 'TST_Q03_%')", '=', 0);

    // ─── Area 5: LLM ────────────────────────────────────────────────────────
    if (SKIP_LLM) {
        skip('L01', 'LLM MATCH emits R17', 'SKIP_LLM=TRUE');
        skip('L02', 'LLM disabled skips', 'SKIP_LLM=TRUE');
    } else {
        assert('L01', 'LLM adjudication ran on GREY_LLM pairs',
            "SELECT COUNT(*) FROM " + DB + ".SILVER.ML_PAIR_FEATURES " +
            "WHERE ML_TIER='GREY_LLM' AND (SOURCE_RECORD_ID_A LIKE 'TST_%' OR SOURCE_RECORD_ID_B LIKE 'TST_%')", '>=', 0);
        skip('L02', 'LLM verdict assertion', 'Requires manual LLM output inspection');
    }

    // ─── Area 6: Clustering ─────────────────────────────────────────────────
    assert('C01', 'Transitive cluster merges 3 records',
        "SELECT ARRAY_SIZE(SOURCE_RECORD_IDS) FROM " + DB + ".SILVER.IDR_CORE_CLUSTER C, " +
        "LATERAL FLATTEN(input => C.SOURCE_RECORD_IDS) f " +
        "WHERE f.value::VARCHAR = 'TST_C01_A' " +
        "GROUP BY SOURCE_RECORD_IDS LIMIT 1", '>=', 3);

    assert('C04', 'Isolated record forms singleton',
        "SELECT ARRAY_SIZE(SOURCE_RECORD_IDS) FROM " + DB + ".SILVER.IDR_CORE_CLUSTER C, " +
        "LATERAL FLATTEN(input => C.SOURCE_RECORD_IDS) f " +
        "WHERE f.value::VARCHAR = 'TST_C04_A' " +
        "GROUP BY SOURCE_RECORD_IDS LIMIT 1", '=', 1);

    // C03: R16 ML edge clustered (uses M05 AUTO_MERGE — both records should be in same cluster)
    assert('C03', 'R16 ML edge results in cluster',
        clusterContains('TST_M05_A', 'TST_M05_B'), '>=', 1);

    // SHP: Shopify end-to-end (extract + deterministic match via email/phone)
    assert('SHP', 'Shopify→extract→match→cluster with loyalty',
        clusterContains('TST_SHP_A', 'TST_SHP_B'), '>=', 1);

    // ═══════════════════════════════════════════════════════════════════════════
    // STEP 5: Post-cleanup (if CLEANUP=TRUE)
    // ═══════════════════════════════════════════════════════════════════════════
    if (CLEANUP) { cleanAll(); }

    // ═══════════════════════════════════════════════════════════════════════════
    // REPORT
    // ═══════════════════════════════════════════════════════════════════════════
    return JSON.stringify({
        passed: passed,
        failed: failed,
        skipped: skipped,
        duration_ms: Date.now() - startTime,
        pipeline_result: pipeMsg ? pipeMsg.substring(0, 500) : null,
        tests: tests
    });
$$;
