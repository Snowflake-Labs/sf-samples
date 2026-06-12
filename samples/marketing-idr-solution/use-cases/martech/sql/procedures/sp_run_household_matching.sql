-- ============================================================================
-- Martech: APP.SP_RUN_HOUSEHOLD_MATCHING
--
-- Cluster-grain mirror of engine SP_RUN_MATCHING.
-- Reads HOUSEHOLD-tier rules from CONFIG.IDR_MATCHING_RULES and dynamically
-- synthesizes the JOIN SQL per rule, joining DT_CUSTOMER_PROFILE to itself.
-- Output: IDR_CORE_HOUSEHOLD_MATCH_RESULTS (one row per cluster-pair edge).
--
-- Mirrors the engine's:
--   - LANGUAGE JAVASCRIPT, EXECUTE AS CALLER
--   - process state row (process_name='SP_RUN_HOUSEHOLD_MATCHING')
--   - incremental batching via batchStartTime
--   - stale-match deactivation
--   - rule loop: anchor + exact_match_fields + fuzzy_match_field
--                with CANONICAL / JARO_WINKLER / NONE branches
--   - scoreExpr override on fuzzy
--   - require_cross_source / require_different_record flags
--   - priority dedup via ROW_NUMBER OVER (PARTITION BY pair ORDER BY priority)
--   - NOT EXISTS guard against already-active edges
--
-- Differences from the engine (cluster grain vs record grain):
--   - No source-record pivot — DT_CUSTOMER_PROFILE is already pivoted at
--     CLUSTER_ID grain. Identifier-type names from the rule config are
--     translated to DT physical column names via a small mapping dict.
--   - "_new_clusters" plays the role of "_new_sources": clusters with
--     UPDATED_AT >= batchStartTime, scoping the join filter.
--   - VECTOR rules are not expected at household grain; pattern preserved
--     with a warn log + skip if encountered.
-- ============================================================================

CREATE OR REPLACE PROCEDURE IDENTIFIER('&{deployment_db}.APP.SP_RUN_HOUSEHOLD_MATCHING')(DEPLOYMENT_DB VARCHAR)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
    var DB = DEPLOYMENT_DB;
    var q  = String.fromCharCode(39);

    snowflake.execute({sqlText: "USE DATABASE " + DB});
    snowflake.execute({sqlText: "USE SCHEMA SILVER"});

    // -------------------------------------------------------------------
    // STEP 0a: force a synchronous DT refresh so DT_CUSTOMER_PROFILE reflects
    // the clusters the IDR pipeline just produced. DT_CUSTOMER_PROFILE has a
    // ~5-min TARGET_LAG; when this SP runs immediately after clustering, the DT
    // would otherwise be stale and household matching silently finds nothing.
    // -------------------------------------------------------------------
    try {
        snowflake.execute({sqlText: "ALTER DYNAMIC TABLE " + DB + ".GOLD.DT_CUSTOMER_PROFILE REFRESH"});
    } catch (e) {
        snowflake.log("warn", "SP_RUN_HOUSEHOLD_MATCHING: DT refresh failed, continuing with current DT state: " + e.message);
    }

    // -------------------------------------------------------------------
    // Identifier-type -> DT_CUSTOMER_PROFILE physical column name.
    // Config (IDR_MATCHING_RULES) uses identifier_type names; this dict is
    // the single place to extend if DT columns are renamed or new household
    // attributes are added.
    // -------------------------------------------------------------------
    var DT_COL = {
        'NAME_LAST':       'PRIMARY_LAST_NAME',
        'NAME_FIRST':      'PRIMARY_FIRST_NAME',
        'POSTAL_CODE':     'BILLING_POSTAL',
        'STREET_ADDRESS':  'BILLING_STREET',
        'CITY':            'BILLING_CITY',
        'STATE':           'BILLING_STATE',
        'EMAIL':           'PRIMARY_EMAIL',
        'PHONE':           'PRIMARY_PHONE',
        'LOYALTY_ID':      'LOYALTY_MEMBER_ID'
    };

    function mapCol(name) {
        if (!name) return null;
        return DT_COL[name.trim().toUpperCase()] || null;
    }

    // -------------------------------------------------------------------
    // STEP 0: process state — read batchStartTime
    // -------------------------------------------------------------------
    var batchTimeResult = snowflake.execute({sqlText:
        "SELECT COALESCE( " +
        "  (SELECT TO_VARCHAR(LAST_RUN_AT, 'YYYY-MM-DD HH24:MI:SS.FF3') " +
        "   FROM " + DB + ".SILVER.IDR_CORE_PROCESS_STATE " +
        "   WHERE PROCESS_NAME = 'SP_RUN_HOUSEHOLD_MATCHING' AND LAST_RUN_STATUS = 'SUCCESS'), " +
        "  '2000-01-01 00:00:00.000' " +
        ")"
    });
    batchTimeResult.next();
    var batchStartTime = batchTimeResult.getColumnValue(1);

    // -------------------------------------------------------------------
    // STEP 1: count new / changed clusters since last successful run
    // -------------------------------------------------------------------
    var newCountResult = snowflake.execute({sqlText:
        "SELECT COUNT(*) FROM " + DB + ".SILVER.IDR_CORE_CLUSTER " +
        "WHERE STATUS = 'ACTIVE' AND COALESCE(UPDATED_AT, CREATED_AT) >= '" + batchStartTime + "'"
    });
    newCountResult.next();
    var newClusterCount = newCountResult.getColumnValue(1);

    if (newClusterCount === 0) {
        snowflake.log("info", "SP_RUN_HOUSEHOLD_MATCHING: no new/changed clusters since " + batchStartTime);
        return "No new clusters to match";
    }

    snowflake.log("info", "SP_RUN_HOUSEHOLD_MATCHING: processing " + newClusterCount + " new/changed clusters since " + batchStartTime);

    // -------------------------------------------------------------------
    // STEP 2: deactivate stale edges (either side cluster no longer ACTIVE)
    // -------------------------------------------------------------------
    snowflake.execute({sqlText:
        "UPDATE " + DB + ".SILVER.IDR_CORE_HOUSEHOLD_MATCH_RESULTS " +
        "SET IS_ACTIVE = FALSE, UPDATED_AT = CURRENT_TIMESTAMP() " +
        "WHERE IS_ACTIVE = TRUE AND ( " +
        "    A_CLUSTER_ID NOT IN (SELECT CLUSTER_ID FROM " + DB + ".SILVER.IDR_CORE_CLUSTER WHERE STATUS = 'ACTIVE') " +
        " OR B_CLUSTER_ID NOT IN (SELECT CLUSTER_ID FROM " + DB + ".SILVER.IDR_CORE_CLUSTER WHERE STATUS = 'ACTIVE'))"
    });

    // -------------------------------------------------------------------
    // STEP 3: temp table _new_clusters — drives the (a IN _new OR b IN _new)
    // filter in every rule, mirroring engine's _new_sources pattern.
    // -------------------------------------------------------------------
    snowflake.execute({sqlText:
        "CREATE OR REPLACE TEMPORARY TABLE _new_clusters AS " +
        "SELECT CLUSTER_ID FROM " + DB + ".SILVER.IDR_CORE_CLUSTER " +
        "WHERE STATUS = 'ACTIVE' AND COALESCE(UPDATED_AT, CREATED_AT) >= '" + batchStartTime + "'"
    });

    // -------------------------------------------------------------------
    // STEP 4: read household rules and synthesize per-rule SELECTs.
    // -------------------------------------------------------------------
    var rulesResult = snowflake.execute({sqlText:
        "SELECT rule_id, rule_name, match_type, anchor_field, exact_match_fields, " +
        "       fuzzy_match_field, fuzzy_algorithm, fuzzy_threshold, base_score, " +
        "       require_cross_source, require_different_record, rule_priority " +
        "FROM " + DB + ".CONFIG.IDR_MATCHING_RULES " +
        "WHERE match_type = 'HOUSEHOLD' AND is_active = TRUE " +
        "ORDER BY rule_priority"
    });

    var unionParts = [];
    while (rulesResult.next()) {
        var ruleId             = rulesResult.getColumnValue(1);
        var ruleName           = rulesResult.getColumnValue(2);
        var matchType          = rulesResult.getColumnValue(3);
        var anchorField        = rulesResult.getColumnValue(4);
        var exactMatchFields   = rulesResult.getColumnValue(5);
        var fuzzyMatchField    = rulesResult.getColumnValue(6);
        var fuzzyAlgorithm     = rulesResult.getColumnValue(7) || "NONE";
        var fuzzyThreshold     = rulesResult.getColumnValue(8) || 0.85;
        var baseScore          = rulesResult.getColumnValue(9) || 0.9;
        var requireCrossSource = rulesResult.getColumnValue(10);
        // requireDifferentRecord is implicit at cluster grain via CLUSTER_ID < CLUSTER_ID
        var rulePriority       = rulesResult.getColumnValue(12);

        if (matchType === "VECTOR") {
            snowflake.log("warn", "SP_RUN_HOUSEHOLD_MATCHING: rule " + ruleId + " has match_type=VECTOR; not supported at household grain, skipped");
            continue;
        }

        var anchorCol = mapCol(anchorField);
        if (!anchorCol) {
            snowflake.log("warn", "SP_RUN_HOUSEHOLD_MATCHING: rule " + ruleId + " skipped — anchor_field '" + anchorField + "' has no DT_COL mapping");
            continue;
        }

        var conds = [];
        conds.push("a." + anchorCol + " = b." + anchorCol);
        conds.push("a." + anchorCol + " IS NOT NULL");

        if (exactMatchFields) {
            var exactFields = exactMatchFields.split(',').map(function(s){return s.trim().toUpperCase();}).filter(function(c){return c;});
            for (var i = 0; i < exactFields.length; i++) {
                var col = mapCol(exactFields[i]);
                if (!col) {
                    snowflake.log("warn", "SP_RUN_HOUSEHOLD_MATCHING: rule " + ruleId + " skipping unmapped exact field '" + exactFields[i] + "'");
                    continue;
                }
                conds.push("a." + col + " = b." + col);
                conds.push("a." + col + " IS NOT NULL");
            }
        }

        var scoreExpr = baseScore.toString();
        var jwExpr    = "NULL";

        if (fuzzyMatchField && fuzzyAlgorithm !== "NONE") {
            var fuzzyCols = fuzzyMatchField.split(',').map(function(s){return s.trim().toUpperCase();}).filter(function(c){return c;});
            if (fuzzyCols.length > 0) {
                if (fuzzyAlgorithm === "CANONICAL") {
                    // For household rules canonical path is unusual; supported for parity.
                    for (var fc = 0; fc < fuzzyCols.length; fc++) {
                        var ccol = mapCol(fuzzyCols[fc]);
                        if (!ccol) continue;
                        conds.push("a." + ccol + " = b." + ccol);
                        conds.push("a." + ccol + " IS NOT NULL");
                        break;
                    }
                } else if (fuzzyAlgorithm === "JARO_WINKLER") {
                    var jaroParts = [];
                    for (var j = 0; j < fuzzyCols.length; j++) {
                        var jcol = mapCol(fuzzyCols[j]);
                        if (!jcol) continue;
                        conds.push(DB + ".SILVER.JARO_WINKLER_SIMILARITY(a." + jcol + ", b." + jcol + ") >= " + fuzzyThreshold);
                        // Avoid double-counting an exact-match-eligible pair on the fuzzy path.
                        conds.push("a." + jcol + " != b." + jcol);
                        jaroParts.push(DB + ".SILVER.JARO_WINKLER_SIMILARITY(a." + jcol + ", b." + jcol + ")");
                    }
                    if (jaroParts.length > 0) {
                        scoreExpr = jaroParts.length === 1 ? jaroParts[0] : "((" + jaroParts.join(" + ") + ") / " + jaroParts.length + ")";
                        // Capture the LAST-name JW into JW_LAST when the fuzzy field is NAME_LAST.
                        if (fuzzyCols.length === 1 && fuzzyCols[0] === 'NAME_LAST') {
                            jwExpr = jaroParts[0];
                        }
                    }
                }
            }
        }

        // require_cross_source has no analog at cluster grain (cross-cluster
        // is always required); kept here for parity if a future rule uses it.
        if (requireCrossSource) {
            // no-op at cluster grain; both sides are clusters by construction
        }

        conds.push("a.CLUSTER_ID < b.CLUSTER_ID");
        conds.push("(a.CLUSTER_ID IN (SELECT CLUSTER_ID FROM _new_clusters) OR b.CLUSTER_ID IN (SELECT CLUSTER_ID FROM _new_clusters))");

        var selectSql =
            "SELECT " +
            "  a.CLUSTER_ID AS A_CLUSTER_ID, " +
            "  b.CLUSTER_ID AS B_CLUSTER_ID, " +
            "  " + q + ruleId   + q + " AS RULE_ID, " +
            "  " + q + ruleName + q + " AS RULE_NAME, " +
            "  " + scoreExpr + " AS MATCH_SCORE, " +
            "  " + jwExpr    + " AS JW_LAST, " +
            "  CAST(a." + anchorCol + " AS VARCHAR) AS MATCHED_ON, " +
            "  " + rulePriority + " AS RULE_PRIORITY " +
            "FROM " + DB + ".GOLD.DT_CUSTOMER_PROFILE a " +
            "JOIN " + DB + ".GOLD.DT_CUSTOMER_PROFILE b ON " + conds.join(" AND ");
        unionParts.push(selectSql);
    }

    if (unionParts.length === 0) {
        snowflake.execute({sqlText: "DROP TABLE IF EXISTS _new_clusters"});
        return "No active HOUSEHOLD rules";
    }

    // -------------------------------------------------------------------
    // STEP 5: insert with priority dedup + NOT EXISTS guard
    // -------------------------------------------------------------------
    var matchSql =
        "INSERT INTO " + DB + ".SILVER.IDR_CORE_HOUSEHOLD_MATCH_RESULTS " +
        "(A_CLUSTER_ID, B_CLUSTER_ID, RULE_ID, RULE_NAME, MATCH_SCORE, JW_LAST, MATCHED_ON, IS_ACTIVE, IS_CURRENT) " +
        "WITH all_matches AS (" + unionParts.join(" UNION ALL ") + "), " +
        "ranked AS ( " +
        "  SELECT A_CLUSTER_ID, B_CLUSTER_ID, RULE_ID, RULE_NAME, MATCH_SCORE, JW_LAST, MATCHED_ON, " +
        "    ROW_NUMBER() OVER (PARTITION BY A_CLUSTER_ID, B_CLUSTER_ID ORDER BY RULE_PRIORITY ASC, MATCH_SCORE DESC) AS rn " +
        "  FROM all_matches " +
        ") " +
        "SELECT A_CLUSTER_ID, B_CLUSTER_ID, RULE_ID, RULE_NAME, MATCH_SCORE, JW_LAST, MATCHED_ON, TRUE, TRUE " +
        "FROM ranked WHERE rn = 1 " +
        "AND NOT EXISTS ( " +
        "  SELECT 1 FROM " + DB + ".SILVER.IDR_CORE_HOUSEHOLD_MATCH_RESULTS mr " +
        "  WHERE mr.A_CLUSTER_ID = ranked.A_CLUSTER_ID AND mr.B_CLUSTER_ID = ranked.B_CLUSTER_ID AND mr.IS_ACTIVE = TRUE)";

    snowflake.log("debug", "SP_RUN_HOUSEHOLD_MATCHING matchSql: " + matchSql);
    var matchResult = snowflake.execute({sqlText: matchSql});
    var newMatches = 0;
    try { newMatches = matchResult.getNumRowsAffected(); } catch (e) {}

    // -------------------------------------------------------------------
    // STEP 6: update process state
    //
    // Watermark = MAX cluster timestamp ACTUALLY VISIBLE in the DT (not wall
    // clock). A cluster created just before NOW() but not yet materialized in
    // DT_CUSTOMER_PROFILE must remain "new" for the next run; advancing the
    // watermark to CURRENT_TIMESTAMP() would skip it permanently. We force-
    // refreshed the DT in STEP 0a, so DT-visible == current, but deriving the
    // watermark from observed DT content is the robust guarantee against the
    // DT-lag race.
    // -------------------------------------------------------------------
    var wmResult = snowflake.execute({sqlText:
        "SELECT COALESCE( " +
        "  TO_VARCHAR(MAX(COALESCE(c.UPDATED_AT, c.CREATED_AT)), 'YYYY-MM-DD HH24:MI:SS.FF3'), " +
        "  '" + batchStartTime + "' " +
        ") " +
        "FROM " + DB + ".SILVER.IDR_CORE_CLUSTER c " +
        "WHERE c.STATUS = 'ACTIVE' " +
        "  AND c.CLUSTER_ID IN (SELECT CLUSTER_ID FROM " + DB + ".GOLD.DT_CUSTOMER_PROFILE)"
    });
    wmResult.next();
    var newWatermark = wmResult.getColumnValue(1);

    snowflake.execute({sqlText:
        "MERGE INTO " + DB + ".SILVER.IDR_CORE_PROCESS_STATE tgt " +
        "USING (SELECT 'SP_RUN_HOUSEHOLD_MATCHING' AS process_name) src " +
        "ON tgt.process_name = src.process_name " +
        "WHEN MATCHED THEN UPDATE SET " +
        "  last_run_at = TO_TIMESTAMP('" + newWatermark + "', 'YYYY-MM-DD HH24:MI:SS.FF3'), last_run_status = 'SUCCESS', " +
        "  last_run_details = PARSE_JSON('{\"new_matches\": " + newMatches + ", \"new_clusters\": " + newClusterCount + "}') " +
        "WHEN NOT MATCHED THEN INSERT (process_name, last_run_at, last_run_status, last_run_details) " +
        "VALUES ('SP_RUN_HOUSEHOLD_MATCHING', TO_TIMESTAMP('" + newWatermark + "', 'YYYY-MM-DD HH24:MI:SS.FF3'), 'SUCCESS', PARSE_JSON('{\"new_matches\": " + newMatches + ", \"new_clusters\": " + newClusterCount + "}'))"
    });

    snowflake.execute({sqlText: "DROP TABLE IF EXISTS _new_clusters"});

    var totalActive = snowflake.execute({sqlText:
        "SELECT COUNT(*) FROM " + DB + ".SILVER.IDR_CORE_HOUSEHOLD_MATCH_RESULTS WHERE IS_ACTIVE = TRUE"
    });
    totalActive.next();
    return "New clusters processed: " + newClusterCount + ", new household edges: " + newMatches + ", total active: " + totalActive.getColumnValue(1);
$$;
