-- ============================================================================
-- SP_RUN_MATCHING: Generic rules-based matching for Normalized Model
-- Reads rules from APP.IDR_MATCHING_RULES; builds _source_attributes pivot
-- dynamically from rule fields (anchor, exact, fuzzy). Anchor = blocking key.
-- ============================================================================

USE ROLE ACCOUNTADMIN;
USE DATABASE IDR;
USE SCHEMA PROCEDURES;

CREATE OR REPLACE PROCEDURE IDR.PROCEDURES.SP_RUN_MATCHING(DEPLOYMENT_DB VARCHAR)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
    var DB = DEPLOYMENT_DB;
    var q = String.fromCharCode(39);
    
    // Rule field names = IDR_IDENTIFIER tag value = IDR_CORE_ENTITY_IDENTIFIERS.identifier_type.
    // UPPER is applied so user-entered values (email, Email, EMAIL) match tags/identifier_type consistently.
    
    // Set context
    snowflake.execute({sqlText: "USE DATABASE " + DB});
    snowflake.execute({sqlText: "USE SCHEMA SILVER"});
    
    var vectorEnabled = false;
    try {
        var vecCheck = snowflake.execute({sqlText:
            "SELECT COUNT(*) AS CNT FROM " + DB + ".CONFIG.IDR_MATCHING_RULES " +
            "WHERE MATCH_TYPE = 'VECTOR' AND IS_ACTIVE = TRUE"
        });
        vecCheck.next();
        vectorEnabled = (vecCheck.getColumnValue('CNT') > 0);
    } catch(e) { }
    
    // Get the batch start time from IDR_CORE_PROCESS_STATE table (last successful run)
    var batchTimeResult = snowflake.execute({sqlText: 
        "SELECT COALESCE(" +
        "  (SELECT TO_VARCHAR(LAST_RUN_AT, 'YYYY-MM-DD HH24:MI:SS.FF3') " +
        "   FROM " + DB + ".SILVER.IDR_CORE_PROCESS_STATE " +
        "   WHERE PROCESS_NAME = 'SP_RUN_MATCHING' AND LAST_RUN_STATUS = 'SUCCESS'), " +
        "  '2000-01-01 00:00:00.000'" +
        ")"
    });
    batchTimeResult.next();
    var batchStartTime = batchTimeResult.getColumnValue(1);
    
    // Count new links to process
    var newCountResult = snowflake.execute({sqlText:
        "SELECT COUNT(DISTINCT source_record_id) FROM " + DB + ".SILVER.IDR_CORE_IDENTIFIER_LINK " +
        "WHERE created_at >= '" + batchStartTime + "' AND is_active = TRUE"
    });
    newCountResult.next();
    var newSourceCount = newCountResult.getColumnValue(1);
    
    if (newSourceCount === 0) {
        return "No new source records to match";
    }
    
    snowflake.log("info", "SP_RUN_MATCHING: Processing " + newSourceCount + " new sources since " + batchStartTime);
    
    // =========================================================================
    // STEP 1: Deactivate matches for links that are no longer active
    // =========================================================================
    snowflake.execute({sqlText:
        "UPDATE " + DB + ".SILVER.IDR_CORE_MATCH_RESULTS " +
        "SET is_active = FALSE, updated_at = CURRENT_TIMESTAMP() " +
        "WHERE is_active = TRUE AND (" +
        "    new_source_record_id IN (SELECT source_record_id FROM " + DB + ".SILVER.IDR_CORE_IDENTIFIER_LINK WHERE is_active = FALSE) " +
        "    OR matched_source_record_id IN (SELECT source_record_id FROM " + DB + ".SILVER.IDR_CORE_IDENTIFIER_LINK WHERE is_active = FALSE)" +
        ")"
    });
    
    // =========================================================================
    // STEP 2: Get distinct column names used by active rules (for dynamic pivot)
    // =========================================================================
    var colsResult = snowflake.execute({sqlText:
        "SELECT DISTINCT UPPER(TRIM(column_name)) AS column_name " +
        "FROM ( " +
        "  SELECT anchor_field AS column_name FROM " + DB + ".CONFIG.IDR_MATCHING_RULES WHERE is_active = TRUE AND anchor_field IS NOT NULL AND match_type != 'HOUSEHOLD' " +
        "  UNION " +
        "  SELECT TRIM(f.value::VARCHAR) FROM " + DB + ".CONFIG.IDR_MATCHING_RULES r, LATERAL FLATTEN(INPUT => SPLIT(COALESCE(r.exact_match_fields,''), ',')) f WHERE r.is_active = TRUE AND r.match_type != 'HOUSEHOLD' AND TRIM(f.value::VARCHAR) != '' " +
        "  UNION " +
        "  SELECT TRIM(ff.value::VARCHAR) FROM " + DB + ".CONFIG.IDR_MATCHING_RULES r, LATERAL FLATTEN(INPUT => SPLIT(COALESCE(r.fuzzy_match_field,''), ',')) ff WHERE r.is_active = TRUE AND r.match_type != 'HOUSEHOLD' AND TRIM(ff.value::VARCHAR) != '' " +
        ") u " +
        "WHERE column_name IS NOT NULL AND column_name != ''"
    });
    var requiredCols = {};
    var needFullName = false;
    var needFirstNameCanonical = false;
    while (colsResult.next()) {
        var col = colsResult.getColumnValue(1);
        requiredCols[col] = true;
        if (col === 'FULL_NAME') needFullName = true;
        if (col === 'FIRST_NAME_CANONICAL') needFirstNameCanonical = true;
    }
    
    // Build aggregated SELECT list for pivot. Rule field name = identifier_type (same as IDR_IDENTIFIER / IDR_CORE_ENTITY_IDENTIFIERS).
    var selectParts = ['source_record_id', 'source_type'];
    var addedTypes = {};
    var col;
    for (col in requiredCols) {
        if (col === 'FULL_NAME' || col === 'FIRST_NAME_CANONICAL') continue;  // derived, handled below
        if (addedTypes[col]) continue;
        addedTypes[col] = true;
        if (col === 'DOB') {
            selectParts.push("MAX(CASE WHEN identifier_type = " + q + "DOB" + q + " THEN TRY_TO_DATE(identifier_value_normalized) END) AS DOB");
        } else if (col === 'NAME_FIRST') {
            selectParts.push("MAX(CASE WHEN identifier_type = " + q + "NAME_FIRST" + q + " THEN identifier_value_normalized END) AS NAME_FIRST");
            selectParts.push("MAX(CASE WHEN identifier_type = " + q + "NAME_FIRST" + q + " THEN identifier_value END) AS NAME_FIRST_ORIGINAL");
        } else {
            selectParts.push("MAX(CASE WHEN identifier_type = " + q + col + q + " THEN identifier_value_normalized END) AS " + col);
        }
    }
    if (needFullName) {
        if (!addedTypes['NAME_FIRST']) {
            addedTypes['NAME_FIRST'] = true;
            selectParts.push("MAX(CASE WHEN identifier_type = " + q + "NAME_FIRST" + q + " THEN identifier_value_normalized END) AS NAME_FIRST");
            selectParts.push("MAX(CASE WHEN identifier_type = " + q + "NAME_FIRST" + q + " THEN identifier_value END) AS NAME_FIRST_ORIGINAL");
        }
        if (!addedTypes['NAME_LAST']) {
            addedTypes['NAME_LAST'] = true;
            selectParts.push("MAX(CASE WHEN identifier_type = " + q + "NAME_LAST" + q + " THEN identifier_value_normalized END) AS NAME_LAST");
        }
        selectParts.push("CONCAT(COALESCE(MAX(CASE WHEN identifier_type = " + q + "NAME_FIRST" + q + " THEN identifier_value_normalized END), ''), ' ', COALESCE(MAX(CASE WHEN identifier_type = " + q + "NAME_LAST" + q + " THEN identifier_value_normalized END), '')) AS FULL_NAME");
    }
    if (needFirstNameCanonical && !addedTypes['NAME_FIRST']) {
        addedTypes['NAME_FIRST'] = true;
        selectParts.push("MAX(CASE WHEN identifier_type = " + q + "NAME_FIRST" + q + " THEN identifier_value_normalized END) AS NAME_FIRST");
        selectParts.push("MAX(CASE WHEN identifier_type = " + q + "NAME_FIRST" + q + " THEN identifier_value END) AS NAME_FIRST_ORIGINAL");
    }
    
    var aggregatedSelect = selectParts.join(", ");
    
    var pivotSql = 
        "CREATE OR REPLACE TEMPORARY TABLE _source_attributes AS " +
        "WITH new_sources AS ( " +
        "  SELECT DISTINCT source_record_id FROM " + DB + ".SILVER.IDR_CORE_IDENTIFIER_LINK " +
        "  WHERE created_at >= '" + batchStartTime + "' AND is_active = TRUE " +
        "), " +
        "new_anchor_values AS ( " +
        "  SELECT DISTINCT UPPER(TRIM(e.identifier_type)) AS identifier_type, e.identifier_value_normalized AS identifier_value " +
        "  FROM new_sources ns " +
        "  JOIN " + DB + ".SILVER.IDR_CORE_IDENTIFIER_LINK l ON l.source_record_id = ns.source_record_id AND l.is_active = TRUE " +
        "  JOIN " + DB + ".SILVER.IDR_CORE_ENTITY_IDENTIFIERS e ON l.identifier_id = e.identifier_id AND e.is_active = TRUE " +
        "), " +
        "candidate_sources AS ( " +
        "  SELECT DISTINCT l.source_record_id FROM " + DB + ".SILVER.IDR_CORE_IDENTIFIER_LINK l " +
        "  JOIN " + DB + ".SILVER.IDR_CORE_ENTITY_IDENTIFIERS e ON l.identifier_id = e.identifier_id AND e.is_active = TRUE " +
        "  JOIN new_anchor_values nav ON UPPER(TRIM(e.identifier_type)) = nav.identifier_type AND e.identifier_value_normalized = nav.identifier_value " +
        "  WHERE l.is_active = TRUE " +
        "  UNION " +
        "  SELECT source_record_id FROM new_sources " +
        "), " +
        "source_identifiers AS ( " +
        "  SELECT l.source_record_id, l.source_type, " + (vectorEnabled ? "l.SOURCE_EMBEDDING, " : "") + "UPPER(TRIM(e.identifier_type)) AS identifier_type, e.identifier_value, e.identifier_value_normalized, e.identifier_id " +
        "  FROM candidate_sources cs " +
        "  JOIN " + DB + ".SILVER.IDR_CORE_IDENTIFIER_LINK l ON l.source_record_id = cs.source_record_id " +
        "  JOIN " + DB + ".SILVER.IDR_CORE_ENTITY_IDENTIFIERS e ON l.identifier_id = e.identifier_id " +
        "  WHERE l.is_active = TRUE AND e.is_active = TRUE " +
        "), " +
        "aggregated AS ( " +
        "  SELECT " + aggregatedSelect + (vectorEnabled ? ", ANY_VALUE(SOURCE_EMBEDDING) AS SOURCE_EMBEDDING" : "") + " " +
        "  FROM source_identifiers " +
        "  GROUP BY source_record_id, source_type " +
        ") " +
        "SELECT a.*";
    if (needFirstNameCanonical) {
        pivotSql += ", COALESCE(nm.canonical_name, a.NAME_FIRST) AS FIRST_NAME_CANONICAL ";
        pivotSql += "FROM aggregated a LEFT JOIN " + DB + ".CONFIG.IDR_CORE_NICKNAME_MAP nm ON nm.nickname = a.NAME_FIRST_ORIGINAL";
    } else {
        pivotSql += " FROM aggregated a";
    }
    snowflake.log("debug", "pivotSql: "+ pivotSql);
    snowflake.execute({sqlText: pivotSql});
    
    // Create temp table for new sources
    snowflake.execute({sqlText: 
        "CREATE OR REPLACE TEMPORARY TABLE _new_sources AS " +
        "SELECT DISTINCT source_record_id FROM " + DB + ".SILVER.IDR_CORE_IDENTIFIER_LINK " +
        "WHERE created_at >= '" + batchStartTime + "' AND is_active = TRUE"
    });
    
    // =========================================================================
    // STEP 3: Build matching queries from rules
    // =========================================================================
    var rulesResult = snowflake.execute({sqlText:
        "SELECT rule_id, rule_name, match_type, anchor_field, exact_match_fields, " +
        "       fuzzy_match_field, fuzzy_algorithm, fuzzy_threshold, base_score, " +
        "       require_cross_source, require_different_record, rule_priority " +
        "FROM " + DB + ".CONFIG.IDR_MATCHING_RULES " +
        "WHERE is_active = TRUE " +
        "ORDER BY rule_priority"
    });
    
    var unionParts = [];
    while (rulesResult.next()) {
        var ruleId = rulesResult.getColumnValue(1);
        var ruleName = rulesResult.getColumnValue(2);
        var matchType = rulesResult.getColumnValue(3);
        var anchorField = rulesResult.getColumnValue(4);
        var exactMatchFields = rulesResult.getColumnValue(5);
        var fuzzyMatchField = rulesResult.getColumnValue(6);
        var fuzzyAlgorithm = rulesResult.getColumnValue(7) || "NONE";
        var fuzzyThreshold = rulesResult.getColumnValue(8) || 0.85;
        var baseScore = rulesResult.getColumnValue(9) || 0.9;
        var requireCrossSource = rulesResult.getColumnValue(10);
        var requireDifferentRecord = rulesResult.getColumnValue(11);
        var rulePriority = rulesResult.getColumnValue(12);
        
        if (matchType === "HOUSEHOLD") continue;
        
        var anchorCol = anchorField ? anchorField.trim().toUpperCase() : null;
        
        if (matchType === "VECTOR") {
            if (!anchorCol) {
                snowflake.log("warn", "Vector rule " + ruleId + " skipped: no anchor_field for blocking");
                continue;
            }
            var vecJoinConditions = [];
            vecJoinConditions.push("a." + anchorCol + " = b." + anchorCol);
            vecJoinConditions.push("a." + anchorCol + " IS NOT NULL");
            vecJoinConditions.push("a.SOURCE_EMBEDDING IS NOT NULL");
            vecJoinConditions.push("b.SOURCE_EMBEDDING IS NOT NULL");
            var vecScoreExpr = "VECTOR_COSINE_SIMILARITY(a.SOURCE_EMBEDDING, b.SOURCE_EMBEDDING)";
            vecJoinConditions.push(vecScoreExpr + " >= " + fuzzyThreshold);
            if (requireCrossSource) vecJoinConditions.push("a.source_type != b.source_type");
            vecJoinConditions.push("a.source_record_id < b.source_record_id");
            vecJoinConditions.push("(a.source_record_id IN (SELECT source_record_id FROM _new_sources) OR b.source_record_id IN (SELECT source_record_id FROM _new_sources))");
            var vecSelectSql =
                "SELECT " +
                "  a.source_record_id AS new_source_record_id, " +
                "  b.source_record_id AS matched_source_record_id, " +
                "  " + q + ruleId + q + " AS rule_id, " +
                "  " + q + ruleName + q + " AS rule_name, " +
                "  " + vecScoreExpr + " AS match_score, " +
                "  CAST(a." + anchorCol + " AS VARCHAR) AS anchor_value, " +
                "  " + rulePriority + " AS rule_priority " +
                "FROM _source_attributes a " +
                "JOIN _source_attributes b ON " + vecJoinConditions.join(" AND ");
            unionParts.push(vecSelectSql);
            continue;
        }
        
        if (!anchorCol || !requiredCols[anchorCol]) {
            snowflake.log("warn", "Rule " + ruleId + " skipped: anchor_field missing or not in pivot");
            continue;
        }
        
        var joinConditions = [];
        joinConditions.push("a." + anchorCol + " = b." + anchorCol);
        joinConditions.push("a." + anchorCol + " IS NOT NULL");
        
        if (exactMatchFields) {
            var fields = exactMatchFields.split(',').map(function(s) { return s.trim().toUpperCase(); }).filter(function(c) { return c; });
            for (var i = 0; i < fields.length; i++) {
                joinConditions.push("a." + fields[i] + " = b." + fields[i]);
                joinConditions.push("a." + fields[i] + " IS NOT NULL");
            }
        }
        
        var scoreExpr = baseScore.toString();
        if (fuzzyMatchField && fuzzyAlgorithm !== "NONE") {
            var fuzzyCols = fuzzyMatchField.split(',').map(function(s) { return s.trim().toUpperCase(); }).filter(function(c) { return c; });
            if (fuzzyCols.length > 0) {
                if (fuzzyAlgorithm === "CANONICAL") {
                    for (var fc = 0; fc < fuzzyCols.length; fc++) {
                        if (fuzzyCols[fc] === 'FIRST_NAME_CANONICAL') {
                            joinConditions.push("a.FIRST_NAME_CANONICAL = b.FIRST_NAME_CANONICAL");
                            joinConditions.push("a.FIRST_NAME_CANONICAL IS NOT NULL");
                            break;
                        }
                    }
                } else if (fuzzyAlgorithm === "JARO_WINKLER") {
                    var jaroParts = [];
                    for (var j = 0; j < fuzzyCols.length; j++) {
                        var col = fuzzyCols[j];
                        joinConditions.push("" + DB + ".SILVER.JARO_WINKLER_SIMILARITY(a." + col + ", b." + col + ") >= " + fuzzyThreshold);
                        jaroParts.push("" + DB + ".SILVER.JARO_WINKLER_SIMILARITY(a." + col + ", b." + col + ")");
                    }
                    scoreExpr = jaroParts.length === 1 ? jaroParts[0] : "(" + jaroParts.join(" + ") + ") / " + jaroParts.length;
                }
            }
        }
        
        // Only require different source types (e.g. booking vs loyalty) when rule has require_cross_source = TRUE
        if (requireCrossSource) joinConditions.push("a.source_type != b.source_type");
        joinConditions.push("a.source_record_id < b.source_record_id");
        joinConditions.push("(a.source_record_id IN (SELECT source_record_id FROM _new_sources) OR b.source_record_id IN (SELECT source_record_id FROM _new_sources))");
        
        var selectSql = 
            "SELECT " +
            "  a.source_record_id AS new_source_record_id, " +
            "  b.source_record_id AS matched_source_record_id, " +
            "  " + q + ruleId + q + " AS rule_id, " +
            "  " + q + ruleName + q + " AS rule_name, " +
            "  " + scoreExpr + " AS match_score, " +
            "  CAST(a." + anchorCol + " AS VARCHAR) AS anchor_value, " +
            "  " + rulePriority + " AS rule_priority " +
            "FROM _source_attributes a " +
            "JOIN _source_attributes b ON " + joinConditions.join(" AND ");
        unionParts.push(selectSql);
    }
    
    if (unionParts.length === 0) {
        snowflake.execute({sqlText: "DROP TABLE IF EXISTS _source_attributes"});
        snowflake.execute({sqlText: "DROP TABLE IF EXISTS _new_sources"});
        return "No active matching rules found (or all skipped)";
    }
    
    // =========================================================================
    // STEP 4: Execute matching with deduplication (keep best rule per pair)
    // =========================================================================
    var matchSql = 
        "INSERT INTO " + DB + ".SILVER.IDR_CORE_MATCH_RESULTS " +
        "(new_source_record_id, matched_source_record_id, rule_id, rule_name, match_score, identifier_1, identifier_2) " +
        "WITH all_matches AS (" + unionParts.join(" UNION ALL ") + "), " +
        "ranked AS ( " +
        "  SELECT new_source_record_id, matched_source_record_id, rule_id, rule_name, match_score, anchor_value, " +
        "    ROW_NUMBER() OVER (PARTITION BY new_source_record_id, matched_source_record_id ORDER BY rule_priority ASC, match_score DESC) AS rn " +
        "  FROM all_matches " +
        ") " +
        "SELECT new_source_record_id, matched_source_record_id, rule_id, rule_name, match_score, anchor_value AS identifier_1, anchor_value AS identifier_2 " +
        "FROM ranked WHERE rn = 1 " +
        "AND NOT EXISTS ( " +
        "  SELECT 1 FROM " + DB + ".SILVER.IDR_CORE_MATCH_RESULTS mr " +
        "  WHERE mr.new_source_record_id = ranked.new_source_record_id AND mr.matched_source_record_id = ranked.matched_source_record_id AND mr.is_active = TRUE " +
        ")";
    
    snowflake.log("debug", "matchSql: "+ matchSql);
    var matchResult = snowflake.execute({sqlText: matchSql});
    var newMatches = 0;
    try { newMatches = matchResult.getNumRowsAffected(); } catch (e) {}
    
    snowflake.log("info", "SP_RUN_MATCHING: " + newMatches + " new matches found");
    
    var householdMatches = 0;
    
    // =========================================================================
    // STEP 5: Update process state
    // =========================================================================
    snowflake.execute({sqlText:
        "MERGE INTO " + DB + ".SILVER.IDR_CORE_PROCESS_STATE tgt " +
        "USING (SELECT 'SP_RUN_MATCHING' AS process_name) src " +
        "ON tgt.process_name = src.process_name " +
        "WHEN MATCHED THEN UPDATE SET " +
        "  last_run_at = CURRENT_TIMESTAMP(), last_run_status = 'SUCCESS', " +
        "  last_run_details = PARSE_JSON('{\"new_matches\": " + newMatches + ", \"household_matches\": " + householdMatches + ", \"new_sources\": " + newSourceCount + "}') " +
        "WHEN NOT MATCHED THEN INSERT (process_name, last_run_at, last_run_status, last_run_details) " +
        "VALUES ('SP_RUN_MATCHING', CURRENT_TIMESTAMP(), 'SUCCESS', PARSE_JSON('{\"new_matches\": " + newMatches + ", \"household_matches\": " + householdMatches + ", \"new_sources\": " + newSourceCount + "}'))"
    });
    
    snowflake.execute({sqlText: "DROP TABLE IF EXISTS _source_attributes"});
    snowflake.execute({sqlText: "DROP TABLE IF EXISTS _new_sources"});
    
    var totalResult = snowflake.execute({sqlText: "SELECT COUNT(*) FROM " + DB + ".SILVER.IDR_CORE_MATCH_RESULTS WHERE is_active = TRUE"});
    totalResult.next();
    var totalMatches = totalResult.getColumnValue(1);
    
    return "New sources: " + newSourceCount + ", Identity matches: " + newMatches + ", Household: " + householdMatches + ", Total active: " + totalMatches;
$$;
