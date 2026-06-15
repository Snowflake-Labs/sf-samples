-- ============================================================================
-- Martech: APP.SP_ML_SCORE_CANDIDATES (v1 — 4-bucket router)
-- Score PENDING pairs in ML_PAIR_FEATURES; route into 4 tiers per
-- requirements.md FR-9, FR-10:
--   ML_PROB >= ML_AUTO_MERGE_THRESHOLD     → AUTO_MERGE → emit R16 edge
--   ML_DROP <= ML_PROB < AUTO_MERGE        → GREY_LLM   (LLM stage picks up)
--   ML_PROB <  ML_DROP, det edge exists    → DISPUTE    (severity by Def B)
--   ML_PROB <  ML_DROP, no det edge        → DROP       (no further action)
--
-- v1 keeps the linear scoring formula (parked: trained-model swap is a
-- single-statement change in the SCORE expression).
-- All thresholds + model version pulled from CONFIG.IDR_ML_AI_EVALUATION_CONFIG.
-- Writes IDR_ML_INDIVIDUAL_MATCH_EXPLAIN per emitted edge.
-- Writes IDR_INDIVIDUAL_MATCH_DISPUTE_QUEUE per dispute.
-- ============================================================================

CREATE OR REPLACE PROCEDURE IDENTIFIER('&{deployment_db}.APP.SP_ML_SCORE_CANDIDATES')()
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
    var dbResult = snowflake.execute({sqlText: "SELECT CURRENT_DATABASE() AS DB"});
    dbResult.next();
    var DB = dbResult.getColumnValue('DB');

    var pipelineRunResult = snowflake.execute({sqlText: "SELECT UUID_STRING() AS RID"});
    pipelineRunResult.next();
    var pipelineRunId = pipelineRunResult.getColumnValue('RID');

    var errors = [];
    function exec(sql) {
        try { return snowflake.execute({sqlText: sql}); }
        catch (e) { errors.push({sql: sql.substring(0, 200), error: e.message}); return null; }
    }

    // ─── Step 1: Load thresholds + model version from config ───────────────────
    var cfg = { auto_merge: 0.85, drop: 0.55, dispute_gate: 0.55, dispute_strong: 0.20, model_version: 'idr_pair_v0_linear' };
    var cr = exec(
        "SELECT CONFIG_KEY, CONFIG_VALUE FROM " + DB + ".CONFIG.IDR_ML_AI_EVALUATION_CONFIG " +
        "WHERE CONFIG_KEY IN (" +
        "  'ML_AUTO_MERGE_THRESHOLD','ML_DROP_THRESHOLD'," +
        "  'ML_DISPUTE_GATE_THRESHOLD','ML_DISPUTE_STRONG_THRESHOLD'," +
        "  'ML_DEPLOYED_MODEL_VERSION'" +
        ")"
    );
    while (cr && cr.next()) {
        var k = cr.getColumnValue('CONFIG_KEY');
        var v = cr.getColumnValue('CONFIG_VALUE');
        if (k === 'ML_AUTO_MERGE_THRESHOLD')      cfg.auto_merge = parseFloat(v);
        if (k === 'ML_DROP_THRESHOLD')            cfg.drop = parseFloat(v);
        if (k === 'ML_DISPUTE_GATE_THRESHOLD')    cfg.dispute_gate = parseFloat(v);
        if (k === 'ML_DISPUTE_STRONG_THRESHOLD') cfg.dispute_strong = parseFloat(v);
        if (k === 'ML_DEPLOYED_MODEL_VERSION')   cfg.model_version = v;
    }

    // ─── Step 2: Score PENDING pairs (v2 — with EMAIL_HANDLE_EQ, PHONE_LAST7_EQ, COMBO_BONUS) ───
    // BASE_SCORE uses rebalanced weights (sum=1.0).
    // COMBO_BONUS (+0.20) fires when handle + last7 + strong name all hold,
    // simulating feature-interaction a real ML model would learn.
    var baseFormula =
        "0.15 * IFF(EMAIL_EQ OR HEM_EQ, 1, 0) + " +
        "0.20 * IFF(EMAIL_HANDLE_EQ, 1, 0) + " +
        "0.08 * IFF(PHONE_EQ, 1, 0) + " +
        "0.12 * IFF(PHONE_LAST7_EQ, 1, 0) + " +
        "0.08 * IFF(LOYALTY_ID_EQ, 1, 0) + " +
        "0.02 * IFF(DEVICE_ID_EQ, 1, 0) + " +
        "0.14 * COALESCE(JW_LAST_NAME, 0) + " +
        "0.10 * COALESCE(JW_FIRST_NAME, 0) + " +
        "0.05 * COALESCE(JW_STREET, 0) + " +
        "0.04 * IFF(POSTAL_EQ, 1, 0) + " +
        "0.01 * IFF(STATE_EQ, 1, 0) + " +
        "0.01 * IFF(NICKNAME_FIRST_EQ, 1, 0)";
    var comboFormula =
        "IFF(EMAIL_HANDLE_EQ AND PHONE_LAST7_EQ " +
        "    AND COALESCE(JW_FIRST_NAME, 0) >= 0.90 " +
        "    AND COALESCE(JW_LAST_NAME, 0) >= 0.90, 0.20, 0)";
    var sumExpr = baseFormula + " + " + comboFormula;

    // Coincidental-identifier collision guard: a strong identifier matches
    // (email handle OR phone last-7) but the surnames strongly conflict
    // (JW_LAST_NAME < 0.60) and there is NO corroborating exact email or loyalty
    // match. These are almost always two DIFFERENT people who happen to share a
    // handle or a 7-digit phone tail. Instead of letting the identifier weight
    // push them to AUTO_MERGE (a silent false merge), clamp the score into the
    // GREY_LLM band [drop, auto_merge) so the LLM adjudicates and can reject.
    var collisionCond =
        "(COALESCE(EMAIL_HANDLE_EQ, FALSE) OR COALESCE(PHONE_LAST7_EQ, FALSE)) " +
        "AND COALESCE(JW_LAST_NAME, 0) < 0.60 " +
        "AND NOT COALESCE(EMAIL_EQ, FALSE) AND NOT COALESCE(HEM_EQ, FALSE) " +
        "AND NOT COALESCE(LOYALTY_ID_EQ, FALSE)";
    var greyFloor = cfg.drop.toFixed(4);
    var greyCeil  = (cfg.auto_merge - 0.01).toFixed(4);
    var routedExpr =
        "IFF(" + collisionCond + ", " +
        "    LEAST(GREATEST((" + sumExpr + "), " + greyFloor + "), " + greyCeil + "), " +
        "    (" + sumExpr + "))";
    var fullFormula = "LEAST(1.0, GREATEST(0.0, " + routedExpr + "))";

    var scoreSql =
        "UPDATE " + DB + ".SILVER.ML_PAIR_FEATURES f SET " +
        "  SCORE = " + fullFormula + ", " +
        "  ML_PROB = " + fullFormula + ", " +
        "  ML_MODEL_VERSION = '" + cfg.model_version + "', " +
        "  STATUS = 'SCORED', " +
        "  SCORED_AT = CURRENT_TIMESTAMP(), " +
        "  UPDATED_AT = CURRENT_TIMESTAMP() " +
        "WHERE STATUS = 'PENDING'";
    var rs = exec(scoreSql);
    var scored = 0;
    if (rs) { rs.next(); scored = rs.getColumnValue(1); }

    // ─── Step 3: Tag ML_TIER on the just-scored rows ──────────────────────────
    // Note: dispute determination requires checking deterministic edges,
    // so DISPUTE tag happens after we identify pairs with deterministic edges.
    exec(
        "UPDATE " + DB + ".SILVER.ML_PAIR_FEATURES SET ML_TIER = " +
        "  CASE " +
        "    WHEN ML_PROB >= " + cfg.auto_merge + " THEN 'AUTO_MERGE' " +
        "    WHEN ML_PROB >= " + cfg.drop        + " THEN 'GREY_LLM' " +
        "    ELSE 'DROP' " +
        "  END " +
        "WHERE STATUS = 'SCORED' AND ML_TIER IS NULL"
    );

    // ─── Step 4: AUTO_MERGE — emit R16 edges + explainability ─────────────────
    var emitSql =
        "INSERT INTO " + DB + ".SILVER.IDR_CORE_MATCH_RESULTS (" +
        "  SOURCE_RECORD_ID_A, SOURCE_RECORD_ID_B, NEW_SOURCE_RECORD_ID, MATCHED_SOURCE_RECORD_ID, " +
        "  RULE_ID, RULE_NAME, MATCH_SCORE, MATCHED_ON, IS_ACTIVE, IS_CURRENT, " +
        "  EDGE_POLARITY, EDGE_TARGET, RUN_ID, PIPELINE_RUN_ID" +
        ") " +
        "SELECT SOURCE_RECORD_ID_A, SOURCE_RECORD_ID_B, SOURCE_RECORD_ID_A, SOURCE_RECORD_ID_B, " +
        "       'MARTECH_R16', 'ML Candidate Score >= ' || " + cfg.auto_merge + ", " +
        "       ML_PROB, 'ML', TRUE, TRUE, " +
        "       'MATCH', 'INDIVIDUAL', " +
        "       '" + pipelineRunId + "', '" + pipelineRunId + "' " +
        "FROM " + DB + ".SILVER.ML_PAIR_FEATURES " +
        "WHERE ML_TIER = 'AUTO_MERGE' AND EMITTED_MATCH_ID IS NULL";
    var re = exec(emitSql);
    var emitted = 0;
    if (re) { re.next(); emitted = re.getColumnValue(1); }

    // Match the just-inserted MATCH_RESULTS rows back to ML_PAIR_FEATURES
    // by (LEAST,GREATEST) of source IDs + RULE_ID + PIPELINE_RUN_ID, capture MATCH_ID.
    exec(
        "CREATE OR REPLACE TEMPORARY TABLE _just_emitted AS " +
        "SELECT mr.MATCH_ID, " +
        "       LEAST(mr.SOURCE_RECORD_ID_A, mr.SOURCE_RECORD_ID_B) AS SRC_LO, " +
        "       GREATEST(mr.SOURCE_RECORD_ID_A, mr.SOURCE_RECORD_ID_B) AS SRC_HI " +
        "FROM " + DB + ".SILVER.IDR_CORE_MATCH_RESULTS mr " +
        "WHERE mr.PIPELINE_RUN_ID = '" + pipelineRunId + "' AND mr.RULE_ID = 'MARTECH_R16'"
    );

    // Update ML_PAIR_FEATURES with the emitted MATCH_ID + status
    exec(
        "UPDATE " + DB + ".SILVER.ML_PAIR_FEATURES f " +
        "SET STATUS = 'EMITTED', EMITTED_MATCH_ID = je.MATCH_ID, UPDATED_AT = CURRENT_TIMESTAMP() " +
        "FROM _just_emitted je " +
        "WHERE LEAST(f.SOURCE_RECORD_ID_A, f.SOURCE_RECORD_ID_B) = je.SRC_LO " +
        "  AND GREATEST(f.SOURCE_RECORD_ID_A, f.SOURCE_RECORD_ID_B) = je.SRC_HI " +
        "  AND f.ML_TIER = 'AUTO_MERGE'"
    );

    // INSERT explainability rows
    exec(
        "INSERT INTO " + DB + ".SILVER.IDR_ML_INDIVIDUAL_MATCH_EXPLAIN (" +
        "  MATCH_ID, PAIR_ID, ML_MODEL_VERSION, ML_PROB, AUTO_MERGE_THRESHOLD, " +
        "  FEATURE_VALUES, TOP_POSITIVE_FEATURES, TOP_NEGATIVE_FEATURES, BLOCKING_KEYS_HIT" +
        ") " +
        "SELECT f.EMITTED_MATCH_ID, f.PAIR_ID, f.ML_MODEL_VERSION, f.ML_PROB, " + cfg.auto_merge + ", " +
        "       OBJECT_CONSTRUCT(" +
        "         'EMAIL_EQ', f.EMAIL_EQ, 'HEM_EQ', f.HEM_EQ, 'PHONE_EQ', f.PHONE_EQ, " +
        "         'LOYALTY_ID_EQ', f.LOYALTY_ID_EQ, 'DEVICE_ID_EQ', f.DEVICE_ID_EQ, " +
        "         'JW_FIRST_NAME', f.JW_FIRST_NAME, 'JW_LAST_NAME', f.JW_LAST_NAME, " +
        "         'JW_STREET', f.JW_STREET, 'JW_CITY', f.JW_CITY, " +
        "         'POSTAL_EQ', f.POSTAL_EQ, 'STATE_EQ', f.STATE_EQ, 'NICKNAME_FIRST_EQ', f.NICKNAME_FIRST_EQ, " +
        "         'EMAIL_LEN_DIFF', f.EMAIL_LEN_DIFF, 'PHONE_LEN_DIFF', f.PHONE_LEN_DIFF" +
        "       ), " +
        // top positive: features whose weighted contribution is highest
        "       ARRAY_CONSTRUCT_COMPACT(" +
        "         CASE WHEN (f.EMAIL_EQ OR f.HEM_EQ) THEN OBJECT_CONSTRUCT('name','EMAIL_EQ_OR_HEM','contribution',0.30) END," +
        "         CASE WHEN f.PHONE_EQ THEN OBJECT_CONSTRUCT('name','PHONE_EQ','contribution',0.20) END," +
        "         CASE WHEN f.LOYALTY_ID_EQ THEN OBJECT_CONSTRUCT('name','LOYALTY_ID_EQ','contribution',0.20) END," +
        "         CASE WHEN COALESCE(f.JW_LAST_NAME,0) >= 0.85 THEN OBJECT_CONSTRUCT('name','JW_LAST_NAME','value',f.JW_LAST_NAME,'contribution',0.10*f.JW_LAST_NAME) END," +
        "         CASE WHEN f.POSTAL_EQ THEN OBJECT_CONSTRUCT('name','POSTAL_EQ','contribution',0.03) END" +
        "       ), " +
        // top negative: features whose absence/disagreement matters most
        "       ARRAY_CONSTRUCT_COMPACT(" +
        "         CASE WHEN NOT (f.EMAIL_EQ OR f.HEM_EQ) THEN OBJECT_CONSTRUCT('name','EMAIL_NOT_MATCHED','contribution',-0.30) END," +
        "         CASE WHEN NOT f.PHONE_EQ THEN OBJECT_CONSTRUCT('name','PHONE_NOT_MATCHED','contribution',-0.20) END," +
        "         CASE WHEN NOT f.POSTAL_EQ THEN OBJECT_CONSTRUCT('name','POSTAL_NOT_MATCHED','contribution',-0.03) END" +
        "       ), " +
        "       f.BLOCKING_KEYS_HIT " +
        "FROM " + DB + ".SILVER.ML_PAIR_FEATURES f " +
        "JOIN _just_emitted je ON f.EMITTED_MATCH_ID = je.MATCH_ID " +
        "WHERE NOT EXISTS (" +
        "  SELECT 1 FROM " + DB + ".SILVER.IDR_ML_INDIVIDUAL_MATCH_EXPLAIN x WHERE x.MATCH_ID = f.EMITTED_MATCH_ID" +
        ")"
    );

    // ─── Step 5: DISPUTE — pairs below drop threshold WITH a deterministic edge ─
    // For each such pair: insert into dispute queue with severity tagging.
    exec(
        "CREATE OR REPLACE TEMPORARY TABLE _dispute_candidates AS " +
        "SELECT f.PAIR_ID, f.SOURCE_RECORD_ID_A, f.SOURCE_RECORD_ID_B, " +
        "       f.ML_PROB, f.ML_MODEL_VERSION, f.BLOCKING_KEYS_HIT " +
        "FROM " + DB + ".SILVER.ML_PAIR_FEATURES f " +
        "WHERE f.ML_TIER = 'DROP' AND f.STATUS = 'SCORED'"
    );

    // Find deterministic edges for these pairs (excluding R16/R17 which are ML/LLM).
    // IDR_CORE_MATCH_RESULTS stores the pair under SOURCE_RECORD_ID_A/B for ML-emitted
    // edges (R16/R17) and under NEW_SOURCE_RECORD_ID/MATCHED_SOURCE_RECORD_ID for
    // deterministic edges from SP_RUN_MATCHING. COALESCE picks whichever pair is set.
    exec(
        "CREATE OR REPLACE TEMPORARY TABLE _dispute_with_det AS " +
        "SELECT dc.PAIR_ID, dc.SOURCE_RECORD_ID_A, dc.SOURCE_RECORD_ID_B, " +
        "       dc.ML_PROB, dc.ML_MODEL_VERSION, dc.BLOCKING_KEYS_HIT, " +
        "       mr.MATCH_ID AS DETERMINISTIC_MATCH_ID, mr.RULE_ID AS DETERMINISTIC_RULE_ID " +
        "FROM _dispute_candidates dc " +
        "JOIN " + DB + ".SILVER.IDR_CORE_MATCH_RESULTS mr " +
        "  ON LEAST(COALESCE(mr.SOURCE_RECORD_ID_A, mr.NEW_SOURCE_RECORD_ID), COALESCE(mr.SOURCE_RECORD_ID_B, mr.MATCHED_SOURCE_RECORD_ID)) " +
        "       = LEAST(dc.SOURCE_RECORD_ID_A, dc.SOURCE_RECORD_ID_B) " +
        " AND GREATEST(COALESCE(mr.SOURCE_RECORD_ID_A, mr.NEW_SOURCE_RECORD_ID), COALESCE(mr.SOURCE_RECORD_ID_B, mr.MATCHED_SOURCE_RECORD_ID)) " +
        "       = GREATEST(dc.SOURCE_RECORD_ID_A, dc.SOURCE_RECORD_ID_B) " +
        " AND mr.IS_ACTIVE = TRUE " +
        " AND COALESCE(mr.EDGE_POLARITY, 'MATCH') = 'MATCH' " +
        " AND mr.RULE_ID NOT IN ('MARTECH_R16','MARTECH_R17') " +
        "QUALIFY ROW_NUMBER() OVER (PARTITION BY dc.PAIR_ID ORDER BY mr.CREATED_AT) = 1"
    );

    // Insert into dispute queue (skip if already exists for this pair).
    // ML_TOP_NEGATIVE_FEATURES computed inline via a JOIN to ML_PAIR_FEATURES
    // (NOT a correlated scalar subquery — Snowflake rejects those with
    // "Unsupported subquery type" inside INSERT...SELECT).
    var dispSql =
        "INSERT INTO " + DB + ".SILVER.IDR_INDIVIDUAL_MATCH_DISPUTE_QUEUE (" +
        "  PAIR_ID, SOURCE_RECORD_ID_A, SOURCE_RECORD_ID_B, " +
        "  DETERMINISTIC_MATCH_ID, DETERMINISTIC_RULE_ID, " +
        "  ML_PROB, ML_MODEL_VERSION, ML_TOP_NEGATIVE_FEATURES, " +
        "  DISPUTE_SEVERITY, DISPUTE_REASON, STATUS" +
        ") " +
        "SELECT d.PAIR_ID, d.SOURCE_RECORD_ID_A, d.SOURCE_RECORD_ID_B, " +
        "       d.DETERMINISTIC_MATCH_ID, d.DETERMINISTIC_RULE_ID, " +
        "       d.ML_PROB, d.ML_MODEL_VERSION, " +
        "       ARRAY_CONSTRUCT_COMPACT(" +
        "         CASE WHEN NOT (f.EMAIL_EQ OR f.HEM_EQ) THEN OBJECT_CONSTRUCT('name','EMAIL_NOT_MATCHED','contribution',-0.30) END," +
        "         CASE WHEN NOT f.PHONE_EQ THEN OBJECT_CONSTRUCT('name','PHONE_NOT_MATCHED','contribution',-0.20) END," +
        "         CASE WHEN COALESCE(f.JW_FIRST_NAME,0) < 0.5 THEN OBJECT_CONSTRUCT('name','FIRST_NAME_DIVERGES','value',f.JW_FIRST_NAME,'contribution',-0.05*(1-COALESCE(f.JW_FIRST_NAME,0))) END," +
        "         CASE WHEN NOT f.POSTAL_EQ THEN OBJECT_CONSTRUCT('name','POSTAL_NOT_MATCHED','contribution',-0.03) END" +
        "       ), " +
        "       CASE WHEN d.ML_PROB < " + cfg.dispute_strong + " THEN 'STRONG' ELSE 'MILD' END, " +
        "       'ML_BELOW_DISPUTE_GATE', " +
        "       'PENDING' " +
        "FROM _dispute_with_det d " +
        "JOIN " + DB + ".SILVER.ML_PAIR_FEATURES f ON f.PAIR_ID = d.PAIR_ID " +
        "WHERE NOT EXISTS (" +
        "  SELECT 1 FROM " + DB + ".SILVER.IDR_INDIVIDUAL_MATCH_DISPUTE_QUEUE q " +
        "  WHERE q.PAIR_ID = d.PAIR_ID AND q.STATUS = 'PENDING'" +
        ")";
    var dr = exec(dispSql);
    var disputed = 0;
    if (dr) { dr.next(); disputed = dr.getColumnValue(1); }

    // Mark disputed pairs with the DISPUTE tier (overrides DROP)
    exec(
        "UPDATE " + DB + ".SILVER.ML_PAIR_FEATURES f " +
        "SET ML_TIER = 'DISPUTE', UPDATED_AT = CURRENT_TIMESTAMP() " +
        "FROM _dispute_with_det d " +
        "WHERE f.PAIR_ID = d.PAIR_ID"
    );

    // ─── Step 6: distribution stats ───────────────────────────────────────────
    var dist = {};
    var dr2 = exec(
        "SELECT ML_TIER, COUNT(*) AS CNT FROM " + DB + ".SILVER.ML_PAIR_FEATURES " +
        "WHERE STATUS IN ('SCORED','EMITTED') AND SCORED_AT >= DATEADD('minute', -5, CURRENT_TIMESTAMP()) " +
        "GROUP BY ML_TIER"
    );
    while (dr2 && dr2.next()) {
        dist[dr2.getColumnValue('ML_TIER')] = dr2.getColumnValue('CNT');
    }

    return JSON.stringify({
        scored: scored,
        edges_emitted: emitted,
        disputes_filed: disputed,
        tier_distribution: dist,
        thresholds_used: cfg,
        pipeline_run_id: pipelineRunId,
        errors: errors
    });
$$;
