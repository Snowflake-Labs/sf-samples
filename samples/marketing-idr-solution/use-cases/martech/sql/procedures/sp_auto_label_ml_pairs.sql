-- ============================================================================
-- Martech: APP.SP_AUTO_LABEL_ML_PAIRS
-- Auto-label ML_PAIR_FEATURES rows from existing deterministic edges in
-- IDR_CORE_MATCH_RESULTS (rules R01-R15). Pairs in the same blocking bucket
-- with no deterministic edge are labeled negative.
-- ============================================================================

CREATE OR REPLACE PROCEDURE IDENTIFIER('&{deployment_db}.APP.SP_AUTO_LABEL_ML_PAIRS')()
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
    var dbResult = snowflake.execute({sqlText: "SELECT CURRENT_DATABASE() AS DB"});
    dbResult.next();
    var DB = dbResult.getColumnValue('DB');

    // Positive label: any deterministic rule produced an edge for the pair
    var posSql =
        "UPDATE " + DB + ".SILVER.ML_PAIR_FEATURES f SET LABEL = 1, UPDATED_AT = CURRENT_TIMESTAMP() " +
        "WHERE f.LABEL IS NULL AND EXISTS (" +
        "  SELECT 1 FROM " + DB + ".SILVER.IDR_CORE_MATCH_RESULTS m " +
        "  WHERE m.IS_CURRENT = TRUE " +
        "    AND m.RULE_ID IN ('MARTECH_R01','MARTECH_R02','MARTECH_R03','MARTECH_R04', " +
        "                      'MARTECH_R05','MARTECH_R06','MARTECH_R07','MARTECH_R08', " +
        "                      'MARTECH_R09','MARTECH_R10','MARTECH_R11','MARTECH_R12', " +
        "                      'MARTECH_R13','MARTECH_R14','MARTECH_R15') " +
        "    AND LEAST(m.SOURCE_RECORD_ID_A, m.SOURCE_RECORD_ID_B) = LEAST(f.SOURCE_RECORD_ID_A, f.SOURCE_RECORD_ID_B) " +
        "    AND GREATEST(m.SOURCE_RECORD_ID_A, m.SOURCE_RECORD_ID_B) = GREATEST(f.SOURCE_RECORD_ID_A, f.SOURCE_RECORD_ID_B))";
    var rp = snowflake.execute({sqlText: posSql});
    rp.next();
    var positives = rp.getColumnValue(1);

    // Negative label: same-block pair with no deterministic match
    var negSql =
        "UPDATE " + DB + ".SILVER.ML_PAIR_FEATURES SET LABEL = 0, UPDATED_AT = CURRENT_TIMESTAMP() " +
        "WHERE LABEL IS NULL";
    var rn = snowflake.execute({sqlText: negSql});
    rn.next();
    var negatives = rn.getColumnValue(1);

    return JSON.stringify({ positives: positives, negatives: negatives });
$$;
