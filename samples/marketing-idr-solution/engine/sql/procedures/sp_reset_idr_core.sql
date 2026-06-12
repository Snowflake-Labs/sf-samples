-- =====================================================
-- SP_RESET_IDR_CORE
-- Resets shared SILVER tables used by the IDR engine
-- Called by per-use-case SP_RESET_IDR() implementations
-- =====================================================

CREATE OR REPLACE PROCEDURE IDR.PROCEDURES.SP_RESET_IDR_CORE(DEPLOYMENT_DB VARCHAR)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
    var DB = DEPLOYMENT_DB;
    var tables = [
        "IDR_CORE_ENTITY_IDENTIFIERS",
        "IDR_CORE_IDENTIFIER_LINK",
        "IDR_CORE_CLUSTER",
        "IDR_CORE_MATCH_RESULTS",
        "IDR_CORE_MATCH_LOG",
        "IDR_CORE_CLUSTER_LOG",
        "IDR_CORE_EVENT_LOG",
        "SP_EVENT_LOG",
        "IDR_ML_AI_CANDIDATE_PAIRS",
        "IDR_ML_AI_EVALUATION_LOG",
        "IDR_CORE_PROCESS_STATE"
    ];
    
    var truncated = [];
    for (var i = 0; i < tables.length; i++) {
        try {
            snowflake.execute({sqlText: "TRUNCATE TABLE " + DB + ".SILVER." + tables[i]});
            truncated.push(tables[i]);
        } catch (err) {
            snowflake.log("warn", "Skipped " + tables[i] + ": " + err.message);
        }
    }
    
    return "Core reset: truncated " + truncated.length + " SILVER tables (" + truncated.join(", ") + ")";
$$;
