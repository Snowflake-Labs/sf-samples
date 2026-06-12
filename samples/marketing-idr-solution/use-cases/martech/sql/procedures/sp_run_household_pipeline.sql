-- ============================================================================
-- Martech: APP.SP_RUN_HOUSEHOLD_PIPELINE
--
-- Orchestrator that mirrors engine SP_RUN_IDR_PIPELINE for the household
-- cluster space. Chains:
--   SP_RUN_HOUSEHOLD_MATCHING        - generate cluster-pair edges
--   SP_UPDATE_HOUSEHOLD_CLUSTERS     - recompute connected components
-- Designed to be called AFTER engine SP_RUN_IDR_PIPELINE so DT_CUSTOMER_PROFILE
-- (the input) is fresh.
-- LANGUAGE JS to mirror engine SP_RUN_IDR_PIPELINE: SQL Scripting CALL does
-- not support dynamic IDENTIFIER lookup.
-- ============================================================================

CREATE OR REPLACE PROCEDURE IDENTIFIER('&{deployment_db}.APP.SP_RUN_HOUSEHOLD_PIPELINE')(DEPLOYMENT_DB VARCHAR)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
    var DB = DEPLOYMENT_DB;
    var matchSummary   = "";
    var clusterSummary = "";
    try {
        var r1 = snowflake.execute({sqlText: "CALL " + DB + ".APP.SP_RUN_HOUSEHOLD_MATCHING('" + DB + "')"});
        r1.next();
        matchSummary = r1.getColumnValue(1);
    } catch (e) {
        return "SP_RUN_HOUSEHOLD_MATCHING failed: " + e.message;
    }
    try {
        var r2 = snowflake.execute({sqlText: "CALL " + DB + ".APP.SP_UPDATE_HOUSEHOLD_CLUSTERS('" + DB + "')"});
        r2.next();
        clusterSummary = r2.getColumnValue(1);
    } catch (e) {
        return "SP_UPDATE_HOUSEHOLD_CLUSTERS failed: " + e.message;
    }
    return "Match: " + matchSummary + " | Cluster: " + clusterSummary;
$$;
