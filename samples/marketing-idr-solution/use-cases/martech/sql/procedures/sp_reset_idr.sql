-- ============================================================================
-- Martech: APP.SP_RESET_IDR
-- Full nuclear reset: truncates BRONZE + SILVER, recreates streams.
-- Matches SSP gold-standard reset behavior.
-- ============================================================================

CREATE OR REPLACE PROCEDURE IDENTIFIER('&{deployment_db}.APP.SP_RESET_IDR')()
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
    var dbResult = snowflake.execute({sqlText: "SELECT CURRENT_DATABASE() AS DB"});
    dbResult.next();
    var DB = dbResult.getColumnValue('DB');

    var truncated = [];

    // --- BRONZE layer ---
    var bronzeTables = [
        'POS_TRANSACTION_RAW', 'LOYALTY_MEMBER_RAW',
        'WEB_CLICKSTREAM_RAW', 'SHOPIFY_ORDER_RAW'
    ];
    for (var b = 0; b < bronzeTables.length; b++) {
        try {
            snowflake.execute({sqlText: "TRUNCATE TABLE " + DB + ".BRONZE." + bronzeTables[b]});
            truncated.push("BRONZE." + bronzeTables[b]);
        } catch (e) { /* table may not exist yet */ }
    }

    // --- SILVER layer ---
    var silverTables = [
        'IDR_CORE_ENTITY_IDENTIFIERS', 'IDR_CORE_IDENTIFIER_LINK',
        'IDR_CORE_CLUSTER', 'IDR_CORE_CLUSTER_MEMBERSHIP',
        'IDR_CORE_CLUSTER_LOG', 'IDR_CORE_MATCH_RESULTS', 'IDR_CORE_MATCH_LOG',
        'IDR_CORE_PROCESS_STATE', 'IDR_CORE_EVENT_LOG',
        'STD_POS_TRANSACTION_RAW', 'STD_LOYALTY_MEMBER_RAW',
        'STD_WEB_CLICKSTREAM_RAW', 'STD_SHOPIFY_ORDER_RAW',
        'ML_PAIR_FEATURES', 'LLM_REVIEW_QUEUE',
        'IDR_ML_INDIVIDUAL_MATCH_EXPLAIN',
        'IDR_CORE_HOUSEHOLD_CLUSTER', 'IDR_CORE_HOUSEHOLD_MATCH_RESULTS',
        'IDR_CORE_HOUSEHOLD_MEMBERSHIP'
    ];
    for (var i = 0; i < silverTables.length; i++) {
        try {
            snowflake.execute({sqlText: "TRUNCATE TABLE " + DB + ".SILVER." + silverTables[i]});
            truncated.push("SILVER." + silverTables[i]);
        } catch (e) { /* table may not exist yet */ }
    }

    // --- Recreate streams on now-empty bronze tables ---
    var streams = [
        ['POS_TRANSACTION_STREAM',  'POS_TRANSACTION_RAW'],
        ['LOYALTY_MEMBER_STREAM',   'LOYALTY_MEMBER_RAW'],
        ['WEB_CLICKSTREAM_STREAM',  'WEB_CLICKSTREAM_RAW'],
        ['SHOPIFY_ORDER_STREAM',    'SHOPIFY_ORDER_RAW']
    ];
    for (var s = 0; s < streams.length; s++) {
        snowflake.execute({sqlText:
            "CREATE OR REPLACE STREAM " + DB + ".BRONZE." + streams[s][0] +
            " ON TABLE " + DB + ".BRONZE." + streams[s][1] +
            " APPEND_ONLY = TRUE SHOW_INITIAL_ROWS = FALSE"});
    }

    return JSON.stringify({ truncated: truncated, streams_recreated: streams.map(function(x){return x[0];}) });
$$;
