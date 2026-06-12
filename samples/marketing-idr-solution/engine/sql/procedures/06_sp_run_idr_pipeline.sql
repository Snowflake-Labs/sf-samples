-- ============================================================================
-- IDR Demo: Main Pipeline Orchestrator
-- Calls all stages and collects metrics
-- Updated to include data standardization step
-- ============================================================================

USE ROLE ACCOUNTADMIN;
USE DATABASE IDR;
USE SCHEMA PROCEDURES;

CREATE OR REPLACE PROCEDURE IDR.PROCEDURES.SP_RUN_IDR_PIPELINE(DEPLOYMENT_DB VARCHAR)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
    var DB = DEPLOYMENT_DB;
    var metrics = {
        start_time: new Date().toISOString(),
        stages: {},
        totals: {}
    };
    var pipelineStart = Date.now();
    var q = String.fromCharCode(39);

    var runIdResult = snowflake.execute({sqlText: "SELECT UUID_STRING()"});
    runIdResult.next();
    var RUN_ID = runIdResult.getColumnValue(1);

    var TASK_QUERY_ID = null;
    try {
        var tqResult = snowflake.execute({sqlText: "SELECT SYSTEM$TASK_RUNTIME_INFO('CURRENT_TASK_GRAPH_RUN_GROUP_ID')"});
        tqResult.next();
        TASK_QUERY_ID = tqResult.getColumnValue(1);
    } catch(e) {}

    function logStageComplete(stageName, durationMs, details) {
        try {
            var safeDetails = JSON.stringify(details).replace(/'/g, "''");
            snowflake.execute({sqlText:
                "INSERT INTO " + DB + ".SILVER.IDR_CORE_EVENT_LOG " +
                "(event_id, event_type, event_source, processing_time_ms, event_details, event_timestamp, run_id, task_query_id) " +
                "SELECT UUID_STRING(), 'IDR_STAGE_COMPLETE', 'SP_RUN_IDR_PIPELINE', " +
                durationMs + ", PARSE_JSON(" + q + safeDetails + q + "), CURRENT_TIMESTAMP(), " +
                q + RUN_ID + q + ", " + (TASK_QUERY_ID ? q + TASK_QUERY_ID + q : "NULL")
            });
        } catch(e) {
            snowflake.log("warn", "Failed to log stage complete for " + stageName + ": " + e.message);
        }
    }

    // =========================================================================
    // Stage -1: Consume Streams
    // Creates _BATCH_{TABLE} temp tables from streams (or base tables if full refresh).
    // These temp tables are used by SP_STANDARDIZE_DATA and SP_CUSTOM_STANDARDIZE.
    // Must run at pipeline top-level so temp tables are visible to all child calls.
    // =========================================================================
    var consumeStart = Date.now();
    var discoverySql = "SELECT TABLE_NAME FROM " + DB + ".SILVER.IDR_CORE_TABLE_METADATA_CACHE ORDER BY PRIORITY NULLS LAST, TABLE_NAME";
    var discoveryResult = snowflake.execute({sqlText: discoverySql});
    var batchTables = [];
    while (discoveryResult.next()) {
        batchTables.push(discoveryResult.getColumnValue('TABLE_NAME'));
    }
    var streamResults = [];
    for (var bi = 0; bi < batchTables.length; bi++) {
        var btName = batchTables[bi];
        var btBatch = '_BATCH_' + btName;
        var btSource = DB + '.BRONZE.' + btName;
        try {
            snowflake.execute({sqlText:
                "CREATE OR REPLACE TRANSIENT TABLE " + DB + ".BRONZE." + btBatch +
                " AS SELECT *, METADATA$ACTION AS _ACTION, METADATA$ISUPDATE AS _ISUPDATE FROM " + btSource + "_STREAM"
            });
            var btCnt = snowflake.execute({sqlText: "SELECT COUNT(*) AS CNT FROM " + DB + ".BRONZE." + btBatch});
            btCnt.next();
            var btRows = btCnt.getColumnValue('CNT');
            streamResults.push({table: btName, rows: btRows});
            snowflake.log("info", "Stream consumed for " + btName + ": " + btRows + " rows");
        } catch(e) {
            snowflake.log("warn", "Failed to consume stream for " + btName + ": " + e.message);
            streamResults.push({table: btName, rows: 0, error: e.message});
        }
    }
    metrics.stages.consume_streams = {
        duration_ms: Date.now() - consumeStart,
        tables: streamResults
    };
    logStageComplete("consume_streams", metrics.stages.consume_streams.duration_ms, {stage: "consume_streams", tables_consumed: batchTables.length});
    
    // =========================================================================
    // Stage 0: Standardize Data
    // Reads from _BATCH_{TABLE} temp tables, applies standardization rules, writes to STD_*
    // =========================================================================
    var stdStart = Date.now();
    snowflake.execute({sqlText: "INSERT INTO " + DB + ".SILVER.IDR_CORE_EVENT_LOG (event_id, event_type, event_source, processing_time_ms, event_details, event_timestamp, run_id, task_query_id) SELECT UUID_STRING(), 'IDR_STAGE_START', 'SP_RUN_IDR_PIPELINE', 0, PARSE_JSON(" + q + JSON.stringify({stage: "standardize", started_at: new Date().toISOString()}).replace(/'/g, "''") + q + "), CURRENT_TIMESTAMP(), " + q + RUN_ID + q + ", " + (TASK_QUERY_ID ? q + TASK_QUERY_ID + q : "NULL")});
    var stdResult = snowflake.execute({sqlText: "CALL IDR.PROCEDURES.SP_STANDARDIZE_ALL_TABLES('" + DB + "', FALSE)"});
    stdResult.next();
    var stdOutput = stdResult.getColumnValue(1);
    
    // stdOutput is a VARIANT (JSON object)
    var stdParsed = typeof stdOutput === 'string' ? JSON.parse(stdOutput) : stdOutput;
    
    var tableCounts = {};
    if (stdParsed.tables_processed) {
        for (var i = 0; i < stdParsed.tables_processed.length; i++) {
            var tbl = stdParsed.tables_processed[i];
            tableCounts[tbl.table] = tbl.row_count;
        }
    }
    
    metrics.stages.standardize = {
        duration_ms: Date.now() - stdStart,
        table_counts: tableCounts,
        total_rows: stdParsed.total_rows || 0,
        status: stdParsed.status
    };
    logStageComplete("standardize", metrics.stages.standardize.duration_ms, {stage: "standardize", total_rows: stdParsed.total_rows || 0, status: stdParsed.status});
    
    // =========================================================================
    // Stage 0b: Custom Standardize Hook (use-case specific)
    // Calls <DB>.APP.SP_CUSTOM_STANDARDIZE() if it exists.
    // Handles VARIANT flattening, polymorphic pivoting, or any domain-specific
    // bronze-to-STD transformation not covered by core IDR_CORE_STANDARDIZATION_RULES.
    // =========================================================================
    var customStdStart = Date.now();
    try {
        snowflake.execute({sqlText: "INSERT INTO " + DB + ".SILVER.IDR_CORE_EVENT_LOG (event_id, event_type, event_source, processing_time_ms, event_details, event_timestamp, run_id, task_query_id) SELECT UUID_STRING(), 'IDR_STAGE_START', 'SP_RUN_IDR_PIPELINE', 0, PARSE_JSON(" + q + JSON.stringify({stage: "custom_standardize", started_at: new Date().toISOString()}).replace(/'/g, "''") + q + "), CURRENT_TIMESTAMP(), " + q + RUN_ID + q + ", " + (TASK_QUERY_ID ? q + TASK_QUERY_ID + q : "NULL")});
        var customStdResult = snowflake.execute({sqlText: "CALL " + DB + ".APP.SP_CUSTOM_STANDARDIZE()"});
        customStdResult.next();
        var customStdMsg = customStdResult.getColumnValue(1);
        metrics.stages.custom_standardize = {
            duration_ms: Date.now() - customStdStart,
            result: customStdMsg
        };
        snowflake.log("info", "Custom standardize hook completed: " + customStdMsg);
        logStageComplete("custom_standardize", metrics.stages.custom_standardize.duration_ms, {stage: "custom_standardize", status: "SUCCESS"});
    } catch (hookErr) {
        metrics.stages.custom_standardize = {
            duration_ms: Date.now() - customStdStart,
            result: "SKIPPED: " + hookErr.message
        };
        logStageComplete("custom_standardize", metrics.stages.custom_standardize.duration_ms, {stage: "custom_standardize", status: "SKIPPED"});
        snowflake.log("info", "Custom standardize hook not found or failed (expected for use-cases without custom logic): " + hookErr.message);
    }
    
    // =========================================================================
    // Stage 1: Extract Identifiers (now reads from STD_* tables)
    // =========================================================================
    var extractStart = Date.now();
    snowflake.execute({sqlText: "INSERT INTO " + DB + ".SILVER.IDR_CORE_EVENT_LOG (event_id, event_type, event_source, processing_time_ms, event_details, event_timestamp, run_id, task_query_id) SELECT UUID_STRING(), 'IDR_STAGE_START', 'SP_RUN_IDR_PIPELINE', 0, PARSE_JSON(" + q + JSON.stringify({stage: "extract", started_at: new Date().toISOString()}).replace(/'/g, "''") + q + "), CURRENT_TIMESTAMP(), " + q + RUN_ID + q + ", " + (TASK_QUERY_ID ? q + TASK_QUERY_ID + q : "NULL")});
    var extractResult = snowflake.execute({sqlText: "CALL IDR.PROCEDURES.SP_EXTRACT_IDENTIFIERS('" + DB + "')"});
    extractResult.next();
    var extractMsg = extractResult.getColumnValue(1);
    metrics.stages.extract = {
        duration_ms: Date.now() - extractStart,
        result: extractMsg
    };
    logStageComplete("extract", metrics.stages.extract.duration_ms, {stage: "extract"});
    
    var sourceRecords = {total_inserts: 0, total_deletes: 0, by_table: {}};
    var tablePattern = /(\w+):\s*(\d+)\s*ins\/(\d+)\s*del/g;
    var tableMatch;
    while ((tableMatch = tablePattern.exec(extractMsg)) !== null) {
        var tName = tableMatch[1];
        var ins = parseInt(tableMatch[2]);
        var del = parseInt(tableMatch[3]);
        sourceRecords.by_table[tName] = {inserts: ins, deletes: del};
        sourceRecords.total_inserts += ins;
        sourceRecords.total_deletes += del;
    }
    metrics.totals.source_records = sourceRecords;
    
    // Get identifier counts by type and source (using normalized model)
    var idCounts = snowflake.execute({sqlText: "SELECT e.identifier_type, l.source_type AS source_system, COUNT(*) as cnt FROM " + DB + ".SILVER.IDR_CORE_IDENTIFIER_LINK l JOIN " + DB + ".SILVER.IDR_CORE_ENTITY_IDENTIFIERS e ON l.identifier_id = e.identifier_id WHERE l.is_active = TRUE AND e.is_active = TRUE GROUP BY 1, 2"});
    metrics.totals.identifiers = {by_type: {}, by_source: {}};
    var totalIdentifiers = 0;
    while (idCounts.next()) {
        var idType = idCounts.getColumnValue(1);
        var source = idCounts.getColumnValue(2);
        var cnt = idCounts.getColumnValue(3);
        totalIdentifiers += cnt;
        metrics.totals.identifiers.by_type[idType] = (metrics.totals.identifiers.by_type[idType] || 0) + cnt;
        metrics.totals.identifiers.by_source[source] = (metrics.totals.identifiers.by_source[source] || 0) + cnt;
    }
    metrics.totals.identifiers.total = totalIdentifiers;
    
    // =========================================================================
    // Stage 2: Run Matching
    // =========================================================================
    var matchStart = Date.now();
    snowflake.execute({sqlText: "INSERT INTO " + DB + ".SILVER.IDR_CORE_EVENT_LOG (event_id, event_type, event_source, processing_time_ms, event_details, event_timestamp, run_id, task_query_id) SELECT UUID_STRING(), 'IDR_STAGE_START', 'SP_RUN_IDR_PIPELINE', 0, PARSE_JSON(" + q + JSON.stringify({stage: "match", started_at: new Date().toISOString()}).replace(/'/g, "''") + q + "), CURRENT_TIMESTAMP(), " + q + RUN_ID + q + ", " + (TASK_QUERY_ID ? q + TASK_QUERY_ID + q : "NULL")});
    var matchResult = snowflake.execute({sqlText: "CALL IDR.PROCEDURES.SP_RUN_MATCHING('" + DB + "')"});
    matchResult.next();
    var matchMsg = matchResult.getColumnValue(1);
    metrics.stages.match = {
        duration_ms: Date.now() - matchStart,
        result: matchMsg
    };
    logStageComplete("match", metrics.stages.match.duration_ms, {stage: "match"});
    
    // Get match counts by rule
    var matchCounts = snowflake.execute({sqlText: "SELECT rule_name, COUNT(*) as cnt FROM " + DB + ".SILVER.IDR_CORE_MATCH_RESULTS WHERE is_active = TRUE GROUP BY 1"});
    metrics.totals.matches = {by_rule: {}, total: 0};
    while (matchCounts.next()) {
        var rule = matchCounts.getColumnValue(1);
        var cnt = matchCounts.getColumnValue(2);
        metrics.totals.matches.by_rule[rule] = cnt;
        metrics.totals.matches.total += cnt;
    }
    
    // =========================================================================
    // Stage 3 (cluster) was moved to AFTER ML/LLM so R16/R17 edges land in
    // clusters in the same pipeline tick. v1 contract: deterministic -> ML ->
    // LLM -> cluster (single pass). See cluster block below the ml_scoring stage.
    // =========================================================================

    // =========================================================================
    // Stage 3b: ML Scoring Layer (gated by R16 rule active in IDR_MATCHING_RULES)
    // Order: candidate gen → scoring → (nested) LLM adjudication
    // =========================================================================
    var mlStart = Date.now();
    var mlResults = {};

    // Check if any ML rule is active for the INDIVIDUAL track.
    var mlActive = false;
    try {
        var mlGate = snowflake.execute({sqlText:
            "SELECT COUNT(*) AS C FROM " + DB + ".CONFIG.IDR_MATCHING_RULES " +
            "WHERE MATCH_TYPE = 'ML' AND COALESCE(TARGET, 'INDIVIDUAL') = 'INDIVIDUAL' AND IS_ACTIVE = TRUE"});
        mlGate.next();
        mlActive = (mlGate.getColumnValue('C') > 0);
    } catch (gateErr) {
        mlResults.gate_error = gateErr.message;
    }

    if (!mlActive) {
        mlResults.status = 'SKIPPED';
        mlResults.reason = 'NO_ACTIVE_ML_RULE';
    } else {
        try {
            // Optional: SSP-style fingerprint accumulation. Martech does not have
            // this proc — fail gracefully and continue.
            try {
                var fpResult = snowflake.execute({sqlText: "CALL " + DB + ".APP.SP_UPDATE_FINGERPRINT_OBSERVATIONS('" + DB + "')"});
                fpResult.next();
                var fpJson = fpResult.getColumnValue(1);
                try { mlResults.fingerprints = JSON.parse(fpJson); } catch(e) { mlResults.fingerprints = fpJson; }
            } catch (fpErr) {
                mlResults.fingerprints = { status: 'SKIPPED', reason: fpErr.message.substring(0, 200) };
            }

            // Note: martech ML procs are zero-param and read CURRENT_DATABASE().
            var candResult = snowflake.execute({sqlText: "CALL " + DB + ".APP.SP_GENERATE_ML_CANDIDATES()"});
            candResult.next();
            var candJson = candResult.getColumnValue(1);
            try { mlResults.candidates = JSON.parse(candJson); } catch(e) { mlResults.candidates = candJson; }

            var scoreResult = snowflake.execute({sqlText: "CALL " + DB + ".APP.SP_ML_SCORE_CANDIDATES()"});
            scoreResult.next();
            var scoreJson = scoreResult.getColumnValue(1);
            try { mlResults.scoring = JSON.parse(scoreJson); } catch(e) { mlResults.scoring = scoreJson; }

            var mlOnlyDuration = Date.now() - mlStart;
            logStageComplete("ml_scoring_only", mlOnlyDuration, {stage: "ml_scoring_only", status: "SUCCESS"});

            // Nested LLM adjudication — only runs when ML ran AND flag enabled.
            var llmStart = Date.now();
            var llmEnabled = false;
            try {
                var llmGate = snowflake.execute({sqlText:
                    "SELECT CONFIG_VALUE FROM " + DB + ".CONFIG.IDR_ML_AI_EVALUATION_CONFIG " +
                    "WHERE CONFIG_KEY = 'LLM_ADJUDICATION_ENABLED'"});
                if (llmGate.next()) {
                    llmEnabled = (String(llmGate.getColumnValue(1)).toUpperCase() === 'TRUE');
                }
            } catch (lge) {
                mlResults.llm_gate_error = lge.message;
            }

            if (llmEnabled) {
                try {
                    var llmResult = snowflake.execute({sqlText: "CALL " + DB + ".APP.SP_LLM_ADJUDICATE_PAIRS()"});
                    llmResult.next();
                    var llmJson = llmResult.getColumnValue(1);
                    try { mlResults.llm = JSON.parse(llmJson); } catch(e) { mlResults.llm = llmJson; }
                    // Check for silent parse failures
                    if (mlResults.llm && mlResults.llm.adjudicated > 0 &&
                        (mlResults.llm.match || 0) === 0 && (mlResults.llm.not_match || 0) === 0) {
                        mlResults.llm.warning = mlResults.llm.warning ||
                            'ALL_PARSES_MAY_HAVE_FAILED: adjudicated=' + mlResults.llm.adjudicated +
                            ' but match+not_match=0. Check LLM_RAW_OUTPUT in LLM_REVIEW_QUEUE.';
                    }
                } catch (llmErr) {
                    mlResults.llm = { status: 'ERROR', error: llmErr.message };
                }
            } else {
                mlResults.llm = { status: 'SKIPPED', reason: 'LLM_ADJUDICATION_ENABLED=FALSE' };
            }

            var llmDuration = Date.now() - llmStart;
            logStageComplete("llm_adjudication", llmDuration, {stage: "llm_adjudication", status: mlResults.llm ? (mlResults.llm.status || "SUCCESS") : "SKIPPED"});

            mlResults.status = 'SUCCESS';
        } catch (mlErr) {
            mlResults.status = 'ERROR';
            mlResults.error = mlErr.message;
            snowflake.log("info", "ML scoring layer error: " + mlErr.message);
        }
    }
    var mlPairsIdentified = 0;
    var mlPairsScored = 0;
    var mlDistribution = {};
    var mlAvgScore = 0;
    if (mlResults.candidates) { if (typeof mlResults.candidates === 'object') {
        mlPairsIdentified = (mlResults.candidates.total_pairs || 0);
    }}
    var mlRescored = 0;
    var mlUpgraded = 0;
    if (mlResults.scoring) { if (typeof mlResults.scoring === 'object') {
        mlPairsScored = (mlResults.scoring.pairs_scored || 0);
        mlDistribution = (mlResults.scoring.distribution || {});
        mlAvgScore = (mlResults.scoring.avg_score || 0);
        mlRescored = (mlResults.scoring.rescored || 0);
        mlUpgraded = (mlResults.scoring.upgraded || 0);
    }}
    metrics.stages.ml_scoring = {
        duration_ms: Date.now() - mlStart,
        results: mlResults,
        ml_pairs_identified: mlPairsIdentified,
        ml_pairs_scored: mlPairsScored,
        ml_distribution: mlDistribution,
        ml_avg_score: mlAvgScore,
        ml_rescored: mlRescored,
        ml_upgraded: mlUpgraded
    };
    logStageComplete("ml_scoring", metrics.stages.ml_scoring.duration_ms, {stage: "ml_scoring", status: mlResults.status || "UNKNOWN", pairs_scored: mlPairsScored, pairs_identified: mlPairsIdentified});

    // =========================================================================
    // Stage 3 (relocated): Update Clusters
    // Runs AFTER ML scoring + LLM adjudication so R16 (AUTO_MERGE) and R17
    // (LLM-MATCH) edges are folded into clusters in the same pipeline tick.
    // =========================================================================
    var clusterStart = Date.now();
    snowflake.execute({sqlText: "INSERT INTO " + DB + ".SILVER.IDR_CORE_EVENT_LOG (event_id, event_type, event_source, processing_time_ms, event_details, event_timestamp, run_id, task_query_id) SELECT UUID_STRING(), 'IDR_STAGE_START', 'SP_RUN_IDR_PIPELINE', 0, PARSE_JSON(" + q + JSON.stringify({stage: "cluster", started_at: new Date().toISOString()}).replace(/'/g, "''") + q + "), CURRENT_TIMESTAMP(), " + q + RUN_ID + q + ", " + (TASK_QUERY_ID ? q + TASK_QUERY_ID + q : "NULL")});
    var clusterResult = snowflake.execute({sqlText: "CALL IDR.PROCEDURES.SP_UPDATE_CLUSTERS('" + DB + "')"});
    clusterResult.next();
    var clusterMsg = clusterResult.getColumnValue(1);
    metrics.stages.cluster = {
        duration_ms: Date.now() - clusterStart,
        result: clusterMsg
    };
    logStageComplete("cluster", metrics.stages.cluster.duration_ms, {stage: "cluster"});

    // Parse cluster/profile changes from result
    var createdMatch = clusterMsg.match(/(\d+) created/);
    var updatedMatch = clusterMsg.match(/(\d+) updated/);
    var mergedMatch = clusterMsg.match(/(\d+) merges/);

    metrics.totals.profiles = {
        created: createdMatch ? parseInt(createdMatch[1]) : 0,
        updated: updatedMatch ? parseInt(updatedMatch[1]) : 0,
        merged: mergedMatch ? parseInt(mergedMatch[1]) : 0
    };

    // Keep backward compatibility with old field name
    metrics.totals.records = metrics.totals.profiles;

    // Get cluster stats
    var clusterStats = snowflake.execute({sqlText: "SELECT status, COUNT(*) as cnt FROM " + DB + ".SILVER.IDR_CORE_CLUSTER GROUP BY 1"});
    metrics.totals.clusters = {by_status: {}, total: 0};
    while (clusterStats.next()) {
        var status = clusterStats.getColumnValue(1);
        var cnt = clusterStats.getColumnValue(2);
        metrics.totals.clusters.by_status[status] = cnt;
        metrics.totals.clusters.total += cnt;
    }

    var activeClusters = snowflake.execute({sqlText: "SELECT COUNT(*) FROM " + DB + ".SILVER.IDR_CORE_CLUSTER WHERE status = " + q + "ACTIVE" + q});
    activeClusters.next();
    metrics.totals.active_clusters = activeClusters.getColumnValue(1);
    metrics.totals.active_travelers = metrics.totals.active_clusters;

    // =========================================================================
    // Stage 4: Build Profile Index (blocking tokens for AI second pass)
    // =========================================================================
    var profileStart = Date.now();
    snowflake.execute({sqlText: "INSERT INTO " + DB + ".SILVER.IDR_CORE_EVENT_LOG (event_id, event_type, event_source, processing_time_ms, event_details, event_timestamp, run_id, task_query_id) SELECT UUID_STRING(), 'IDR_STAGE_START', 'SP_RUN_IDR_PIPELINE', 0, PARSE_JSON(" + q + JSON.stringify({stage: "profile_index", started_at: new Date().toISOString()}).replace(/'/g, "''") + q + "), CURRENT_TIMESTAMP(), " + q + RUN_ID + q + ", " + (TASK_QUERY_ID ? q + TASK_QUERY_ID + q : "NULL")});
    var profileResult = snowflake.execute({sqlText: "CALL IDR.PROCEDURES.SP_BUILD_PROFILE_INDEX('" + DB + "', FALSE)"});
    profileResult.next();
    var profileMsg = profileResult.getColumnValue(1);
    metrics.stages.profile_index = {
        duration_ms: Date.now() - profileStart,
        result: profileMsg
    };
    logStageComplete("profile_index", metrics.stages.profile_index.duration_ms, {stage: "profile_index"});
    
    // Parse profile index update count
    var profileUpdatedMatch = profileMsg.match(/Updated (\d+) clusters/);
    metrics.totals.profile_index = {
        clusters_updated: profileUpdatedMatch ? parseInt(profileUpdatedMatch[1]) : 0
    };
    
    // =========================================================================
    // Stage 5: Create Golden Records (use-case SP in <DB>.SILVER)
    // =========================================================================
    var goldenStart = Date.now();
    snowflake.execute({sqlText: "INSERT INTO " + DB + ".SILVER.IDR_CORE_EVENT_LOG (event_id, event_type, event_source, processing_time_ms, event_details, event_timestamp, run_id, task_query_id) SELECT UUID_STRING(), 'IDR_STAGE_START', 'SP_RUN_IDR_PIPELINE', 0, PARSE_JSON(" + q + JSON.stringify({stage: "golden_records", started_at: new Date().toISOString()}).replace(/'/g, "''") + q + "), CURRENT_TIMESTAMP(), " + q + RUN_ID + q + ", " + (TASK_QUERY_ID ? q + TASK_QUERY_ID + q : "NULL")});
    try {
        var goldenResult = snowflake.execute({sqlText: "CALL " + DB + ".SILVER.SP_CREATE_GOLDEN_RECORDS()"});
        goldenResult.next();
        var goldenMsg = goldenResult.getColumnValue(1);
        metrics.stages.golden_records = {
            duration_ms: Date.now() - goldenStart,
            result: goldenMsg
        };
        logStageComplete("golden_records", metrics.stages.golden_records.duration_ms, {stage: "golden_records", status: "SUCCESS"});
    } catch (err) {
        metrics.stages.golden_records = {
            duration_ms: Date.now() - goldenStart,
            result: "SKIPPED: " + err.message
        };
        logStageComplete("golden_records", metrics.stages.golden_records.duration_ms, {stage: "golden_records", status: "SKIPPED"});
    }
    
    // Calculate totals
    metrics.total_duration_ms = Date.now() - pipelineStart;
    metrics.end_time = new Date().toISOString();
    
    // Log to event table
    var uuidResult = snowflake.execute({sqlText: "SELECT UUID_STRING()"});
    uuidResult.next();
    var eventId = uuidResult.getColumnValue(1);
    
    // Sanitize metrics - remove problematic characters from result strings
    function sanitizeForJson(obj) {
        if (typeof obj === 'string') {
            return obj.replace(/[\n\r\t]/g, ' ').replace(/\\/g, '\\\\').replace(/"/g, '\\"');
        } else if (typeof obj === 'object' && obj !== null) {
            if (Array.isArray(obj)) {
                return obj.map(sanitizeForJson);
            } else {
                var result = {};
                for (var key in obj) {
                    if (obj.hasOwnProperty(key)) {
                        result[key] = sanitizeForJson(obj[key]);
                    }
                }
                return result;
            }
        }
        return obj;
    }
    var sanitizedMetrics = sanitizeForJson(metrics);
    var metricsJson = JSON.stringify(sanitizedMetrics).replace(/'/g, "''");
    snowflake.execute({sqlText: "INSERT INTO " + DB + ".SILVER.IDR_CORE_EVENT_LOG (event_id, event_type, event_source, processing_time_ms, event_details, event_timestamp, run_id, task_query_id) SELECT " + q + eventId + q + ", " + q + "IDR_COMPLETE" + q + ", " + q + "SP_RUN_IDR_PIPELINE" + q + ", " + metrics.total_duration_ms + ", PARSE_JSON(" + q + metricsJson + q + "), CURRENT_TIMESTAMP(), " + q + RUN_ID + q + ", " + (TASK_QUERY_ID ? q + TASK_QUERY_ID + q : "NULL")});
    
    var stdParts = [];
    var tc = metrics.stages.standardize.table_counts;
    for (var tKey in tc) {
        if (tc.hasOwnProperty(tKey)) {
            stdParts.push(tc[tKey] + " " + tKey.toLowerCase());
        }
    }
    var srcIns = metrics.totals.source_records.total_inserts;
    var srcDel = metrics.totals.source_records.total_deletes;
    var profCreated = metrics.totals.profiles.created;
    var profUpdated = metrics.totals.profiles.updated;
    var profMerged = metrics.totals.profiles.merged;
    
    // =========================================================================
    // Cleanup: Drop _BATCH transient tables
    // =========================================================================
    for (var ci = 0; ci < batchTables.length; ci++) {
        try {
            snowflake.execute({sqlText: "DROP TABLE IF EXISTS " + DB + ".BRONZE._BATCH_" + batchTables[ci]});
        } catch(ce) {
            snowflake.log("warn", "Failed to drop batch table _BATCH_" + batchTables[ci] + ": " + ce.message);
        }
    }
    
    return "IDR Pipeline completed in " + metrics.total_duration_ms + "ms: Standardized(" + stdParts.join(", ") + ") Sources(" + srcIns + " ins, " + srcDel + " del) Profiles(" + profCreated + " new, " + profUpdated + " updated, " + profMerged + " merged) " + metrics.totals.active_clusters + " clusters";
$$;
