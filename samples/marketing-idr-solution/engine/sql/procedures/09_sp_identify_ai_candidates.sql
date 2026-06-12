-- ============================================================================
-- SP_IDENTIFY_AI_CANDIDATES
-- Metadata-driven candidate pair generation for AI second pass
-- Uses blocking configuration from IDR_ML_AI_BLOCKING_CONFIG table
-- Supports incremental processing (only new/updated clusters)
-- ============================================================================

USE ROLE ACCOUNTADMIN;
USE DATABASE IDR;
USE WAREHOUSE IDR_DEMO_WH;

CREATE OR REPLACE PROCEDURE IDR.PROCEDURES.SP_IDENTIFY_AI_CANDIDATES(
    DEPLOYMENT_DB VARCHAR,
    MAX_CANDIDATES_PER_TIER FLOAT DEFAULT 1000,
    MIN_WEIGHT_THRESHOLD FLOAT DEFAULT 0,
    FULL_REBUILD BOOLEAN DEFAULT FALSE
)
RETURNS VARIANT
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
    var DB = DEPLOYMENT_DB;
    var startTime = Date.now();
    var maxCandidates = Math.floor(MAX_CANDIDATES_PER_TIER);
    var minWeight = Math.floor(MIN_WEIGHT_THRESHOLD);
    var fullRebuild = FULL_REBUILD;
    var results = {
        success: true,
        mode: fullRebuild ? 'FULL_REBUILD' : 'INCREMENTAL',
        tiers_processed: [],
        total_candidates: 0,
        candidates_by_tier: {},
        clusters_scanned: 0,
        stoplist_filtered: 0,
        duplicate_filtered: 0,
        errors: []
    };
    
    try {
        // Get last run timestamp for incremental processing
        var lastRunTimestamp = null;
        if (!fullRebuild) {
            var lastRunQuery = `
                SELECT LAST_RUN_AT 
                FROM ` + DB + `.SILVER.IDR_CORE_PROCESS_STATE 
                WHERE PROCESS_NAME = 'IDENTIFY_AI_CANDIDATES'
            `;
            var lastRunStmt = snowflake.createStatement({sqlText: lastRunQuery});
            var lastRunResult = lastRunStmt.execute();
            if (lastRunResult.next()) {
                lastRunTimestamp = lastRunResult.getColumnValue('LAST_RUN_AT');
            }
        }
        
        results.last_run_at = lastRunTimestamp;
        
        // If full rebuild, clear existing PENDING candidates
        if (fullRebuild) {
            var clearQuery = `DELETE FROM ` + DB + `.SILVER.IDR_ML_AI_CANDIDATE_PAIRS WHERE STATUS = 'PENDING'`;
            var clearStmt = snowflake.createStatement({sqlText: clearQuery});
            clearStmt.execute();
            results.cleared_pending = clearStmt.getNumRowsAffected();
        }
        
        // Get enabled blocking configurations
        var configQuery = `
            SELECT 
                BLOCKING_TIER,
                BLOCKING_NAME,
                BLOCKING_KEY_COLUMN,
                WEIGHT,
                MIN_CLUSTER_SIZE
            FROM ` + DB + `.CONFIG.IDR_ML_AI_BLOCKING_CONFIG
            WHERE IS_ENABLED = TRUE
              AND WEIGHT >= ` + minWeight + `
            ORDER BY WEIGHT DESC, BLOCKING_TIER ASC
        `;
        
        var configStmt = snowflake.createStatement({sqlText: configQuery});
        var configResult = configStmt.execute();
        
        var blockingConfigs = [];
        while (configResult.next()) {
            blockingConfigs.push({
                tier: configResult.getColumnValue('BLOCKING_TIER'),
                name: configResult.getColumnValue('BLOCKING_NAME'),
                column: configResult.getColumnValue('BLOCKING_KEY_COLUMN'),
                weight: configResult.getColumnValue('WEIGHT'),
                minClusterSize: configResult.getColumnValue('MIN_CLUSTER_SIZE')
            });
        }
        
        if (blockingConfigs.length === 0) {
            results.success = false;
            results.errors.push('No enabled blocking configurations found');
            return results;
        }
        
        // Build incremental filter clause
        var incrementalFilter = '';
        if (!fullRebuild && lastRunTimestamp) {
            // Format timestamp properly for SQL
            var tsStr = lastRunTimestamp.toISOString().replace('T', ' ').replace('Z', '');
            incrementalFilter = ` AND (c1.PROFILE_UPDATED_AT > '` + tsStr + `' OR c2.PROFILE_UPDATED_AT > '` + tsStr + `')`;
            results.incremental_filter = 'PROFILE_UPDATED_AT > ' + tsStr;
        }
        
        // Process each blocking tier
        for (var i = 0; i < blockingConfigs.length; i++) {
            var config = blockingConfigs[i];
            var tierStart = Date.now();
            
            try {
                // Generate candidate pairs for this tier
                // Self-join on blocking key, excluding:
                // 1. Same cluster pairs
                // 2. Already identified candidates
                // 3. Stoplist values
                // 4. NULL blocking keys
                // 5. Unchanged clusters (incremental mode)
                var candidateQuery = `
                    INSERT INTO ` + DB + `.SILVER.IDR_ML_AI_CANDIDATE_PAIRS 
                        (CANDIDATE_ID, CLUSTER_ID_1, CLUSTER_ID_2, BLOCKING_TIER, BLOCKING_NAME, BLOCKING_KEY_VALUE, WEIGHT, STATUS)
                    SELECT DISTINCT
                        UUID_STRING() AS CANDIDATE_ID,
                        LEAST(c1.CLUSTER_ID, c2.CLUSTER_ID) AS CLUSTER_ID_1,
                        GREATEST(c1.CLUSTER_ID, c2.CLUSTER_ID) AS CLUSTER_ID_2,
                        ` + config.tier + ` AS BLOCKING_TIER,
                        '` + config.name + `' AS BLOCKING_NAME,
                        c1.` + config.column + ` AS BLOCKING_KEY_VALUE,
                        ` + config.weight + ` AS WEIGHT,
                        'PENDING' AS STATUS
                    FROM ` + DB + `.SILVER.IDR_CORE_CLUSTER c1
                    JOIN ` + DB + `.SILVER.IDR_CORE_CLUSTER c2
                        ON c1.` + config.column + ` = c2.` + config.column + `
                        AND c1.CLUSTER_ID < c2.CLUSTER_ID
                    WHERE c1.STATUS = 'ACTIVE'
                      AND c2.STATUS = 'ACTIVE'
                      AND c1.` + config.column + ` IS NOT NULL
                      AND c1.` + config.column + ` != ''
                      AND ARRAY_SIZE(c1.SOURCE_RECORD_IDS) >= ` + config.minClusterSize + `
                      AND ARRAY_SIZE(c2.SOURCE_RECORD_IDS) >= ` + config.minClusterSize + `
                      -- Exclude pairs already in candidates table (any status)
                      AND NOT EXISTS (
                          SELECT 1 FROM ` + DB + `.SILVER.IDR_ML_AI_CANDIDATE_PAIRS cp
                          WHERE (cp.CLUSTER_ID_1 = LEAST(c1.CLUSTER_ID, c2.CLUSTER_ID)
                             AND cp.CLUSTER_ID_2 = GREATEST(c1.CLUSTER_ID, c2.CLUSTER_ID))
                      )
                      -- Exclude stoplist values
                      AND NOT EXISTS (
                          SELECT 1 FROM ` + DB + `.CONFIG.AI_BLOCKING_STOPLIST sl
                          WHERE sl.BLOCKING_NAME = '` + config.name + `'
                            AND sl.BLOCKING_KEY_VALUE = c1.` + config.column + `
                      )
                      ` + incrementalFilter + `
                    LIMIT ` + maxCandidates + `
                `;
                
                var candidateStmt = snowflake.createStatement({sqlText: candidateQuery});
                var candidateResult = candidateStmt.execute();
                var rowsInserted = candidateStmt.getNumRowsAffected();
                
                results.tiers_processed.push(config.name);
                results.candidates_by_tier[config.name] = {
                    tier: config.tier,
                    weight: config.weight,
                    candidates: rowsInserted,
                    duration_ms: Date.now() - tierStart
                };
                results.total_candidates += rowsInserted;
                
            } catch (tierErr) {
                results.errors.push({
                    tier: config.tier,
                    name: config.name,
                    error: tierErr.message
                });
            }
        }
        
        // Update stoplist with high-frequency blocking keys
        var stoplistQuery = `
            MERGE INTO ` + DB + `.CONFIG.AI_BLOCKING_STOPLIST tgt
            USING (
                SELECT 
                    BLOCKING_NAME,
                    BLOCKING_KEY_VALUE,
                    COUNT(*) AS FREQUENCY
                FROM ` + DB + `.SILVER.IDR_ML_AI_CANDIDATE_PAIRS
                WHERE STATUS = 'PENDING'
                GROUP BY BLOCKING_NAME, BLOCKING_KEY_VALUE
                HAVING COUNT(*) > 100  -- Threshold for stoplist
            ) src
            ON tgt.BLOCKING_NAME = src.BLOCKING_NAME 
               AND tgt.BLOCKING_KEY_VALUE = src.BLOCKING_KEY_VALUE
            WHEN NOT MATCHED THEN INSERT 
                (BLOCKING_NAME, BLOCKING_KEY_VALUE, FREQUENCY, REASON)
            VALUES 
                (src.BLOCKING_NAME, src.BLOCKING_KEY_VALUE, src.FREQUENCY, 'Auto-detected high frequency')
        `;
        
        try {
            var stoplistStmt = snowflake.createStatement({sqlText: stoplistQuery});
            stoplistStmt.execute();
            results.stoplist_updated = stoplistStmt.getNumRowsAffected();
        } catch (slErr) {
            results.errors.push({step: 'stoplist_update', error: slErr.message});
        }
        
        // Update process state with current run timestamp
        var updateStateQuery = `
            MERGE INTO ` + DB + `.SILVER.IDR_CORE_PROCESS_STATE tgt
            USING (SELECT 'IDENTIFY_AI_CANDIDATES' AS PROCESS_NAME) src
            ON tgt.PROCESS_NAME = src.PROCESS_NAME
            WHEN MATCHED THEN UPDATE SET
                LAST_RUN_AT = CURRENT_TIMESTAMP(),
                LAST_RUN_STATUS = 'SUCCESS',
                LAST_RUN_DETAILS = PARSE_JSON('` + JSON.stringify(results).replace(/'/g, "''") + `')
            WHEN NOT MATCHED THEN INSERT 
                (PROCESS_NAME, LAST_RUN_AT, LAST_RUN_STATUS, LAST_RUN_DETAILS)
            VALUES 
                ('IDENTIFY_AI_CANDIDATES', CURRENT_TIMESTAMP(), 'SUCCESS', PARSE_JSON('` + JSON.stringify(results).replace(/'/g, "''") + `'))
        `;
        
        try {
            var updateStateStmt = snowflake.createStatement({sqlText: updateStateQuery});
            updateStateStmt.execute();
        } catch (stateErr) {
            results.errors.push({step: 'update_state', error: stateErr.message});
        }
        
        results.duration_ms = Date.now() - startTime;
        
    } catch (err) {
        results.success = false;
        results.errors.push(err.message);
    }
    
    return results;
$$;

-- Test the procedure
-- CALL SP_IDENTIFY_AI_CANDIDATES(100, 0, FALSE);  -- Incremental
-- CALL SP_IDENTIFY_AI_CANDIDATES(100, 0, TRUE);   -- Full rebuild

SELECT 'SP_IDENTIFY_AI_CANDIDATES (with incremental support) created successfully' AS STATUS;
