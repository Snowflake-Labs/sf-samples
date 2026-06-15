/*
 * ============================================================================
 * SP_STANDARDIZE_DATA - Generic Convention-Based Data Standardization
 * ============================================================================
 * 
 * Purpose:
 *   Standardizes data from any BRONZE table based on SEMANTIC_CATEGORY tags
 *   applied via SYSTEM$CLASSIFY. Fully dynamic - discovers columns at runtime.
 * 
 * Parameters:
 *   SOURCE_TABLE VARCHAR - Name of the source table in BRONZE schema
 *                         e.g., 'BOOKING', 'LOYALTY_MEMBER'
 *   HISTORICAL_REFRESH BOOLEAN (default FALSE)
 *     - FALSE: Read from BRONZE stream (incremental)
 *     - TRUE:  Truncate STD_* table, read from BRONZE base table (full refresh)
 * 
 * NAMING CONVENTIONS (CRITICAL):
 *   Source table:      BRONZE.{TABLE_NAME}
 *   Target table:      SILVER.STD_{TABLE_NAME}
 *   Stream:            BRONZE.{TABLE_NAME}_STREAM
 *   STD column:        {SOURCE_COLUMN}_STD
 *   Canonical column:  {SOURCE_COLUMN}_CANONICAL (NAME category only)
 * 
 * How It Works:
 *   1. Derives table names from conventions
 *   2. Queries INFORMATION_SCHEMA.COLUMNS for all source columns
 *   3. Queries TAG_REFERENCES_ALL_COLUMNS for tagged columns
 *   4. Joins with IDR_CORE_STANDARDIZATION_RULES for transformation SQL
 *   5. Builds and executes dynamic INSERT statement
 * 
 * Dependencies:
 *   - SILVER.IDR_CORE_STANDARDIZATION_RULES - Transformation SQL per semantic category
 *   - SILVER.IDR_CORE_NICKNAME_MAP - Canonical name lookups for NAME category
 *   - Source table must have SEMANTIC_CATEGORY tags via SYSTEM$CLASSIFY
 *   - Target STD_* table must exist with proper column naming
 * 
 * Logging:
 *   Uses snowflake.log() to write to IDR_DEMO.SILVER.SP_EVENT_LOG event table.
 *   Query logs: SELECT * FROM IDR_DEMO.SILVER.SP_EVENT_LOG WHERE SCOPE['name'] = 'SP_STANDARDIZE_DATA';
 * 
 * Usage:
 *   -- Single table
 *   CALL IDR_DEMO.SILVER.SP_STANDARDIZE_DATA('BOOKING', TRUE);
 *   CALL IDR_DEMO.SILVER.SP_STANDARDIZE_DATA('CUSTOMER_BASE', FALSE);
 *   
 *   -- All tables (use wrapper)
 *   CALL IDR_DEMO.SILVER.SP_STANDARDIZE_ALL_TABLES(TRUE);
 * 
 * Returns:
 *   JSON: {source_table, row_count, batch_id, status, errors}
 * 
 * ============================================================================
 */

CREATE OR REPLACE PROCEDURE IDR.PROCEDURES.SP_STANDARDIZE_DATA(
    DEPLOYMENT_DB VARCHAR,
    SOURCE_TABLE VARCHAR,
    HISTORICAL_REFRESH BOOLEAN DEFAULT FALSE
)
RETURNS VARIANT
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
    var DB = DEPLOYMENT_DB;
    var sourceTable = SOURCE_TABLE.toUpperCase();
    var historicalRefresh = HISTORICAL_REFRESH;
    
    var result = {
        source_table: sourceTable,
        row_count: 0,
        batch_id: null,
        historical_refresh: historicalRefresh,
        status: 'SUCCESS',
        errors: [],
        metadata_driven: true,
        convention_based: true
    };
    
    snowflake.log("info", "SP_STANDARDIZE_DATA started for table: " + sourceTable + ", historical_refresh: " + historicalRefresh);
    
    try {
        snowflake.execute({sqlText: "USE DATABASE " + DB});
        
        var rulesCount = 0;
        try {
            var rulesCheck = snowflake.execute({sqlText:
                "SELECT COUNT(*) AS CNT FROM " + DB + ".CONFIG.IDR_CORE_STANDARDIZATION_RULES WHERE IS_ACTIVE = TRUE"
            });
            rulesCheck.next();
            rulesCount = rulesCheck.getColumnValue('CNT');
        } catch(e) { }
        
        var vectorEnabled = false;
        try {
            var vecCheck = snowflake.execute({sqlText:
                "SELECT COUNT(*) AS CNT FROM " + DB + ".CONFIG.IDR_MATCHING_RULES " +
                "WHERE MATCH_TYPE = 'VECTOR' AND IS_ACTIVE = TRUE"
            });
            vecCheck.next();
            vectorEnabled = (vecCheck.getColumnValue('CNT') > 0);
        } catch(e) { }
        
        var sourceFQ = DB + '.BRONZE.' + sourceTable;
        var targetFQ = DB + '.SILVER.STD_' + sourceTable;
        var batchTable = DB + '.BRONZE._BATCH_' + sourceTable;
        
        snowflake.log("info", "Source: " + sourceFQ + ", Target: " + targetFQ + ", Batch: " + batchTable);
        
        var batchIdResult = snowflake.execute({sqlText: "SELECT UUID_STRING() AS BATCH_ID"});
        batchIdResult.next();
        result.batch_id = batchIdResult.getColumnValue('BATCH_ID');
        
        snowflake.log("info", "Batch ID: " + result.batch_id);
        
        if (historicalRefresh) {
            snowflake.log("info", "Historical refresh: truncating " + targetFQ);
            snowflake.execute({sqlText: "TRUNCATE TABLE " + targetFQ});
        }
        
        try {
            var countResult = snowflake.execute({sqlText: "SELECT COUNT(*) AS CNT FROM " + batchTable});
            countResult.next();
            result.row_count = countResult.getColumnValue('CNT');
        } catch(e) {
            snowflake.log("info", "No batch table " + batchTable + " found for " + sourceTable + " - skipping");
            result.status = 'SKIPPED_NO_BATCH';
            result.row_count = 0;
            return result;
        }
        
        snowflake.log("info", "Rows to process: " + result.row_count);
        
        if (rulesCount === 0) {
            snowflake.log("info", "No active IDR_CORE_STANDARDIZATION_RULES for " + DB + ", skipping core standardization for " + sourceTable + " (use-case hook will handle)");
            result.status = 'SKIPPED_NO_RULES';
            return result;
        }
        
        if (result.row_count > 0) {
            var colSql = "SELECT COLUMN_NAME FROM " + DB + ".INFORMATION_SCHEMA.COLUMNS " +
                "WHERE TABLE_SCHEMA = 'BRONZE' AND TABLE_NAME = '" + sourceTable + "' " +
                "ORDER BY ORDINAL_POSITION";
            var colResult = snowflake.execute({sqlText: colSql});
            var originalCols = [];
            while (colResult.next()) {
                originalCols.push(colResult.getColumnValue('COLUMN_NAME'));
            }
            
            snowflake.log("info", "Original columns: " + originalCols.length);
            
            var metaSql = "SELECT " +
                "t.COLUMN_NAME AS SOURCE_COLUMN, " +
                "t.COLUMN_NAME || '_STD' AS STD_COLUMN, " +
                "CASE WHEN t.TAG_VALUE = 'NAME' THEN t.COLUMN_NAME || '_CANONICAL' ELSE NULL END AS CANONICAL_COLUMN, " +
                "r.SQL_EXPRESSION, " +
                "t.TAG_VALUE AS SEMANTIC_CATEGORY " +
                "FROM TABLE(" + DB + ".INFORMATION_SCHEMA.TAG_REFERENCES_ALL_COLUMNS('" + sourceFQ + "', 'TABLE')) t " +
                "JOIN " + DB + ".CONFIG.IDR_CORE_STANDARDIZATION_RULES r ON t.TAG_VALUE = r.SEMANTIC_CATEGORY " +
                "WHERE t.TAG_NAME = 'SEMANTIC_CATEGORY' AND r.IS_ACTIVE = TRUE " +
                "ORDER BY t.COLUMN_NAME";
            
            var metaResult = snowflake.execute({sqlText: metaSql});
            var stdSelectParts = [];
            var stdColNames = [];
            var canonicalJoins = [];
            var joinCounter = 0;
            var taggedColCount = 0;
            var embeddingSourceExprs = {};
            
            while (metaResult.next()) {
                taggedColCount++;
                var srcCol = metaResult.getColumnValue('SOURCE_COLUMN');
                var stdCol = metaResult.getColumnValue('STD_COLUMN');
                var canonicalCol = metaResult.getColumnValue('CANONICAL_COLUMN');
                var sqlExpr = metaResult.getColumnValue('SQL_EXPRESSION');
                var semCat = metaResult.getColumnValue('SEMANTIC_CATEGORY');
                
                snowflake.log("debug", "Processing column: " + srcCol + " (" + semCat + ")");
                
                var expr = sqlExpr.replace(/{col}/g, 'src.' + srcCol);
                stdSelectParts.push(expr);
                stdColNames.push(stdCol);
                
                if (vectorEnabled && ['NAME', 'EMAIL', 'PHONE_NUMBER', 'DATE_OF_BIRTH', 'CITY', 'POSTAL_CODE'].indexOf(semCat) >= 0) {
                    if (!embeddingSourceExprs[semCat]) embeddingSourceExprs[semCat] = [];
                    embeddingSourceExprs[semCat].push("COALESCE(CAST(" + expr + " AS VARCHAR), '')");
                }
                
                if (canonicalCol && semCat === 'NAME') {
                    joinCounter++;
                    var alias = 'nm' + joinCounter;
                    canonicalJoins.push('LEFT JOIN ' + DB + '.CONFIG.IDR_CORE_NICKNAME_MAP ' + alias + 
                        ' ON UPPER(TRIM(src.' + srcCol + ')) = ' + alias + '.NICKNAME');
                    stdSelectParts.push('COALESCE(' + alias + '.CANONICAL_NAME, ' + expr + ')');
                    stdColNames.push(canonicalCol);
                }
            }
            
            if (vectorEnabled) {
                var allEmbeddingParts = [];
                var catOrder = ['NAME', 'EMAIL', 'PHONE_NUMBER', 'DATE_OF_BIRTH', 'CITY', 'POSTAL_CODE'];
                for (var c = 0; c < catOrder.length; c++) {
                    if (embeddingSourceExprs[catOrder[c]]) {
                        allEmbeddingParts = allEmbeddingParts.concat(embeddingSourceExprs[catOrder[c]]);
                    }
                }
                if (allEmbeddingParts.length > 0) {
                    var embeddingExpr = "SNOWFLAKE.CORTEX.EMBED_TEXT_768(" +
                        "'snowflake-arctic-embed-m-v1.5', " +
                        "CONCAT_WS(' ', " + allEmbeddingParts.join(", ") + ")" +
                        ")";
                    stdSelectParts.push(embeddingExpr);
                    stdColNames.push('SOURCE_EMBEDDING');
                    snowflake.log("info", "Vector embedding enabled: added SOURCE_EMBEDDING column");
                }
            }
            
            snowflake.log("info", "Tagged columns with rules: " + taggedColCount + ", STD columns: " + stdColNames.length);
            
            var insertCols = originalCols.concat(stdColNames).concat(['STD_BATCH_ID', 'METADATA_ACTION', 'METADATA_ISUPDATE']);
            
            var selectExprs = originalCols.map(function(c) { return 'src.' + c; });
            selectExprs = selectExprs.concat(stdSelectParts);
            selectExprs.push("'" + result.batch_id + "'");
            selectExprs.push('src._ACTION');
            selectExprs.push('src._ISUPDATE');
            
            var insertSql = "INSERT INTO " + targetFQ + " (" + insertCols.join(', ') + ") " +
                "SELECT " + selectExprs.join(', ') + " " +
                "FROM " + batchTable + " src " + canonicalJoins.join(' ');
            
            snowflake.log("debug", "Executing INSERT into " + targetFQ);
            snowflake.log("info","Final query: "+insertSql)
            snowflake.execute({sqlText: insertSql});
            snowflake.log("info", "Inserted " + result.row_count + " rows into " + targetFQ);
        } else {
            snowflake.log("info", "No rows to process for " + sourceTable);
        }
        
        snowflake.log("info", "SP_STANDARDIZE_DATA completed successfully for " + sourceTable);
        
    } catch (err) {
        result.status = 'ERROR';
        result.errors.push(err.message);
        snowflake.log("error", "SP_STANDARDIZE_DATA failed for " + sourceTable + ": " + err.message);
    }
    
    return result;
$$;

GRANT USAGE ON PROCEDURE IDR.PROCEDURES.SP_STANDARDIZE_DATA(VARCHAR, VARCHAR, BOOLEAN) TO ROLE PUBLIC;

/*
 * ============================================================================
 * SP_STANDARDIZE_ALL_TABLES - Wrapper to Process All Configured Tables
 * ============================================================================
 */

CREATE OR REPLACE PROCEDURE IDR.PROCEDURES.SP_STANDARDIZE_ALL_TABLES(
    DEPLOYMENT_DB VARCHAR,
    HISTORICAL_REFRESH BOOLEAN DEFAULT FALSE
)
RETURNS VARIANT
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
    var DB = DEPLOYMENT_DB;
    var historicalRefresh = HISTORICAL_REFRESH;
    var results = {
        tables_processed: [],
        total_rows: 0,
        historical_refresh: historicalRefresh,
        status: 'SUCCESS',
        errors: [],
        discovery_method: 'TAG_DRIVEN'
    };
    
    // TAG-DRIVEN DISCOVERY: Query IDR_CORE_TABLE_METADATA_CACHE for IDR-enabled tables
    var discoverySql = "SELECT TABLE_NAME FROM " + DB + ".SILVER.IDR_CORE_TABLE_METADATA_CACHE ORDER BY PRIORITY NULLS LAST, TABLE_NAME";
    var discoveryResult = snowflake.execute({sqlText: discoverySql});
    var tables = [];
    while (discoveryResult.next()) {
        tables.push(discoveryResult.getColumnValue('TABLE_NAME'));
    }
    
    snowflake.log("info", "SP_STANDARDIZE_ALL_TABLES started (TAG-DRIVEN), discovered tables: " + tables.join(', ') + ", historical_refresh: " + historicalRefresh);
    
    try {
        for (var i = 0; i < tables.length; i++) {
            var tableName = tables[i];
            snowflake.log("info", "Processing table " + (i+1) + "/" + tables.length + ": " + tableName);
            
            var callSql = "CALL IDR.PROCEDURES.SP_STANDARDIZE_DATA('" + DB + "', '" + tableName + "', " + historicalRefresh + ")";
            var callResult = snowflake.execute({sqlText: callSql});
            callResult.next();
            var rawResult = callResult.getColumnValue(1);
            var tableResult = (typeof rawResult === 'string') ? JSON.parse(rawResult) : rawResult;
            
            results.tables_processed.push({
                table: tableName,
                row_count: tableResult.row_count,
                status: tableResult.status
            });
            results.total_rows += tableResult.row_count;
            
            if (tableResult.status === 'ERROR') {
                results.errors = results.errors.concat(tableResult.errors);
                snowflake.log("warn", "Table " + tableName + " had errors: " + tableResult.errors.join('; '));
            } else {
                snowflake.log("info", "Table " + tableName + " completed: " + tableResult.row_count + " rows");
            }
        }
        
        if (results.errors.length > 0) {
            results.status = 'PARTIAL_ERROR';
        }
        
        snowflake.log("info", "SP_STANDARDIZE_ALL_TABLES completed: " + results.total_rows + " total rows, status: " + results.status);
        
    } catch (err) {
        results.status = 'ERROR';
        results.errors.push(err.message);
        snowflake.log("error", "SP_STANDARDIZE_ALL_TABLES failed: " + err.message);
    }
    
    return results;
$$;

GRANT USAGE ON PROCEDURE IDR.PROCEDURES.SP_STANDARDIZE_ALL_TABLES(VARCHAR, BOOLEAN) TO ROLE PUBLIC;

SELECT 'SP_STANDARDIZE_DATA and SP_STANDARDIZE_ALL_TABLES created successfully' AS STATUS;
