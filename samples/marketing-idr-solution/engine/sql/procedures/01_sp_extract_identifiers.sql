-- ============================================================================
-- SP_EXTRACT_IDENTIFIERS: TAG-DRIVEN Identifier Extraction (Normalized Model)
-- 
-- OPTIMIZED: Batches all column extractions into single MERGE + INSERT per table
-- instead of executing per-column, reducing ~42 serial DML ops to ~4.
--
-- Extracts identifiers from STD_* tables into:
-- 1. IDR_CORE_ENTITY_IDENTIFIERS (deduplicated - one row per unique identifier)
-- 2. IDR_CORE_IDENTIFIER_LINK (connects source records to identifiers)
--
-- Uses IDR_CORE_TABLE_METADATA_CACHE and IDR_CORE_COLUMN_METADATA_CACHE for discovery
-- Returns insert/delete counts in format: "Loyalty: X ins/Y del, Booking: Z ins/W del"
-- ============================================================================

USE ROLE ACCOUNTADMIN;
USE DATABASE IDR;
USE SCHEMA PROCEDURES;

CREATE OR REPLACE PROCEDURE IDR.PROCEDURES.SP_EXTRACT_IDENTIFIERS(DEPLOYMENT_DB VARCHAR)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
    var DB = DEPLOYMENT_DB;
    var results = {
        tables_processed: [],
        total_identifiers: 0,
        total_links: 0,
        total_inserts: 0,
        total_deletes: 0,
        discovery_method: 'TAG_DRIVEN_NORMALIZED_BATCHED'
    };

    var friendlyNames = {
        'CUSTOMER_BASE': 'Loyalty',
        'BOOKING': 'Booking'
    };

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

    snowflake.log("info", "SP_EXTRACT_IDENTIFIERS started (BATCHED), vectorEnabled=" + vectorEnabled);

    try {
        var tablesSql = `
            SELECT TABLE_NAME, PRIMARY_KEY_COLUMN, SOURCE_ROLE
            FROM ` + DB + `.SILVER.IDR_CORE_TABLE_METADATA_CACHE
            ORDER BY PRIORITY NULLS LAST, TABLE_NAME
        `;
        var tablesResult = snowflake.execute({sqlText: tablesSql});
        var tables = [];
        while (tablesResult.next()) {
            tables.push({
                name: tablesResult.getColumnValue('TABLE_NAME'),
                primaryKey: tablesResult.getColumnValue('PRIMARY_KEY_COLUMN'),
                sourceRole: tablesResult.getColumnValue('SOURCE_ROLE')
            });
        }

        snowflake.log("info", "Discovered " + tables.length + " IDR-enabled tables");

        if (tables.length === 0) {
            return "No IDR-enabled tables found";
        }

        for (var t = 0; t < tables.length; t++) {
            var table = tables[t];
            var tableName = table.name;
            var primaryKey = table.primaryKey;

            var stdTableFQ = DB + '.SILVER.STD_' + tableName;
            var batchTableName = '_BATCH_' + tableName;

            var batchSql = "CREATE OR REPLACE TEMPORARY TABLE " + batchTableName + 
                " AS SELECT * FROM " + stdTableFQ + " WHERE IDR_PROCESSED = FALSE";
            snowflake.execute({sqlText: batchSql});

            var countResult = snowflake.execute({sqlText: "SELECT COUNT(*) AS CNT FROM " + batchTableName});
            countResult.next();
            var rowCount = countResult.getColumnValue('CNT');

            var insertCount = 0;
            var deleteCount = 0;
            var countActionsSql = "SELECT METADATA_ACTION, COUNT(*) AS CNT FROM " + batchTableName + " GROUP BY METADATA_ACTION";
            var actionsResult = snowflake.execute({sqlText: countActionsSql});
            while (actionsResult.next()) {
                var action = actionsResult.getColumnValue('METADATA_ACTION');
                var cnt = actionsResult.getColumnValue('CNT');
                if (action === 'INSERT') insertCount = cnt;
                if (action === 'DELETE') deleteCount = cnt;
            }

            if (rowCount === 0) {
                snowflake.log("info", "No unprocessed records for " + tableName);
                snowflake.execute({sqlText: "DROP TABLE IF EXISTS " + batchTableName});
                results.tables_processed.push({table: tableName, identifiers: 0, links: 0, inserts: 0, deletes: 0});
                continue;
            }

            snowflake.log("info", tableName + ": " + rowCount + " records to process (" + insertCount + " ins, " + deleteCount + " del)");

            var colsSql = `
                SELECT COLUMN_NAME, IDENTIFIER_TYPE
                FROM ` + DB + `.SILVER.IDR_CORE_COLUMN_METADATA_CACHE
                WHERE TABLE_NAME = '` + tableName + `'
                  AND IDENTIFIER_TYPE IS NOT NULL
            `;
            var colsResult = snowflake.execute({sqlText: colsSql});
            var columns = [];
            while (colsResult.next()) {
                columns.push({
                    column: colsResult.getColumnValue('COLUMN_NAME'),
                    identifierType: colsResult.getColumnValue('IDENTIFIER_TYPE')
                });
            }

            snowflake.log("info", tableName + ": Found " + columns.length + " tagged columns");

            if (columns.length === 0) {
                snowflake.log("info", "No IDR_IDENTIFIER columns for " + tableName + " — skipping extraction (custom standardize handles this table)");
                snowflake.execute({sqlText: "DROP TABLE IF EXISTS " + batchTableName});
                results.tables_processed.push({table: tableName, identifiers: 0, links: 0, inserts: 0, deletes: 0, skipped: "no_tagged_columns"});
                continue;
            }

            var tableIdentifiers = 0;
            var tableLinks = 0;

            // =========================================================================
            // STEP 4: Probe _STD columns once, then build batched SQL
            // =========================================================================
            var colMeta = [];
            for (var i = 0; i < columns.length; i++) {
                var col = columns[i];
                var colName = col.column;
                var idType = col.identifierType;
                var normalizedCol = colName + '_STD';

                var hasStdCol = true;
                try {
                    snowflake.execute({sqlText: "SELECT " + normalizedCol + " FROM " + batchTableName + " LIMIT 1"});
                } catch (e) {
                    hasStdCol = false;
                }

                var pkCol = "CAST(b." + primaryKey + " AS VARCHAR)";
                var valsFrom, valsWhere, valCol, normCol;

                valCol = "TRIM(f_raw.value::VARCHAR)";
                valsWhere = "b." + colName + " IS NOT NULL AND TRIM(CAST(b." + colName + " AS VARCHAR)) != '' AND b.METADATA_ACTION = 'INSERT'";

                if (hasStdCol) {
                    valsFrom = batchTableName + " b, LATERAL FLATTEN(INPUT => SPLIT(TRIM(COALESCE(CAST(b." + colName + " AS VARCHAR), '')), ','), OUTER => FALSE) f_raw, LATERAL FLATTEN(INPUT => SPLIT(TRIM(COALESCE(CAST(b." + normalizedCol + " AS VARCHAR), '')), ','), OUTER => FALSE) f_std";
                    valsWhere += " AND f_raw.seq = f_std.seq AND TRIM(f_raw.value::VARCHAR) != '' AND TRIM(f_std.value::VARCHAR) != ''";
                    normCol = "TRIM(f_std.value::VARCHAR)";
                } else {
                    valsFrom = batchTableName + " b, LATERAL FLATTEN(INPUT => SPLIT(TRIM(COALESCE(CAST(b." + colName + " AS VARCHAR), '')), ','), OUTER => FALSE) f_raw";
                    valsWhere += " AND TRIM(f_raw.value::VARCHAR) != ''";
                    normCol = (idType === 'PHONE') ? "REGEXP_REPLACE(TRIM(f_raw.value::VARCHAR), '[^0-9]', '')" : "UPPER(TRIM(f_raw.value::VARCHAR))";
                }

                colMeta.push({
                    colName: colName,
                    idType: idType,
                    hasStdCol: hasStdCol,
                    pkCol: pkCol,
                    valsFrom: valsFrom,
                    valsWhere: valsWhere,
                    valCol: valCol,
                    normCol: normCol
                });
            }

            // =========================================================================
            // STEP 4a: Single batched MERGE into IDR_CORE_ENTITY_IDENTIFIERS
            // =========================================================================
            var mergeUnionParts = [];
            for (var i = 0; i < colMeta.length; i++) {
                var m = colMeta[i];
                mergeUnionParts.push(
                    "SELECT '" + m.idType + "' AS identifier_type, " +
                    m.valCol + " AS identifier_value, " +
                    m.normCol + " AS identifier_value_normalized, " +
                    m.pkCol + " AS pk_val " +
                    "FROM " + m.valsFrom + " " +
                    "WHERE " + m.valsWhere
                );
            }

            var combinedMergeSql = `
                MERGE INTO ` + DB + `.SILVER.IDR_CORE_ENTITY_IDENTIFIERS tgt
                USING (
                    SELECT identifier_type, identifier_value, identifier_value_normalized
                    FROM (
                        SELECT identifier_type, identifier_value, identifier_value_normalized,
                               ROW_NUMBER() OVER (PARTITION BY identifier_type, identifier_value_normalized ORDER BY pk_val) AS rn
                        FROM (
                            ` + mergeUnionParts.join("\n                            UNION ALL\n                            ") + `
                        )
                    )
                    WHERE rn = 1
                ) src
                ON tgt.identifier_type = src.identifier_type
                    AND tgt.identifier_value_normalized = src.identifier_value_normalized
                WHEN NOT MATCHED THEN INSERT (
                    identifier_type, identifier_value, identifier_value_normalized
                ) VALUES (
                    src.identifier_type, src.identifier_value, src.identifier_value_normalized
                )
                WHEN MATCHED THEN UPDATE SET
                    updated_at = CURRENT_TIMESTAMP()
            `;

            snowflake.log("debug", "Batched MERGE for " + tableName + " with " + mergeUnionParts.length + " column unions");
            var upsertResult = snowflake.execute({sqlText: combinedMergeSql});
            tableIdentifiers = upsertResult.getNumRowsAffected();

            // =========================================================================
            // STEP 4b: Single batched INSERT into IDR_CORE_IDENTIFIER_LINK
            // =========================================================================
            var embeddingSelect = vectorEnabled ? ", b.SOURCE_EMBEDDING" : "";
            var linkUnionParts = [];
            for (var i = 0; i < colMeta.length; i++) {
                var m = colMeta[i];
                linkUnionParts.push(
                    "SELECT DISTINCT " +
                    m.pkCol + " AS source_record_id, " +
                    "'" + tableName + "' AS source_type, " +
                    "e.identifier_id" + embeddingSelect + " " +
                    "FROM " + m.valsFrom + " " +
                    "JOIN " + DB + ".SILVER.IDR_CORE_ENTITY_IDENTIFIERS e " +
                    "ON e.identifier_type = '" + m.idType + "' " +
                    "AND e.identifier_value_normalized = " + m.normCol + " " +
                    "WHERE " + m.valsWhere
                );
            }

            var embeddingCol = vectorEnabled ? ", SOURCE_EMBEDDING" : "";
            var embeddingInsertSelect = vectorEnabled ? ", src.SOURCE_EMBEDDING" : "";
            var combinedInsertSql = `
                INSERT INTO ` + DB + `.SILVER.IDR_CORE_IDENTIFIER_LINK (source_record_id, source_type, identifier_id` + embeddingCol + `)
                SELECT DISTINCT src.source_record_id, src.source_type, src.identifier_id` + embeddingInsertSelect + `
                FROM (
                    ` + linkUnionParts.join("\n                    UNION ALL\n                    ") + `
                ) src
                WHERE NOT EXISTS (
                    SELECT 1 FROM ` + DB + `.SILVER.IDR_CORE_IDENTIFIER_LINK l
                    WHERE l.source_record_id = src.source_record_id
                      AND l.source_type = src.source_type
                      AND l.identifier_id = src.identifier_id
                )
            `;

            snowflake.log("debug", "Batched INSERT LINK for " + tableName + " with " + linkUnionParts.length + " column unions");
            var linkResult = snowflake.execute({sqlText: combinedInsertSql});
            tableLinks = linkResult.getNumRowsAffected();

            snowflake.log("info", tableName + " batched: " + tableIdentifiers + " identifiers, " + tableLinks + " links");

            // STEP 5: Handle DELETES
            var deactivateLinksSql = `
                UPDATE ` + DB + `.SILVER.IDR_CORE_IDENTIFIER_LINK
                SET is_active = FALSE
                WHERE source_type = '` + tableName + `'
                AND source_record_id IN (
                    SELECT CAST(` + primaryKey + ` AS VARCHAR) FROM ` + batchTableName + `
                    WHERE METADATA_ACTION = 'DELETE' AND METADATA_ISUPDATE = FALSE
                )
            `;
            snowflake.execute({sqlText: deactivateLinksSql});

            // STEP 6: Mark records as IDR processed
            var markProcessedSql = `
                UPDATE ` + stdTableFQ + `
                SET IDR_PROCESSED = TRUE, IDR_PROCESSED_AT = CURRENT_TIMESTAMP()
                WHERE ` + primaryKey + ` IN (SELECT ` + primaryKey + ` FROM ` + batchTableName + `)
            `;
            snowflake.execute({sqlText: markProcessedSql});

            snowflake.execute({sqlText: "DROP TABLE IF EXISTS " + batchTableName});

            results.tables_processed.push({
                table: tableName,
                identifiers: tableIdentifiers,
                links: tableLinks,
                inserts: insertCount,
                deletes: deleteCount
            });
            results.total_identifiers += tableIdentifiers;
            results.total_links += tableLinks;
            results.total_inserts += insertCount;
            results.total_deletes += deleteCount;

            snowflake.log("info", tableName + " completed: " + tableIdentifiers + " identifiers, " + tableLinks + " links, " + insertCount + " ins, " + deleteCount + " del");
        }

    } catch (err) {
        snowflake.log("error", "SP_EXTRACT_IDENTIFIERS failed: " + err.message);
        return "Error: " + err.message;
    }

    var msg = "Extracted from STD tables - ";
    for (var i = 0; i < results.tables_processed.length; i++) {
        var tp = results.tables_processed[i];
        if (i > 0) msg += ", ";
        var displayName = friendlyNames[tp.table] || tp.table;
        msg += displayName + ": " + tp.inserts + " ins/" + tp.deletes + " del";
    }

    snowflake.log("info", "SP_EXTRACT_IDENTIFIERS completed: " + msg);
    return msg;
$$;
