-- ============================================================================
-- Martech: APP.SP_LLM_ADJUDICATE_PAIRS
-- For ML pairs with score in [LLM_REVIEW_SCORE_LOW, LLM_REVIEW_SCORE_HIGH),
-- call Cortex AI_COMPLETE with retail/DTC contextualized prompt; persist
-- verdict + rationale; auto-emit MARTECH_R17 edges on MATCH.
--
-- v2: Improved prompt (explicit JSON-only), REGEXP fallback for markdown fences,
--     LLM_RAW_OUTPUT + LLM_PARSE_ERROR columns for observability.
--
-- Reads thresholds, model, prompt key from CONFIG.IDR_ML_AI_EVALUATION_CONFIG.
-- ============================================================================

CREATE OR REPLACE PROCEDURE IDENTIFIER('&{deployment_db}.APP.SP_LLM_ADJUDICATE_PAIRS')()
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
    var dbResult = snowflake.execute({sqlText: "SELECT CURRENT_DATABASE() AS DB"});
    dbResult.next();
    var DB = dbResult.getColumnValue('DB');

    function cfg(key) {
        var r = snowflake.execute({sqlText:
            "SELECT CONFIG_VALUE FROM " + DB + ".CONFIG.IDR_ML_AI_EVALUATION_CONFIG WHERE CONFIG_KEY = '" + key + "'"});
        if (r.next()) return r.getColumnValue(1);
        return null;
    }

    var lowScore  = parseFloat(cfg('LLM_REVIEW_SCORE_LOW')) || 0.55;
    var highScore = parseFloat(cfg('LLM_REVIEW_SCORE_HIGH')) || 0.85;
    var batchSize = parseInt(cfg('LLM_REVIEW_BATCH_SIZE')) || 50;
    var model     = cfg('LLM_REVIEW_MODEL') || 'claude-4-sonnet';
    var promptKey = cfg('LLM_REVIEW_PROMPT_KEY') || 'MARTECH_RETAIL_DTC_V1';

    var enabledRaw = cfg('LLM_ADJUDICATION_ENABLED');
    var enabled = (enabledRaw === null || String(enabledRaw).toUpperCase() === 'TRUE');
    if (!enabled) {
        return JSON.stringify({ status: 'SKIPPED', reason: 'LLM_ADJUDICATION_ENABLED=FALSE' });
    }

    // Prompt with explicit JSON-only instruction
    var promptHeader =
        "You are an identity-resolution adjudicator for a retail/DTC customer engagement platform. " +
        "Two records may belong to the same customer across POS, Loyalty, Web Clickstream, and Shopify sources. " +
        "Compare the feature comparison below and decide if these two records belong to the same person.\\n\\n" +
        "Respond with ONLY valid JSON (no markdown, no code fences, no explanation outside the JSON). " +
        "Use this exact structure:\\n" +
        "{\"verdict\": \"MATCH\" or \"NOT_MATCH\" or \"UNCLEAR\", \"confidence\": 0.0 to 1.0, \"rationale\": \"one sentence explanation\"}\\n" +
        "confidence is your probability (0.0-1.0) that the two records are the SAME person; use the full range, do not return a fixed value.\\n\\n" +
        "How to read the features: each *_eq field is true (the two values match), " +
        "false (they differ), or null (the field is missing on one or both records). " +
        "The jw_* fields are Jaro-Winkler name/address similarities in [0,1]. " +
        "Base your verdict and rationale ONLY on the values shown below. " +
        "Cite a field as matching ONLY when its value is exactly true. " +
        "A null value means there is NO information for that field — never treat null as agreement or as a match.\\n\\n" +
        "Decision guidance: a single shared identifier (only phone_last7_eq true, OR only email_handle_eq true) is NOT sufficient by itself — " +
        "two different people can coincidentally share a 7-digit phone tail or an email handle. " +
        "When jw_last_name is low (below 0.6) and there is no exact email (email_eq), the surnames conflict: " +
        "return NOT_MATCH unless several other strong fields independently agree. " +
        "These are pre-filtered borderline pairs, so exact identifiers (full email, phone, loyalty ID) are EXPECTED to be absent — " +
        "do NOT treat a missing or differing exact identifier as evidence against a match; judge on the probabilistic signals shown. " +
        "Use UNCLEAR when the evidence is genuinely mixed. Do NOT default to MATCH.\\n" +
        "Example — {\"email_handle_eq\": true, \"phone_last7_eq\": true, \"jw_first_name\": 0.95, \"jw_last_name\": 0.30, \"postal_eq\": false} " +
        "-> {\"verdict\": \"NOT_MATCH\", \"confidence\": 0.18, \"rationale\": \"Shared handle and phone tail but strongly conflicting surnames and address indicate two different people.\"}\\n\\n" +
        "Feature comparison:\\n";

    // Use REGEXP_REPLACE to strip markdown fences before parsing
    // Pattern: remove leading ```json or ``` and trailing ```
    var stripFences =
        "REGEXP_REPLACE(REGEXP_REPLACE(TRIM(LLM_OUT), '^```(json)?\\\\s*', ''), '\\\\s*```$', '')";

    var adjudicateSql =
        "INSERT INTO " + DB + ".SILVER.LLM_REVIEW_QUEUE " +
        "  (PAIR_ID, SOURCE_RECORD_ID_A, SOURCE_TYPE_A, SOURCE_RECORD_ID_B, SOURCE_TYPE_B, " +
        "   ML_SCORE, LLM_VERDICT, LLM_RATIONALE, LLM_CONFIDENCE, LLM_RAW_OUTPUT, LLM_PARSE_ERROR, " +
        "   LLM_MODEL, LLM_PROMPT_KEY, STATUS, CREATED_AT) " +
        "WITH borderline AS (" +
        "  SELECT * FROM " + DB + ".SILVER.ML_PAIR_FEATURES " +
        "  WHERE ML_TIER = 'GREY_LLM' " +
        "    AND PAIR_ID NOT IN (SELECT PAIR_ID FROM " + DB + ".SILVER.LLM_REVIEW_QUEUE WHERE PAIR_ID IS NOT NULL) " +
        "  ORDER BY RANDOM() LIMIT " + batchSize +
        "), " +
        "raw_results AS (" +
        "  SELECT b.PAIR_ID, b.SOURCE_RECORD_ID_A, b.SOURCE_TYPE_A, b.SOURCE_RECORD_ID_B, b.SOURCE_TYPE_B, b.SCORE, " +
        "    SNOWFLAKE.CORTEX.COMPLETE(" +
        "      '" + model + "', " +
        "      '" + promptHeader + "' || " +
        "      'Record A: ' || OBJECT_CONSTRUCT_KEEP_NULL(" +
        "        'source_type', b.SOURCE_TYPE_A, 'record_id', b.SOURCE_RECORD_ID_A, " +
        "        'email_eq', b.EMAIL_EQ, 'phone_eq', b.PHONE_EQ, " +
        "        'email_handle_eq', b.EMAIL_HANDLE_EQ, 'phone_last7_eq', b.PHONE_LAST7_EQ, " +
        "        'jw_first_name', ROUND(b.JW_FIRST_NAME, 3), " +
        "        'jw_last_name', ROUND(b.JW_LAST_NAME, 3), " +
        "        'jw_street', ROUND(b.JW_STREET, 3), " +
        "        'postal_eq', b.POSTAL_EQ, 'state_eq', b.STATE_EQ, " +
        "        'nickname_first_eq', b.NICKNAME_FIRST_EQ" +
        "      )::VARCHAR " +
        "      || '\\nRecord B: ' || OBJECT_CONSTRUCT(" +
        "        'source_type', b.SOURCE_TYPE_B, 'record_id', b.SOURCE_RECORD_ID_B" +
        "      )::VARCHAR " +
        "      || '\\nML Score: ' || ROUND(b.SCORE, 4)::VARCHAR " +
        "      || '\\nBlocking keys hit: ' || COALESCE(ARRAY_TO_STRING(b.BLOCKING_KEYS_HIT, ', '), 'none')" +
        "    ) AS LLM_OUT " +
        "  FROM borderline b" +
        "), " +
        "parsed AS (" +
        "  SELECT *, " +
        "    TRY_PARSE_JSON(" + stripFences + ") AS PARSED_JSON " +
        "  FROM raw_results" +
        ") " +
        "SELECT PAIR_ID, SOURCE_RECORD_ID_A, SOURCE_TYPE_A, SOURCE_RECORD_ID_B, SOURCE_TYPE_B, SCORE, " +
        "       UPPER(PARSED_JSON:verdict::VARCHAR) AS VERDICT, " +
        "       PARSED_JSON:rationale::VARCHAR AS RATIONALE, " +
        "       PARSED_JSON:confidence::FLOAT AS CONFIDENCE, " +
        "       LEFT(LLM_OUT, 2000), " +
        "       CASE WHEN PARSED_JSON IS NULL THEN 'PARSE_FAILED' ELSE NULL END, " +
        "       '" + model + "', '" + promptKey + "', " +
        "       CASE WHEN UPPER(PARSED_JSON:verdict::VARCHAR) = 'MATCH' THEN 'ACCEPTED' " +
        "            WHEN UPPER(PARSED_JSON:verdict::VARCHAR) = 'NOT_MATCH' THEN 'REJECTED' " +
        "            ELSE 'PENDING_HUMAN' END, " +
        "       CURRENT_TIMESTAMP() " +
        "FROM parsed";

    var ra = snowflake.execute({sqlText: adjudicateSql});
    ra.next();
    var queued = ra.getColumnValue(1);

    // Auto-emit R17 edges for verdict = MATCH
    var pipelineRunResult = snowflake.execute({sqlText: "SELECT UUID_STRING() AS RID"});
    pipelineRunResult.next();
    var pipelineRunId = pipelineRunResult.getColumnValue('RID');

    var emitSql =
        "INSERT INTO " + DB + ".SILVER.IDR_CORE_MATCH_RESULTS (" +
        "  SOURCE_RECORD_ID_A, SOURCE_RECORD_ID_B, NEW_SOURCE_RECORD_ID, MATCHED_SOURCE_RECORD_ID, " +
        "  RULE_ID, RULE_NAME, MATCH_SCORE, MATCHED_ON, IS_ACTIVE, IS_CURRENT, RUN_ID, PIPELINE_RUN_ID" +
        ") " +
        "SELECT SOURCE_RECORD_ID_A, SOURCE_RECORD_ID_B, SOURCE_RECORD_ID_A, SOURCE_RECORD_ID_B, " +
        "       'MARTECH_R17', 'LLM Adjudicated MATCH', COALESCE(LLM_CONFIDENCE, 0.90), 'LLM', " +
        "       TRUE, TRUE, '" + pipelineRunId + "', '" + pipelineRunId + "' " +
        "FROM " + DB + ".SILVER.LLM_REVIEW_QUEUE " +
        "WHERE LLM_VERDICT = 'MATCH' AND EMITTED_MATCH_ID IS NULL";
    var re = snowflake.execute({sqlText: emitSql});
    re.next();
    var emitted = re.getColumnValue(1);

    snowflake.execute({sqlText:
        "UPDATE " + DB + ".SILVER.LLM_REVIEW_QUEUE SET EMITTED_MATCH_ID = REVIEW_ID, " +
        "       UPDATED_AT = CURRENT_TIMESTAMP() " +
        "WHERE LLM_VERDICT = 'MATCH' AND EMITTED_MATCH_ID IS NULL"});

    // Aggregate stats including parse failures
    var statsR = snowflake.execute({sqlText:
        "SELECT COUNT_IF(LLM_VERDICT='MATCH'), COUNT_IF(LLM_VERDICT='NOT_MATCH'), " +
        "       COUNT_IF(LLM_VERDICT='UNCLEAR' OR LLM_VERDICT IS NULL), " +
        "       COUNT_IF(LLM_PARSE_ERROR = 'PARSE_FAILED') " +
        "FROM " + DB + ".SILVER.LLM_REVIEW_QUEUE WHERE PAIR_ID IS NOT NULL"});
    statsR.next();

    var result = {
        adjudicated: queued,
        match: statsR.getColumnValue(1),
        not_match: statsR.getColumnValue(2),
        unclear: statsR.getColumnValue(3),
        parse_failures: statsR.getColumnValue(4),
        edges_emitted: emitted
    };

    // Flag if all adjudicated pairs failed parsing
    if (queued > 0 && result.match === 0 && result.not_match === 0) {
        result.warning = 'ALL_PARSES_FAILED: Cortex returned output that could not be parsed as JSON. Check LLM_RAW_OUTPUT column.';
    }

    return JSON.stringify(result);
$$;
