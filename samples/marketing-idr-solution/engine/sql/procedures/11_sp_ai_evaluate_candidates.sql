CREATE OR REPLACE PROCEDURE IDR.PROCEDURES.SP_AI_EVALUATE_CANDIDATES(DEPLOYMENT_DB VARCHAR, MAX_PAIRS FLOAT DEFAULT NULL)
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    v_max_pairs INT;
    v_batch_size INT DEFAULT 100;
    v_auto_match_threshold FLOAT;
    v_review_min_threshold FLOAT;
    v_session_id VARCHAR;
    v_transaction_id VARCHAR;
    v_batch_number INT DEFAULT 0;
    v_total_evaluated INT DEFAULT 0;
    v_total_matches INT DEFAULT 0;
    v_total_reviews INT DEFAULT 0;
    v_total_no_matches INT DEFAULT 0;
    v_total_errors INT DEFAULT 0;
    v_pending_count INT;
    v_batch_start TIMESTAMP_NTZ;
    v_batch_processed INT;
    v_batch_matches INT;
    v_batch_reviews INT;
    v_batch_no_matches INT;
    v_result VARIANT;
    DB VARCHAR DEFAULT DEPLOYMENT_DB;
    v_config_sql VARCHAR;
    res RESULTSET;
BEGIN
    v_session_id := UUID_STRING();
    EXECUTE IMMEDIATE 'USE DATABASE ' || DB;
    EXECUTE IMMEDIATE 'USE SCHEMA SILVER';
    
    v_config_sql := 'SELECT TRY_TO_NUMBER(CONFIG_VALUE) AS V FROM ' || DB || '.CONFIG.IDR_ML_AI_EVALUATION_CONFIG WHERE CONFIG_KEY = ''MAX_PAIRS_PER_RUN''';
    res := (EXECUTE IMMEDIATE :v_config_sql);
    LET c1 CURSOR FOR res;
    OPEN c1;
    FETCH c1 INTO v_max_pairs;
    CLOSE c1;
    
    v_config_sql := 'SELECT TRY_TO_DOUBLE(CONFIG_VALUE) AS V FROM ' || DB || '.CONFIG.IDR_ML_AI_EVALUATION_CONFIG WHERE CONFIG_KEY = ''AUTO_MATCH_THRESHOLD''';
    res := (EXECUTE IMMEDIATE :v_config_sql);
    LET c2 CURSOR FOR res;
    OPEN c2;
    FETCH c2 INTO v_auto_match_threshold;
    CLOSE c2;
    
    v_config_sql := 'SELECT TRY_TO_DOUBLE(CONFIG_VALUE) AS V FROM ' || DB || '.CONFIG.IDR_ML_AI_EVALUATION_CONFIG WHERE CONFIG_KEY = ''REVIEW_MIN_THRESHOLD''';
    res := (EXECUTE IMMEDIATE :v_config_sql);
    LET c3 CURSOR FOR res;
    OPEN c3;
    FETCH c3 INTO v_review_min_threshold;
    CLOSE c3;
    
    IF (MAX_PAIRS IS NOT NULL) THEN v_max_pairs := MAX_PAIRS::INT; END IF;
    v_max_pairs := COALESCE(v_max_pairs, 500);
    v_auto_match_threshold := COALESCE(v_auto_match_threshold, 0.95);
    v_review_min_threshold := COALESCE(v_review_min_threshold, 0.60);
    
    SELECT COUNT(*) INTO v_pending_count FROM IDR_ML_AI_CANDIDATE_PAIRS WHERE STATUS = 'PENDING';
    
    WHILE (v_total_evaluated < v_max_pairs AND v_pending_count > 0) DO
        v_batch_number := v_batch_number + 1;
        v_transaction_id := UUID_STRING();
        v_batch_start := CURRENT_TIMESTAMP();
        v_batch_processed := 0;
        v_batch_matches := 0;
        v_batch_reviews := 0;
        v_batch_no_matches := 0;
        
        BEGIN
            CREATE OR REPLACE TEMPORARY TABLE _ai_eval_batch AS
            SELECT cp.CANDIDATE_ID, 
                   TRY_PARSE_JSON(SNOWFLAKE.CORTEX.COMPLETE('claude-4-sonnet', 
                       'You are an identity resolution expert. Determine if these two profiles represent the SAME PERSON.

Profile 1:
- Name: ' || COALESCE(c1.BEST_FIRST_NAME, 'Unknown') || ' ' || COALESCE(c1.BEST_LAST_NAME, 'Unknown') || '
- Email: ' || COALESCE(c1.BEST_EMAIL, 'Not provided') || '
- Phone: ' || COALESCE(c1.BEST_PHONE, 'Not provided') || '
- DOB: ' || COALESCE(TO_VARCHAR(c1.BEST_DOB), 'Not provided') || '
- Location: ' || COALESCE(c1.BEST_CITY, '') || ' ' || COALESCE(c1.BEST_ZIP, '') || '
- Cluster size: ' || ARRAY_SIZE(c1.SOURCE_RECORD_IDS)::VARCHAR || ' records

Profile 2:
- Name: ' || COALESCE(c2.BEST_FIRST_NAME, 'Unknown') || ' ' || COALESCE(c2.BEST_LAST_NAME, 'Unknown') || '
- Email: ' || COALESCE(c2.BEST_EMAIL, 'Not provided') || '
- Phone: ' || COALESCE(c2.BEST_PHONE, 'Not provided') || '
- DOB: ' || COALESCE(TO_VARCHAR(c2.BEST_DOB), 'Not provided') || '
- Location: ' || COALESCE(c2.BEST_CITY, '') || ' ' || COALESCE(c2.BEST_ZIP, '') || '
- Cluster size: ' || ARRAY_SIZE(c2.SOURCE_RECORD_IDS)::VARCHAR || ' records

Blocking key that matched: ' || cp.BLOCKING_NAME || ' = ' || cp.BLOCKING_KEY_VALUE || '

SCORING GUIDE (you MUST follow this):
- 0.95-1.0: DEFINITE MATCH - Exact email OR exact phone with similar name
- 0.70-0.94: LIKELY MATCH - Strong evidence like same phone + plausible name variation
- 0.40-0.69: UNCERTAIN - Some overlap but conflicting data
- 0.10-0.39: UNLIKELY - Mostly different with weak connection
- 0.00-0.09: NO MATCH - Clearly different people

CRITICAL: Your confidence score MUST align with your reasoning. If profiles appear to be different people, score MUST be below 0.4.

Respond with ONLY valid JSON:
{"match": true, "confidence": 0.85, "reasoning": "2-3 sentence explanation"}'
                   )) AS ai_response
            FROM IDR_ML_AI_CANDIDATE_PAIRS cp
            JOIN IDR_CORE_CLUSTER c1 ON cp.CLUSTER_ID_1 = c1.CLUSTER_ID
            JOIN IDR_CORE_CLUSTER c2 ON cp.CLUSTER_ID_2 = c2.CLUSTER_ID
            WHERE cp.STATUS = 'PENDING' LIMIT :v_batch_size;
            
            UPDATE IDR_ML_AI_CANDIDATE_PAIRS cp 
            SET AI_SCORE = t.ai_response:confidence::FLOAT, 
                AI_REASONING = t.ai_response:reasoning::VARCHAR, 
                AI_EVALUATED_AT = CURRENT_TIMESTAMP(), 
                STATUS = CASE 
                    WHEN t.ai_response:confidence::FLOAT >= :v_auto_match_threshold THEN 'MATCH' 
                    WHEN t.ai_response:confidence::FLOAT >= :v_review_min_threshold THEN 'REVIEW' 
                    ELSE 'NO_MATCH' 
                END 
            FROM _ai_eval_batch t WHERE cp.CANDIDATE_ID = t.CANDIDATE_ID;
            
            SELECT COUNT(*) INTO v_batch_processed FROM _ai_eval_batch;
            SELECT COUNT(*) INTO v_batch_matches FROM _ai_eval_batch WHERE ai_response:confidence::FLOAT >= :v_auto_match_threshold;
            SELECT COUNT(*) INTO v_batch_no_matches FROM _ai_eval_batch WHERE ai_response:confidence::FLOAT < :v_review_min_threshold;
            v_batch_reviews := v_batch_processed - v_batch_matches - v_batch_no_matches;
            
            INSERT INTO IDR_ML_AI_EVALUATION_LOG 
                (SESSION_ID, TRANSACTION_ID, BATCH_NUMBER, BATCH_SIZE, RECORDS_PROCESSED, MATCHES, REVIEWS, NO_MATCHES, ERRORS, START_TIME, END_TIME, DURATION_MS, STATUS) 
            SELECT :v_session_id, :v_transaction_id, :v_batch_number, :v_batch_size, :v_batch_processed, :v_batch_matches, :v_batch_reviews, :v_batch_no_matches, 0, :v_batch_start, CURRENT_TIMESTAMP(), TIMESTAMPDIFF(MILLISECOND, :v_batch_start, CURRENT_TIMESTAMP()), 'SUCCESS';
            
            v_total_evaluated := v_total_evaluated + v_batch_processed;
            v_total_matches := v_total_matches + v_batch_matches;
            v_total_reviews := v_total_reviews + v_batch_reviews;
            v_total_no_matches := v_total_no_matches + v_batch_no_matches;
        EXCEPTION
            WHEN OTHER THEN
                LET error_msg VARCHAR := 'Error in batch ' || v_batch_number::VARCHAR;
                INSERT INTO IDR_ML_AI_EVALUATION_LOG 
                    (SESSION_ID, TRANSACTION_ID, BATCH_NUMBER, BATCH_SIZE, RECORDS_PROCESSED, ERRORS, START_TIME, END_TIME, DURATION_MS, STATUS, ERROR_MESSAGE) 
                SELECT :v_session_id, :v_transaction_id, :v_batch_number, :v_batch_size, 0, 1, :v_batch_start, CURRENT_TIMESTAMP(), TIMESTAMPDIFF(MILLISECOND, :v_batch_start, CURRENT_TIMESTAMP()), 'ERROR', :error_msg;
                v_total_errors := v_total_errors + 1;
        END;
        
        SELECT COUNT(*) INTO v_pending_count FROM IDR_ML_AI_CANDIDATE_PAIRS WHERE STATUS = 'PENDING';
    END WHILE;
    
    v_result := OBJECT_CONSTRUCT('session_id', v_session_id, 'batches_processed', v_batch_number, 'total_evaluated', v_total_evaluated, 'matches', v_total_matches, 'reviews', v_total_reviews, 'no_matches', v_total_no_matches, 'errors', v_total_errors);
    RETURN v_result;
END;
$$;
