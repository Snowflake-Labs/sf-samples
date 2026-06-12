-- ============================================================================
-- SP_PREPARE_AI_MERGE: Prepare AI-approved match for cluster merge
-- Inserts edge into IDR_CORE_MATCH_RESULTS with AI metadata for lineage tracking
-- ============================================================================

USE ROLE ACCOUNTADMIN;
USE DATABASE IDR;
USE SCHEMA PROCEDURES;

CREATE OR REPLACE PROCEDURE IDR.PROCEDURES.SP_PREPARE_AI_MERGE(DEPLOYMENT_DB VARCHAR, P_CANDIDATE_ID VARCHAR, P_REVIEWER VARCHAR DEFAULT 'system')
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    v_cluster_id_1 VARCHAR;
    v_cluster_id_2 VARCHAR;
    v_source_1 VARCHAR;
    v_source_2 VARCHAR;
    v_status VARCHAR;
    v_ai_score FLOAT;
    v_ai_reasoning VARCHAR;
    v_ai_identifier VARCHAR;
    DB VARCHAR DEFAULT DEPLOYMENT_DB;
BEGIN
    EXECUTE IMMEDIATE 'USE DATABASE ' || DB;
    EXECUTE IMMEDIATE 'USE SCHEMA SILVER';
    -- Get cluster IDs, AI score/reasoning, and verify status is MATCH
    SELECT cluster_id_1, cluster_id_2, status, ai_score, ai_reasoning
    INTO :v_cluster_id_1, :v_cluster_id_2, :v_status, :v_ai_score, :v_ai_reasoning
    FROM IDR_ML_AI_CANDIDATE_PAIRS 
    WHERE candidate_id = :p_candidate_id;
    
    IF (:v_status != 'MATCH') THEN
        RETURN 'Error: Candidate status must be MATCH to execute merge';
    END IF;
    
    -- Get first source_record_id from each cluster
    SELECT SOURCE_RECORD_IDS[0]::VARCHAR INTO :v_source_1
    FROM IDR_CORE_CLUSTER 
    WHERE CLUSTER_ID = :v_cluster_id_1 AND STATUS = 'ACTIVE';
    
    SELECT SOURCE_RECORD_IDS[0]::VARCHAR INTO :v_source_2
    FROM IDR_CORE_CLUSTER 
    WHERE CLUSTER_ID = :v_cluster_id_2 AND STATUS = 'ACTIVE';
    
    -- Validate both clusters exist and are active
    IF (:v_source_1 IS NULL OR :v_source_2 IS NULL) THEN
        RETURN 'Error: One or both clusters not found or not active';
    END IF;
    
    -- Build AI identifier JSON for lineage tracking
    SELECT OBJECT_CONSTRUCT(
        'type', 'AI_REVIEW',
        'candidate_id', :p_candidate_id,
        'ai_score', :v_ai_score,
        'ai_reasoning', :v_ai_reasoning,
        'reviewer', :p_reviewer
    )::VARCHAR INTO :v_ai_identifier;
    
    -- Insert single edge into IDR_CORE_MATCH_RESULTS with AI metadata in identifier fields
    -- These fields flow through to IDR_CORE_MATCH_LOG.match_details.rules[].id1/id2
    INSERT INTO IDR_CORE_MATCH_RESULTS 
        (new_source_record_id, matched_source_record_id, rule_name, match_score, identifier_1, identifier_2, is_active)
    VALUES 
        (:v_source_1, :v_source_2, 'AI_REVIEW_APPROVED', COALESCE(:v_ai_score, 1.0), :v_ai_identifier, :v_ai_identifier, TRUE);
    
    RETURN 'Edge inserted: ' || :v_source_1 || ' <-> ' || :v_source_2;
END;
$$;
