-- ============================================================================
-- SP_BUILD_PROFILE_INDEX
-- Computes derived blocking tokens for IDR clusters to support AI second pass.
-- This SP should be called after clusters are created/updated in Pass 1.
--
-- Updated for NORMALIZED model (IDR_CORE_IDENTIFIER_LINK + IDR_CORE_ENTITY_IDENTIFIERS)
-- ============================================================================

USE DATABASE IDR;
USE SCHEMA PROCEDURES;
USE WAREHOUSE IDR_DEMO_WH;

-- Step 2: Create the stored procedure (NORMALIZED MODEL)
CREATE OR REPLACE PROCEDURE IDR.PROCEDURES.SP_BUILD_PROFILE_INDEX(DEPLOYMENT_DB VARCHAR, FULL_REBUILD BOOLEAN DEFAULT FALSE)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    rows_updated INT DEFAULT 0;
    start_time TIMESTAMP_NTZ;
    DB VARCHAR DEFAULT DEPLOYMENT_DB;
BEGIN
    start_time := CURRENT_TIMESTAMP();
    
    EXECUTE IMMEDIATE '
    UPDATE ' || DB || '.SILVER.IDR_CORE_CLUSTER c
    SET
        BEST_FIRST_NAME = profile.best_first_name,
        BEST_LAST_NAME = profile.best_last_name,
        BEST_EMAIL = profile.best_email,
        BEST_PHONE = profile.best_phone,
        BEST_DOB = profile.best_dob,
        BEST_CITY = profile.best_city,
        BEST_ZIP = profile.best_zip,
        LAST_NAME_SOUNDEX = SOUNDEX(profile.best_last_name),
        FIRST_INITIAL = LEFT(profile.best_first_name, 1),
        EMAIL_DOMAIN = SPLIT_PART(profile.best_email, ''@'', 2),
        EMAIL_LOCAL_PART = SPLIT_PART(profile.best_email, ''@'', 1),
        PHONE_LAST7 = RIGHT(REGEXP_REPLACE(profile.best_phone, ''[^0-9]'', ''''), 7),
        PHONE_LAST4 = RIGHT(REGEXP_REPLACE(profile.best_phone, ''[^0-9]'', ''''), 4),
        ZIP3 = LEFT(profile.best_zip, 3),
        DOB_MMDD = CASE WHEN profile.best_dob IS NOT NULL THEN TO_CHAR(profile.best_dob, ''MM-DD'') ELSE NULL END,
        DOB_YYMM = CASE WHEN profile.best_dob IS NOT NULL THEN TO_CHAR(profile.best_dob, ''YY-MM'') ELSE NULL END,
        BLOCK_ZIP3_SOUNDEX_FI = CONCAT_WS(''|'', LEFT(profile.best_zip, 3), SOUNDEX(profile.best_last_name), LEFT(profile.best_first_name, 1)),
        BLOCK_DOMAIN_SOUNDEX = CONCAT_WS(''|'', SPLIT_PART(profile.best_email, ''@'', 2), SOUNDEX(profile.best_last_name)),
        BLOCK_PHONE7_SOUNDEX = CONCAT_WS(''|'', RIGHT(REGEXP_REPLACE(profile.best_phone, ''[^0-9]'', ''''), 7), SOUNDEX(profile.best_last_name)),
        BLOCK_EMAIL_LOCAL_DOB_YYMM = CONCAT_WS(''|'', SPLIT_PART(profile.best_email, ''@'', 1), CASE WHEN profile.best_dob IS NOT NULL THEN TO_CHAR(profile.best_dob, ''YY-MM'') ELSE NULL END),
        BLOCK_EMAIL_LOCAL_DOB_YYMM_SOUNDEX = CONCAT_WS(''|'', SPLIT_PART(profile.best_email, ''@'', 1), CASE WHEN profile.best_dob IS NOT NULL THEN TO_CHAR(profile.best_dob, ''YY-MM'') ELSE NULL END, SOUNDEX(profile.best_last_name)),
        BLOCK_EMAIL_LOCAL_DOB_MMDD_SOUNDEX = CONCAT_WS(''|'', SPLIT_PART(profile.best_email, ''@'', 1), CASE WHEN profile.best_dob IS NOT NULL THEN TO_CHAR(profile.best_dob, ''MM-DD'') ELSE NULL END, SOUNDEX(profile.best_last_name)),
        BLOCK_EMAIL_LOCAL_DOB_SOUNDEX = CONCAT_WS(''|'', SPLIT_PART(profile.best_email, ''@'', 1), TO_CHAR(profile.best_dob, ''YYYY-MM-DD''), SOUNDEX(profile.best_last_name)),
        PROFILE_UPDATED_AT = CURRENT_TIMESTAMP()
    FROM (
        WITH flattened AS (
            SELECT ic.cluster_id, flat_rec.value::VARCHAR as source_record_id
            FROM ' || DB || '.SILVER.IDR_CORE_CLUSTER ic,
                 LATERAL FLATTEN(input => ic.source_record_ids) flat_rec
            WHERE ic.status = ''ACTIVE''
        ),
        source_identifiers AS (
            SELECT 
                f.cluster_id,
                l.source_record_id,
                l.source_type,
                e.identifier_type,
                e.identifier_value,
                e.identifier_value_normalized,
                COALESCE(sp.priority, 999) AS source_priority
            FROM flattened f
            JOIN ' || DB || '.SILVER.IDR_CORE_IDENTIFIER_LINK l 
                ON f.source_record_id = l.source_record_id AND l.is_active = TRUE
            JOIN ' || DB || '.SILVER.IDR_CORE_ENTITY_IDENTIFIERS e 
                ON l.identifier_id = e.identifier_id AND e.is_active = TRUE
            LEFT JOIN ' || DB || '.CONFIG.SOURCE_PRIORITY sp
                ON l.source_type = sp.source_system
        ),
        name_counts AS (
            SELECT 
                si.cluster_id, 
                MAX(CASE WHEN si.identifier_type = ''NAME_FIRST'' THEN si.identifier_value_normalized END) AS first_name,
                MAX(CASE WHEN si.identifier_type = ''NAME_LAST'' THEN si.identifier_value_normalized END) AS last_name,
                MAX(CASE WHEN si.identifier_type = ''DOB'' THEN TRY_TO_DATE(si.identifier_value_normalized) END) AS date_of_birth,
                MIN(si.source_priority) AS best_source_priority,
                COUNT(*) AS freq
            FROM source_identifiers si
            WHERE si.identifier_type IN (''NAME_FIRST'', ''NAME_LAST'', ''DOB'')
            GROUP BY si.cluster_id, si.source_record_id
        ),
        ranked_names AS (
            SELECT cluster_id, first_name, last_name, date_of_birth,
                   ROW_NUMBER() OVER (
                       PARTITION BY cluster_id 
                       ORDER BY best_source_priority, freq DESC, first_name NULLS LAST
                   ) AS rn
            FROM name_counts
            WHERE first_name IS NOT NULL
        ),
        email_counts AS (
            SELECT cluster_id, identifier_value_normalized AS email,
                   MIN(source_priority) AS best_source_priority,
                   COUNT(*) AS freq
            FROM source_identifiers
            WHERE identifier_type = ''EMAIL'' AND identifier_value_normalized IS NOT NULL
            GROUP BY cluster_id, identifier_value_normalized
        ),
        ranked_emails AS (
            SELECT cluster_id, email,
                   ROW_NUMBER() OVER (
                       PARTITION BY cluster_id 
                       ORDER BY best_source_priority, freq DESC, email
                   ) AS rn
            FROM email_counts
        ),
        phone_counts AS (
            SELECT cluster_id, identifier_value_normalized AS phone,
                   MIN(source_priority) AS best_source_priority,
                   COUNT(*) AS freq
            FROM source_identifiers
            WHERE identifier_type = ''PHONE'' AND identifier_value_normalized IS NOT NULL
            GROUP BY cluster_id, identifier_value_normalized
        ),
        ranked_phones AS (
            SELECT cluster_id, phone,
                   ROW_NUMBER() OVER (
                       PARTITION BY cluster_id 
                       ORDER BY best_source_priority, freq DESC, phone
                   ) AS rn
            FROM phone_counts
        ),
        location_data AS (
            SELECT 
                si.cluster_id, 
                MAX(CASE WHEN si.identifier_type = ''ADDRESS_CITY'' THEN si.identifier_value END) AS city,
                MAX(CASE WHEN si.identifier_type = ''ADDRESS_ZIP'' THEN si.identifier_value END) AS postal_code,
                si.source_priority,
                COUNT(*) AS freq
            FROM source_identifiers si
            WHERE si.identifier_type IN (''ADDRESS_CITY'', ''ADDRESS_ZIP'')
            GROUP BY si.cluster_id, si.source_record_id, si.source_priority
        ),
        ranked_locations AS (
            SELECT cluster_id, city, postal_code,
                   ROW_NUMBER() OVER (
                       PARTITION BY cluster_id 
                       ORDER BY source_priority, freq DESC, city NULLS LAST
                   ) AS rn
            FROM location_data
            WHERE city IS NOT NULL OR postal_code IS NOT NULL
        )
        SELECT 
            f.cluster_id,
            rn.first_name AS best_first_name,
            rn.last_name AS best_last_name,
            re.email AS best_email,
            rp.phone AS best_phone,
            rn.date_of_birth AS best_dob,
            rl.city AS best_city,
            rl.postal_code AS best_zip
        FROM (SELECT DISTINCT cluster_id FROM flattened) f
        LEFT JOIN ranked_names rn ON f.cluster_id = rn.cluster_id AND rn.rn = 1
        LEFT JOIN ranked_emails re ON f.cluster_id = re.cluster_id AND re.rn = 1
        LEFT JOIN ranked_phones rp ON f.cluster_id = rp.cluster_id AND rp.rn = 1
        LEFT JOIN ranked_locations rl ON f.cluster_id = rl.cluster_id AND rl.rn = 1
    ) profile
    WHERE c.cluster_id = profile.cluster_id 
      AND c.status = ''ACTIVE''
      AND (' || :FULL_REBUILD || ' = TRUE OR c.PROFILE_UPDATED_AT IS NULL OR c.UPDATED_AT > c.PROFILE_UPDATED_AT)';
    
    rows_updated := SQLROWCOUNT;
    RETURN 'Profile index built. Updated ' || rows_updated || ' clusters in ' || DATEDIFF('second', start_time, CURRENT_TIMESTAMP()) || 's';
END;
$$;
