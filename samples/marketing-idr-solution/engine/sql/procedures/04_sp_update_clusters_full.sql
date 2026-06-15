-- ============================================================================
-- SP_UPDATE_CLUSTERS_FULL: Full recompute of connected components across
-- the entire match graph. Kept as fallback / validation reference.
-- Use for one-time rebuilds or to validate incremental correctness.
-- ============================================================================

CREATE OR REPLACE PROCEDURE IDR.PROCEDURES.SP_UPDATE_CLUSTERS_FULL(DEPLOYMENT_DB VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    created_count INTEGER DEFAULT 0;
    updated_count INTEGER DEFAULT 0;
    merge_count INTEGER DEFAULT 0;
    unchanged_count INTEGER DEFAULT 0;
    match_count INTEGER DEFAULT 0;
    no_match_count INTEGER DEFAULT 0;
    diff_count INTEGER DEFAULT 1;
    DB VARCHAR DEFAULT DEPLOYMENT_DB;
BEGIN
    EXECUTE IMMEDIATE 'USE DATABASE ' || DB;
    EXECUTE IMMEDIATE 'USE SCHEMA SILVER';
    
    EXECUTE IMMEDIATE 'CREATE OR REPLACE TEMPORARY TABLE temp_match_edges AS
    SELECT DISTINCT
        LEAST(new_source_record_id, matched_source_record_id) AS src1,
        GREATEST(new_source_record_id, matched_source_record_id) AS src2
    FROM ' || DB || '.SILVER.IDR_CORE_MATCH_RESULTS mr
    WHERE is_active = TRUE
      AND COALESCE(edge_polarity, ''MATCH'') = ''MATCH''
      AND NOT EXISTS (
        SELECT 1 FROM ' || DB || '.SILVER.IDR_CORE_MATCH_RESULTS v
        WHERE v.is_active = TRUE
          AND v.edge_polarity = ''VETO''
          AND LEAST(v.new_source_record_id, v.matched_source_record_id) = LEAST(mr.new_source_record_id, mr.matched_source_record_id)
          AND GREATEST(v.new_source_record_id, v.matched_source_record_id) = GREATEST(mr.new_source_record_id, mr.matched_source_record_id)
      )';

    EXECUTE IMMEDIATE 'CREATE OR REPLACE TEMPORARY TABLE temp_all_sources AS
    SELECT DISTINCT source_record_id
    FROM ' || DB || '.SILVER.IDR_CORE_IDENTIFIER_LINK
    WHERE is_active = TRUE';

    CREATE OR REPLACE TEMPORARY TABLE temp_bidirectional_edges AS
    SELECT src1 AS node, src2 AS neighbor FROM temp_match_edges
    UNION
    SELECT src2 AS node, src1 AS neighbor FROM temp_match_edges;

    -- Connected components via iterative label propagation (faster than recursive CTE for large graphs)
    CREATE OR REPLACE TEMPORARY TABLE roots AS
    SELECT source_record_id AS node, source_record_id AS root FROM temp_all_sources;

    FOR i IN 1 TO 100 DO
        CREATE OR REPLACE TEMPORARY TABLE roots_new AS
        SELECT r.node, LEAST(r.root, COALESCE(MIN(r2.root), r.root)) AS root
        FROM roots r
        LEFT JOIN temp_bidirectional_edges e ON r.node = e.node
        LEFT JOIN roots r2 ON e.neighbor = r2.node
        GROUP BY r.node, r.root;

        SELECT COUNT(*) INTO diff_count FROM roots r
        INNER JOIN roots_new rn ON r.node = rn.node WHERE r.root != rn.root;
        IF (diff_count = 0) THEN BREAK; END IF;

        CREATE OR REPLACE TEMPORARY TABLE roots AS SELECT node, root FROM roots_new;
    END FOR;

    CREATE OR REPLACE TEMPORARY TABLE temp_clusters AS
    SELECT root AS cluster_root, ARRAY_AGG(node) WITHIN GROUP (ORDER BY node) AS members
    FROM roots GROUP BY root;

    CREATE OR REPLACE TEMPORARY TABLE temp_cluster_details AS
    SELECT
        c.cluster_root,
        c.members,
        ARRAY_SIZE(c.members) AS member_count,
        CASE WHEN ARRAY_SIZE(c.members) > 1 THEN 'MATCH' ELSE 'NO_MATCH' END AS decision,
        UUID_STRING() AS new_cluster_id,
        UUID_STRING() AS new_match_id
    FROM temp_clusters c;

    CREATE OR REPLACE TEMPORARY TABLE temp_cluster_details_flat AS
    SELECT cd.cluster_root, m.value::VARCHAR AS member_id
    FROM temp_cluster_details cd,
    LATERAL FLATTEN(input => cd.members) m;

    EXECUTE IMMEDIATE 'CREATE OR REPLACE TEMPORARY TABLE temp_existing_mapping AS
    SELECT 
        f.value::VARCHAR AS source_record_id,
        ec.cluster_id,
        ec.source_record_ids AS existing_members,
        ec.created_at
    FROM ' || DB || '.SILVER.IDR_CORE_CLUSTER ec,
    LATERAL FLATTEN(input => ec.source_record_ids) f
    WHERE ec.status = ''ACTIVE''';

    CREATE OR REPLACE TEMPORARY TABLE temp_cluster_actions AS
    WITH cluster_existing_pairs AS (
        SELECT DISTINCT
            cdf.cluster_root,
            em.cluster_id AS existing_cluster_id,
            em.existing_members,
            em.created_at
        FROM temp_cluster_details_flat cdf
        JOIN temp_existing_mapping em ON em.source_record_id = cdf.member_id
    ),
    cluster_to_existing AS (
        SELECT 
            cd.cluster_root,
            cd.members,
            cd.member_count,
            cd.decision,
            cd.new_cluster_id,
            cd.new_match_id,
            cep.existing_cluster_id,
            cep.existing_members,
            ROW_NUMBER() OVER (PARTITION BY cd.cluster_root ORDER BY cep.created_at) AS rn
        FROM temp_cluster_details cd
        LEFT JOIN cluster_existing_pairs cep ON cd.cluster_root = cep.cluster_root
    ),
    first_existing AS (
        SELECT * FROM cluster_to_existing WHERE rn = 1 OR existing_cluster_id IS NULL
    ),
    cluster_existing_count AS (
        SELECT cluster_root, COUNT(DISTINCT existing_cluster_id) AS existing_count
        FROM cluster_existing_pairs
        GROUP BY cluster_root
    )
    SELECT 
        fe.cluster_root,
        fe.members,
        fe.member_count,
        fe.decision,
        fe.new_cluster_id,
        fe.new_match_id,
        fe.existing_cluster_id,
        fe.existing_members,
        COALESCE(cec.existing_count, 0) AS existing_count,
        CASE 
            WHEN fe.existing_cluster_id IS NULL THEN 'CREATE'
            WHEN COALESCE(cec.existing_count, 0) > 1 THEN 'MERGE'
            WHEN NOT ARRAYS_OVERLAP(fe.members, fe.existing_members) 
                 OR ARRAY_SIZE(fe.members) != ARRAY_SIZE(fe.existing_members) THEN 'UPDATE'
            ELSE 'UNCHANGED'
        END AS action
    FROM first_existing fe
    LEFT JOIN cluster_existing_count cec ON fe.cluster_root = cec.cluster_root;

    EXECUTE IMMEDIATE 'CREATE OR REPLACE TEMPORARY TABLE temp_identifier_lookup AS
    SELECT DISTINCT
        l.source_record_id,
        l.identifier_id
    FROM ' || DB || '.SILVER.IDR_CORE_IDENTIFIER_LINK l
    WHERE l.is_active = TRUE';

    CREATE OR REPLACE TEMPORARY TABLE temp_cluster_identifiers AS
    SELECT 
        ca.cluster_root,
        ARRAY_AGG(til.identifier_id) AS identifier_ids
    FROM temp_cluster_actions ca,
    LATERAL FLATTEN(input => ca.members) m
    JOIN temp_identifier_lookup til ON m.value::VARCHAR = til.source_record_id
    GROUP BY ca.cluster_root;

    CREATE OR REPLACE TEMPORARY TABLE temp_cluster_members_flat AS
    SELECT 
        ca.cluster_root,
        m.value::VARCHAR AS member_id
    FROM temp_cluster_actions ca,
    LATERAL FLATTEN(input => ca.members) m;

    EXECUTE IMMEDIATE 'CREATE OR REPLACE TEMPORARY TABLE temp_match_rules AS
    WITH all_edges AS (
        SELECT 
            m1.cluster_root,
            mr.new_source_record_id AS source_1,
            mr.matched_source_record_id AS source_2,
            mr.rule_name,
            mr.match_score,
            mr.identifier_1
        FROM temp_cluster_members_flat m1
        JOIN temp_cluster_members_flat m2 
            ON m1.cluster_root = m2.cluster_root AND m1.member_id != m2.member_id
        JOIN ' || DB || '.SILVER.IDR_CORE_MATCH_RESULTS mr 
            ON mr.new_source_record_id = m1.member_id 
            AND mr.matched_source_record_id = m2.member_id
            AND mr.is_active = TRUE
            AND COALESCE(mr.edge_polarity, ''MATCH'') = ''MATCH''
    ),
    deduped_edges AS (
        SELECT DISTINCT
            cluster_root,
            LEAST(source_1, source_2) AS source_1,
            GREATEST(source_1, source_2) AS source_2,
            rule_name,
            match_score,
            identifier_1
        FROM all_edges
    )
    SELECT 
        cluster_root,
        ARRAY_AGG(
            CASE 
                WHEN rule_name = ''AI_REVIEW_APPROVED'' THEN
                    OBJECT_CONSTRUCT(
                        ''rule'', rule_name,
                        ''score'', match_score,
                        ''source_1'', source_1,
                        ''source_2'', source_2,
                        ''type'', TRY_PARSE_JSON(identifier_1):type::VARCHAR,
                        ''ai_reasoning'', TRY_PARSE_JSON(identifier_1):ai_reasoning::VARCHAR,
                        ''reviewer'', TRY_PARSE_JSON(identifier_1):reviewer::VARCHAR
                    )
                ELSE
                    OBJECT_CONSTRUCT(
                        ''rule'', rule_name,
                        ''score'', match_score,
                        ''source_1'', source_1,
                        ''source_2'', source_2
                    )
            END
        ) AS rules
    FROM deduped_edges
    GROUP BY cluster_root';

    CREATE OR REPLACE TEMPORARY TABLE temp_match_details AS
    SELECT 
        ca.cluster_root,
        OBJECT_CONSTRUCT(
            'members', ca.members,
            'rules', COALESCE(tmr.rules, ARRAY_CONSTRUCT())
        ) AS match_details
    FROM temp_cluster_actions ca
    LEFT JOIN temp_match_rules tmr ON ca.cluster_root = tmr.cluster_root;

    CREATE OR REPLACE TEMPORARY TABLE temp_clusters_to_merge AS
    SELECT DISTINCT 
        em.cluster_id AS old_cluster_id,
        ca.existing_cluster_id AS new_cluster_id
    FROM temp_existing_mapping em
    JOIN temp_cluster_details_flat cdf ON em.source_record_id = cdf.member_id
    JOIN temp_cluster_actions ca ON cdf.cluster_root = ca.cluster_root
    WHERE ca.action = 'MERGE'
      AND em.cluster_id != ca.existing_cluster_id;

    EXECUTE IMMEDIATE 'UPDATE ' || DB || '.SILVER.IDR_CORE_MATCH_LOG m
    SET is_current = FALSE, superseded_by = t.new_match_id
    FROM (
        SELECT c.match_id AS old_match_id, ca.new_match_id
        FROM ' || DB || '.SILVER.IDR_CORE_CLUSTER c
        JOIN temp_cluster_actions ca ON c.cluster_id = ca.existing_cluster_id
        WHERE ca.action IN (''UPDATE'', ''MERGE'') AND c.match_id IS NOT NULL
        UNION
        SELECT c.match_id AS old_match_id, ca.new_match_id
        FROM ' || DB || '.SILVER.IDR_CORE_CLUSTER c
        JOIN temp_clusters_to_merge tm ON c.cluster_id = tm.old_cluster_id
        JOIN temp_cluster_actions ca ON ca.existing_cluster_id = tm.new_cluster_id AND ca.action = ''MERGE''
        WHERE c.match_id IS NOT NULL
    ) t
    WHERE m.match_id = t.old_match_id AND m.is_current = TRUE';

    EXECUTE IMMEDIATE 'INSERT INTO ' || DB || '.SILVER.IDR_CORE_MATCH_LOG (match_id, source_record_ids, decision, is_current, match_details)
    SELECT 
        ca.new_match_id,
        ca.members,
        ca.decision,
        TRUE,
        tmd.match_details
    FROM temp_cluster_actions ca
    JOIN temp_match_details tmd ON ca.cluster_root = tmd.cluster_root
    WHERE ca.action IN (''CREATE'', ''UPDATE'', ''MERGE'')
      AND (ca.member_count > 1 OR ca.existing_cluster_id IS NOT NULL)';

    EXECUTE IMMEDIATE 'INSERT INTO ' || DB || '.SILVER.IDR_CORE_CLUSTER (cluster_id, source_record_ids, identifier_ids, status, match_id)
    SELECT 
        ca.new_cluster_id,
        ca.members,
        ci.identifier_ids,
        ''ACTIVE'',
        CASE WHEN ca.member_count > 1 THEN ca.new_match_id ELSE NULL END
    FROM temp_cluster_actions ca
    JOIN temp_cluster_identifiers ci ON ca.cluster_root = ci.cluster_root
    WHERE ca.action = ''CREATE''';

    EXECUTE IMMEDIATE 'UPDATE ' || DB || '.SILVER.IDR_CORE_CLUSTER tgt
    SET 
        source_record_ids = ca.members,
        identifier_ids = ci.identifier_ids,
        match_id = ca.new_match_id,
        updated_at = CURRENT_TIMESTAMP()
    FROM temp_cluster_actions ca
    JOIN temp_cluster_identifiers ci ON ca.cluster_root = ci.cluster_root
    WHERE tgt.cluster_id = ca.existing_cluster_id
      AND ca.action IN (''UPDATE'', ''MERGE'')';

    EXECUTE IMMEDIATE 'UPDATE ' || DB || '.SILVER.IDR_CORE_CLUSTER tgt
    SET status = ''MERGED'',
        merged_into = tm.new_cluster_id
    FROM temp_clusters_to_merge tm
    WHERE tgt.cluster_id = tm.old_cluster_id
      AND tgt.status = ''ACTIVE''';

    EXECUTE IMMEDIATE 'INSERT INTO ' || DB || '.SILVER.IDR_CORE_CLUSTER_LOG (cluster_id, event_type, source_record_ids, previous_source_record_ids, merged_from_clusters, match_id, event_details)
    SELECT 
        CASE WHEN ca.action = ''CREATE'' THEN ca.new_cluster_id ELSE ca.existing_cluster_id END,
        CASE ca.action
            WHEN ''CREATE'' THEN ''CREATED''
            WHEN ''UPDATE'' THEN ''MEMBERS_ADDED''
            WHEN ''MERGE'' THEN ''MERGED''
            ELSE ''UNCHANGED''
        END,
        ca.members,
        ca.existing_members,
        CASE WHEN ca.action = ''MERGE'' THEN 
            (SELECT ARRAY_AGG(DISTINCT tm.old_cluster_id) 
             FROM temp_clusters_to_merge tm 
             WHERE tm.new_cluster_id = ca.existing_cluster_id)
        ELSE NULL END,
        ca.new_match_id,
        OBJECT_CONSTRUCT(''action'', ca.action, ''member_count'', ca.member_count)
    FROM temp_cluster_actions ca
    WHERE ca.action != ''UNCHANGED''';

    SELECT COUNT(*) INTO :created_count FROM temp_cluster_actions WHERE action = 'CREATE';
    SELECT COUNT(*) INTO :updated_count FROM temp_cluster_actions WHERE action = 'UPDATE';
    SELECT COUNT(*) INTO :merge_count FROM temp_cluster_actions WHERE action = 'MERGE';
    SELECT COUNT(*) INTO :unchanged_count FROM temp_cluster_actions WHERE action = 'UNCHANGED';
    SELECT COUNT(*) INTO :match_count FROM temp_cluster_actions WHERE member_count > 1;
    SELECT COUNT(*) INTO :no_match_count FROM temp_cluster_actions WHERE member_count = 1;

    DROP TABLE IF EXISTS temp_match_edges;
    DROP TABLE IF EXISTS temp_bidirectional_edges;
    DROP TABLE IF EXISTS temp_all_sources;
    DROP TABLE IF EXISTS roots;
    DROP TABLE IF EXISTS roots_new;
    DROP TABLE IF EXISTS temp_clusters;
    DROP TABLE IF EXISTS temp_cluster_details;
    DROP TABLE IF EXISTS temp_existing_mapping;
    DROP TABLE IF EXISTS temp_cluster_actions;
    DROP TABLE IF EXISTS temp_identifier_lookup;
    DROP TABLE IF EXISTS temp_cluster_identifiers;
    DROP TABLE IF EXISTS temp_cluster_members_flat;
    DROP TABLE IF EXISTS temp_match_rules;
    DROP TABLE IF EXISTS temp_match_details;
    DROP TABLE IF EXISTS temp_cluster_details_flat;
    DROP TABLE IF EXISTS temp_clusters_to_merge;

    RETURN 'Clusters (FULL): ' || :created_count || ' created, ' || :updated_count || ' updated, ' || 
           :merge_count || ' merges, ' || :unchanged_count || ' unchanged. Decisions: ' || 
           :match_count || ' MATCH, ' || :no_match_count || ' NO_MATCH';
END;
$$;
