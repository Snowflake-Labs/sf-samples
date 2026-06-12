-- ============================================================================
-- Martech: APP.SP_UPDATE_HOUSEHOLD_CLUSTERS
--
-- Cluster-grain mirror of engine SP_UPDATE_CLUSTERS.
-- Computes connected components over IS_ACTIVE+IS_CURRENT edges in
-- IDR_CORE_HOUSEHOLD_MATCH_RESULTS and rebuilds:
--   IDR_CORE_HOUSEHOLD_MEMBERSHIP   (one row per (household_id, cluster_id))
--   IDR_CORE_HOUSEHOLD_CLUSTER      (one row per household with shared addr)
--
-- For demo simplicity this is a full rebuild each run. A future
-- _FULL/incremental split (mirroring engine's SP_UPDATE_CLUSTERS_FULL) can
-- limit work to households touched by new edges in this batch.
--
-- LANGUAGE JS to mirror engine SP_RUN_IDR_PIPELINE pattern: SQL Scripting
-- USING-clause with || concat is brittle across Snowflake versions; building
-- the SQL strings in JS is consistent with how the engine builds dynamic SQL.
-- ============================================================================

CREATE OR REPLACE PROCEDURE IDENTIFIER('&{deployment_db}.APP.SP_UPDATE_HOUSEHOLD_CLUSTERS')(DEPLOYMENT_DB VARCHAR)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
    var DB = DEPLOYMENT_DB;

    // -------------------------------------------------------------------
    // STEP 1: connected components over active household edges. Each
    // component (set of CLUSTER_IDs reachable via R14/R15 edges) becomes
    // one household. HOUSEHOLD_ID = MIN(cluster_id) for stability.
    //
    // Implementation: iterative label propagation, mirroring engine's
    // SP_UPDATE_CLUSTERS_FULL. We deliberately AVOID a recursive CTE here:
    // with bidirectional edges and no cycle-prevention (Snowflake's
    // recursive CTE disallows correlated NOT EXISTS in the recursive arm),
    // the intermediate row count grows O(2^depth) before the GROUP BY
    // collapses it. Label propagation does a bounded number of cheap
    // GROUP BY joins and converges in O(graph_diameter) iterations.
    // -------------------------------------------------------------------
    snowflake.execute({sqlText:
        "CREATE OR REPLACE TEMPORARY TABLE _hh_edges AS " +
        "SELECT A_CLUSTER_ID AS node, B_CLUSTER_ID AS neighbor " +
        "FROM " + DB + ".SILVER.IDR_CORE_HOUSEHOLD_MATCH_RESULTS " +
        "WHERE IS_ACTIVE = TRUE AND IS_CURRENT = TRUE " +
        "UNION ALL " +
        "SELECT B_CLUSTER_ID, A_CLUSTER_ID " +
        "FROM " + DB + ".SILVER.IDR_CORE_HOUSEHOLD_MATCH_RESULTS " +
        "WHERE IS_ACTIVE = TRUE AND IS_CURRENT = TRUE"
    });

    // Seed: every node is its own root. Restrict to nodes that appear in
    // at least one edge; isolated clusters do not form households.
    snowflake.execute({sqlText:
        "CREATE OR REPLACE TEMPORARY TABLE _hh_roots AS " +
        "SELECT DISTINCT node, node AS root FROM _hh_edges"
    });

    // Iterate label propagation until fixed point (max 100 rounds). Each
    // round, every node adopts the minimum root of itself + its neighbors.
    var MAX_ITERS = 100;
    for (var i = 0; i < MAX_ITERS; i++) {
        snowflake.execute({sqlText:
            "CREATE OR REPLACE TEMPORARY TABLE _hh_roots_new AS " +
            "SELECT r.node, LEAST(r.root, COALESCE(MIN(r2.root), r.root)) AS root " +
            "FROM _hh_roots r " +
            "LEFT JOIN _hh_edges e ON e.node = r.node " +
            "LEFT JOIN _hh_roots r2 ON r2.node = e.neighbor " +
            "GROUP BY r.node, r.root"
        });
        var diff = snowflake.execute({sqlText:
            "SELECT COUNT(*) FROM _hh_roots r " +
            "JOIN _hh_roots_new rn ON r.node = rn.node " +
            "WHERE r.root <> rn.root"
        });
        diff.next();
        var diffCount = diff.getColumnValue(1);
        snowflake.execute({sqlText:
            "CREATE OR REPLACE TEMPORARY TABLE _hh_roots AS " +
            "SELECT node, root FROM _hh_roots_new"
        });
        if (diffCount === 0) break;
    }

    snowflake.execute({sqlText:
        "CREATE OR REPLACE TEMPORARY TABLE _hh_components AS " +
        "SELECT node AS CLUSTER_ID, root AS HOUSEHOLD_ID FROM _hh_roots"
    });

    // -------------------------------------------------------------------
    // STEP 2: full rebuild of MEMBERSHIP and HOUSEHOLD_CLUSTER tables.
    // -------------------------------------------------------------------
    snowflake.execute({sqlText: "TRUNCATE TABLE " + DB + ".SILVER.IDR_CORE_HOUSEHOLD_MEMBERSHIP"});
    snowflake.execute({sqlText: "TRUNCATE TABLE " + DB + ".SILVER.IDR_CORE_HOUSEHOLD_CLUSTER"});

    snowflake.execute({sqlText:
        "INSERT INTO " + DB + ".SILVER.IDR_CORE_HOUSEHOLD_MEMBERSHIP (HOUSEHOLD_ID, INDIVIDUAL_CLUSTER_ID) " +
        "SELECT HOUSEHOLD_ID, CLUSTER_ID FROM _hh_components"
    });

    // HOUSEHOLD_TYPE = 'SAME_SURNAME' if all members share PRIMARY_LAST_NAME,
    //                  'FUZZY_SURNAME' otherwise.
    snowflake.execute({sqlText:
        "INSERT INTO " + DB + ".SILVER.IDR_CORE_HOUSEHOLD_CLUSTER " +
        "  (HOUSEHOLD_ID, HOUSEHOLD_TYPE, SHARED_STREET, SHARED_CITY, SHARED_STATE, SHARED_POSTAL, " +
        "   MEMBER_COUNT, PRIMARY_LAST_NAME, STATUS, FIRST_SEEN, LAST_SEEN, UPDATED_AT) " +
        "SELECT m.HOUSEHOLD_ID, " +
        "       CASE WHEN COUNT(DISTINCT p.PRIMARY_LAST_NAME) = 1 THEN 'SAME_SURNAME' ELSE 'FUZZY_SURNAME' END, " +
        "       MAX(p.BILLING_STREET), MAX(p.BILLING_CITY), MAX(p.BILLING_STATE), MAX(p.BILLING_POSTAL), " +
        "       COUNT(*), " +
        "       MIN(p.PRIMARY_LAST_NAME), " +
        "       'ACTIVE', " +
        "       MIN(p.FIRST_SEEN_TS), MAX(p.LAST_SEEN_TS), " +
        "       CURRENT_TIMESTAMP() " +
        "FROM " + DB + ".SILVER.IDR_CORE_HOUSEHOLD_MEMBERSHIP m " +
        "JOIN " + DB + ".GOLD.DT_CUSTOMER_PROFILE p ON p.CLUSTER_ID = m.INDIVIDUAL_CLUSTER_ID " +
        "GROUP BY m.HOUSEHOLD_ID"
    });

    // -------------------------------------------------------------------
    // STEP 3: counts + process state
    // -------------------------------------------------------------------
    var hRow = snowflake.execute({sqlText: "SELECT COUNT(*) FROM " + DB + ".SILVER.IDR_CORE_HOUSEHOLD_CLUSTER"});
    hRow.next();
    var householdCount = hRow.getColumnValue(1);

    var mRow = snowflake.execute({sqlText: "SELECT COUNT(*) FROM " + DB + ".SILVER.IDR_CORE_HOUSEHOLD_MEMBERSHIP"});
    mRow.next();
    var memberCount = mRow.getColumnValue(1);

    snowflake.execute({sqlText:
        "MERGE INTO " + DB + ".SILVER.IDR_CORE_PROCESS_STATE tgt " +
        "USING (SELECT 'SP_UPDATE_HOUSEHOLD_CLUSTERS' AS process_name) src " +
        "ON tgt.process_name = src.process_name " +
        "WHEN MATCHED THEN UPDATE SET " +
        "  last_run_at = CURRENT_TIMESTAMP(), last_run_status = 'SUCCESS', " +
        "  last_run_details = PARSE_JSON('{\"households\": " + householdCount + ", \"members\": " + memberCount + "}') " +
        "WHEN NOT MATCHED THEN INSERT (process_name, last_run_at, last_run_status, last_run_details) " +
        "VALUES ('SP_UPDATE_HOUSEHOLD_CLUSTERS', CURRENT_TIMESTAMP(), 'SUCCESS', " +
        "        PARSE_JSON('{\"households\": " + householdCount + ", \"members\": " + memberCount + "}'))"
    });

    snowflake.execute({sqlText: "DROP TABLE IF EXISTS _hh_components"});
    snowflake.execute({sqlText: "DROP TABLE IF EXISTS _hh_roots"});
    snowflake.execute({sqlText: "DROP TABLE IF EXISTS _hh_roots_new"});
    snowflake.execute({sqlText: "DROP TABLE IF EXISTS _hh_edges"});

    return "Households: " + householdCount + ", members: " + memberCount;
$$;
