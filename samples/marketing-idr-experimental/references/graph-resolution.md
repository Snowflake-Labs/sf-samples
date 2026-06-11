# Graph Resolution

Staged iterative label propagation: deterministic-first resolution, then probabilistic only for unresolved singletons. Plus Stream + Task wiring for incremental execution.

## Why Not a Dynamic Table?

`WITH RECURSIVE` is NOT supported in Dynamic Table incremental refresh mode. Graph traversal (finding connected components) is inherently iterative. Therefore we use:
- A **stored procedure** that performs staged iterative label propagation
- A **Stream** on the ALL_EDGES Dynamic Table to detect new edges
- A **Task** triggered by the Stream to re-run resolution when edges change

## Algorithm: Staged Resolution

**Stage 1 — Deterministic Only:**
1. Every node with a deterministic edge starts with itself as its cluster label
2. Each iteration: every node adopts the MINIMUM label reachable through DETERMINISTIC edges only
3. Repeat until convergence
4. Identify singletons (cluster_size = 1) — these are records that didn't match deterministically

**Stage 2 — Probabilistic for Singletons Only:**
5. Filter probabilistic edges to only those where at least one side is a singleton from Stage 1
6. Add these edges to the graph and re-run convergence on the combined edge set
7. This dramatically reduces the probabilistic candidate space

**Benefits of staged resolution:**
- Reduces probabilistic edge volume (only singletons are candidates, not already-resolved nodes)
- Prevents probabilistic edges from contaminating high-confidence deterministic clusters
- Faster execution (Stage 2 operates on a much smaller edge set)
- Better precision (deterministic clusters are "locked in" before fuzzy matching runs)

## Identity Clusters Table

```sql
CREATE OR REPLACE TABLE <schema>.IDENTITY_CLUSTERS (
    node_id              VARCHAR NOT NULL,
    master_customer_id   VARCHAR NOT NULL,
    cluster_size         INT,
    iteration_resolved   INT,
    run_id               VARCHAR,
    is_oversized         BOOLEAN DEFAULT FALSE,
    resolved_at          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);
```

## Stored Procedure Template (Staged)

```sql
CREATE OR REPLACE PROCEDURE <schema>.RESOLVE_IDENTITY_GRAPH_STAGED()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    v_run_id VARCHAR DEFAULT UUID_STRING();
    v_iteration INT DEFAULT 0;
    v_changed INT DEFAULT 1;
    v_max_cluster INT;
    v_det_clusters INT;
    v_det_nodes INT;
    v_singletons INT;
    v_final_clusters INT;
    v_final_nodes INT;
    v_oversized INT;
    v_iter2 INT DEFAULT 0;
BEGIN
    SELECT max_cluster_size INTO :v_max_cluster
    FROM <schema>.MATCH_WEIGHTS_CONFIG WHERE is_active = TRUE LIMIT 1;

    -- ========== STAGE 1: Deterministic edges only ==========

    CREATE OR REPLACE TEMPORARY TABLE <schema>.TMP_LABELS AS
    SELECT DISTINCT node_id, node_id AS label
    FROM (
        SELECT id_a AS node_id FROM <schema>.DETERMINISTIC_EDGES
        UNION
        SELECT id_b AS node_id FROM <schema>.DETERMINISTIC_EDGES
    );

    WHILE (v_changed > 0 AND v_iteration < 25) DO
        v_iteration := v_iteration + 1;

        CREATE OR REPLACE TEMPORARY TABLE <schema>.TMP_NEW_LABELS AS
        SELECT
            l.node_id,
            LEAST(l.label, MIN(nl.label)) AS label
        FROM <schema>.TMP_LABELS l
        LEFT JOIN <schema>.DETERMINISTIC_EDGES e
            ON l.node_id = e.id_a OR l.node_id = e.id_b
        LEFT JOIN <schema>.TMP_LABELS nl
            ON nl.node_id = IFF(e.id_a = l.node_id, e.id_b, e.id_a)
        GROUP BY l.node_id, l.label;

        SELECT COUNT(*) INTO :v_changed
        FROM <schema>.TMP_LABELS old_l
        JOIN <schema>.TMP_NEW_LABELS new_l ON old_l.node_id = new_l.node_id
        WHERE old_l.label != new_l.label;

        DROP TABLE <schema>.TMP_LABELS;
        ALTER TABLE <schema>.TMP_NEW_LABELS RENAME TO <schema>.TMP_LABELS;
    END WHILE;

    -- Identify singletons
    CREATE OR REPLACE TEMPORARY TABLE <schema>.TMP_DET_CLUSTERS AS
    SELECT node_id, label, COUNT(*) OVER (PARTITION BY label) AS cluster_size
    FROM <schema>.TMP_LABELS;

    SELECT COUNT(DISTINCT label) INTO :v_det_clusters FROM <schema>.TMP_DET_CLUSTERS;
    SELECT COUNT(*) INTO :v_det_nodes FROM <schema>.TMP_DET_CLUSTERS;
    SELECT COUNT(*) INTO :v_singletons FROM <schema>.TMP_DET_CLUSTERS WHERE cluster_size = 1;

    -- ========== STAGE 2: Probabilistic for singletons only ==========

    CREATE OR REPLACE TEMPORARY TABLE <schema>.TMP_PROB_EDGES AS
    SELECT p.id_a, p.id_b, p.confidence
    FROM <schema>.PROBABILISTIC_EDGES p
    WHERE p.match_status = 'ACCEPTED'
    AND (
        p.id_a IN (SELECT node_id FROM <schema>.TMP_DET_CLUSTERS WHERE cluster_size = 1)
        OR p.id_b IN (SELECT node_id FROM <schema>.TMP_DET_CLUSTERS WHERE cluster_size = 1)
    );

    -- Add new nodes from probabilistic edges
    INSERT INTO <schema>.TMP_LABELS (node_id, label)
    SELECT DISTINCT node_id, node_id
    FROM (
        SELECT id_a AS node_id FROM <schema>.TMP_PROB_EDGES
        UNION
        SELECT id_b AS node_id FROM <schema>.TMP_PROB_EDGES
    ) p
    WHERE NOT EXISTS (SELECT 1 FROM <schema>.TMP_LABELS l WHERE l.node_id = p.node_id);

    -- Re-run convergence on combined edges
    v_changed := 1;

    WHILE (v_changed > 0 AND v_iter2 < 25) DO
        v_iter2 := v_iter2 + 1;

        CREATE OR REPLACE TEMPORARY TABLE <schema>.TMP_NEW_LABELS AS
        WITH combined_edges AS (
            SELECT id_a, id_b FROM <schema>.DETERMINISTIC_EDGES
            UNION ALL
            SELECT id_a, id_b FROM <schema>.TMP_PROB_EDGES
        )
        SELECT
            l.node_id,
            LEAST(l.label, MIN(nl.label)) AS label
        FROM <schema>.TMP_LABELS l
        LEFT JOIN combined_edges e
            ON l.node_id = e.id_a OR l.node_id = e.id_b
        LEFT JOIN <schema>.TMP_LABELS nl
            ON nl.node_id = IFF(e.id_a = l.node_id, e.id_b, e.id_a)
        GROUP BY l.node_id, l.label;

        SELECT COUNT(*) INTO :v_changed
        FROM <schema>.TMP_LABELS old_l
        JOIN <schema>.TMP_NEW_LABELS new_l ON old_l.node_id = new_l.node_id
        WHERE old_l.label != new_l.label;

        DROP TABLE <schema>.TMP_LABELS;
        ALTER TABLE <schema>.TMP_NEW_LABELS RENAME TO <schema>.TMP_LABELS;
    END WHILE;

    -- ========== Write final clusters ==========

    TRUNCATE TABLE <schema>.IDENTITY_CLUSTERS;

    INSERT INTO <schema>.IDENTITY_CLUSTERS
        (node_id, master_customer_id, cluster_size, iteration_resolved, run_id, is_oversized, resolved_at)
    SELECT
        node_id,
        label AS master_customer_id,
        COUNT(*) OVER (PARTITION BY label) AS cluster_size,
        :v_iteration + :v_iter2 AS iteration_resolved,
        :v_run_id AS run_id,
        IFF(COUNT(*) OVER (PARTITION BY label) > :v_max_cluster, TRUE, FALSE) AS is_oversized,
        CURRENT_TIMESTAMP()
    FROM <schema>.TMP_LABELS;

    SELECT COUNT(DISTINCT master_customer_id) INTO :v_final_clusters FROM <schema>.IDENTITY_CLUSTERS;
    SELECT COUNT(*) INTO :v_final_nodes FROM <schema>.IDENTITY_CLUSTERS;
    SELECT COUNT(*) INTO :v_oversized FROM <schema>.IDENTITY_CLUSTERS WHERE is_oversized = TRUE;

    RETURN 'STAGED | Stage 1 (det): ' || :v_iteration || ' iters, ' || :v_det_nodes || ' nodes, '
        || :v_det_clusters || ' clusters, ' || :v_singletons || ' singletons'
        || ' | Stage 2 (prob): ' || :v_iter2 || ' iters'
        || ' | Final: ' || :v_final_nodes || ' nodes, ' || :v_final_clusters || ' identities, '
        || :v_oversized || ' oversized';
END;
$$;
```

## Stream + Task Wiring

```sql
-- Stream detects new/changed edges
CREATE OR REPLACE STREAM <schema>.EDGES_STREAM
    ON DYNAMIC TABLE <schema>.ALL_EDGES
    SHOW_INITIAL_ROWS = FALSE;

-- Task re-runs resolution when new edges arrive
CREATE OR REPLACE TASK <schema>.RESOLVE_GRAPH_TASK
    WAREHOUSE = <warehouse>
    SCHEDULE = '60 MINUTES'
    WHEN SYSTEM$STREAM_HAS_DATA('<schema>.EDGES_STREAM')
AS
    CALL <schema>.RESOLVE_IDENTITY_GRAPH();

ALTER TASK <schema>.RESOLVE_GRAPH_TASK RESUME;
```

## Initial Execution

After creating all objects, execute the SP once to populate initial clusters:

```sql
CALL <schema>.RESOLVE_IDENTITY_GRAPH();
```

Verify output:
```sql
SELECT
    COUNT(*) AS total_nodes,
    COUNT(DISTINCT master_customer_id) AS unique_identities,
    AVG(cluster_size) AS avg_cluster_size,
    MAX(cluster_size) AS max_cluster_size,
    COUNT(CASE WHEN is_oversized THEN 1 END) AS oversized_nodes
FROM <schema>.IDENTITY_CLUSTERS;
```

## Cluster Quality Checks

After resolution, check for quality issues:

```sql
-- Oversized clusters (potential false merges)
SELECT master_customer_id, cluster_size
FROM <schema>.IDENTITY_CLUSTERS
WHERE is_oversized = TRUE
GROUP BY master_customer_id, cluster_size
ORDER BY cluster_size DESC;

-- Singleton nodes (appeared in edges but resolved to themselves)
SELECT COUNT(*) AS singletons
FROM <schema>.IDENTITY_CLUSTERS
WHERE cluster_size = 1;

-- Convergence check (should be < 10 iterations for healthy data)
SELECT DISTINCT iteration_resolved FROM <schema>.IDENTITY_CLUSTERS;
```

## Troubleshooting

| Symptom | Likely Cause | Fix |
|---------|-------------|-----|
| Doesn't converge (hits 25 iterations) | Extremely dense graph, possibly a super-node | Check max cluster size; add toxic IDs; raise confidence threshold |
| Very large clusters (>100 nodes) | Shared identifier poisoning the graph | Find the connecting edge with lowest confidence; add to toxic blacklist |
| Many singletons | Edges exist but aren't being followed | Check that `OR` in the JOIN condition works correctly (id_a OR id_b) |
| Task never fires | Stream is empty or not consuming | Check `SYSTEM$STREAM_HAS_DATA`; verify DT is refreshing upstream |
| SP runs slowly | Large ALL_EDGES table | Consider partitioning by confidence tier; only resolve edges > 0.85 |

## Manual Re-Resolution

To force a re-run (e.g., after adding toxic IDs or changing thresholds):

```sql
CALL <schema>.RESOLVE_IDENTITY_GRAPH();
```

This is safe to run anytime — it truncates and rewrites IDENTITY_CLUSTERS atomically.
