# Observability

Pipeline metrics views, cluster health checks, review queue, and refresh monitoring.

## Pipeline Metrics View

```sql
CREATE OR REPLACE VIEW <schema>.PIPELINE_METRICS AS
SELECT
    (SELECT COUNT(DISTINCT master_customer_id) FROM <schema>.GOLDEN_RECORD) AS unique_golden_records,
    (SELECT COUNT(*) FROM <schema>.KEYRING) AS total_source_records_resolved,
    (SELECT COUNT(*) FROM <schema>.IDENTITY_SIGNALS) AS total_source_records_all,
    ROUND(1.0 - (SELECT COUNT(DISTINCT master_customer_id) FROM <schema>.GOLDEN_RECORD)::FLOAT
        / NULLIF((SELECT COUNT(*) FROM <schema>.KEYRING), 0), 4) AS dedup_rate,
    (SELECT AVG(linked_identities_count) FROM <schema>.GOLDEN_RECORD) AS avg_cluster_size,
    (SELECT MAX(linked_identities_count) FROM <schema>.GOLDEN_RECORD) AS max_cluster_size,
    (SELECT COUNT(*) FROM <schema>.IDENTITY_CLUSTERS WHERE is_oversized) AS oversized_cluster_nodes,
    (SELECT COUNT(*) FROM <schema>.FIELD_CONFLICTS) AS field_conflict_count;
```

## Edge Distribution View

```sql
CREATE OR REPLACE VIEW <schema>.EDGE_DISTRIBUTION AS
SELECT
    match_type,
    match_subtype,
    COUNT(*) AS edge_count,
    ROUND(AVG(confidence), 3) AS avg_confidence,
    ROUND(MIN(confidence), 3) AS min_confidence
FROM <schema>.ALL_EDGES
GROUP BY match_type, match_subtype
ORDER BY edge_count DESC;
```

## Review Queue View

Only create if user selected "Full with review queue" in Phase 3:

```sql
CREATE OR REPLACE VIEW <schema>.REVIEW_QUEUE AS
SELECT
    id_a, id_b, match_subtype, confidence, match_status, field_scores
FROM <schema>.PROBABILISTIC_EDGES
WHERE match_status = 'REVIEW'
ORDER BY confidence DESC;
```

## Cluster Size Distribution

```sql
CREATE OR REPLACE VIEW <schema>.CLUSTER_DISTRIBUTION AS
SELECT
    CASE
        WHEN cluster_size = 1 THEN '1 (singleton)'
        WHEN cluster_size BETWEEN 2 AND 5 THEN '2-5'
        WHEN cluster_size BETWEEN 6 AND 10 THEN '6-10'
        WHEN cluster_size BETWEEN 11 AND 20 THEN '11-20'
        WHEN cluster_size BETWEEN 21 AND 50 THEN '21-50'
        ELSE '50+ (oversized)'
    END AS size_bucket,
    COUNT(DISTINCT master_customer_id) AS cluster_count,
    SUM(1) AS node_count
FROM <schema>.IDENTITY_CLUSTERS
GROUP BY size_bucket
ORDER BY MIN(cluster_size);
```

## DT Refresh Status

```sql
CREATE OR REPLACE VIEW <schema>.REFRESH_STATUS AS
SELECT
    name,
    refresh_mode,
    scheduling_state,
    rows,
    data_timestamp
FROM TABLE(INFORMATION_SCHEMA.DYNAMIC_TABLES())
WHERE schema_name = '<schema_name_only>'
ORDER BY name;
```

## Health Check Queries

Run these after initial build and periodically to ensure pipeline health:

### 1. All DTs are INCREMENTAL

```sql
SELECT name, refresh_mode
FROM TABLE(INFORMATION_SCHEMA.DYNAMIC_TABLES())
WHERE schema_name = '<schema_name>'
AND refresh_mode != 'INCREMENTAL';
-- Should return 0 rows
```

### 2. No stale DTs

```sql
SELECT name, data_timestamp,
       DATEDIFF('minute', data_timestamp, CURRENT_TIMESTAMP()) AS minutes_stale
FROM TABLE(INFORMATION_SCHEMA.DYNAMIC_TABLES())
WHERE schema_name = '<schema_name>'
AND DATEDIFF('minute', data_timestamp, CURRENT_TIMESTAMP()) > 120
ORDER BY minutes_stale DESC;
```

### 3. Graph resolution audit trail

```sql
SELECT run_id, COUNT(*) AS edges_resolved, MIN(resolved_at) AS run_time
FROM <schema>.MATCH_AUDIT_LOG
GROUP BY run_id
ORDER BY run_time DESC
LIMIT 10;
```

### 4. Suspicious clusters (potential false merges)

```sql
SELECT
    c.master_customer_id,
    c.cluster_size,
    COUNT(DISTINCT s.email_clean) AS distinct_emails,
    COUNT(DISTINCT s.phone_clean) AS distinct_phones,
    COUNT(DISTINCT s.last_name) AS distinct_last_names
FROM <schema>.IDENTITY_CLUSTERS c
JOIN <schema>.IDENTITY_SIGNALS s
    ON c.node_id = s.source_system || '::' || s.source_id
WHERE c.cluster_size > 10
GROUP BY c.master_customer_id, c.cluster_size
HAVING COUNT(DISTINCT s.last_name) > 3
ORDER BY c.cluster_size DESC;
```

Clusters with many distinct last names likely contain false merges.

### 5. Source contribution

```sql
SELECT
    source_system,
    COUNT(*) AS records_in_graph,
    COUNT(DISTINCT master_customer_id) AS unique_identities_contributed
FROM <schema>.KEYRING
GROUP BY source_system
ORDER BY records_in_graph DESC;
```

## Alerting (Optional)

If the user wants proactive monitoring, suggest a Task-based alert:

```sql
CREATE OR REPLACE TASK <schema>.IDR_HEALTH_CHECK
    WAREHOUSE = <warehouse>
    SCHEDULE = 'USING CRON 0 8 * * * UTC'
AS
BEGIN
    LET v_oversized INT;
    SELECT COUNT(*) INTO :v_oversized
    FROM <schema>.IDENTITY_CLUSTERS WHERE is_oversized = TRUE;

    IF (v_oversized > 0) THEN
        -- Could call an external notification function, or insert into an alerts table
        INSERT INTO <schema>.IDR_ALERTS (alert_type, message, created_at)
        VALUES ('OVERSIZED_CLUSTER', :v_oversized || ' nodes in oversized clusters', CURRENT_TIMESTAMP());
    END IF;
END;
```
