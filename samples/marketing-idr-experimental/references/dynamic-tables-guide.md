# Dynamic Tables Guide (Identity Resolution)

All pipeline layers except graph resolution use `REFRESH_MODE = INCREMENTAL` Dynamic Tables. This file covers IDR-specific DT patterns and constraints.

## Target Lag Defaults

| Layer | Default TARGET_LAG | Rationale |
|-------|-------------------|-----------|
| Standardization (STD_SOURCE_*) | DOWNSTREAM | Only refresh when downstream needs it |
| Identity Signals (union) | DOWNSTREAM | Same — feeds edges |
| Deterministic Edges | DOWNSTREAM | Feeds ALL_EDGES |
| Probabilistic Candidates | DOWNSTREAM | Feeds scoring |
| Probabilistic Edges | DOWNSTREAM | Feeds ALL_EDGES |
| All Edges | DOWNSTREAM | Feeds graph resolution (via Stream) |
| Keyring | 1 hour | User-facing output |
| Golden Record | 1 hour | User-facing output |
| Field Conflicts | 1 hour | Data quality surface |

Override via user config in Phase 3 if needed.

## Standard DDL Pattern

```sql
CREATE OR REPLACE DYNAMIC TABLE <schema>.<name>
    TARGET_LAG = '<lag>'
    WAREHOUSE = <warehouse>
    REFRESH_MODE = INCREMENTAL
    INITIALIZE = ON_CREATE
AS
<query>;
```

Always explicitly set `REFRESH_MODE = INCREMENTAL`. If Snowflake falls back to FULL, check `refresh_mode_reason` via `SHOW DYNAMIC TABLES`.

## Incremental-Safe SQL Patterns

### DO (safe for incremental refresh)

- `SELECT` with deterministic expressions
- `WHERE` / `HAVING` / `QUALIFY` filters
- `UNION ALL`
- `INNER JOIN` on equality predicates
- `LEFT JOIN` on equality predicates
- `GROUP BY` with standard aggregates (MIN, MAX, SUM, COUNT, AVG)
- `QUALIFY ROW_NUMBER() OVER (PARTITION BY ... ORDER BY ...) = 1`
- `FIRST_VALUE`, `LAST_VALUE`, `NTH_VALUE` window functions
- `MIN_BY`, `MAX_BY`
- `JAROWINKLER_SIMILARITY`, `EDITDISTANCE` (deterministic built-ins)
- `REGEXP_REPLACE`, `SPLIT_PART`, `LOWER`, `UPPER`, `TRIM` (deterministic)
- `COALESCE`, `IFF`, `CASE WHEN`
- `CROSS JOIN` (fine when one side is a single-row config table)
- CTEs (non-recursive)

### DON'T (forces full refresh or not supported)

- `WITH RECURSIVE` — NOT SUPPORTED for incremental. Use SP + Task instead.
- `CURRENT_TIMESTAMP()` in SELECT — only allowed in WHERE/HAVING/QUALIFY
- `UUID_STRING()`, `RANDOM()` — non-deterministic
- `SEQ4()`, sequence functions — not supported
- `PIVOT` / `UNPIVOT` — not supported
- Subqueries outside of FROM clause (e.g., `WHERE EXISTS (SELECT ...)`) — not supported
- `MINUS`, `EXCEPT`, `INTERSECT` — not supported (use UNION ALL + anti-join pattern)
- External functions — not supported
- Volatile UDFs — not supported

## IDR-Specific Patterns

### Self-Join for Edge Generation (Incremental-Safe)

```sql
-- This pattern IS incremental-safe
SELECT a.id, b.id
FROM signals a
JOIN signals b
    ON a.email = b.email
    AND a.id < b.id;  -- equality + inequality, both sides tracked
```

Snowflake handles changes to both sides of the join incrementally.

### QUALIFY ROW_NUMBER for Deduplication (Optimized)

```sql
-- Highly efficient in incremental mode
SELECT *
FROM candidates
QUALIFY ROW_NUMBER() OVER (PARTITION BY id_a, id_b ORDER BY blocking_strategy) = 1;
```

This is an optimized pattern — Snowflake uses a special fast path for `QUALIFY ROW_NUMBER() = 1`.

### Window Functions for Survivorship (Incremental-Safe)

```sql
FIRST_VALUE(email) IGNORE NULLS OVER (
    PARTITION BY master_customer_id
    ORDER BY source_priority ASC, source_updated_at DESC
) AS best_email
```

Window functions with PARTITION BY are incremental-safe. Without PARTITION BY they force full recompute.

### OBJECT_CONSTRUCT for Score Storage (Safe)

```sql
OBJECT_CONSTRUCT('name_score', x, 'addr_score', y) AS field_scores
```

Deterministic function — safe for incremental.

## Performance Tips

1. **Cluster source tables** by identity fields used in JOINs:
   ```sql
   ALTER TABLE <source> CLUSTER BY (email);
   ```

2. **Split complex DTs** if a single DT has many JOINs + GROUP BY + window functions. Materialize intermediate results.

3. **Use DOWNSTREAM for intermediate DTs** — avoids unnecessary refreshes.

4. **Monitor refresh history** to catch regressions:
   ```sql
   SELECT * FROM TABLE(INFORMATION_SCHEMA.DYNAMIC_TABLE_REFRESH_HISTORY())
   WHERE name = '<dt_name>'
   ORDER BY refresh_start_time DESC
   LIMIT 10;
   ```

5. **Less than 5% change volume** between refreshes is ideal for incremental performance.
