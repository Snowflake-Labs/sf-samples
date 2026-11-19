---
name: swap-databases
title: "Swap Snowflake Databases"
summary: "Swap two Snowflake databases safely with object sync, grant mirroring, and zerocopy support."
description: "Swap the names of two Snowflake databases safely with object sync and grant mirroring. Also handles read-only databases by redirecting dependent objects from one DB to another without renaming. Also handles zerocopy swap of an imported datashare database with a catalog-linked database (CLD). Use when: swapping databases, renaming databases bidirectionally, blue-green database cutover, migrating database names, database name swap, redirecting dependencies, read-only database migration, shared database replacement, migrate shared views, redirect abstracted views, sfshares migration, zerocopy swap, datashare to CLD, imported database swap CLD. Triggers: swap databases, rename database, swap DB names, database cutover, blue-green swap, exchange databases, redirect dependencies, read-only database, replace shared database, migrate references, migrate sfshares, abstracted views, migrate views, zerocopy, datashare, catalog linked database, CLD, SYSTEM$ZEROCOPY_SWAP."
tools:
  - snowflake_sql_execute
  - snowflake_object_search
  - Read
  - Bash
prompt: "Swap databases MY_DB_OLD and MY_DB_NEW"
language: en
status: Published
author: Chandra Nayak
type: snowflake
---

# Database Swap

Three workflow modes:
- **Mode A (Name Swap):** Renames two databases by swapping their names with object sync and grant mirroring.
- **Mode B (Read-Only Dependency Redirect):** For read-only/shared databases that cannot be renamed. Finds all objects across the account that depend on the first database and recreates them to reference the second database instead.
- **Mode C (Zerocopy Swap — Datashare to CLD):** For swapping an imported datashare database with a catalog-linked database (CLD) using Snowflake's built-in system function.

## Intent Detection

**Ask user which mode applies:**

| Signal | Mode |
|--------|------|
| User says "swap", "rename", "exchange", databases are writable | **Mode A** |
| User says "read-only", "shared", "imported", "redirect", "replace", databases cannot be renamed | **Mode B** |
| User says "datashare", "catalog linked database", "CLD", "zerocopy", or one DB is an imported share and the other is a CLD | **Mode C** |

If Mode A → proceed to [Workflow A](#workflow-a-name-swap)
If Mode B → proceed to [Workflow B](#workflow-b-read-only-dependency-redirect)
If Mode C → proceed to [Workflow C](#workflow-c-zerocopy-swap--datashare-to-cld)

---

## Prerequisites

- Role: ACCOUNTADMIN (or SYSADMIN with MANAGE GRANTS)
- Both databases must exist
- For Mode A: A single matching schema exists in both databases (e.g., PUBLIC)
- For Mode B: A writable Level 1 database/schema containing views that reference the source database
- For Mode C: One database is an imported datashare and the other is a catalog-linked database (CLD)

---

## Workflow A: Name Swap

Use when both databases are writable and you want to swap their names.

### Workflow A Prerequisites

- Both databases must be writable (not imported/shared)
- A single matching schema exists in both databases


### Step 1: Gather Parameters

**Ask user for:**

```
1. First database name (DB1)
2. Second database name (DB2)
3. Schema(s) to swap (comma-separated list, e.g. PUBLIC, SALES, ANALYTICS)
```

Store as variables: `${DB1}`, `${DB2}`, `${SCHEMAS}` (list of schema names)

**Note:** All subsequent steps that reference `${SCHEMA}` should be executed for **each schema** in `${SCHEMAS}`.

### Step 2: Pre-flight Checks

Execute these validations:

```sql
USE ROLE ACCOUNTADMIN;
SHOW DATABASES LIKE '${DB1}';
SHOW DATABASES LIKE '${DB2}';
SHOW DATABASES LIKE '${DB1}_TEMP_SWAP';
```

**If either DB does not exist:** Stop and inform user.
**If TEMP_SWAP name exists:** Ask user for an alternate temp name.

**Schema mismatch check:**

```sql
-- Compare schemas between both databases
SELECT schema_name FROM ${DB1}.INFORMATION_SCHEMA.SCHEMATA WHERE schema_name != 'INFORMATION_SCHEMA' ORDER BY schema_name;
SELECT schema_name FROM ${DB2}.INFORMATION_SCHEMA.SCHEMATA WHERE schema_name != 'INFORMATION_SCHEMA' ORDER BY schema_name;
```

If the schema names do not match between the two databases:

**MANDATORY STOPPING POINT**: Present both schema lists to the user and warn that schemas differ. Ask user whether to proceed anyway or abort. Do NOT continue until user explicitly confirms.

### Step 3: Audit Missing Objects

Run these queries to identify objects in ${DB1} missing from ${DB2}:

```sql
-- Missing tables (excludes views)
SELECT t.table_schema, t.table_name, t.table_type
FROM ${DB1}.INFORMATION_SCHEMA.TABLES t
WHERE (t.table_schema, t.table_name) NOT IN (
    SELECT table_schema, table_name FROM ${DB2}.INFORMATION_SCHEMA.TABLES
)
AND t.table_schema != 'INFORMATION_SCHEMA'
AND t.table_type = 'BASE TABLE';

-- Missing views (compare by name only, do NOT compare view columns/definitions)
SELECT t.table_schema, t.table_name
FROM ${DB1}.INFORMATION_SCHEMA.TABLES t
WHERE t.table_type = 'VIEW'
AND (t.table_schema, t.table_name) NOT IN (
    SELECT table_schema, table_name FROM ${DB2}.INFORMATION_SCHEMA.TABLES
    WHERE table_type = 'VIEW'
)
AND t.table_schema != 'INFORMATION_SCHEMA';

-- Missing sequences
SELECT sequence_schema, sequence_name
FROM ${DB1}.INFORMATION_SCHEMA.SEQUENCES
WHERE (sequence_schema, sequence_name) NOT IN (
    SELECT sequence_schema, sequence_name FROM ${DB2}.INFORMATION_SCHEMA.SEQUENCES
)
AND sequence_schema != 'INFORMATION_SCHEMA';

-- Missing procedures
SELECT procedure_schema, procedure_name, argument_signature
FROM ${DB1}.INFORMATION_SCHEMA.PROCEDURES
WHERE (procedure_schema, procedure_name) NOT IN (
    SELECT procedure_schema, procedure_name FROM ${DB2}.INFORMATION_SCHEMA.PROCEDURES
)
AND procedure_schema != 'INFORMATION_SCHEMA';

-- Missing functions
SELECT function_schema, function_name, argument_signature
FROM ${DB1}.INFORMATION_SCHEMA.FUNCTIONS
WHERE (function_schema, function_name) NOT IN (
    SELECT function_schema, function_name FROM ${DB2}.INFORMATION_SCHEMA.FUNCTIONS
)
AND function_schema != 'INFORMATION_SCHEMA';
```

**Present findings to user.**

### Step 4: Sync Missing Objects

For each missing object from Step 3, retrieve DDL and create in ${DB2}:

```sql
-- For tables:
SELECT GET_DDL('TABLE', '${DB1}.${SCHEMA}.<OBJECT_NAME>');
-- For views:
SELECT GET_DDL('VIEW', '${DB1}.${SCHEMA}.<OBJECT_NAME>');
-- For sequences:
SELECT GET_DDL('SEQUENCE', '${DB1}.${SCHEMA}.<OBJECT_NAME>');
-- For procedures:
SELECT GET_DDL('PROCEDURE', '${DB1}.${SCHEMA}.<PROC_NAME>(<ARG_TYPES>)');
-- For functions:
SELECT GET_DDL('FUNCTION', '${DB1}.${SCHEMA}.<FUNC_NAME>(<ARG_TYPES>)');
```

**Critical:** Replace all occurrences of `${DB1}.` with `${DB2}.` in the DDL output before executing. Also replace any hardcoded `${DB1}.` references inside view bodies or procedure logic.

Execute modified DDL using `CREATE ... IF NOT EXISTS` form.

**If no missing objects:** Skip this step.

### Step 5: Mirror Grants from ${DB1} onto ${DB2}

Capture and replicate all access controls so that after the swap, roles accessing ${DB1} by name still work.

```sql
-- Database-level
SHOW GRANTS ON DATABASE ${DB1};
-- For each row: GRANT <privilege> ON DATABASE ${DB2} TO ROLE <grantee_name>;

-- Schema-level
SHOW GRANTS ON SCHEMA ${DB1}.${SCHEMA};
-- For each row: GRANT <privilege> ON SCHEMA ${DB2}.${SCHEMA} TO ROLE <grantee_name>;

-- Object-level (for each table/view in ${DB1}.${SCHEMA})
SHOW GRANTS ON TABLE ${DB1}.${SCHEMA}.<TABLE_NAME>;
-- For each row: GRANT <privilege> ON TABLE ${DB2}.${SCHEMA}.<TABLE_NAME> TO ROLE <grantee_name>;

-- Future grants
SHOW FUTURE GRANTS IN DATABASE ${DB1};
SHOW FUTURE GRANTS IN SCHEMA ${DB1}.${SCHEMA};
-- For each row: GRANT <privilege> ON FUTURE <object_type_plural> IN DATABASE ${DB2} TO ROLE <grantee_name>;
-- For each row: GRANT <privilege> ON FUTURE <object_type_plural> IN SCHEMA ${DB2}.${SCHEMA} TO ROLE <grantee_name>;
```

**Bulk alternative (up to 3h latency):**

```sql
SELECT
    'GRANT ' || privilege || ' ON ' || granted_on || ' ' ||
    '${DB2}' || SUBSTR(name, LEN('${DB1}') + 1) ||
    ' TO ROLE ' || grantee_name || ';' AS grant_statement
FROM SNOWFLAKE.ACCOUNT_USAGE.GRANTS_TO_ROLES
WHERE name LIKE '${DB1}.%'
  AND deleted_on IS NULL
  AND grantee_name NOT IN ('ACCOUNTADMIN')
ORDER BY granted_on, name;
```

**MANDATORY STOPPING POINT**: Present generated GRANT statements and get user approval before executing.

### Step 6: Name Swap (Three-Step Rename)

```sql
ALTER DATABASE ${DB1} RENAME TO ${DB1}_TEMP_SWAP;
ALTER DATABASE ${DB2} RENAME TO ${DB1};
ALTER DATABASE ${DB1}_TEMP_SWAP RENAME TO ${DB2};
```

**Result after swap:**
- `${DB1}` now contains former ${DB2} data + mirrored grants
- `${DB2}` now contains former ${DB1} data + original grants (followed the object)

### Step 7: Capture Recent Queries

Pull last 24h of successful queries against ${DB1} for replay validation:

```sql
CREATE OR REPLACE TEMPORARY TABLE ${DB1}_QUERY_REPLAY AS
SELECT
    query_id,
    query_text,
    user_name,
    role_name,
    warehouse_name,
    schema_name,
    execution_status,
    start_time
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE database_name = '${DB1}'
  AND execution_status = 'SUCCESS'
  AND start_time >= DATEADD('hour', -24, CURRENT_TIMESTAMP())
  AND query_type IN ('SELECT', 'INSERT', 'UPDATE', 'DELETE', 'MERGE', 'CALL')
ORDER BY start_time DESC;

SELECT COUNT(*) AS total_queries_captured FROM ${DB1}_QUERY_REPLAY;
```

**Present count to user.**

### Step 8: Verify Object Counts and Grants

```sql
SELECT table_schema, COUNT(*) AS object_count
FROM ${DB1}.INFORMATION_SCHEMA.TABLES
WHERE table_schema != 'INFORMATION_SCHEMA'
GROUP BY table_schema;

SELECT table_schema, COUNT(*) AS object_count
FROM ${DB2}.INFORMATION_SCHEMA.TABLES
WHERE table_schema != 'INFORMATION_SCHEMA'
GROUP BY table_schema;

SHOW GRANTS ON DATABASE ${DB1};
SHOW GRANTS ON DATABASE ${DB2};
SHOW GRANTS ON SCHEMA ${DB1}.${SCHEMA};
SHOW GRANTS ON SCHEMA ${DB2}.${SCHEMA};
```

**Present results to user.**

### Step 9: Replay Validation

**MANDATORY STOPPING POINT**: The replay includes destructive query types (INSERT, UPDATE, DELETE, MERGE, CALL) that will modify data. Present the captured query count and types to the user. Get explicit approval before proceeding with replay. Do NOT execute replay without user confirmation.

Execute captured queries under their original roles to verify no permission errors:

```sql
CREATE OR REPLACE TEMPORARY TABLE ${DB1}_REPLAY_RESULTS (
    query_id        VARCHAR,
    role_name       VARCHAR,
    query_text      VARCHAR(16777216),
    replay_status   VARCHAR,
    error_message   VARCHAR(16777216),
    replayed_at     TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE PROCEDURE REPLAY_${DB1}_QUERIES()
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
DECLARE
    result_count INTEGER DEFAULT 0;
    fail_count INTEGER DEFAULT 0;
    c1 CURSOR FOR SELECT query_id, role_name, query_text FROM IDENTIFIER('${DB1}_QUERY_REPLAY') ORDER BY start_time;
    v_qid VARCHAR;
    v_role VARCHAR;
    v_sql VARCHAR;
    v_status VARCHAR;
    v_err VARCHAR;
BEGIN
    FOR rec IN c1 DO
        v_qid := rec.query_id;
        v_role := rec.role_name;
        v_sql := rec.query_text;
        v_status := 'SUCCESS';
        v_err := '';
        BEGIN
            EXECUTE IMMEDIATE 'USE ROLE ' || :v_role;
            EXECUTE IMMEDIATE :v_sql;
        EXCEPTION
            WHEN OTHER THEN
                v_status := 'FAILED';
                v_err := SQLERRM;
                fail_count := fail_count + 1;
        END;
        EXECUTE IMMEDIATE 'USE ROLE ACCOUNTADMIN';
        INSERT INTO IDENTIFIER('${DB1}_REPLAY_RESULTS')
            (query_id, role_name, query_text, replay_status, error_message)
        VALUES (:v_qid, :v_role, :v_sql, :v_status, :v_err);
        result_count := result_count + 1;
    END FOR;
    EXECUTE IMMEDIATE 'USE ROLE ACCOUNTADMIN';
    RETURN 'Replayed ' || :result_count || ' queries. Failures: ' || :fail_count;
END;

CALL REPLAY_${DB1}_QUERIES();

-- Summary
SELECT replay_status, COUNT(*) AS query_count
FROM ${DB1}_REPLAY_RESULTS
GROUP BY replay_status;

-- Failed queries detail
SELECT query_id, role_name, LEFT(query_text, 300) AS query_preview, error_message
FROM ${DB1}_REPLAY_RESULTS
WHERE replay_status = 'FAILED'
ORDER BY role_name;
```

**If failures exist:** Investigate missing grants and apply fixes, then re-run failed queries.

## Stopping Points

- After Step 3 (audit results) — user reviews what will be synced
- After Step 5 (grant mirroring) — user approves GRANT statements before execution
- Before Step 9 (replay validation) — user approves replaying destructive queries
- After Step 9 (replay results) — user reviews any failures

## Rollback

If the swap needs to be undone:

```sql
ALTER DATABASE ${DB2} RENAME TO ${DB1}_TEMP_SWAP;
ALTER DATABASE ${DB1} RENAME TO ${DB2};
ALTER DATABASE ${DB1}_TEMP_SWAP RENAME TO ${DB1};
```

Grants that followed the physical objects revert automatically. Manually revoke any mirrored grants from Step 5 that are no longer appropriate.

## Caveats

| Concern | Action |
|---------|--------|
| Views with hardcoded DB references in body | Replace DB name in view DDL before creating |
| Dynamic tables / streams / tasks referencing DB name | Inspect and update definitions after swap |
| ACCOUNT_USAGE latency (up to 3h) | Use SHOW GRANTS per object for real-time capture |
| Temp name conflict | Verify no DB named ${DB1}_TEMP_SWAP exists first |

---

## Workflow B: Read-Only Dependency Redirect

Use when both databases are read-only (e.g., imported shared databases) and cannot be renamed. This workflow recreates all views in a Level 1 (consumer) database/schema so they point to the target replacement database instead of the source database.

### Step B1: Gather Parameters

**Ask user for:**

```
1. Source database being retired/replaced (DB1) — the read-only L0 database being replaced
2. Target replacement database (DB2) — the new read-only L0 database to point to
3. Level 1 database name (L1_DB) — the writable database containing views that reference DB1
4. Level 1 schema name (L1_SCHEMA) — the schema within L1_DB whose views will be recreated
```

Store as variables: `${DB1}`, `${DB2}`, `${L1_DB}`, `${L1_SCHEMA}`

### Step B2: Pre-flight Checks

```sql
USE ROLE ACCOUNTADMIN;
SHOW DATABASES LIKE '${DB1}';
SHOW DATABASES LIKE '${DB2}';
SHOW DATABASES LIKE '${L1_DB}';
SELECT schema_name FROM ${L1_DB}.INFORMATION_SCHEMA.SCHEMATA WHERE schema_name = '${L1_SCHEMA}';
```

Verify all databases exist and the L1 schema exists.

### Step B3: List All Views in L1 Database Schema

List all views in `${L1_DB}.${L1_SCHEMA}` that will be recreated to point to ${DB2}:

```sql
SELECT table_name
FROM ${L1_DB}.INFORMATION_SCHEMA.TABLES
WHERE table_schema = '${L1_SCHEMA}'
  AND table_type = 'VIEW'
ORDER BY table_name;
```

**Present the view list to user.**

**MANDATORY STOPPING POINT**: Get user approval before proceeding.

### Step B4: Get DDL and Grants for Each View

For each view found in Step B3:

```sql
-- Get current DDL
SELECT GET_DDL('VIEW', '${L1_DB}.${L1_SCHEMA}.<VIEW_NAME>');

-- Capture current grants (before recreation resets them)
SHOW GRANTS ON VIEW ${L1_DB}.${L1_SCHEMA}.<VIEW_NAME>;
```

### Step B5: Recreate All Views Pointing to ${DB2}

For each view DDL retrieved in Step B4:

1. **Replace** all occurrences of `${DB1}` with `${DB2}` in the DDL text (including inside view body references)
2. **Execute** the modified DDL using `CREATE OR REPLACE` to update the view in place

```sql
-- Example: Original view references ${DB1}
-- Original: SELECT * FROM ${DB1}."schema_DEMO_SHARE"."Cust_by_Sex"
-- Modified: SELECT * FROM ${DB2}."schema_DEMO_SHARE"."Cust_by_Sex"
CREATE OR REPLACE VIEW ${L1_DB}.${L1_SCHEMA}.<VIEW_NAME>(...) AS
  SELECT * FROM ${DB2}.<SCHEMA>.<TABLE_NAME>;
```

### Step B6: Reapply Grants on Recreated Views

After recreating views with `CREATE OR REPLACE`, ownership may reset. Reapply grants captured in Step B4:

```sql
-- For each non-OWNERSHIP grant captured:
GRANT <privilege> ON VIEW ${L1_DB}.${L1_SCHEMA}.<VIEW_NAME> TO ROLE <ROLE_NAME>;
```

### Step B7: Verify

Smoke test each recreated view to confirm it reads from ${DB2}:

```sql
-- Test each recreated view
SELECT * FROM ${L1_DB}.${L1_SCHEMA}.<VIEW_NAME> LIMIT 1;
```

Optionally, confirm via OBJECT_DEPENDENCIES (note: up to 3h latency):

```sql
SELECT referencing_object_name, referenced_database
FROM SNOWFLAKE.ACCOUNT_USAGE.OBJECT_DEPENDENCIES
WHERE referencing_database = '${L1_DB}'
  AND referencing_schema = '${L1_SCHEMA}';
```

### Step B8: Prompt to Drop Source Database

After all views are verified, ask the user if they want to drop the retired source database:

**Ask user:**

```
All views in ${L1_DB}.${L1_SCHEMA} have been successfully redirected to ${DB2}.
Would you like to drop the old source database ${DB1}?
```

**If user approves:**

```sql
DROP DATABASE ${DB1};
```

**If user declines:** Leave ${DB1} in place. Inform user they can drop it later with `DROP DATABASE ${DB1};` when ready.

### Stopping Points (Mode B)

- After Step B3 (view list) — user reviews which views will be recreated
- After Step B5 (recreated views) — user verifies views work correctly
- After Step B7 (verification) — user decides whether to drop ${DB1}

### Caveats (Mode B)

| Concern | Action |
|---------|--------|
| GET_DDL not supported on shared databases | Only run GET_DDL on the L1 (writable) database views, not on the L0 shared databases |
| ACCOUNT_USAGE latency (up to 3h) | Dependencies view may not reflect changes immediately; use smoke tests for immediate validation |
| CREATE OR REPLACE resets grants | Capture grants before recreating each view, reapply after |
| Mixed-case identifiers | Use double quotes for schema/object names that are case-sensitive |

---

## Workflow C: Zerocopy Swap — Datashare to CLD

Use when one database is an imported datashare and the other is a catalog-linked database (CLD). Snowflake provides a system function that atomically swaps the imported database with the CLD, preserving grants and references.

### Step C1: Gather Parameters

**Ask user for:**

```
1. Imported datashare database name (IMPORTED_DB) — the database created from a data share
2. Catalog-linked database name (CLD_DB) — the catalog-linked database to swap with
```

Store as variables: `${IMPORTED_DB}`, `${CLD_DB}`

### Step C2: Pre-flight Checks

```sql
USE ROLE ACCOUNTADMIN;
SHOW DATABASES LIKE '${IMPORTED_DB}';
SHOW DATABASES LIKE '${CLD_DB}';
```

Verify both databases exist. Confirm:
- `${IMPORTED_DB}` is an imported share (check the `origin` column is non-empty, or `kind` indicates it's shared/imported)
- `${CLD_DB}` is a catalog-linked database (check `kind` = `LINKED_CATALOG` or similar indicator)

**Schema mismatch check:**

```sql
-- Compare schemas between both databases
SELECT schema_name FROM ${IMPORTED_DB}.INFORMATION_SCHEMA.SCHEMATA WHERE schema_name != 'INFORMATION_SCHEMA' ORDER BY schema_name;
SELECT schema_name FROM ${CLD_DB}.INFORMATION_SCHEMA.SCHEMATA WHERE schema_name != 'INFORMATION_SCHEMA' ORDER BY schema_name;
```

If the schema names do not match between the two databases:

**MANDATORY STOPPING POINT**: Present both schema lists to the user and warn that schemas differ. Ask user whether to proceed anyway or abort. Do NOT continue until user explicitly confirms.

### Step C3: Review Current State

```sql
-- Check grants on both databases before swap
SHOW GRANTS ON DATABASE ${IMPORTED_DB};
SHOW GRANTS ON DATABASE ${CLD_DB};

-- Check object counts
SELECT table_schema, COUNT(*) AS object_count
FROM ${IMPORTED_DB}.INFORMATION_SCHEMA.TABLES
WHERE table_schema != 'INFORMATION_SCHEMA'
GROUP BY table_schema;

SELECT table_schema, COUNT(*) AS object_count
FROM ${CLD_DB}.INFORMATION_SCHEMA.TABLES
WHERE table_schema != 'INFORMATION_SCHEMA'
GROUP BY table_schema;
```

**Present findings to user.**

**MANDATORY STOPPING POINT**: Get user approval before executing the swap.

### Step C4: Execute Zerocopy Swap

```sql
SELECT SYSTEM$ZEROCOPY_SWAP_IMPORTED_DB_WITH_CLD('${IMPORTED_DB}', '${CLD_DB}');
```

This system function atomically:
- Swaps the database names
- Preserves grants and privileges
- Maintains references from dependent objects

### Step C5: Verify

```sql
-- Verify databases exist with swapped names
SHOW DATABASES LIKE '${IMPORTED_DB}';
SHOW DATABASES LIKE '${CLD_DB}';

-- Verify grants are preserved
SHOW GRANTS ON DATABASE ${IMPORTED_DB};
SHOW GRANTS ON DATABASE ${CLD_DB};

-- Smoke test: query an object in the swapped database
SELECT * FROM ${IMPORTED_DB}.INFORMATION_SCHEMA.TABLES WHERE table_schema != 'INFORMATION_SCHEMA' LIMIT 5;
```

**Present results to user.**

### Stopping Points (Mode C)

- After Step C3 (review) — user confirms the swap should proceed
- After Step C5 (verification) — user confirms the swap completed correctly

### Caveats (Mode C)

| Concern | Action |
|---------|--------|
| Requires ACCOUNTADMIN | System function requires elevated privileges |
| Atomic operation | The swap is all-or-nothing; no partial state |
| Database types must match | First argument must be an imported share, second must be a CLD |
| Dependent views/objects | References are preserved automatically by the system function |

---

## Output

**Mode A:**
- Both databases swapped successfully
- All grants mirrored and validated via query replay
- Summary of replay results (pass/fail counts)
- List of any failed queries requiring manual remediation

**Mode B:**
- All dependent objects across the account recreated to reference ${DB2}
- Grants preserved on recreated objects
- Verification that no remaining dependencies on ${DB1} exist

**Mode C:**
- Imported datashare database and CLD swapped atomically via SYSTEM$ZEROCOPY_SWAP_IMPORTED_DB_WITH_CLD
- Grants and references preserved automatically
- Verification that swapped databases are accessible
