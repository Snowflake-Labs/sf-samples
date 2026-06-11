---
name: marketing-identity-resolution
description: "Build a first-party identity resolution pipeline on any set of source tables in Snowflake. Discovers source schemas, maps columns to canonical identity fields, generates a fully incremental Dynamic Table pipeline with deterministic and probabilistic matching, graph resolution, a keyring, and a golden record with field-level survivorship. Use for: identity resolution, IDR, identity graph, customer deduplication, identity stitching, master customer ID, golden record, customer 360 identity, first-party data graph, merge duplicate customers, link customer records across systems. Triggers: identity resolution, build IDR, identity graph, deduplicate customers, stitch identities, master customer, golden record, link records, merge duplicates, resolve identities, first-party identity, 1PD identity, identity pipeline, customer dedup."
---

# Marketing Identity Resolution

Build a production-grade first-party identity resolution pipeline entirely in SQL on Snowflake. Discovers your source tables, maps identity fields, generates incremental Dynamic Tables for deterministic and probabilistic matching, resolves connected components via iterative graph traversal, and produces a unified golden record with field-level survivorship.

## Workflow

```
Customer Context → Discover Sources → Profile Schemas → Map Identity Fields →
Configure → Build Standardization → Build Edges →
Build Graph Resolution → Build Output → Verify → Deliver
```

## When to Use

Use this skill when the user wants to:
- Build a first-party identity graph across multiple source systems
- Deduplicate customers across CRM, POS, web, mobile, email, etc.
- Create a master_customer_id / golden record from fragmented data
- Link customer records that share emails, phones, loyalty IDs, or other identifiers
- Implement deterministic and/or probabilistic identity stitching

Typical triggers:
- "Build identity resolution on our customer data"
- "We have customers across 5 systems, help me deduplicate"
- "Create a golden record / unified customer profile"
- "Stitch identities across CRM and web data"
- "Build a first-party identity graph in Snowflake"

## Prerequisites

- 2+ source tables containing customer/identity data in Snowflake
- A usable Snowflake warehouse for DT refresh and ad-hoc queries
- Write permissions on the target database/schema where the pipeline will be created
- Source tables should have at least one shared identifier (email, phone, loyalty ID, etc.)

## Key Design Decisions

1. **Incremental by default**: All layers except graph resolution use `REFRESH_MODE = INCREMENTAL` Dynamic Tables. Graph resolution uses a Stream + Task because `WITH RECURSIVE` is not supported in incremental DTs.
2. **Signal strength tiers**: Email, phone, and loyalty_id are "strong" deterministic signals. Device ID and cookie ID are "weak" and require corroboration from another field.
3. **Configurable scoring**: Probabilistic weights are stored in a `MATCH_WEIGHTS_CONFIG` table — tunable without SQL changes.
4. **Toxic identifier blacklist**: Known-bad values (test emails, placeholder phones) are excluded before edge generation.
5. **Field-level survivorship**: The golden record picks the best value per field (not best record), using source priority + recency + staleness penalty.
6. **Cluster guardrails**: Oversized clusters are flagged and excluded from the golden record to prevent false-merge contamination.

---

## Phase 0: Customer Context

Before building anything, understand the customer and their use case:

```
ask_user_question:
  header: "Customer"
  question: "Who is this identity resolution pipeline for? (Company name or description)"
  type: text
  defaultValue: "Acme Retail"
```

```
ask_user_question:
  header: "Use case"
  question: "What's the primary business goal? (e.g., unified customer view for personalization, deduplication for campaign suppression, loyalty program consolidation)"
  type: text
  defaultValue: "Unified customer view for personalization and analytics"
```

Record the customer context. When generating artifacts:
- Use a **look-alike company name** in schema/database naming if customer-specific (never use the real customer name in Snowflake objects)
- Adapt pipeline naming to match their domain language if relevant
- Store context for use in delivery summary

---

## Phase 1: Preflight

Before any work, read these references:
- `references/column-inference-patterns.md` — how to map arbitrary column names to canonical identity fields
- `references/dynamic-tables-guide.md` — incremental DT constraints for IDR (WITH RECURSIVE not allowed, etc.)

Confirm the user has a target Snowflake connection configured.

---

## Phase 1: Discover Source Tables

### Step 1.1: Ask for sources

```
ask_user_question:
  header: "Sources"
  question: "Which tables contain your customer/identity data? Provide fully-qualified names, or I can scan for candidate tables."
  type: text
  defaultValue: "DATABASE.SCHEMA.TABLE1, DATABASE.SCHEMA.TABLE2"
```

Alternative: If user says "scan", search for identity-bearing tables:

```sql
SELECT DISTINCT table_catalog, table_schema, table_name
FROM INFORMATION_SCHEMA.COLUMNS
WHERE LOWER(column_name) IN (
    'email','email_address','phone','phone_number','mobile',
    'first_name','last_name','loyalty_id','customer_id',
    'device_id','maid','cookie_id'
)
AND table_schema NOT IN ('INFORMATION_SCHEMA')
ORDER BY table_catalog, table_schema, table_name;
```

Present candidates and ask user to confirm which ones to include.

### Step 1.2: Profile each source

For each confirmed source table, run:

```sql
SELECT
    COUNT(*) AS row_count,
    MIN(<timestamp_col>) AS earliest,
    MAX(<timestamp_col>) AS latest
FROM <source_table>;
```

Also get column list with NULL rates:

```sql
SELECT column_name, data_type,
       COUNT_IF(<col> IS NOT NULL) / COUNT(*) AS fill_rate
FROM <source_table>;
```

Report profile to user. Do NOT proceed until user confirms Phase 1 findings.

---

## Phase 2: Schema Mapping

For each source table, use `references/column-inference-patterns.md` to map columns to canonical fields:

**Canonical fields:**
- `email_clean` (from email/email_address/emailaddr/contact_email/etc.)
- `phone_clean` (from phone/phone_number/mobile/cell/telephone/etc.)
- `loyalty_id` (from loyalty_id/membership_id/loyalty_number/rewards_id/etc.)
- `device_id` (from device_id/maid/idfa/gaid/advertising_id/etc.)
- `cookie_id` (from cookie_id/anonymous_id/visitor_id/session_id/etc.)
- `first_name` (from first_name/fname/given_name/etc.)
- `last_name` (from last_name/lname/surname/family_name/etc.)
- `address_line_1` (from address/address_line_1/street/shipping_address/etc.)
- `city` (from city/town/municipality/etc.)
- `state` (from state/province/region/state_code/etc.)
- `zip5` (from zip/postal_code/zipcode/zip_code — extract first 5 digits)
- `date_of_birth` (from dob/date_of_birth/birth_date/birthday/etc.)
- `gender` (from gender/sex/etc.)
- `source_updated_at` (from updated_at/modified_at/last_updated/created_at/etc.)

Present the mapping to the user for confirmation:

```
ask_user_question:
  header: "Confirm mapping"
  question: "Here's how I mapped your columns. Correct any mistakes:"
  type: text
  defaultValue: "<pre-filled mapping table>"
```

Handle special cases:
- Composite name fields (e.g., `full_name` -> split on space into first/last)
- Cardholder name format (e.g., `LAST/FIRST`)
- Columns with >95% NULL -> skip them
- Multiple email columns -> ask which one is primary

---

## Phase 3: Configuration

Ask the user in one compact `ask_user_question` block:

```
ask_user_question (4 questions):
  1. header: "Target"
     question: "Where should the IDR pipeline be created?"
     type: text
     defaultValue: "<source_db>.IDR"

  2. header: "Warehouse"
     question: "Which warehouse should run the DT refreshes?"
     type: text
     defaultValue: "<user_current_warehouse>"

  3. header: "Priority"
     question: "Rank your sources from most trusted to least (comma-separated):"
     type: text
     defaultValue: "<source1>, <source2>, <source3>"

  4. header: "Depth"
     question: "How deep should the matching go?"
     options:
       - "Deterministic only" — exact match on shared identifiers (fastest, highest precision)
       - "Deterministic + Probabilistic" — adds fuzzy matching for unresolved records (recommended)
       - "Full with review queue" — adds a medium-confidence review queue for human inspection

  5. header: "Prob Method"
     question: "Which probabilistic matching method? (only applies if Depth includes Probabilistic)"
     options:
       - "Jaro-Winkler" — traditional weighted scoring with blocking passes (fast, explainable, no extra cost, DT incremental-compatible)
       - "Cortex Search" — semantic matching via Arctic embeddings (handles nicknames/typos/abbreviations automatically, requires Cortex Search Service, DT full-refresh only)
       - "Both (compare)" — run both methods side-by-side, merge results with deduplication (useful for initial evaluation)
```

**Probabilistic method guidance:**
- Recommend **Jaro-Winkler** when: per-field score explainability is needed, cost sensitivity is high, data is relatively clean, or incremental DT refresh is required
- Recommend **Cortex Search** when: data has many nickname variations (Jenny/Jennifer), format inconsistencies, abbreviations, or when zero-maintenance matching is preferred
- Recommend **Both** only for initial evaluation — compare results, then prune to one method for production

Record all selections for pipeline generation.

---

## Phase 4: Build Standardization Layer

### Step 4.1: Create schema and config tables

```sql
CREATE SCHEMA IF NOT EXISTS <target_schema>;
```

Create `SOURCE_REGISTRY`, `MATCH_WEIGHTS_CONFIG`, `TOXIC_IDENTIFIERS` tables per templates in `references/deterministic-matching.md`.

Populate `SOURCE_REGISTRY` from Phase 3 priority ranking.
Populate `TOXIC_IDENTIFIERS` with standard blacklist entries.
Populate `MATCH_WEIGHTS_CONFIG` with defaults (or user overrides).

### Step 4.2: Generate standardization DTs

For EACH source table, generate a `STD_SOURCE_<name>` DT using this template pattern:

```sql
CREATE OR REPLACE DYNAMIC TABLE <target_schema>.STD_SOURCE_<SOURCE_NAME>
    TARGET_LAG = DOWNSTREAM
    WAREHOUSE = <warehouse>
    REFRESH_MODE = INCREMENTAL
AS
SELECT
    '<SOURCE_NAME>' AS source_system,
    <pk_column> AS source_id,
    <email_mapping_with_toxic_exclusion> AS email_clean,
    <phone_mapping_with_toxic_exclusion> AS phone_clean,
    <loyalty_mapping> AS loyalty_id,
    <device_mapping> AS device_id,
    <cookie_mapping> AS cookie_id,
    <first_name_mapping> AS first_name,
    <last_name_mapping> AS last_name,
    <address_mapping> AS address_line_1,
    <city_mapping> AS city,
    <state_mapping> AS state,
    <zip_mapping> AS zip5,
    <dob_mapping> AS date_of_birth,
    <gender_mapping> AS gender,
    <timestamp_mapping> AS source_updated_at,
    -- Include ONLY when Prob Method is "Cortex Search" or "Both":
    COALESCE(<first_name_mapping>, '') || ' ' || COALESCE(<last_name_mapping>, '') || ' | ' ||
    COALESCE(<email_mapping>, '') || ' | ' ||
    COALESCE(<phone_mapping>, '') || ' | ' ||
    COALESCE(<address_mapping>, '') || ' ' || COALESCE(<city_mapping>, '') || ' ' ||
    COALESCE(<state_mapping>, '') || ' ' || COALESCE(<zip_mapping>, '')
    AS search_text
FROM <source_table>
LEFT JOIN <target_schema>.TOXIC_IDENTIFIERS t_email ...
LEFT JOIN <target_schema>.TOXIC_IDENTIFIERS t_phone ...;
```

**Note on search_text**: Only include the `search_text` column when the user selected "Cortex Search" or "Both" as the probabilistic method. When using Jaro-Winkler only, omit this column to keep DTs lean.

Fill NULL for any fields the source doesn't have. Apply cleansing:
- Email: `LOWER(TRIM(...))`
- Phone: `REGEXP_REPLACE(..., '[^0-9]', '')`
- Names: `UPPER(TRIM(...))`
- ZIP: `LEFT(REGEXP_REPLACE(..., '[^0-9]', ''), 5)`

### Step 4.3: Generate identity signals union DT

```sql
CREATE OR REPLACE DYNAMIC TABLE <target_schema>.IDENTITY_SIGNALS
    TARGET_LAG = DOWNSTREAM
    WAREHOUSE = <warehouse>
    REFRESH_MODE = INCREMENTAL
AS
SELECT * FROM <target_schema>.STD_SOURCE_<SOURCE1>
UNION ALL
SELECT * FROM <target_schema>.STD_SOURCE_<SOURCE2>
...
UNION ALL
SELECT * FROM <target_schema>.STD_SOURCE_<SOURCEN>;
```

When using Cortex Search: ensure the `search_text` column is included in all STD_SOURCE DTs and propagated through IDENTITY_SIGNALS. This is the column that will be indexed by the Cortex Search Service.

---

## Phase 5: Build Edge Generation

### Step 5.1: Deterministic edges

Read `references/deterministic-matching.md` for the full template. Generate the DT based on which identity fields are ACTUALLY PRESENT across sources (don't generate email edge logic if no source has email).

Key guardrails to always include:
- Email match: require name similarity > 60 OR name is NULL (household email protection)
- Phone match: require LENGTH >= 10 (avoid partial numbers)
- Device/Cookie: require corroboration from another field (weak signal tier)

### Step 5.2: Probabilistic edges (if selected in Phase 3)

Branch based on the **Prob Method** selected in Phase 3:

#### If method == "Jaro-Winkler" (or "Both"):

Read `references/probabilistic-scoring.md`. Select blocking strategies based on available fields:
- ZIP + last name prefix (if address data exists)
- DOB + SOUNDEX first name (if DOB exists)
- Phone last 7 + first initial (if phone exists)
- Email username + city (if email + city exist)

Generate:
1. Blocking pass DT(s)
2. Unified scoring DT (`PROBABILISTIC_EDGES`) using `MATCH_WEIGHTS_CONFIG`

#### If method == "Cortex Search" (or "Both"):

Read `references/cortex-search-matching.md`. Use **Pattern A** (full replacement) by default:

Generate:
1. Cortex Search Service (`IDENTITY_SEARCH_SERVICE`) on `IDENTITY_SIGNALS.search_text`
2. Batch resolution DT (`PROBABILISTIC_EDGES_CSS`) using `CORTEX_SEARCH_BATCH` LATERAL join
3. Optional: cascading filter strategy for higher precision at scale

**Important**: `PROBABILISTIC_EDGES_CSS` must use `REFRESH_MODE = FULL` because `CORTEX_SEARCH_BATCH` is non-deterministic.

#### If method == "Both":

Generate both `PROBABILISTIC_EDGES` (Jaro-Winkler) and `PROBABILISTIC_EDGES_CSS` (Cortex Search). The ALL_EDGES union will merge from both, deduplicating pairs that appear in both (keeping the higher confidence score). Note: ALL_EDGES must use `REFRESH_MODE = FULL` when any upstream DT is FULL refresh.

### Step 5.3: All edges union

```sql
-- When using Jaro-Winkler only:
CREATE OR REPLACE DYNAMIC TABLE <target_schema>.ALL_EDGES ...
AS
SELECT ... FROM DETERMINISTIC_EDGES
UNION ALL
SELECT ... FROM PROBABILISTIC_EDGES WHERE match_status = 'ACCEPTED';

-- When using Cortex Search only:
CREATE OR REPLACE DYNAMIC TABLE <target_schema>.ALL_EDGES
    REFRESH_MODE = FULL ...
AS
SELECT ... FROM DETERMINISTIC_EDGES
UNION ALL
SELECT ... FROM PROBABILISTIC_EDGES_CSS WHERE match_status = 'ACCEPTED';

-- When using Both:
CREATE OR REPLACE DYNAMIC TABLE <target_schema>.ALL_EDGES
    REFRESH_MODE = FULL ...
AS
WITH combined AS (
    SELECT id_a, id_b, match_type, match_subtype, confidence
    FROM DETERMINISTIC_EDGES
    UNION ALL
    SELECT id_a, id_b, match_type, match_subtype, confidence
    FROM PROBABILISTIC_EDGES WHERE match_status = 'ACCEPTED'
    UNION ALL
    SELECT id_a, id_b, match_type, match_subtype, confidence
    FROM PROBABILISTIC_EDGES_CSS WHERE match_status = 'ACCEPTED'
)
SELECT * FROM combined
QUALIFY ROW_NUMBER() OVER (PARTITION BY id_a, id_b ORDER BY confidence DESC) = 1;
```

---

## Phase 7: Build Graph Resolution

Read `references/graph-resolution.md`. The resolution is **staged**:
1. Stage 1: Resolve deterministic edges only → identify singletons (cluster_size = 1)
2. Stage 2: Only apply probabilistic edges where at least one side is a singleton

This prevents probabilistic matches from contaminating high-confidence deterministic clusters and dramatically reduces the probabilistic candidate space.

Create:
1. `IDENTITY_CLUSTERS` table
2. `RESOLVE_IDENTITY_GRAPH_STAGED` stored procedure
3. Stream on `ALL_EDGES`
4. Task triggered by stream

Execute the SP once to populate initial clusters.

---

## Phase 7: Build Output Layer

### Step 7.1: Keyring DT

Maps every source record to its `master_customer_id`.

### Step 7.2: Golden Record DT

Read `references/golden-record-survivorship.md`. Use `FIRST_VALUE IGNORE NULLS` per field with ordering by source priority + recency + staleness penalty.

### Step 7.3: Field Conflicts DT

Surface cases where top-priority sources disagree on key fields.

### Step 7.4: Observability views (if selected)

Read `references/observability.md`. Create:
- `PIPELINE_METRICS` view
- `EDGE_DISTRIBUTION` view
- `REVIEW_QUEUE` view (if probabilistic + review queue selected)

---

## Phase 8: Verify

For each created object:
```sql
SELECT name, refresh_mode, scheduling_state, rows
FROM TABLE(INFORMATION_SCHEMA.DYNAMIC_TABLES())
WHERE schema_name = '<target_schema>';
```

Report:
- Total source records vs. unique identities (dedup rate)
- Average and max cluster size
- Edge distribution by match type
- Any oversized clusters flagged

---

## Phase 9: Deliver

Present to user:
1. Summary table of all created objects (type, rows, refresh mode)
2. Key queries:
   - `SELECT * FROM <schema>.GOLDEN_RECORD LIMIT 10;`
   - `SELECT * FROM <schema>.KEYRING WHERE source_system = '<name>' LIMIT 10;`
   - `SELECT * FROM <schema>.PIPELINE_METRICS;`
3. How to onboard new sources (register + create STD DT + add to UNION ALL)
4. How to tune weights (UPDATE MATCH_WEIGHTS_CONFIG)
5. How to re-resolve graph manually (CALL RESOLVE_IDENTITY_GRAPH())

---

## Stopping Points

- After Phase 0: Customer context confirmed
- After Phase 2: Source discovery and profiling complete — "Just profile my data, don't build anything"
- After Phase 4: Configuration confirmed — "Show me the plan but don't execute"
- After Phase 6: Deterministic edges built — "I only want deterministic matching, skip probabilistic"
- After Phase 9: Pipeline verified — "Done — don't need the delivery summary"

Always respect user intent. If they say "just build it", skip confirmations and execute all phases.

---

## Troubleshooting

| Issue | Cause | Fix |
|-------|-------|-----|
| DT shows FULL refresh instead of INCREMENTAL | Query has unsupported construct | Check `refresh_mode_reason` via SHOW DYNAMIC TABLES; likely a non-deterministic function or WITH RECURSIVE |
| Graph resolution doesn't converge | Circular edges or poisoned identifier | Check max cluster sizes; add toxic IDs to blacklist |
| Low precision (too many false merges) | Weak signals creating bad edges | Raise auto-accept threshold; remove COOKIE_ID from deterministic; add name similarity guard |
| Low recall (missing matches) | Blocking keys too restrictive | Add more blocking passes; lower JAROWINKLER threshold in blocking |
| Oversized clusters | Shared identifier (call center phone, family email) | Add to TOXIC_IDENTIFIERS; strengthen corroboration requirements |
| Probabilistic DT very large | Too many candidate pairs from blocking | Tighten blocking keys (require 3-char prefix instead of 2); add more conditions to WHERE clause |

---

## Output

When complete, the pipeline produces:
- **GOLDEN_RECORD** — one row per unique identity with best-available value per field
- **KEYRING** — maps every source_system + source_id to its master_customer_id
- **FIELD_CONFLICTS** — data quality issues where trusted sources disagree
- **PIPELINE_METRICS** — KPIs (dedup rate, cluster sizes, edge counts)
- **REVIEW_QUEUE** — medium-confidence matches for human review (optional)

All output refreshes incrementally as source data changes.
