---
name: cloudflare-r2-logpush-agent
description: "Build an end-to-end Cloudflare HTTP log analytics pipeline in Snowflake with a Cortex Agent. Ingests Logpush data from R2 into a parsed view, semantic view, and queryable agent. Use when: cloudflare logs, R2 logpush, http log analytics, web traffic agent, cloudflare to snowflake. Triggers: cloudflare, logpush, R2, http logs, web analytics agent."
---

# Cloudflare R2 Logpush to Cortex Agent

Build a complete pipeline: **Cloudflare Logpush -> R2 -> Snowflake -> Semantic View -> Cortex Agent**.

The result is a natural-language queryable agent over your Cloudflare HTTP request logs covering traffic, errors, geography, bots, cache, security/WAF, origin performance, and more.

## When NOT to Use

- DNS, firewall, or non-HTTP Cloudflare datasets (this skill covers HTTP requests only)
- Real-time sub-minute streaming analytics (Logpush is batch-oriented)
- Request payload/header inspection (Cloudflare logs scalar metadata, not bodies)

## Prerequisites

- Active Snowflake connection with ACCOUNTADMIN (or a role with CREATE DATABASE, CREATE AGENT)
- Cloudflare Logpush configured to push HTTP request logs to an R2 bucket
- R2 API credentials (access key + secret key) for that bucket
- A warehouse for queries

## Stopping Points

- ✋ Step 1: Confirm configuration before creating any objects
- ✋ Step 3: Confirm data loaded and sample rows look correct
- ✋ Step 5: Confirm semantic view has expected facts/dimensions/metrics
- ✋ Step 6: Confirm agent returns a meaningful response with data
- ✋ Step 7: Confirm grants work and scheduled task (if created) is RESUMED

---

## Workflow

### Step 1: Gather Configuration

**Goal:** Collect all parameters needed for the pipeline.

**Ask the user for:**

1. **R2 credentials:**
   - R2 endpoint (e.g., `abc123.r2.cloudflarestorage.com`) — no `https://` prefix
   - R2 bucket name (e.g., `cloudflare-managed-xxxxx`)
   - R2 access key ID
   - R2 secret access key
   - Path prefix in the bucket (e.g., `20250101/` or empty for root)

2. **Snowflake naming:**
   - Database name (e.g., `CF_LOGS_DB`)
   - Schema name (default: `HTTP_LOGS`)
   - Agent name (e.g., `MY_SITE_AGENT`)
   - Warehouse (e.g., `COMPUTE_WH`)
   - Role for agent access grants (e.g., `PUBLIC`)

3. **Site info:**
   - Zone name / domain (e.g., `example.com`) — used in agent instructions

**Derive object names** from user input:
- Raw table: `<AGENT_NAME>_HTTP_RAW`
- Parsed view: `<AGENT_NAME>_HTTP_REQUESTS`
- Semantic view: `<AGENT_NAME>_HTTP_ANALYTICS_SEMANTIC`
- Stage: `<AGENT_NAME>_HTTP_STAGE`
- File format: `<AGENT_NAME>_HTTP_JSON_FF`

**⚠️ STOP:** Confirm all configuration values with the user before proceeding.

---

### Step 2: Create Infrastructure

**Goal:** Create database, schema, file format, external stage, and raw table.

**Load** `references/sql-templates.md` for all SQL templates.

**Actions:**

1. Run templates 1-4 (database, schema, file format, stage, raw table) with substituted values.

2. Verify the stage connects to R2:
   ```sql
   LIST @{{DATABASE}}.{{SCHEMA}}.{{STAGE_NAME}};
   ```
   This should return `.gz` files. If it fails with access errors, re-check R2 credentials and endpoint.

**Troubleshooting:**
- "Access Denied" → Wrong R2 access key, secret key, or bucket name
- "Could not connect" → Wrong endpoint (should be bare hostname, no `https://`)
- No files listed → Wrong path prefix, or Logpush hasn't pushed any files yet

---

### Step 3: Load Data

**Goal:** Copy NDJSON gzip files from R2 into the raw VARIANT table.

**Actions:**

1. Run template 5 (COPY INTO). This is idempotent — Snowflake tracks loaded files for 64 days, so re-running only loads new files.

2. Verify row count:
   ```sql
   SELECT COUNT(*) AS row_count FROM {{DATABASE}}.{{SCHEMA}}.{{RAW_TABLE}};
   ```

3. Spot-check one row:
   ```sql
   SELECT RAW FROM {{DATABASE}}.{{SCHEMA}}.{{RAW_TABLE}} LIMIT 1;
   ```
   Confirm it contains Cloudflare HTTP request fields (ClientRequestHost, EdgeResponseStatus, etc.).

**⚠️ STOP:** Confirm row count looks reasonable and sample data is correct.

---

### Step 4: Create Parsed View

**Goal:** Create a view that extracts all ~75 scalar Cloudflare HTTP request fields from the raw VARIANT into typed columns, plus computed flags.

**Actions:**

1. Run template 6 (parsed view). This uses a CTE pattern to avoid SQL column alias referencing issues.

2. Quick verify:
   ```sql
   SELECT COUNT(*) FROM {{DATABASE}}.{{SCHEMA}}.{{VIEW_NAME}};
   SELECT * FROM {{DATABASE}}.{{SCHEMA}}.{{VIEW_NAME}} LIMIT 5;
   ```

**Fields covered** (all scalar fields from Cloudflare's HTTP requests dataset):
- **Timestamps:** EDGE_START_TS, EDGE_END_TS, EDGE_HOUR
- **Request:** HOST, URI, PATH, METHOD, PROTOCOL, SCHEME, REFERER, REQUEST_BYTES
- **Client:** CLIENT_IP, COUNTRY, CITY, REGION_CODE, DEVICE_TYPE, USER_AGENT, CLIENT_ASN
- **Edge response:** STATUS, EDGE_RESPONSE_BYTES, TTFB_MS, CONTENT_TYPE, EDGE_COLO_CODE
- **Cache:** CACHE_STATUS, CACHE_RESPONSE_BYTES, CACHE_RESERVE_USED
- **Origin:** ORIGIN_IP, ORIGIN_STATUS, ORIGIN_RESPONSE_TIME_MS, ORIGIN_DNS_MS, ORIGIN_TCP/TLS_HANDSHAKE_MS
- **Bot detection:** BOT_SCORE, BOT_SCORE_SRC, VERIFIED_BOT_CATEGORY, JA3_HASH, JA4
- **Security/WAF:** SECURITY_ACTION, WAF_ATTACK_SCORE, WAF_SQLI_SCORE, WAF_XSS_SCORE, WAF_RCE_SCORE
- **Workers:** WORKER_SCRIPT_NAME, WORKER_CPU_TIME_US, WORKER_WALL_TIME_US
- **Computed flags:** IS_402, IS_4XX, IS_5XX, IS_BOT (score < 30), IS_CACHE_HIT, REQUEST_COUNT (always 1)

---

### Step 5: Create Semantic View

**Goal:** Create a semantic view with facts, dimensions, and pre-built metrics for the Cortex Agent.

**Actions:**

1. Run template 7 (semantic view). Contains:
   - **28 facts** — numeric columns for aggregation (bytes, latencies, scores, flags)
   - **42 dimensions** — categorical columns for filtering/grouping
   - **16 metrics** — pre-built aggregates (total_requests, rate_402, rate_4xx, rate_5xx, bot_rate, cache_hit_rate, avg_ttfb_ms, etc.)
   - **AI_SQL_GENERATION** hint — guides the agent on field semantics (score ranges, status codes, etc.)

2. Verify:
   ```sql
   DESCRIBE SEMANTIC VIEW {{DATABASE}}.{{SCHEMA}}.{{SEMANTIC_VIEW}};
   ```

**⚠️ STOP:** Confirm semantic view has the expected facts/dimensions/metrics.

---

### Step 6: Create Cortex Agent

**Goal:** Create a Cortex Agent backed by the semantic view.

**Actions:**

1. **Create the agent** using template 8–9 (combined). Run the `CREATE AGENT ... FROM SPECIFICATION $$...$$` statement with all placeholders replaced. Use `USE ROLE ACCOUNTADMIN` (or a role with CREATE AGENT privilege).

2. **Set execution_environment via REST API** using template 10.

   > **KNOWN ISSUE:** `CREATE AGENT FROM SPECIFICATION` silently drops `execution_environment` from `tool_resources`. Without it, the agent returns error 399504: "missing execution environment." The REST API PUT is the reliable workaround.

   The execution_environment must use this nested format:
   ```json
   {"type": "warehouse", "warehouse": "COMPUTE_WH"}
   ```

3. **Test the agent:**
   ```sql
   SELECT SNOWFLAKE.CORTEX.AGENT('{{DATABASE}}.{{SCHEMA}}.{{AGENT_NAME}}',
     PARSE_JSON('{"messages": [{"role": "user", "content": [{"type": "text", "text": "How many total requests are in the logs?"}]}]}')
   );
   ```
   If this returns error 399504, the execution_environment was not set — re-run template 10.

**⚠️ STOP:** Confirm agent returns a meaningful response with data.

---

### Step 7: Grant Access and Optional Scheduled Refresh

**Goal:** Make the agent accessible and optionally set up recurring data loads.

**Actions:**

1. **Grant access** using template 11. Run all GRANT statements for the target role.

2. **(Optional) Create a weekly refresh task** using template 12. This runs `COPY INTO` on a schedule (default: Sundays 6 AM UTC) to keep data current. Because `COPY INTO` tracks loaded files for 64 days, it only appends new files — no duplicates.

3. **Final verification** — ask the agent a question that exercises multiple field categories:
   - "What percentage of traffic is from bots?"
   - "What is the cache hit rate by country?"
   - "Show me the top 10 paths by request count"

**⚠️ STOP:** Confirm grants work and scheduled task (if created) is RESUMED.

---

## Output

After completing all steps, the user has:

- **Raw table** — VARIANT storage of all Cloudflare HTTP request JSON
- **Parsed view** — ~75 typed columns covering every scalar field plus computed flags
- **Semantic view** — 28 facts, 42 dimensions, 16 metrics with AI hints
- **Cortex Agent** — natural-language queryable over the semantic view
- **Grants** — agent accessible to the specified role
- **(Optional) Scheduled task** — weekly data refresh from R2
- **Cleanup template** — template 13 for tearing down all objects if needed

## Gotchas

- **Stage URL protocol:** Uses `s3compat://` (not `s3://`) for Cloudflare R2
- **execution_environment bug:** Must be set via REST API after `CREATE AGENT` — SQL silently drops it (see template 10)
- **BOT_SCORE is inverted:** 0-99 where *lower* = *more likely* bot (< 30 = bot)
- **WAF scores are inverted:** 1-99 where *lower* = *higher* attack likelihood
