---
name: meta-capi-pipeline
description: "Build and manage Meta Conversions API (CAPI) pipelines in Snowflake. Triggers: set up Meta CAPI, create CAPI pipeline, discover tables, find CAPI candidates, get signal recommendations, run pipeline, send events to Meta, check CAPI status, pipeline health, debug CAPI, CAPI errors, resend failed events."
---

# Meta CAPI Pipeline

Build and manage Meta Conversions API pipelines in Snowflake with human-in-the-loop approval workflows.

## CRITICAL RULES — DO NOT SKIP

> ⛔ THESE RULES ARE NON-NEGOTIABLE. VIOLATING ANY RULE IS A PIPELINE FAILURE.

1. **PII hashing is MANDATORY.** All PII fields (email, phone, name, city, state, zip) MUST be SHA256-hashed. The pipeline generator handles this automatically — never send unhashed PII.
2. **DISCOVERY GATE: You MUST ask the user which column to use as `event_id`** for deduplication BEFORE presenting any approval prompt. Do NOT auto-generate without asking. Do NOT skip this question.
3. **DISCOVERY GATE: You MUST ask the user about custom fields** — ask if they want to include any additional custom fields in `custom_data` BEFORE presenting any approval prompt. No need to list all unmapped columns; a simple question is sufficient.
4. **Never deploy a pipeline without explicit user approval** on the final config.
5. **The Discovery flow has THREE mandatory stops** (table selection → event_id + custom fields → approval). You CANNOT compress these into fewer stops.

## Prerequisites

- ACCOUNTADMIN or CREATE INTEGRATION privilege
- Meta Pixel ID (from Events Manager)
- Meta Access Token (with `ads_management` permission)
- PII must be pre-hashed (SHA256, lowercase, trimmed) — handled automatically by mapping views

## Schema Design

`META_CAPI_EVENTS` uses a flexible VARIANT schema:
- **Fixed columns**: EVENT_ID, EVENT_NAME, EVENT_TIME, ACTION_SOURCE, EVENT_SOURCE_URL
- **`USER_DATA` (VARIANT)**: em, ph, fn, ln, ct, st, zp, country, external_id, client_ip_address, fbc, fbp, + any future fields
- **`CUSTOM_DATA` (VARIANT)**: value, currency, content_ids, order_id, search_string, predicted_ltv, + any custom/future fields

This means new Meta fields or custom parameters never require table DDL changes — just update the mapping view.

## Intent Detection

| Intent | Triggers | Action |
|--------|----------|--------|
| **Setup** | "set up Meta CAPI", "create CAPI pipeline", "install CAPI" | → Step 1: Setup |
| **Discover** | "discover tables", "find CAPI candidates", "scan for Meta data" | → Step 2: Discovery |
| **Recommend** | "get signal recommendations", "what events should I send" | → Step 3: Recommendations |
| **Run** | "run pipeline", "send events to Meta", "process pending" | → Step 4: Processing |
| **Monitor** | "check CAPI status", "pipeline health", "view Meta events" | → Step 5: Monitoring |
| **Troubleshoot** | "debug CAPI", "CAPI errors", "resend failed events" | → Step 6: Troubleshooting |

## Workflow

### Step 1: Setup (01_setup)

**Goal:** Create infrastructure for Meta CAPI integration.

**Actions:**

1. **Ask** user for credentials:
   ```
   Please provide:
   - Pixel ID: [from Meta Events Manager]
   - Access Token: [with ads_management permission]
   - Warehouse: [for task execution]
   ```

2. **Execute** schema creation:
   ```sql
   CREATE DATABASE IF NOT EXISTS META_CAPI_DB;
   CREATE SCHEMA IF NOT EXISTS META_CAPI_DB.PIPELINE;
   ```

3. **Execute** table creation (META_CAPI_EVENTS, META_CAPI_LOG, META_CAPI_CONFIG)

4. **Execute** network access setup:
   ```sql
   CREATE SECRET meta_capi_access_token TYPE = GENERIC_STRING SECRET_STRING = '<TOKEN>';
   CREATE NETWORK RULE meta_capi_network_rule MODE = EGRESS TYPE = HOST_PORT VALUE_LIST = ('graph.facebook.com:443', 'api.facebook.com:443');
   CREATE EXTERNAL ACCESS INTEGRATION meta_capi_integration ALLOWED_NETWORK_RULES = (meta_capi_network_rule) ALLOWED_AUTHENTICATION_SECRETS = (meta_capi_access_token) ENABLED = TRUE;
   ```

5. **Execute** UDTF and procedure creation

6. **Execute** task creation for scheduled processing

**Output:** Fully configured META_CAPI_DB.PIPELINE schema with all objects.

---

### Step 2: Discovery (00_discovery)

**Goal:** Find candidate tables and generate pipeline configs for human review.

> ⛔ THIS STEP HAS THREE MANDATORY STOPS. YOU MUST COMPLETE ALL THREE IN ORDER.
> STOP A → Table selection
> STOP B → Event ID + Custom fields (DO NOT offer approval here)
> STOP C → Final approval

---

#### Step 2A: Scan & Select Tables

1. **Ask** user for scope:
   ```
   Where should I look for event data?
   - Database: [optional, default: all accessible]
   - Schema: [optional, default: all schemas]
   ```

2. **Execute** table discovery:
   ```sql
   CALL discover_capi_candidate_tables('<DATABASE>', '<SCHEMA>');
   ```

3. **Present** candidates with match scores (EXCELLENT/GOOD/MODERATE)

4. **⚠️ STOP A**: Ask user to select table(s) for pipeline creation. **Wait for response.**

---

#### Step 2B: Event ID & Custom Fields (MANDATORY — DO NOT SKIP)

> ⛔ YOU MUST COMPLETE THIS STEP. DO NOT JUMP TO APPROVAL.
> If you find yourself about to show "Do you approve?" without having asked about event_id and custom fields, STOP and come back here.

5. **Execute** config generation:
   ```sql
   CALL generate_pipeline_config('<DB>', '<SCHEMA>', '<TABLE>', '<EVENT_TYPE>', '<ACTION_SOURCE>');
   ```

6. **Present** the auto-mapped config (show which columns were mapped to which Meta fields).

7. **In the SAME message**, ask BOTH of these questions:

   ```
   Before I finalize this config, I need two inputs from you:

   1. EVENT ID (Deduplication): Which column should be used as event_id?
      Meta uses this to deduplicate events across sources (pixel, server, app).
      Candidate columns from your table: [list ID-like columns: order_id, transaction_id, lead_id, etc.]
      Or choose "auto" to auto-generate unique IDs (no deduplication).

   2. CUSTOM FIELDS: Would you like to include any additional columns as
      custom_data fields? They'll be available for custom audiences and
      optimization signals in Meta Ads Manager.
      If yes, list the column names. Otherwise, say "none".
   ```

8. **⚠️ STOP B**: **STOP HERE. DO NOT show approval options. DO NOT offer to deploy. Wait for user response.**

---

#### Step 2C: Final Config & Approval

> You may ONLY reach this step AFTER receiving answers to Step 2B.

9. **Incorporate** user answers into the config:
   - Set `column_mapping.event_id` to the user's chosen column (or leave as auto-generate)
   - Add user-selected columns to `column_mapping.custom_fields`

10. **Present** the FINAL config summary:
    ```
    Final pipeline config:
    - Event type: [X]
    - Event ID column: [X or auto-generated]
    - PII fields (auto-hashed): [list]
    - Standard fields: [value, currency, order_id, etc.]
    - Custom fields: [user-selected or none]
    - Action source: [website/app/etc.]

    Do you approve this config for deployment? (Yes/No/Edit)
    ```

11. **⚠️ STOP C**: Wait for approval.

12. **If approved**, execute deployment:
    ```sql
    CALL save_pipeline_config(<config>, '<name>');
    CALL approve_pipeline_config('<config_id>', '<approval_notes>');
    CALL deploy_pipeline_from_config('<config_id>');
    ```

**Output:** Deployed CDC pipeline (stream + view + task) loading to META_CAPI_EVENTS.

---

#### Discovery Completion Checklist

Before deploying ANY pipeline, verify:
- [ ] User selected source table(s) — **STOP A complete**
- [ ] User specified event_id column (or chose "auto") — **STOP B complete**
- [ ] User was asked about custom fields and confirmed (or "none") — **STOP B complete**
- [ ] User explicitly approved final config — **STOP C complete**

If ANY box is unchecked, DO NOT DEPLOY.

---

### Step 3: Recommendations (05_recommendations)

**Goal:** Fetch Meta AI recommendations and implement with human approval.

**Actions:**

1. **Retrieve** Pixel ID from config (already provided during Setup):
   ```sql
   SELECT CONFIG_VALUE FROM META_CAPI_DB.PIPELINE.META_CAPI_CONFIG WHERE CONFIG_KEY = 'pixel_id';
   ```

2. **Execute** recommendation fetch using Pixel ID as data_source_id:
   ```sql
   CALL get_use_case_recommendations(data_source_id => '<PIXEL_ID>');
   ```

3. **Present** recommendations (Online Purchase, Lead Gen, pLTV, etc.)

4. **⚠️ MANDATORY STOPPING POINT**: Ask user which recommendation to implement.

5. **Execute** table discovery for selected recommendation:
   ```sql
   CALL discover_tables_for_recommendations('<DATABASE>', '<SCHEMA>');
   ```

6. **Present** matching tables with fit assessment.

7. **⚠️ MANDATORY STOPPING POINT**: Get table selection from user.

8. **Execute** config generation:
   ```sql
   CALL generate_reco_config('<use_case>', '<DB>', '<SCHEMA>', '<TABLE>');
   ```

9. **Present** config with Meta requirements and column mappings.

10. **Ask event_id and custom fields questions** (same as Step 2B above). Wait for response.

11. **⚠️ MANDATORY STOPPING POINT**: Present final config, get explicit approval.
    ```
    Review the config above. Do you approve? (Yes/No/Edit)
    ```

12. **If approved**, execute deployment:
    ```sql
    CALL save_reco_config(<config>, '<name>');
    CALL approve_reco_config('<config_id>', '<approval_notes>');
    CALL deploy_reco_config('<config_id>');
    ```

**Output:** Deployed RECO_* pipeline objects (stream + view + task).

---

### Step 4: Processing (02_processing)

**Goal:** Send pending events to Meta Graph API.

**Actions:**

1. **Execute** manual processing:
   ```sql
   CALL process_pending_capi_events('<PIXEL_ID>');
   ```

2. **Or** check task status:
   ```sql
   SHOW TASKS LIKE 'meta_capi_task' IN SCHEMA META_CAPI_DB.PIPELINE;
   ```

3. **Present** results (events_received count, any failures).

**Output:** Events sent to Meta, STATUS updated to SENT or FAILED.

---

### Step 5: Monitoring (03_monitoring)

**Goal:** Check pipeline health and event delivery status.

**Actions:**

1. **Execute** health check:
   ```sql
   CALL capi_health_check();
   ```

2. **Present** health summary:
   - Pending events count
   - Success rate (24h)
   - Failed events count
   - Task state
   - Last batch time

3. **If issues detected**, suggest troubleshooting steps.

**Output:** Health report with actionable recommendations.

---

### Step 6: Troubleshooting (04_troubleshooting)

**Goal:** Diagnose and fix pipeline issues.

**Actions:**

1. **Execute** validation check:
   ```sql
   SELECT * FROM V_CAPI_VALIDATION WHERE VALIDATION_STATUS != 'VALID';
   ```

2. **Execute** error analysis:
   ```sql
   SELECT EVENT_ID, ERROR_MESSAGE, META_RESPONSE 
   FROM META_CAPI_EVENTS WHERE STATUS = 'FAILED' 
   ORDER BY PROCESSED_AT DESC LIMIT 20;
   ```

3. **Present** findings with error codes:
   | Error | Cause | Solution |
   |-------|-------|----------|
   | 190 | Invalid token | Regenerate access token |
   | 100 | Invalid parameter | Check payload format |
   | 803 | Rate limited | Reduce batch frequency |

4. **⚠️ MANDATORY STOPPING POINT**: Ask user before retry.
   ```
   Found X failed events. Reset them for retry? (Yes/No)
   ```

5. **If approved**, execute retry:
   ```sql
   CALL resend_failed_events(3, 1000);
   ```

**Output:** Failed events reset to PENDING for reprocessing.

---

## Stopping Points Summary

| Step | Stop | Gate | What to ask |
|------|------|------|-------------|
| Discovery | **A** | Table selection | "Which table(s) do you want to set up?" |
| Discovery | **B** | Event ID + Custom fields | "Which column for event_id?" + "Include any unmapped columns?" |
| Discovery | **C** | Final approval | "Do you approve this config?" |
| Recommendations | 1 | Use case selection | "Which recommendation to implement?" |
| Recommendations | 2 | Table selection | "Which table fits this use case?" |
| Recommendations | 3 | Event ID + Custom fields | Same as Discovery Stop B |
| Recommendations | 4 | Final approval | "Do you approve?" |
| Troubleshooting | 1 | Retry confirmation | "Reset X failed events for retry?" |

**Resume rule:** Upon user approval ("yes", "approved", "proceed"), continue to next step without re-asking.

## Objects Created

| Object | Type | Purpose |
|--------|------|---------|
| `META_CAPI_DB.PIPELINE` | Schema | Container for all objects |
| `META_CAPI_EVENTS` | Table | Event staging (flexible VARIANT schema) |
| `META_CAPI_LOG` | Table | Batch processing logs |
| `META_CAPI_CONFIG` | Table | Configuration storage |
| `PIPELINE_CONFIGS` | Table | Discovery pipeline configs |
| `META_CAPI_RECOMMENDATIONS` | Table | Meta API recommendations |
| `META_RECO_CONFIGS` | Table | Recommendation configs |
| `send_to_meta_capi` | UDTF | Calls Meta Graph API (pass-through) |
| `meta_capi_access_token` | Secret | API credentials |
| `meta_capi_network_rule` | Network Rule | Egress (graph + api.facebook.com) |
| `meta_capi_integration` | Integration | External access |

## Output

- Configured Meta CAPI pipeline in Snowflake
- CDC streams loading events from source tables
- Scheduled task processing events hourly
- Health monitoring and alerting
