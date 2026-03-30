# SQL Templates — Cloudflare R2 Logpush to Cortex Agent

All templates use `{{PLACEHOLDER}}` variables. Replace before executing.

## Placeholders

| Variable | Description | Example |
|----------|-------------|---------|
| `{{DATABASE}}` | Snowflake database name | `CF_LOGS_DB` |
| `{{SCHEMA}}` | Snowflake schema name | `HTTP_LOGS` |
| `{{RAW_TABLE}}` | Raw VARIANT table name | `HTTP_RAW` |
| `{{VIEW_NAME}}` | Parsed view name | `HTTP_REQUESTS` |
| `{{SEMANTIC_VIEW}}` | Semantic view name | `HTTP_ANALYTICS_SEMANTIC` |
| `{{AGENT_NAME}}` | Cortex Agent name | `MY_CF_AGENT` |
| `{{WAREHOUSE}}` | Warehouse for queries | `COMPUTE_WH` |
| `{{ROLE}}` | Role for grants | `PUBLIC` |
| `{{STAGE_NAME}}` | External stage name | `CF_HTTP_STAGE` |
| `{{FILE_FORMAT}}` | File format name | `CF_HTTP_JSON_FF` |
| `{{R2_ENDPOINT}}` | R2 endpoint (no https://) | `abc123.r2.cloudflarestorage.com` |
| `{{R2_BUCKET}}` | R2 bucket name | `cloudflare-managed-xxxxx` |
| `{{R2_ACCESS_KEY}}` | R2 access key ID | `e4d1d04d...` |
| `{{R2_SECRET_KEY}}` | R2 secret access key | `a026cd62...` |
| `{{R2_PATH_PREFIX}}` | Path prefix in bucket | `20250101/` |
| `{{ZONE_NAME}}` | Cloudflare zone (domain) | `example.com` |
| `{{CONNECTION_NAME}}` | Snowflake connection name (for REST API script) | `my_connection` |

---

## 1. Database and Schema

```sql
CREATE DATABASE IF NOT EXISTS {{DATABASE}};
CREATE SCHEMA IF NOT EXISTS {{DATABASE}}.{{SCHEMA}};
USE DATABASE {{DATABASE}};
USE SCHEMA {{SCHEMA}};
```

## 2. File Format (NDJSON gzip)

```sql
CREATE OR REPLACE FILE FORMAT {{DATABASE}}.{{SCHEMA}}.{{FILE_FORMAT}}
  TYPE = 'JSON'
  STRIP_OUTER_ARRAY = FALSE
  COMPRESSION = 'GZIP';
```

## 3. External Stage (Cloudflare R2 via s3compat)

```sql
CREATE OR REPLACE STAGE {{DATABASE}}.{{SCHEMA}}.{{STAGE_NAME}}
  URL = 's3compat://{{R2_BUCKET}}/{{R2_PATH_PREFIX}}'
  ENDPOINT = '{{R2_ENDPOINT}}'
  CREDENTIALS = (AWS_KEY_ID = '{{R2_ACCESS_KEY}}' AWS_SECRET_KEY = '{{R2_SECRET_KEY}}')
  FILE_FORMAT = {{DATABASE}}.{{SCHEMA}}.{{FILE_FORMAT}};
```

## 4. Raw Table

```sql
CREATE TABLE IF NOT EXISTS {{DATABASE}}.{{SCHEMA}}.{{RAW_TABLE}} (
  RAW VARIANT
);
```

## 5. COPY INTO (idempotent — Snowflake tracks loaded files for 64 days)

```sql
COPY INTO {{DATABASE}}.{{SCHEMA}}.{{RAW_TABLE}}
  FROM @{{DATABASE}}.{{SCHEMA}}.{{STAGE_NAME}}
  FILE_FORMAT = {{DATABASE}}.{{SCHEMA}}.{{FILE_FORMAT}}
  MATCH_BY_COLUMN_NAME = NONE
  ON_ERROR = 'CONTINUE';
```

## 6. Parsed View (all Cloudflare HTTP request scalar fields)

```sql
CREATE OR REPLACE VIEW {{DATABASE}}.{{SCHEMA}}.{{VIEW_NAME}} AS
WITH parsed AS (
  SELECT
    -- Timestamps
    CONVERT_TIMEZONE('UTC', TO_TIMESTAMP_TZ(RAW:EdgeStartTimestamp::string))::TIMESTAMP_NTZ AS EDGE_START_TS,
    CONVERT_TIMEZONE('UTC', TO_TIMESTAMP_TZ(RAW:EdgeEndTimestamp::string))::TIMESTAMP_NTZ AS EDGE_END_TS,
    -- Request
    RAW:ClientRequestHost::string AS HOST,
    RAW:ClientRequestURI::string AS URI,
    RAW:ClientRequestPath::string AS PATH,
    RAW:ClientRequestMethod::string AS METHOD,
    RAW:ClientRequestProtocol::string AS PROTOCOL,
    RAW:ClientRequestScheme::string AS SCHEME,
    RAW:ClientRequestReferer::string AS REFERER,
    RAW:ClientRequestBytes::number AS REQUEST_BYTES,
    RAW:ClientRequestSource::string AS REQUEST_SOURCE,
    RAW:ClientXRequestedWith::string AS X_REQUESTED_WITH,
    -- Client
    RAW:ClientIP::string AS CLIENT_IP,
    RAW:ClientCountry::string AS COUNTRY,
    RAW:ClientCity::string AS CITY,
    RAW:ClientRegionCode::string AS REGION_CODE,
    RAW:ClientLatitude::float AS CLIENT_LATITUDE,
    RAW:ClientLongitude::float AS CLIENT_LONGITUDE,
    RAW:ClientDeviceType::string AS DEVICE_TYPE,
    RAW:ClientASN::number AS CLIENT_ASN,
    RAW:ClientIPClass::string AS CLIENT_IP_CLASS,
    RAW:ClientRequestUserAgent::string AS USER_AGENT,
    RAW:ClientSSLCipher::string AS SSL_CIPHER,
    RAW:ClientSSLProtocol::string AS SSL_PROTOCOL,
    RAW:ClientSrcPort::number AS CLIENT_SRC_PORT,
    RAW:ClientTCPRTTMs::number AS CLIENT_TCP_RTT_MS,
    -- Edge response
    RAW:EdgeResponseStatus::number AS STATUS,
    RAW:EdgeResponseBytes::number AS EDGE_RESPONSE_BYTES,
    RAW:EdgeResponseBodyBytes::number AS EDGE_RESPONSE_BODY_BYTES,
    RAW:EdgeResponseCompressionRatio::float AS COMPRESSION_RATIO,
    RAW:EdgeResponseContentType::string AS CONTENT_TYPE,
    RAW:EdgeTimeToFirstByteMs::number AS TTFB_MS,
    RAW:EdgeColoCode::string AS EDGE_COLO_CODE,
    RAW:EdgeColoID::number AS EDGE_COLO_ID,
    RAW:EdgeServerIP::string AS EDGE_SERVER_IP,
    RAW:EdgeRequestHost::string AS EDGE_REQUEST_HOST,
    RAW:EdgeCFConnectingO2O::boolean AS IS_O2O,
    -- Cache
    RAW:CacheCacheStatus::string AS CACHE_STATUS,
    RAW:CacheResponseStatus::number AS CACHE_RESPONSE_STATUS,
    RAW:CacheResponseBytes::number AS CACHE_RESPONSE_BYTES,
    RAW:CacheReserveUsed::boolean AS CACHE_RESERVE_USED,
    RAW:CacheTieredFill::boolean AS CACHE_TIERED_FILL,
    -- Origin
    RAW:OriginIP::string AS ORIGIN_IP,
    RAW:OriginResponseStatus::number AS ORIGIN_STATUS,
    RAW:OriginResponseBytes::number AS ORIGIN_RESPONSE_BYTES,
    RAW:OriginResponseTime::number AS ORIGIN_RESPONSE_TIME_MS,
    RAW:OriginResponseDurationMs::number AS ORIGIN_RESPONSE_DURATION_MS,
    RAW:OriginDNSResponseTimeMs::number AS ORIGIN_DNS_MS,
    RAW:OriginTCPHandshakeDurationMs::number AS ORIGIN_TCP_HANDSHAKE_MS,
    RAW:OriginTLSHandshakeDurationMs::number AS ORIGIN_TLS_HANDSHAKE_MS,
    RAW:OriginRequestHeaderSendDurationMs::number AS ORIGIN_HEADER_SEND_MS,
    RAW:OriginResponseHeaderReceiveDurationMs::number AS ORIGIN_HEADER_RECEIVE_MS,
    RAW:OriginSSLProtocol::string AS ORIGIN_SSL_PROTOCOL,
    -- Bot detection
    RAW:BotScore::number AS BOT_SCORE,
    RAW:BotScoreSrc::string AS BOT_SCORE_SRC,
    RAW:VerifiedBotCategory::string AS VERIFIED_BOT_CATEGORY,
    RAW:JA3Hash::string AS JA3_HASH,
    RAW:JA4::string AS JA4,
    RAW:JSDetectionPassed::string AS JS_DETECTION_PASSED,
    -- Security / WAF
    RAW:SecurityAction::string AS SECURITY_ACTION,
    RAW:SecurityRuleID::string AS SECURITY_RULE_ID,
    RAW:SecurityRuleDescription::string AS SECURITY_RULE_DESC,
    RAW:LeakedCredentialCheckResult::string AS LEAKED_CRED_CHECK,
    RAW:WAFAttackScore::number AS WAF_ATTACK_SCORE,
    RAW:WAFSQLiAttackScore::number AS WAF_SQLI_SCORE,
    RAW:WAFXSSAttackScore::number AS WAF_XSS_SCORE,
    RAW:WAFRCEAttackScore::number AS WAF_RCE_SCORE,
    -- Workers
    RAW:WorkerScriptName::string AS WORKER_SCRIPT_NAME,
    RAW:WorkerStatus::string AS WORKER_STATUS,
    RAW:WorkerCPUTime::number AS WORKER_CPU_TIME_US,
    RAW:WorkerWallTimeUs::number AS WORKER_WALL_TIME_US,
    RAW:WorkerSubrequest::boolean AS IS_WORKER_SUBREQUEST,
    RAW:WorkerSubrequestCount::number AS WORKER_SUBREQUEST_COUNT,
    -- IDs
    RAW:RayID::string AS RAY_ID,
    RAW:ParentRayID::string AS PARENT_RAY_ID,
    RAW:ZoneName::string AS ZONE_NAME,
    RAW AS RAW_JSON
  FROM {{DATABASE}}.{{SCHEMA}}.{{RAW_TABLE}}
)
SELECT
  parsed.*,
  DATE_TRUNC('hour', EDGE_START_TS) AS EDGE_HOUR,
  COALESCE(NULLIF(PATH, ''), '/') AS PATH_CLEAN,
  IFF(STATUS = 402, 1, 0) AS IS_402,
  IFF(STATUS >= 400 AND STATUS < 500, 1, 0) AS IS_4XX,
  IFF(STATUS >= 500 AND STATUS < 600, 1, 0) AS IS_5XX,
  IFF(BOT_SCORE < 30, 1, 0) AS IS_BOT,
  IFF(LOWER(CACHE_STATUS) = 'hit', 1, 0) AS IS_CACHE_HIT,
  1 AS REQUEST_COUNT
FROM parsed
WHERE EDGE_START_TS IS NOT NULL;
```

## 7. Semantic View

```sql
CREATE OR REPLACE SEMANTIC VIEW {{DATABASE}}.{{SCHEMA}}.{{SEMANTIC_VIEW}}
  TABLES (
    req AS {{DATABASE}}.{{SCHEMA}}.{{VIEW_NAME}}
  )
  FACTS (
    req.request_count AS REQUEST_COUNT,
    req.is_402 AS IS_402,
    req.is_5xx AS IS_5XX,
    req.is_bot AS IS_BOT,
    req.is_cache_hit AS IS_CACHE_HIT,
    req.edge_response_bytes AS EDGE_RESPONSE_BYTES,
    req.edge_response_body_bytes AS EDGE_RESPONSE_BODY_BYTES,
    req.request_bytes AS REQUEST_BYTES,
    req.cache_response_bytes AS CACHE_RESPONSE_BYTES,
    req.origin_response_bytes AS ORIGIN_RESPONSE_BYTES,
    req.ttfb_ms AS TTFB_MS,
    req.client_tcp_rtt_ms AS CLIENT_TCP_RTT_MS,
    req.origin_response_time_ms AS ORIGIN_RESPONSE_TIME_MS,
    req.origin_response_duration_ms AS ORIGIN_RESPONSE_DURATION_MS,
    req.origin_dns_ms AS ORIGIN_DNS_MS,
    req.origin_tcp_handshake_ms AS ORIGIN_TCP_HANDSHAKE_MS,
    req.origin_tls_handshake_ms AS ORIGIN_TLS_HANDSHAKE_MS,
    req.origin_header_send_ms AS ORIGIN_HEADER_SEND_MS,
    req.origin_header_receive_ms AS ORIGIN_HEADER_RECEIVE_MS,
    req.bot_score AS BOT_SCORE,
    req.waf_attack_score AS WAF_ATTACK_SCORE,
    req.waf_sqli_score AS WAF_SQLI_SCORE,
    req.waf_xss_score AS WAF_XSS_SCORE,
    req.waf_rce_score AS WAF_RCE_SCORE,
    req.compression_ratio AS COMPRESSION_RATIO,
    req.worker_cpu_time_us AS WORKER_CPU_TIME_US,
    req.worker_wall_time_us AS WORKER_WALL_TIME_US,
    req.is_4xx AS IS_4XX
  )
  DIMENSIONS (
    req.edge_start_ts AS EDGE_START_TS,
    req.edge_end_ts AS EDGE_END_TS,
    req.edge_hour AS EDGE_HOUR,
    req.host AS HOST, req.uri AS URI, req.path AS PATH,
    req.method AS METHOD, req.protocol AS PROTOCOL, req.scheme AS SCHEME,
    req.referer AS REFERER, req.content_type AS CONTENT_TYPE,
    req.client_ip AS CLIENT_IP, req.country AS COUNTRY, req.city AS CITY,
    req.region_code AS REGION_CODE, req.device_type AS DEVICE_TYPE,
    req.client_ip_class AS CLIENT_IP_CLASS, req.client_asn AS CLIENT_ASN,
    req.user_agent AS USER_AGENT,
    req.request_source AS REQUEST_SOURCE, req.ssl_protocol AS SSL_PROTOCOL,
    req.ssl_cipher AS SSL_CIPHER, req.x_requested_with AS X_REQUESTED_WITH,
    req.status AS STATUS, req.edge_colo_code AS EDGE_COLO_CODE,
    req.edge_request_host AS EDGE_REQUEST_HOST, req.zone_name AS ZONE_NAME,
    req.origin_ip AS ORIGIN_IP, req.origin_status AS ORIGIN_STATUS,
    req.origin_ssl_protocol AS ORIGIN_SSL_PROTOCOL,
    req.cache_status AS CACHE_STATUS, req.cache_reserve_used AS CACHE_RESERVE_USED,
    req.cache_tiered_fill AS CACHE_TIERED_FILL,
    req.bot_score_src AS BOT_SCORE_SRC, req.verified_bot_category AS VERIFIED_BOT_CATEGORY,
    req.ja3_hash AS JA3_HASH, req.ja4 AS JA4,
    req.js_detection_passed AS JS_DETECTION_PASSED,
    req.security_action AS SECURITY_ACTION, req.security_rule_id AS SECURITY_RULE_ID,
    req.security_rule_desc AS SECURITY_RULE_DESC,
    req.leaked_cred_check AS LEAKED_CRED_CHECK
  )
  METRICS (
    req.total_requests AS SUM(req.request_count),
    req.total_edge_bytes AS SUM(req.edge_response_bytes),
    req.total_origin_bytes AS SUM(req.origin_response_bytes),
    req.total_402 AS SUM(req.is_402),
    req.total_4xx AS SUM(req.is_4xx),
    req.total_5xx AS SUM(req.is_5xx),
    req.rate_402 AS IFF(SUM(req.request_count) = 0, NULL, SUM(req.is_402)::FLOAT / SUM(req.request_count)),
    req.rate_4xx AS IFF(SUM(req.request_count) = 0, NULL, SUM(req.is_4xx)::FLOAT / SUM(req.request_count)),
    req.rate_5xx AS IFF(SUM(req.request_count) = 0, NULL, SUM(req.is_5xx)::FLOAT / SUM(req.request_count)),
    req.total_bots AS SUM(req.is_bot),
    req.bot_rate AS IFF(SUM(req.request_count) = 0, NULL, SUM(req.is_bot)::FLOAT / SUM(req.request_count)),
    req.avg_bot_score AS AVG(req.bot_score),
    req.cache_hit_rate AS IFF(SUM(req.request_count) = 0, NULL, SUM(req.is_cache_hit)::FLOAT / SUM(req.request_count)),
    req.avg_ttfb_ms AS AVG(req.ttfb_ms),
    req.avg_origin_response_ms AS AVG(req.origin_response_time_ms),
    req.avg_compression_ratio AS AVG(req.compression_ratio)
  )
  AI_SQL_GENERATION 'Use STATUS for HTTP response codes (not CACHE_RESPONSE_STATUS or ORIGIN_STATUS unless specifically asked about origin/cache responses). All timestamps are UTC. BOT_SCORE ranges 0-99 where scores below 30 indicate likely automated/bot traffic; IS_BOT is pre-computed for this threshold. CACHE_STATUS values include hit, miss, expired, dynamic, bypass, etc.; IS_CACHE_HIT is pre-computed for hits. WAF_ATTACK_SCORE, WAF_SQLI_SCORE, WAF_XSS_SCORE, and WAF_RCE_SCORE range 1-99 where lower scores indicate higher attack likelihood. IS_4XX covers all 400-499 client errors; IS_402 is specific to HTTP 402. TTFB_MS is edge time-to-first-byte in milliseconds. ORIGIN_RESPONSE_TIME_MS is total origin response time. EDGE_COLO_CODE is the IATA airport code of the Cloudflare data center. CLIENT_ASN is the autonomous system number of the client network.';
```

## 8–9. Create Agent via SQL

The agent specification JSON is inlined directly in the `CREATE AGENT` statement. Key parts of the spec:
- **tool_spec** — `cortex_analyst_text_to_sql` tool named `query_http_logs` pointing at the semantic view
- **instructions** — orchestration prompt scoped to `{{ZONE_NAME}}` HTTP traffic; response prompt enforces concise markdown tables

```sql
USE ROLE ACCOUNTADMIN;

CREATE OR REPLACE AGENT {{DATABASE}}.{{SCHEMA}}.{{AGENT_NAME}}
  FROM SPECIFICATION $${
  "models": {"orchestration": "auto"},
  "tools": [{
    "tool_spec": {
      "name": "query_http_logs",
      "type": "cortex_analyst_text_to_sql",
      "description": "Purpose: Query Cloudflare HTTP request logs for {{ZONE_NAME}}.\n\nData coverage:\n- Each row is one HTTP request recorded by Cloudflare.\n- All timestamps are UTC.\n- 28 numeric facts (bytes, latencies, scores, flags), 42 dimensions (geo, device, cache, security), 16 pre-built metrics (rates, averages).\n\nCategories: traffic volume, error rates (4xx/5xx/402), geographic breakdowns, bot detection, cache performance, origin latency, WAF/security events, Workers, TLS/SSL.\n\nWhen NOT to use: real-time sub-minute data, request payloads/headers, DNS/firewall events, non-HTTP protocols."
    }
  }],
  "tool_resources": {
    "query_http_logs": {
      "semantic_view": "{{DATABASE}}.{{SCHEMA}}.{{SEMANTIC_VIEW}}"
    }
  },
  "instructions": {
    "orchestration": "You are a web analytics assistant for {{ZONE_NAME}}. You answer questions about HTTP traffic patterns using Cloudflare request logs stored in Snowflake. All timestamps are in UTC. Use the query_http_logs tool for any question about traffic, errors, pages, geography, bots, cache, security, or request volumes.",
    "response": "Be concise. Use markdown tables for multi-row results. Mention the time range when relevant. If asked about something outside HTTP log scope, explain what data is available."
  }
}$$;
```

**KNOWN ISSUE:** `CREATE AGENT FROM SPECIFICATION` silently drops `execution_environment` from `tool_resources`. You must set it via REST API after creation — see template 10.

## 10. Set execution_environment via REST API

Required after agent creation. The REST API PUT reliably sets `execution_environment`.

**Steps:** Use `snow` CLI to get a session token, then GET the current agent spec, patch in `execution_environment`, and PUT it back.

```bash
# 1. Get session token and host from snow CLI
TOKEN=$(snow connection generate-jwt --connection {{CONNECTION_NAME}} 2>/dev/null || \
        snow sql -q "SELECT SYSTEM\$GENERATE_JWT()" --connection {{CONNECTION_NAME}} -o json | python3 -c "import sys,json; print(json.load(sys.stdin)[0])")

HOST=$(snow connection describe {{CONNECTION_NAME}} -o json | python3 -c "import sys,json; c=json.load(sys.stdin); print(c.get('host', c.get('account') + '.snowflakecomputing.com'))")

API_URL="https://${HOST}/api/v2/databases/{{DATABASE}}/schemas/{{SCHEMA}}/agents/{{AGENT_NAME}}"

# 2. GET current spec
SPEC=$(curl -s "${API_URL}" \
  -H "Authorization: Snowflake Token=\"${TOKEN}\"" \
  -H "Content-Type: application/json")

# 3. Patch in execution_environment
UPDATED=$(echo "${SPEC}" | python3 -c "
import sys, json
spec = json.load(sys.stdin)
spec['tool_resources']['query_http_logs']['execution_environment'] = {
    'type': 'warehouse',
    'warehouse': '{{WAREHOUSE}}'
}
json.dump(spec, sys.stdout)
")

# 4. PUT updated spec
curl -s -X PUT "${API_URL}" \
  -H "Authorization: Snowflake Token=\"${TOKEN}\"" \
  -H "Content-Type: application/json" \
  -d "${UPDATED}"
```

> **Note:** If `snow` CLI is unavailable, Cortex Code can use `snowflake_sql_execute` to run `SELECT SYSTEM$GENERATE_JWT()` for the token and construct the REST calls directly.

## 11. Grant Access

```sql
GRANT USAGE ON DATABASE {{DATABASE}} TO ROLE {{ROLE}};
GRANT USAGE ON SCHEMA {{DATABASE}}.{{SCHEMA}} TO ROLE {{ROLE}};
GRANT USAGE ON AGENT {{DATABASE}}.{{SCHEMA}}.{{AGENT_NAME}} TO ROLE {{ROLE}};
GRANT SELECT, REFERENCES ON SEMANTIC VIEW {{DATABASE}}.{{SCHEMA}}.{{SEMANTIC_VIEW}} TO ROLE {{ROLE}};
GRANT SELECT ON VIEW {{DATABASE}}.{{SCHEMA}}.{{VIEW_NAME}} TO ROLE {{ROLE}};
GRANT USAGE ON WAREHOUSE {{WAREHOUSE}} TO ROLE {{ROLE}};
```

## 12. Weekly Refresh Task (optional)

```sql
CREATE OR REPLACE TASK {{DATABASE}}.{{SCHEMA}}.{{AGENT_NAME}}_COPY_TASK
  WAREHOUSE = '{{WAREHOUSE}}'
  SCHEDULE = 'USING CRON 0 6 * * 0 UTC'
  AS
  COPY INTO {{DATABASE}}.{{SCHEMA}}.{{RAW_TABLE}}
    FROM @{{DATABASE}}.{{SCHEMA}}.{{STAGE_NAME}}
    FILE_FORMAT = {{DATABASE}}.{{SCHEMA}}.{{FILE_FORMAT}}
    MATCH_BY_COLUMN_NAME = NONE
    ON_ERROR = 'CONTINUE';

ALTER TASK {{DATABASE}}.{{SCHEMA}}.{{AGENT_NAME}}_COPY_TASK RESUME;
```

## 13. Cleanup / Rollback (drop all objects)

Use this to tear down the pipeline if something goes wrong or the pipeline is no longer needed. Run in this order to respect dependencies.

```sql
-- Suspend task first (if it exists)
ALTER TASK IF EXISTS {{DATABASE}}.{{SCHEMA}}.{{AGENT_NAME}}_COPY_TASK SUSPEND;
DROP TASK IF EXISTS {{DATABASE}}.{{SCHEMA}}.{{AGENT_NAME}}_COPY_TASK;

-- Drop agent
DROP AGENT IF EXISTS {{DATABASE}}.{{SCHEMA}}.{{AGENT_NAME}};

-- Drop semantic view, parsed view, raw table
DROP SEMANTIC VIEW IF EXISTS {{DATABASE}}.{{SCHEMA}}.{{SEMANTIC_VIEW}};
DROP VIEW IF EXISTS {{DATABASE}}.{{SCHEMA}}.{{VIEW_NAME}};
DROP TABLE IF EXISTS {{DATABASE}}.{{SCHEMA}}.{{RAW_TABLE}};

-- Drop stage and file format
DROP STAGE IF EXISTS {{DATABASE}}.{{SCHEMA}}.{{STAGE_NAME}};
DROP FILE FORMAT IF EXISTS {{DATABASE}}.{{SCHEMA}}.{{FILE_FORMAT}};

-- Optionally drop schema and database (only if created for this pipeline)
-- DROP SCHEMA IF EXISTS {{DATABASE}}.{{SCHEMA}};
-- DROP DATABASE IF EXISTS {{DATABASE}};
```
