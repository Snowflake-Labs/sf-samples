# Deterministic Matching

SQL templates for exact-match identity edge generation with signal strength tiers and guardrails.

## Config Tables DDL

### Source Registry

```sql
CREATE OR REPLACE TABLE <schema>.SOURCE_REGISTRY (
    source_name       VARCHAR NOT NULL,
    source_table_fqn  VARCHAR NOT NULL,
    source_priority   INT NOT NULL,       -- 1=highest trust, 2, 3...
    has_email         BOOLEAN DEFAULT FALSE,
    has_phone         BOOLEAN DEFAULT FALSE,
    has_loyalty_id    BOOLEAN DEFAULT FALSE,
    has_device_id     BOOLEAN DEFAULT FALSE,
    has_cookie_id     BOOLEAN DEFAULT FALSE,
    has_address       BOOLEAN DEFAULT FALSE,
    is_active         BOOLEAN DEFAULT TRUE,
    staleness_days    INT DEFAULT 730,
    registered_at     TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);
```

### Match Weights Config

```sql
CREATE OR REPLACE TABLE <schema>.MATCH_WEIGHTS_CONFIG (
    config_version        INT NOT NULL DEFAULT 1,
    is_active             BOOLEAN DEFAULT TRUE,
    weight_name           FLOAT DEFAULT 0.30,
    weight_address        FLOAT DEFAULT 0.20,
    weight_phone_fuzzy    FLOAT DEFAULT 0.15,
    weight_email_user     FLOAT DEFAULT 0.15,
    weight_dob            FLOAT DEFAULT 0.15,
    weight_zip            FLOAT DEFAULT 0.05,
    threshold_auto_accept FLOAT DEFAULT 0.90,
    threshold_review      FLOAT DEFAULT 0.70,
    max_cluster_size      INT DEFAULT 50,
    updated_at            TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

INSERT INTO <schema>.MATCH_WEIGHTS_CONFIG (config_version) VALUES (1);
```

### Toxic Identifiers

```sql
CREATE OR REPLACE TABLE <schema>.TOXIC_IDENTIFIERS (
    identifier_value  VARCHAR NOT NULL,
    identifier_type   VARCHAR NOT NULL,  -- EMAIL, PHONE, LOYALTY_ID, DEVICE_ID, COOKIE_ID
    reason            VARCHAR,
    added_at          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

INSERT INTO <schema>.TOXIC_IDENTIFIERS (identifier_value, identifier_type, reason) VALUES
    ('test@test.com', 'EMAIL', 'test account'),
    ('noreply@company.com', 'EMAIL', 'system email'),
    ('admin@admin.com', 'EMAIL', 'admin account'),
    ('info@company.com', 'EMAIL', 'generic inbox'),
    ('0000000000', 'PHONE', 'placeholder zeros'),
    ('1111111111', 'PHONE', 'placeholder ones'),
    ('9999999999', 'PHONE', 'placeholder nines'),
    ('1234567890', 'PHONE', 'sequential placeholder'),
    ('5555555555', 'PHONE', 'repeated fives'),
    ('00000000-0000-0000-0000-000000000000', 'DEVICE_ID', 'zeroed UUID'),
    ('unknown', 'DEVICE_ID', 'placeholder');
```

### Match Audit Log

```sql
CREATE OR REPLACE TABLE <schema>.MATCH_AUDIT_LOG (
    audit_id          VARCHAR DEFAULT UUID_STRING(),
    run_id            VARCHAR NOT NULL,
    id_a              VARCHAR NOT NULL,
    id_b              VARCHAR NOT NULL,
    match_type        VARCHAR NOT NULL,
    match_subtype     VARCHAR,
    confidence        FLOAT NOT NULL,
    status            VARCHAR NOT NULL,
    field_scores      VARIANT,
    resolved_at       TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);
```

## Standardization DT Template

Generate one per source. All must output the SAME column set:

```sql
CREATE OR REPLACE DYNAMIC TABLE <schema>.STD_SOURCE_<NAME>
    TARGET_LAG = DOWNSTREAM
    WAREHOUSE = <warehouse>
    REFRESH_MODE = INCREMENTAL
AS
SELECT
    '<NAME>' AS source_system,
    <pk_col> AS source_id,
    -- Email with toxic exclusion
    CASE WHEN t_email.identifier_value IS NULL
         THEN LOWER(TRIM(<email_col>))
         ELSE NULL END AS email_clean,
    -- Phone with toxic exclusion
    CASE WHEN t_phone.identifier_value IS NULL
         THEN REGEXP_REPLACE(<phone_col>, '[^0-9]', '')
         ELSE NULL END AS phone_clean,
    <loyalty_col_or_null> AS loyalty_id,
    <device_col_or_null> AS device_id,
    <cookie_col_or_null> AS cookie_id,
    UPPER(TRIM(<first_name_col>)) AS first_name,
    UPPER(TRIM(<last_name_col>)) AS last_name,
    UPPER(TRIM(<address_col>)) AS address_line_1,
    UPPER(TRIM(<city_col>)) AS city,
    UPPER(TRIM(<state_col>)) AS state,
    LEFT(REGEXP_REPLACE(<zip_col>, '[^0-9]', ''), 5) AS zip5,
    <dob_col_or_null>::DATE AS date_of_birth,
    <gender_col_or_null> AS gender,
    <timestamp_col> AS source_updated_at
FROM <source_table> src
LEFT JOIN <schema>.TOXIC_IDENTIFIERS t_email
    ON LOWER(TRIM(src.<email_col>)) = t_email.identifier_value
    AND t_email.identifier_type = 'EMAIL'
LEFT JOIN <schema>.TOXIC_IDENTIFIERS t_phone
    ON REGEXP_REPLACE(src.<phone_col>, '[^0-9]', '') = t_phone.identifier_value
    AND t_phone.identifier_type = 'PHONE';
```

For fields the source doesn't have, use `NULL AS <field_name>` with appropriate cast (e.g., `NULL::DATE AS date_of_birth`).

## Deterministic Edges DT Template

Generate ONLY the edge types where at least 2 sources have the field populated. Structure as separate CTEs per match type then UNION ALL:

### Strong Signals (confidence = 1.0)

#### Email Edges (with household guard)

```sql
email_edges AS (
    SELECT
        a.source_system || '::' || a.source_id AS id_a,
        b.source_system || '::' || b.source_id AS id_b,
        'EMAIL' AS match_type,
        1.0 AS confidence
    FROM <schema>.IDENTITY_SIGNALS a
    JOIN <schema>.IDENTITY_SIGNALS b
        ON a.email_clean = b.email_clean
        AND a.email_clean IS NOT NULL
        AND a.email_clean != ''
        AND a.source_system || '::' || a.source_id < b.source_system || '::' || b.source_id
    WHERE (
        JAROWINKLER_SIMILARITY(
            COALESCE(a.first_name,'X') || ' ' || COALESCE(a.last_name,'X'),
            COALESCE(b.first_name,'X') || ' ' || COALESCE(b.last_name,'X')
        ) > 60
        OR a.first_name IS NULL OR b.first_name IS NULL
        OR a.last_name IS NULL OR b.last_name IS NULL
    )
)
```

**Guardrail**: Name similarity > 60 prevents household email false links. If either record has no name, allow the match (benefit of the doubt).

#### Phone Edges (with length and recency guard)

```sql
phone_edges AS (
    SELECT
        a.source_system || '::' || a.source_id AS id_a,
        b.source_system || '::' || b.source_id AS id_b,
        'PHONE' AS match_type,
        1.0 AS confidence
    FROM <schema>.IDENTITY_SIGNALS a
    JOIN <schema>.IDENTITY_SIGNALS b
        ON a.phone_clean = b.phone_clean
        AND a.phone_clean IS NOT NULL
        AND a.phone_clean != ''
        AND LENGTH(a.phone_clean) >= 10
        AND a.source_system || '::' || a.source_id < b.source_system || '::' || b.source_id
)
```

**Guardrail**: LENGTH >= 10 ensures we're matching full phone numbers, not fragments.

#### Loyalty ID Edges (strongest signal)

```sql
loyalty_edges AS (
    SELECT
        a.source_system || '::' || a.source_id AS id_a,
        b.source_system || '::' || b.source_id AS id_b,
        'LOYALTY_ID' AS match_type,
        1.0 AS confidence
    FROM <schema>.IDENTITY_SIGNALS a
    JOIN <schema>.IDENTITY_SIGNALS b
        ON a.loyalty_id = b.loyalty_id
        AND a.loyalty_id IS NOT NULL
        AND a.loyalty_id != ''
        AND a.source_system || '::' || a.source_id < b.source_system || '::' || b.source_id
)
```

### Weak Signals (require corroboration)

#### Device ID (confidence = 0.85)

```sql
device_edges AS (
    SELECT
        a.source_system || '::' || a.source_id AS id_a,
        b.source_system || '::' || b.source_id AS id_b,
        'DEVICE_ID' AS match_type,
        0.85 AS confidence
    FROM <schema>.IDENTITY_SIGNALS a
    JOIN <schema>.IDENTITY_SIGNALS b
        ON a.device_id = b.device_id
        AND a.device_id IS NOT NULL
        AND a.device_id != ''
        AND a.source_system || '::' || a.source_id < b.source_system || '::' || b.source_id
    WHERE (a.email_clean = b.email_clean AND a.email_clean IS NOT NULL)
       OR (a.phone_clean = b.phone_clean AND a.phone_clean IS NOT NULL)
       OR JAROWINKLER_SIMILARITY(
              COALESCE(a.first_name,'') || ' ' || COALESCE(a.last_name,''),
              COALESCE(b.first_name,'') || ' ' || COALESCE(b.last_name,'')
          ) > 85
)
```

#### Cookie ID (confidence = 0.80)

```sql
cookie_edges AS (
    SELECT
        a.source_system || '::' || a.source_id AS id_a,
        b.source_system || '::' || b.source_id AS id_b,
        'COOKIE_ID' AS match_type,
        0.80 AS confidence
    FROM <schema>.IDENTITY_SIGNALS a
    JOIN <schema>.IDENTITY_SIGNALS b
        ON a.cookie_id = b.cookie_id
        AND a.cookie_id IS NOT NULL
        AND a.cookie_id != ''
        AND a.source_system || '::' || a.source_id < b.source_system || '::' || b.source_id
    WHERE (a.email_clean = b.email_clean AND a.email_clean IS NOT NULL)
       OR (a.loyalty_id = b.loyalty_id AND a.loyalty_id IS NOT NULL)
)
```

**Guardrail**: Cookie matches require EITHER a matching email OR matching loyalty_id. Name alone is not enough for cookie corroboration (too many people share names).

## Final DT Assembly

```sql
CREATE OR REPLACE DYNAMIC TABLE <schema>.DETERMINISTIC_EDGES
    TARGET_LAG = DOWNSTREAM
    WAREHOUSE = <warehouse>
    REFRESH_MODE = INCREMENTAL
AS
WITH
<include only CTEs for match types that have 2+ sources with the field>,
...
SELECT id_a, id_b, match_type, confidence FROM email_edges
UNION ALL
SELECT id_a, id_b, match_type, confidence FROM phone_edges
UNION ALL
...;
```

## Scalability Notes

For very large datasets (>50M records), the self-join in deterministic edges can be expensive even with equality predicates. Mitigation strategies:

1. **Cluster source tables** by the join key: `ALTER TABLE <source> CLUSTER BY (<identity_field>);`
2. **Split by match type**: Create separate DTs per match type (EMAIL_EDGES, PHONE_EDGES, etc.) and UNION ALL into a final DETERMINISTIC_EDGES DT. This lets Snowflake optimize each join independently.
3. **Use DOWNSTREAM target lag** on intermediate DTs to avoid redundant refreshes.
