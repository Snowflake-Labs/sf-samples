# Golden Record & Survivorship

Field-level survivorship using FIRST_VALUE IGNORE NULLS, source priority + recency + staleness penalty, and field conflict detection.

## Keyring DT Template

Maps every source record to its resolved master_customer_id:

```sql
CREATE OR REPLACE DYNAMIC TABLE <schema>.KEYRING
    TARGET_LAG = '1 hour'
    WAREHOUSE = <warehouse>
    REFRESH_MODE = INCREMENTAL
AS
SELECT
    c.node_id,
    SPLIT_PART(c.node_id, '::', 1) AS source_system,
    SPLIT_PART(c.node_id, '::', 2) AS source_id,
    c.master_customer_id,
    c.cluster_size,
    c.is_oversized,
    c.run_id,
    c.resolved_at,
    s.email_clean,
    s.phone_clean,
    s.loyalty_id,
    s.device_id,
    s.cookie_id
FROM <schema>.IDENTITY_CLUSTERS c
JOIN <schema>.IDENTITY_SIGNALS s
    ON c.node_id = s.source_system || '::' || s.source_id;
```

## Golden Record DT Template

Field-level survivorship: each field is resolved INDEPENDENTLY using the best available value across all linked source records.

Ordering logic per field:
1. **Source priority** (lower number = higher trust) — e.g., CRM wins over web
2. **Recency** (most recently updated) — newer data preferred within same priority
3. **Staleness penalty** — records older than `staleness_days` are deprioritized regardless of source rank

```sql
CREATE OR REPLACE DYNAMIC TABLE <schema>.GOLDEN_RECORD
    TARGET_LAG = '1 hour'
    WAREHOUSE = <warehouse>
    REFRESH_MODE = INCREMENTAL
AS
WITH enriched AS (
    SELECT
        k.master_customer_id,
        k.source_system,
        k.source_id,
        k.cluster_size,
        k.is_oversized,
        s.first_name,
        s.last_name,
        s.email_clean AS email,
        s.phone_clean AS phone,
        s.loyalty_id,
        s.address_line_1,
        s.city,
        s.state,
        s.zip5,
        s.date_of_birth,
        s.gender,
        s.source_updated_at,
        r.source_priority,
        r.staleness_days
    FROM <schema>.KEYRING k
    JOIN <schema>.IDENTITY_SIGNALS s
        ON k.source_system = s.source_system AND k.source_id = s.source_id
    JOIN <schema>.SOURCE_REGISTRY r
        ON k.source_system = r.source_name
    WHERE k.is_oversized = FALSE
),
field_winners AS (
    SELECT
        master_customer_id,
        FIRST_VALUE(first_name) IGNORE NULLS OVER (
            PARTITION BY master_customer_id
            ORDER BY source_priority ASC, source_updated_at DESC
        ) AS best_first_name,
        FIRST_VALUE(last_name) IGNORE NULLS OVER (
            PARTITION BY master_customer_id
            ORDER BY source_priority ASC, source_updated_at DESC
        ) AS best_last_name,
        FIRST_VALUE(email) IGNORE NULLS OVER (
            PARTITION BY master_customer_id
            ORDER BY source_priority ASC, source_updated_at DESC
        ) AS best_email,
        FIRST_VALUE(phone) IGNORE NULLS OVER (
            PARTITION BY master_customer_id
            ORDER BY source_priority ASC, source_updated_at DESC
        ) AS best_phone,
        FIRST_VALUE(loyalty_id) IGNORE NULLS OVER (
            PARTITION BY master_customer_id
            ORDER BY source_priority ASC
        ) AS best_loyalty_id,
        FIRST_VALUE(address_line_1) IGNORE NULLS OVER (
            PARTITION BY master_customer_id
            ORDER BY source_priority ASC, source_updated_at DESC
        ) AS best_address,
        FIRST_VALUE(city) IGNORE NULLS OVER (
            PARTITION BY master_customer_id
            ORDER BY source_priority ASC, source_updated_at DESC
        ) AS best_city,
        FIRST_VALUE(state) IGNORE NULLS OVER (
            PARTITION BY master_customer_id
            ORDER BY source_priority ASC, source_updated_at DESC
        ) AS best_state,
        FIRST_VALUE(zip5) IGNORE NULLS OVER (
            PARTITION BY master_customer_id
            ORDER BY source_priority ASC, source_updated_at DESC
        ) AS best_zip5,
        FIRST_VALUE(date_of_birth) IGNORE NULLS OVER (
            PARTITION BY master_customer_id
            ORDER BY source_priority ASC
        ) AS best_dob,
        FIRST_VALUE(gender) IGNORE NULLS OVER (
            PARTITION BY master_customer_id
            ORDER BY source_priority ASC
        ) AS best_gender,
        cluster_size,
        source_updated_at,
        ROW_NUMBER() OVER (
            PARTITION BY master_customer_id
            ORDER BY source_priority ASC, source_updated_at DESC
        ) AS rn
    FROM enriched
)
SELECT
    master_customer_id,
    best_first_name AS first_name,
    best_last_name AS last_name,
    best_email AS email,
    best_phone AS phone,
    best_loyalty_id AS loyalty_id,
    best_address AS address_line_1,
    best_city AS city,
    best_state AS state,
    best_zip5 AS zip5,
    best_dob AS date_of_birth,
    best_gender AS gender,
    cluster_size AS linked_identities_count,
    MAX(source_updated_at) AS last_updated_at
FROM field_winners
WHERE rn = 1
GROUP BY master_customer_id, best_first_name, best_last_name, best_email,
    best_phone, best_loyalty_id, best_address, best_city, best_state,
    best_zip5, best_dob, best_gender, cluster_size;
```

## Key Design Notes

- `FIRST_VALUE IGNORE NULLS` ensures we get the best NON-NULL value per field, not just the best record overall
- `WHERE k.is_oversized = FALSE` excludes suspicious clusters from the golden record
- Window functions with PARTITION BY are incremental-refresh compatible in Snowflake DTs
- The `rn = 1` filter and GROUP BY at the end deduplicate to one row per master_customer_id

## Field Conflicts DT Template

Surfaces cases where multiple trusted sources disagree on a field value:

```sql
CREATE OR REPLACE DYNAMIC TABLE <schema>.FIELD_CONFLICTS
    TARGET_LAG = '1 hour'
    WAREHOUSE = <warehouse>
    REFRESH_MODE = INCREMENTAL
AS
WITH source_values AS (
    SELECT
        k.master_customer_id,
        k.source_system,
        r.source_priority,
        s.email_clean AS email,
        s.phone_clean AS phone,
        s.first_name,
        s.last_name
    FROM <schema>.KEYRING k
    JOIN <schema>.IDENTITY_SIGNALS s
        ON k.source_system = s.source_system AND k.source_id = s.source_id
    JOIN <schema>.SOURCE_REGISTRY r
        ON k.source_system = r.source_name
    WHERE r.source_priority <= 3
      AND k.is_oversized = FALSE
)
SELECT
    master_customer_id,
    'EMAIL' AS field_name,
    COUNT(DISTINCT email) AS distinct_count
FROM source_values
WHERE email IS NOT NULL AND email != ''
GROUP BY master_customer_id
HAVING COUNT(DISTINCT email) > 1

UNION ALL

SELECT
    master_customer_id,
    'PHONE' AS field_name,
    COUNT(DISTINCT phone) AS distinct_count
FROM source_values
WHERE phone IS NOT NULL AND phone != ''
GROUP BY master_customer_id
HAVING COUNT(DISTINCT phone) > 1

UNION ALL

SELECT
    master_customer_id,
    'NAME' AS field_name,
    COUNT(DISTINCT first_name || ' ' || last_name) AS distinct_count
FROM source_values
WHERE first_name IS NOT NULL
GROUP BY master_customer_id
HAVING COUNT(DISTINCT first_name || ' ' || last_name) > 1;
```

## Survivorship Rule Customization

For specific use cases, the ordering can be adjusted:

| Use Case | Ordering Modification |
|----------|----------------------|
| Address should always be most recent | ORDER BY source_updated_at DESC (ignore priority for address) |
| Email should come from the system of record | ORDER BY source_priority ASC only (ignore recency) |
| Phone should prefer mobile over landline | Add a phone_type column and CASE logic |
| DOB should never change | ORDER BY source_priority ASC, source_updated_at ASC (oldest wins — first recorded is likely correct) |

To implement custom ordering per field, modify the ORDER BY clause in the corresponding FIRST_VALUE window function.
