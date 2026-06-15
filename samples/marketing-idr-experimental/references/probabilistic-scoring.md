# Probabilistic Scoring

Multi-pass blocking strategies, composite scoring with configurable weights, and threshold tiering for fuzzy identity matching.

## When to Use Probabilistic Matching

Apply AFTER deterministic matching, only to records that remain unresolved. Probabilistic matching handles:
- Typos in email domains (gmial.com vs gmail.com)
- Nickname vs legal name (Bob vs Robert)
- Address format variations (123 Main St vs 123 Main Street)
- Phone format inconsistencies caught by blocking but not exact match
- Records missing the shared identifier used for deterministic matching

## Blocking Strategy Selection

Choose blocking passes based on which fields are ACTUALLY PRESENT across sources. Only create a blocking pass if at least 2 sources have the required fields with reasonable fill rates (>20%).

| Pass | Required Fields | Join Condition | When to Use |
|------|----------------|---------------|-------------|
| ZIP + Last Name Prefix | zip5, last_name | `a.zip5 = b.zip5 AND LEFT(a.last_name, 2) = LEFT(b.last_name, 2)` | Address data exists in 2+ sources |
| DOB + SOUNDEX First Name | date_of_birth, first_name | `a.date_of_birth = b.date_of_birth AND SOUNDEX(a.first_name) = SOUNDEX(b.first_name)` | DOB exists in 2+ sources |
| Phone Last 7 + First Initial | phone_clean, first_name | `RIGHT(a.phone_clean, 7) = RIGHT(b.phone_clean, 7) AND LEFT(a.first_name, 1) = LEFT(b.first_name, 1)` | Phone exists but might have country code differences |
| Email Username + City | email_clean, city | `SPLIT_PART(a.email_clean, '@', 1) = SPLIT_PART(b.email_clean, '@', 1) AND a.city = b.city` | Same person, different email domain |

## Blocking Pass DT Template

```sql
CREATE OR REPLACE DYNAMIC TABLE <schema>.PROB_CANDIDATES_<PASS_NAME>
    TARGET_LAG = DOWNSTREAM
    WAREHOUSE = <warehouse>
    REFRESH_MODE = INCREMENTAL
AS
SELECT
    a.source_system || '::' || a.source_id AS id_a,
    b.source_system || '::' || b.source_id AS id_b,
    '<PASS_NAME>' AS blocking_strategy,
    a.first_name AS first_name_a, a.last_name AS last_name_a,
    b.first_name AS first_name_b, b.last_name AS last_name_b,
    a.address_line_1 AS addr_a, b.address_line_1 AS addr_b,
    a.date_of_birth AS dob_a, b.date_of_birth AS dob_b,
    a.phone_clean AS phone_a, b.phone_clean AS phone_b,
    a.email_clean AS email_a, b.email_clean AS email_b,
    a.zip5 AS zip_a, b.zip5 AS zip_b
FROM <schema>.IDENTITY_SIGNALS a
JOIN <schema>.IDENTITY_SIGNALS b
    ON <blocking_join_condition>
    AND a.source_system || '::' || a.source_id < b.source_system || '::' || b.source_id
WHERE JAROWINKLER_SIMILARITY(
    COALESCE(a.first_name,'') || ' ' || COALESCE(a.last_name,''),
    COALESCE(b.first_name,'') || ' ' || COALESCE(b.last_name,'')
) > 70;
```

The final WHERE clause is a pre-filter — only pass candidates with at least moderate name similarity to the scoring stage. This dramatically reduces the number of pairs to score.

## Unified Scoring DT Template

Reads from all blocking passes, deduplicates pairs, scores with configurable weights:

```sql
CREATE OR REPLACE DYNAMIC TABLE <schema>.PROBABILISTIC_EDGES
    TARGET_LAG = DOWNSTREAM
    WAREHOUSE = <warehouse>
    REFRESH_MODE = INCREMENTAL
AS
WITH all_candidates AS (
    SELECT * FROM <schema>.PROB_CANDIDATES_PASS1
    UNION ALL
    SELECT * FROM <schema>.PROB_CANDIDATES_PASS2
    -- ... add more passes as needed
),
deduped AS (
    SELECT *
    FROM all_candidates
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id_a, id_b ORDER BY blocking_strategy) = 1
),
scored AS (
    SELECT
        c.id_a,
        c.id_b,
        c.blocking_strategy,
        -- Name score (0-1)
        JAROWINKLER_SIMILARITY(
            COALESCE(c.first_name_a,'') || ' ' || COALESCE(c.last_name_a,''),
            COALESCE(c.first_name_b,'') || ' ' || COALESCE(c.last_name_b,'')
        ) / 100.0 AS name_score,
        -- Address score (0-1)
        CASE WHEN c.addr_a IS NOT NULL AND c.addr_b IS NOT NULL
                  AND c.addr_a != '' AND c.addr_b != ''
             THEN JAROWINKLER_SIMILARITY(c.addr_a, c.addr_b) / 100.0
             ELSE 0 END AS addr_score,
        -- Phone fuzzy score (0-1, based on edit distance)
        CASE WHEN c.phone_a IS NOT NULL AND c.phone_b IS NOT NULL
                  AND c.phone_a != '' AND c.phone_b != ''
             THEN GREATEST(0, 1.0 - (EDITDISTANCE(c.phone_a, c.phone_b, 3)::FLOAT / 3.0))
             ELSE 0 END AS phone_score,
        -- Email username score (0-1)
        CASE WHEN c.email_a IS NOT NULL AND c.email_b IS NOT NULL
                  AND c.email_a != '' AND c.email_b != ''
             THEN JAROWINKLER_SIMILARITY(
                      SPLIT_PART(c.email_a, '@', 1),
                      SPLIT_PART(c.email_b, '@', 1)
                  ) / 100.0
             ELSE 0 END AS email_user_score,
        -- DOB exact match (binary: 0 or 1)
        CASE WHEN c.dob_a = c.dob_b AND c.dob_a IS NOT NULL THEN 1.0 ELSE 0 END AS dob_score,
        -- ZIP exact match (binary: 0 or 1)
        CASE WHEN c.zip_a = c.zip_b AND c.zip_a IS NOT NULL AND c.zip_a != '' THEN 1.0 ELSE 0 END AS zip_score
    FROM deduped c
)
SELECT
    s.id_a,
    s.id_b,
    'PROBABILISTIC' AS match_type,
    s.blocking_strategy AS match_subtype,
    -- Weighted composite score using config table
    (s.name_score * w.weight_name
     + s.addr_score * w.weight_address
     + s.phone_score * w.weight_phone_fuzzy
     + s.email_user_score * w.weight_email_user
     + s.dob_score * w.weight_dob
     + s.zip_score * w.weight_zip) AS confidence,
    -- Tiered status
    CASE
        WHEN (s.name_score * w.weight_name + s.addr_score * w.weight_address
              + s.phone_score * w.weight_phone_fuzzy + s.email_user_score * w.weight_email_user
              + s.dob_score * w.weight_dob + s.zip_score * w.weight_zip) >= w.threshold_auto_accept
            THEN 'ACCEPTED'
        WHEN (s.name_score * w.weight_name + s.addr_score * w.weight_address
              + s.phone_score * w.weight_phone_fuzzy + s.email_user_score * w.weight_email_user
              + s.dob_score * w.weight_dob + s.zip_score * w.weight_zip) >= w.threshold_review
            THEN 'REVIEW'
        ELSE 'REJECTED'
    END AS match_status,
    -- Store individual scores for audit/debugging
    OBJECT_CONSTRUCT(
        'name_score', s.name_score,
        'addr_score', s.addr_score,
        'phone_score', s.phone_score,
        'email_user_score', s.email_user_score,
        'dob_score', s.dob_score,
        'zip_score', s.zip_score
    ) AS field_scores
FROM scored s
CROSS JOIN <schema>.MATCH_WEIGHTS_CONFIG w
WHERE w.is_active = TRUE
AND (s.name_score * w.weight_name + s.addr_score * w.weight_address
     + s.phone_score * w.weight_phone_fuzzy + s.email_user_score * w.weight_email_user
     + s.dob_score * w.weight_dob + s.zip_score * w.weight_zip) >= w.threshold_review;
```

## All Edges Union

Only ACCEPTED probabilistic edges flow into graph resolution:

```sql
CREATE OR REPLACE DYNAMIC TABLE <schema>.ALL_EDGES
    TARGET_LAG = DOWNSTREAM
    WAREHOUSE = <warehouse>
    REFRESH_MODE = INCREMENTAL
AS
SELECT id_a, id_b, match_type, match_type AS match_subtype, confidence
FROM <schema>.DETERMINISTIC_EDGES
UNION ALL
SELECT id_a, id_b, match_type, match_subtype, confidence
FROM <schema>.PROBABILISTIC_EDGES
WHERE match_status = 'ACCEPTED';
```

## Weight Tuning Guidelines

Default weights assume typical B2C retail data:

| Field | Weight | Rationale |
|-------|--------|-----------|
| Name | 0.30 | Strongest fuzzy signal but affected by nicknames/typos |
| Address | 0.20 | Good discriminator but people move |
| Phone | 0.15 | Transposed digits are common; edit distance handles this |
| Email Username | 0.15 | Same username, different domain is strong signal |
| DOB | 0.15 | Binary — either matches or doesn't. Very discriminating when present |
| ZIP | 0.05 | Weak alone but useful tiebreaker |

Tuning process:
1. Run pipeline with defaults
2. Sample the REVIEW queue — what's the false positive rate?
3. If too many false positives: raise `threshold_auto_accept` or increase `weight_name`
4. If too many missed matches: lower `threshold_auto_accept` or add more blocking passes
5. If specific field is unreliable in this dataset: lower its weight

```sql
UPDATE <schema>.MATCH_WEIGHTS_CONFIG
SET threshold_auto_accept = 0.85,
    weight_name = 0.35,
    updated_at = CURRENT_TIMESTAMP()
WHERE is_active = TRUE;
```

## Performance Considerations

- Blocking passes can generate MANY candidate pairs. The `JAROWINKLER_SIMILARITY > 70` pre-filter in the WHERE clause is critical.
- If `PROB_CANDIDATES` DT has >10M rows, consider tightening blocking keys (e.g., 3-char last name prefix instead of 2).
- The `CROSS JOIN MATCH_WEIGHTS_CONFIG` is cheap (1 row) but ensure `is_active = TRUE` filter is present.
- All scoring functions (JAROWINKLER_SIMILARITY, EDITDISTANCE) are deterministic built-in functions — they are incremental-refresh compatible.
