# Cortex Search Batch Matching

Semantic probabilistic matching using Snowflake's Batch Cortex Search. Replaces or complements the traditional Jaro-Winkler blocking + scoring approach with Arctic embedding-based semantic similarity.

## When to Use Cortex Search Matching

Choose Cortex Search over Jaro-Winkler when:
- Data has many nickname variations (Jenny/Jennifer, Bob/Robert, Mike/Michael)
- Address abbreviations are common (Blvd/Boulevard, St/Street, Ave/Avenue)
- Phone/email format inconsistencies are heavy
- You want zero-maintenance matching (no regex rules, no lookup tables)
- Record count is > 2,000 (batch warmup overhead makes smaller batches less efficient)

Choose Jaro-Winkler instead when:
- Per-field score explainability is required (audit/compliance needs)
- Cost sensitivity is high (Cortex Search has serving + embedding costs beyond warehouse)
- Record count is < 2,000 (interactive Cortex Search API is faster for small batches)
- You need full control over weight tuning per field

## Pattern A: Full Probabilistic Layer Replacement

Replaces blocking passes entirely. The Cortex Search Service indexes all identity signals and finds semantically similar records via LATERAL join.

### Step 1: Add SEARCH_TEXT to Standardization

Each `STD_SOURCE_*` DT must include a concatenated search text column:

```sql
COALESCE(first_name, '') || ' ' || COALESCE(last_name, '') || ' | ' ||
COALESCE(email_clean, '') || ' | ' ||
COALESCE(phone_clean, '') || ' | ' ||
COALESCE(address_line_1, '') || ' ' || COALESCE(city, '') || ' ' ||
COALESCE(state, '') || ' ' || COALESCE(zip5, '')
AS search_text
```

The `IDENTITY_SIGNALS` union DT must also propagate this column.

### Step 2: Create Cortex Search Service

```sql
CREATE OR REPLACE CORTEX SEARCH SERVICE <schema>.IDENTITY_SEARCH_SERVICE
    ON search_text
    ATTRIBUTES source_system, source_id, first_name, last_name,
               email_clean, phone_clean, city, state, zip5
    WAREHOUSE = <warehouse>
    TARGET_LAG = '1 hour'
    EMBEDDING_MODEL = 'snowflake-arctic-embed-m-v1.5'
AS (
    SELECT
        search_text, source_system, source_id,
        first_name, last_name, email_clean, phone_clean,
        city, state, zip5
    FROM <schema>.IDENTITY_SIGNALS
    WHERE search_text IS NOT NULL
      AND TRIM(search_text) != '| | |'
);
```

**Notes:**
- `TARGET_LAG = '1 hour'` keeps the index fresh as source data changes
- The WHERE clause excludes records with no usable identity fields
- ATTRIBUTES exposes fields for filtering and result enrichment

### Step 3: Batch Resolution Query

```sql
CREATE OR REPLACE DYNAMIC TABLE <schema>.PROBABILISTIC_EDGES_CSS
    TARGET_LAG = DOWNSTREAM
    WAREHOUSE = <warehouse>
    REFRESH_MODE = FULL
AS
WITH matches AS (
    SELECT
        src.source_system || '::' || src.source_id AS id_a,
        s."source_system" || '::' || s."source_id" AS id_b,
        s.METADATA$RANK AS css_confidence,
        src.first_name AS first_name_a,
        src.last_name AS last_name_a,
        s."first_name" AS first_name_b,
        s."last_name" AS last_name_b,
        src.email_clean AS email_a,
        s."email_clean" AS email_b
    FROM <schema>.IDENTITY_SIGNALS src,
    LATERAL CORTEX_SEARCH_BATCH(
        service_name => '<db>.<schema>.IDENTITY_SEARCH_SERVICE',
        query => src.search_text,
        limit => 3
    ) AS s
    WHERE src.source_system || '::' || src.source_id != s."source_system" || '::' || s."source_id"
)
SELECT
    LEAST(id_a, id_b) AS id_a,
    GREATEST(id_a, id_b) AS id_b,
    'PROBABILISTIC' AS match_type,
    'CORTEX_SEARCH' AS match_subtype,
    css_confidence AS confidence,
    CASE
        WHEN css_confidence >= 0.70 THEN 'ACCEPTED'
        WHEN css_confidence >= 0.40 THEN 'REVIEW'
        ELSE 'REJECTED'
    END AS match_status,
    OBJECT_CONSTRUCT(
        'css_rank', css_confidence,
        'name_a', first_name_a || ' ' || last_name_a,
        'name_b', first_name_b || ' ' || last_name_b,
        'email_match', (email_a = email_b AND email_a IS NOT NULL)
    ) AS field_scores
FROM matches
WHERE id_a != id_b
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY LEAST(id_a, id_b), GREATEST(id_a, id_b)
    ORDER BY css_confidence DESC
) = 1;
```

**Key design choices:**
- `limit => 3`: Returns top 3 candidates per record to catch near-misses
- `LEAST/GREATEST`: Normalizes edge direction for deduplication
- `QUALIFY ROW_NUMBER()`: Keeps only the best match per pair
- `REFRESH_MODE = FULL`: Required because CORTEX_SEARCH_BATCH is not incremental-compatible
- Self-match exclusion via WHERE clause

### Step 4: Cascading Filter Strategy (Optional, Higher Precision)

For large datasets, restrict matches progressively:

```sql
-- Pass 1: Strict (same state)
LATERAL CORTEX_SEARCH_BATCH(
    service_name => '<db>.<schema>.IDENTITY_SEARCH_SERVICE',
    query => src.search_text,
    filter => OBJECT_CONSTRUCT('@eq', OBJECT_CONSTRUCT('state', src.state)),
    limit => 3
) AS s

-- Pass 2: Relaxed (no filter, only for unresolved from pass 1)
-- Run on records that got no ACCEPTED match in pass 1
```

This reduces false positives at the expense of an additional pass for unresolved records.

### Step 5: Confidence Routing

| Confidence Band | METADATA$RANK | Action |
|----------------|---------------|--------|
| HIGH | ≥ 0.70 | Auto-merge (ACCEPTED) |
| MEDIUM | 0.40 – 0.70 | Human review (REVIEW) |
| LOW | < 0.40 | Reject / likely different person |

**Tuning:** These thresholds are based on typical identity concatenation behavior with Arctic embeddings. Adjust based on your data:
- If too many false positives: raise HIGH threshold to 0.75 or 0.80
- If too many missed matches: lower HIGH threshold to 0.65

### Step 6: Integration with All Edges

```sql
CREATE OR REPLACE DYNAMIC TABLE <schema>.ALL_EDGES
    TARGET_LAG = DOWNSTREAM
    WAREHOUSE = <warehouse>
    REFRESH_MODE = FULL
AS
SELECT id_a, id_b, match_type, match_type AS match_subtype, confidence
FROM <schema>.DETERMINISTIC_EDGES
UNION ALL
SELECT id_a, id_b, match_type, match_subtype, confidence
FROM <schema>.PROBABILISTIC_EDGES_CSS
WHERE match_status = 'ACCEPTED';
```

Note: When using Cortex Search matching, ALL_EDGES must use `REFRESH_MODE = FULL` since PROBABILISTIC_EDGES_CSS is FULL refresh.

---

## Pattern B: Hybrid (Blocking + Cortex Search Scoring)

Keep the traditional blocking passes to generate candidate pairs, but use Cortex Search to score them instead of the Jaro-Winkler weighted formula.

### When to Use Pattern B

- Dataset is very large (>10M records) and you need blocking for performance
- You want the candidate generation control of blocking passes
- But you want better scoring accuracy than Jaro-Winkler for the final match decision

### Implementation

#### Step 1: Generate blocking candidates (same as Jaro-Winkler approach)

Use the same blocking pass DTs from `probabilistic-scoring.md`:
- `PROB_CANDIDATES_ZIP_LASTNAME`
- `PROB_CANDIDATES_DOB_SOUNDEX`
- etc.

BUT remove the `JAROWINKLER_SIMILARITY > 70` pre-filter in the WHERE clause (let Cortex Search decide).

#### Step 2: Create a candidate-pair search table

```sql
CREATE OR REPLACE DYNAMIC TABLE <schema>.PROB_CANDIDATES_SEARCH_TEXT
    TARGET_LAG = DOWNSTREAM
    WAREHOUSE = <warehouse>
    REFRESH_MODE = INCREMENTAL
AS
WITH all_candidates AS (
    SELECT * FROM <schema>.PROB_CANDIDATES_PASS1
    UNION ALL
    SELECT * FROM <schema>.PROB_CANDIDATES_PASS2
),
deduped AS (
    SELECT *
    FROM all_candidates
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id_a, id_b ORDER BY blocking_strategy) = 1
)
SELECT
    d.id_a,
    d.id_b,
    d.blocking_strategy,
    COALESCE(d.first_name_a,'') || ' ' || COALESCE(d.last_name_a,'') || ' | ' ||
    COALESCE(d.email_a,'') || ' | ' || COALESCE(d.phone_a,'') || ' | ' ||
    COALESCE(d.addr_a,'') || ' ' || COALESCE(d.zip_a,'')
    AS search_text_a,
    COALESCE(d.first_name_b,'') || ' ' || COALESCE(d.last_name_b,'') || ' | ' ||
    COALESCE(d.email_b,'') || ' | ' || COALESCE(d.phone_b,'') || ' | ' ||
    COALESCE(d.addr_b,'') || ' ' || COALESCE(d.zip_b,'')
    AS search_text_b
FROM deduped d;
```

#### Step 3: Score via Cortex Search

Create a Cortex Search Service on the B-side identity text, then query with A-side text:

```sql
CREATE OR REPLACE CORTEX SEARCH SERVICE <schema>.CANDIDATE_SCORING_SERVICE
    ON search_text_b
    ATTRIBUTES id_b
    WAREHOUSE = <warehouse>
    TARGET_LAG = '1 hour'
    EMBEDDING_MODEL = 'snowflake-arctic-embed-m-v1.5'
AS (
    SELECT DISTINCT search_text_b, id_b
    FROM <schema>.PROB_CANDIDATES_SEARCH_TEXT
);
```

Then score:

```sql
SELECT
    c.id_a,
    s."id_b" AS id_b,
    'PROBABILISTIC' AS match_type,
    'CORTEX_SEARCH_HYBRID' AS match_subtype,
    s.METADATA$RANK AS confidence,
    CASE
        WHEN s.METADATA$RANK >= 0.70 THEN 'ACCEPTED'
        WHEN s.METADATA$RANK >= 0.40 THEN 'REVIEW'
        ELSE 'REJECTED'
    END AS match_status
FROM <schema>.PROB_CANDIDATES_SEARCH_TEXT c,
LATERAL CORTEX_SEARCH_BATCH(
    service_name => '<db>.<schema>.CANDIDATE_SCORING_SERVICE',
    query => c.search_text_a,
    limit => 1
) AS s
WHERE s."id_b" = c.id_b;
```

**Note:** Pattern B is more complex and may not offer significant advantages over Pattern A for most use cases. Pattern A is recommended as the default Cortex Search approach.

---

## Performance Considerations

- **Warmup**: Cortex Search Batch spins up dedicated resources. First call takes ~60-180 seconds. Subsequent calls in the same session are fast (~100 QPS sustained).
- **Throughput**: ~100 queries/second sustained, independent of warehouse size.
- **Cost**: Three components — serving cost (index size × duration), query embedding cost (tokens), warehouse cost.
- **Refresh mode**: CORTEX_SEARCH_BATCH is not deterministic → DTs using it must be `REFRESH_MODE = FULL`.
- **Minimum batch**: For < 2,000 queries, the interactive Cortex Search API (Python/REST) is faster than batch due to warmup overhead.
- **Service suspension**: Batch search can query suspended services — no need to keep the service actively serving for batch-only workloads.

## Comparison: Cortex Search vs Jaro-Winkler

| Aspect | Jaro-Winkler | Cortex Search Batch |
|--------|-------------|---------------------|
| Nickname handling | ❌ (Jenny ≠ Jennifer) | ✅ (semantic) |
| Typo tolerance | Partial (edit distance) | ✅ (embedding similarity) |
| Per-field explainability | ✅ (individual field scores) | ❌ (single rank score) |
| Cost | Warehouse only | Serving + embedding + warehouse |
| Blocking needed | Yes (for performance) | Optional (filter param) |
| Min viable batch | Any size | 2,000+ records |
| DT refresh mode | INCREMENTAL | FULL only |
| Setup complexity | Low (pure SQL functions) | Medium (requires Cortex Search Service) |
| Maintenance | Weight tuning needed | Zero — just enrich source data |
| Scale ceiling | Blocking limits candidate pairs | Handles any scale natively |
