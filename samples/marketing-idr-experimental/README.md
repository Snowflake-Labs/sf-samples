# Marketing IDR Experimental

A pure SQL identity resolution pipeline for Snowflake — deterministic + probabilistic matching with incremental Dynamic Tables, staged graph resolution, and field-level survivorship.

## What This Contains

This is a Cortex Code skill (`marketing-identity-resolution`) that builds a first-party identity resolution pipeline on any set of source tables in Snowflake. It discovers source schemas, maps columns to canonical identity fields, and generates a fully incremental pipeline.

## Pipeline Architecture

```
Source Tables → Standardization (DT per source) → Identity Signals Union →
Deterministic Edges → Probabilistic Edges (multi-pass blocking + scoring) →
Staged Graph Resolution (SP + Task) → Keyring → Golden Record
```

## Key Features

- **Incremental by default**: All layers use `REFRESH_MODE = INCREMENTAL` Dynamic Tables except graph resolution (which uses a Stream + Task)
- **Staged resolution**: Deterministic matching runs first, then probabilistic only operates on unresolved singletons
- **Configurable scoring**: Weights stored in a config table — tunable without SQL changes
- **Multi-pass blocking**: ZIP+name, DOB+SOUNDEX, phone+initial, email+city (adapts to available fields)
- **Field-level survivorship**: Golden record picks the best value per field, not best record
- **Guardrails**: Toxic identifier blacklist, cluster size caps, household email protection, weak signal corroboration

## Files

| File | Purpose |
|------|---------|
| `SKILL.md` | Main workflow definition for Cortex Code |
| `references/column-inference-patterns.md` | Column name → canonical field mapping rules |
| `references/deterministic-matching.md` | SQL templates for exact-match edge generation |
| `references/probabilistic-scoring.md` | Multi-pass blocking + composite scoring templates |
| `references/graph-resolution.md` | Staged iterative label propagation SP |
| `references/golden-record-survivorship.md` | FIRST_VALUE IGNORE NULLS survivorship logic |
| `references/dynamic-tables-guide.md` | Incremental DT constraints for IDR |
| `references/observability.md` | Pipeline metrics and health check views |

## Usage

### With Cortex Code

Install the skill to `~/.snowflake/cortex/skills/marketing-identity-resolution/` and invoke it with prompts like:
- "Build identity resolution on my customer data"
- "Deduplicate customers across CRM and web"
- "Create a golden record from my source tables"

### Without Cortex Code

Use the SQL templates in the `references/` directory directly. The key components are:

1. **Standardization**: Create one Dynamic Table per source that normalizes identifiers
2. **Edge generation**: Self-join on shared identifiers (email, phone, loyalty ID, etc.)
3. **Graph resolution**: Run the stored procedure in `references/graph-resolution.md`
4. **Golden record**: Use the FIRST_VALUE IGNORE NULLS pattern in `references/golden-record-survivorship.md`

## Requirements

- Snowflake account with Dynamic Tables enabled
- 2+ source tables with customer/identity data
- A warehouse for DT refresh (MEDIUM recommended)
- Write permissions on target schema

## Related

- [Snowflake Dynamic Tables](https://docs.snowflake.com/en/user-guide/dynamic-tables-about)
- [JAROWINKLER_SIMILARITY](https://docs.snowflake.com/en/sql-reference/functions/jarowinkler_similarity)
- [EDITDISTANCE](https://docs.snowflake.com/en/sql-reference/functions/editdistance)
