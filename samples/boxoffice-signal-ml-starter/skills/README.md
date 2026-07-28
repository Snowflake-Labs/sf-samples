# Skills — sanitized, installable CoCo skills

These are portable versions of the skills used to build the original pipeline, with all
provider names, API keys, personal info, and internal database identifiers removed and
replaced by placeholders. Each captures the **method**, not anyone's data.

## Install a skill into Cortex Code Desktop

1. Copy a skill folder (e.g. `search-interest-normalize/`) into your CoCo skills directory
   (`~/.snowflake/cortex/skills/` on macOS), or open this repo in CoCo and ask:
   > "Install the skill in `skills/search-interest-normalize/` for me."
2. Fill in placeholders the first time you use it:
   - `{{SANDBOX_DB}}`, `{{SCHEMA}}` — your sandbox (docs/02).
   - `{{SEARCH_INTEREST_LIB}}`, `{{ENTITY_LOOKUP_API}}`, `{{COMMENT_PLATFORM}}`,
     `{{PAGEVIEW_API}}`, `{{BOXOFFICE_SOURCE}}`, `{{METADATA_API}}` — the source you choose
     for each signal, which CoCo helps you compare using the `sources/` dossiers (docs/03).
   - Keys come from your git-ignored `.env`.

## What's here

| Skill | Purpose | Dossier |
|---|---|---|
| `search-interest-normalize` | Pull + normalize a search/attention demand signal | Source A |
| `comment-ingest-score` | Ingest trailer comments, pseudonymize, AISQL sentiment/intent | Source B |
| `research-pageviews` | Pull daily pageviews, build demand percentiles | Source C |
| `box-office-history` | Obtain the label via **licensed/official** access + validation | Source D |
| `pipeline-refresh` | Orchestrate the active-window refresh across all signals | all |

## Sanitization guarantee

None of these files contain real API keys, tokens, account locators, emails, provider
names, or internal database/schema/table names. If you find one, it's a bug — please open
an issue. See the repo's pre-push scan in `docs/03`/the project notes.
