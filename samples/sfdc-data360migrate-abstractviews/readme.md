# sfdc-data360migrate-abstractviews

A Cortex Code skill for safely swapping two Snowflake databases with three operational modes.

## Modes

| Mode | Use Case |
|------|----------|
| **A — Name Swap** | Both databases are writable. Swaps names via three-step rename with object sync and grant mirroring. |
| **B — Read-Only Dependency Redirect** | Databases are read-only (imported/shared). Recreates dependent views in a writable L1 database to reference the new target. |
| **C — Zerocopy Swap** | One database is an imported datashare, the other is a catalog-linked database (CLD). Uses `SYSTEM$ZEROCOPY_SWAP_IMPORTED_DB_WITH_CLD` for an atomic swap. |

## Features

- Pre-flight checks (database existence, temp name availability)
- Schema mismatch detection with mandatory user confirmation
- Object audit and sync (tables, views, sequences, procedures, functions)
- Grant mirroring with user approval before execution
- Post-swap verification and query replay validation
- Rollback instructions provided for all modes

## Usage

Install as a Cortex Code skill and invoke with:

```
$sfdc-data360migrate-abstractviews swap databases DB1 and DB2
```

Or for specific modes:

```
$sfdc-data360migrate-abstractviews redirect views from OLD_SHARE_DB to NEW_CLD_DB
$sfdc-data360migrate-abstractviews zerocopy swap IMPORTED_DB with CLD_DB
```

## Prerequisites

- Role: ACCOUNTADMIN (or SYSADMIN with MANAGE GRANTS)
- Both databases must exist
- For Mode C: one database must be an imported datashare and the other a catalog-linked database

## Author

Chandra Nayak (Snowflake)
