# Overture Maps Addresses share

The geocoding solution reads a single Snowflake Marketplace share: the
**Overture Maps "Addresses"** dataset published by **Carto**. No external
geocoding APIs are used.

## Acquiring the listing

1. In Snowsight: **Data Products -> Marketplace**, search **"Overture Maps
   Addresses"** (publisher: Carto), and **Get** it.
2. It installs as a shared database. Depending on the account / how it was
   imported, the database is named either:
   - `OVERTURE_MAPS__ADDRESSES` (no prefix), or
   - `DS_OVERTURE_MAPS__ADDRESSES` (some accounts add a `DS_` prefix).
3. Grant your working role access if needed:
   ```sql
   GRANT IMPORTED PRIVILEGES ON DATABASE OVERTURE_MAPS__ADDRESSES TO ROLE <role>;
   ```

## Why the installer never hard-codes the name

`sql/00_setup.sql` tries each candidate name in turn and creates a stable view:

```
GEOCODING.PUBLIC.OVERTURE_ADDRESS  ->  <resolved share>.CARTO.ADDRESS
```

Every UDF, procedure, and example reads `GEOCODING.PUBLIC.OVERTURE_ADDRESS`, so
the physical share name lives in exactly one place. If your account exposes the
share under a different name, add it to the `candidates` array in `00_setup.sql`.

## Table shape

`CARTO.ADDRESS` columns used by the solution:

| Column | Notes |
|--------|-------|
| `NUMBER` | house number (exact-match key for forward) |
| `STREET` | full street name (Overture spells out abbreviations) |
| `UNIT` | secondary unit (unused by matching) |
| `POSTCODE` | ZIP / postal code |
| `POSTAL_CITY` | ~38% of US rows are NULL — matching also allows postcode |
| `COUNTRY` | ISO code; forward filters `= 'US'`, reverse optionally filters |
| `GEOMETRY` | `GEOGRAPHY` point; `ST_X`/`ST_Y` give lon/lat |
