# Overture Maps Addresses share

The geocoding solution reads a single Snowflake Marketplace share: the
**Overture Maps "Addresses"** dataset published by **Carto**. No external
geocoding APIs are used.

## Auto-acquisition (default)

`sql/00_setup.sql` acquires the listing for you — no manual Marketplace step:

```sql
CALL SYSTEM$ACCEPT_LEGAL_TERMS('DATA_EXCHANGE_LISTING', 'GZT0Z4CM1E9NQ');
CREATE DATABASE IF NOT EXISTS OVERTURE_MAPS__ADDRESSES FROM LISTING GZT0Z4CM1E9NQ;
```

- `GZT0Z4CM1E9NQ` is the free **Overture Maps - Addresses** (Carto) listing.
- The call is wrapped so a region where the listing is not auto-fulfillable does
  not abort setup; an already-installed copy (either name) is reused instead.
- The acquiring role needs `CREATE DATABASE` / `IMPORT SHARE`.

## Manual acquisition (fallback)

If auto-acquisition isn't available in your region:

1. In Snowsight: **Data Products -> Marketplace**, search **"Overture Maps
   Addresses"** (publisher: Carto), and **Get** it.
2. It installs as a shared database, named either:
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
