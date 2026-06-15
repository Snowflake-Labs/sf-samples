# Martech Deployment Runbook

## Prerequisites

- Snowflake account with `ACCOUNTADMIN` access (for first-time install)
- `snow` CLI configured with a connection profile (`snow connection list` to verify)
- Docker (only needed for `deploy_spcs.sh`)
- Python 3.11+ with `pip` (only needed for `--seed` mode)

## One-shot install (clean account)

```bash
cd use-cases/martech
bash deploy/deploy_all.sh --db MARTECH --connection <your-conn-name> --seed
```

This:

1. Creates database `MARTECH`, schemas (BRONZE/SILVER/GOLD/APP/CONFIG), warehouse (`MARTECH_WH`), role (`MARTECH_APP_ROLE`)
2. Deploys engine SQL into `MARTECH` (procs, UDF, tags, tables, seeds — all rewritten from `IDR_DEMO` → `MARTECH` qualifier on the fly via sed)
3. Deploys martech tables (bronze + 10 idr_core state/config tables + silver + gold dynamic table)
4. Seeds matching rules, source priority, LLM config, std rules
5. Deploys martech procedures (custom standardize, ML, LLM, reset)
6. Creates 4 bronze streams + IDR_INCREMENTAL_TASK (SUSPENDED)
7. Generates ~100K synthetic customers + ~1M events
8. Runs `SP_RUN_IDR_PIPELINE()` once to populate clusters

Total runtime on a Medium warehouse: ~20–30 minutes for the full --seed flow.

## Custom DB name

```bash
bash deploy/deploy_all.sh --db MARTECH_PROD --connection prod-conn
```

`config.yaml`'s `deployment_db: MARTECH` is the default; `--db` overrides it. No procedure hard-codes the DB name (NFR-1).

## Deploy SPCS service

After the SQL deploy succeeds:

```bash
bash deploy/deploy_spcs.sh \
    --db MARTECH \
    --connection aws-east1 \
    --registry sfsenorthamerica-account.registry.snowflakecomputing.com/MARTECH/app \
    --compute-pool MARTECH_POOL
```

Get the public URL:

```bash
snow sql -q "SHOW ENDPOINTS IN SERVICE MARTECH.APP.MARTECH_BACKEND;" --connection aws-east1
```

## Resume the incremental task

```bash
snow sql -q "ALTER TASK MARTECH.APP.IDR_INCREMENTAL_TASK RESUME;" --connection aws-east1
```

(Or use the Settings page in the deployed UI.)

## Reset the demo

```bash
snow sql -q "CALL MARTECH.APP.SP_RESET_IDR();" --connection aws-east1
```

This truncates IDR state and recreates streams to advance offsets past existing bronze rows. Bronze tables are preserved.

## Tear down

```bash
snow sql -q "DROP DATABASE IF EXISTS MARTECH CASCADE;" --connection aws-east1
snow sql -q "DROP SERVICE IF EXISTS MARTECH.APP.MARTECH_BACKEND;" --connection aws-east1
```

## Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| `Database 'IDR_DEMO' does not exist` during engine deploy | sed rewrite missed a literal | Check engine source file; rerun `deploy_all.sh` |
| `Object IDR_CORE_CLUSTER does not exist` after engine deploy | `03_martech_tables.sql` not run | Re-run `bash deploy/deploy_all.sh` (idempotent) |
| Empty `DT_CUSTOMER_PROFILE` after pipeline run | Bronze tables empty or task suspended | `--seed` mode or manually invoke `MartechDataGenerator` |
| LLM adjudication errors | Missing Cortex AI entitlement | Check `SHOW PARAMETERS LIKE 'CORTEX%'` and grant |
| Matching rules not firing | Rule columns reference fields not produced by std proc | Inspect `SP_CUSTOM_STANDARDIZE` body and STD_* tables |
