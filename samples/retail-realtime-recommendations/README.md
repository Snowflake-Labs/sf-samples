# Retail Real-Time Recommendations with Snowflake

Companion code for the [Build Real-Time Product Recommendations for Retail with Snowflake](https://quickstarts.snowflake.com/guide/retail-realtime-recommendations) quickstart.

## What's Inside

| File / Directory | Description |
|---|---|
| `notebooks/01_train_and_register.ipynb` | Snowflake Notebook: Feature Store setup, XGBoost model training (propensity + ranking), Model Registry, SPCS deployment |
| `orchestrator/app.py` | FastAPI orchestrator service — fetches data from Snowflake Postgres in parallel, calls ranking model, returns top-N recommendations |
| `orchestrator/Dockerfile` | Docker build file for the orchestrator container |
| `orchestrator/requirements.txt` | Python dependencies for the orchestrator |
| `load_pg.py` | Python script to export data from Snowflake and load it into Snowflake Postgres for low-latency serving |
| `teardown/cleanup.sql` | SQL to drop all Snowflake objects created by the quickstart |

## Architecture

The solution uses a **hybrid batch + real-time** architecture:

- **Batch path (daily):** Customer 360 Dynamic Table → Feature Store → XGBoost propensity model → scores stored in `GOLD.CUSTOMER_PROPENSITY`
- **Real-time path (per request):** Orchestrator fetches customer features, propensity scores, and inventory from Snowflake Postgres (~15ms), calls the SPCS ranking model, returns top-N recommendations (~78ms end-to-end)

## Prerequisites

- Snowflake Enterprise Edition account with SPCS and Snowflake Postgres enabled
- `ACCOUNTADMIN` role or equivalent privileges
- Docker (for building and pushing the orchestrator container)
- Python 3.11+ (for `load_pg.py`)

## Getting Started

Follow the full step-by-step quickstart at:
[https://quickstarts.snowflake.com/guide/retail-realtime-recommendations](https://quickstarts.snowflake.com/guide/retail-realtime-recommendations)

## License

Licensed under the [Apache License 2.0](../../LICENSE).
