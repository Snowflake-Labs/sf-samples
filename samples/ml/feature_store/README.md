# Online Feature Tables

Online Feature Tables provide low‑latency, key‑based feature retrieval for real‑time ML inference. They continuously ingest feature data from offline sources (Views or Dynamic Tables) and are fully managed by Snowflake for scaling, synchronization, and maintenance.

## Key characteristics

- **Low-latency reads**: Optimized for point lookups by entity keys.
- **Fresh data**: Background refresh keeps online data in sync with offline sources.
- **Fully managed**: Snowflake handles sync, compaction, and maintenance.
- **Scalable**: Built on Snowflake’s elastic infrastructure.

## Data freshness and refresh

- **Target lag**: Controls desired data propagation delay (min 10s, max 8d). Scheduler may run slightly earlier than the lag to meet freshness goals.
- **Refresh modes**: `INCREMENTAL` (preferred), `FULL`, or `AUTO` (let Snowflake choose). Incremental is generally more cost‑ and compute‑efficient.
- **Deduplication**: Optional `timestamp_col` ensures the most recent row per primary key is kept; otherwise, the last processed row wins.

## Quick start (Python)

```python
from snowflake.ml.feature_store import FeatureStore, OnlineConfig
from snowflake.ml.feature_store.entities import FeatureView

fs = FeatureStore()

fv = FeatureView(
    name="MY_FV",
    entities=[...],              # one or more entities with join keys
    feature_df=my_df,            # Snowpark DataFrame with feature transforms
    timestamp_col="ts",          # optional
    refresh_freq="5 minutes",    # offline features refresh cadence
    refresh_mode="AUTO",         # AUTO | FULL | INCREMENTAL
    desc="example feature view",
    online_config=OnlineConfig(enabled=True, target_lag="30 seconds"),
)

# Read online features for one or more keys
from snowflake.ml.feature_store import StoreType
fs.read_feature_view(
    feature_view=fv,
    version="v1",
    keys=[["<k1>"]],
    feature_names=["f1", "f2"],
    store_type=StoreType.ONLINE,
)

# Manage lifecycle
fs.suspend_feature_view(feature_view=fv, version="v1")
fs.resume_feature_view(feature_view=fv, version="v1")
fs.refresh_feature_view(feature_view=fv, version="v1", store_type=StoreType.ONLINE)
fs.get_refresh_history(feature_view=fv, version="v1", store_type=StoreType.ONLINE)
```

## Quick start (SQL)

```sql
-- Create an online feature table backed by a view or dynamic table
CREATE OR REPLACE ONLINE FEATURE TABLE MY_ONLINE_FT
  PRIMARY KEY (ID)
  TARGET_LAG = '30 seconds'
  WAREHOUSE = MY_WH
  REFRESH_MODE = AUTO
  TIMESTAMP_COLUMN = TS
FROM MY_SOURCE_DYNAMIC_TABLE;

-- Operational controls
ALTER ONLINE FEATURE TABLE MY_ONLINE_FT SUSPEND;
ALTER ONLINE FEATURE TABLE MY_ONLINE_FT RESUME;
ALTER ONLINE FEATURE TABLE MY_ONLINE_FT REFRESH;
```

## Access control (high level)

- To create: `CREATE ONLINE FEATURE TABLE` on schema, `USAGE` on database/schema/warehouse, and `SELECT` on the source.
- To query: `USAGE` on database/schema/warehouse and `SELECT` on the online feature table.
- To operate/alter: `OPERATE` (or `OWNERSHIP`).
- To view metadata: `MONITOR`.

## Notes

- Online feature tables are optimized for SELECT point lookups by primary key. Complex queries (e.g., `ORDER BY`, `GROUP BY`) may have higher latency.
- Consider avoiding adaptive warehouses for online lookups.
- Monitor refreshes via `INFORMATION_SCHEMA.ONLINE_FEATURE_TABLE_REFRESH_HISTORY` and the Account Usage view.
