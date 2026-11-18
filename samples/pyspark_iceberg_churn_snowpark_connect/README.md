# PySpark on Apache Iceberg with Snowpark Connect for Spark

Companion code for the Quickstart [Run PySpark on Apache Iceberg Tables with Snowpark Connect for Spark](https://www.snowflake.com/en/developers/guides/pyspark-iceberg-churn-snowpark-connect/).

StreamCo, a fictional streaming company, runs its PySpark churn pipeline unchanged on Snowflake with Snowpark Connect for Spark. Every layer is a Snowflake-managed Apache Iceberg table.

The notebook covers:

- Bronze, silver and gold layers with DataFrameWriterV2
- Day partitions, `overwritePartitions()` and table properties
- Iceberg tags and time travel by tag, snapshot ID and timestamp
- Incremental reads, the changelog and targeted overwrites
- Write-Audit-Publish on Iceberg branches (preview)
- Lineage with Iceberg metadata tables

## Files

- **setup.sql**: role, grants, warehouse, database, schemas and stage
- **pyspark_iceberg_churn_snowpark_connect.ipynb**: the notebook

## Run it

1. Run **setup.sql** as ACCOUNTADMIN, replacing `<your_username>`.
2. In Snowsight, go to **Projects** > **Workspaces** and upload the notebook.
3. Connect it to a service as **SCOS_CHURN_ROLE**, with artifact repository `snowflake.snowpark.pypi_shared_repository`.
4. Run the install cell, restart the kernel, then run the remaining cells.

Clean-up statements are at the end of **setup.sql**.
