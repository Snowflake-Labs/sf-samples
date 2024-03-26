# Iceberg & Cortex demo
This demo will show you how you can easily use Snowflake Cortex LLM functions for AI-powered text analysis on Iceberg tables. A blog post with step-by-step instructions are available [here](https://medium.com/snowflake/how-to-use-llama-2-on-iceberg-tables-for-sentiment-analysis-with-snowflake-cortex-32c030a8102b).

1. Run through line 43 of demo.sql in your Snowflake SQL worksheet. External Volume setup instructions can be found [here](https://docs.snowflake.com/en/user-guide/tables-iceberg-configure-external-volume#configure-an-external-volume-for-amazon-s3).
2. Run through snowpark.ipynb cells, and stop after writing jan_df to the Iceberg table (save_as_table).
3. Continue running demo.sql through line 61.
4. Setup Snowflake Reader Account, which can be done through the Snowsight UI or with [SQL](https://docs.snowflake.com/en/user-guide/data-sharing-reader-create#ddl-for-reader-accounts).
5. Create a share to the reader account created in step 4, and add the Iceberg table to the share.
6. Run all cells in spark.ipynb.