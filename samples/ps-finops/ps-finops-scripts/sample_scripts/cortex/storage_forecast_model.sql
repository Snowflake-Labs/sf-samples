DECLARE
res1 resultset;
res2 resultset;
res3 resultset;

BEGIN
   --Update model data:
   res1 := (
           EXECUTE IMMEDIATE $$ CREATE
               OR replace SNOWFLAKE.ML.FORECAST STORAGE_FORECAST_MODEL (
               INPUT_DATA => SYSTEM$QUERY_REFERENCE('select usage_date::TIMESTAMP_NTZ AS usage_date
   , (storage_bytes+stage_bytes+failsafe_bytes+hybrid_table_storage_bytes)/power(1024,3)::FLOAT as STORAGE_GB
from snowflake.account_usage.storage_usage
where usage_date > current_date()-60
and usage_date < current_date()
'),
               TIMESTAMP_COLNAME => 'USAGE_DATE',
               TARGET_COLNAME => 'STORAGE_GB'
               );$$
           );

   CALL STORAGE_FORECAST_MODEL ! FORECAST(FORECASTING_PERIODS => 60, CONFIG_OBJECT => { 'prediction_interval' : 0.9 });

   --res2 := (EXECUTE IMMEDIATE 'select * from table(result_scan(last_query_id()))');
   --Combine your actual data with the forecasted data:
   res3 := (
           EXECUTE IMMEDIATE $$
           SELECT usage_date::TIMESTAMP_NTZ AS usage_date,
               (storage_bytes + stage_bytes + failsafe_bytes + hybrid_table_storage_bytes) / power(1024, 3)::FLOAT AS STORAGE_GB,
               NULL AS forecast,
               NULL AS lower_bound,
               NULL AS upper_bound
           FROM snowflake.account_usage.storage_usage
           WHERE usage_date > CURRENT_DATE () - 31
               AND usage_date < CURRENT_DATE ()
          
           UNION ALL
          
           SELECT ts,
               NULL AS actual,
               forecast,
               lower_bound,
               upper_bound
           FROM TABLE (RESULT_SCAN(last_query_id())) $$
           );

   RETURN TABLE (res3);
END;