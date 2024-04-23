--Tagging Task to run every night at midnight CST

CREATE TASK LLM_QUERY_TAG_TASK
  WAREHOUSE = <MY_TASK_WH>
  SCHEDULE = 'USING CRON 0 0 * * * America/Chicago'
  AS
  INSERT INTO <MY_CUSTOM_TAG_TABLE>
  SELECT
        QUERY_ID,
        -- QUERY_TEXT,
        SNOWFLAKE.CORTEX.COMPLETE(
            'mixtral-8x7b',
            CONCAT(
                'You are a Snowflake SQL Expert who responds ONLY with tag names from the list provided. You do not use any filler words or the word "sure". 
                 Here are the categories for tagging Snowflake queries:
                    1. Data Management
                    2. Data Analysis & Reporting
                    3. Data Transformation & Processing
                    4. Performance & Testing
                    5. Compliance & Security
            ONLY respond with Data Management, Data Analysis & Reporting, Data Transformation & Processing, Performance & Testing, or Compliance & Security.
                    <query>',QUERY_TEXT,'</query>'
            )
        ) as LLM_QUERY_TAG
    FROM
        SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
        WHERE WAREHOUSE_NAME = 'TASTY_DE_WH'
        AND START_TIME >= DATEADD(day,-1,current_date());