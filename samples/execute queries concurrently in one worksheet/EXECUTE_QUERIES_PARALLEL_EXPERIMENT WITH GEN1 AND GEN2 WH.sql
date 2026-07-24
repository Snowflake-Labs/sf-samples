----CREATE RANDOM TABLE-----
CREATE DATABASE MH;
CREATE SCHEMA SCH;


ALTER ACCOUNT SET  PYTHON_SNOWPARK_ENABLE_THREAD_SAFE_SESSION=true;

CREATE OR REPLACE PROCEDURE create_random_table()
RETURNS STRING
LANGUAGE SQL
AS
DECLARE
    random_num_cols NUMBER := UNIFORM(800, 800, RANDOM());      -- CAN BE ADJUSTED EXAMPLE 100–800 columns
    random_num_rows NUMBER := UNIFORM(100000, 100000, RANDOM()); --CAN BE ADJUSTED EXAMPLE 10K–100K rows
    column_definitions STRING;
    column_expressions STRING;
    table_name STRING := 'RANDOM_TABLE_' || UNIFORM(1, 10000000, RANDOM());
BEGIN
    -- Generate column names and corresponding expressions with different types
    SELECT
        LISTAGG('col_' || seq4 || ' ' || 
            CASE MOD(seq4, 6)
                WHEN 0 THEN 'NUMBER'
                WHEN 1 THEN 'VARCHAR'
                WHEN 2 THEN 'FLOAT'
                WHEN 3 THEN 'NUMBER'
                WHEN 4 THEN 'FLOAT'
                WHEN 5 THEN 'VARCHAR'
            END, ', ') 
            WITHIN GROUP (ORDER BY seq4),
        
        LISTAGG(
            CASE MOD(seq4, 6)
                WHEN 0 THEN 'SEQ4()'
                WHEN 1 THEN 'UUID_STRING()'
                WHEN 2 THEN 'RANDOM()'
                WHEN 3 THEN 'UNIFORM(1, 100, RANDOM())'
                WHEN 4 THEN 'NORMAL(50, 10, RANDOM())'
                WHEN 5 THEN 'RANDSTR(10, RANDOM())'
            END, ', ') 
            WITHIN GROUP (ORDER BY seq4)
    INTO
        column_definitions,
        column_expressions
    FROM (
        SELECT SEQ4() AS seq4
        FROM TABLE(GENERATOR(ROWCOUNT => :random_num_cols))
    );

    -- Create the table
    EXECUTE IMMEDIATE 'CREATE OR REPLACE TABLE ' || table_name || ' (' || column_definitions || ');';

    -- Insert synthetic data
    EXECUTE IMMEDIATE 'INSERT INTO ' || table_name || 
                      ' SELECT ' || column_expressions || 
                      ' FROM TABLE(GENERATOR(ROWCOUNT => ' || random_num_rows || '));';

    RETURN '✅ Table "' || table_name || '" created with ' || random_num_cols || ' columns and ' || random_num_rows || ' rows.';
END;





CALL create_random_table();





    SELECT '''DROP TABLE ' || table_name || ''',' AS drop_statement
    FROM INFORMATION_SCHEMA.TABLES
    WHERE table_name LIKE 'RANDOM%';





SELECT GET_DDL('WAREHOUSE', 'COMPUTE_WH');




create or replace warehouse COMPUTE_GEN1_WH
with
	warehouse_type='STANDARD'
	resource_constraint='STANDARD_GEN_1'
	warehouse_size='X-Small'
	max_cluster_count=1
	min_cluster_count=1
	scaling_policy=STANDARD
	auto_suspend=600
	auto_resume=TRUE
	initially_suspended=TRUE
	COMMENT='GEN1'
	enable_query_acceleration=FALSE
	query_acceleration_max_scale_factor=8
	max_concurrency_level=8
	statement_queued_timeout_in_seconds=0
	statement_timeout_in_seconds=172800
;





create or replace warehouse COMPUTE_GEN2_WH
with
	warehouse_type='STANDARD'
	resource_constraint='STANDARD_GEN_2'
	warehouse_size='X-Small'
	max_cluster_count=1
	min_cluster_count=1
	scaling_policy=STANDARD
	auto_suspend=600
	auto_resume=TRUE
	initially_suspended=TRUE
	COMMENT='GEN2'
	enable_query_acceleration=FALSE
	query_acceleration_max_scale_factor=8
	max_concurrency_level=8
	statement_queued_timeout_in_seconds=0
	statement_timeout_in_seconds=172800
;
 












----EXECUTE_QUERIES_PARALLEL

CREATE OR REPLACE PROCEDURE MH.SCH.EXECUTE_QUERIES_PARALLEL("QUERIES" ARRAY)
RETURNS TABLE ()
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'execute_queries_parallel'
COMMENT='EXECUTE_QUERIES_PARALLEL'
EXECUTE AS OWNER
AS '
import snowflake.snowpark as snowpark
from snowflake.snowpark.types import StructType, StructField, StringType, DoubleType
import concurrent.futures
import time

def execute_queries_parallel(session: snowpark.Session, queries: list):
    """
    Author: MAHANTESH HIREMATH
    Executes multiple SQL queries in parallel using ThreadPoolExecutor with exception handling.

    Parameters:
        session (snowpark.Session): Snowflake Snowpark session object.
        queries (list): List of SQL queries.

    Returns:
        Snowpark DataFrame: A DataFrame containing query text, execution time, and result (or error message).
    """
    if not isinstance(queries, list) or len(queries) == 0:
        raise ValueError("Input must be a non-empty list of SQL queries.")

    results = []

    def run_query(query):
        start_time = time.time()
        try:
            df = session.sql(query)  # Execute query
            result_data = df.collect()  # Collect result
            execution_time = round(time.time() - start_time, 3)
            return (query, str(result_data), execution_time)  # Convert result to string
        except Exception as e:
            execution_time = round(time.time() - start_time, 3)
            return (query, f"ERROR: {str(e)}", execution_time)  # Capture error message

    # Execute queries in parallel
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = {executor.submit(run_query, query): query for query in queries}
        for future in concurrent.futures.as_completed(futures):
            results.append(future.result())

    # Convert results to Snowpark DataFrame
    schema = StructType([
        StructField("query", StringType()),
        StructField("result", StringType()),  # Store result or error message as a string
        StructField("execution_time_seconds", DoubleType()),
    ])
    
    df_result = session.create_dataframe(results, schema)
    return df_result
';



CALL EXECUTE_QUERIES_PARALLEL([
 'CALL create_random_table()',
 'CALL create_random_table()']);
 
 
 
 ----SELECT * FROM  snowflake.account_usage.query_history qh
where qh.query_id in ('01bc3242-0205-2fd0-0000-0008974fb0b9',---EXECUTE_QUERIES_PARALLEL WITH GEN1
'01bc3248-0205-2de7-0000-0008974fa125',------EXECUTE_QUERIES_PARALLEL WITH GEN2
'01bc3289-0205-2c62-0008-974f00010092',----ASYNC WITH GEN1
'01bc328a-0205-2de7-0008-974f0001510e'---ASYNC WITH GEN2

);


 Select QAH.QUERY_ID,
 QAH.WAREHOUSE_NAME,QAH.CREDITS_ATTRIBUTED_COMPUTE,QAH.CREDITS_ATTRIBUTED_COMPUTE*3 AS DOLLAR_SPEND,
 DATEDIFF('MINUTE',QAH.START_TIME,QAH.END_TIME) DURATION_EXECUTION_IN_MINUTE
 from  SNOWFLAKE.ACCOUNT_USAGE.QUERY_ATTRIBUTION_HISTORY QAH
 WHERE QAH.QUERY_ID in ('01bc3242-0205-2fd0-0000-0008974fb0b9',---EXECUTE_QUERIES_PARALLEL WITH GEN1
'01bc3248-0205-2de7-0000-0008974fa125'------EXECUTE_QUERIES_PARALLEL WITH GEN2
)

