# EXECUTE_SNOWFLAKE_QUERIES_PARALLELLY
EXECUTE_SNOWFLAKE_QUERIES_PARALLELLY
# Idea Submitted  more than year back

![alt text](idea.png)

# Step 1: SNOWPARK CODE TO BE DEPLOYED AS PROCEDURE (EXEC IN PYTHON WORKSHEET)

https://www.youtube.com/watch?v=RGiB1cRMzc0

![alt text](PARALLEL-Demo.gif)


```python

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

```

## CODE TO TRY (EXEC IN SQL WORKSHEET)

# Step 2

```sql

CREATE OR REPLACE PROCEDURE SP_PARALLEL_1("P_LOAD_DATE" TIMESTAMP_NTZ(9))
RETURNS VARCHAR(16777216)
LANGUAGE SQL
EXECUTE AS OWNER
AS DECLARE 
V_SP_NAME VARCHAR(50):='SP_PARALLEL_1';
ERR_MSG VARCHAR(500);
ERR_STEP VARCHAR(10);

BEGIN


create or replace TABLE PARALLEL_1 (
	SR_NO NUMBER(38,0),
	INSRT_TMS TIMESTAMP_NTZ(9)
);


CALL SYSTEM$WAIT(30);
INSERT INTO PARALLEL_1  (SELECT seq4()+1 SR_NO, CURRENT_TIMESTAMP FROM TABLE(GENERATOR(ROWCOUNT => 100)) T  ORDER BY 1);

 
RETURN :V_SP_NAME|| 'EXECUTED SUCCESSFULLY';
EXCEPTION
WHEN OTHER THEN
                       
RETURN 'ERROR IN CODE';                            
RAISE;                            
    
END;




CREATE OR REPLACE PROCEDURE SP_PARALLEL_2("P_LOAD_DATE" TIMESTAMP_NTZ(9))
RETURNS VARCHAR(16777216)
LANGUAGE SQL
EXECUTE AS OWNER
AS DECLARE 
V_SP_NAME VARCHAR(50):='SP_PARALLEL_2';
ERR_MSG VARCHAR(500);
ERR_STEP VARCHAR(10);

BEGIN


create or replace TABLE PARALLEL_2 (
	SR_NO NUMBER(38,0),
	INSRT_TMS TIMESTAMP_NTZ(9)
);
CALL SYSTEM$WAIT(30);
INSERT INTO PARALLEL_2  (SELECT seq4()+1 SR_NO, CURRENT_TIMESTAMP FROM TABLE(GENERATOR(ROWCOUNT => 100)) T  ORDER BY 1);

 
RETURN :V_SP_NAME|| 'EXECUTED SUCCESSFULLY';
EXCEPTION
WHEN OTHER THEN
                       
RETURN 'ERROR IN CODE';                            
RAISE;                            
    
END;







CREATE OR REPLACE PROCEDURE SP_PARALLEL_3("P_LOAD_DATE" TIMESTAMP_NTZ(9))
RETURNS VARCHAR(16777216)
LANGUAGE SQL
EXECUTE AS OWNER
AS DECLARE 
V_SP_NAME VARCHAR(50):='SP_PARALLEL_3';
ERR_MSG VARCHAR(500);
ERR_STEP VARCHAR(10);

BEGIN


create or replace TABLE PARALLEL_3 (
	SR_NO NUMBER(38,0),
	INSRT_TMS TIMESTAMP_NTZ(9)
);


CALL SYSTEM$WAIT(30);
INSERT INTO PARALLEL_3  (SELECT seq4()+1 SR_NO, CURRENT_TIMESTAMP FROM TABLE(GENERATOR(ROWCOUNT => 100)) T  ORDER BY 1);

 
RETURN :V_SP_NAME|| 'EXECUTED SUCCESSFULLY';
EXCEPTION
WHEN OTHER THEN
                       
RETURN 'ERROR IN CODE';                            
RAISE;                            
    
END;




CREATE OR REPLACE PROCEDURE SP_PARALLEL_4("P_LOAD_DATE" TIMESTAMP_NTZ(9))
RETURNS VARCHAR(16777216)
LANGUAGE SQL
EXECUTE AS OWNER
AS DECLARE 
V_SP_NAME VARCHAR(50):='SP_PARALLEL_4';
ERR_MSG VARCHAR(500);
ERR_STEP VARCHAR(10);

BEGIN


create or replace TABLE PARALLEL_4 (
	SR_NO NUMBER(38,0),
	INSRT_TMS TIMESTAMP_NTZ(9)
);


CALL SYSTEM$WAIT(30);
INSERT INTO PARALLEL_4  (SELECT seq4()+1 SR_NO, CURRENT_TIMESTAMP FROM TABLE(GENERATOR(ROWCOUNT => 100)) T  ORDER BY 1);

 
RETURN :V_SP_NAME|| 'EXECUTED SUCCESSFULLY';
EXCEPTION
WHEN OTHER THEN
                       
RETURN 'ERROR IN CODE';                            
RAISE;                            
    
END;


CREATE OR REPLACE PROCEDURE SP_PARALLEL_5("P_LOAD_DATE" TIMESTAMP_NTZ(9))
RETURNS VARCHAR(16777216)
LANGUAGE SQL
EXECUTE AS OWNER
AS DECLARE 
V_SP_NAME VARCHAR(50):='SP_PARALLEL_5';
ERR_MSG VARCHAR(500);
ERR_STEP VARCHAR(10);

BEGIN


create or replace TABLE PARALLEL_5 (
	SR_NO NUMBER(38,0),
	INSRT_TMS TIMESTAMP_NTZ(9)
);


CALL SYSTEM$WAIT(30);
INSERT INTO PARALLEL_5  (SELECT seq4()+1 SR_NO, CURRENT_TIMESTAMP FROM TABLE(GENERATOR(ROWCOUNT => 100)) T  ORDER BY 1);

 
RETURN :V_SP_NAME|| 'EXECUTED SUCCESSFULLY';
EXCEPTION
WHEN OTHER THEN
                       
RETURN 'ERROR IN CODE';                            
RAISE;                            
    
END;
```
# Step 3

```sql

CALL EXECUTE_QUERIES_PARALLEL([
    'CALL SP_PARALLEL_1(CURRENT_TIMESTAMP)',
    'CALL SP_PARALLEL_2(CURRENT_TIMESTAMP)',
    'CALL SP_PARALLEL_3(CURRENT_TIMESTAMP)',
    'CALL SP_PARALLEL_4(CURRENT_TIMESTAMP)',
    'CALL SP_PARALLEL_5(CURRENT_TIMESTAMP)'
    ]);
```
