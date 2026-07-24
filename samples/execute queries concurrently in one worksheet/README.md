# Execute Snowflake Queries in Parallel

This project demonstrates how to execute multiple Snowflake queries concurrently using Snowpark and SQL procedures.

## Overview

The solution leverages Python's `ThreadPoolExecutor` for parallel execution of queries and Snowflake SQL procedures for database operations. This approach is useful for improving performance in scenarios involving multiple independent queries.

![Idea Illustration](Idea.jpg)

---

## Step 1: Deploy Snowpark Code as a Procedure

The following Python code defines a function to execute SQL queries in parallel. Deploy this code in a Python worksheet.

[Watch the Demo](https://www.youtube.com/watch?v=RGiB1cRMzc0)

![Demo GIF](PARALLEL-Demo.gif)

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

---

## Step 2: Create SQL Procedures

Define the following SQL procedures in your Snowflake environment. Each procedure performs independent operations.

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

---

## Step 3: Execute Procedures in Parallel

Use the following SQL command to execute the procedures concurrently:

```sql

CALL EXECUTE_QUERIES_PARALLEL([
    'CALL SP_PARALLEL_1(CURRENT_TIMESTAMP)',
    'CALL SP_PARALLEL_2(CURRENT_TIMESTAMP)',
    'CALL SP_PARALLEL_3(CURRENT_TIMESTAMP)',
    'CALL SP_PARALLEL_4(CURRENT_TIMESTAMP)',
    'CALL SP_PARALLEL_5(CURRENT_TIMESTAMP)'
    ]);
```

---

## Notes

- Ensure the `PYTHON_SNOWPARK_ENABLE_THREAD_SAFE_SESSION` parameter is set to `True` for thread-safe execution. By default, it is `False`.
- The procedures create separate tables and insert data independently.

![Thread-Safe Session Setting](https://github.com/user-attachments/assets/65444518-25da-423a-94f6-0e3c792e5f83)

# Comparison of Execution Times with Snowflake WH GEN 1 and GEN 2

https://medium.com/@mahantesh-hiremath/what-happens-when-1000-queries-hit-an-x-small-snowflake-warehouse-whats-gen1-vs-gen2-sf-wh-69a532585a65

![alt text](<1000 QUERIES FIRED ON X-SMAL WH.JPG>)

!