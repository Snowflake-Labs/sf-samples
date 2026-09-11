
-- MediaWallah [[APP_NAME_CAP]] App

-------------------------------------------------------------------------------
-- 01_account_setup.sql
-- This script sets up the Consumer\'s account to install the Native App
-- Note: it takes some time to switch between ROLEs,
--       wait a few seconds before running the next lines after switching roles.
-------------------------------------------------------------------------------
--get current user
SET APP_USER = CURRENT_USER();

--set parameter for admin role
SET APP_ADMIN_ROLE = 'C_[[APP_CODE]]_APP_ADMIN';

--set parameter for warehouse
SET APP_WH = 'C_[[APP_CODE]]_APP_WH';

--create admin role
USE ROLE SECURITYADMIN;
CREATE ROLE IF NOT EXISTS IDENTIFIER($APP_ADMIN_ROLE) ;
ALTER ROLE IF EXISTS IDENTIFIER($APP_ADMIN_ROLE) SET COMMENT = '{"origin":"sf_ps_wls","name":"acf","version":{"major":1, "minor":3},"attributes":{"role":"consumer","component":"app_admin_role"}}';
GRANT ROLE IDENTIFIER($APP_ADMIN_ROLE) TO ROLE SYSADMIN;
GRANT ROLE IDENTIFIER($APP_ADMIN_ROLE) TO USER IDENTIFIER($APP_USER);

--grant privileges to admin role
USE ROLE ACCOUNTADMIN;
GRANT IMPORT SHARE ON ACCOUNT TO ROLE IDENTIFIER($APP_ADMIN_ROLE);
GRANT CREATE SHARE ON ACCOUNT TO ROLE IDENTIFIER($APP_ADMIN_ROLE);
GRANT MANAGE SHARE TARGET ON ACCOUNT TO ROLE IDENTIFIER($APP_ADMIN_ROLE);
GRANT CREATE DATABASE ON ACCOUNT TO ROLE IDENTIFIER($APP_ADMIN_ROLE);
GRANT CREATE WAREHOUSE ON ACCOUNT TO ROLE IDENTIFIER($APP_ADMIN_ROLE);
GRANT CREATE APPLICATION ON ACCOUNT TO ROLE IDENTIFIER($APP_ADMIN_ROLE);

USE ROLE IDENTIFIER($APP_ADMIN_ROLE);

CREATE OR REPLACE WAREHOUSE IDENTIFIER($APP_WH) WITH WAREHOUSE_SIZE = 'XSMALL' 
  WAREHOUSE_TYPE = 'STANDARD' 
  AUTO_SUSPEND = 300 
  AUTO_RESUME = TRUE 
  --MIN_CLUSTER_COUNT = 1 
  --MAX_CLUSTER_COUNT = 1 
  --SCALING_POLICY = 'STANDARD'
  COMMENT = '{"origin":"sf_ps_wls","name":"acf","version":{"major":1, "minor":3},"attributes":{"role":"consumer","component":"app_warehouse"}}'
;

-------------------------------------------------------------------------------
-- 02_create_helper_db.sql
-- This script creates a database that houses the input data for the application
-- and the helper stored procedure.
-------------------------------------------------------------------------------
--set parameter for admin role
SET APP_ADMIN_ROLE = 'C_[[APP_CODE]]_APP_ADMIN';

--set parameter for warehouse
SET APP_WH = 'C_[[APP_CODE]]_APP_WH';

--set parameter for helper db
SET HELPER_DB = 'C_[[APP_CODE]]_HELPER_DB';

USE ROLE IDENTIFIER($APP_ADMIN_ROLE);
USE WAREHOUSE IDENTIFIER($APP_WH);

--source data db/schemas
CREATE DATABASE IF NOT EXISTS IDENTIFIER($HELPER_DB);

USE DATABASE IDENTIFIER($HELPER_DB);
CREATE SCHEMA IF NOT EXISTS SOURCE;
CREATE SCHEMA IF NOT EXISTS PRIVATE; --to store wrapper stored procedures

-------------------------------------------------------------------------------
-- 03_create_create_log_share_procedure.sql
-- This procedure serves as a wrapper procedure that calls the app\'s LOG_SHARE_INSERT procedure.
-- In adddition it makes the necessary grants to the log/metric tables to the app
-- and creates the log share to the Provider.
-------------------------------------------------------------------------------

CREATE OR REPLACE PROCEDURE PRIVATE.CREATE_LOG_SHARE(app_name VARCHAR, app_code VARCHAR, provider_locator VARCHAR)
  RETURNS VARCHAR
  LANGUAGE JAVASCRIPT
  COMMENT = '{"origin":"sf_ps_wls","name":"acf","version":{"major":1, "minor":3},"attributes":{"role":"consumer","component":"helper_sproc_create_log_share}}'
  EXECUTE AS CALLER
  AS
  $$
    
    //get consumer_name
    var rset = snowflake.execute({sqlText: `SELECT consumer_name FROM ${APP_NAME}.UTIL_APP.METADATA_C_V;`});
    rset.next();
    var consumer_name = rset.getColumnValue(1);
    
    var timeout = 300000; //milliseconds - 5 min timeout

    try {

      snowflake.execute({sqlText:`USE ROLE C_[[APP_CODE]]_APP_ADMIN;`});

      //create app share DB
      snowflake.execute({sqlText:`CREATE OR REPLACE DATABASE [[APP_CODE]]_APP_SHARE;`});

      //create log share schema
      snowflake.execute({sqlText:`CREATE OR REPLACE SCHEMA [[APP_CODE]]_APP_SHARE.logs;`});
    
      //create logs table
      snowflake.execute({sqlText:`CREATE OR REPLACE TABLE [[APP_CODE]]_APP_SHARE.logs.logs(msg VARIANT, signature BINARY) CHANGE_TRACKING=TRUE COMMENT = '{"origin":"sf_ps_wls","name":"acf","version":{"major":1, "minor":3},"attributes":{"role":"consumer","component":"consumer_log_share"}}';`});
        
      //create metrics share schema
      snowflake.execute({sqlText:`CREATE OR REPLACE SCHEMA [[APP_CODE]]_APP_SHARE.metrics;`});

      //create metrics table
      snowflake.execute({sqlText:`CREATE OR REPLACE TABLE [[APP_CODE]]_APP_SHARE.metrics.metrics(msg VARIANT, signature BINARY) CHANGE_TRACKING=TRUE COMMENT = '{"origin":"sf_ps_wls","name":"acf","version":{"major":1, "minor":3},"attributes":{"role":"consumer","component":"consumer_log_share"}}';`});

      //grant privileges on logs/metrics to application
      snowflake.execute({sqlText:`GRANT USAGE ON DATABASE [[APP_CODE]]_APP_SHARE TO APPLICATION ${APP_NAME};`});
      snowflake.execute({sqlText:`GRANT USAGE ON SCHEMA [[APP_CODE]]_APP_SHARE.LOGS TO APPLICATION ${APP_NAME};`});
      snowflake.execute({sqlText:`GRANT USAGE ON SCHEMA [[APP_CODE]]_APP_SHARE.METRICS TO APPLICATION ${APP_NAME};`});
      snowflake.execute({sqlText:`GRANT SELECT, INSERT, UPDATE ON TABLE [[APP_CODE]]_APP_SHARE.LOGS.LOGS TO APPLICATION ${APP_NAME};`});
      snowflake.execute({sqlText:`GRANT SELECT, INSERT, UPDATE ON TABLE [[APP_CODE]]_APP_SHARE.METRICS.METRICS TO APPLICATION ${APP_NAME};`});

      //call provider log_share_insert SP to insert installation logs
      var rset = snowflake.execute({sqlText:`CALL ${APP_NAME}.PROCS_APP.LOG_SHARE_INSERT('${PROVIDER_LOCATOR}', '${APP_CODE}');`});
      rset.next();
      var provision_results = rset.getColumnValue(1);

      //create share
      snowflake.execute({sqlText:`CREATE SHARE IF NOT EXISTS [[APP_CODE]]_${consumer_name}_APP_SHARE COMMENT = '{"origin":"sf_ps_wls","name":"acf","version":{"major":1, "minor":3},"attributes":{"role":"consumer","component":"consumer_log_share"}}';`});

      //grant privileges on logs table to share
      snowflake.execute({sqlText:`GRANT USAGE ON DATABASE [[APP_CODE]]_APP_SHARE TO SHARE [[APP_CODE]]_${consumer_name}_APP_SHARE;`});

      snowflake.execute({sqlText:`GRANT USAGE ON SCHEMA [[APP_CODE]]_APP_SHARE.logs TO SHARE [[APP_CODE]]_${consumer_name}_APP_SHARE;`});
      snowflake.execute({sqlText:`GRANT SELECT ON TABLE [[APP_CODE]]_APP_SHARE.logs.logs TO SHARE [[APP_CODE]]_${consumer_name}_APP_SHARE;`});

      snowflake.execute({sqlText:`GRANT USAGE ON SCHEMA [[APP_CODE]]_APP_SHARE.metrics TO SHARE [[APP_CODE]]_${consumer_name}_APP_SHARE;`});
      snowflake.execute({sqlText:`GRANT SELECT ON TABLE [[APP_CODE]]_APP_SHARE.metrics.metrics TO SHARE [[APP_CODE]]_${consumer_name}_APP_SHARE;`});

      //share logs with provider
      snowflake.execute({sqlText:`ALTER SHARE [[APP_CODE]]_${consumer_name}_APP_SHARE ADD ACCOUNTS=${PROVIDER_LOCATOR};`});
      

      //current date
      const date = Date.now();
      var currentDate = null;
      
      //poll metadata_c_v view until consumer is enabled, or timeout is reached
      do {
          currentDate = Date.now();
          var rset = snowflake.execute({sqlText:`SELECT value FROM ${APP_NAME}.UTIL_APP.METADATA_C_V WHERE UPPER(key) = 'ENABLED' AND UPPER(value) = 'Y';`});
      } while ((rset.getRowCount() == 0) && (currentDate - date < timeout));              

      //if the timeout is reached, disable consumer and return
      while (currentDate - date >= timeout){                     
        return `WARNING:  Consumer has not been enabled yet. Continue to monitor the METADATA_C_V view and contact Provider for more details.`;
      }

      return 'Consumer is now enabled';
    } catch (err) {
      var result = `
      Failed: Code: `+err.code + `
      State: `+err.state+`
      Message: `+err.message+`
      Stack Trace:`+ err.stack;

      return `Error: ${result}`;
  }
  $$
  ;

-------------------------------------------------------------------------------
-- 04_create_generate_request_procedure.sql
-- This procedure serves as a wrapper procedure that calls the app\'s REQUEST stored procedure,
-- passing in a parameters object that includes the input table (if applicable),
-- the app procedure to call, the procedure parameters, and the results table (if applicable)
-------------------------------------------------------------------------------

CREATE OR REPLACE PROCEDURE PRIVATE.GENERATE_REQUEST(app_name VARCHAR, app_code VARCHAR, parameters VARCHAR)
  RETURNS VARCHAR
  LANGUAGE JAVASCRIPT
  COMMENT = '{"origin":"sf_ps_wls","name":"acf","version":{"major":1, "minor":3},"attributes":{"role":"consumer","component":"helper_sproc_generate_request"}}'
  EXECUTE AS CALLER
  AS
  $$

    try {
      var PARAMETERS_JSON = JSON.parse(PARAMETERS);
      //clean up input_table_name and results_table_name and append to inner parameters object
      PARAMETERS_JSON.input_table = PARAMETERS_JSON.input_table.replace(/"/g, "");
      PARAMETERS_JSON.results_table = PARAMETERS_JSON.results_table.replace(/"/g, "");
      PARAMETERS_JSON.proc_parameters[0].input_table_name = PARAMETERS_JSON.input_table;
      PARAMETERS_JSON.proc_parameters[0].results_table_name = PARAMETERS_JSON.results_table;

      //update PARAMETERS string
      PARAMETERS = JSON.stringify(PARAMETERS_JSON).replace(/\'/g, "\\'");

      let { input_table } = PARAMETERS_JSON;

      snowflake.execute({sqlText:`USE ROLE C_[[APP_CODE]]_APP_ADMIN;`});

      if(input_table) {

        //grant privs to source database, schema, and table to application
        const [src_db, src_sch, src_tbl] = input_table.split(".");

        snowflake.execute({sqlText:`GRANT USAGE ON DATABASE ${src_db} TO APPLICATION ${APP_NAME};`});
        snowflake.execute({sqlText:`GRANT USAGE ON SCHEMA ${src_db}.${src_sch} TO APPLICATION ${APP_NAME};`});
        snowflake.execute({sqlText:`GRANT SELECT ON TABLE ${src_db}.${src_sch}.${src_tbl} TO APPLICATION ${APP_NAME};`});
      }

      //if the application can write results outside of its DB, provide grants here:  FEATURE NOT ENABLED YET
      //TODO:  add a field called RESULTS_LOCATION to specify db.sch where results should reside, if desired.
      //snowflake.execute({sqlText:`GRANT USAGE,CREATE TABLE ON SCHEMA C_[[APP_CODE]]_HELPER_DB.RESULTS TO APPLICATION ${APP_NAME};`});
      

      //call the app REQUST sproc
      var rset = snowflake.execute({sqlText:`CALL ${APP_NAME}.PROCS_APP.REQUEST('[[APP_CODE]]', '${PARAMETERS}');`});
      rset.next();
      var response_json = JSON.parse(rset.getColumnValue(1));

      return `${response_json.state}: ${response_json.message}`;
    } catch (err) {
      var result = `
      failed: Code: `+err.code + `
      state: `+err.state+`
      message: `+err.message+`
      stackTrace:`+err.stackTrace || err.stack;

      return `Error: ${result}`;
  }
  $$
  ;

UNSET (APP_ADMIN_ROLE, APP_WH, HELPER_DB);

-------------------------------------------------------------------------------
-- 05_create_uninstall_procedure.sql
-- This procedure serves as a wrapper procedure that uninstalls the Provider\'s app and removes
-------------------------------------------------------------------------------

CREATE OR REPLACE PROCEDURE PRIVATE.UNINSTALL(app_name VARCHAR)
  RETURNS VARCHAR
  LANGUAGE JAVASCRIPT
  COMMENT = '{"origin":"sf_ps_wls","name":"acf","version":{"major":1, "minor":3},"attributes":{"role":"consumer","component":"helper_sproc_app_uninstall"}}'
  EXECUTE AS CALLER
  AS
  $$

  try {
    //drop all existing outbound shares to the Provider account
    snowflake.execute({sqlText: `SHOW SHARES LIKE '%[[APP_CODE]]_%_APP_SHARE%'`});
    snowflake.execute({sqlText: `CREATE OR REPLACE TEMPORARY TABLE C_[[APP_CODE]]_HELPER_DB.PRIVATE.OUTBOUND_SHARES AS SELECT "name" FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())) WHERE "owner" = 'C_[[APP_CODE]]_APP_ADMIN' AND "kind" = 'OUTBOUND';`});
    
    var rset = snowflake.execute({sqlText: `SELECT * FROM C_[[APP_CODE]]_HELPER_DB.PRIVATE.OUTBOUND_SHARES;`});
    while(rset.next()){
      var full_share_name = rset.getColumnValue(1);
      full_share_name_arr = full_share_name.split(".");
      share_name = full_share_name_arr[2];

      snowflake.execute({sqlText:`DROP SHARE IF EXISTS ${full_share_name};`});
      snowflake.execute({sqlText:`DROP SHARE IF EXISTS ${full_share_name};`});
    }

    //drop log database
    snowflake.execute({sqlText: `DROP DATABASE IF EXISTS [[APP_CODE]]_APP_SHARE;`});

    //drop application
    snowflake.execute({sqlText: `DROP APPLICATION IF EXISTS ${APP_NAME};`});

    return `App: ${APP_NAME} removed.`;

  } catch (err) {
    var result = `
    Failed: Code: `+err.code + `
    State: `+err.state+`
    Message: `+err.message+`
    Stack Trace:`+err.stack;

    return `Error: ${result}`;
    }
$$
;

-------------------------------------------------------------------------------
-- 06_grant_application_role.sql
-- This grants the application role to a specific user of the application if one was not granted on installation
-------------------------------------------------------------------------------
USE ROLE ACCOUNTADMIN;
SHOW APPLICATIONS;

SET APP_OWNER = (SELECT "owner" FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())) WHERE UPPER("name") = '[[APP_NAME_INSTALLED]]');

USE ROLE IDENTIFIER($APP_OWNER);

GRANT APPLICATION ROLE [[APP_NAME_INSTALLED]].APP_ROLE TO ROLE C_[[APP_CODE]]_APP_ADMIN;
