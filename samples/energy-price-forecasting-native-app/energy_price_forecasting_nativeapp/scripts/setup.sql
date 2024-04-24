-- Set up script for the Hello Snowflake! application.
CREATE APPLICATION ROLE IF NOT EXISTS APP_PUBLIC;
CREATE SCHEMA IF NOT EXISTS CORE;
GRANT USAGE ON SCHEMA CORE TO APPLICATION ROLE APP_PUBLIC;

CREATE SCHEMA IF NOT EXISTS TASKS;
GRANT USAGE ON SCHEMA TASKS TO APPLICATION ROLE APP_PUBLIC;


-- 2nd Part
CREATE OR ALTER VERSIONED SCHEMA CRM;
GRANT USAGE ON SCHEMA CRM TO APPLICATION ROLE APP_PUBLIC;

CREATE OR ALTER VERSIONED SCHEMA PYTHON_FUNCTIONS;
GRANT USAGE ON SCHEMA PYTHON_FUNCTIONS TO APPLICATION ROLE APP_PUBLIC;

CREATE STREAMLIT CORE.PRICE_FORECASTING_APP_ST
  FROM '/streamlit'
  MAIN_FILE = '/nativeApp.py'
;

GRANT USAGE ON STREAMLIT CORE.PRICE_FORECASTING_APP_ST TO APPLICATION ROLE APP_PUBLIC;

create or replace stage CRM.ML_MODELS
	directory = ( enable = true )
    comment = 'used for holding ML Models.';

GRANT READ ON STAGE CRM.ML_MODELS TO APPLICATION ROLE APP_PUBLIC;
GRANT WRITE ON STAGE CRM.ML_MODELS TO APPLICATION ROLE APP_PUBLIC;


create or replace stage CRM.ML_SPIKE_MODELS
	directory = ( enable = true )
    comment = 'used for holding ML Spike Models.';

GRANT READ ON STAGE CRM.ML_SPIKE_MODELS TO APPLICATION ROLE APP_PUBLIC;
GRANT WRITE ON STAGE CRM.ML_SPIKE_MODELS TO APPLICATION ROLE APP_PUBLIC;



create or replace stage CORE.UDF
	directory = ( enable = true )
    comment = 'used for holding UDFs.';

GRANT READ ON STAGE CORE.UDF TO APPLICATION ROLE APP_PUBLIC;
GRANT WRITE ON STAGE CORE.UDF TO APPLICATION ROLE APP_PUBLIC;


create or replace procedure PYTHON_FUNCTIONS.sproc_final_model(training_table varchar, split int, app_db varchar, app_sch varchar)
    returns varchar
    language python
    runtime_version = '3.8'
    packages = ('snowflake-snowpark-python',
                'scikit-learn',
                'xgboost',
                'joblib',
                'sqlalchemy',
                'tqdm',
                'colorlog',
                'numpy',
                'pandas')
    imports = ('/python/sproc_final_model.py')
    handler = 'sproc_final_model.sproc_final_model'
    ;

GRANT USAGE ON PROCEDURE PYTHON_FUNCTIONS.sproc_final_model(varchar, int, varchar, varchar) TO APPLICATION ROLE APP_PUBLIC;

create or replace procedure PYTHON_FUNCTIONS.sproc_deploy_model(training_table varchar, file_path varchar, 
                                        target_table varchar, append_mode varchar,  app_db varchar, app_sch varchar)
    returns varchar
    language python
    runtime_version = '3.8'
    packages = ('snowflake-snowpark-python','xgboost', 'scikit-learn','pandas','joblib','cachetools')
    imports = ('/python/sproc_deploy_model.py')
    handler = 'sproc_deploy_model.main'
    ;

GRANT USAGE ON PROCEDURE PYTHON_FUNCTIONS.sproc_deploy_model(varchar, varchar, varchar, varchar, varchar, varchar) TO APPLICATION ROLE APP_PUBLIC;


-- Experiment
-- create or replace procedure PYTHON_FUNCTIONS.udf_deploy_model()
--     returns varchar
--     language python
--     runtime_version = '3.8'
--     packages = ('snowflake-snowpark-python','xgboost', 'scikit-learn','pandas','joblib','cachetools')
--     imports = ('/python/udf_deploy_model.py')
--     handler = 'udf_deploy_model.main'
--     ;

-- GRANT USAGE ON PROCEDURE PYTHON_FUNCTIONS.udf_deploy_model() TO APPLICATION ROLE APP_PUBLIC;



create or replace procedure PYTHON_FUNCTIONS.sproc_optuna_optimized_model(
        table_name varchar,
        model_name varchar,
        n_trials int, app_db varchar, app_sch varchar)
    returns varchar
    language python
    volatile
    runtime_version = '3.8'
    imports = ('/python/sproc_optuna_optimized_model.py')
    packages=('snowflake-snowpark-python',
                'scikit-learn',
                'xgboost',
                'joblib',
                'sqlalchemy',
                'tqdm',
                'colorlog',
                'numpy', 'optuna','cmaes',
                'pandas')
    handler = 'sproc_optuna_optimized_model.sproc_optuna_optimized_model'
    ;

GRANT USAGE ON PROCEDURE PYTHON_FUNCTIONS.sproc_optuna_optimized_model(varchar, varchar, int, varchar, varchar) TO APPLICATION ROLE APP_PUBLIC;


-- SPIKE FUNCTIONS

create or replace procedure PYTHON_FUNCTIONS.sproc_spike_forecast(training_table varchar, app_db varchar, app_sch varchar)
    returns varchar
    language python
    runtime_version = '3.8'
    imports=('/python/sproc_spike_forecast.py'),
    packages = ('snowflake-snowpark-python','xgboost', 'scikit-learn','pandas','joblib','cachetools')
    handler = 'sproc_spike_forecast.main'
    ;

GRANT USAGE ON PROCEDURE PYTHON_FUNCTIONS.sproc_spike_forecast(varchar, varchar, varchar) TO APPLICATION ROLE APP_PUBLIC;

-- Create a stored procedure for backtesting
create or replace procedure PYTHON_FUNCTIONS.sproc_optuna_optimized_model_spike(
        table_name varchar,
        model_name varchar,
        n_trials int, app_db varchar, app_sch varchar)
    returns varchar
    language python
    volatile
    runtime_version = '3.8'
    imports = ('/python/sproc_optuna_optimized_model_spike.py')
    packages=('snowflake-snowpark-python',
                'scikit-learn',
                'xgboost',
                'joblib',
                'sqlalchemy',
                'tqdm',
                'colorlog',
                'numpy','optuna','cmaes',
                'pandas')
    handler = 'sproc_optuna_optimized_model_spike.sproc_optuna_optimized_model'
    ;

GRANT USAGE ON PROCEDURE PYTHON_FUNCTIONS.sproc_optuna_optimized_model_spike(varchar, varchar, int, varchar, varchar) TO APPLICATION ROLE APP_PUBLIC;

create or replace procedure PYTHON_FUNCTIONS.sproc_final_model_spike(training_table varchar, split int, app_db varchar, app_sch varchar)
    returns varchar
    language python
    runtime_version = '3.8'
    packages = ('snowflake-snowpark-python',
                'scikit-learn',
                'xgboost',
                'sqlalchemy',
                'tqdm',
                'numpy',
                'pandas',
                'joblib')
    imports = ('/python/sproc_final_model_spike.py')
    handler = 'sproc_final_model_spike.sproc_final_model1'
    ;
GRANT USAGE ON PROCEDURE PYTHON_FUNCTIONS.sproc_final_model_spike(varchar, int, varchar, varchar) TO APPLICATION ROLE APP_PUBLIC;


create or replace procedure PYTHON_FUNCTIONS.sproc_deploy_model_spike(training_table varchar, file_path varchar, target_table varchar, append_mode varchar, app_db varchar, app_sch varchar)
    returns varchar
    language python
    runtime_version = '3.8'
    packages = ('snowflake-snowpark-python','xgboost', 'scikit-learn','pandas','joblib','cachetools')
    imports = ('/python/sproc_deploy_model_spike.py')
    handler = 'sproc_deploy_model_spike.main'
    ;

GRANT USAGE ON PROCEDURE PYTHON_FUNCTIONS.sproc_deploy_model_spike(varchar, varchar, varchar, varchar, varchar, varchar) TO APPLICATION ROLE APP_PUBLIC;

CREATE OR REPLACE SECURE VIEW CRM.ML_DATA_SPIKE_VW AS SELECT * FROM SHARED_CONTENT.ML_DATA_SPIKE_VW;
GRANT SELECT ON VIEW CRM.ML_DATA_SPIKE_VW TO APPLICATION ROLE APP_PUBLIC;

CREATE OR REPLACE SECURE VIEW CRM.ML_DATA_VW AS SELECT * FROM SHARED_CONTENT.ML_DATA_VW;
GRANT SELECT ON VIEW CRM.ML_DATA_VW TO APPLICATION ROLE APP_PUBLIC;

