-- Setup script for sales forecast application

CREATE APPLICATION ROLE app_public;
CREATE APPLICATION ROLE app_admin;

CREATE OR ALTER VERSIONED SCHEMA code_schema;
GRANT USAGE ON SCHEMA code_schema TO APPLICATION ROLE app_public;
GRANT USAGE ON SCHEMA code_schema TO APPLICATION ROLE app_admin;


CREATE OR REPLACE FUNCTION code_schema.evaluate_model(month float, day_of_week float, latitude float, longitude float, count_locations_within_half_mile float, city_population float, avg_location_shift_sales float, shift float)
RETURNS FLOAT
LANGUAGE PYTHON
RUNTIME_VERSION=3.8
IMPORTS = ('/python/evaluate_model.py', '/python/linreg_location_sales_model.sav')
PACKAGES = ('snowflake-snowpark-python', 'scikit-learn', 'joblib','pandas')
HANDLER='evaluate_model.evaluate_model';

CREATE OR REPLACE PROCEDURE code_schema.inference(city string, shift int, forecast_date date)
RETURNS TABLE()
LANGUAGE PYTHON
RUNTIME_VERSION=3.8
IMPORTS = ('/python/inference_pipeline.py')
PACKAGES = ('snowflake-snowpark-python', 'scikit-learn', 'joblib','pandas')
HANDLER='inference_pipeline.inference';

GRANT USAGE ON PROCEDURE code_schema.inference(STRING, INT, DATE) TO APPLICATION ROLE app_public;


CREATE OR ALTER VERSIONED SCHEMA config;
GRANT USAGE ON SCHEMA config TO APPLICATION ROLE app_admin;

CREATE PROCEDURE CONFIG.register_sales_data_callback(ref_name STRING, operation STRING, ref_or_alias STRING)
  RETURNS STRING
  LANGUAGE SQL
  AS $$
    BEGIN
      CASE (operation)
        WHEN 'ADD' THEN
          SELECT SYSTEM$SET_REFERENCE(:ref_name, :ref_or_alias);
        WHEN 'REMOVE' THEN
          SELECT SYSTEM$REMOVE_REFERENCE(:ref_name);
        WHEN 'CLEAR' THEN
          SELECT SYSTEM$REMOVE_REFERENCE(ref_name);
      ELSE
        RETURN 'unknown operation: ' || operation;
      END CASE;
      RETURN NULL;
    END;
  $$;

GRANT USAGE ON PROCEDURE CONFIG.register_sales_data_callback(STRING, STRING, STRING) TO APPLICATION ROLE app_admin;

CREATE STREAMLIT code_schema.forecast_streamlit_app
  FROM '/streamlit'
  MAIN_FILE = '/forecast_ui.py';

GRANT USAGE ON STREAMLIT code_schema.forecast_streamlit_app TO APPLICATION ROLE app_admin;
GRANT USAGE ON STREAMLIT code_schema.forecast_streamlit_app TO APPLICATION ROLE app_public;
