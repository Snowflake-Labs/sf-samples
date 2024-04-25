-- The following resources are assumed and pre-existing
use warehouse &SNOW_CONN_warehouse;
use role sysadmin;

create or replace database &APP_DB_database
    comment = 'used for demonstrating fashion demo';

-- Transfer ownership
grant ownership on database &APP_DB_database
    to role &APP_DB_role;

grant ownership  on schema &APP_DB_database.&APP_DB_schema
    to role &APP_DB_role;

grant all privileges  on database &APP_DB_database
    to role &APP_DB_role;

grant all privileges  on schema &APP_DB_database.&APP_DB_schema
    to role &APP_DB_role;


-- =========================
-- Define stages
-- =========================
use role &APP_DB_role;
use schema &APP_DB_database.&APP_DB_schema;

create or replace stage model_stg
    comment = 'used for holding ml models.';
    
create or replace stage UDF_STG

-- =========================
-- Define tables
-- =========================
