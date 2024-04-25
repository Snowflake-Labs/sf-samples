-- The following resources are assumed and pre-existing

use role &APP_DB_role;
use warehouse &SNOW_CONN_warehouse;
use schema &APP_DB_database.&APP_DB_schema;

-- =========================
PUT file://./data1/*
@data_stg/data
    auto_compress = false
    overwrite = true
    parallel=50; 

--      IMAGES
create or replace transient table images (
	image varchar,
	sender_id number,
	label varchar,
	kids boolean
);
copy into images
    from @data_stg/data/images.csv
    FILE_FORMAT = ( TYPE = CSV SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"')
    ON_ERROR = CONTINUE;

--      MODEL_DEFINITIONS
create or replace transient table model_definitions (
	training_dt datetime,
	model_name varchar,
	treatment varchar,
	outcome varchar,
	confounders varchar, 
	ate varchar,
	pred_model_file_name varchar,
	pred_model_accuracy varchar,
    causallib_eval_file_name varchar,
    AVP varchar,
    SCORES varchar

);
