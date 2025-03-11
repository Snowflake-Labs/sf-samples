create application role app_public;

drop schema if exists src;
create schema src;
grant usage on schema src 
    to application role app_public;

create or alter versioned schema revenue_timeseries;
grant usage on schema revenue_timeseries 
    to application role app_public;


--create proxy views here as well
create view revenue_timeseries.daily_revenue as 
    select *
    from shared_content.daily_revenue;
create view revenue_timeseries.daily_revenue_by_product as 
    select *
    from shared_content.daily_revenue_by_product;
create view revenue_timeseries.daily_revenue_by_region as 
    select *
    from shared_content.daily_revenue_by_region;


--grants for these proxy views 
grant select on view revenue_timeseries.daily_revenue to application role app_public;
grant select on view revenue_timeseries.daily_revenue_by_product to application role app_public;
grant select on view revenue_timeseries.daily_revenue_by_region to application role app_public;

create stage if not exists src.public_stage directory = (enable = true) ;
grant read, write on stage src.public_stage 
    to application role app_public;

create or replace procedure src.sp_init()
returns string
language python 
runtime_version = 3.11
handler = 'main'
packages = ('snowflake-snowpark-python')
as 
$$

from io import BytesIO

def main(session):

    try:
        # move this to a parameter into the SP
        output_list = []
        output_list.append('Begin')

        output_list.append('file read attempt')
        input_file = session.file.get_stream('/revenue_timeseries.yaml')
        output_list.append('file read complete')

        output_list.append('yml file creation attempt')
        session.file.put_stream(
            input_stream=input_file,
            stage_location='@src.public_stage/revenue_timeseries.yaml',
            parallel = 1, 
            auto_compress = False,
            source_compression = 'NONE',
            overwrite = True)
        output_list.append('yml file create complete')

        output_list.append('process complete')

    except Exception as e:
        output_list.append(e.message)

    return str(output_list)

$$
;

grant usage on procedure src.sp_init() 
    to application role app_public;

comment on procedure src.sp_init() 
    is '{"origin":"sf_sit","name":"na_demo","version":{"major":1, "minor":0},"attributes":{"component":"cortex_analyst_app"}}';


CREATE STREAMLIT src.ux
  FROM '/'
  MAIN_FILE = '/ux_main.py';

GRANT USAGE ON STREAMLIT src.ux TO APPLICATION ROLE app_public;


comment on STREAMLIT src.ux
    is '{"origin":"sf_sit","name":"na_demo","version":{"major":1, "minor":0},"attributes":{"component":"cortex_analyst_app"}}';

