//Step 1 - Create the application package
use role accountadmin; 

--or use a role with the following grants: 
    --CREATE DATABASE 
    --CREATE APPLICATION PACKAGE
    --CREATE APPLICATION
    --CREATE DATA EXCHANGE LISTING 
    --CREATE INTEGRATION
    --IMPORT SHARE 
    --CREATE ROLE 
    --CREATE SHARE 

--create warehouse or use existing    
create warehouse xs_wh with warehouse_size='X-SMALL';
use warehouse xs_wh;

use schema CORTEX_ANALYST_DEMO.REVENUE_TIMESERIES;
show tables in schema;
--+--------------------------+------+                                             
--| name                     | rows |
--|--------------------------+------|
--| DAILY_REVENUE            |  730 |
--| DAILY_REVENUE_BY_PRODUCT | 3650 |
--| DAILY_REVENUE_BY_REGION  | 3650 |
--+--------------------------+------+


create application package cortex_analyst_app_pkg;

//Step 2 - Create associated schemas for the application package
create schema cortex_analyst_app_pkg.stages;
create stage cortex_analyst_app_pkg.stages.app_code_stage; 
create schema cortex_analyst_app_pkg.shared_content;

//Step 3 - create three "proxy views" in the Native App Package
create view cortex_analyst_app_pkg.shared_content.daily_revenue as 
    select *
    from cortex_analyst_demo.revenue_timeseries.daily_revenue;

create view cortex_analyst_app_pkg.shared_content.daily_revenue_by_product as 
    select *
    from cortex_analyst_demo.revenue_timeseries.daily_revenue_by_product;

create view cortex_analyst_app_pkg.shared_content.daily_revenue_by_region as 
    select *
    from cortex_analyst_demo.revenue_timeseries.daily_revenue_by_region;

//Step 4 - grants for these proxy views 
grant usage on schema cortex_analyst_app_pkg.shared_content to share in application package cortex_analyst_app_pkg;
grant reference_usage on database cortex_analyst_demo to share in application package cortex_analyst_app_pkg;
grant select on view cortex_analyst_app_pkg.shared_content.daily_revenue to share in application package cortex_analyst_app_pkg;
grant select on view cortex_analyst_app_pkg.shared_content.daily_revenue_by_product to share in application package cortex_analyst_app_pkg;
grant select on view cortex_analyst_app_pkg.shared_content.daily_revenue_by_region to share in application package cortex_analyst_app_pkg;

--test the proxy views to ensure all OK
select * from cortex_analyst_app_pkg.shared_content.daily_revenue limit 10;
select * from cortex_analyst_app_pkg.shared_content.daily_revenue_by_product limit 10;
select * from cortex_analyst_app_pkg.shared_content.daily_revenue_by_region limit 10;


//Step 5 - Load the files listed above from your desktop to the Native Application Snowflake Stage as described in the blog

-- Upload code assets to stage via SnowSQL, SnowCLI, Python, using the VS Code, or the File Upload Wizard in your Snowsight UI 
-- - manifest.yml 
-- - environment.yml 
-- - setup.sql 
-- - ux_main.py 
-- - revenue_timeseries.yaml 
-- - readme.md
-- - sit_logo_color.png

ls @cortex_analyst_app_pkg.stages.app_code_stage;

//Step 6 - Create a version, release directive, and "local" test instance of the Native Application.
alter application package cortex_analyst_app_pkg
    ADD VERSION v01
    USING '@cortex_analyst_app_pkg.stages.app_code_stage';

alter application package cortex_analyst_app_pkg 
    set default release directive version=v01 patch=0;

--test locally in provider account
create application cortex_analyst_app from APPLICATION PACKAGE cortex_analyst_app_pkg;


//Step 7 - Call an initialization Stored Procedure which is required to allow the Native App to read the semantic model yaml file.
--call this stored procedure to copy semantic model yml file from "package" stage to "app" stage
--note that by the end of CY24, this call should be able to be moved to the setup script 
call cortex_analyst_app.src.sp_init();

--go to the UI/UX and try it out

--cleanup if needed
drop application if exists cortex_analyst_app CASCADE;
drop application package if exists cortex_analyst_app_pkg;


