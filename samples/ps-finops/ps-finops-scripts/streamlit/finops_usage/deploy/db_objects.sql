use role <% ctx.env.finops_sis_admin %>;
use database <% ctx.env.finops_sis_db %>;
use schema <% ctx.env.finops_sis_usage_sc %>;

create or replace table costcenter (id number identity start 1 increment 1 order, costcenter_name varchar);

create table if not exists OBJECT_COSTCENTER_MAPPING
(
OBJECT_TYPE  VARCHAR,
OBJECT_NAME  VARCHAR,
COSTCENTER   VARCHAR
);

create or replace stream st_object_costcenter_mapping on table object_costcenter_mapping;


create or replace procedure proc_pop_object_costcenter_mapping()
RETURNS varchar
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
result varchar;

begin
result := 'proc executed successfully';

create or replace temporary table temp_object_costcenter_mapping(object_type varchar,object_name varchar);

insert into temp_object_costcenter_mapping(object_type,object_name)
select 'USER',name
from snowflake.account_usage.users
where deleted_on is null
and name <> 'SNOWFLAKE'

union all

select 'ROLE',name
from snowflake.account_usage.roles
where deleted_on is null
and name not in ('ACCOUNTADMIN','SYSADMIN','SECURITYADMIN','USERADMIN','PUBLIC','ORGADMIN','GLOBALORGADMIN')
and role_type='ROLE'

union all

select 'DATABASE',database_name
from snowflake.account_usage.databases
where deleted is null
and TYPE='STANDARD'

union all

select 'SCHEMA',catalog_name||'.'||schema_name
from snowflake.account_usage.schemata
where deleted is null
and owner_role_type='ROLE';

show warehouses;

insert into temp_object_costcenter_mapping(object_type,object_name)
select 'WAREHOUSE',"name" from table(result_scan(last_query_id()));

insert into object_costcenter_mapping(object_type,object_name)
select object_type,object_name from temp_object_costcenter_mapping
where object_name not in (select object_name from object_costcenter_mapping) ;

return result;
end
$$
;


CREATE OR REPLACE PROCEDURE EXEC_SQL(object_name varchar)
RETURNS varchar
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
user_sql resultset default (select sql from identifier(:OBJECT_NAME));
c1 CURSOR FOR user_sql;
result varchar default 'proc executed successfully';
statement_1 varchar;

begin
    OPEN c1;
    FOR rec IN c1 DO
        statement_1 :=  rec.sql;
        execute immediate :statement_1;
    END FOR;
return result;
end
$$
;
