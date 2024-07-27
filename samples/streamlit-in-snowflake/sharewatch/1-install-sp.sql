/*************************************************************************************************************
Script:             Share Watch 1.0 Install 
Create Date:        2023-01-04
Author:             Amit Gupta, Fady Heiba 
Description:        Script installs objects and stored procedures for share watch app. Execute this file as is 
                    Requires accountadmin role
*************************************************************************************************************/

--Uninstall if current setup exists
drop notification integration if exists sharewatch;
drop warehouse if exists sharewatch;
drop database if exists sharewatch;

--Install as accountadmin
use role accountadmin;
CREATE notification integration if not exists sharewatch_notification_integration type=email enabled=true;
describe notification integration sharewatch_notification_integration;
grant usage ON integration sharewatch_notification_integration to role accountadmin;


-- sharewatch database: stores all objects required for share watch app to run
create database if not exists sharewatch QUOTED_IDENTIFIERS_IGNORE_CASE=true DATA_RETENTION_TIME_IN_DAYS= 30;
drop schema if exists sharewatch.public;
-- sharewatch warehouse: dedicated warehouse to run share watch app
create warehouse if not exists sharewatch;
-- sharewatch.util schema: stores all objects required for share watch app to run. Including Stored procedures and sp run log table
create schema if not exists sharewatch.util;
-- sharewatch.util.sp_runlog table: records all exectuions of sharewatch stored procedures whether run via share watch app or manually executed 
create table if not exists sharewatch.util.sp_runlog (sp_name varchar(100), sp_arguments varchar, sp_exec_status varchar(10), sp_exec_msg varchar, sp_exec_time timestamp, sp_exec_user varchar, sp_exec_session_id varchar);
-- sharewatch.util.databases_monitored table: stores all databases (mounted shares) monitored by share watch app
create table if not exists sharewatch.util.databases_monitored (db_name varchar(100), origin varchar, scheduled_task_name varchar, schemadrift_flag boolean, cdc_flag boolean); 
-- sharewatch.util.app_config: records all app configurable parameters and their current value
create table if not exists sharewatch.util.app_config (parameter varchar, value variant);
-- sharewatch.util.master_task: master task defaulted to start daily at 7 am New York Time
create task if not exists sharewatch.util.master_task schedule= 'USING CRON 0 7 * * *  America/New_York' as call sharewatch.util.run_schemadrift_monitor();
--populate defaults for app_config
insert into sharewatch.util.app_config select 'MASTER_TASK', parse_json('{"name": "master_task","schedule": "0 7 * * *  America/New_York"}');
insert into sharewatch.util.app_config select 'NOTIFICATION_EMAIL', null;
insert into sharewatch.util.app_config select 'NOTIFICATION_INTEGRATION', parse_json('{"name": "sharewatch_notification_integration","type": "email","enabled":"true"}');



use role accountadmin;
use schema sharewatch.util;

--drop procedure setup_schemadrift_monitor (varchar);
--create or replace procedure  sharewatch.util.setup_schemadrift_monitor (MOUNTEDDB_NAME varchar)
create procedure if not exists sharewatch.util.setup_schemadrift_monitor (MOUNTEDDB_NAME varchar)
returns string
language javascript
execute as caller
as
$$
 // Set variables
    var sharewatchdbname = 'sharewatch';
    var sharewatchutilschema_qualified = 'sharewatch'.concat('.UTIL');
    var sharewatch_sprunlogtbl_qualified = 'sharewatch'.concat('.UTIL.sp_runlog');
    var sharewatch_monitoreddbtbl_qualified = 'sharewatch'.concat('.UTIL.databases_monitored');
    var mountdbname = MOUNTEDDB_NAME.toUpperCase();
    var sharewatch_mountdbschema_qualified = sharewatchdbname.concat('.').concat(mountdbname).toUpperCase();
    var receivedobject_vw_qualified = sharewatch_mountdbschema_qualified.concat('._RECEIVED_OBJECTS');
    var expectedobject_tbl_qualified = sharewatch_mountdbschema_qualified.concat('._EXPECTED_OBJECTS');
    var infoschema = 'information_schema'.toUpperCase();
    var infoschema_qualified = mountdbname.concat('.').concat(infoschema);

    // test if install script was run correctly
 try { snowflake.execute({ sqlText: `describe table `+sharewatch_monitoreddbtbl_qualified}); } 
catch (err) {  
    //log sp run in log table
    escaped_error_msg = err.message.replaceAll("'","\\'");
    escaped_error_msg += ". Run install share watch successfully.";
 snowflake.execute({ sqlText:  ` insert into `+sharewatch_sprunlogtbl_qualified+` values ('setup_schemadrift_monitor', '`+mountdbname+`','failed','`+escaped_error_msg+`',current_timestamp(), current_user(), current_session())`});
     
     return err;}
     
// test if mount_db exist else error out and do nothing
 try { snowflake.execute({ sqlText: `describe database `+mountdbname}); } 
catch (err) {  
    //log sp run in log table
    escaped_error_msg = err.message.replaceAll("'","\\'");
     snowflake.execute({ sqlText:  ` insert into `+sharewatch_sprunlogtbl_qualified+` values ('setup_schemadrift_monitor', '`+mountdbname+`','failed','`+escaped_error_msg+`',current_timestamp(), current_user(), current_session())`});
     
     return err;}
    
// if mount_db exists then setup monitoring
try {
    // Create a schema under sharewatch db to hold objects for database mounted from a share
    snowflake.execute({ sqlText:`create schema if not exists `+ sharewatch_mountdbschema_qualified+` comment = 'Schema to hold objects for the database mounted from a share'`});

    // Create view to show currently receivedobjects
     snowflake.execute({ sqlText:`
     create view if not exists `+receivedobject_vw_qualified+` as
        select
        'SCHEMA' AS OBJECT_TYPE
        , CATALOG_NAME||'.'||SCHEMA_NAME AS OBJECT_NAME
        , current_timestamp() as snapshot_timestamp
        FROM `+infoschema_qualified+`.schemata
        WHERE SCHEMA_NAME != 'INFORMATION_SCHEMA'
        UNION
        select
         tbl.TABLE_TYPE AS OBJECT_TYPE
        , tbl.TABLE_CATALOG||'.'||tbl.TABLE_SCHEMA||'.'||tbl.TABLE_NAME AS OBJECT_NAME
        , current_timestamp() as snapshot_timestamp
        FROM `+infoschema_qualified+`.tables tbl
        WHERE tbl.TABLE_SCHEMA != 'INFORMATION_SCHEMA'
        UNION
        select
        'FUNCTION' AS OBJECT_TYPE
        , FUNCTION_CATALOG||'.'||FUNCTION_SCHEMA||'.'||FUNCTION_NAME AS OBJECT_NAME
        , current_timestamp() as snapshot_timestamp
        FROM `+infoschema_qualified+`.functions
        WHERE FUNCTION_SCHEMA != 'INFORMATION_SCHEMA'
        UNION
        select
        'EXTERNAL_TABLE' AS OBJECT_TYPE
        , tbl.TABLE_CATALOG||'.'||tbl.TABLE_SCHEMA||'.'||tbl.TABLE_NAME AS OBJECT_NAME
        , current_timestamp() as snapshot_timestamp
        FROM `+infoschema_qualified+`.external_tables  tbl
        WHERE tbl.TABLE_SCHEMA != 'INFORMATION_SCHEMA'
        UNION
        select
        'COLUMN' AS OBJECT_TYPE
        , TABLE_CATALOG||'.'||TABLE_SCHEMA||'.'||TABLE_NAME||'.'||COLUMN_NAME AS OBJECT_NAME
        , current_timestamp() as snapshot_timestamp
        FROM `+infoschema_qualified+`.columns
        WHERE TABLE_SCHEMA != 'INFORMATION_SCHEMA'
        UNION
        select
        'FUNCTION_SIGNATURE' AS OBJECT_TYPE
        , FUNCTION_CATALOG||'.'||FUNCTION_SCHEMA||'.'||FUNCTION_NAME||'.'||ARGUMENT_SIGNATURE AS OBJECT_NAME
        , current_timestamp() as snapshot_timestamp
        FROM `+infoschema_qualified+`.functions
        WHERE FUNCTION_SCHEMA != 'INFORMATION_SCHEMA'
        order by case when object_type ='SCHEMA' then '0' when object_type ='COLUMN' then '~' else object_type end, object_name
     `});

     // Create table to snapshot expected objects
     snowflake.execute({ sqlText:`
     create table if not exists `+expectedobject_tbl_qualified+` as
        select
        'SCHEMA' AS OBJECT_TYPE
        , CATALOG_NAME||'.'||SCHEMA_NAME AS OBJECT_NAME
        , current_timestamp() as snapshot_timestamp
        FROM `+infoschema_qualified+`.schemata
        WHERE SCHEMA_NAME != 'INFORMATION_SCHEMA'
        UNION
        select
         tbl.TABLE_TYPE AS OBJECT_TYPE
        , tbl.TABLE_CATALOG||'.'||tbl.TABLE_SCHEMA||'.'||tbl.TABLE_NAME AS OBJECT_NAME
        , current_timestamp() as snapshot_timestamp
        FROM `+infoschema_qualified+`.tables tbl
        WHERE tbl.TABLE_SCHEMA != 'INFORMATION_SCHEMA'
        UNION
        select
        'FUNCTION' AS OBJECT_TYPE
        , FUNCTION_CATALOG||'.'||FUNCTION_SCHEMA||'.'||FUNCTION_NAME AS OBJECT_NAME
        , current_timestamp() as snapshot_timestamp
        FROM `+infoschema_qualified+`.functions
        WHERE FUNCTION_SCHEMA != 'INFORMATION_SCHEMA'
        UNION
        select
        'EXTERNAL_TABLE' AS OBJECT_TYPE
        , tbl.TABLE_CATALOG||'.'||tbl.TABLE_SCHEMA||'.'||tbl.TABLE_NAME AS OBJECT_NAME
        , current_timestamp() as snapshot_timestamp
        FROM `+infoschema_qualified+`.external_tables  tbl
        WHERE tbl.TABLE_SCHEMA != 'INFORMATION_SCHEMA'
        UNION
        select
        'COLUMN' AS OBJECT_TYPE
        , TABLE_CATALOG||'.'||TABLE_SCHEMA||'.'||TABLE_NAME||'.'||COLUMN_NAME AS OBJECT_NAME
        , current_timestamp() as snapshot_timestamp
        FROM `+infoschema_qualified+`.columns
        WHERE TABLE_SCHEMA != 'INFORMATION_SCHEMA'
        UNION
        select
        'FUNCTION_SIGNATURE' AS OBJECT_TYPE
        , FUNCTION_CATALOG||'.'||FUNCTION_SCHEMA||'.'||FUNCTION_NAME||'.'||ARGUMENT_SIGNATURE AS OBJECT_NAME
        , current_timestamp() as snapshot_timestamp
        FROM `+infoschema_qualified+`.functions
        WHERE FUNCTION_SCHEMA != 'INFORMATION_SCHEMA'
        order by case when object_type ='SCHEMA' then '0' when object_type ='COLUMN' then '~' else object_type end, object_name
     `});


    //entry in databases_monitored    
    snowflake.execute({ sqlText: `insert into `+sharewatch_monitoreddbtbl_qualified+` values ('`+mountdbname+`',null,null,1,0)`});
   
     }
    catch (err)  {

    err_msg = err.message;
                //log sp run in log table
 snowflake.execute({ sqlText:  ` insert into `+sharewatch_sprunlogtbl_qualified+` values ('setup_schemadrift_monitor', '`+mountdbname+`','failed','',current_timestamp(), current_user(), current_session())`});
                

                return err_msg;  // Return a success/error indicator.
       

                }

    
success_msg = "Schema drift monitor is successfully setup for ".concat(mountdbname);
success_msg += "\n =================================================================";
success_msg += "\n Expected objects for mounted DB can be viewed here: ".concat(expectedobject_tbl_qualified) ;
success_msg += "\n Received objects for mounted DB can be viewed here: ".concat(receivedobject_vw_qualified) ;

    //log sp run in log table     
    snowflake.execute({ sqlText:  ` insert into `+sharewatch_sprunlogtbl_qualified+` values ('setup_schemadrift_monitor', '`+mountdbname+`','success','`+success_msg+`',current_timestamp(), current_user(), current_session())`});

     return success_msg
$$;

--drop procedure sharewatch.util.run_schemadrift_monitor (varchar);
--create or replace procedure  sharewatch.util.run_schemadrift_monitor (MOUNTEDDB_NAME varchar)
create procedure if not exists  sharewatch.util.run_schemadrift_monitor (MOUNTEDDB_NAME varchar)
returns string
language javascript
execute as caller
as
$$

// Set variables
var sharewatchdbname = 'sharewatch';
var sharewatchutilschema_qualified = 'sharewatch'.concat('.UTIL');
var sharewatch_sprunlogtbl_qualified = 'sharewatch'.concat('.UTIL.sp_runlog');
var mountdbname = MOUNTEDDB_NAME.toUpperCase();
var sharewatch_mountdbschema_qualified = sharewatchdbname.concat('.').concat(mountdbname).toUpperCase();
var receivedobject_vw_qualified = sharewatch_mountdbschema_qualified.concat('._RECEIVED_OBJECTS');
var expectedobject_tbl_qualified = sharewatch_mountdbschema_qualified.concat('._EXPECTED_OBJECTS');
var infoschema = 'information_schema'.toUpperCase();
var infoschema_qualified = mountdbname.concat('.').concat(infoschema);

// set notification email parameters
try { 
param = snowflake.execute({ sqlText: `select trim(value:name::string) from sharewatch.util.app_config where upper(parameter) = 'NOTIFICATION_INTEGRATION'`});
param.next();
var sharewatch_notification_integration_name = param.getColumnValue(1);

param = snowflake.execute({ sqlText: `select trim(value:email_address::string) from sharewatch.util.app_config where upper(parameter) = 'NOTIFICATION_EMAIL'`}); 
param.next();
var sharewatch_notification_email = param.getColumnValue(1);
}
catch (err) {  
    //log sp run in log table
    error_msg = err.message;
    //error_msg = "Error: App is not installed properly. Please re-run the install script";
    escaped_error_msg = error_msg.replaceAll("'","\\'");
     snowflake.execute({ sqlText:  ` insert into `+sharewatch_sprunlogtbl_qualified+` values ('run_schemadrift_monitor', '`+mountdbname+`','failed','`+escaped_error_msg+`',current_timestamp(), current_user(), current_session())`});
     return error_msg;};


// test if mount_db is setup for monitoring
 try { snowflake.execute({ sqlText: `describe schema `+sharewatch_mountdbschema_qualified}); }
catch (err) {  
    //log sp run in log table
    error_msg = "Error: sharewatch monitor is not setup on the database. Run 'call setup_schemadrift_monitor(".concat(mountdbname).concat(")' to setup monitor");
    escaped_error_msg = error_msg.replaceAll("'","\\'");
     snowflake.execute({ sqlText:  ` insert into `+sharewatch_sprunlogtbl_qualified+` values ('run_schemadrift_monitor', '`+mountdbname+`','failed','`+escaped_error_msg+`',current_timestamp(), current_user(), current_session())`});
     return error_msg;};
    
// if mount_db exists then setup monitoring


// check for schemadrifts in monitored db
var result = snowflake.execute({ sqlText: `select case when eo.object_type is not null then 'Outage: Share schema altered'
             when  ro.object_type is not null then 'Warning: Share schema appended'
             else 'Share schema is as expected' end as alert_flag,
 eo.object_type as expected_object_type, eo.object_name as expected_object_name,  
 ro.object_type as received_object_type, ro.object_name as received_object_name, ro.snapshot_timestamp as received_on
from  `+expectedobject_tbl_qualified+` eo
full outer join `+receivedobject_vw_qualified+` ro
on eo.object_type = ro.object_type
and eo.object_name = ro.object_name
where eo.object_type is null or ro.object_type is null 
order by case when expected_object_type ='SCHEMA' then '1' when expected_object_type ='COLUMN' then '~' else expected_object_type end, expected_object_name, case when received_object_type ='SCHEMA' then '1' when received_object_type ='COLUMN' then '~'else received_object_type end, received_object_name -- order by is important to detect alert correctly`});  

// if rows are returned by query then set alert message else no alert is raised
if(result.next())
{
    alert_flag = result.getColumnValue(1);
    if (alert_flag == "Warning: Share schema appended")
    {
        alert_msg = alert_flag.concat(" - New objects received are listed below");
        alert_msg += "\n --------------------------------------------------------------------------"
        alert_msg += "\n object type: ".concat(result.getColumnValue(4)).concat(", object name:    ").concat(result.getColumnValue(5));
        while (result.next() ) 
        {alert_msg += "\n object type: ".concat(result.getColumnValue(4)).concat(", object name: ").concat(result.getColumnValue(5));
        }
    }
    else
    {
        alert_msg = alert_flag.concat(" - Expected objects not received are listed below");
        alert_msg += "\n --------------------------------------------------------------------------"
        alert_msg += "\n object type: ".concat(result.getColumnValue(2)).concat(", object name:    ").concat(result.getColumnValue(3));
        outageobjects_received_msg = "";
        while (result.next() && result.getColumnValue(2)) 
        {
         alert_msg += "\n object type: ".concat(result.getColumnValue(2)).concat(", object name: ").concat(result.getColumnValue(3)); 
        }
    }
//Send notification email when schema has drifted
snowflake.execute({ sqlText:  `CALL SYSTEM$SEND_EMAIL('`+sharewatch_notification_integration_name+`',
    '`+sharewatch_notification_email+`','ShareWatch Alert: `+alert_flag+` for  `+mountdbname+`','`+alert_msg+`');` })
}
else
{
    alert_msg = "Share schema is as expected";
}

//log sp run in log table
 snowflake.execute({ sqlText:  ` insert into `+sharewatch_sprunlogtbl_qualified+` values ('run_schemadrift_monitor', '`+mountdbname+`','success','`+alert_msg+`',current_timestamp(), current_user(), current_session())`});

 return alert_msg;
$$;


--drop procedure sharewatch.util.run_schemadrift_monitor ();
--create or replace procedure  sharewatch.util.run_schemadrift_monitor ()
create procedure if not exists  sharewatch.util.run_schemadrift_monitor ()
returns string
language javascript
execute as caller
as
$$
var sharewatchdbname = 'sharewatch';
var sharewatchutilschema_qualified = 'sharewatch'.concat('.UTIL');
var sharewatch_monitoreddb_tbl_qualified = 'sharewatch'.concat('.UTIL.databases_monitored');
var sharewatch_sprunlogtbl_qualified = 'sharewatch'.concat('.UTIL.sp_runlog');

 try { var result = snowflake.execute({ sqlText: `select db_name from `+sharewatch_monitoreddb_tbl_qualified+` order by 1` }); }
 catch (err) {  
  //log sp run in log table
    error_msg = err.message;
    escaped_error_msg = error_msg.replaceAll("'","\\'");
     snowflake.execute({ sqlText:  ` insert into `+sharewatch_sprunlogtbl_qualified+` values ('run_schemadrift_monitor', 'All Monitored DBs','failed','`+escaped_error_msg+`',current_timestamp(), current_user(), current_session())`});
     return error_msg;
 }
// exit if there are no monitor setup 
if (!result.next()) {
    callsp_msg = "Nothing to run. Monitor is not setup on any database"; 
     //log sp run in log table
    error_msg = callsp_msg;
    escaped_error_msg = error_msg.replaceAll("'","\\'");
     snowflake.execute({ sqlText:  ` insert into `+sharewatch_sprunlogtbl_qualified+` values ('run_schemadrift_monitor', 'All Monitored DBs','failed','`+escaped_error_msg+`',current_timestamp(), current_user(), current_session())`});
     return error_msg;
    }
else {
    callsp_msg = "Successfully run monitor for following databases ";
    callsp_msg +="\n ----------------------------------------------";
    
    // Run Monitor for 1st DB
    var db_name = result.getColumnValue(1);
    callsp_result=snowflake.execute({ sqlText:  `call `+sharewatchutilschema_qualified+`.run_schemadrift_monitor ('`+db_name+`');`});
    callsp_result.next();
    callsp_msg +="\n".concat(db_name).concat(' MSG: ').concat(callsp_result.getColumnValue(1));
   
   // Run Monitor for 2nd DB and Onwards
    while (result.next()) {
            db_name = result.getColumnValue(1);
            callsp_result=snowflake.execute({ sqlText:  `call `+sharewatchutilschema_qualified+`.run_schemadrift_monitor ('`+db_name+`');`});
            callsp_result.next();
            callsp_msg +="\n".concat(db_name).concat(' MSG: ').concat(callsp_result.getColumnValue(1));
        }

    //log sp run in log table
 snowflake.execute({ sqlText:  ` insert into `+sharewatch_sprunlogtbl_qualified+` values ('run_schemadrift_monitor', 'All Monitored DBs','success','`+callsp_msg+`',current_timestamp(), current_user(), current_session())`});
    
     return callsp_msg;

     
}

$$;
--drop procedure sharewatch.util.drop_schemadrift_monitor (varchar);
-- create or replace procedure  sharewatch.util.drop_schemadrift_monitor (MOUNTEDDB_NAME varchar)
create procedure if not exists  sharewatch.util.drop_schemadrift_monitor (MOUNTEDDB_NAME varchar)
returns string
language javascript
execute as caller
as
$$
 // Set variables
    var sharewatchdbname = 'sharewatch';
    var sharewatchutilschema_qualified = 'sharewatch'.concat('.UTIL');
    var sharewatch_sprunlogtbl_qualified = 'sharewatch'.concat('.UTIL.sp_runlog');
    var sharewatch_monitoreddbtbl_qualified = 'sharewatch'.concat('.UTIL.databases_monitored');
    var mountdbname = MOUNTEDDB_NAME.toUpperCase();
    var sharewatch_mountdbschema_qualified = sharewatchdbname.concat('.').concat(mountdbname).toUpperCase();
    var receivedobject_vw_qualified = sharewatch_mountdbschema_qualified.concat('._RECEIVED_OBJECTS');
    var expectedobject_tbl_qualified = sharewatch_mountdbschema_qualified.concat('._EXPECTED_OBJECTS');
    var infoschema = 'information_schema'.toUpperCase();
    var infoschema_qualified = mountdbname.concat('.').concat(infoschema);

    

     
// test if mount_db exist else error out and do nothing
 try { snowflake.execute({ sqlText: `describe database `+mountdbname}); } 
catch (err) {  
    //log sp run in log table
    escaped_error_msg = err.message.replaceAll("'","\\'");
     snowflake.execute({ sqlText:  ` insert into `+sharewatch_sprunlogtbl_qualified+` values ('drop_schemadrift_monitor', '`+mountdbname+`','failed','`+escaped_error_msg+`',current_timestamp(), current_user(), current_session())`});
     
     return err;}

//test if database is monitored for schemadrift. If not, do nothing
    result=snowflake.execute({ sqlText:  `select db_name, schemadrift_flag from `+sharewatch_monitoreddbtbl_qualified+` where db_name = '`+mountdbname+`' and schemadrift_flag=1;`});
    if (!result.next()) {
            //log sp run in log table
            escaped_error_msg = 'ERROR: Database is not setup for schemadrift monitoring';
             snowflake.execute({ sqlText:  ` insert into `+sharewatch_sprunlogtbl_qualified+` values ('drop_schemadrift_monitor', '`+mountdbname+`','failed','`+escaped_error_msg+`',current_timestamp(), current_user(), current_session())`});
    
    return escaped_error_msg ;}
    
// if mount_db is setup for schemadrift monitoring then cleanup
try {
    // Drop schema under sharewatch db 
    snowflake.execute({ sqlText:`drop schema if exists `+ sharewatch_mountdbschema_qualified});
    // Delete entry from databases monitored table
    snowflake.execute({ sqlText:`delete from `+sharewatch_monitoreddbtbl_qualified+` where db_name = '`+mountdbname+`' and schemadrift_flag=1;`});
   
     }
    catch (err)  {

         err_msg = err.message;
        //log sp run in log table
 snowflake.execute({ sqlText:  ` insert into `+sharewatch_sprunlogtbl_qualified+` values ('drop_schemadrift_monitor', '`+mountdbname+`','failed','',current_timestamp(), current_user(), current_session())`});
                
                return err_msg;  // Return a success/error indicator
                }

    
success_msg = "Schema drift monitor is successfully dropped for ".concat(mountdbname);


    //log sp run in log table     
    snowflake.execute({ sqlText:  ` insert into `+sharewatch_sprunlogtbl_qualified+` values ('drop_schemadrift_monitor', '`+mountdbname+`','success','`+success_msg+`',current_timestamp(), current_user(), current_session())`});

     return success_msg
$$;

--create or replace procedure sharewatch.util.update_app_config (PARAMETER_NAME varchar, PARAMETER_VALUE_JSON_STRING varchar)
create procedure if not exists  sharewatch.util.update_app_config (PARAMETER_NAME varchar, PARAMETER_VALUE_JSON_STRING varchar)
returns string
language javascript
execute as caller
as
$$
var sharewatchdbname = 'sharewatch';
var sharewatchutilschema_qualified = 'sharewatch'.concat('.util');
var sharewatch_appconfigtbl_qualified = 'sharewatch'.concat('.util.app_config');
var sharewatch_sprunlogtbl_qualified = 'sharewatch'.concat('.util.sp_runlog');
var parameter_name = PARAMETER_NAME.toUpperCase();
var parameter_value_json_string = PARAMETER_VALUE_JSON_STRING; // Not upper case since CRON Schedule timezones are case sensitive
var sp_argument = parameter_name.concat(',').concat(parameter_value_json_string);
try {
     // delete parameter if exists
     snowflake.execute({ sqlText: `delete from  `+sharewatch_appconfigtbl_qualified+`  where upper(parameter) ='`+parameter_name+`'`});
     //insert new value
     snowflake.execute({ sqlText: `insert into `+sharewatch_appconfigtbl_qualified+` select '`+parameter_name+`', parse_json('`+parameter_value_json_string+`')`});}
    catch(err){
    //log sp run in log table
        error_msg = err.message;
        escaped_error_msg = error_msg.replaceAll("'","\\'");
        snowflake.execute({ sqlText:  ` insert into `+sharewatch_sprunlogtbl_qualified+` values ('update_app_config','`+sp_argument+`','failed','`+escaped_error_msg+`',current_timestamp(), current_user(), current_session())`});
         return error_msg;
    }
success_msg = 'Successfully updated table:'.concat(sharewatch_appconfigtbl_qualified);
//log sp run in log table
snowflake.execute({ sqlText:  ` insert into `+sharewatch_sprunlogtbl_qualified+` values ('update_app_config','`+sp_argument+`','success','`+success_msg+`',current_timestamp(), current_user(), current_session())`});
    
return success_msg;

$$;

--create or replace procedure  sharewatch.util.configure_task (FULLY_QUALIFIEFD_TASK_NAME varchar, CRON_SCHEDULE varchar)
create procedure if not exists  sharewatch.util.configure_task (FULLY_QUALIFIEFD_TASK_NAME varchar, CRON_SCHEDULE varchar)
returns string
language javascript
execute as caller
as
$$
var sharewatchdbname = 'sharewatch';
var sharewatchutilschema_qualified = 'sharewatch'.concat('.UTIL');
var taskname = FULLY_QUALIFIEFD_TASK_NAME.toUpperCase();
var sharewatch_master_task_qualified = taskname;
var sharewatch_monitoreddb_tbl_qualified = 'sharewatch'.concat('.UTIL.databases_monitored');
var sharewatch_sprunlogtbl_qualified = 'sharewatch'.concat('.UTIL.sp_runlog');

var cron_schedule = CRON_SCHEDULE; // Not upper case since CRON Schedule timezones are case sensitive
var sp_argument = taskname.concat(',').concat(cron_schedule);

 try { snowflake.execute({ sqlText: `describe task `+sharewatch_master_task_qualified}); } 
 catch (err) {  
  //log sp run in log table
    error_msg = err.message;
    escaped_error_msg = error_msg.replaceAll("'","\\'");
     snowflake.execute({ sqlText:  ` insert into `+sharewatch_sprunlogtbl_qualified+` values('configure_task','`+sp_argument+`','failed','`+escaped_error_msg+`',current_timestamp(), current_user(), current_session())`});
     return error_msg;
 }

//suspend to alter task, alter task, resume task
 snowflake.execute({ sqlText: `alter task `+sharewatch_master_task_qualified+` suspend;`  }); 
 snowflake.execute({ sqlText: `alter task `+sharewatch_master_task_qualified+`  set schedule= 'USING CRON `+cron_schedule+`';`  });
snowflake.execute({ sqlText: `alter task `+sharewatch_master_task_qualified+` resume;`  }); 
success_msg = "Task successfully altered: ".concat(taskname);

//log sp run in log table
snowflake.execute({ sqlText:  ` insert into `+sharewatch_sprunlogtbl_qualified+` values ('configure_task','`+sp_argument+`','success','`+success_msg+`',current_timestamp(), current_user(), current_session())`});
    
return success_msg;
$$;


--create or replace procedure sharewatch.util.configure_master_task (CRON_SCHEDULE varchar)
create procedure if not exists  sharewatch.util.configure_master_task (CRON_SCHEDULE varchar)
returns string
language javascript
execute as caller
as
$$
var sharewatchdbname = 'sharewatch';
var sharewatchutilschema_qualified = 'sharewatch'.concat('.UTIL');
var sharewatch_sprunlogtbl_qualified = 'sharewatch'.concat('.UTIL.sp_runlog');
var cron_schedule = CRON_SCHEDULE;
var app_config_parameter = 'MASTER_TASK'
var master_taskname = 'MASTER_TASK';
var sharewatch_master_task_qualified = 'sharewatch'.concat('.UTIL.').concat(master_taskname);
var sp_argument = cron_schedule;
try{
    snowflake.execute({ sqlText:  `call `+sharewatchutilschema_qualified+`.configure_task ('`+sharewatch_master_task_qualified+`','`+cron_schedule+`');`});
    snowflake.execute({ sqlText:  `call `+sharewatchutilschema_qualified+`.update_app_config ('`+app_config_parameter+`','{"name": "`+master_taskname+`","schedule": "`+cron_schedule+`"}')`});
}
catch(err)
{
      //log sp run in log table
    error_msg = err.message;
    escaped_error_msg = error_msg.replaceAll("'","\\'");
     snowflake.execute({ sqlText:  ` insert into `+sharewatch_sprunlogtbl_qualified+` values('configure_master_task','`+sp_argument+`','failed','`+escaped_error_msg+`',current_timestamp(), current_user(), current_session())`});
     return error_msg;
}

success_msg = "Task successfully altered: ".concat(master_taskname);
//log sp run in log table
snowflake.execute({ sqlText:  ` insert into `+sharewatch_sprunlogtbl_qualified+` values ('configure_master_task','`+sp_argument+`','success','`+success_msg+`',current_timestamp(), current_user(), current_session())`});
    
return success_msg;

$$;




-- --/*
-- --- Manually Running ShareWatch Stored Procedures (Per Share)

-- -- Assuming you already have a share mounted 
-- show shares;

-- -- Once the share passes the onboarding checklist (available in the ShareWatch whitepaper), set up share 
-- -- for monitoring by enabling the ShareWatch monitor on the shared database
-- call sharewatch.util.setup_schemadrift_monitor('share_example_db');

-- -- Run monitor to check the status of shares
-- call sharewatch.util.run_schemadrift_monitor('share_example_db');

-- -- Run monitor to check the status of all shares being watched
-- call sharewatch.util.run_schemadrift_monitor();

-- -- Drop monitor on a share
-- call sharewatch.util.drop_schemadrift_monitor('share_example_db');

-- -- Configure the master task. Called by configure_master_task (NOT to be called standalone)
-- --call sharewatch.util.configure_task('sharewatch.util.master_task','0 7 * * *  America/New_York');

-- -- Update app parameters including master_task and notification_email. Called by configure_master_task (NOT to be called standalone)
-- --call sharewatch.util.update_app_config('master_task','{"name": "master_task","schedule": "0 7 * * *  America/New_York"}');
-- --call sharewatch.util.update_app_config('notificiation_email','{"email_address": "fady.heiba@snowflake.com"}');

-- -- Configure master task
-- call sharewatch.util.configure_master_task ('* 7 * * *  America/New_York'); -- Runs at 7 AM New York Time Every Day

-- -- Configure email recepient for alerts
-- call sharewatch.util.update_app_config('notification_email','{"email_address": "fady.heiba@snowflake.com"}');

-- -- Manually trigger master task
-- execute task sharewatch.util.master_task;

-- -- Determine task state (suspended or started), specifically the state field
-- describe task sharewatch.util.master_task ;

-- -- Get master task run status. If it returns no rows, it means the task has not run in the last 7 days or is not scheduled to run in the next 8 days.
-- select name,
-- case when state = 'SCHEDULED' then state else 'SUSPENDED' END as status,
-- case when state = 'SCHEDULED' then scheduled_time else NULL END  as next_run_scheduled_at,  
-- case when state = 'SCHEDULED' then lead(state)over(order by scheduled_time desc) else state END as last_run_status, 
-- case when state = 'SCHEDULED' then lead(scheduled_time)over(order by scheduled_time desc) else scheduled_time END as last_run_at,
-- case when state = 'SCHEDULED' then lead(scheduled_from)over(order by scheduled_time desc) else scheduled_from END as last_run_scheduled_from,
-- case when last_run_scheduled_from = 'EXECUTE TASK' then 'MANUALLY TRIGERRED' ELSE 'SCHEDULED RUN' END as last_run_trigerred_by
--   from table(sharewatch.information_schema.task_history(task_name=>'master_task'))
--   qualify row_number() over(order by scheduled_time desc) = 1 ;

-- -- Table tracks all stored procedure runs and their output 
-- select * from sharewatch.UTIL.SP_RUNLOG order by SP_EXEC_TIME desc;

-- -- Table tracks all databases setup for monitoring
-- select db_name from sharewatch.util.databases_monitored order by 1;

-- -- Table tracks all application config parameters including master_task, notification integration, notification email
-- select * from sharewatch.util.app_config;

-- -- Master task that runs monitor for all databases setup for monitoring (default is suspended state)
-- describe task sharewatch.util.master_task;
