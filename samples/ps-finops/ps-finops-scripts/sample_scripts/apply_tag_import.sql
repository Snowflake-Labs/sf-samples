/*
Script goal: Apply ingested list of taggable objects and their corresponding tag values

Instructions: 1) Import completed csv file output from export_taggable_objects_list.sql script into UTIL_DB.ADMIN_TOOLS (same schema where tags were created)
                              !!!      WARNING! Script will not work without first completing this step.     !!!
                a. In Snowsight, go to Data --> Add data --> Load data into table
                b. Browse to or drag in csv file with completed cost_center and environment assignments
                c. In "Select or create a database and schema", set context as UTIL_DB.ADMIN_TOOLS
                d. For table name, type "taggable_objects_import"
                e. Click Next --> Load
              2) Copy this script, paste it into a blank Snowsight window and run
              3) Script should run as accountadmin to have maximum visibility to objects

*/

USE ROLE <% ctx.env.finops_tag_admin_role%>;
USE DATABASE <% ctx.env.finops_acct_db %>;
USE SCHEMA <% ctx.env.finops_tag_schema %>;

declare
    v_sql text;
    v_cnt_cost_center number(16,0) := 0;
    v_cnt_environment number(16,0) := 0;
    cur cursor for select 'ALTER ' ||tag_level|| ' IF EXISTS ' ||object_name|| ' SET TAG <% ctx.env.finops_acct_db %>.admin_tools.cost_center = ''' ||cost_center_tag_value||''';' as sql
                   from <% ctx.env.finops_acct_db %>.<% ctx.env.finops_tag_schema %>.TAGGABLE_OBJECTS_IMPORT
                   where cost_center_tag_value is not null

begin

    OPEN CUR;
    
    for row_var in cur do 
        v_sql := row_var.sql;
        execute immediate :v_sql;
        if (v_sql like '%cost_center%') then
            v_cnt_cost_center := v_cnt_cost_center + 1;
        else
            v_cnt_environment := v_cnt_environment + 1;
        end if;
    end for;

    return 'Done. Set tag counts:\n  cost_center: '||:v_cnt_cost_center || 
            '\n  environment: '||:v_cnt_environment;
end;