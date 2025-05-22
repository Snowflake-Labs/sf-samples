use role <% ctx.env.finops_db_admin_role %>;
USE DATABASE <% ctx.env.finops_sis_db %>;
USE SCHEMA <% ctx.env.finops_sis_tag_sc %>;

-- Create a task to populate the data daily:
CREATE OR REPLACE TASK OBJECT_COSTCENTER_MAPPING_POP
warehouse = <% ctx.env.admin_wh %> --Choose a warehouse name and size that can handle the data volume.
schedule = 'USING CRON 0 0 * * * America/New_York'
AS
CALL PROC_POP_OBJECT_COSTCENTER_MAPPING();


--Enable the task:
ALTER TASK OBJECT_COSTCENTER_MAPPING_POP RESUME;

-- Optional if role has execute task on account privileges:
-- EXECUTE TASK OBJECT_COSTCENTER_MAPPING_POP;
