-- CONSENT MANAGEMENT DEMO, CONSUMER ACCOUNT

-- STEP 2. Run from here after running STEP 1 in PROVIDER ACCOUNT

-- Create warehouse, database, and schema for demo
use role accountadmin;
CREATE WAREHOUSE CONSENT_CONSUMER_DEMO_WH WITH WAREHOUSE_SIZE='XSmall'  STATEMENT_TIMEOUT_IN_SECONDS=15 STATEMENT_QUEUED_TIMEOUT_IN_SECONDS=15;
USE WAREHOUSE CONSENT_CONSUMER_DEMO_WH;
CREATE DATABASE CONSENT_CONSUMER_DEMO;
CREATE SCHEMA CONSENT_CONSUMER_DEMO.DEMO;

USE CONSENT_CONSUMER_DEMO.DEMO;

-- show shares then create the DB from the share
show shares;

-- replace account info with data from show shares above
CREATE DATABASE CONSENT_PROV FROM SHARE <ACCOUNT INFO>.CONSENT_SHARE;

-- select to see the data shared
select * from CONSENT_PROV.demo.customer_data_view;

-- select to see if there are deletes to process
select * from CONSENT_PROV.demo.delete_requests_view;

-- create table to hold delete request ack
CREATE or replace TABLE data_deletion_ack (
  deletion_request_id integer,
  delete_acknowledged timestamp
);

-- share the acks back to the provider account - place provider account ID here
CREATE SHARE consent_delete_ack_share;
grant usage on database CONSENT_CONSUMER_DEMO to share consent_delete_ack_share;
grant usage on schema CONSENT_CONSUMER_DEMO.DEMO to share consent_delete_ack_share;
grant select on CONSENT_CONSUMER_DEMO.DEMO.data_deletion_ack to share consent_delete_ack_share;
alter share consent_delete_ack_share add accounts=<PROVIDER ACCOUNT HERE>;

-- task to take delete requests and process them
CREATE OR REPLACE TASK process_delete_requests
  WAREHOUSE = 'CONSENT_CONSUMER_DEMO_WH'
  SCHEDULE = '60 minute'
  AS
  EXECUTE IMMEDIATE
  $$
  DECLARE
    delete_req_cursor cursor for select id, user_id from CONSENT_PROV.demo.delete_requests_view;
  BEGIN
    for record in delete_req_cursor do
      -- go do the delete wherever this data has gone using record.user_id
      let delete_id integer := record.id;
      insert into CONSENT_CONSUMER_DEMO.DEMO.data_deletion_ack(deletion_request_id, delete_acknowledged) values (:delete_id, current_timestamp());
    end for;
  END;
  $$;

-- END OF STEP 2.  Return to the provider to execute STEP 3 and STEP 4.

-- STEP 5. Check shared data again and delete request. Process by running task.

-- select to see the data shared, notice data has changed.
select * from CONSENT_PROV.demo.customer_data_view;

-- select to see if there are deletes to process - now there is one.
select * from CONSENT_PROV.demo.delete_requests_view;

-- execute the task to process the request
EXECUTE TASK process_delete_requests;

-- verify that it made the ack (may take a little time)
select * from data_deletion_ack;

-- END OF STEP 5.  Return to provider for STEP 6.

-- STEP 7. Check delete requests

-- select to see if there are deletes to process - notice we can not see acknowledged ones
select * from CONSENT_PROV.demo.delete_requests_view;

 -- cleanup
ALTER TASK process_delete_requests SUSPEND;
ALTER WAREHOUSE CONSENT_CONSUMER_DEMO_WH SUSPEND;

-- END OF STEP 7
