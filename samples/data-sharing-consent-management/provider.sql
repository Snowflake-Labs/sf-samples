-- CONSENT MANAGEMENT DEMO, PROVIDER ACCOUNT

-- STEP 1. START HERE.

-- Create warehouse, database, and schema for demo
use role accountadmin;
CREATE WAREHOUSE CONSENT_PROV_DEMO_WH WITH WAREHOUSE_SIZE='XSmall'  STATEMENT_TIMEOUT_IN_SECONDS=15 STATEMENT_QUEUED_TIMEOUT_IN_SECONDS=15;
USE WAREHOUSE CONSENT_PROV_DEMO_WH;
CREATE DATABASE CONSENT_PROV_DEMO;
CREATE SCHEMA CONSENT_PROV_DEMO.DEMO;

USE CONSENT_PROV_DEMO.DEMO;

-- create a table to hold the data partner info
CREATE or replace TABLE data_partners (
  id integer,
  name varchar(100) not null,
  snowflake_account varchar(10) null,
  active boolean default true
);

-- insert data partner info - place second account ID here
insert into data_partners(id, name, snowflake_account) values
 (1, 'Service provider', 'PLACEHOLDER'),
 (2, 'Fraud detector', '<CONSUMER ACCOUNT ID>');

-- verify the data in the data partners table
select * from data_partners;

-- Create consumer data with generated data
CREATE OR REPLACE TABLE consumer_data AS
SELECT sha1(seq4()) as user_id,
  'user'||seq4()||'_'||uniform(1, 3, random(1))||'@email.com' as email,
  case when uniform(1,6,random(2))=1 then 'Less than $20,000'
       when uniform(1,6,random(2))=2 then '$20,000 to $34,999'
       when uniform(1,6,random(2))=3 then '$35,000 to $49,999'
       when uniform(1,6,random(2))=3 then '$50,000 to $74,999'
       when uniform(1,6,random(2))=3 then '$75,000 to $99,999'
  else 'Over $100,000' end as household_income,
  round(18+uniform(0,10,random(3))+uniform(0,50,random(4)),-1)+5*uniform(0,1,random(5)) as age_band,
    case when uniform(1,10,random(6))<4 then 'Single'
       when uniform(1,10,random(6))<8 then 'Married'
       when uniform(1,10,random(6))<10 then 'Divorced'
  else 'Widowed' end as marital_status
  FROM table(generator(rowcount => 1000000));

-- verify generated consumer data
select * from consumer_data;

-- create a data share permission table
-- this table joins the consumer data with the data partners when share is allowed (and not revoked)
create or replace table data_share_permissions as
select c.user_id, p.id as partner_id,
 dateadd(minute, uniform(1, 525600, random(1)), ('2021-01-01'::timestamp)) as granted_date,
 case when uniform(1,20,random(2))=1 then CURRENT_TIMESTAMP()
  else null::timestamp end as revoke_date
from
         consumer_data as c sample (75),    -- 75% of rows in consumer_data
         data_partners as p;

-- verify data share permissions data
select * from data_share_permissions;

-- create table for delete requests, if a granted permission is later revoked
CREATE or replace TABLE data_deletion_requests (
  id integer autoincrement,
  user_id VARCHAR(40),
  partner_id integer,
  delete_requested timestamp,
  delete_acknowledged timestamp null
);

-- create view joining to permissions, for share
create or replace secure view customer_data_view as
select d.user_id, d.email, d.household_income, d.age_band, d.marital_status, p.granted_date
from consumer_data d
 inner join data_share_permissions p on p.user_id=d.user_id
 inner join data_partners dp on dp.id=p.partner_id
where p.revoke_date is null and
 dp.snowflake_account=current_account() and dp.active=true;

-- create view for delete requests, for share
create or replace secure view delete_requests_view as
  select r.id, r.user_id, r.delete_requested
 from data_deletion_requests r
  inner join data_partners dp on dp.id=r.partner_id
 where r.delete_acknowledged is null and dp.snowflake_account=current_account();

-- create share for views - place second account ID here
CREATE SHARE consent_share;
grant usage on database CONSENT_PROV_DEMO to share consent_share;
grant usage on schema CONSENT_PROV_DEMO.DEMO to share consent_share;
grant select on CONSENT_PROV_DEMO.DEMO.customer_data_view to share consent_share;
grant select on CONSENT_PROV_DEMO.DEMO.delete_requests_view to share consent_share;
alter share consent_share add accounts=<CONSUMER ACCOUNT ID>;

-- END OF STEP 1.  Go to the consumer account to execute STEP 2.

-- STEP 3. Execute from here to mount share from consumer and create task.

-- show shares then create the DB from the share of the ack
show shares;

-- replace account info with data from show shares above
CREATE DATABASE PARTNER_2_DELETE_ACK_DB FROM SHARE <ACCOUNT INFO>.CONSENT_DELETE_ACK_SHARE;

-- create the task to resolve the delete requests
-- task to take delete requests and process them
CREATE OR REPLACE TASK process_delete_acks
  WAREHOUSE = 'CONSENT_PROV_DEMO_WH'
  SCHEDULE = '60 minute'
  AS
  EXECUTE IMMEDIATE
  $$
  DECLARE
    delete_ack_cursor cursor for select DELETION_REQUEST_ID from PARTNER_2_DELETE_ACK_DB.DEMO.DATA_DELETION_ACK;
  BEGIN
    for record in delete_ack_cursor do
      let del_req_id integer := record.DELETION_REQUEST_ID;
      update CONSENT_PROV_DEMO.DEMO.data_deletion_requests set delete_acknowledged=current_timestamp() where id=:del_req_id;
    end for;
  END;
  $$;

-- END OF STEP 3.  Now all objects are created.

-- STEP 4. Now mark a permission as revoked and create a delete request

-- now someone comes along and revokes their permission
update data_share_permissions set revoke_date=current_timestamp() where user_id='7c5ca14c59916165c62cc4b932569195d05fd907' and partner_id=2;
insert into data_deletion_requests(user_id, partner_id, delete_requested)
values ('7c5ca14c59916165c62cc4b932569195d05fd907', 2, current_timestamp());

-- END OF STEP 4. Return to consumer for STEP 5.

-- STEP 6. Check for delete acknowledgements and process them with the task

-- check for delete acks
select * from PARTNER_2_DELETE_ACK_DB.DEMO.DATA_DELETION_ACK;

-- execute the task to process the ack
EXECUTE TASK process_delete_acks;

-- make sure request was updated - may take a bit of time
select * from data_deletion_requests;

-- cleanup
ALTER TASK process_delete_acks SUSPEND;
ALTER WAREHOUSE CONSENT_PROV_DEMO_WH SUSPEND;

-- END OF STEP 6. Return to consumer for STEP 7.
