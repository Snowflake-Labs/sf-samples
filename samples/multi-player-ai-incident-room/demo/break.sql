-- Multi-player AI Incident Room demo: simulate an upstream schema change that breaks the nightly load.
-- The upstream team renames RAW_ORDERS.AMOUNT -> ORDER_AMOUNT; LOAD_DAILY_REVENUE then fails.
--   snow sql --connection <name> -f demo/break.sql

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE INCIDENT_ROOM_WH;
ALTER TABLE INCIDENT_ROOM_DEMO.SALES.RAW_ORDERS RENAME COLUMN AMOUNT TO ORDER_AMOUNT;
-- Run the (suspended) task now so the failure shows up in task history.
EXECUTE TASK INCIDENT_ROOM_DEMO.SALES.LOAD_DAILY_REVENUE;
