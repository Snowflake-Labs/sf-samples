-- Reset the team's memory to one seeded past incident, so every demo opens with
-- "@triage has seen this once before" instead of a count that grows with each run.
--   snow sql --connection <name> -f demo/reset_memory.sql
-- Clears INCIDENT_ROOM.APP.DECISIONS and INCIDENT_ROOM.APP.POSTMORTEMS. Run demo/reset.sql too for a healthy pipeline.
USE ROLE ACCOUNTADMIN;
USE WAREHOUSE INCIDENT_ROOM_WH;

DELETE FROM INCIDENT_ROOM.APP.DECISIONS;
DELETE FROM INCIDENT_ROOM.APP.POSTMORTEMS;

-- The seeded incident: last week's version of today's failure.
INSERT INTO INCIDENT_ROOM.APP.DECISIONS (ID, INCIDENT_ID, FILED_BY, PARTICIPANTS, BODY, FILED_AT)
SELECT 'seed-decision-1', 'INC-SEED-0001', 'EVA', ARRAY_CONSTRUCT('EVA', 'CLAIRE', 'DASH'),
       'Patch the task to use ORDER_AMOUNT rather than reverting the upstream rename. Skip rows where ORDER_AMOUNT is null.',
       DATEADD(day, -7, CURRENT_TIMESTAMP());

INSERT INTO INCIDENT_ROOM.APP.POSTMORTEMS (INCIDENT_ID, BODY, WRITTEN_AT)
SELECT 'INC-SEED-0001', $$## Summary
LOAD_DAILY_REVENUE failed with 000904 invalid identifier 'AMOUNT' after the source team renamed RAW_ORDERS.AMOUNT to ORDER_AMOUNT.

## Root cause
The task SQL still summed AMOUNT. RAW_ORDERS only has ORDER_AMOUNT.

## Fix
Patch replaced SUM(AMOUNT) with SUM(ORDER_AMOUNT) and added WHERE ORDER_AMOUNT IS NOT NULL. Proven on a zero-copy clone (rows and revenue matched the last good load), approved by CLAIRE and DASH, shipped by DASH.

## Who decided
EVA filed: "Patch the task to use ORDER_AMOUNT rather than reverting the upstream rename."

## Follow-ups
- Find whatever redeploys the old task SQL.
- Alert when the task definition changes.$$,
       DATEADD(day, -7, CURRENT_TIMESTAMP());
