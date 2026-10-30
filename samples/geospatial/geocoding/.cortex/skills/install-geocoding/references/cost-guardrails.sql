-- ============================================================================
-- cost-guardrails.sql — OPT-IN spending limits for the geocoding install
-- ============================================================================
-- Not run by the installer. Review, set the placeholders, then run as
-- ACCOUNTADMIN:
--   snow sql -c <conn> -D email=<you@company.com> -D credits=<monthly> -f cost-guardrails.sql
--
-- Two independent guardrails:
--   1. A resource monitor on the two build/benchmark warehouses, which
--      SUSPENDS them at the quota. Resource monitors cover warehouses only.
--   2. A custom budget over the libpostal compute pool and the GEOCODING
--      database (the tester app runs inside it). Budgets do not suspend
--      anything; they email when the month is forecast to exceed the limit.
--
-- Idle cost is already low without these: the libpostal service suspends after
-- 30 min without calls and the pool 10 min after that.
-- ============================================================================

USE ROLE ACCOUNTADMIN;

-- ---------------------------------------------------------------------------
-- 1. Resource monitor: hard stop for GEOCODING_BUILD_WH / GEOCODING_XS_WH
-- ---------------------------------------------------------------------------
CREATE RESOURCE MONITOR IF NOT EXISTS GEOCODING_WH_MONITOR
  WITH CREDIT_QUOTA = &{credits}
  FREQUENCY = MONTHLY
  START_TIMESTAMP = IMMEDIATELY
  TRIGGERS
    ON 80 PERCENT DO NOTIFY
    ON 100 PERCENT DO SUSPEND
    ON 110 PERCENT DO SUSPEND_IMMEDIATE;

ALTER WAREHOUSE IF EXISTS GEOCODING_BUILD_WH SET RESOURCE_MONITOR = GEOCODING_WH_MONITOR;
ALTER WAREHOUSE IF EXISTS GEOCODING_XS_WH    SET RESOURCE_MONITOR = GEOCODING_WH_MONITOR;

-- ---------------------------------------------------------------------------
-- 2. Custom budget: email alert for the SPCS pool and the GEOCODING database
-- ---------------------------------------------------------------------------
-- The email address must be verified in Snowsight (Profile > Email).
CREATE NOTIFICATION INTEGRATION IF NOT EXISTS GEOCODING_BUDGET_EMAIL
  TYPE = EMAIL
  ENABLED = TRUE
  ALLOWED_RECIPIENTS = ('&{email}')
  COMMENT = '{"origin":"sf_sit-is-geocoding","name":"oss-geocoding","version":{"major":1,"minor":0},"attributes":{"is_quickstart":1,"source":"sql"}}';
GRANT USAGE ON INTEGRATION GEOCODING_BUDGET_EMAIL TO APPLICATION SNOWFLAKE;

CREATE SCHEMA IF NOT EXISTS GEOCODING.BUDGETS
  COMMENT = '{"origin":"sf_sit-is-geocoding","name":"oss-geocoding","version":{"major":1,"minor":0},"attributes":{"is_quickstart":1,"source":"sql"}}';
CREATE SNOWFLAKE.CORE.BUDGET IF NOT EXISTS GEOCODING.BUDGETS.GEOCODING_BUDGET();

CALL GEOCODING.BUDGETS.GEOCODING_BUDGET!SET_SPENDING_LIMIT(&{credits});
CALL GEOCODING.BUDGETS.GEOCODING_BUDGET!SET_EMAIL_NOTIFICATIONS('GEOCODING_BUDGET_EMAIL', '&{email}');

CALL GEOCODING.BUDGETS.GEOCODING_BUDGET!ADD_RESOURCE(
  SYSTEM$REFERENCE('COMPUTE_POOL', 'GEOCODING_LIBPOSTAL_POOL', 'SESSION', 'applybudget'));
CALL GEOCODING.BUDGETS.GEOCODING_BUDGET!ADD_RESOURCE(
  SYSTEM$REFERENCE('DATABASE', 'GEOCODING', 'SESSION', 'applybudget'));

CALL GEOCODING.BUDGETS.GEOCODING_BUDGET!GET_LINKED_RESOURCES();

-- ---------------------------------------------------------------------------
-- Remove the guardrails
-- ---------------------------------------------------------------------------
-- DROP SNOWFLAKE.CORE.BUDGET GEOCODING.BUDGETS.GEOCODING_BUDGET;
-- DROP NOTIFICATION INTEGRATION GEOCODING_BUDGET_EMAIL;
-- ALTER WAREHOUSE GEOCODING_BUILD_WH UNSET RESOURCE_MONITOR;
-- ALTER WAREHOUSE GEOCODING_XS_WH UNSET RESOURCE_MONITOR;
-- DROP RESOURCE MONITOR GEOCODING_WH_MONITOR;
