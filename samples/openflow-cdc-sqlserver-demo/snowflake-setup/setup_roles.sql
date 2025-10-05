-- ============================================================================
-- Openflow SPCS Quickstart: Core Snowflake Setup
-- ============================================================================
-- This script sets up the core Snowflake components required for Openflow SPCS
-- including the admin role, network rules, and required configurations.
--
-- Prerequisites:
--   - ACCOUNTADMIN role or equivalent privileges
--   - Snowflake account in AWS or Azure Commercial Regions
--
-- Duration: ~10 minutes
-- ============================================================================

-- Step 1: Create Openflow Admin Role
-- ----------------------------------------------------------------------------
USE ROLE ACCOUNTADMIN;

-- Create the Openflow admin role
CREATE ROLE IF NOT EXISTS OPENFLOW_ADMIN;

-- Grant necessary privileges
GRANT CREATE DATABASE ON ACCOUNT TO ROLE OPENFLOW_ADMIN;
GRANT CREATE COMPUTE POOL ON ACCOUNT TO ROLE OPENFLOW_ADMIN;
GRANT CREATE INTEGRATION ON ACCOUNT TO ROLE OPENFLOW_ADMIN;
GRANT BIND SERVICE ENDPOINT ON ACCOUNT TO ROLE OPENFLOW_ADMIN;

-- Grant role to current user and ACCOUNTADMIN
GRANT ROLE OPENFLOW_ADMIN TO ROLE ACCOUNTADMIN;
GRANT ROLE OPENFLOW_ADMIN TO USER IDENTIFIER(CURRENT_USER());


-- Step 2: Create Snowflake Deployments Network Rule
-- ----------------------------------------------------------------------------
USE ROLE OPENFLOW_ADMIN;

-- Create network rule for Snowflake Openflow deployments
CREATE OR REPLACE NETWORK RULE snowflake_deployment_network_rule
  MODE = EGRESS
  TYPE = IPV4
  VALUE_LIST = ('10.16.0.0/12');

-- Verify the network rule
DESC NETWORK RULE snowflake_deployment_network_rule;


-- Step 3: Add Network Rule to Account Network Policy (OPTIONAL)
-- ----------------------------------------------------------------------------
-- NOTE: Only required if your account has a network policy configured
-- Run this section ONLY if the SHOW command below returns results

USE ROLE ACCOUNTADMIN;

-- Check for account-level network policies
SHOW NETWORK POLICIES IN ACCOUNT;

-- If network policies exist, uncomment and run the following command:
-- Replace <YOUR_ACCOUNT_LEVEL_NETWORK_POLICY_NAME> with your actual policy name
/*
ALTER NETWORK POLICY <YOUR_ACCOUNT_LEVEL_NETWORK_POLICY_NAME> 
  ADD ALLOWED_NETWORK_RULE_LIST = (snowflake_deployment_network_rule);
*/


-- Step 4: Enable BCR Bundle 2025_06 (CONDITIONAL)
-- ----------------------------------------------------------------------------
-- NOTE: Only required if you plan to use Database CDC, SaaS, Streaming, or Slack connectors
-- Check the bundle status first, then enable only if needed

USE ROLE ACCOUNTADMIN;

-- Check if BCR Bundle 2025_06 is already enabled
CALL SYSTEM$BEHAVIOR_CHANGE_BUNDLE_STATUS('2025_06');

-- If the result shows 'DISABLED', uncomment and run the following command:
/*
CALL SYSTEM$ENABLE_BEHAVIOR_CHANGE_BUNDLE('2025_06');
*/


-- Step 5: Verify Setup
-- ----------------------------------------------------------------------------
-- Verify role exists
SHOW ROLES LIKE 'OPENFLOW_ADMIN';

-- Verify grants
SHOW GRANTS TO ROLE OPENFLOW_ADMIN;

-- Verify network rule
DESC NETWORK RULE snowflake_deployment_network_rule;

-- ============================================================================
-- Setup Complete!
-- ============================================================================
-- Next Steps:
--   1. Create Openflow deployment via Snowsight UI
--   2. Run quickstart_runtime_role.sql to create runtime resources
--
-- Documentation: 
--   https://docs.snowflake.com/en/user-guide/data-integration/openflow/setup-openflow-spcs-sf
-- ============================================================================

