# Snowflake Professional Services FinOps Framework

The PS FinOps Framework was developed to help customer's visualize credit and in some cases currency consumption by a Team/Project using common Snowflake objects that the Cost Management Snowsight interface either does not provide, or is not available to those that need to see this data. The data it consumes is from the Snowflake Application telemetry database available in all accounts. We have provided two methods to collect data:
1. Per Account install that includes tables, views, tasks, and optional Streamlit applications.
2. Organizational Account install that includes tables, views, tasks, and optional Streamlit applications. This new org feature aggregates all your account telemetry into one central account. The Organizational Account views do not cover all Account views as of Apr 2025, so it is possible to not have detailed data on new services with it.

The installation and setup is designed to be implemented by Snowflake Solution Architects, but this could be installed by engineers competent in Snowflake metrics, automation, and have the appropriate rights to install. Installation incorporates separation of duties - admins can do the security, database, and tag setup, while a delegated admin can deploy the remaining object code.

The base install prepares for installation of the two Streamlit apps for you to extend and develop more. These are in the POC phase, so optionally you may want to exclude them if you have other preferred visualization tools.

### Prequisites:
- Snowflake Enterprise Edition or higher for Tag use.
- Accountadmin Role rights to setup a new COST_CENTER tag and Snowflake DB access grants.
- Securityadmin Role rights for creating roles and users.
- Snow CLI installed for easy deployment.

## Security Model
Our example creates a delegated FinOps admin user with password or RSA Key Pair to create (or use if the database and schema exists) where FinOps objects reside and optionally the same with the COST_CENTER tag in its own location or same location. We also create a viewer role to view the data and views in FinOps db and Snowflake telemetry database (Admin and viewer have these database roles: GOVERNANCE_VIEWER, OBJECT_VIEWER, USAGE_VIEWER, ORGANIZATION_BILLING_VIEWER, and ORGANIZATION_USAGE_VIEWER). The variable values can be set in the snowflake.yml config file as described below:

- finops_admin_user - User name for finops admin - the service account used to connect to accounts and create objects except the cost_center tag itself.
- finops_db_admin_role - Functional role that can create databases and warehouses along with their respective FinOps objects.
- finops_tag_admin_role - Functional role used to create the tag if it does not exist and apply tags as it will have global apply tag rights.
- finops_viewer_role - Functional role used to grant select rights on objects for read only reporting.

Other things to consider that are not implemented in our solution - granual visibility to specific cost center data based on your persona.

## Why Did We Choose Snowflake Tags?
Tagging is an Enterprise and above feature, but most all of our customer's asking for FinOps are using this tier of Snowflake Accounts. It is flexible and easy to consume because of the Snowflake telemetry database view called tag_references that gathers all the tags together where they are assigned on inherited making for easy consumption. Alternatives that you could look into would be object comments with json or naming conventions and table lookups.

We chose the COST_CENTER name because most HR systems think in terms of Teams, but accountants and finance think in terms of Cost Centers. Cost Center is the more generic term that could include Project names, or Business Units. COST_CENTER is the codified tag name and is not case sensitive. If you have an existing tag, feel free to update the snowflake.yml variable value since this is just a custom tag and not a Snowflake provided tag name.

The one downside that could be an issue is you are limited to 300 values in a Tag. So, this prevents you from doing things like tagging user's with their username or being very granular. You could use other tags and combine ideas, say Environment tag if separating environment costs within an account is a concern.

## Decision Details and Requirements
Detailed cost attribution in FinOps requires the use of object and query tagging. It is assumed that a Schema is attributable to one cost center. In some cases this is not true, so alternative script ideas are provided, but up to you to implement.

Organizational Account (Org V2) installation - Optional use, but recommended for many account customers that have significant usage in more than 1 account. This is using new PuPr feature https://docs.snowflake.com/en/user-guide/organization-accounts Organization Accounts (Org V2/Org 2.0). It aggregates all the remote account telemetry data into one new Org Account. It will incur storage and compute costs and right now is missing a few of the new Snowflake feature views, so not all of the services view data is available, but the core compute and storage metrics work. Organization Accounts enable Organization users and groups, so that is another thing to consider.

For installation that works for most customers, we chose SNOW CLI for installation (ver 3.5.0 minimum), so some command line familiarity is required. Also, ability to setup a process to do the install. There are some steps that are manual, but most objects are deployable code. Scripts sections are manually deployed and require you to read the code to make some decisions. The first scripts will require ACCOUNTADMIN level role to run, so do find someone with that level of privileges. We create a delegated admin for WH and DB object creation for all the FinOps stuff, or you can use SYSADMIN, up to you, just update the snowflake.yml file with names of objects and roles.

For Query and Warehouse attribution costs, a helper function was created such that all queries can use the same logic and one update changes them all. The function is GET_TAG_COST_CENTER and the priority defaults to this - of which you many want to customize to your specific needs:
```sql
    # finops/core/functions/get_tag_cost_center.sql
    SELECT COALESCE(IFF(QUERY_TAG <> '',try_parse_json(QUERY_TAG):"cost_center"::varchar, NULL)
    , user_tag_value -- Add a tag to user/service only when they can always attribute to a cost center.
    , schema_tag_value -- Add a tag to schema for all objects it contains to attribute to a cost center.
    , role_tag_value -- Add a tag to the role for query_history attribution if above are not tagged.
    , warehouse_tag_value -- Add a tag to the Warehouse only if it can be attributed to one cost center.
    , 'Unknown' -- Placeholder value if no tag found (could be telemetry delay). Change to another value as desired.
    ) as TEAM
```

To accommodate cost attribution we will need to tag these objects/queries in Snowflake:
* Query Tags - Optionally used for "shared" resources where it defines the cost center. Only needed if the other methods would not accurately define cost center.
* Roles - Each functional role needs a cost center tag. Access roles shouldn't be used by users directly and schema tag is preferred for schema objects.
* Schemas - Each schema should have one cost center and tagged. If this is not the case, use Query Tags to set the cost center.
* Users - Service accounts or users that need a cost center tag as an override. In the function GET_TAG_COST_CENTER, user is the 2nd check to quickly bailout for service accounts as they should only be used for 1 cost center and for admin human users that only have one cost center, like Snowflake Admin that use many roles, schemas, and warehouses. If this doesn't apply for a user, don't add the tag, rely on other object tags for attribution.
* Warehouses - Optionally used as a last override value. Having dedicated WH's for teams for cost attribution is a quick way to spend extra credits from low utilization and one of the reasons this solution was developed so you don't have to.

See folder sample_scripts for a couple of options to help with tagging. Also we have Tagging Streamlit app in streamlit folder that is simple working example application.

## General Notes:
Snowflake APPLICATIONS are not tagged in this solution. They are not part of the cost attribution model yet.
How to see immediate tag results:
`SELECT SYSTEM$GET_TAG('<DB>.<TAGSCHEMA>.COST_CENTER', 'DBT_ROLE', 'ROLE');`

In this solution, we use delayed telemetry data from Snowflake database for performance & cost reasons. If you need immediate results, you can use the SYSTEM$GET_TAG function to see the immediate results.:
`SELECT * FROM TABLE(SNOWFLAKE.ACCOUNT_USAGE.TAG_REFERENCES_WITH_LINEAGE('<DB>.<TAGSCHEMA>.COST_CENTER')) WHERE OBJECT_NAME = 'DBT_ROLE';`

```sql
--See if existing cost center tag in use (2 hour telemetry delay). Just FYI, code will not re-create, but make sure to point to same db and schema in snowflake.yml to reuse it.
SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.TAG_REFERENCES
WHERE TAG_NAME = 'COST_CENTER';
```

### Change credits to your local currency example (many exist already, but not all):
```sql
WITH MR
AS (
   -- Option 1: Retrieve Metering Rates per date if you have data in these two org views.
   SELECT MAX(EFFECTIVE_RATE) AS METERING_RATE, DATE
   FROM SNOWFLAKE.ORGANIZATION_USAGE.RATE_SHEET_DAILY RS
   WHERE SERVICE_TYPE = 'WAREHOUSE_METERING' --  Alter this to service type data you are joining too.
       AND IS_ADJUSTMENT = FALSE
       AND ACCOUNT_LOCATOR = CURRENT_ACCOUNT()
       AND RS.DATE > (
           SELECT MIN(START_DATE)
           FROM SNOWFLAKE.ORGANIZATION_USAGE.CONTRACT_ITEMS
           WHERE START_DATE <= CURRENT_DATE()
            AND CONTRACT_ITEM IN ('Capacity', 'Additional Capacity')
            AND NVL(EXPIRATION_DATE, '2999-01-01') > END_DATE
           )
    GROUP BY DATE

    -- OR

    -- Option 2 with static value (adjust as required)
    SELECT 3.25 AS METERING_RATE

   )
SELECT
   DATE_TRUNC('DAY', ACH.START_TIME) AS DAY,
   COALESCE(TS.TAG_VALUE, 'SCHEMA:' || ACH.DATABASE_NAME || '.' || ACH.SCHEMA_NAME) COST_CENTER,
   ACH.DATABASE_NAME,
   ACH.SCHEMA_NAME,
   -- Option 1:
   ROUND(SUM(CREDITS_USED * MR.METERING_RATE), 2) AS AC_COST_CURRENCY, -- TOTAL AUTOMATIC CLUSTERING COST IN RESPECTIVE CURRENCY FOR THIS DAY.
   -- Or Option 2:   ROUND(SUM(CREDITS_USED * (SELECT METERING_RATE FROM MR)), 2) AS AC_COST_CURRENCY, -- If a static value.
   ROUND(SUM(NUM_BYTES_RECLUSTERED) / POWER(2, 30), 1) AS RECLUSTERED_GB,
   ROUND(SUM(NUM_BYTES_RECLUSTERED) / POWER(2, 40), 1) AS RECLUSTERED_TB,
   ROUND(SUM(NUM_ROWS_RECLUSTERED) / POWER(10, 6), 1) AS RECLUSTERED_ROWS_MM
FROM SNOWFLAKE.ACCOUNT_USAGE.AUTOMATIC_CLUSTERING_HISTORY ACH
--OPTION 1 only:
JOIN MR ON MR.DATE = DAY -- Add this join if you have per day values
LEFT JOIN SNOWFLAKE.ACCOUNT_USAGE.TAG_REFERENCES TS ON TS.OBJECT_DATABASE = ACH.DATABASE_NAME
   AND TS.OBJECT_NAME = ACH.SCHEMA_NAME
   AND TS.TAG_NAME = '<% ctx.env.finops_tag_name %>'
   AND TS.DOMAIN = 'SCHEMA'
GROUP BY ALL;
```
### Snowsight Dashboard Tips
If using Snowsight Dashboards, setup a custom cost center Snowsight managed filter (Display of `Cost Center (SF)` and name of costcenter - it is case sensitive) in Snowsight and have a daily update of this code to populate the costcenter tag data:
Include the ALL option - Any Value type.
Enable multiple values to be selected.

```sql
--Managed Filter costcenter code:
SELECT DISTINCT TAG_VALUE
FROM SNOWFLAKE.ACCOUNT_USAGE.TAG_REFERENCES TS
WHERE ts.TAG_NAME = 'COST_CENTER'
AND TS.DOMAIN IN ('DATABASE','SCHEMA','ROLE','USER','REPLICATION GROUP','COMPUTE POOL')
ORDER BY 1;
```

## High level installation process
We need to tag objects in every account with current design and potentially have Account FinOps visibility in that account, so we deploy all objects there and expect Organization views (Org V1) to be available (if they are not, skip OrgV1 folder deployment). Org V1 views will exist if the Account Exec team has created an Organization for customer and grouped all their accounts under it. This typically happens when customer signs a contract.

If using the new Organizational Account, install the Org_v2 folder, you don't need the Account folder install for this account.

## Long term maintenance requirements
Once everything has been tagged, more than likely new objects coming in will be missing tags, so in the maintenance section of finops/sample_scripts/tagging_module_create_tags.sql run the tagging scripts to find missing tag objects.

# Installation process
## Command line tool setup on a Mac
We need a mechanism to easily deploy via command line to enable you to use CI/CD tools and for Snowflake the SNOW CLI is the way of the future, so let's get it setup on your local box and would do similar for CI/CD container runner.

Install Snow CLI: https://docs.snowflake.com/en/developer-guide/snowflake-cli/installation/installation

If Snow CLI is already installed, make sure that you are using an up-to-date version. Some process in the installation requires version >= 3.5. If you're having difficulties upgrading Snow CLI version, make sure that you have python >= 3.10.

If you plan on developing improvements to the Streamlit app, you will need to install python. Use the below for help installing.

### Optional Python install for Streamlit local develop use only
Install Brew in your terminal if you haven't already from the web site brew.sh will give this command to do so:
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"

```bash
brew install python@3.12
brew install python@3.13
# pipx helps you not deal with dependency hell of python for pypi python apps.
brew install pipx

# Snow cli install with pipx
pipx install snowflake-cli
pipx upgrade snowflake-cli
```
Open up .zprofile (pico ~/.zprofile) and add these:
```bash
export PIPX_DEFAULT_PYTHON=/opt/homebrew/opt/python@3.12/libexec/bin/python
export SNOWFLAKE_HOME=~/.snowflake/
```
Reopen your terminal or run `source ~/.zprofile` to use these new settings.

### Snow CLI Setup
Switch to using connections.toml for your connection strings as that is the future. So, create it and add a connection.
```bash
cd ~/.snowflake
pico connections.toml

# Example add user/password connection like this (later install selection shows RSA key use)
[demo]
account = "DEMO"
user = "MYSELF"
authenticator = "SNOWFLAKE" # or username_password_mfa if using MFA caching
warehouse = "ADMIN_XSMALL"
role = "ACCOUNTADMIN"

# exit and save
```

### If forced into password for service account
If you are forced into passwords, look into using MFA caching: https://docs.snowflake.com/en/user-guide/security-mfa
```
-- Run this as Accountadmin in account:
ALTER ACCOUNT SET ALLOW_CLIENT_MFA_CACHING = TRUE;

-- Set up env variable to store password securely and not in your code:
pico ~/.zprofile
# add password here like SNOWFLAKE_CONNECTIONS_<connection name>_PASSWORD (don't have a . in your password, it doesn't get passed along to Snowflake correctly from command line it seems):
export SNOWFLAKE_CONNECTIONS_DEMO_PASSWORD="yourgoodpasswordhere"

#exit and save

# Reopen your terminal or run
source ~/.zprofile

# make sure you have an output, otherwise you could lock your account for 15 minutes as for whatever reason, snow cli will try 3 times to login with null password.
echo $SNOWFLAKE_CONNECTIONS_DEMO_PASSWORD

# see connections (will ping MFA if one account uses it)
snow connections list

# test a connection
snow connection test -c demo

# set default connection
snow connection set-default demo
```

## Installation general notes
Installation code below assumes MacOS unless PC instructions are provided. I think for a PC, use TYPE instead of CAT command. Rest should be the same.

You can only have 1 default connection in snow cli, so instead of resetting every time, use the -c ConnectionNameHere appended to the commands below. e.g. `snow sql -c platformadmin -q "select current_account()"`

Run all snow commands from root folder as snowflake.yml needs to be in your current location, use `pwd` command to get your present working directory.

## Step 1. Authentication Setup
We need a user account to deploy with for automation, so let's create this "service account" and enable it to create databases and warehouses. For everything else, we need need ACCOUNTADMIN role and expect you to have a human or other service account with those rights to create roles, tags, grant apply tag on account to FinOps Admin account, and possibly create database and pass grant ownership to FinOps admin role.

Passwords will not be allowed as Snowflake moves to more trusted connections, so might as well get started on updating existing users to RSA keys. Once moved to key, just set password to null to remove it and then it won't show up on audit reports.

Create a new connection with SSO for Account Admin role rights we need for initial setup or use service account and key pair authentication.

```bash
# Add to connections.toml you the platform admin, a human user with Accountadmin role and SSO connection (change as required):
[platformadmin]
account = "ORGNAME-ACCOUNTNAME"
user = "USERNAME"
authenticator = "EXTERNALBROWSER"
warehouse = "ADMIN_XSMALL"
role = "ACCOUNTADMIN"
```



### Optional: If you want to use a service account for deploying:
Next we need to create a FinOps service account. We show below how to create RSA key and create user with it.

```bash
# MacOS commands to create a Stronger key *necessary* for Terraform than our docs show (4096 vs 2048 and aes-256-cbc vs des3).
cd ~
mkdir .ssh
chmod 700 ~/.ssh/
cd .ssh\
openssl genrsa 4096 | openssl pkcs8 -topk8 -v2 aes-256-cbc -inform PEM -out rsa_key_finopsadmin.p8
# enter password: somegoodpassphrasehere
openssl rsa -in rsa_key_finopsadmin.p8 -pubout -out rsa_key_finopsadmin.pub
# enter password: somegoodpassphrasehere  # with copy and paste hit enter twice.

# Make sure they look correct text.
cat rsa_key_finopsadmin.pub
cat rsa_key_finopsadmin.p8

# On a PC
cd $Home
mkdir .ssh
cd .ssh\
openssl genrsa 4096 | openssl pkcs8 -topk8 -v2 aes-256-cbc -inform PEM -out rsa_key_finopsadmin.pem
# enter password: somegoodpassphrasehere
openssl rsa -in rsa_key_finopsadmin.pem -pubout -out rsa_key_finopsadmin.pub
# enter password: somegoodpassphrasehere


# Add to connections.toml the Finops Admin connection string:
[finopsadmin]
account = "ORGNAME-ACCOUNTNAME"
user = "FINOPS_ADMIN_USER"
warehouse = "ADMIN_XSMALL"
role = "FINOPS_ADMIN"
authenticator = "SNOWFLAKE_JWT"
private_key_file = "users/clathrope/.ssh/rsa_key_finopsadmin.p8"

# Add to connections.toml the Tag Admin connection string - which we have same info as FinOps admin for brevity:
[tagadmin]
account = "ORGNAME-ACCOUNTNAME"
user = "FINOPS_ADMIN_USER"
warehouse = "ADMIN_XSMALL"
role = "FINOPS_ADMIN"
authenticator = "SNOWFLAKE_JWT"
private_key_file = "users/clathrope/.ssh/rsa_key_finopsadmin.p8"

# On a Mac
    # add this export item to .zprofile or to each new terminal session manually
    export PRIVATE_KEY_PASSPHRASE=<passphrase here>
    # if you updated your profile, run this to re-run your setup:
    source ~/.zprofile
    echo $PRIVATE_KEY_PASSPHRASE
    # you should see a value.

    # On a Mac/Linux create an OS Env variable to store key
    export RSA_PUB_KEY=`cat ~/.ssh/rsa_key_finopsadmin.pub | grep -v "^-" | tr -d '\n'`
    # echo $RSA_PUB_KEY is how to see a variable.

# On a PC within Powershell enabled command line
    # Read the public key file into a string
    $publicKey = Get-Content -Path "rsa_key_tfadmin.pem" -Raw

    # Remove the header and footer lines and newlines
    $publicKey = $publicKey -replace '-----BEGIN PUBLIC KEY-----' -replace '-----END PUBLIC KEY-----' -replace "`r`n"

    # Output the cleaned public key to verify
    Write-Host $publicKey

    # Set the OS env variable
    $env:RSA_PUB_KEY=$publicKey

    # If you use email addresses for a username, you will need to use env variable:
    $env:FINOPS_ADMIN_USER = '"USER@DOMAIN.COM"'
    ECHO $env:FINOPS_ADMIN_USER


# Now create user with SNOW CLI PC (with Powershell) or MAC is the same. If using CMD only on a PC, use TYPE instead of CAT.
# Create a user in account for creating finops objects after sysadmin like role creates db.
# Note --silent hides the SQL to be run as it will contain sensitive information.
snow sql -c platformadmin -f account_level_scripts/user_creation/create_user.sql --env rsa_public_key=$RSA_PUB_KEY --silent

# Optional: Test alter user command to make sure all is setup okay:
snow sql -c platformadmin -f account_level_scripts/user_creation/alter_user_set_wh.sql

# Optional: Alter the finops admin user account to add RSA key if it doesn't have one now
snow sql -c platformadmin -f account_level_scripts/user_creation/alter_user_set_rsakey.sql --env rsa_public_key=$RSA_PUB_KEY --silent
```


## Deployment Process
After you review each of these files to make sure everything would be appropriate for your environment you can now install. The -c connection tells Snow CLI which connection setup to use that we showed above.

```bash
# Using the Accountadmin Role and Connection:
snow sql -c platformadmin -f account_level_scripts/00_security_and_db_setup.sql

# Delegated Admin Role:
snow sql -c platformadmin -f account_level_scripts/01_tagging_module_create_db_schema_role.sql
snow sql -c finopsadmin -f account_level_scripts/02_cost_per_query_automation.sql
snow sql -c finopsadmin -f account_level_scripts/03_table_storage_detailed_task.sql
# Optional hybrid table snapshot task:
snow sql -c finopsadmin -f account_level_scripts/04_hybrid_storage_detailed_task.sql

# Core function creation:
snow sql -c finopsadmin -f core/functions/get_tag_cost_center.sql

# Account objects:
cat account/tables/*.sql | snow sql -i -c finopsadmin
cat account/views/*.sql | snow sql -i -c finopsadmin
cat account/procedures/*.sql | snow sql -i -c finopsadmin
cat org_v1/views/*.sql | snow sql -i -c finopsadmin

# Streamlit apps RBAC
snow sql -c finopsadmin -f streamlit/00_db_ar.sql

# Tagging Streamlit app
snow sql -c finopsadmin -f streamlit/finops_tag/deploy/db_objects.sql
snow sql -c finopsadmin -f streamlit/finops_tag/deploy/task_object_mapping_proc.sql
snow streamlit deploy tag_app -c finopsadmin --replace

# Usage Streamlit app
snow streamlit deploy usage_app -c finopsadmin --replace


# Enable jobs:
snow sql -c finopsadmin -f account_level_scripts/05_task_enable_disable.sql
```

## Organizational Account (Org V2) installation
Optional use for Organizational Accounts only.

```bash
# Accountadmin Role:
snow sql -f org_v2/account_level_scripts/00_security_and_db_setup.sql
# Delegated Admin Role:
snow sql -f org_v2/account_level_scripts/01_tagging_module_create_db_schema_role.sql
snow sql -f org_v2/account_level_scripts/02_cost_per_query_automation.sql
snow sql -f org_v2/account_level_scripts/03_table_storage_detailed_task.sql

# These object scripts assume connection is connecting to the created db and schema and using delegated admin:
snow sql -f core/functions/get_tag_cost_center.sql

cat org_v2/tables/*.sql | snow sql -i
cat org_v2/views/*.sql | snow sql -i
cat org_v2/procedures/*.sql | snow sql -i
```


### File Hierarchy
```
.
├── CHANGELOG.md
├── LEGAL.md
├── LICENSE.txt
├── README.md
├── account
│   ├── procedures
│   │   ├── cost_per_query_account.sql
│   │   └── get_tag_cost_center.sql
│   ├── tables
│   │   ├── hybrid_storage_detailed.sql
│   │   ├── sf_credit_tables.sql
│   │   └── table_storage_detailed.sql
│   └── views
│       ├── autoclustering_schema_credits_day.sql
│       ├── autoclustering_schema_currency_day.sql
│       ├── compute_and_qas_cc_avg_per_query_credits_day.sql
│       ├── compute_and_qas_cc_avg_per_query_currency_day.sql
│       ├── compute_and_qas_cc_credits_day.sql
│       ├── compute_and_qas_cc_credits_wow.sql
│       ├── compute_and_qas_cc_currency_day.sql
│       ├── compute_and_qas_wh_cc_credits_day.sql
│       ├── compute_and_qas_wh_cc_currency_day.sql
│       ├── logging_events_cc_credits_day.sql
│       ├── materialized_view_cc_credits_day.sql
│       ├── query_acceleration_cc_credits_day.sql
│       ├── query_acceleration_cc_currency_day.sql
│       ├── querycounts_cc_day.sql
│       ├── replication_data_transfer_cc_credits_day.sql
│       ├── replication_group_data_transfer_cc_credits_day.sql
│       ├── serverless_task_cc_credits_day.sql
│       ├── snowpipe_costs_cc_credits_day.sql
│       ├── sos_cc_credits_day.sql
│       ├── spcs_cc_credits_day.sql
│       ├── storage_cc_credits_day.sql
│       ├── storage_cc_currency_day.sql
│       └── table_recreation_schema_tabletype_count.sql
├── account_level_scripts
│   ├── 00_security_and_db_setup.sql
│   ├── 01_tagging_module_create_db_schema_role.sql
│   ├── 02_cost_per_query_automation.sql
│   ├── 03_table_storage_detailed_task.sql
│   └── 04_hybrid_storage_detailed_task.sql
├── core
│   ├── functions
│   │   └── get_tag_cost_center.sql
│   └── user_creation
│       ├── alter_user_set_rsakey.sql
│       └── create_user.sql
├── org_v1
│   └── views
│       ├── orgv1_account_currency_day.sql
│       ├── orgv1_cloud_services_credit_day.sql
│       ├── orgv1_compute_wh_account_credit_day.sql
│       ├── orgv1_contract_actual_and_forecast_linear.sql
│       ├── orgv1_remaining_balance_currency.sql
│       ├── orgv1_service_account_currency_day.sql
│       ├── orgv1_servicecategory_account_currency_day.sql
│       ├── orgv1_storage_account_currency_day.sql
│       └── orgv1_wh_account_count_day.sql
├── org_v2
│   ├── orgv2_level_scripts
│   │   ├── 00_orgv2_security_and_db_setup.sql
│   │   ├── 01_orgv2_tagging_module_create_db_schema_role.sql
│   │   ├── 02_orgv2_cost_per_query_automation.sql
│   │   └── 03_orgv2_table_storage_detailed_task.sql
│   ├── procedures
│   │   └── cost_per_query_org.sql
│   ├── tables
│   │   ├── create_table_storage_detailed_org.sql
│   │   └── sf_credit_tables.sql
│   └── views
│       ├── org_account_currency_day.sql
│       ├── org_autoclustering_schema_credits_daily.sql
│       ├── org_cloud_services_credits_day.sql
│       ├── org_compute_and_qas_cc_credits_day.sql
│       ├── org_compute_and_qas_cc_credits_wow.sql
│       ├── org_compute_and_qas_cc_currency_day.sql
│       ├── org_contract_actual_and_forecast_linear.sql
│       ├── org_logging_events_cc_credits_day.sql
│       ├── org_materialized_view_cc_credits_day.sql
│       ├── org_query_acceleration_cc_credits_day.sql
│       ├── org_query_acceleration_cc_currency_day.sql
│       ├── org_querycounts_cc_day.sql
│       ├── org_snowpipe_costs_cc_credits_day.sql
│       ├── org_sos_cc_credits_day.sql
│       ├── org_storage_account.sql
│       ├── org_storage_credits_approx_cc_day.sql
│       ├── org_storage_currency_approx_cc_day.sql
│       └── org_table_recreation_schema_count_day.sql
├── sample_scripts
│   ├── apply_tag_import.sql
│   ├── cortex
│   │   ├── orgv2_storage_forecast_model.sql
│   │   └── storage_forecast_model.sql
│   └── tagging_module_create_tags.sql
├── snowflake.yml
└── streamlit
    ├── finops_tag
    │   ├── db_objects.sql
    │   ├── stage
    │   │   └── streamlit_main.py
    │   └── task_object_mapping_proc.sql
    ├── finops_usage
    │   ├── finops_app_setup.sql
    │   └── stage
    │       ├── bug-sno-blue.png
    │       ├── compute_qas_cost_attribution.py
    │       ├── environment.yml
    │       ├── serverless_compute_optimization.py
    │       ├── storage_cost_attribution.py
    │       └── streamlit_main.py
    └── finops_usage_sis_app_objects.sql
```

### Testing access after install
```sql
USE ROLE <% ctx.env.finops_viewer_role %>;

select * from snowflake.account_usage.tag_references where tag_name = '<% ctx.env.finops_tag_name %>';

--Show all grants to role
show grants to role <% ctx.env.finops_viewer_role %>;
show grants to role <% ctx.env.finops_db_admin_role %>;
```
