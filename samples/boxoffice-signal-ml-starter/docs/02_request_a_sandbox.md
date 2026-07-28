# 02 — Request a dedicated sandbox database

You need a place you **fully control** — where you can create schemas, tables, views,
stages, and run Cortex AISQL — without touching production or needing a ticket for every
change. Ask your data/platform team for a dedicated **sandbox database**.

Building in an isolated sandbox (rather than an existing shared database) matters because:

- You'll iterate fast and create/drop objects constantly — that shouldn't risk shared data.
- Ingested third-party data must stay contained and easy to purge.
- Clear ownership keeps you compliant: your sandbox, your data-access responsibility.

## What to ask for

- A **database** you own (e.g. `MY_SANDBOX_DB`).
- A **role** granted to you with **full privileges inside that database** (create schema,
  table, view, stage, function, task) — scoped to the sandbox only.
- Rights to use **Snowflake Cortex** functions (AISQL) — e.g. usage on the Cortex
  functions and a warehouse that permits them.
- A **warehouse** you may use (an XS/S is plenty to start).
- Optionally, an **external access integration** if you'll pull APIs from inside Snowflake
  (many people instead pull locally with CoCo and load results — either is fine).

## Copy-paste request for your data team

> **Subject: Request for a personal sandbox database (analytics research)**
>
> Hi team — I'm prototyping a data-science pipeline with Cortex Code and need an isolated
> place to build. Could you set up the following?
>
> 1. A dedicated database, e.g. `MY_SANDBOX_DB`, that I own.
> 2. A role (e.g. `MY_SANDBOX_ROLE`) granted to my user, with full build privileges
>    **inside that database only** (create/modify schema, table, view, stage, function,
>    task).
> 3. Usage on a warehouse (an XS is fine) and permission to call Snowflake Cortex / AISQL
>    functions.
> 4. (Optional) An external access integration if I'll call external APIs from inside
>    Snowflake — otherwise I'll ingest locally and load results.
>
> All data I bring in will be third-party data I ingest under my own access; I'll keep it
> contained in this database and won't reshare it. Happy to follow any governance you
> require.
>
> Thanks!

## Reference: what the grants roughly look like

Your admin can adapt this. (You typically won't run it yourself — it's here so you can
speak your team's language.)

```sql
-- Admin-run, illustrative only
CREATE DATABASE IF NOT EXISTS MY_SANDBOX_DB;
CREATE ROLE IF NOT EXISTS MY_SANDBOX_ROLE;

GRANT OWNERSHIP ON DATABASE MY_SANDBOX_DB TO ROLE MY_SANDBOX_ROLE COPY CURRENT GRANTS;
GRANT USAGE ON WAREHOUSE MY_XS_WH TO ROLE MY_SANDBOX_ROLE;

-- full build rights inside the sandbox
GRANT ALL ON DATABASE MY_SANDBOX_DB TO ROLE MY_SANDBOX_ROLE;
GRANT ALL ON ALL SCHEMAS IN DATABASE MY_SANDBOX_DB TO ROLE MY_SANDBOX_ROLE;
GRANT ALL ON FUTURE SCHEMAS IN DATABASE MY_SANDBOX_DB TO ROLE MY_SANDBOX_ROLE;

-- assign to you
GRANT ROLE MY_SANDBOX_ROLE TO USER <your_username>;
```

## Once you have it

Update your CoCo connection defaults so you don't repeat yourself:

```bash
snow connection set-default my_sandbox
snow sql --connection my_sandbox -q "USE ROLE MY_SANDBOX_ROLE; USE DATABASE MY_SANDBOX_DB; SELECT CURRENT_DATABASE(), CURRENT_ROLE();"
```

Then tell CoCo:

> "My sandbox is `MY_SANDBOX_DB` and my role is `MY_SANDBOX_ROLE`. Use these for everything.
> Create a `RESEARCH` schema and confirm I have build rights."

Next: **`docs/03_identify_your_sources.md`**.
