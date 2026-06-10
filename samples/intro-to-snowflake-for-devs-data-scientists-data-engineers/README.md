# Intro to Snowflake - Course Overview

## Repository Use

The code in this repository is for use with the Introduction to Snowflake for Devs, Data Scientists, and Data Engineers course. This course can be found on several paltforms including Coursera, LinkedIn, and DataCamp (coming soon).

This repository contains notebooks built around the **Tasty Bytes** fictional food truck dataset.

## Notebooks

### Learner_Reference_Code.ipynb

The companion code notebook for the video lessons. It mirrors the SQL and Python demonstrated in the course videos, organized by module and section. Some cells include additional setup code (e.g., `CREATE OR REPLACE`, `COPY INTO`) so they can be run independently.

**Module 1** — Snowflake fundamentals:
- Worksheets and basic queries
- Virtual warehouses (create, resize, multi-cluster, auto-suspend)
- Stages and bulk data ingestion (`COPY INTO`)
- Tables (create, insert, drop, undrop, dynamic tables)
- Views (standard, secure, materialized)
- Semi-structured data (VARIANT, JSON traversal, FLATTEN)

**Module 2** — Advanced data management:
- Time Travel (AT, OFFSET, BEFORE, UNDROP)
- Permanent, transient, and temporary tables
- Zero-copy cloning (tables, schemas, databases)
- Resource monitors
- User-Defined Functions (SQL, Python) and UDTFs
- Stored procedures
- Role-Based Access Control (RBAC)
- Snowpark DataFrames and the VS Code extension
- Snowflake CLI

**Module 3** — Engineering, AI, and applications:
- Snowpipe (continuous ingestion with AUTO_INGEST)
- Snowflake Cortex LLM functions (COMPLETE, SUMMARIZE, SENTIMENT)
- Cortex Search Service (managed RAG)
- Cortex CLI
- Snowpark ML (preprocessing, XGBoost model training)
- Streamlit in Snowflake (interactive data apps)

### Assignment_Code.ipynb

Setup code that must be run **before** each hands-on assignment. It provisions the databases, schemas, tables, stages, and sample data needed for the exercises. Sections include:

- **Module 1 — Worksheets**: Creates `tasty_bytes_sample_data` database, menu table, and loads data from S3.
- **Module 1 — Stages & Ingestion**: Builds the full `tasty_bytes` database with all schemas, tables, file formats, and loads data.
- **Module 1 — Tables**: Creates `test_database`/`test_schema` with sample tables and a dynamic table example.
- **Module 2 — Time Travel**: Clones the truck table and intentionally corrupts it so learners can practice recovery.
- **Module 2 — Applications**: Provides a complete Streamlit app (Tasty Bytes city sales dashboard) to paste into a Snowsight Streamlit app.
