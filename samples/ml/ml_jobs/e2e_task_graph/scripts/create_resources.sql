-- Create database and schema
CREATE DATABASE IF NOT EXISTS <% database_name %>;
CREATE SCHEMA IF NOT EXISTS <% database_name %>.<% schema_name %>;

-- Create stages for storing DAG artifacts
CREATE STAGE IF NOT EXISTS <% database_name %>.<% schema_name %>.DAG_STAGE;
CREATE STAGE IF NOT EXISTS <% database_name %>.<% schema_name %>.JOB_STAGE;

-- Create an image repository to ensure account has access to Snowflake images
CREATE IMAGE REPOSITORY IF NOT EXISTS <% database_name %>.<% schema_name %>.DEMO_IMAGE_REPO;

-- Set up data schema
CREATE SCHEMA IF NOT EXISTS <% database_name %>.<% data_schema_name %>;