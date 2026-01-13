-- 1. Create stage 
CREATE STAGE IF NOT EXISTS VINO_LAKEHOUSE_VHOL.PUBLIC.SEMANTIC_MODELS;

-- TODO: manually upload the yaml file to the stage

-- 2. Setup Snowflake Intelligence database/schema
CREATE DATABASE IF NOT EXISTS SNOWFLAKE_INTELLIGENCE;
GRANT USAGE ON DATABASE SNOWFLAKE_INTELLIGENCE TO ROLE PUBLIC;

CREATE SCHEMA IF NOT EXISTS SNOWFLAKE_INTELLIGENCE.AGENTS;
GRANT USAGE ON SCHEMA SNOWFLAKE_INTELLIGENCE.AGENTS TO ROLE PUBLIC;
GRANT CREATE AGENT ON SCHEMA SNOWFLAKE_INTELLIGENCE.AGENTS TO ROLE ACCOUNTADMIN;

-- 3. Create the Agent
CREATE OR REPLACE AGENT SNOWFLAKE_INTELLIGENCE.AGENTS.AMAZON_PRODUCT_REVIEWS_AGENT
  COMMENT = 'Agent for querying Amazon product reviews data'
  PROFILE = '{"display_name": "Amazon Product Reviews Assistant"}'
  FROM SPECIFICATION $$
  {
    "models": {
      "orchestration": "claude-4-sonnet"
    },
    "instructions": {
      "orchestration": "Use the product reviews tool to answer questions about reviews, ratings, and customer feedback.",
      "response": "Be concise and data-driven. When showing ratings, include counts for context."
    },
    "tools": [
      {
        "tool_spec": {
          "type": "cortex_analyst_text_to_sql",
          "name": "product_reviews",
          "description": "Query product reviews data including ratings, review text, summaries, and reviewer information"
        }
      }
    ],
    "tool_resources": {
      "product_reviews": {
        "semantic_model_file": "@VINO_LAKEHOUSE_VHOL.PUBLIC.SEMANTIC_MODELS/product_reviews_semantic_model.yaml",
        "execution_environment": {
          "type": "warehouse",
          "warehouse": "VINO_XS"
        }
      }
    }
  }
  $$;

-- 4. Grant access
GRANT USAGE ON AGENT SNOWFLAKE_INTELLIGENCE.AGENTS.AMAZON_PRODUCT_REVIEWS_AGENT TO ROLE PUBLIC;