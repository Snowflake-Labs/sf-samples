# AI-Powered Equity Research Analytics with Snowflake Cortex

## Overview

This solution demonstrates how to leverage Snowflake's AI-SQL capabilities to process and analyze unstructured financial research documents. By combining AI functions with traditional SQL operations, this notebook creates a streamlined workflow for extracting actionable insights from equity research reports—enabling more efficient financial analysis and investment decision support.

You can leverage different parts of Snowflake Cortex features to build your workflow. For this notebook specifically, we focus on leveraging AISQL to supercharge the unstructured data processing part.

![Equity Research Analytics](./images/equity_research_ai_architecture.png)

## Key Features

- Automated PDF research report processing
- Entity extraction with sentiment analysis
- Company name disambiguation and ticker mapping
- Cross-document insight aggregation by ticker
- Research summarization with contextual understanding

## Technical Components

The solution leverages several Snowflake Cortex AI-SQL functions:

- **[PARSE_DOCUMENT](https://docs.snowflake.com/en/user-guide/snowflake-cortex/parse-document)**: Extract text content from PDF research reports
- **[AI_COMPLETE](https://docs.snowflake.com/en/sql-reference/functions/ai_complete) with Structured Output**: Extract company entities and sentiment in structured format
- **[AI_SIMILARITY](https://docs.snowflake.com/en/sql-reference/functions/ai_similarity)**: Find potential ticker matches based on semantic similarity
- **[AI_FILTER](https://docs.snowflake.com/en/sql-reference/functions/ai_filter)**: Disambiguate company names for accurate ticker mapping
- **[AI_AGG](https://docs.snowflake.com/en/sql-reference/functions/ai_agg)**: Aggregate insights across multiple research documents by ticker

## Example Use Cases

1. **Investment Research**: Consolidate insights on specific companies from multiple sources
2. **Competitive Analysis**: Extract mentions and sentiment about competitors
3. **Market Intelligence**: Identify trending companies and sentiment across research reports
4. **Insights Summary**: Aggregate research insights about companies in a portfolio

## Prerequisites

1. Create a DB (or reuse an existing DB)
    - the lab uses the name "AI_SQL_TEAM_DB" if you prefer to use that name
    - and grant usage for all roles doing this QuickStart
2. Create a Schema (or reuse an existing Schema)
    - the lab uses the name "SE_SAMPLE_DATA"
    - at least grant usage, create table, create view, create semantic view, create stage for all roles doing this QuickStart
3. Create an Internal Snowflake Stage
    - The lab uses the name "EQUITY_RESEARCH"
    - It's easiest to do this in the Snowflake UI
    - Ensure you use Server Side Encryption
    - Ensure directory tables are enabled
4. Upload the [Equity research PDFs](data/) to the Snowflake Stage that was just created
    - It's easiest to do this in the Snowflake UI
5. Grant the [Cortex Database Role](https://docs.snowflake.com/en/user-guide/snowflake-cortex/aisql#required-privileges) for all roles doing this QuickStart
6. Install the free Snowflake Marketplace [S&P 500 sample ticker data](https://app.snowflake.com/marketplace/listing/GZT1ZA3NHF/similarweb-ltd-s-p-500-by-domain-and-aggregated-by-tickers-sample?search=S%26P+500) dataset in your Snowflake account(s)
    - grant imported privileges to the public role (or an equivalent role)

## Usage / Implementation Steps

Walk through the included Notebook [data_processing_equity_research.ipynb](data_processing_equity_research.ipynb) for a step-by-step demonstration of how to implement this equity research processing pipeline within Snowflake, using Cortex AI-SQL functions for intelligent analysis.

The notebook demonstrates a complete pipeline:

1. Parse PDF research documents into text using PARSE_DOCUMENT
2. Extract company mentions and sentiment using structured output
3. Map extracted companies to S&P 500 tickers using similarity and AI_FILTER
4. Aggregate insights across multiple documents for specific tickers
