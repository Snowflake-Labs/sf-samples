# Topic Discovery

## Overview
This solution demonstrates how to use Snowflake's Cortex AISQL functions within an interactive Streamlit app to discover topics and themes within unstructured text data. These extracted topics can be used in AI_CLASSIFY pipelines to categorize your data, see trends and discover new business insights.

## Key Features
- Interactive data exploration and sampling
- Topic discovery from text data using custom prompts
- Automatic category extraction from analysis results
- Text classification into discovered categories
- Outlier analysis to identify patterns in uncategorized content

## Technical Components
The solution leverages several Snowflake Cortex AI functions:
- **AI_AGG**: Aggregate and analyze text data to discover topics and patterns
- **AI_COMPLETE**: Extract structured category lists from unstructured analysis results
- **AI_CLASSIFY**: Classify text into discovered categories

## Business Value
This approach enables organizations to understand large volumes of unstructured text without manual review. Use cases include customer feedback analysis, support ticket categorization, content organization, and identifying emerging themes in user-generated content.

## Usage
Run the Streamlit application in Snowflake to interactively explore your text data. The workflow guides you through discovering topics, extracting categories, classifying content, and analyzing outliers. A sample dataset is provided in the data folder.

