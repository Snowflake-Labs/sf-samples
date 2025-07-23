# AISQL Demo Playbook: Complete Guide to Snowflake Cortex AI-SQL

Welcome to the comprehensive AISQL Demo Playbook! This guide provides a structured learning path through Snowflake's AI-SQL capabilities, from basic concepts to advanced implementations.

## 📚 Table of Contents

1. [Quick Start Guide](#quick-start-guide)
2. [AISQL Function Reference](#aisql-function-reference)
3. [Demo Use Cases](#demo-use-cases)
4. [Common Patterns & Best Practices](#common-patterns--best-practices)
5. [Advanced Examples](#advanced-examples)
6. [Performance & Optimization](#performance--optimization)
7. [Templates & Reusable Components](#templates--reusable-components)
8. [Troubleshooting](#troubleshooting)

## 🚀 Quick Start Guide

### What is AISQL?

AISQL (AI-SQL) brings the power of AI directly into SQL queries, enabling you to process unstructured data alongside traditional structured data without leaving your database. This revolutionary approach allows you to:

- Analyze text, images, audio, and documents using familiar SQL syntax
- Combine AI operations with traditional SQL joins, aggregations, and filters
- Scale AI workloads using Snowflake's compute engine
- Maintain data governance and security within your existing framework

### Core Value Propositions

1. **Unified Data Processing**: Process structured and unstructured data in a single workflow
2. **SQL-Native AI**: Leverage AI capabilities without learning new tools or languages
3. **Enterprise Scale**: Handle large volumes of multimodal data with Snowflake's performance
4. **Integrated Analytics**: Combine AI insights with traditional business intelligence

### Prerequisites

```sql
-- Setup required for all demos
USE ROLE ACCOUNTADMIN;
CREATE ROLE IF NOT EXISTS aisql_demo_role;
GRANT CREATE WAREHOUSE ON ACCOUNT TO ROLE aisql_demo_role WITH GRANT OPTION;
GRANT CREATE DATABASE ON ACCOUNT TO ROLE aisql_demo_role WITH GRANT OPTION;
GRANT USAGE ON INTEGRATION IF EXISTS SNOWPARK_ENVIRONMENT TO ROLE aisql_demo_role;

USE ROLE aisql_demo_role;
CREATE WAREHOUSE IF NOT EXISTS aisql_demo_wh WITH WAREHOUSE_SIZE = 'SMALL';
CREATE DATABASE IF NOT EXISTS aisql_demo_db;
CREATE SCHEMA IF NOT EXISTS aisql_demo_db.demos;
```

## 🔧 AISQL Function Reference

### Text Processing Functions

| Function | Purpose | Use Cases |
|----------|---------|-----------|
| `AI_COMPLETE` | Text generation and completion | Content creation, responses, summarization |
| `AI_CLASSIFY` | Categorize text into predefined classes | Content categorization, sentiment classification |
| `AI_FILTER` | Boolean filtering based on AI criteria | Content filtering, relevance scoring |
| `AI_AGG` | Aggregate insights across multiple records | Cross-document summarization, trend analysis |
| `SENTIMENT` | Analyze emotional tone of text | Customer feedback analysis, social media monitoring |

### Multimodal Functions

| Function | Purpose | Use Cases |
|----------|---------|-----------|
| `FILE` Data Type | Process files directly in SQL | Image, PDF, audio analysis |
| `AI_TRANSCRIBE` | Convert audio to text | Call center analysis, meeting transcription |
| `PARSE_DOCUMENT` | Extract text from documents | PDF processing, document analysis |
| `AI_SIMILARITY` | Semantic similarity comparison | Entity matching, duplicate detection |

### Quick Examples

```sql
-- Basic text classification
SELECT AI_CLASSIFY(review_text, ['positive', 'negative', 'neutral']) as sentiment
FROM customer_reviews;

-- Image content filtering
SELECT * FROM product_images 
WHERE AI_FILTER('This image contains a coffee machine', image_file);

-- Document summarization
SELECT AI_AGG(document_content, 'Summarize key findings') as summary
FROM research_documents;
```

## 🎯 Demo Use Cases

### 1. [Marketing Lead Pipeline](./marketing_lead_pipelines/)
**Complexity: Beginner** | **Functions: AI_FILTER, AI_CLASSIFY, AI_COMPLETE**

Automate lead qualification and personalization:
- Filter spam and irrelevant submissions
- Score leads based on ICP fit
- Generate personalized outreach emails

**Key Learning:** Sequential AI function chaining for workflow automation

### 2. [E-commerce Customer Reviews](./ecommerce_customer_review/)
**Complexity: Beginner** | **Functions: SENTIMENT, AI_FILTER, AI_AGG, AI_CLASSIFY**

Transform customer feedback into actionable insights:
- Sentiment analysis correlation with ratings
- Identify and categorize product issues
- Generate automated responses

**Key Learning:** Text analytics pipeline development

### 3. [Ad Campaign Analytics](./ads_image_analytics/)
**Complexity: Intermediate** | **Functions: FILE, AI_COMPLETE, AI_FILTER, AI_CLASSIFY, AI_AGG**

Optimize advertising through image content analysis:
- Classify ad types automatically
- Filter content based on performance criteria
- Generate compelling taglines
- Aggregate customer feedback by ad type

**Key Learning:** Multimodal data processing with images

### 4. [Healthcare Multimodal Analytics](./healthcare_multimodal_analytics/)
**Complexity: Advanced** | **Functions: FILE, AI_FILTER, AI_TRANSCRIBE, AI_CLASSIFY, AI_AGG, PARSE_DOCUMENT**

Comprehensive healthcare data processing:
- Medical image classification and routing
- Audio transcription and call categorization
- PDF document analysis
- Population health insights generation

**Key Learning:** Complex multimodal workflows

### 5. [Financial Services Equity Research](./financial_service_equity_research/)
**Complexity: Advanced** | **Functions: PARSE_DOCUMENT, AI_COMPLETE, AI_SIMILARITY, AI_FILTER, AI_AGG**

Automate financial document analysis:
- Extract entities with sentiment from research reports
- Map companies to tradeable tickers
- Aggregate insights across multiple documents

**Key Learning:** Structured output and entity resolution

## 📋 Common Patterns & Best Practices

### Pattern 1: Filter → Classify → Aggregate
```sql
-- 1. Filter relevant records
WITH filtered_data AS (
  SELECT * FROM source_table
  WHERE AI_FILTER('Criteria for relevance', text_column)
),
-- 2. Classify into categories
classified_data AS (
  SELECT *, AI_CLASSIFY(text_column, ['cat1', 'cat2', 'cat3']) as category
  FROM filtered_data
)
-- 3. Aggregate insights by category
SELECT 
  category,
  COUNT(*) as count,
  AI_AGG(text_column, 'Summarize key themes') as summary
FROM classified_data
GROUP BY category;
```

### Pattern 2: Multimodal Processing Pipeline
```sql
-- Process different file types in unified workflow
SELECT 
  file_name,
  file_type,
  CASE 
    WHEN file_type = 'image' THEN AI_CLASSIFY('Describe this image', file_content)
    WHEN file_type = 'audio' THEN AI_TRANSCRIBE(file_content)
    WHEN file_type = 'pdf' THEN PARSE_DOCUMENT(file_content)
  END as processed_content
FROM multimodal_files;
```

### Pattern 3: Entity Resolution with Similarity
```sql
-- Match and resolve entities using semantic similarity
WITH potential_matches AS (
  SELECT 
    input.entity_name,
    reference.standard_name,
    AI_SIMILARITY(input.entity_name, reference.standard_name) as similarity_score
  FROM input_entities input
  CROSS JOIN reference_entities reference
  WHERE similarity_score > 0.8
),
best_matches AS (
  SELECT entity_name, standard_name,
    ROW_NUMBER() OVER (PARTITION BY entity_name ORDER BY similarity_score DESC) as rn
  FROM potential_matches
)
SELECT entity_name, standard_name
FROM best_matches WHERE rn = 1;
```

### Best Practices

1. **Start Simple**: Begin with single-function examples before chaining
2. **Use CTEs**: Break complex workflows into readable steps
3. **Optimize Prompts**: Be specific and provide context in prompts
4. **Batch Processing**: Process multiple records together for efficiency
5. **Error Handling**: Include validation and fallback logic
6. **Performance**: Monitor compute usage and optimize warehouse sizing

## 🎓 Advanced Examples

### Cross-Industry Content Analysis
Combine multiple AISQL functions to analyze content across different industries:

```sql
-- Advanced: Multi-industry content analysis
WITH content_analysis AS (
  SELECT 
    content_id,
    industry,
    content_text,
    content_image,
    -- Multi-function analysis
    AI_CLASSIFY(content_text, ['technical', 'marketing', 'educational']) as text_category,
    AI_CLASSIFY('Analyze this image', content_image, ['product', 'person', 'landscape']) as image_category,
    SENTIMENT(content_text) as sentiment_score,
    AI_COMPLETE('Suggest 3 relevant hashtags for this content: ' || content_text) as hashtags
  FROM multi_industry_content
),
industry_insights AS (
  SELECT 
    industry,
    text_category,
    image_category,
    COUNT(*) as content_count,
    AVG(sentiment_score) as avg_sentiment,
    AI_AGG(content_text, 'Identify top 3 themes across this industry content') as key_themes,
    AI_AGG(hashtags, 'Most relevant hashtags for ' || industry) as trending_hashtags
  FROM content_analysis
  GROUP BY industry, text_category, image_category
)
SELECT * FROM industry_insights
ORDER BY industry, content_count DESC;
```

### Real-time Content Moderation Pipeline
```sql
-- Advanced: Real-time content moderation
CREATE OR REPLACE STREAM content_moderation_stream ON TABLE user_generated_content;

CREATE OR REPLACE TASK content_moderation_task
  WAREHOUSE = aisql_demo_wh
  SCHEDULE = '1 minute'
AS
INSERT INTO moderated_content
SELECT 
  content_id,
  user_id,
  content_text,
  content_image,
  CURRENT_TIMESTAMP as processed_at,
  -- Multi-stage moderation
  AI_FILTER('This content contains inappropriate language or hate speech', content_text) as has_inappropriate_text,
  AI_FILTER('This image contains inappropriate or harmful content', content_image) as has_inappropriate_image,
  AI_CLASSIFY(content_text, ['spam', 'promotional', 'genuine', 'suspicious']) as content_type,
  CASE 
    WHEN AI_FILTER('This content requires human review', content_text || ' ' || content_image) 
    THEN 'human_review'
    ELSE 'auto_approved'
  END as moderation_status
FROM content_moderation_stream
WHERE METADATA$ACTION = 'INSERT';
```

## ⚡ Performance & Optimization

### Warehouse Sizing Guidelines

| Use Case | Recommended Size | Concurrent Users | Data Volume |
|----------|------------------|------------------|-------------|
| Development/Testing | XS-S | 1-5 | < 1GB |
| Production Batch Processing | M-L | 5-20 | 1-100GB |
| Real-time Processing | L-XL | 20+ | 100GB+ |
| Multimodal Large Files | XL-2XL | Variable | 1TB+ |

### Performance Optimization Tips

1. **Batch Similar Operations**: Group similar AI operations together
2. **Use Appropriate Data Types**: Leverage FILE type for binary data
3. **Optimize Prompts**: Shorter, more specific prompts perform better
4. **Parallel Processing**: Use multiple warehouses for independent workloads
5. **Result Caching**: Cache AI function results when possible

```sql
-- Example: Optimized batch processing
SELECT 
  batch_id,
  -- Process multiple items in single operation
  AI_CLASSIFY(STRING_AGG(text_content, ' | '), ['cat1', 'cat2', 'cat3']) as batch_classification,
  AI_AGG(text_content, 'Summarize this batch of content') as batch_summary
FROM content_table
GROUP BY batch_id;
```

## 📄 Templates & Reusable Components

### Template: Basic Text Analysis Pipeline
```sql
-- TEMPLATE: Basic Text Analysis
-- Replace {{TABLE_NAME}}, {{TEXT_COLUMN}}, {{CATEGORIES}} with your values

WITH analyzed_content AS (
  SELECT 
    *,
    SENTIMENT({{TEXT_COLUMN}}) as sentiment_score,
    AI_CLASSIFY({{TEXT_COLUMN}}, {{CATEGORIES}}) as category,
    AI_COMPLETE('Summarize in one sentence: ' || {{TEXT_COLUMN}}) as summary
  FROM {{TABLE_NAME}}
  WHERE AI_FILTER('This text is relevant to our analysis', {{TEXT_COLUMN}})
)
SELECT 
  category,
  COUNT(*) as count,
  AVG(sentiment_score) as avg_sentiment,
  AI_AGG(summary, 'Key insights across this category') as category_insights
FROM analyzed_content
GROUP BY category;
```

### Template: Multimodal File Processing
```sql
-- TEMPLATE: Multimodal Processing
-- Replace {{STAGE_NAME}}, {{FILE_TYPES}} with your values

WITH file_inventory AS (
  SELECT 
    relative_path,
    TO_FILE('{{STAGE_NAME}}', relative_path) as file_content,
    SPLIT_PART(relative_path, '.', -1) as file_extension
  FROM DIRECTORY('{{STAGE_NAME}}')
),
processed_files AS (
  SELECT 
    relative_path,
    file_extension,
    CASE file_extension
      WHEN 'pdf' THEN PARSE_DOCUMENT(file_content)
      WHEN 'wav' THEN AI_TRANSCRIBE(file_content):text
      WHEN 'jpg' THEN AI_COMPLETE('Describe this image', file_content)
      WHEN 'png' THEN AI_COMPLETE('Describe this image', file_content)
    END as extracted_content
  FROM file_inventory
)
SELECT 
  file_extension,
  COUNT(*) as file_count,
  AI_AGG(extracted_content, 'Common themes across ' || file_extension || ' files') as themes
FROM processed_files
GROUP BY file_extension;
```

## 🔧 Troubleshooting

### Common Issues and Solutions

| Issue | Symptom | Solution |
|-------|---------|----------|
| Prompt too generic | Inconsistent AI results | Add specific context and examples |
| Large file processing | Timeout errors | Use smaller batches or larger warehouse |
| Classification accuracy | Wrong categories | Refine category definitions and provide examples |
| Performance degradation | Slow query execution | Optimize warehouse size and query structure |

### Debugging Techniques

```sql
-- Debug AI function results
SELECT 
  input_text,
  AI_CLASSIFY(input_text, ['cat1', 'cat2']) as classification,
  -- Add debug information
  LENGTH(input_text) as text_length,
  REGEXP_COUNT(input_text, '\\w+') as word_count
FROM debug_table
WHERE classification IS NULL; -- Find failed classifications
```

## 📚 Additional Resources

- [Snowflake Cortex Documentation](https://docs.snowflake.com/en/user-guide/snowflake-cortex/aisql)
- [AISQL Blog Post](https://www.snowflake.com/en/blog/ai-sql-query-language/)
- [Community Examples](https://github.com/snowflake-labs)

## 🤝 Contributing

We welcome contributions to enhance this playbook! Please include:
1. Clear use case description
2. Complete, runnable code examples
3. Sample data or setup instructions
4. Business value explanation

---

*This playbook is designed to help you master AISQL capabilities through practical, real-world examples. Start with the beginner demos and progress to advanced use cases as you build confidence with the technology.* 