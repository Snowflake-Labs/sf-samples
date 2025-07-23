# Getting Started with AISQL: Your First Steps

Welcome! This guide will take you from zero to productive with Snowflake's AI-SQL capabilities in 30 minutes.

## 🎯 Learning Objectives

By the end of this guide, you'll be able to:
- Set up your AISQL environment
- Run your first AI-powered SQL queries
- Understand core AISQL functions
- Build a simple AI pipeline

## 📋 Prerequisites

- Access to a Snowflake account
- Basic SQL knowledge
- `ACCOUNTADMIN` role or equivalent permissions

## 🚀 Quick Setup (5 minutes)

### Step 1: Environment Setup

```sql
-- Run these commands in your Snowflake worksheet
USE ROLE ACCOUNTADMIN;

-- Create demo role and permissions
CREATE ROLE IF NOT EXISTS aisql_demo_role;
GRANT CREATE WAREHOUSE ON ACCOUNT TO ROLE aisql_demo_role WITH GRANT OPTION;
GRANT CREATE DATABASE ON ACCOUNT TO ROLE aisql_demo_role WITH GRANT OPTION;
GRANT ROLE aisql_demo_role TO USER YOUR_USERNAME; -- Replace with your username

-- Switch to demo role
USE ROLE aisql_demo_role;

-- Create warehouse and database
CREATE WAREHOUSE IF NOT EXISTS aisql_demo_wh WITH 
    WAREHOUSE_SIZE = 'SMALL' 
    AUTO_SUSPEND = 60 
    AUTO_RESUME = TRUE;

USE WAREHOUSE aisql_demo_wh;

CREATE DATABASE IF NOT EXISTS aisql_demo_db;
CREATE SCHEMA IF NOT EXISTS aisql_demo_db.getting_started;
USE SCHEMA aisql_demo_db.getting_started;
```

### Step 2: Create Sample Data

```sql
-- Create sample customer reviews table
CREATE OR REPLACE TABLE customer_reviews (
    review_id INTEGER,
    product_name VARCHAR,
    review_text VARCHAR,
    rating INTEGER
);

-- Insert sample data
INSERT INTO customer_reviews VALUES
(1, 'Coffee Maker Pro', 'This coffee maker is amazing! Makes perfect coffee every time.', 5),
(2, 'Coffee Maker Pro', 'The coffee tastes terrible and the machine is too loud.', 2),
(3, 'Wireless Headphones', 'Great sound quality but the battery life is disappointing.', 3),
(4, 'Wireless Headphones', 'Best headphones I have ever owned! Crystal clear audio.', 5),
(5, 'Smart Watch', 'The watch is okay but the app keeps crashing.', 2),
(6, 'Smart Watch', 'Love the fitness tracking features and sleek design.', 4),
(7, 'Laptop Stand', 'Perfect for my home office setup. Very sturdy.', 5),
(8, 'Laptop Stand', 'Cheap plastic that broke after a week.', 1);

-- Verify data
SELECT * FROM customer_reviews;
```

## 🎓 Core AISQL Functions - Learn by Doing

### Function 1: `AI_CLASSIFY` - Categorize Your Data

**What it does:** Automatically categorizes text into predefined groups.

```sql
-- Example 1: Classify sentiment
SELECT 
    review_id,
    product_name,
    review_text,
    rating,
    AI_CLASSIFY(review_text, ['positive', 'negative', 'neutral']) as ai_sentiment
FROM customer_reviews;
```

**Try it yourself:** Modify the categories to `['excellent', 'good', 'poor']` and see how results change.

```sql
-- Example 2: Classify by topic
SELECT 
    review_id,
    product_name,
    review_text,
    AI_CLASSIFY(review_text, ['product_quality', 'usability', 'design', 'price']) as topic
FROM customer_reviews;
```

### Function 2: `SENTIMENT` - Measure Emotional Tone

**What it does:** Analyzes the emotional tone of text and returns a score.

```sql
-- Analyze sentiment with scores
SELECT 
    product_name,
    review_text,
    rating,
    SENTIMENT(review_text) as sentiment_score,
    CASE 
        WHEN SENTIMENT(review_text) > 0.1 THEN 'Positive'
        WHEN SENTIMENT(review_text) < -0.1 THEN 'Negative' 
        ELSE 'Neutral'
    END as sentiment_label
FROM customer_reviews
ORDER BY sentiment_score DESC;
```

**Insight:** Compare the `sentiment_score` with the numerical `rating`. Do they align?

### Function 3: `AI_FILTER` - Smart Content Filtering

**What it does:** Filters records based on AI understanding of content.

```sql
-- Find reviews mentioning specific issues
SELECT 
    product_name,
    review_text,
    rating
FROM customer_reviews
WHERE AI_FILTER('This review mentions a problem or complaint about the product', review_text);
```

**Try different filters:**
```sql
-- Find positive reviews
SELECT * FROM customer_reviews
WHERE AI_FILTER('This review expresses satisfaction or happiness', review_text);

-- Find reviews about specific features
SELECT * FROM customer_reviews  
WHERE AI_FILTER('This review mentions battery life or power', review_text);
```

### Function 4: `AI_COMPLETE` - Generate Content

**What it does:** Generates text based on prompts and context.

```sql
-- Generate response templates for negative reviews
SELECT 
    review_id,
    product_name,
    review_text,
    AI_COMPLETE(
        'Write a professional customer service response to this review: ' || review_text ||
        '. Be empathetic and offer to help resolve the issue.'
    ) as suggested_response
FROM customer_reviews
WHERE rating <= 2;
```

### Function 5: `AI_AGG` - Intelligent Aggregation

**What it does:** Summarizes and analyzes data across multiple records.

```sql
-- Get insights by product
SELECT 
    product_name,
    COUNT(*) as review_count,
    AVG(rating) as avg_rating,
    AI_AGG(review_text, 'Summarize the main themes in these customer reviews') as review_summary,
    AI_AGG(review_text, 'What are the top 3 issues customers mention?') as main_issues
FROM customer_reviews
GROUP BY product_name;
```

## 🔗 Building Your First AI Pipeline

Now let's combine multiple functions to create a complete analysis pipeline:

```sql
-- Complete Review Analysis Pipeline
WITH sentiment_analysis AS (
    -- Step 1: Add sentiment analysis
    SELECT 
        *,
        SENTIMENT(review_text) as sentiment_score,
        AI_CLASSIFY(review_text, ['positive', 'negative', 'neutral']) as sentiment_category
    FROM customer_reviews
),
issue_identification AS (
    -- Step 2: Identify problematic reviews
    SELECT 
        *,
        AI_FILTER('This review mentions a problem, complaint, or dissatisfaction', review_text) as has_issues,
        CASE 
            WHEN has_issues THEN AI_CLASSIFY(review_text, ['quality_issue', 'usability_issue', 'design_issue', 'other'])
            ELSE NULL
        END as issue_type
    FROM sentiment_analysis
),
final_analysis AS (
    -- Step 3: Generate action items
    SELECT 
        *,
        CASE 
            WHEN has_issues AND rating <= 2 THEN 
                AI_COMPLETE('Generate a brief action item to address this customer concern: ' || review_text)
            ELSE NULL
        END as action_item
    FROM issue_identification
)
SELECT * FROM final_analysis
ORDER BY sentiment_score ASC; -- Show most negative first
```

## 📊 Real-World Exercise: Product Insights Dashboard

Create a comprehensive product insights summary:

```sql
-- Product Insights Dashboard Query
WITH review_analysis AS (
    SELECT 
        product_name,
        review_text,
        rating,
        SENTIMENT(review_text) as sentiment_score,
        AI_CLASSIFY(review_text, ['quality', 'usability', 'design', 'value', 'other']) as review_focus,
        AI_FILTER('This review mentions a specific problem or improvement suggestion', review_text) as has_feedback
    FROM customer_reviews
),
product_summary AS (
    SELECT 
        product_name,
        COUNT(*) as total_reviews,
        AVG(rating) as avg_rating,
        AVG(sentiment_score) as avg_sentiment,
        SUM(CASE WHEN has_feedback THEN 1 ELSE 0 END) as feedback_count,
        -- Aggregate insights by focus area
        AI_AGG(
            CASE WHEN review_focus:labels[0]::text = 'quality' THEN review_text END,
            'Summarize quality-related feedback'
        ) as quality_insights,
        AI_AGG(
            CASE WHEN review_focus:labels[0]::text = 'usability' THEN review_text END,
            'Summarize usability feedback'  
        ) as usability_insights,
        -- Overall recommendation
        AI_AGG(review_text, 'Based on these reviews, what are the top 3 recommendations for product improvement?') as improvement_recommendations
    FROM review_analysis
    GROUP BY product_name
)
SELECT 
    product_name,
    total_reviews,
    ROUND(avg_rating, 2) as avg_rating,
    ROUND(avg_sentiment, 3) as avg_sentiment,
    feedback_count,
    quality_insights,
    usability_insights,
    improvement_recommendations
FROM product_summary
ORDER BY avg_sentiment DESC;
```

## 🎯 Next Steps

Congratulations! You've learned the core AISQL functions. Here's what to explore next:

### Immediate Practice
1. **Expand the dataset**: Add more product reviews and categories
2. **Try different prompts**: Experiment with prompt engineering for better results
3. **Combine with traditional SQL**: Join AI insights with sales data, inventory, etc.

### Advanced Learning Path
1. **[Marketing Lead Pipeline](../marketing_lead_pipelines/)** - Learn workflow automation
2. **[Multimodal Processing](../healthcare_multimodal_analytics/)** - Work with images and documents  
3. **[Advanced Patterns](../AISQL_DEMO_PLAYBOOK.md#common-patterns--best-practices)** - Master complex AI pipelines

### Best Practices You've Learned
- ✅ Start with simple, single-function queries
- ✅ Use CTEs to break complex workflows into steps
- ✅ Experiment with different prompt phrasings
- ✅ Combine AI functions with traditional SQL operations
- ✅ Always validate AI results against business logic

## 🔧 Troubleshooting

**Common issues and solutions:**

| Problem | Solution |
|---------|----------|
| "Function not found" error | Ensure you're using Snowflake's Cortex-enabled account |
| Unexpected classifications | Refine your category definitions and add examples |
| Slow performance | Use appropriate warehouse sizing (SMALL for learning) |
| Empty AI_AGG results | Check that input data isn't NULL or empty |

## 📚 Additional Resources

- [Main AISQL Playbook](../AISQL_DEMO_PLAYBOOK.md)
- [Snowflake Cortex Documentation](https://docs.snowflake.com/en/user-guide/snowflake-cortex/aisql)
- [Function Reference](../AISQL_DEMO_PLAYBOOK.md#aisql-function-reference)

---

**Ready for more?** Choose your next adventure based on your interests:
- 🎯 **Marketing Professional**: Try the [Marketing Lead Pipeline](../marketing_lead_pipelines/)
- 🛒 **E-commerce Focus**: Explore [Customer Review Analysis](../ecommerce_customer_review/)  
- 🏥 **Healthcare/Multimodal**: Dive into [Healthcare Analytics](../healthcare_multimodal_analytics/)
- 💰 **Financial Services**: Check out [Equity Research Processing](../financial_service_equity_research/)

*Happy learning with AISQL! 🚀* 