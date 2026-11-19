# Snowflake AISQL Function Evaluation UI

A comprehensive web-based UI for testing and evaluating Snowflake AISQL functions including `AI_COMPLETE`, `AI_CLASSIFY`, and `AI_FILTER`. This tool allows you to test end-to-end evaluation workflows with mock data without needing an actual Snowflake connection.

## Features

### 1. **Mock Snowflake Tables**
- Pre-loaded sample tables with realistic data:
  - `customer_reviews` - Product reviews for sentiment analysis
  - `support_tickets` - Support tickets for priority classification
  - `product_descriptions` - Products for summarization
  - `customer_feedback` - Feedback for filtering

### 2. **Query Editor**
- Write and execute AISQL queries
- Sample queries for quick testing:
  - `AI_CLASSIFY` for sentiment/priority classification
  - `AI_COMPLETE` for text generation/summarization
  - `AI_FILTER` for filtering based on conditions

### 3. **Execution Results & Golden Dataset Creation**
- **Table-based display** for easy side-by-side comparison
- View execution results with input data and AI predictions in one view
- **Reusable golden datasets** - Automatically detects and offers to load existing golden data for the same query
- **Interactive editing** - Directly edit AI results in the golden dataset table
- **Visual highlighting** - Modified values are highlighted in yellow
- Sticky table headers for easy navigation through large datasets
- Save golden data for evaluation and reuse across sessions

### 4. **Evaluation Metrics**

#### Classification Metrics (AI_CLASSIFY, AI_FILTER)
- Precision, Recall, F1-score per class
- Overall accuracy
- Sample-by-sample comparison view

#### Generative Metrics (AI_COMPLETE)
- LLM-as-judge evaluation
- Relevance, Coherence, Fluency scores
- Overall quality assessment
- Detailed feedback

## Getting Started

### Prerequisites
- Python 3.8+
- pip

### Installation

1. Navigate to the project directory:
```bash
cd samples/aisql_eval_ui
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

### Running the Application

1. Start the Flask server:
```bash
python app.py
```

2. Open your browser and navigate to:
```
http://localhost:5000
```

## Usage Workflow

### Step 1: Select a Table
Choose one of the pre-loaded mock tables from the dropdown. You'll see the row count and column information.

### Step 2: Write AISQL Query
Write your query using Snowflake AISQL functions. Click on sample queries for quick examples:

**Example - Sentiment Classification:**
```sql
SELECT *, AI_CLASSIFY(review, ARRAY['positive', 'negative', 'neutral']) as ai_result 
FROM customer_reviews
```

**Example - Text Summarization:**
```sql
SELECT *, AI_COMPLETE('Summarize this: ' || specs) as ai_result 
FROM product_descriptions
```

**Example - Filtering:**
```sql
SELECT * FROM customer_feedback 
WHERE AI_FILTER(text, 'needs response or action')
```

### Step 3: View Results
After executing the query, review the AI-generated results in a **table format** showing:
- Row number for easy reference
- All input columns (ID, description, etc.)
- AI-generated predictions

The table has a sticky header so you can scroll through large result sets easily.

### Step 4: Golden Dataset (Sequential Flow)

You'll be presented with a choice:

**Option A: Use an Existing Golden Dataset**
1. Click "Yes, Select from Existing Golden Datasets"
2. Browse the dropdown to see all saved golden datasets
   - Format: `table_name - SQL_query (row_count)`
3. Select a dataset to see:
   - Full SQL query (untruncated)
   - Table name, row count, and save timestamp
4. Click "Load & Use This Dataset"
5. ✅ Golden dataset is loaded - you can now execute queries and evaluate

**Option B: Create a New Golden Dataset**
1. Click "No, I'll Create a New Golden Dataset"
2. Execute a query in Step 2 to get AI predictions
3. Edit the results in the table:
   - **AI Result** column (read-only) - Shows what the AI predicted
   - **Golden Result** column (editable) - Edit to set correct values
4. Click "Save as Golden Dataset"
5. ✅ Golden dataset is saved and ready for evaluation

**Tips:**
- Use "Back" button to return to the choice screen anytime
- Modified fields turn **yellow** to show they've been changed
- Saved datasets are available for reuse in future sessions
- **Golden datasets are keyed by SQL query + table name**

### Step 5: Evaluate Performance
Choose evaluation type:
- **Classification Metrics**: For AI_CLASSIFY and AI_FILTER
  - Calculates precision, recall, F1-score
  - Shows per-class and overall metrics
  - Displays sample comparisons

- **LLM as Judge**: For AI_COMPLETE and generative tasks
  - Evaluates relevance, coherence, fluency
  - Provides overall quality score
  - Includes detailed feedback

## API Endpoints

The application exposes the following REST API endpoints:

- `GET /api/tables` - List all available tables
- `GET /api/table/<table_name>` - Get table data
- `POST /api/execute` - Execute AISQL query
- `POST /api/golden` - Save golden dataset (stored by execution ID and query hash)
- `GET /api/golden/<execution_id>` - Get golden dataset by execution ID
- `POST /api/golden/query` - Check if golden dataset exists for a query
- `GET /api/golden/datasets` - List all saved golden datasets
- `POST /api/evaluate` - Evaluate results against golden data
- `GET /api/history` - Get evaluation history

## Architecture

### Backend (Flask)
- Mock data generation
- Query parsing and execution simulation
- Metrics calculation (classification and generative)
- Golden dataset management

### Frontend (HTML/CSS/JavaScript)
- Modern, responsive UI
- Interactive query editor
- Real-time result editing
- Dynamic metrics visualization

## Sample Data

The application includes several pre-configured tables:

1. **customer_reviews**: Product reviews with sentiment labels
2. **support_tickets**: Support tickets with priority levels
3. **product_descriptions**: Products with specifications
4. **customer_feedback**: Customer feedback messages

All data is mock data generated for testing purposes.

## Metrics Explained

### Classification Metrics
- **Precision**: Of all items predicted as a class, how many were correct?
- **Recall**: Of all items that should be a class, how many were found?
- **F1-Score**: Harmonic mean of precision and recall
- **Accuracy**: Overall percentage of correct predictions

### LLM as Judge Metrics
- **Relevance**: How relevant is the generated text to the reference?
- **Coherence**: How well-structured and logical is the text?
- **Fluency**: How natural and grammatically correct is the text?
- **Overall Score**: Combined quality assessment

## Future Enhancements

- [ ] Connect to real Snowflake instance
- [ ] Batch evaluation support
- [ ] Export evaluation reports
- [ ] Historical comparison charts
- [ ] Custom metric configuration
- [ ] Multi-user support with authentication

## License

See the main repository LICENSE file.

## Contributing

This is part of the Snowflake samples repository. For contributions, please refer to the main repository guidelines.

