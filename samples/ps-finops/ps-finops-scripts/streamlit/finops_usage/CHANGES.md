# FinOps Streamlit App - Fixed to Use Real Data

## Summary of Changes

The Streamlit app was showing random data instead of hitting real Snowflake views. I've updated the `streamlit_main.py` file to query actual data from your FinOps views and tables.

## Changes Made

### 1. **Compute & QAS Cost Attribution**
- **Compute**: Now uses `COMPUTE_AND_QAS_CC_CURRENCY_DAY` view
- **QAS**: Attempts to use `QUERY_ACCELERATION_CC_CURRENCY_DAY` view, falls back to portion of compute costs
- **Query Count**: Now uses `QUERYCOUNTS_CC_DAY` view and sums NON_WH_QUERY_COUNT + WH_QUERY_COUNT

### 2. **Serverless Compute & Optimization Features**
- **Serverless Tasks**: Uses `SERVERLESS_TASK_CC_CREDITS_DAY` view 
- **Cloud Services**: Uses `LOGGING_EVENTS_CC_CREDITS_DAY` view
- **Snowpipe**: Uses `SNOWPIPE_COSTS_CC_CREDITS_DAY` view
- **Auto-clustering**: Uses `AUTOCLUSTERING_SCHEMA_CREDITS_DAY` view
- **Search Optimization**: Uses `SOS_CC_CREDITS_DAY` view
- **Materialized Views**: Uses `MATERIALIZED_VIEW_CC_CREDITS_DAY` view

### 3. **Storage Cost Attribution**
- **Active Storage**: Queries `TABLE_STORAGE_DETAILED` table for `ACTIVE_BYTES`
- **Fail Safe**: Queries `TABLE_STORAGE_DETAILED` table for `FAILSAFE_BYTES`
- **Time Travel**: Queries `TABLE_STORAGE_DETAILED` table for `TIME_TRAVEL_BYTES`

### 4. **Configuration & Error Handling**
- Added proper tag name configuration from `snowflake.yml`
- Added error handling with fallback to sample data if real data can't be loaded
- Added cost center filtering and date range filtering for all queries
- Added warning messages when real data cannot be loaded

### 5. **Data Processing**
- All queries respect the selected cost centers and date ranges
- Proper aggregation by day for trend visualizations
- Storage costs converted from bytes to currency using approximate $23/TB/month rate

## Files Modified

- `/streamlit/finops_usage/streamlit_main.py` - Main application file with data queries
- Added test script: `/streamlit/finops_usage/test_connection.py` (for testing connectivity)

## Testing

To test the changes:

1. Deploy the updated Streamlit app to Snowflake
2. Run the app and verify that real data appears instead of random values
3. Test the cost center and date filters to ensure they work properly
4. Check for any warning messages that indicate data loading issues

## Fallback Behavior

If any real data cannot be loaded (due to missing views, permissions, etc.), the app will:
1. Display a warning message explaining what failed
2. Fall back to the original random data generation
3. Continue to function normally for demonstration purposes

This ensures the app remains functional even if some views haven't been created yet or there are permission issues.
