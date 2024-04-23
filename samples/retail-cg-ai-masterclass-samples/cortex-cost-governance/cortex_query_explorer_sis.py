# import streamlit and set page width
import streamlit as st
st.set_page_config(layout="wide")

# other imports
import datetime
import pandas as pd
from snowflake.snowpark.context import get_active_session
from snowflake.snowpark.functions import col, concat, lit
import snowflake.snowpark.functions as F
from snowflake.cortex import Complete


# Get the current credentials
session = get_active_session()

st.title('Query Explorer 🤖')

st.write('Filter to a subset of queries to understand what is going on in your Snowflake environment without reading SQL!')

# snowflake COMPLETE credits per 1M tokens (info as of 04/15/2024 - check the docs for up to date models and pricing)
snowflake_llm_options = {
    'mixtral-8x7b': 0.22,
    'mistral-large': 5.10,
    'llama2-70b-chat': 0.45,
    'gemma-7b': 0.12,
    'mistral-7b': 0.12,
    'reka-flash': 0.45
}

# Example System Prompt
system_message = 'You are specialized in interpreting Snowflake SQL queries. Your task is to describe the purpose of a query in a single, easily understandable sentence, even for those unfamiliar with SQL. DO NOT USE specific database object names directly. For example, a "USE ROLE" statement should be described as the user changing their active role, and a "USE WAREHOUSE" statement as the user selecting a different warehouse to work with. I am going to give you a SQL query, and I want you to perform your task at the highest possible level!'
summary_instruction = 'You are a genius at succintly summarizing the purpose of lists of Snowflake SQL query descriptions and formatting your response into a well organize, readable, and pretty format. Summarize the general profile of this list of query descriptions in a few sentences or less.  At the beginning of your summary, tell me what type of persona is likely running these queries. Begin with the phrase, "Based on the profile of these queries"'

# Example Tag Strategy
tag_strategy = """
                You are a Snowflake SQL Expert who responds strictly with tag names from the list provided. You do not use any filler words or the word 'sure'. Here are the categories for tagging Snowflake queries:
                
                1. Data Management:
                    - Data Loading, Data Export, Batch Processing
                    - Use for queries handling the import, export, or batch processing of data.
                
                2. Data Analysis & Reporting:
                    - Report Generation, Data Exploration, User Analytics, Historical Analysis
                    - Use for analyzing data, generating reports, and conducting both current and historical data studies.
                
                3. Data Transformation & Processing:
                    - Data Transformation, Real-Time Processing, Machine Learning
                    - Use for queries that transform or process data, including real-time data streams and feeding machine learning models.
                
                4. Performance & Testing:
                    - Performance Tuning, Testing
                    - Use for queries aimed at optimizing database performance and testing data integrity or functionality.
                
                5. Compliance & Security:
                    - Compliance and Audit, Security Analysis
                    - Use for queries ensuring data compliance with regulations and conducting security-related analysis.
                
                Given the following query, select the most appropriate tag from the list above and respond with the tag name only:
                
                Query: SELECT m.brand_name, SUM(CASE WHEN is_gluten_free_flag = 'Y' THEN 1 ELSE 0 END) AS gluten_free_item_count, SUM(CASE WHEN is_dairy_free_flag = 'Y' THEN 1 ELSE 0 END) AS dairy_free_item_count, SUM(CASE WHEN is_nut_free_flag = 'Y' THEN 1 ELSE 0 END) AS nut_free_item_count FROM FROSTBYTE_TASTY_BYTES_V2.analytics.menu_v m WHERE m.brand_name IN ('Plant Palace', 'Peking Truck','Revenge of the Curds') GROUP BY m.brand_name;
                
                
                Response: Data Analysis & Reporting
                
                Here is the query text: 
                """


# App Settings
with st.expander('App Settings'):
    selected_model = st.selectbox('LLM',snowflake_llm_options.keys())
    max_records = st.slider('Max Records Per Group',10,100,50) # good practice here to limit the # of records of analysis
    

numeric_data_types = ['int','bigint','smallint','tinyint','byteint','float','float4','float8', \
                      'double','double precision','real','decimal']

# Create a transient table with filtered query history for a faster app experience
@st.cache_data
def create_transient_table(table: str, date_range: list):
    session.table(table) \
         .filter(
             (col('START_TIME') >= date_range[0]) & (col('START_TIME') <= date_range[1]) \
              & (col('IS_CLIENT_GENERATED_STATEMENT') == False) \
              & (col('QUERY_TEXT') != 'select 1') \
              & (col('QUERY_TEXT') != '<redacted>')
         ) \
         .write.mode('overwrite').save_as_table('temp_query_history',table_type='transient')
             
    return True
    

# Display a section header for filtering the data
st.write('## Select Date Range')

max = datetime.datetime.today()
min = datetime.datetime.today()

date_filter_value = st.date_input(f'Date Range: ',value=(min,max))

# Get dataset

run_query_bool = st.checkbox('Run Query')

if run_query_bool:

    # You can also swap this to use the Information Schema
    table = 'SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY'

    # Retrieve the selected table
    create_transient_table(table, date_filter_value)
    df = session.table('temp_query_history')

    # Define two functions to retrieve the minimum and maximum values of a numeric column
    @st.cache_data
    def get_slider_min(column_name: str):
        return df.select(F.min(df[column_name])).collect()[0][0]
    
    @st.cache_data
    def get_slider_max(column_name: str):
        return df.select(F.max(df[column_name])).collect()[0][0]
    
    # Create a dataframe of column names and data types
    columns = pd.DataFrame(df.select(
      "QUERY_ID",
      # "QUERY_TEXT",
      "QUERY_TYPE",
      "SESSION_ID",
      "USER_NAME",
      "ROLE_NAME",
      "WAREHOUSE_NAME",
      "WAREHOUSE_SIZE",
      # "WAREHOUSE_TYPE",
      "QUERY_TAG",
      # "EXECUTION_STATUS",
      # "ERROR_CODE",
      # "ERROR_MESSAGE",
      # "END_TIME",
      # "TOTAL_ELAPSED_TIME",
      "ROWS_PRODUCED",
      "ROWS_INSERTED"
      # "PARTITIONS_SCANNED"
    ).dtypes,columns=["Column","Type"])
    columns['Type'] = [x.split('(')[0] for x in columns['Type']]
    
    # Get a list of numeric columns
    numeric_columns = columns[columns['Type'].isin(numeric_data_types)]['Column'].to_list()
    numeric_columns.remove('SESSION_ID')
    
    # Get a list of date columns
    date_columns = columns[columns['Type'].isin(['date','timestamp'])]['Column'].to_list()
    
    # Display a section header for filtering the data
    st.write('## Flexible Filtering')

    st.write('Pick relevant fields like Warehouse, User, Role, SessionID, etc to drill into specific activity within the date range selected.')
    
    # Allow the user to select which fields to filter by
    filter_fields = st.multiselect('Select fields to filter by: ',columns)
    
    # If at least one field is selected for filtering, create a filter expression for each selected field
    if len(filter_fields) != 0:
    
        filter_expr_dict = {}
    
       
        for field in filter_fields:
    
            if field in date_columns:
                    date_toggle = st.checkbox('Date Range Mode',True,field)
    
            # If the field is numeric, allow the user to select a range of values to filter by
            if field in numeric_columns:
                min = get_slider_min(field) # type: ignore
                max = get_slider_max(field) # type: ignore
                filter_value = st.slider(f'{field}',float(min),float(max),(float(min),float(max))) # type: ignore
                filter_expr = f"({field} >= {filter_value[0]} and {field} <= {filter_value[1]})"
    
            # If field is a date, use a date_picker object to filter 
            elif field in date_columns and date_toggle:
                
                min = get_slider_min(field) # type: ignore
                max = get_slider_max(field) # type: ignore
    
                filter_value = st.date_input(f'{field} date range: ',value=(min,max),min_value=min, max_value=max)
    
                if len(filter_value) == 0:
                    break
                elif len(filter_value) == 1:
                    filter_expr = f"({field} = '{filter_value[0]}')"
                else:
                    # filter_expr = f"({field} in {tuple([str(item) for item in filter_value])})"
                    filter_expr = f"({field} >= '{filter_value[0]}' and {field} <= '{filter_value[1]}')"
    
            # elif field == 'SESSION_ID':
            #     filter_value = st.text_input(f'Input a {field}')
            #     filter_expr = f"({field} = '{filter_value}')"
                
            
            # If the field is not numeric or a date, allow the user to select a value or values to filter by
            else:
                filter_value = st.multiselect(f'{field}',df[[field]].distinct()) # type: ignore
    
                if len(filter_value) == 0:
                    break
                elif len(filter_value) == 1:
                    filter_expr = f"({field} = '{filter_value[0]}')"
                else:
                    filter_expr = f"({field} in {tuple([str(item) for item in filter_value])})"
    
            filter_expr_dict[field] = filter_expr
    
        # Display the filter expressions
        with st.expander('View active filters'):
            st.write(filter_expr_dict)
    
        # If at least one filter expression was created, apply the filters to the data
        if len(filter_expr_dict) != 0:
    
            final_filter = " and ".join(list(filter_expr_dict.values()))
            filtered_df = df.filter(final_filter)
    
            # Display metrics and a sample of the filtered data
            st.metric('Rows',"{:,}".format(filtered_df.count()))
            st.dataframe(df.limit(10).filter(final_filter))

    llm_enabled = st.checkbox('Enable LLM Summarization')


    if llm_enabled:

        # Pass each query into the Complete call for summarization and tagging
        summary_df = filtered_df[['QUERY_TEXT']] \
                .with_column("description",Complete(selected_model,concat(lit(system_message),filtered_df['QUERY_TEXT']))) \
                .with_column("llm_query_tag",Complete(selected_model,concat(lit(tag_strategy),filtered_df['QUERY_TEXT'])))
        
        # Display results in dataframe
        st.header('Query Analysis :mag_right:')
        st.dataframe(summary_df.limit(max_records))


        # concatenate the query text into an array, and pass in one call for summarization
        query_summary_list = summary_df.select_expr("ARRAY_AGG(DESCRIPTION)")
        
        ai_summary = query_summary_list.with_column("summary", \
            Complete(selected_model, 
                     concat(
                         lit(summary_instruction),
                         F.to_varchar(query_summary_list["ARRAY_AGG(DESCRIPTION)"])
                           )
                    )).collect()[0][1]

        
        st.header('AI Summary :brain:')
        st.write(ai_summary)