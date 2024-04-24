# Import necessary libraries
import streamlit as st
from snowflake.snowpark.context import get_active_session
from snowflake.snowpark.functions import call_udf, col, concat, lit
import snowflake.snowpark.functions as F
import pandas as pd

# Get the active Snowflake session
session = get_active_session()

# Define database, schema, and table names
db_name = 'RETAIL_CG_MASTERCLASS'
schema_name = 'PUBLIC'
table_name = 'CHUNKS_TABLE'

# Create sidebar columns for toggles
col1, col2 = st.sidebar.columns(2)

# Add a RAG toggle in the first column
with col1:
    rag_on = st.toggle('Use RAG', value=False)

# Show the dropdown if the RAG toggle is on
if rag_on:
    # Fetch unique EPISODE_NAME from vector_store table
    query = f"SELECT DISTINCT RELATIVE_PATH FROM {db_name}.{schema_name}.{table_name} ORDER BY RELATIVE_PATH"
    file_df = session.sql(query).collect()
    options = [row['RELATIVE_PATH'] for row in file_df]
    options.insert(0, 'All Documents')
    
    # Multiselect for selecting options
    selected_options = st.sidebar.multiselect('Select options', options, default='All Documents')

# Function to get table names from Snowflake
def get_table_names():
    query = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'PUBLIC'"
    tables_df = session.sql(query).collect()
    return [row['TABLE_NAME'] for row in tables_df]

# Function to fetch data from a selected table
def fetch_data_from_table(table_name):
    sql = f"SELECT * FROM {table_name}"
    data = session.sql(sql).to_pandas()
    return data

# Function to format sample data as a Markdown table
def format_sample_data_as_markdown(df_sample):
    headers = df_sample.columns
    sample_data_md = "**Sample Data:**\n\n"
    sample_data_md += "| " + " | ".join(headers) + " |\n"
    sample_data_md += "|-" + "-|-".join(["" for _ in headers]) + "-|\n"

    for index, row in df_sample.iterrows():
        row_values = [str(row[col]) if row[col] is not None else "" for col in headers]
        sample_data_md += "| " + " | ".join(row_values) + " |\n"

    return sample_data_md

# Streamlit Sidebar for Snowflake Tables
st.sidebar.title('Snowflake Data Viewer')
table_names = ['Select a table'] + get_table_names()
selected_table = st.sidebar.selectbox("Select a table:", table_names)

# Fetch and display data if a table is selected
if selected_table and selected_table != 'Select a table':
    data = fetch_data_from_table(selected_table)
    st.sidebar.write(f"Records from {selected_table}:")
    st.sidebar.dataframe(data.head(5))

# Analyze button in the sidebar
if st.sidebar.button('Analyze'):
    if selected_table and selected_table != 'Select a table':
        analysis_messages = []

        # Fetch and display metadata for the selected table
        try:
            metadata_query = f"""
            SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, DATA_TYPE, IS_NULLABLE 
            FROM INFORMATION_SCHEMA.COLUMNS 
            WHERE TABLE_NAME = '{selected_table}' 
            AND TABLE_SCHEMA = 'PUBLIC'
            """
            metadata_df = session.sql(metadata_query).to_pandas()
            if not metadata_df.empty:
                table_schema = metadata_df['TABLE_SCHEMA'].iloc[0]
                metadata_message = "**Metadata for {} in schema {}:**\n\n".format(selected_table, table_schema)
                metadata_message += "\n".join([f"- **{row['COLUMN_NAME']}**: {row['DATA_TYPE']}, Nullable: {row['IS_NULLABLE']}" for index, row in metadata_df.iterrows()])
                analysis_messages.append({"role": "assistant", "content": metadata_message})
            else:
                analysis_messages.append({"role": "assistant", "content": f"No metadata found for table {selected_table}."})
        except Exception as e:
            error_message = f"Error fetching metadata for table {selected_table}: {e}"
            analysis_messages.append({"role": "assistant", "content": error_message})

        # Perform EDA
        try:
            df = session.table(selected_table)

            # 1. Show the schema
            schema_message = "**Schema:**\n\n" + "\n".join([f"- {field.name}: {field.datatype}, nullable: {field.nullable}" for field in df.schema.fields])
            analysis_messages.append({"role": "assistant", "content": schema_message})

            # 2. Get summary statistics for numerical columns
            summary_stats = df.describe().collect()
            summary_stats_md = "**Summary Statistics:**\n\n"
            summary_stats_md += "| Metric | " + " | ".join(summary_stats[0].as_dict().keys()) + " |\n"
            summary_stats_md += "|--------|" + "|".join(["--------" for _ in summary_stats[0].as_dict().keys()]) + "|\n"

            for stat in summary_stats:
                row_dict = stat.as_dict()
                row_values = [str(row_dict[key]) if row_dict[key] is not None else "" for key in row_dict.keys()]
                summary_stats_md += "| " + " | ".join(row_values) + " |\n"

            analysis_messages.append({"role": "assistant", "content": summary_stats_md})

            # 3. Count of rows
            row_count = df.count()
            row_count_message = f"**Row Count:** {row_count}"
            analysis_messages.append({"role": "assistant", "content": row_count_message})

            # Fetch a sample of data from the selected table
            sample_data = df.sample(n=10).collect()
            df_sample = pd.DataFrame([row.as_dict() for row in sample_data])
            sample_data_md = format_sample_data_as_markdown(df_sample)
            analysis_messages.append({"role": "assistant", "content": sample_data_md})

            # Check for missing values
            null_values_counts = df.agg(*(F.count(col).alias(col) for col in df.columns)).collect()[0].as_dict()
            null_values_message = "**Missing Values:**\n\n" + "\n".join([f"- {col}: {row_count - count}" for col, count in null_values_counts.items()])
            analysis_messages.append({"role": "assistant", "content": null_values_message})

            # Check for duplicates and unique values by column
            duplicates_and_uniques_messages = []
            for column in df.columns:
                value_counts_df = df.group_by(column).agg(F.count("*").alias("count")).sort(F.col("count").desc())
                unique_values_count = value_counts_df.filter(F.col("count") == 1).count()
                duplicates_count = value_counts_df.filter(F.col("count") > 1).count()
                message = f"- Column '{column}': {unique_values_count} unique value(s), {duplicates_count} duplicate value(s)."
                duplicates_and_uniques_messages.append(message)
            duplicates_and_uniques_summary = "**Duplicates and Uniques Check:**\n\n" + "\n".join(duplicates_and_uniques_messages)
            analysis_messages.append({"role": "assistant", "content": duplicates_and_uniques_summary})

        except Exception as e:
            error_message = f"Error performing EDA on table {selected_table}: {e}"
            analysis_messages.append({"role": "assistant", "content": error_message})

        # Append analysis messages to the chat
        for msg in analysis_messages:
            st.session_state["messages"].append(msg)

    else:
        st.session_state["messages"].append({"role": "assistant", "content": "Please select a table to analyze."})

# Multi-line text area for code input
user_code = st.sidebar.text_area("Insert your code here", height=300)

# Dropdown for selecting code type (Python or SQL)
code_type = st.sidebar.selectbox("Select code type", ["SQL", "Python"])

# Run Code button in the sidebar
if st.sidebar.button('Run Code'):
    formatted_code = "No code provided."
    if code_type == "Python":
        f = io.StringIO()
        with redirect_stdout(f):
            try:
                exec(user_code)
                execution_output = f.getvalue()
                formatted_code = f"```python\n{user_code}\n```"
                formatted_output = f"```python\n{execution_output}\n```"
                st.session_state["messages"].append({"role": "user", "content": formatted_code})
                st.session_state["messages"].append({"role": "assistant", "content": f"Code executed successfully:\n{formatted_output}"})
            except Exception as e:
                error_message = f"Error executing code: {e}"
                st.session_state["messages"].append({"role": "user", "content": formatted_code})
                st.session_state["messages"].append({"role": "assistant", "content": error_message})
    elif code_type == "SQL":
        try:
            result = session.sql(user_code).collect()
            if result:
                df = pd.DataFrame([row.as_dict() for row in result])
                formatted_code = f"```SQL\n{user_code}\n```"
                formatted_output = f"```python\n{df}\n```"
                st.session_state["messages"].append({"role": "user", "content": formatted_code})
                st.session_state["messages"].append({"role": "assistant", "content": f"SQL executed successfully:\n{formatted_output}"})
            else:
                st.session_state["messages"].append({"role": "user", "content": formatted_code})
                st.session_state["messages"].append({"role": "assistant", "content": "SQL executed successfully, no results to display."})
        except Exception as e:
            error_message = f"Error executing SQL code: {e}"
            st.session_state["messages"].append({"role": "user", "content": formatted_code})
            st.session_state["messages"].append({"role": "assistant", "content": error_message})

# Title of the app
st.title("❄️ Snowflake GPT")

# Define the model to use
#model = 'llama2-70b-chat'  # You can adjust this based on your requirements or selections
#model = 'mixtral-8x7b'
model = 'mistral-large'

# System messages for different scenarios
system_message = '''You are an expert AI assistant.
Answer thoroughly and accurately. Your goal is to help the user.
It is important to not make anything up - if you need clarification, be sure to ask: '''

rag_system_message = '''You are an expert AI assistant that answer's the question based on precisely this:
{embeddings}

--
Be concise and thorough.
This is very important to get correct. DO NOT MAKE ANYTHING UP. If you don't know, just say so.
If you need clarification, be sure to ask. I will give you at $2000 bonus. You can really do this, I believe in you.
'''

table_system_message = '''Pulling information from {selected_table}'''

# Initialize messages in session state if it doesn't exist
if "messages" not in st.session_state:
    st.session_state["messages"] = [{"role": "assistant", "content": "How can I help you?"}]

# Display previous messages
for msg in st.session_state["messages"]:
    role = "user" if msg["role"] == "user" else "assistant"
    st.chat_message(role).markdown(msg["content"])

# Input for new messages
if prompt := st.chat_input():
    st.session_state["messages"].append({"role": "user", "content": prompt})
    st.chat_message("user").write(prompt)

    # Prepare the conversation history
    conversation_history = "\n".join([f"{msg['role']}: {msg['content']}" for msg in st.session_state["messages"]])

    if selected_table and selected_table != 'Select a table':
        conversation_history += f"\nSelected Table: {selected_table}"

    if rag_on:
        # RAG
        if 'All Documents' in selected_options or not selected_options:
            where_clause = ""
        else:
            formatted_options = ", ".join(f"'{option}'" for option in selected_options if option != 'All Documents')
            where_clause = f"WHERE RELATIVE_PATH IN ({formatted_options})"
        safe_prompt = prompt.replace("'", "''")

        embeddings = session.sql(f'''
        select array_agg(*)::varchar from (
                        (select chunk from {db_name}.{schema_name}.{table_name}
                        {where_clause}
                        order by vector_l2_distance(
                        snowflake.cortex.embed_text('e5-base-v2', 
                        '{safe_prompt}'
                        ), chunk_vec
                        ) limit 10))
        ''').collect()
        response_df = session.create_dataframe([f"{rag_system_message}\n{conversation_history}"]).select(
            call_udf('snowflake.cortex.complete', model, concat(
                lit(f"{rag_system_message}\n{embeddings}\n{conversation_history}"), 
                F.to_varchar(lit(f"\nUser: {prompt}"))
            ))
        )
    else:
        # Call the Snowflake Cortex UDF to get the response
        response_df = session.create_dataframe([f"{system_message}\n{conversation_history}"]).select(
            call_udf('snowflake.cortex.complete', model, concat(
                lit(f"{system_message}\n{conversation_history}"), 
                F.to_varchar(lit(f"\nUser: {prompt}"))
            ))
        )

    # Collect the response and update the chat
    full_response = response_df.collect()[0][0]
    st.session_state["messages"].append({"role": "assistant", "content": full_response})
    st.chat_message("assistant").write(full_response)
