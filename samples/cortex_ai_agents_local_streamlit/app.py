import streamlit as st
import json
import requests
import sseclient
import os
from dotenv import load_dotenv
import generate_jwt
import logging
import shortuuid
import pandas as pd  

load_dotenv(override=True)

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('app.log')
    ]
)
logger = logging.getLogger(__name__)

# Constants
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_ACCOUNT_URL = os.getenv("SNOWFLAKE_ACCOUNT_URL")
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_ROLE = os.getenv("SNOWFLAKE_ROLE")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA")
RSA_PRIVATE_KEY_PATH = os.getenv("RSA_PRIVATE_KEY_PATH")
PRIVATE_KEY_PASSPHRASE = os.getenv("PRIVATE_KEY_PASSPHRASE")
CORTEX_SEARCH_SERVICES = "sales_intelligence.data.sales_conversation_search"
SEMANTIC_MODELS = "@sales_intelligence.data.models/sales_metrics_model.yaml"

# Message Role Enum equivalent
class AgentMessageRole:
    USER = "user"
    ASSISTANT = "assistant"

# Agent State Enum equivalent
class AgentState:
    IDLE = "idle"
    LOADING = "loading"
    STREAMING = "streaming"
    EXECUTING_SQL = "executing_sql"
    RUNNING_ANALYTICS = "running_analytics"

def handle_chart_content(chart_content):
    """Handle chart content from the agent response"""
    try:
        st.write("## Chart Data")
        # Extract the chart specification
        chart_spec_str = chart_content.get("chart", {}).get("chart_spec", "{}")
        
        # Log the raw chart spec for debugging
        logger.debug(f"Raw chart spec: {chart_spec_str}")
        
        try:
            chart_spec = json.loads(chart_spec_str)
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse chart spec JSON: {str(e)}")
            st.error(f"Invalid chart specification: {str(e)}")
            st.code(chart_spec_str, language="json")
            return
        
        # If using Altair (recommended for Vega-Lite specs)
        import altair as alt
        
        # Create the chart from the specification
        if "data" in chart_spec and "values" in chart_spec.get("data", {}):
            chart_data = pd.DataFrame(chart_spec.get("data", {}).get("values", []))
            
            # Log first few rows for debugging
            if not chart_data.empty:
                logger.debug(f"Chart data sample: {chart_data.head(2)}")
            
            # Create the chart based on the encoding from the spec
            x_field = chart_spec.get("encoding", {}).get("x", {}).get("field")
            y_field = chart_spec.get("encoding", {}).get("y", {}).get("field")
            
            if x_field and y_field:
                # Convert date strings to datetime if needed
                if "temporal" in chart_spec.get("encoding", {}).get("x", {}).get("type", ""):
                    chart_data[x_field] = pd.to_datetime(chart_data[x_field])
                
                # Determine mark type (default to bar)
                mark_type = chart_spec.get("mark", "bar")
                
                # Create the chart with the appropriate mark type
                if mark_type == "line":
                    chart = alt.Chart(chart_data).mark_line().encode(
                        x=x_field,
                        y=y_field
                    )
                elif mark_type == "point" or mark_type == "circle":
                    chart = alt.Chart(chart_data).mark_circle().encode(
                        x=x_field,
                        y=y_field
                    )
                else:  # Default to bar
                    chart = alt.Chart(chart_data).mark_bar().encode(
                        x=x_field,
                        y=y_field
                    )
                
                # Add properties
                chart = chart.properties(
                    title=chart_spec.get("title", ""),
                    width=600,
                    height=400
                )
                
                # Display the chart
                st.altair_chart(chart, use_container_width=True)
            else:
                st.warning(f"Missing encoding fields. Need both x and y fields. Found: x={x_field}, y={y_field}")
                # Fallback to displaying the raw data
                st.write("Chart data:")
                st.dataframe(chart_data)
        else:
            st.warning("Chart data is missing or in an unexpected format.")
            st.json(chart_spec)
            
    except Exception as e:
        logger.error(f"Error rendering chart: {str(e)}", exc_info=True)
        st.error(f"Error rendering chart: {str(e)}")
        
        # Display the raw chart specification as fallback
        st.write("Raw chart specification:")
        st.code(chart_content.get("chart", {}).get("chart_spec", "{}"), language="json")

def get_empty_assistant_message(message_id=None):
    """Create an empty assistant message similar to getEmptyAssistantMessage in React"""
    if not message_id:
        message_id = shortuuid.uuid()
    
    return {
        "id": message_id,
        "role": AgentMessageRole.ASSISTANT,
        "content": []
    }

def append_text_to_assistant_message(assistant_message, text):
    """Append text to assistant message similar to appendTextToAssistantMessage in React"""
    # Check if there's already a text content item
    text_content_exists = False
    
    for content_item in assistant_message["content"]:
        if isinstance(content_item, dict) and content_item.get("type") == "text":
            content_item["text"] += text
            text_content_exists = True
            break
    
    # If no text content exists, create one
    if not text_content_exists:
        assistant_message["content"].append({
            "type": "text",
            "text": text
        })
    
    return assistant_message

def append_tool_response_to_assistant_message(assistant_message, tool_response):
    """Append tool response to assistant message similar to appendToolResponseToAssistantMessage in React"""
    assistant_message["content"].append(tool_response)
    return assistant_message

def get_sql_exec_user_message(message_id, query_id):
    """Create a SQL exec user message similar to getSQLExecUserMessage in React"""
    if not message_id:
        message_id = shortuuid.uuid()
    
    return {
        "id": message_id,
        "role": AgentMessageRole.USER,
        "content": [{
            "type": "tool_results",
            "tool_results": {
                "name": "sql_exec",
                "content": [{
                    "type": "json",
                    "json": {
                        "query_id": query_id
                    }
                }]
            }
        }]
    }

def build_standard_request_params(auth_token, messages, input_text, tool_resources=None):
    """Build standard request parameters similar to buildStandardRequestParams in React"""
    if not tool_resources:
        tool_resources = {
            "analyst1": {"semantic_model_file": SEMANTIC_MODELS},
            "search1": {
                "name": CORTEX_SEARCH_SERVICES,
                "max_results": 10
            }
        }
    
    headers = {
        "Content-Type": "application/json",
        "Accept": "text/event-stream",
        "X-Snowflake-Authorization-Token-Type": "KEYPAIR_JWT",
        "Authorization": f"Bearer {auth_token}"
    }
    
    body = {
        "model": "claude-3-5-sonnet",
        "messages": messages,
        "tools": [
            {
                "tool_spec": {
                    "type": "cortex_analyst_text_to_sql",
                    "name": "analyst1"
                }
            },
            {
                "tool_spec": {
                    "type": "cortex_search",
                    "name": "search1"
                }
            },
            {
                "tool_spec": {
                    "type": "data_to_chart",
                    "name": "data_to_chart"
                }
            },
            {
                "tool_spec": {
                    "type": "sql_exec",
                    "name": "sql_exec"
                }
            }
        ],
        "tool_resources": tool_resources
    }
    
    return {"headers": headers, "body": body}

def remove_fetched_table_from_messages(messages):
    # In Python we'll just clone the messages to avoid modifying the original
    # A proper implementation would remove table data to reduce payload size
    return messages.copy()

def append_fetched_table_to_assistant_message(assistant_message, table_data):
    """
    Append a fetched table to the assistant message with proper formatting
    IMPORTANT: is_visible parameter controls whether table is shown in UI
    """
    # Create a properly structured table object
    table_content = {
        "type": "fetched_table",
        "table": {
            "data": table_data  # Full table data from Snowflake
        }
    }
    
    assistant_message["content"].append(table_content)
    return assistant_message

def execute_snowflake_query(query, jwt_token):
    """Execute a Snowflake query via the statements API"""
    try:
        snowflake_url = f"https://{SNOWFLAKE_ACCOUNT_URL}"
        
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "User-Agent": "SalesIntelligenceApp/1.0",
            "X-Snowflake-Authorization-Token-Type": "KEYPAIR_JWT",
            "Authorization": f"Bearer {jwt_token}"
        }
        
        payload = {
            "statement": query,
            "parameters": {
                "BINARY_OUTPUT_FORMAT": "HEX",
                "DATE_OUTPUT_FORMAT": "YYYY-Mon-DD",
                "TIME_OUTPUT_FORMAT": "HH24:MI:SS",
                "TIMESTAMP_LTZ_OUTPUT_FORMAT": "",
                "TIMESTAMP_NTZ_OUTPUT_FORMAT": "YYYY-MM-DD HH24:MI:SS.FF3",
                "TIMESTAMP_TZ_OUTPUT_FORMAT": "",
                "TIMESTAMP_OUTPUT_FORMAT": "YYYY-MM-DD HH24:MI:SS.FF3 TZHTZM",
                "TIMEZONE": "America/Los_Angeles",
            }
        }
        
        logger.info(f"Executing SQL via API: {query}")
        response = requests.post(
            f"{snowflake_url}/api/v2/statements",
            headers=headers,
            json=payload
        )
        
        if response.status_code != 200:
            logger.error(f"Error executing SQL API: {response.status_code} - {response.text}")
            st.error(f"Error executing SQL: {response.status_code} - {response.text}")
            return None
            
        result_data = response.json()
        return result_data
            
    except Exception as e:
        logger.error(f"Error executing SQL API: {str(e)}", exc_info=True)
        st.error(f"Error executing SQL: {str(e)}")
        return None

def handle_new_message(messages, input_text, jwt_token):
    """Handle a new message from the user, similar to handleNewMessage in React"""
    agent_state = AgentState.IDLE
    
    if not jwt_token:
        st.error("Authorization failed: Token is not defined")
        return messages, agent_state
    
    # Clear any previous table data
    if "last_table_data" in st.session_state:
        st.session_state.last_table_data = None
    
    # Clone messages to avoid modifying the original
    new_messages = messages.copy()
    
    # Add user message
    latest_user_message_id = shortuuid.uuid()
    new_messages.append({
        "id": latest_user_message_id,
        "role": AgentMessageRole.USER,
        "content": [{"type": "text", "text": input_text}]
    })
    
    # Build request params
    request_params = build_standard_request_params(
        jwt_token, 
        remove_fetched_table_from_messages(new_messages), 
        input_text
    )
    
    agent_state = AgentState.LOADING
    
    # Create a status placeholder for showing progress
    status_placeholder = st.empty()
    status_placeholder.info("Processing your request...")
    
    # Make initial agent API call
    try:
        response = requests.post(
            f"https://{SNOWFLAKE_ACCOUNT_URL}/api/v2/cortex/agent:run",
            headers=request_params["headers"],
            json=request_params["body"],
            stream=True
        )
        
        if response.status_code != 200:
            status_placeholder.error(f"Error: {response.status_code} - {response.text}")
            return new_messages, AgentState.IDLE
        
        # Create a new assistant message
        latest_assistant_message_id = shortuuid.uuid()
        new_assistant_message = get_empty_assistant_message(latest_assistant_message_id)
        
        # Process the event stream
        sse_client = sseclient.SSEClient(response)
        
        for event in sse_client.events():
            logger.debug(f"Received SSE event: {event.data}")
            
            if event.data == "[DONE]":
                status_placeholder.empty()  # Clear the status message
                agent_state = AgentState.IDLE
                new_messages.append(new_assistant_message)
                return new_messages, agent_state
            
            try:
                data = json.loads(event.data)
                
                if "code" in data:
                    status_placeholder.error(data.get("message", "Unknown error"))
                    agent_state = AgentState.IDLE
                    return new_messages, agent_state
                
                if 'delta' in data and 'content' in data['delta']:
                    delta_content = data['delta']['content']
                    logger.debug(f"Delta content: {delta_content}")
                    
                    # Handle each content item
                    for i, content_item in enumerate(delta_content):
                        # Handle text content
                        if content_item.get("type") == "text" and "text" in content_item:
                            append_text_to_assistant_message(new_assistant_message, content_item["text"])
                            # Update status with current state
                            status_placeholder.info(f"Processing response... ({agent_state})")
                            agent_state = AgentState.STREAMING
                            
                        # Handle tool use and tool results
                        elif content_item.get("type") in ["tool_use", "tool_results"]:
                            logger.debug(f"Tool interaction: {content_item}")
                            
                            # Add the tool interaction to assistant message
                            append_tool_response_to_assistant_message(new_assistant_message, content_item)
                            
                            # If it's a SQL exec tool use, execute the SQL
                            if (content_item.get("type") == "tool_use" and 
                                content_item.get("tool_use", {}).get("name") == "sql_exec"):
                                
                                # Extract SQL statement
                                tool_use_obj = content_item.get("tool_use", {})
                                sql_statement = None
                                if "input" in tool_use_obj:
                                    sql_statement = tool_use_obj["input"].get("query")
                                
                                if sql_statement:
                                    logger.info(f"Executing SQL from tool_use: {sql_statement}")
                                    
                                    # Set agent state
                                    agent_state = AgentState.EXECUTING_SQL
                                    status_placeholder.info(f"Executing SQL query...")
                                    
                                    # Execute SQL query
                                    table_data = execute_snowflake_query(sql_statement, jwt_token)
                                    
                                    if table_data:
                                        # Check for async execution message
                                        if isinstance(table_data.get("message"), str) and "Asynchronous execution in progress" in table_data.get("message", ""):
                                            status_placeholder.error("SQL execution took too long to respond. Please try again.")
                                            agent_state = AgentState.IDLE
                                            return new_messages, agent_state
                                        
                                        # Store the original table data in session state for later use
                                        st.session_state.last_table_data = table_data
                                        
                                        # Add message indicating SQL was executed
                                        append_text_to_assistant_message(
                                            new_assistant_message, 
                                            "\n\nSQL query executed successfully. Processing results..."
                                        )
                                        
                                        # Create the SQL exec user message with just the query ID
                                        sql_exec_user_message_id = shortuuid.uuid()
                                        sql_exec_user_message = {
                                            "id": sql_exec_user_message_id,
                                            "role": AgentMessageRole.USER,
                                            "content": [
                                                {
                                                    "type": "tool_results",
                                                    "tool_results": {
                                                        "name": "sql_exec",
                                                        "content": [
                                                            {
                                                                "type": "json",
                                                                "json": {
                                                                    "query_id": table_data.get("statementHandle")
                                                                }
                                                            }
                                                        ]
                                                    }
                                                }
                                            ]
                                        }
                                        
                                        # Add to messages
                                        new_messages.append(new_assistant_message)
                                        new_messages.append(sql_exec_user_message)

                                        # Make data2answer API call
                                        agent_state = AgentState.RUNNING_ANALYTICS
                                        status_placeholder.info(f"Analyzing data...")
                                        
                                        # Continue with data2answer API call as before
                                        try:
                                            data2_request_params = build_standard_request_params(
                                                jwt_token,
                                                remove_fetched_table_from_messages(new_messages),
                                                input_text
                                            )

                                            data2_response = requests.post(
                                                f"https://{SNOWFLAKE_ACCOUNT_URL}/api/v2/cortex/agent:run",
                                                headers=data2_request_params["headers"],
                                                json=data2_request_params["body"],
                                                stream=True
                                            )
                                            
                                            if data2_response.status_code != 200:
                                                logger.error(f"Error in data2answer: {data2_response.status_code} - {data2_response.text}")
                                                status_placeholder.error(f"Error in data analysis: {data2_response.status_code}. Please try again.")
                                                agent_state = AgentState.IDLE
                                                return new_messages, agent_state
                                            
                                            # Create a new assistant message for data2answer
                                            latest_d2a_message_id = shortuuid.uuid()
                                            new_d2a_message = get_empty_assistant_message(latest_d2a_message_id)
                                            
                                            # Process data2answer event stream
                                            d2a_sse_client = sseclient.SSEClient(data2_response)

                                            st.write(d2a_sse_client)
                                            
                                            for d2a_event in d2a_sse_client.events():
                                                logger.debug(f"Received D2A SSE event: {d2a_event.data}")
                                                
                                                if d2a_event.data == "[DONE]":
                                                    logger.info("Data2answer processing complete")
                                                    status_placeholder.empty()  # Clear status
                                                    agent_state = AgentState.IDLE
                                                    new_messages.append(new_d2a_message)
                                                    return new_messages, agent_state
                                                
                                                try:
                                                    d2a_data = json.loads(d2a_event.data)
                                                    
                                                    if "code" in d2a_data:
                                                        logger.error(f"Error in data2answer event: {d2a_data.get('code')} - {d2a_data.get('message')}")
                                                        status_placeholder.error(d2a_data.get("message", "Unknown error in data analysis"))
                                                        agent_state = AgentState.IDLE
                                                        return new_messages, agent_state
                                                    
                                                    if 'delta' in d2a_data and 'content' in d2a_data['delta']:
                                                        d2a_contents = d2a_data['delta']['content']

                                                        logger.debug(f"Data2answer delta content: {d2a_contents}")
                                                        
                                                        for d2a_content in d2a_contents:
                                                            # Handle different content types
                                                            if d2a_content.get("type") == "text" and "text" in d2a_content:
                                                                # Text content
                                                                append_text_to_assistant_message(new_d2a_message, d2a_content["text"])
                                                            elif d2a_content.get("type") == "chart" and "chart" in d2a_content:
                                                                # Chart content
                                                                append_tool_response_to_assistant_message(new_d2a_message, d2a_content)
                                                                append_fetched_table_to_assistant_message(new_d2a_message, table_data)
                                                            elif d2a_content.get("type") == "table" and "table" in d2a_content:
                                                                # Table content
                                                                append_tool_response_to_assistant_message(new_d2a_message, d2a_content)
                                                                append_fetched_table_to_assistant_message(new_d2a_message, table_data)
                                                            elif d2a_content.get("type") == "tool_results" and "tool_results" in d2a_content:
                                                                # Tool results
                                                                append_tool_response_to_assistant_message(new_d2a_message, d2a_content)
                                                
                                                except json.JSONDecodeError as e:
                                                    logger.warning(f"Failed to parse D2A event data: {str(e)}")
                                                    continue
                                                except Exception as e:
                                                    logger.error(f"Error processing D2A event: {str(e)}", exc_info=True)
                                                    continue
                                            
                                            # If we get here without hitting [DONE], add the message anyway
                                            logger.info("Data2answer stream completed without [DONE] event")
                                            status_placeholder.empty()  # Clear status
                                            new_messages.append(new_d2a_message)
                                            agent_state = AgentState.IDLE
                                            return new_messages, agent_state
                                        
                                        except requests.RequestException as e:
                                            logger.error(f"Network error in data2answer API call: {str(e)}", exc_info=True)
                                            status_placeholder.error(f"Network error during data analysis: {str(e)}")
                                            agent_state = AgentState.IDLE
                                            return new_messages, agent_state
                                        except Exception as e:
                                            logger.error(f"Unexpected error in data2answer processing: {str(e)}", exc_info=True)
                                            status_placeholder.error(f"Error during data analysis: {str(e)}")
                                            agent_state = AgentState.IDLE
                                            return new_messages, agent_state
                                    else:
                                        logger.error("SQL execution failed or returned no data")
                                        status_placeholder.error("SQL execution failed or returned no data. Please try again.")
                                        agent_state = AgentState.IDLE
                                        return new_messages, agent_state
                                else:
                                    logger.warning("SQL exec tool called but no query found in input")
                            
                        # Handle tool results without tool use
                        elif content_item.get("type") == "tool_results":
                            # Only process if not already processed as part of tool use
                            if i == 0 or delta_content[i-1].get("type") != "tool_use":
                                append_tool_response_to_assistant_message(new_assistant_message, content_item)
                
            except json.JSONDecodeError as e:
                logger.warning(f"Failed to parse event data: {str(e)}")
                continue
        
        # If we get here, add the final assistant message
        status_placeholder.empty()  # Clear status
        agent_state = AgentState.IDLE
        new_messages.append(new_assistant_message)
        return new_messages, agent_state
        
    except Exception as e:
        logger.error(f"Error in handle_new_message: {str(e)}", exc_info=True)
        status_placeholder.error(f"Error: {str(e)}")
        return new_messages, AgentState.IDLE

def main():
    st.title("Intelligent Sales Assistant")

    # Initialize JWT Generator
    jwt_token = generate_jwt.JWTGenerator(
        SNOWFLAKE_ACCOUNT,
        SNOWFLAKE_USER,
        RSA_PRIVATE_KEY_PATH,
        PRIVATE_KEY_PASSPHRASE
    ).get_token()

    # Sidebar for new chat and debugging
    with st.sidebar:
        if st.button("New Conversation", key="new_chat"):
            st.session_state.messages = []
            st.session_state.agent_state = AgentState.IDLE
            st.rerun()
            
        # Debug section (can be removed in production)
        with st.expander("Debug Tools", expanded=False):
            if st.button("Show Message Structure", key="show_messages"):
                st.json(st.session_state.messages)
                
            if st.button("Clear All Messages", key="clear_messages"):
                st.session_state.messages = []
                st.rerun()

    # Initialize session state
    if 'messages' not in st.session_state:
        st.session_state.messages = []
    
    if 'agent_state' not in st.session_state:
        st.session_state.agent_state = AgentState.IDLE

    # Display current state (only for debugging, can be removed in production)
    if st.session_state.agent_state != AgentState.IDLE:
        st.info(f"Status: {st.session_state.agent_state}")

    # Chat input form
    with st.form(key="query_form"):
        question = st.text_area(
            "Enter your question here...",
            placeholder="Ask about sales conversations or sales data...",
            key="query_input",
            height=100,
        )
        submit = st.form_submit_button("Submit")

    if submit and question:
        # Process the new message
        st.session_state.messages = []
        updated_messages, new_agent_state = handle_new_message(
            st.session_state.messages,
            question,
            jwt_token
        )
        
        st.session_state.messages = updated_messages
        st.session_state.agent_state = new_agent_state
        st.rerun()  # Refresh the UI to show new messages

    # Display chat history
    for message in st.session_state.messages:
        with st.container():
            if message["role"] == AgentMessageRole.USER:
                # Display text content
                for content in message["content"]:
                    if content.get("type") == "text":
                        st.markdown("**You:**")
                        st.markdown(content["text"].replace("•", "\n\n-"))
            else:
                # Only display assistant message if it has content
                has_displayable_content = False
                
                # Check if message has any displayable content
                for content in message["content"]:
                    if (content.get("type") == "text" and content.get("text")) or \
                    content.get("type") in ["chart", "table", "fetched_table", "tool_results"]:
                        has_displayable_content = True
                        break
                
                if has_displayable_content:
                    st.markdown("**Assistant:**")
                
                    # Display content based on type
                for content in message["content"]:
                    if content.get("type") == "text":
                        st.markdown(content["text"].replace("•", "\n\n-"))
                    elif content.get("type") == "fetched_table":
                        # Always display the table regardless of is_visible flag
                        st.write("## Query Results Table")
                        table_data = content.get("table", {}).get("data", {})
                        
                        try:
                            # Display table data in a structured format
                            if "resultSetMetaData" in table_data and "rowType" in table_data["resultSetMetaData"] and "data" in table_data:
                                # Extract column names from metadata
                                columns = [col.get("name") for col in table_data["resultSetMetaData"]["rowType"]]
                                logger.debug(f"Table columns: {columns}")
                                
                                # Create a pandas DataFrame from the data
                                if table_data["data"]:
                                    # Convert the data to a proper DataFrame
                                    rows = []
                                    for row in table_data["data"]:
                                        # Handle different row formats
                                        if isinstance(row, list):
                                            rows.append(row)
                                        elif isinstance(row, dict):
                                            rows.append([row.get(col) for col in columns])
                                        else:
                                            logger.warning(f"Unexpected row type: {type(row)}")
                                    
                                    # Create DataFrame with proper column names
                                    import pandas as pd
                                    df = pd.DataFrame(rows, columns=columns)
                                    
                                    # Display table with caption
                                    st.dataframe(
                                        df,
                                        use_container_width=True,
                                        hide_index=True
                                    )
                                    
                                    # Show row count
                                    st.caption(f"Showing {len(df)} rows of {len(table_data['data'])} total rows")
                                else:
                                    st.info("Query returned no data.")
                            else:
                                # Show what fields are missing for debugging
                                missing = []
                                if "resultSetMetaData" not in table_data:
                                    missing.append("resultSetMetaData")
                                elif "rowType" not in table_data["resultSetMetaData"]:
                                    missing.append("rowType")
                                if "data" not in table_data:
                                    missing.append("data")
                                
                                st.warning(f"Table data is missing expected fields: {', '.join(missing)}")
                                
                                # Fallback if the data structure isn't as expected
                                st.write("Table Data (raw format):")
                                st.json(table_data)
                        except Exception as e:
                            st.error(f"Error displaying table data: {str(e)}")
                            logger.error(f"Error displaying table: {str(e)}", exc_info=True)
                            
                            # Show the raw data as fallback
                            st.write("Raw table data:")
                            st.write(table_data)
                    elif content.get("type") == "chart":
                        handle_chart_content(content)
                    elif content.get("type") == "tool_use":
                        tool_name = content.get("tool_use", {}).get("name", "")
                        if tool_name == "sql_exec":
                            sql = content.get("tool_use", {}).get("input", {}).get("query", "")
                            if sql:
                                st.markdown("### Generated SQL")
                                st.code(sql, language="sql")

if __name__ == "__main__":
    main()