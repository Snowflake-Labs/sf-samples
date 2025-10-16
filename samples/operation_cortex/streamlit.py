import streamlit as st
import json
import pandas as pd
import random
import _snowflake
from snowflake.snowpark.context import get_active_session

# Initialize Snowflake session
try:
    session = get_active_session()
except:
    st.error("Failed to establish Snowflake session. Please check your connection.")
    session = None

# Snowflake API configuration
API_ENDPOINT = "/api/v2/cortex/agent:run"
API_TIMEOUT = 50000  # in milliseconds

CORTEX_SEARCH_SERVICES = "spy_agency.intel.spy_mission_search"
SEMANTIC_MODELS = "@spy_agency.intel.stage/spy_report_semantic_model.yaml"

def run_snowflake_query(query):
    """Execute a Snowflake query and return results"""
    try:
        if session is None:
            return None
        
        df = session.sql(query.replace(';',''))
        return df
    except Exception as e:
        st.error(f"Error executing SQL: {str(e)}")
        return None

def snowflake_api_call(query: str, limit: int = 10):
    """Make a call to Snowflake Cortex API"""
    payload = {
        "model": "claude-3-5-sonnet",
        "response-instruction": "You will always maintain a serious tone and provide concise response, you will always refer the user as Agent.",
        "messages": [
            {
                "role": "user",
                "content": [
                    {
                        "type": "text",
                        "text": query
                    }
                ]
            }
        ],
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
            }
        ],
        "tool_resources": {
            "analyst1": {"semantic_model_file": SEMANTIC_MODELS},
            "search1": {
                "name": CORTEX_SEARCH_SERVICES,
                "max_results": limit,
                "id_column": "mission_id"
            }
        }
    }
    
    try:
        resp = _snowflake.send_snow_api_request(
            "POST",  # method
            API_ENDPOINT,  # path
            {},  # headers
            {},  # params
            payload,  # body
            None,  # request_guid
            API_TIMEOUT,  # timeout in milliseconds,
        )
        
        if resp["status"] != 200:
            st.error(f"❌ HTTP Error: {resp['status']} - {resp.get('reason', 'Unknown reason')}")
            st.error(f"Response details: {resp}")
            return None
        
        try:
            response_content = json.loads(resp["content"])
        except json.JSONDecodeError:
            st.error("❌ Failed to parse API response. The server may have returned an invalid JSON format.")
            st.error(f"Raw response: {resp['content'][:200]}...")
            return None
            
        return response_content
            
    except Exception as e:
        st.error(f"Error making request: {str(e)}")
        return None

def process_sse_response(response):
    """Process SSE response from Snowflake API"""
    text = ""
    sql = ""
    citations = []
    
    if not response:
        return text, sql, citations
    if isinstance(response, str):
        return text, sql, citations
    try:
        for event in response:
            if event.get('event') == "message.delta":
                data = event.get('data', {})
                delta = data.get('delta', {})
                
                for content_item in delta.get('content', []):
                    content_type = content_item.get('type')
                    if content_type == "tool_results":
                        tool_results = content_item.get('tool_results', {})
                        if 'content' in tool_results:
                            for result in tool_results['content']:
                                if result.get('type') == 'json':
                                    text += result.get('json', {}).get('text', '')
                                    search_results = result.get('json', {}).get('searchResults', [])
                                    for search_result in search_results:
                                        citations.append({'source_id':search_result.get('source_id',''), 'doc_id':search_result.get('doc_id', '')})
                                    sql = result.get('json', {}).get('sql', '')
                    if content_type == 'text':
                        text += content_item.get('text', '')
                            
    except json.JSONDecodeError as e:
        st.error(f"Error processing events: {str(e)}")
                
    except Exception as e:
        st.error(f"Error processing events: {str(e)}")
        
    return text, sql, citations

# Fetch mission data from Snowflake
def get_spy_missions():
    if session is None:
        # Return fallback data if Snowflake connection fails
        return pd.DataFrame([
            {'mission_id': 'M001', 'message_text': 'Package secured. Rendezvous at midnight. Codeword: Shadow.', 'sender_code': 'X7A', 'receiver_code': 'K9Q', 'mission_location': 'Berlin', 'transmission_date': '2025-03-10 23:45:00', 'encryption_level': 'HIGH'},
            {'mission_id': 'M003', 'message_text': 'Possible breach. Suspect a mole in the network.', 'sender_code': 'K9Q', 'receiver_code': 'B3Z', 'mission_location': 'Paris', 'transmission_date': '2025-03-09 18:20:00', 'encryption_level': 'HIGH'},
            {'mission_id': 'M008', 'message_text': 'The safe house is compromised. Move to Sector 9.', 'sender_code': 'K9Q', 'receiver_code': 'T2M', 'mission_location': 'Tokyo', 'transmission_date': '2025-03-04 14:20:00', 'encryption_level': 'HIGH'},
            {'mission_id': 'M013', 'message_text': 'Final instructions in the briefcase. Retrieve immediately. Keyphrase: Red Falcon.', 'sender_code': 'L3P', 'receiver_code': 'T2M', 'mission_location': 'Toronto', 'transmission_date': '2025-02-27 11:40:00', 'encryption_level': 'HIGH'}
        ])
    
    df = run_snowflake_query("SELECT * FROM spy_missions")
    if df is not None:
        return df.to_pandas()
    else:
        return pd.DataFrame()

# Fetch reports data from Snowflake
def get_spy_reports():
    if session is None:
        # Return fallback data if Snowflake connection fails
        return pd.DataFrame([
            {'report_id': 'R001', 'agent_code': 'X7A', 'mission_id': 'M001', 'mission_outcome': 'Success', 'suspected_double_agent': False, 'last_known_location': 'Berlin', 'failure_reason': 'N/A'},
            {'report_id': 'R003', 'agent_code': 'K9Q', 'mission_id': 'M003', 'mission_outcome': 'Failed', 'suspected_double_agent': True, 'last_known_location': 'Paris', 'failure_reason': 'Double agent interference'},
            {'report_id': 'R008', 'agent_code': 'K9Q', 'mission_id': 'M008', 'mission_outcome': 'Compromised', 'suspected_double_agent': True, 'last_known_location': 'Tokyo', 'failure_reason': 'Safe house compromised'},
            {'report_id': 'R013', 'agent_code': 'L3P', 'mission_id': 'M013', 'mission_outcome': 'Compromised', 'suspected_double_agent': False, 'last_known_location': 'Toronto', 'failure_reason': 'Intercepted courier'}
        ])
    
    df = run_snowflake_query("SELECT * FROM spy_reports")
    if df is not None:
        return df.to_pandas()
    else:
        return pd.DataFrame()

# Load mission and report data
missions_df = get_spy_missions()
reports_df = get_spy_reports()

# Pre-defined mission questions and answers
MISSION_QUESTIONS = [
    {
        "mission_id": "M003",
        "question": "Identify the double agent in Paris. Who was the operative that compromised mission M003?",
        "answer": "K9Q",
        "hint": "Check the mission reports for Paris operations and look for suspected double agents.",
        "cortex_query": "Who was the agent for mission M003 and are they suspected as a double agent?"
    },
    {
        "mission_id": "M008",
        "question": "Locate the compromised safe house. In which city was our safe house compromised?",
        "answer": "Tokyo",
        "hint": "Review the mission reports with 'compromised' status.",
        "cortex_query": "In which city was the safe house compromised?"
    },
    {
        "mission_id": "M001",
        "question": "What is the codeword for the Berlin rendezvous?",
        "answer": "Shadow",
        "hint": "Check the intercepted messages from Berlin operations.",
        "cortex_query": "What is the codeword mentioned in the Berlin mission?"
    },
    {
        "mission_id": "M013",
        "question": "What was the keyphrase mentioned in the Toronto mission?",
        "answer": "Red Falcon",
        "hint": "Review the intercepted messages from Toronto.",
        "cortex_query": "What keyphrase was mentioned in the Toronto mission?"
    }
]

def main():
    # Initialize session state
    if 'game_started' not in st.session_state:
        st.session_state.game_started = False
    
    if 'game_completed' not in st.session_state:
        st.session_state.game_completed = False
        
    if 'current_step' not in st.session_state:
        st.session_state.current_step = 1
    
    if 'selected_questions' not in st.session_state:
        # Randomly select 2 questions for the game
        st.session_state.selected_questions = random.sample(MISSION_QUESTIONS, 2)
    
    if 'correct_answers' not in st.session_state:
        st.session_state.correct_answers = 0
    
    if 'cortex_response' not in st.session_state:
        st.session_state.cortex_response = None
    
    if 'cortex_sql' not in st.session_state:
        st.session_state.cortex_sql = None
    
    if 'cortex_citations' not in st.session_state:
        st.session_state.cortex_citations = []
        
    # Start screen
    if not st.session_state.game_started:
        show_start_screen()
    else:
        # Game is in progress
        show_game_screen()

def show_start_screen():
    st.title("🕵️ Operation Cortex")
    
    st.markdown("""
    ## Top Secret Briefing
    
    **Agent,**
    
    Welcome to Operation Cortex. Your mission, should you choose to accept it, consists of 2 crucial intelligence-gathering tasks:
    
    1. You will be given access to our database of intercepted messages and mission reports
    2. For each step, you must solve one intelligence puzzle using our advanced Cortex AI assistant
    
    ### Mission Objectives:
    - Analyze intercepted messages
    - Review spy reports
    - Identify double agents
    - Locate compromised safe houses
    
    ### How to Use Cortex AI:
    - The Cortex AI assistant will help you analyze intelligence data
    - You can ask Cortex specific questions about the missions
    - Use the intelligence provided to solve each mission step
    
    ### Mission Success Criteria:
    Successfully complete both intelligence tasks to complete the operation.
    
    *This briefing will self-destruct when you begin the mission.*
    """)
    
    if st.button("BEGIN MISSION", key="start_button"):
        st.session_state.game_started = True
        st.rerun()

def show_game_screen():
    if st.session_state.game_completed:
        show_mission_complete()
        return
    
    # Header
    st.title(f"🕵️ Operation Cortex: Step {st.session_state.current_step}/2")
    
    # Get current question
    current_question = st.session_state.selected_questions[st.session_state.current_step - 1]
    
    # Display question
    st.markdown(f"## Intelligence Task #{st.session_state.current_step}")
    st.markdown(f"**{current_question['question']}**")
    
    # Show mission data
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("📡 Intercepted Messages")
        st.dataframe(missions_df, use_container_width=True)
    
    with col2:
        st.subheader("📋 Mission Reports")
        st.dataframe(reports_df, use_container_width=True)
    
    # Cortex Assistant Section
    st.subheader("🤖 Cortex AI Assistant")
    
    # Cortex query input
    with st.form(key="cortex_form"):
        cortex_query = st.text_input(
            "Ask Cortex AI a question:",
            value=current_question["cortex_query"],
            key="cortex_input"
        )
        
        submitted = st.form_submit_button("Query Cortex AI")
        
        if submitted:
            with st.spinner("Cortex AI processing..."):
                # Call the Snowflake API
                response = snowflake_api_call(cortex_query, 1)
                
                # Process the response
                text, sql, citations = process_sse_response(response)
                
                # Store in session state
                st.session_state.cortex_response = text
                st.session_state.cortex_sql = sql
                st.session_state.cortex_citations = citations
                
                # Rerun to refresh the UI
                st.rerun()
    
    # Display Cortex response
    if st.session_state.cortex_response:
        st.markdown("### Cortex AI Response:")
        st.markdown(st.session_state.cortex_response.replace("•", "\n\n"))
        
        # Display citations/mission details if available
        if st.session_state.cortex_citations:
            st.markdown("### Mission Details:")
            for citation in st.session_state.cortex_citations:
                mission_id = citation.get("doc_id", "")
                if mission_id:
                    query = f"SELECT * FROM spy_missions WHERE mission_id = '{mission_id}'"
                    result = run_snowflake_query(query)
                    if result is not None:
                        st.dataframe(result.to_pandas())
        
        # Display SQL if available
        if st.session_state.cortex_sql:
            with st.expander("View Generated SQL"):
                st.code(st.session_state.cortex_sql, language="sql")
                
                # Execute and show results
                results = run_snowflake_query(st.session_state.cortex_sql)
                if results is not None:
                    st.markdown("### SQL Results:")
                    st.dataframe(results.to_pandas())
    
    # Answer form
    with st.form(key=f"step_{st.session_state.current_step}_form"):
        user_answer = st.text_input("Enter your answer to complete this step:")
        
        # Hint expander
        with st.expander("Need a hint?"):
            st.write(current_question["hint"])
        
        submitted = st.form_submit_button("Submit Answer")
        
        if submitted:
            if user_answer.lower() == current_question["answer"].lower():
                st.success("✅ Correct! Intelligence verified.")
                st.session_state.correct_answers += 1
                
                # Advance to next step or complete game
                if st.session_state.current_step < 2:
                    st.session_state.current_step += 1
                    # Reset Cortex responses for next step
                    st.session_state.cortex_response = None
                    st.session_state.cortex_sql = None
                    st.session_state.cortex_citations = []
                    st.rerun()
                else:
                    st.session_state.game_completed = True
                    st.rerun()
            else:
                st.error("❌ Incorrect. Review the intelligence and try again.")

def show_mission_complete():
    st.title("🕵️ Mission Debriefing")
    
    if st.session_state.correct_answers == 2:
        st.markdown("""
        ## 🌟 MISSION ACCOMPLISHED 🌟
        
        **Congratulations, Agent!**
        
        You have successfully completed all intelligence tasks. Your analytical skills have proven invaluable to the agency.
        
        The intelligence you've gathered will be crucial for our ongoing operations. Your performance has been noted in your service record.
        """)
    else:
        st.markdown("""
        ## MISSION PARTIALLY SUCCESSFUL
        
        **Agent,**
        
        While you were able to gather some valuable intelligence, not all tasks were completed successfully.
        
        Further training may be required before your next field assignment.
        """)
    
    if st.button("START NEW MISSION"):
        # Reset game state
        st.session_state.game_started = False
        st.session_state.game_completed = False
        st.session_state.current_step = 1
        st.session_state.selected_questions = random.sample(MISSION_QUESTIONS, 2)
        st.session_state.correct_answers = 0
        st.session_state.cortex_response = None
        st.session_state.cortex_sql = None
        st.session_state.cortex_citations = []
        st.rerun()

if __name__ == "__main__":
    main()