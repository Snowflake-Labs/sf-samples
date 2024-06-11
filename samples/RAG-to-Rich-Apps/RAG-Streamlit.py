# Import python packages
import streamlit as st
from snowflake.snowpark.context import get_active_session

# Get the current credentials
session = get_active_session()

st.title("Ask Your Data Anything :snowflake:")
st.write("""Built using end-to-end RAG in Snowflake with Cortex functions.""")

#allow an end user to select the appropriate model
model = st.sidebar.selectbox("Select your Cortex model:",("mixtral-8x7b","mistral-7b","mistral-large","Gemma-7b","llama2-70b-chat"))

#Initialisation chat message + sample questions
if "messages" not in st.session_state.keys(): # Initialize the chat message history
    st.session_state.messages = [
        {"role": "assistant", "content": 
         """Hello and welcome. My name is Huberbot. I'm built to surface information based on the Andrew Huberman podcast, Huberman Labs. 
         \n Ask me anything. Some of the sample questions you may consider:
         \n - What makes time perceived to be slower?
         \n - How should I optimise my caffeine intake?
         \n - How can I control pain efficiently?"""}
    
    ]

if prompt := st.chat_input("Enter your question here"): # Prompt for user input and save to chat history
    st.session_state.messages.append({"role": "user", "content": prompt})

for message in st.session_state.messages: # Display the prior chat messages
    with st.chat_message(message["role"]):
        st.write(message["content"])

# If last message is not from assistant, generate a new response
if st.session_state.messages[-1]["role"] != "assistant":
    with st.chat_message("assistant"):
        with st.spinner("Thinking..."):
            prompt = prompt.replace("'","\\'") #remove pesky ' chars
            q_prompt = f"""
            select snowflake.CORTEX.complete(
                '{model}', 
                concat( 
                    '<s>[INST] <<SYS>>{{Answer the question using the context provided. The answer should be in your own words. Be concise. Limit your output to 200 words.','Context: ', 
                    (
                        with my_chunks(CHUNK) as (
                        select CHUNK from LLM_DEMO.PODCASTS.VECTOR_STORE
                        order by vector_l2_distance(
                        snowflake.CORTEX.embed_text('e5-base-v2', 
                        '{prompt}'
                        ), CHUNK_EMBEDDING
                        ) limit 3)
                        select listagg(CHUNK) as CHUNKS from my_chunks
                    ),
                    '}} <</SYS>> Question: ', 
                    '{prompt}',
                    'Answer: [/INST]'
                )
            ) as response;
            """
            response = session.sql(q_prompt).collect()
            response = response[0][0].replace('"','')
            st.info(response)
            message = {"role": "assistant", "content": response}
            st.session_state.messages.append(message) # Add response to message history
