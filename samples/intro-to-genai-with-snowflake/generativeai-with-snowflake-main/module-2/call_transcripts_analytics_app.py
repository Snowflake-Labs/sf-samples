import streamlit as st
from snowflake.snowpark.context import get_active_session

st.set_page_config(layout='wide')
session = get_active_session()

transcript_example = f"""Agent: Hello, how can I assist you today? 
Customer: Hi, I recently bought the XYZ-2000 vacuum cleaner and it's not working properly. 
Agent: I'm sorry to hear that. Could you please describe the issue? 
Customer: Sure, when I turn it on, it makes a strange noise and doesn't suck up dirt like it should. 
Agent: It sounds like a motor issue. Have you checked if there's any blockage in the vacuum? 
Customer: Yes, I've checked and there's no blockage. 
Agent: Alright, it seems like the motor might be defective. 
I'll arrange for a replacement motor to be sent to you. 
Customer: Thank you, that would be great. 
Agent: You're welcome. It should arrive within the next few days. 
If you have any other issues, feel free to contact us. 
Customer: Okay, thanks for your help. 
Agent: You're welcome. Have a great day!"""

def summarize():
    with st.container():
        st.header("JSON Summary")
        entered_text = st.text_area("Enter text",label_visibility="hidden",height=400,placeholder='Enter call transcript')    
        if entered_text:
            entered_text = entered_text.replace("'", "\\'")
            prompt = f"Summarize this transcript in less than 200 words. Put the product name, defect if any, and summary in JSON format: {entered_text}"
            cortex_prompt = "'[INST] " + prompt + " [/INST]'"
            cortex_response = session.sql(f"select snowflake.cortex.complete('mistral-large', {cortex_prompt}) as response").to_pandas().iloc[0]['RESPONSE']
            st.write(cortex_response)

def translate():
    supported_languages = {'German':'de','French':'fr','Korean':'ko','Portuguese':'pt','English':'en','Italian':'it','Russian':'ru','Swedish':'sv','Spanish':'es','Japanese':'ja','Polish':'pl'}
    with st.container():
        st.header("Translate With Snowflake Cortex")
        col1,col2 = st.columns(2)
        with col1:
            from_language = st.selectbox('From',dict(sorted(supported_languages.items())))
        with col2:
            to_language = st.selectbox('To',dict(sorted(supported_languages.items())))
        entered_text = st.text_area("Enter text",label_visibility="hidden",height=300,placeholder='For example: call customer transcript')
        if entered_text:
          entered_text = entered_text.replace("'", "\\'")
          cortex_response = session.sql(f"select snowflake.cortex.translate('{entered_text}','{supported_languages[from_language]}','{supported_languages[to_language]}') as response").to_pandas().iloc[0]['RESPONSE']
          st.write(cortex_response)

def sentiment_analysis():
    with st.container():
        st.header("Sentiment Analysis With Snowflake Cortex")
        entered_text = st.text_area("Enter text",label_visibility="hidden",height=400,placeholder='For example: customer call transcript')
        if entered_text:
          entered_text = entered_text.replace("'", "\\'")
          cortex_response = session.sql(f"select snowflake.cortex.sentiment('{entered_text}') as sentiment").to_pandas()
          st.caption("Score is between -1 and 1; -1 = Most negative, 1 = Positive, 0 = Neutral")  
          st.write(cortex_response)

page_names_to_funcs = {
    "JSON Summary": summarize,
    "Translate": translate,
    "Sentiment Analysis": sentiment_analysis,
}

selected_page = st.sidebar.selectbox("Select", page_names_to_funcs.keys())
page_names_to_funcs[selected_page]()