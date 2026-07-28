
from snowflake.snowpark.context import get_active_session
import streamlit as st
import ast
session = get_active_session()

prompt = """You are a customer support representative at a telecommunications company. 
Suddenly there is a spike in customer support tickets. 
You need to understand and analyze the support requests from customers.
Based on the root cause of the main issue in the support request, craft a response to resolve the customer issue.
Write a text message under 25 words, if the contact_preference field is text message.
Write an email in maximum of 100 words if the contact_preference field is email. 
Focus on alleviating the customer issue and improving customer satisfaction in your response.
Strictly follow the word count limit for the response. 
Write only email or text message response based on the contact_preference for every customer. 
Do not generate both email and text message response.
"""

ticket_categories = ['Roaming fees', 'Slow data speed', 'Lost phone', 'Add new line', 'Closing account']

st.subheader("Auto-generate custom emails or text messages")

with st.container():
    with st.expander("Enter customer request and select LLM", expanded=True):
        customer_request = st.text_area('Request',"""I traveled to Japan for two weeks and kept my data usage to a minimum. However, I was charged $90 in international fees. These charges were not communicated to me, and I request a detailed breakdown and a refund. Thank you for your prompt assistance.""")
    
        with st.container():
            left_col, right_col = st.columns(2)
            with left_col:
                selected_preference = st.selectbox('Select contact preference', ('Text message', 'Email'))
            with right_col:
                selected_llm = st.selectbox('Select LLM',('llama3-8b', 'mistral-7b', 'mistral-large', 'SUPPORT_MESSAGES_FINETUNED_MISTRAL_7B',))

with st.container():
    _,mid_col,_ = st.columns([.4,.3,.3])
    with mid_col:
        generate_template = st.button('Generate messages ⚡',type="primary")

with st.container():
    if generate_template:
        category_sql = f"""
        select snowflake.cortex.classify_text('{customer_request}', {ticket_categories}) as ticket_category
        """
        df_category = session.sql(category_sql).to_pandas().iloc[0]['TICKET_CATEGORY']
        df_category_dict = ast.literal_eval(df_category)
        st.subheader("Ticket category")
        st.write(df_category_dict['label'])

        message_sql = f"""
        select snowflake.cortex.complete('{selected_llm}',concat('{prompt}', '{customer_request}', '{selected_preference}')) as custom_message
        """
        df_message = session.sql(message_sql).to_pandas().iloc[0]['CUSTOM_MESSAGE']
        st.subheader(selected_preference)
        st.write(df_message)