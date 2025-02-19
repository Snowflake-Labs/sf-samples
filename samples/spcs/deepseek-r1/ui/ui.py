from openai import OpenAI
import streamlit as st
import os

st.title(os.getenv("MODEL"))
st.subheader("Running in :snowflake: Snowpark Container Services")

client = OpenAI(base_url="http://localhost:8000/v1", api_key="dummy")

if "openai_model" not in st.session_state:
    st.session_state["openai_model"] = os.getenv("MODEL")

if "messages" not in st.session_state:
    st.session_state.messages = []

for message in st.session_state.messages:
    with st.chat_message(message["role"]):
        st.markdown(message["content"])

if prompt := st.chat_input("What is up?"):
    st.session_state.messages.append({"role": "user", "content": prompt})
    with st.chat_message("user"):
        st.markdown(prompt)

    with st.chat_message("assistant"):
        stream = client.chat.completions.create(
            model=st.session_state["openai_model"],
            messages=[
                {"role": m["role"], "content": m["content"]}
                for m in st.session_state.messages
            ],
            stream=True,
        )
        response = st.write_stream(stream)
    st.session_state.messages.append({"role": "assistant", "content": response})