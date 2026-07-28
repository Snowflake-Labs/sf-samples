import os
import json
import streamlit as st

conn = st.connection("snowflake", ttl=os.getenv("SNOWFLAKE_CONNECTION_TTL"))
session = conn.session()

st.set_page_config(page_title="Call Transcript Analyzer", page_icon="📞", layout="wide")
st.title("📞 Call Transcript Analyzer")
st.markdown("Summarize call transcripts into structured JSON using Snowflake Cortex AI models.")

MODELS = ["llama3.1-70b", "mistral-large2", "llama3.1-8b"]

selected_model = st.sidebar.selectbox("Select Model", MODELS)

st.sidebar.markdown("---")
st.sidebar.markdown("**About**")
st.sidebar.markdown(
    "This app uses Snowflake Cortex `complete()` to summarize "
    "call transcripts and extract product, defect, and summary in JSON format."
)

SAMPLE_TRANSCRIPT = """Customer: Hello

Agent: Hi there, I hope you're having a great day! To better assist you, could you please provide your first and last name and the company you are calling from?

Customer: Sure, my name is Jessica Turner and I'm calling from Mountain Ski Adventures.

Agent: Thanks, Jessica. What can I help you with today?

Customer: Well, we recently ordered a batch of XtremeX helmets, and upon inspection, we noticed that the buckles on several helmets are broken and won't secure the helmet properly.

Agent: I apologize for the inconvenience this has caused you. To confirm, is your order number 68910?

Customer: Yes, that's correct.

Agent: Thank you for confirming. I'm going to look into this issue and see what we can do to correct it. Would you prefer a refund or a replacement for the damaged helmets?

Customer: A replacement would be ideal, as we still need the helmets for our customers.

Agent: I understand. I will start the process to send out replacements for the damaged helmets as soon as possible. Can you please specify the quantity of helmets with broken buckles?

Customer: There are ten helmets with broken buckles in total.

Agent: Thank you for providing me with the quantity. We will expedite a new shipment of ten XtremeX helmets with functioning buckles to your location. You should expect them to arrive within 3-5 business days.

Customer: Thank you for your assistance, I appreciate it."""

entered_text = st.text_area(
    "Enter a call transcript",
    height=300,
    placeholder="Paste a call transcript here, or use the sample below.",
)

if st.button("Use Sample Transcript"):
    st.session_state["transcript"] = SAMPLE_TRANSCRIPT
    st.rerun()

if "transcript" in st.session_state and not entered_text:
    entered_text = st.session_state["transcript"]
    st.text_area("Transcript (sample loaded)", value=entered_text, height=300, disabled=True)

if st.button("Summarize", type="primary", disabled=not entered_text):
    with st.spinner(f"Summarizing with {selected_model}..."):
        prompt = (
            "Summarize this transcript in less than 200 words. "
            "Put the product name, defect if any, and summary in JSON format: "
            + entered_text
        )
        result = session.sql(
            "SELECT SNOWFLAKE.CORTEX.COMPLETE(?, ?) AS response",
            params=[selected_model, prompt],
        ).collect()[0]["RESPONSE"]

        st.subheader("Result")
        try:
            parsed = json.loads(result)
            st.json(parsed)
        except json.JSONDecodeError:
            st.markdown(result)

st.markdown("---")
st.subheader("Compare All Models")

if st.button("Run All Models", disabled=not entered_text):
    prompt = (
        "Summarize this transcript in less than 200 words. "
        "Put the product name, defect if any, and summary in JSON format: "
        + entered_text
    )
    cols = st.columns(len(MODELS))
    for i, model in enumerate(MODELS):
        with cols[i]:
            st.markdown(f"**{model}**")
            with st.spinner("Running..."):
                result = session.sql(
                    "SELECT SNOWFLAKE.CORTEX.COMPLETE(?, ?) AS response",
                    params=[model, prompt],
                ).collect()[0]["RESPONSE"]
                try:
                    parsed = json.loads(result)
                    st.json(parsed)
                except json.JSONDecodeError:
                    st.markdown(result)
