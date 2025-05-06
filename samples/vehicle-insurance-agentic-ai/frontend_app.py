import streamlit as st
from backend_logic import run_claim_processing_workflow

st.set_page_config(page_title="Insurance Claim Validator", layout="centered")
st.title("Unified Insurance Claim Processor")

st.header("Upload Claim Documents")
accepted_types = ["jpg", "jpeg", "png", "pdf"]

dl_file = st.file_uploader("Driver's License", type=accepted_types)
claim_file = st.file_uploader("Insurance Claim Document", type=accepted_types)
car_file = st.file_uploader("Car Damage Photo", type=accepted_types)

if st.button("Run Claim Workflow"):
    if not dl_file or not claim_file or not car_file:
        st.warning("Please upload all required documents.")
    else:
        with st.spinner("Running claim processing workflow..."):
            try:
                result = run_claim_processing_workflow({
                    "dl": dl_file,
                    "claim": claim_file,
                    "car": car_file
                })

                st.success("Workflow completed successfully.")

                st.subheader("Workflow Steps")
                for step in result.get("steps", []):
                    st.markdown(f"- {step}")

                st.subheader("Document Comparison")
                for field, data in result.get("comparison", {}).items():
                    st.markdown(f"**{field.replace('_', ' ').title()}**")
                    if isinstance(data, dict):
                        st.markdown(f"- Match: `{data.get('match')}`")
                        for key, value in data.items():
                            if key != "match":
                                st.markdown(f"  - {key}: `{value}`")
                    else:
                        st.warning(f"Unexpected format for field {field}: {data}")

                st.subheader("Policy Validation Decision")
                st.markdown(result.get("decision", "Decision not available."))

                st.subheader("Generated Email Summary")
                st.code(result.get("email", "No email content available."))

                with st.expander("Extracted Data Details"):
                    st.markdown("**Driver's License Data**")
                    st.json(result.get("dl", {}))
                    st.markdown("**Claim Document Data**")
                    st.json(result.get("claim", {}))
                    st.markdown("**Car Image Analysis**")
                    st.json(result.get("car", {}))

            except Exception as e:
                st.error("An error occurred during processing.")
                st.exception(e)
