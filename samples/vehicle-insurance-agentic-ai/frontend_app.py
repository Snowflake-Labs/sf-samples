import streamlit as st
from backend_logic import run_claim_processing_workflow

st.set_page_config(page_title="🧾 Insurance Claim Validator", layout="centered")
st.title("🤖 Unified Insurance Claim Processor")

# Upload files
st.header("📤 Upload Claim Documents")
accepted_types = ["jpg", "jpeg", "png", "pdf"]
dl_file = st.file_uploader("🪪 Driver's License", type=accepted_types)
claim_file = st.file_uploader("📄 Claim Document", type=accepted_types)
car_file = st.file_uploader("🚘 Car Damage Photo", type=accepted_types)

# Submit and trigger workflow
if st.button("🚀 Run Claim Workflow"):
    if not dl_file or not claim_file or not car_file:
        st.warning("Please upload all 3 required documents.")
    else:
        with st.spinner("Processing with Agentic Workflow..."):
            try:
                result = run_claim_processing_workflow({
                    "dl": dl_file,
                    "claim": claim_file,
                    "car": car_file
                })

                st.success("✅ Workflow completed successfully!")

                st.markdown("### 🧭 Workflow Steps")
                for step in result.get("steps", []):
                    st.markdown(f"✅ {step}")

                st.markdown("### 🔍 Document Comparison")
                comparison = result.get("comparison", {})

                if isinstance(comparison, dict) and comparison:
                    for field, data in comparison.items():
                        st.markdown(f"**{field.replace('_', ' ').title()}**")
                        if isinstance(data, dict):
                            match_icon = "✅" if data.get("match") else "❌"
                            st.markdown(f"{match_icon} Match: `{data.get('match')}`")
                            for k, v in data.items():
                                if k != "match":
                                    st.markdown(f"• `{k}`: `{v}`")
                        else:
                            st.warning(f"⚠️ Unexpected data format for field `{field}`: {data}")
                else:
                    st.warning("No comparison data returned.")

                st.markdown("### 📅 Policy Validity Check")
                st.markdown(result.get("decision", "Not available."))

                st.markdown("### 📧 Generated Email to Customer")
                st.code(result.get("email", "No email content."))

                # Show Raw Extracted Data
                with st.expander("📁 Full Extracted Document Data", expanded=False):
                    st.subheader("🪪 Driver's License Data")
                    st.json(result.get("dl", {}))

                    st.subheader("📄 Claim Document Data")
                    st.json(result.get("claim", {}))

                    st.subheader("🚘 Car Image Analysis")
                    st.json(result.get("car", {}))

            except Exception as e:
                st.error("❌ Unexpected error occurred during workflow.")
                st.exception(e)
