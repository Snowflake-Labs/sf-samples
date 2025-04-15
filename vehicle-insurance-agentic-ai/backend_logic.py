# 📁 backend_logic.py (LangGraph – Enhanced Output for UI Display)
import os
import re
import json
import uuid
import base64
import boto3
from datetime import datetime
from typing import TypedDict, Annotated
import operator

from snowflake.snowpark import Session
from langchain_core.messages import HumanMessage
from langchain_community.chat_models import BedrockChat
from langgraph.graph import StateGraph, START, END

from dotenv import load_dotenv
load_dotenv()


# Constants
LOCAL_PATH = "/tmp"
STAGE_NAME = "@doc_ai_stage"

# Load Snowflake credentials securely from environment variables
conn = {
    "account": os.getenv("SNOWFLAKE_ACCOUNT"),
    "user": os.getenv("SNOWFLAKE_USER"),
    "password": os.getenv("SNOWFLAKE_PASSWORD"),
    "role": os.getenv("SNOWFLAKE_ROLE"),
    "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
    "database": os.getenv("SNOWFLAKE_DATABASE"),
    "schema": os.getenv("SNOWFLAKE_SCHEMA")
}
session = Session.builder.configs(conn).create()

# Model setup
claude = BedrockChat(model_id="us.amazon.nova-lite-v1:0")
mistral = BedrockChat(model_id="mistral.mistral-7b-instruct-v0:2")

# Shared types
class State(TypedDict):
    dl_path: str
    claim_path: str
    car_path: str
    car_local: str
    dl: dict
    claim: dict
    car: dict
    steps: Annotated[list, operator.add]
    files: dict
    comparison: dict
    decision: str
    email: str


def upload_to_stage(file, filename):
    local_path = os.path.join(LOCAL_PATH, filename)
    with open(local_path, "wb") as f:
        f.write(file.read())
    session.file.put(local_path, STAGE_NAME, overwrite=True, auto_compress=False)
    return filename, local_path


def upload_all(state: State):
    files = state.get("files")
    if not files:
        raise ValueError("Missing 'files' in workflow state.")
    dl_name, _ = upload_to_stage(files["dl"], f"{uuid.uuid4()}_{files['dl'].name}")
    claim_name, _ = upload_to_stage(files["claim"], f"{uuid.uuid4()}_{files['claim'].name}")
    car_name, car_path = upload_to_stage(files["car"], f"{uuid.uuid4()}_{files['car'].name}")
    return {
        "dl_path": dl_name,
        "claim_path": claim_name,
        "car_path": car_name,
        "car_local": car_path,
        "steps": ["Uploaded"]
    }


def extract_dl(state: State):
    path = state["dl_path"]
    sql = f"SELECT LICENSE_DATA!PREDICT(GET_PRESIGNED_URL(@doc_ai_stage, '{path}'), 1) AS result"
    result = json.loads(session.sql(sql).collect()[0]["RESULT"])
    print("\n🪪 Extracted DL Data:\n", json.dumps(result, indent=2))
    return {"dl": result, "steps": ["DL Extracted"]}


def extract_claim(state: State):
    path = state["claim_path"]
    sql = f"SELECT CLAIMS_DATA!PREDICT(GET_PRESIGNED_URL(@doc_ai_stage, '{path}'), 3) AS result"
    result = json.loads(session.sql(sql).collect()[0]["RESULT"])
    for field in ["description", "vehicle"]:
        text = result.get(field, [{}])[0].get("value", "")
        match = re.search(r"Color:\s*(.+?)(,|$)", text, re.IGNORECASE)
        if match:
            result["vehicle_color"] = [{"value": match.group(1).strip()}]
            break
    print("\n📄 Extracted Claim Document Data:\n", json.dumps(result, indent=2))
    return {"claim": result, "steps": ["Claim Extracted"]}


def extract_car(state: State):
    path = state["car_local"]
    ext = path.split(".")[-1]
    with open(path, "rb") as f:
        b64_img = base64.b64encode(f.read()).decode("utf-8")
    prompt = "Analyze the car image and return car type, make and model, visible damage, severity, and color in JSON."
    msg = [{
        "role": "user",
        "content": [
            {"type": "image", "source": {"type": "base64", "media_type": f"image/{ext}", "data": b64_img}},
            {"type": "text", "text": prompt}
        ]
    }]
    bedrock = boto3.client(service_name='bedrock-runtime')
    response = bedrock.invoke_model(
        body=json.dumps({"anthropic_version": "bedrock-2023-05-31", "max_tokens": 1000, "messages": msg}),
        modelId='anthropic.claude-3-sonnet-20240229-v1:0')
    car_result = json.loads(response["body"].read())
    car_data = json.loads(car_result["content"][0]["text"])
    print("\n🚘 Extracted Car Image Analysis:\n", json.dumps(car_data, indent=2))
    return {"car": car_data, "steps": ["Car Analyzed"]}


def compare_and_email(state: State):
    dl = state["dl"]
    claim = state["claim"]
    car = state["car"]

    customer_id = claim.get("customer_id", [{}])[0].get("value")
    vin_from_db = ""
    policy_end = None
    if customer_id:
        df = session.sql(f"SELECT VEHICLE_NUMBER, POLICY_END FROM customer_policy_view WHERE customer_id = '{customer_id}'").to_pandas()
        if not df.empty:
            vin_from_db = df.iloc[0]["VEHICLE_NUMBER"]
            policy_end = df.iloc[0]["POLICY_END"]

    prompt = f"""
You are a document intelligence agent.

Compare the following fields from three inputs: Driver's License (DL), Claim Document, and Car Image.

Compare these:
- full_name: DL vs Claim
- dl_number: DL vs Claim
- vehicle_color: Car image vs Claim
- vin: VIN from Claim vs VIN from policy database (Snowflake)

Return strictly the following JSON format (and nothing else):

{{
  "full_name": {{ "match": true/false, "dl_value": "...", "claim_value": "..." }},
  "dl_number": {{ "match": true/false, "dl_value": "...", "claim_value": "..." }},
  "vehicle_color": {{ "match": true/false, "image_value": "...", "claim_value": "..." }},
  "vin": {{ "match": true/false, "extracted_from_claim": "...", "customer_record_value": "{vin_from_db}" }}
}}

DL:
{json.dumps(dl)}

CLAIM:
{json.dumps(claim)}

CAR:
{json.dumps(car)}
"""

    bedrock = boto3.client(service_name='bedrock-runtime')
    message = [{"role": "user", "content": [{"type": "text", "text": prompt}]}]
    response = bedrock.invoke_model(
        body=json.dumps({"anthropic_version": "bedrock-2023-05-31", "max_tokens": 2000, "messages": message}),
        modelId='anthropic.claude-3-sonnet-20240229-v1:0')

    try:
        response_body = json.loads(response["body"].read())
        content_blocks = response_body.get("content", [])
        claude_text = ""
        for block in content_blocks:
            if block.get("type") == "text":
                claude_text = block.get("text", "")
                break

        match = re.search(r"\{.*\}", claude_text, re.DOTALL)
        if not match:
            raise ValueError("No valid JSON block found in Claude's response content.")
        comparison = json.loads(match.group(0))

    except Exception as e:
        print("⚠️ Failed to extract comparison JSON from Claude response:", e)
        comparison = {}

    print("\n✅ Parsed Comparison Data:\n", json.dumps(comparison, indent=2))
    print("\n📄 Full Claim Data:\n", json.dumps(claim, indent=2))
    print("\n🪪 Full DL Data:\n", json.dumps(dl, indent=2))
    print("\n🚘 Full Car Image Analysis:\n", json.dumps(car, indent=2))

    incident_date = claim.get("incident_date", [{}])[0].get("value")
    incident_date = datetime.strptime(incident_date, "%Y-%m-%d").date() if incident_date else None
    policy_end_date = policy_end.date() if isinstance(policy_end, datetime) else policy_end
    valid = incident_date and policy_end_date and incident_date <= policy_end_date
    decision = "✅ Claim Accepted" if valid else "❌ Claim Rejected due to expired policy"

    mistral_prompt = f"""
Write a short professional email to a customer summarizing their insurance claim review.

Comparison:
{json.dumps(comparison, indent=2)}

Incident Date: {incident_date}
Policy End: {policy_end_date}
Decision: {decision}
"""
    email = mistral.invoke([HumanMessage(content=mistral_prompt)]).content

    print("\n📧 Email Draft:\n", email)

    return {
        "comparison": comparison,
        "decision": decision,
        "email": email,
        "steps": ["Merged"]
    }


def run_claim_processing_workflow(files):
    class WorkflowState(State):
        files: dict
    builder = StateGraph(WorkflowState)
    builder.add_node("upload", upload_all)
    builder.add_node("extract_dl", extract_dl)
    builder.add_node("extract_claim", extract_claim)
    builder.add_node("extract_car", extract_car)
    builder.add_node("compare_email", compare_and_email)
    builder.set_entry_point("upload")
    builder.add_edge("upload", "extract_dl")
    builder.add_edge("extract_dl", "extract_claim")
    builder.add_edge("extract_claim", "extract_car")
    builder.add_edge("extract_car", "compare_email")
    builder.add_edge("compare_email", END)
    graph = builder.compile()
    return graph.invoke({"files": files, "steps": []})
