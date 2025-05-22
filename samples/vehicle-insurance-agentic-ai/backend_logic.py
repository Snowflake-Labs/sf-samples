import os
import re
import json
import uuid
import base64
import boto3
import mimetypes
from datetime import datetime
from typing import TypedDict, Annotated
import operator

from dotenv import load_dotenv
from snowflake.snowpark import Session
from langgraph.graph import StateGraph, START, END

load_dotenv()

# Constants
LOCAL_PATH = "/tmp"
STAGE_NAME = "@doc_ai_stage"

# Load Snowflake connection configuration
conn = {
    "account": os.getenv("SNOWFLAKE_ACCOUNT"),
    "user": os.getenv("SNOWFLAKE_USER"),
    "password": os.getenv("SNOWFLAKE_PASSWORD"),
    "role": os.getenv("SNOWFLAKE_ROLE"),
    "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
    "database": os.getenv("SNOWFLAKE_DATABASE"),
    "schema": os.getenv("SNOWFLAKE_SCHEMA"),
}
session = Session.builder.configs(conn).create()

# Initialize Bedrock runtime client
bedrock = boto3.client("bedrock-runtime", region_name="us-east-1")


# Typed state used by the workflow
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


def upload_to_stage(file, filename: str) -> tuple[str, str]:
    """
    Uploads a local file to the Snowflake internal stage.
    """
    local_path = os.path.join(LOCAL_PATH, filename)
    with open(local_path, "wb") as f:
        f.write(file.read())
    session.file.put(local_path, STAGE_NAME, overwrite=True, auto_compress=False)
    return filename, local_path


def upload_all(state: State) -> dict:
    """
    Uploads all required documents (DL, claim, car image) to Snowflake stage.
    """
    files = state.get("files")
    dl_name, _ = upload_to_stage(files["dl"], f"{uuid.uuid4()}_{files['dl'].name}")
    claim_name, _ = upload_to_stage(files["claim"], f"{uuid.uuid4()}_{files['claim'].name}")
    car_name, car_path = upload_to_stage(files["car"], f"{uuid.uuid4()}_{files['car'].name}")
    return {"dl_path": dl_name, "claim_path": claim_name, "car_path": car_name, "car_local": car_path, "steps": ["Files Uploaded"]}


def extract_dl(state: State) -> dict:
    """
    Extracts structured data from the driver's license using Document AI.
    """
    sql = f"SELECT LICENSE_DATA!PREDICT(GET_PRESIGNED_URL(@doc_ai_stage, '{state['dl_path']}'), 1) AS result"
    result = json.loads(session.sql(sql).collect()[0]["RESULT"])
    return {"dl": result, "steps": ["Driver's License Extracted"]}


def extract_claim(state: State) -> dict:
    """
    Extracts structured data from the claim form using Document AI and parses additional fields.
    """
    sql = f"SELECT CLAIMS_DATA!PREDICT(GET_PRESIGNED_URL(@doc_ai_stage, '{state['claim_path']}'), 3) AS result"
    result = json.loads(session.sql(sql).collect()[0]["RESULT"])

    # Extract vehicle color from description if available
    text = result.get("description", [{}])[0].get("value", "")
    match = re.search(r"Color:\s*(.+?)(,|$)", text, re.IGNORECASE)
    if match:
        result["vehicle_color"] = [{"value": match.group(1).strip()}]

    return {"claim": result, "steps": ["Claim Document Extracted"]}


def extract_car(state: State) -> dict:
    """
    Sends the car image to Bedrock's image reasoning model for analysis.
    """
    path = state["car_local"]
    ext = path.split(".")[-1]
    mime_type, _ = mimetypes.guess_type(path)
    mime_type = mime_type or "image/jpeg"

    with open(path, "rb") as f:
        b64_img = base64.b64encode(f.read()).decode("utf-8")

    payload = {
        "schemaVersion": "messages-v1",
        "system": [{"text": "You are a vehicle damage assessment expert. Analyze the image and return structured information."}],
        "messages": [{
            "role": "user",
            "content": [
                {"image": {"format": ext, "source": {"bytes": b64_img}}},
                {"text": "Return only this JSON structure: {...}"}
            ]
        }],
        "inferenceConfig": {"maxTokens": 800, "topP": 0.9, "temperature": 0.5}
    }

    try:
        response = bedrock.invoke_model(
            modelId="us.amazon.nova-lite-v1:0",
            body=json.dumps(payload),
            contentType="application/json",
            accept="application/json"
        )
        response_body = json.loads(response["body"].read())
        text_output = response_body.get("output", {}).get("message", {}).get("content", [{}])[0].get("text", "")
        match = re.search(r"\{\s*\"car_type\".*?\}", text_output, re.DOTALL)
        car_data = json.loads(match.group(0)) if match else {}
    except Exception as e:
        print("Car analysis failed:", e)
        car_data = {
            "car_type": "Unknown", "make_and_model": "Unknown", "visible_damage": "Unknown",
            "color": "Unknown", "severity": "Unknown",
            "reason": f"Failed to parse image output: {str(e)}"
        }

    return {"car": car_data, "steps": ["Car Image Analyzed"]}


def compare_and_email(state: State) -> dict:
    """
    Compares the documents and generates a customer-facing email based on damage and policy validation.
    """
    dl = state["dl"]
    claim = state["claim"]
    car = state["car"]

    # Retrieve customer data from policy view
    customer_id = claim.get("customer_id", [{}])[0].get("value")
    vin_from_db = ""
    policy_end = None
    if customer_id:
        df = session.sql(f"SELECT VEHICLE_NUMBER, POLICY_END FROM customer_policy_view WHERE customer_id = '{customer_id}'").to_pandas()
        if not df.empty:
            vin_from_db = df.iloc[0]["VEHICLE_NUMBER"]
            policy_end = df.iloc[0]["POLICY_END"]

    # Construct comparison prompt
    comparison_prompt = {
        "role": "user",
        "content": [{
            "text": f"""Compare DL, Claim, and Car analysis for:
{json.dumps({'dl': dl, 'claim': claim, 'car': car}, indent=2)}
Customer record VIN: {vin_from_db}
Return strict JSON comparison of fields: full_name, dl_number, vehicle_color, vin."""
        }]
    }

    request_body = {
        "schemaVersion": "messages-v1",
        "system": [{"text": "You are a document comparison assistant."}],
        "messages": [comparison_prompt],
        "inferenceConfig": {"maxTokens": 1000, "topP": 0.9, "temperature": 0.3}
    }

    try:
        response = bedrock.invoke_model(
            modelId="us.amazon.nova-lite-v1:0",
            body=json.dumps(request_body),
            contentType="application/json",
            accept="application/json"
        )
        output_text = json.loads(response["body"].read())["output"]["message"]["content"][0]["text"]
        match = re.search(r"\{.*\}", output_text, re.DOTALL)
        comparison = json.loads(match.group(0)) if match else {}
    except Exception as e:
        print("Comparison failed:", e)
        comparison = {}

    # Claim policy validity
    incident_date = claim.get("incident_date", [{}])[0].get("value")
    incident_date = datetime.strptime(incident_date, "%Y-%m-%d").date() if incident_date else None
    policy_end_date = policy_end.date() if isinstance(policy_end, datetime) else policy_end
    decision = "Claim Accepted" if incident_date and policy_end_date and incident_date <= policy_end_date else "Claim Rejected due to expired policy"

    # Generate summary email
    email_prompt = {
        "role": "user",
        "content": [{
            "text": f"""Summarize insurance claim review:
Comparison: {json.dumps(comparison, indent=2)}
Incident Date: {incident_date}
Policy End: {policy_end_date}
Decision: {decision}
Repair Recommendation: {json.dumps(car.get("repair_recommendation", {}), indent=2)}"""
        }]
    }

    try:
        request_body["messages"] = [email_prompt]
        email_response = bedrock.invoke_model(
            modelId="us.amazon.nova-lite-v1:0",
            body=json.dumps(request_body),
            contentType="application/json",
            accept="application/json"
        )
        email = json.loads(email_response["body"].read())["output"]["message"]["content"][0]["text"]
    except Exception as e:
        print("Email generation failed:", e)
        email = "[Email generation failed]"

    return {
        "comparison": comparison,
        "decision": decision,
        "email": email,
        "steps": ["Documents Compared"],
        "dl": dl,
        "claim": claim,
        "car": car
    }


def run_claim_processing_workflow(files: dict) -> dict:
    """
    Executes the full workflow using LangGraph.
    """
    class WorkflowState(State):
        files: dict

    builder = StateGraph(WorkflowState)
    builder.set_entry_point("upload")
    builder.add_node("upload", upload_all)
    builder.add_node("extract_dl", extract_dl)
    builder.add_node("extract_claim", extract_claim)
    builder.add_node("extract_car", extract_car)
    builder.add_node("compare_email", compare_and_email)
    builder.add_edge("upload", "extract_dl")
    builder.add_edge("extract_dl", "extract_claim")
    builder.add_edge("extract_claim", "extract_car")
    builder.add_edge("extract_car", "compare_email")
    builder.add_edge("compare_email", END)

    graph = builder.compile()
    return graph.invoke({"files": files, "steps": []})
