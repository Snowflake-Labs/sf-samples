

---

```markdown
# Agentic Insurance Claim Processor

This is an end-to-end insurance claim processing application built using agentic AI principles. It processes a driver’s license, a claim form, and a car damage photo through a structured workflow. Each step is handled by an intelligent component: document extraction using Snowflake Cortex Document AI, image and text analysis using Claude (via Amazon Bedrock), policy validation using Snowflake queries, and decision email generation using Mistral. The workflow is managed using LangGraph, simulating how a human agent would evaluate and decide on claims.

---

## Features

- Extracts data from driver's license, claim form, and car image
- Uses Snowflake Cortex for structured document field extraction
- Claude performs car image analysis and cross-document comparison
- Snowflake is queried for policy details using customer ID
- Mistral generates professional customer-facing decision emails
- LangGraph manages workflow step-by-step with full state tracking

---

## Tech Stack

| Component       | Tool / Service                          |
|----------------|------------------------------------------|
| Workflow Engine | LangGraph (LangChain)                   |
| Document Parsing| Snowflake Cortex Document AI            |
| Image + Text AI | Claude 3 Sonnet (via Bedrock)           |
| Email Generator | Mistral 7B Instruct (via Bedrock)       |
| Frontend UI     | Streamlit                               |
| Database        | Snowflake                               |
| Language        | Python 3.10+                             |

---


## Getting Started

### 1. Clone the Repository

```bash
git clone https://github.com/curious-bigcat/insurance_agentic_ai.git
cd insurance_agentic_ai
```

### 2. Set Up a Virtual Environment

```bash
python3 -m venv venv
source venv/bin/activate
```

### 3. Install Dependencies

```bash
pip install -r requirements.txt
```

### 4. Set Environment Variables

Create a `.env` file in the root directory:

```
SNOWFLAKE_ACCOUNT=your_account
SNOWFLAKE_USER=your_user
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_ROLE=your_role
SNOWFLAKE_WAREHOUSE=your_warehouse
SNOWFLAKE_DATABASE=your_database
SNOWFLAKE_SCHEMA=your_schema
```

Or export them directly in terminal:

```bash
export SNOWFLAKE_ACCOUNT=...
```

### 5. Run the Application

```bash
streamlit run frontend_app.py
```

---

## Workflow Overview

1. Upload documents
2. Extract driver's license data
3. Extract claim form data
4. Analyze car image
5. Compare fields across all documents
6. Check if policy is valid for the incident date
7. Generate final decision email

---

## Sample Output

### Comparison Result

```json
{
  "full_name": { "match": true, "dl_value": "Steven Smith", "claim_value": "Steven Smith" },
  "dl_number": { "match": true, "dl_value": "DL-2605979475", "claim_value": "DL-2605979475" },
  "vehicle_color": { "match": true, "image_value": "Dark Purple", "claim_value": "Dark Purple" },
  "vin": { "match": true, "extracted_from_claim": "KA81JS7515", "customer_record_value": "KA81JS7515" }
}
```

### Email Example

> Dear Valued Customer,  
> We have reviewed your insurance claim filed on February 14, 2025, under a policy ending June 18, 2025.  
> Based on the submitted documents and assessment, your claim has been accepted.  
> Thank you for choosing our services.

---

## Planned Enhancements

- Add PDF summary export
- Audit log for claims
- Docker/Streamlit Cloud deployment support
- Agent-specific logs and scoring

---

## Maintainer

Created and maintained by [@curious-bigcat](https://github.com/curious-bigcat)

Please open an issue or pull request if you’d like to contribute or have any questions.
```
