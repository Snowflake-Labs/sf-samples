# Agentic Insurance Claim Processor

This is an end-to-end insurance claim processing application built using agentic AI principles. It processes a driver’s license, a claim form, and a car damage photo through a structured workflow. Each step is handled by an intelligent component: document extraction using Snowflake Cortex Document AI, image and text analysis using Claude (via Amazon Bedrock), policy validation using Snowflake queries, and decision email generation using Mistral. The workflow is managed using LangGraph, simulating how a human agent would evaluate and decide on claims.

---

## Features

* **Extracts data** from driver's license, claim form, and car image.
* **Uses Snowflake Cortex** for structured document field extraction.
* **Claude performs image and text analysis** and cross-document comparison.
* **Snowflake queries** customer records for policy details using customer ID.
* **Mistral generates professional customer-facing decision emails**.
* **LangGraph manages the workflow** step-by-step with full state tracking.

---

## Tech Stack

| Component            | Tool / Service                    |
| -------------------- | --------------------------------- |
| **Workflow Engine**  | LangGraph (LangChain)             |
| **Document Parsing** | Snowflake Cortex Document AI      |
| **Image + Text AI**  | Claude 3 Sonnet (via Bedrock)     |
| **Email Generator**  | Mistral 7B Instruct (via Bedrock) |
| **Frontend UI**      | Streamlit                         |
| **Database**         | Snowflake                         |
| **Language**         | Python 3.10+                      |

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

Alternatively, export them directly in terminal:

```bash
export SNOWFLAKE_ACCOUNT=...
```

### 5. Run the Application

```bash
streamlit run frontend_app.py
```

---

## Workflow Overview

1. **Upload Documents**:

   * Upload driver's license, claim form, and car image.

2. **Extract Driver's License Data**:

   * Use Snowflake Cortex Document AI for field extraction.

3. **Extract Claim Form Data**:

   * Process the claim form for relevant details.

4. **Analyze Car Image**:

   * Claude performs image analysis to assess car damage.

5. **Cross-Document Comparison**:

   * Compare extracted fields from documents (e.g., full name, vehicle color, VIN) for consistency.

6. **Check Policy Validity**:

   * Validate the policy using the customer ID and the incident date.

7. **Generate Final Decision Email**:

   * Mistral generates a personalized email for the customer, detailing the claim decision.

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

```text
Dear Valued Customer,

We have reviewed your insurance claim filed on February 14, 2025, under a policy ending June 18, 2025.

Based on the submitted documents and assessment, your claim has been accepted.

Thank you for choosing our services.
```

---

## Planned Enhancements

* **Add PDF summary export** for easy claim documentation.
* **Audit log** for claims to improve traceability.
* **Docker/Streamlit Cloud deployment** support for seamless cloud integration.
* **Agent-specific logs and scoring** for performance analysis.

---