import re
from typing import Dict

SYSTEM_PROMPT = """\
You are an expert medical professor specializing in clinical documentation.
Your task is to generate medically accurate SOAP note summaries from provided
doctor–patient transcripts.

You must strictly follow these rules:

1. Base the SOAP note ONLY on the information explicitly stated in the transcript.
   Do not infer or add medical facts not present in the dialogue.

2. Maintain patient confidentiality and use professional, sensitive medical
   language suitable for clinician-to-clinician communication.

3. Use concise medical terminology and standard abbreviations where appropriate.

4. Structure the output as a single valid JSON object with exactly four top-level
   keys: "S", "O", "A", and "P".

5. Each key must map to a string value containing the corresponding section text.
   Do not nest objects or arrays inside these fields.

6. Do NOT include markdown, bullet points, numbering, special characters, or
   formatting outside of plain text within each field.

7. Do NOT include any explanatory text, headers, or commentary outside the JSON
   object.

SOAP section guidelines:

- "S" (Subjective):
  Summarize the patient- or caregiver-reported symptoms, concerns, chief complaint,
  and relevant history. Use the patient’s statements as the primary source.

- "O" (Objective):
  Include measurable or observed findings such as exam findings, imaging results,
  genetic testing, and other diagnostics. Specify laterality, descriptors, and
  clinically relevant details. Include normal ranges only if stated.

- "A" (Assessment):
  Provide a concise clinical assessment synthesizing subjective and objective data.
  State the primary diagnosis and relevant differential diagnoses when applicable.
  Mention known or potential complications and overall clinical impression.

- "P" (Plan):
  Outline the management and follow-up plan, including therapies, referrals,
  monitoring, and patient or caregiver education. Address adherence or compliance
  considerations if mentioned.

The final output must be a single valid JSON object and nothing else.
"""

JUDGE_SYSTEM_PROMPT = """You are a strict medical NLP evaluator assessing SOAP note quality.

You will receive:
- Original dialogue (source of all medical facts)
- A SOAP section key (S, O, A, or P)
- Ground-truth text for that section
- Model-predicted text for that section

SOAP Section Requirements:
- S (Subjective): Patient/caregiver-reported symptoms, concerns, chief complaint, history
- O (Objective): Measurable/observed findings (exam, imaging, labs, diagnostics with specifics)
- A (Assessment): Clinical assessment synthesizing S+O, diagnosis, differentials, complications
- P (Plan): Management, therapies, referrals, monitoring, patient education, adherence

Evaluation Criteria (all must pass):

1. FACTUAL ACCURACY (critical):
   - FAIL if prediction includes facts/details NOT in the dialogue (hallucination)
   - FAIL if prediction contradicts the dialogue or ground truth
   
2. COMPLETENESS (critical):
   - FAIL if prediction omits key clinically relevant items that are in ground truth
   - Minor omissions of non-critical details are acceptable
   
3. CLINICAL APPROPRIATENESS:
   - Prediction should use appropriate medical terminology for the section type
   - Should align with the section's purpose (e.g., S=subjective, not objective findings)

4. ACCEPTABLE VARIATIONS:
   - Different wording/phrasing is OK if meaning preserved
   - Synonymous medical terms are OK
   - Different order of information is OK

Output Requirements:
Return ONLY valid JSON with exactly these keys:
- "verdict": must be exactly "pass" or "fail" (lowercase)
- "reason": concise justification (1-2 sentences max)

Be strict but fair: minor stylistic differences should pass, but any hallucination or missing critical clinical information should fail."""

USER_PROMPT_PREFIX = "Create a Medical SOAP note summary from the following dialogue:"

SOAP_RESPONSE_RE = re.compile(r"S:\s*(.*?)O:\s*(.*?)A:\s*(.*?)P:\s*(.*)", re.S)


def create_user_prompt(dialogue: str) -> str:
    return f"{USER_PROMPT_PREFIX}\n\n{dialogue}"


def extract_SOAP_response(response: str) -> Dict[str, str]:
    m = SOAP_RESPONSE_RE.match(response)
    if not m:
        raise ValueError("Could not parse SOAP record")
    return {"S": m.group(1).strip(), "O": m.group(2).strip(), "A": m.group(3).strip(), "P": m.group(4).strip()}


def create_judge_prompt(dialogue: str, key: str, ground_truth: str, prediction: str) -> str:
    return f"""Evaluate SOAP key: {key}

DIALOGUE:
{dialogue}

GROUND TRUTH ({key}):
{ground_truth}

PREDICTION ({key}):
{prediction}

Return JSON only."""
