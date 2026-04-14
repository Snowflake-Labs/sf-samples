"""Prompt utilities for Medical SOAP note generation.

This module contains system prompts, user prompts, and helper functions
for generating and evaluating SOAP notes from doctor-patient dialogues.
"""

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

8. Do NOT wrap the output in markdown code fences (e.g., ```json or ```). Output
   the raw JSON object directly.

SOAP section guidelines:

- "S" (Subjective):
  Summarize the patient- or caregiver-reported symptoms, concerns, chief complaint,
  and relevant history. Use the patient's statements as the primary source.

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


# Separate criterion-specific judge system prompts
JUDGE_FACTUAL_ACCURACY_SYSTEM_PROMPT = """You are a strict medical NLP evaluator assessing FACTUAL ACCURACY of SOAP notes.

You will receive:
- Original dialogue (source of all medical facts)
- Model-predicted SOAP note (JSON with S, O, A, P keys)

Your ONLY task is to evaluate FACTUAL ACCURACY:
- FAIL if prediction includes ANY facts/details NOT explicitly stated in the dialogue (hallucination)
- FAIL if prediction contradicts information in the dialogue
- PASS if all information in the prediction can be traced back to the dialogue

ACCEPTABLE:
- Different wording/phrasing if meaning is preserved
- Synonymous medical terms
- Reasonable clinical inferences that are standard practice

NOT ACCEPTABLE:
- Adding diagnoses not mentioned or implied in the dialogue
- Including test results or findings not stated
- Inventing patient history or symptoms

After your analysis, output a JSON object with exactly these keys:
- "verdict": must be exactly "pass" or "fail" (lowercase)
- "reason": concise justification (1-2 sentences max)"""


JUDGE_COMPLETENESS_SYSTEM_PROMPT = """You are a strict medical NLP evaluator assessing COMPLETENESS of SOAP notes.

You will receive:
- Original dialogue (source of all medical facts)
- Ground-truth SOAP note (reference standard)
- Model-predicted SOAP note (JSON with S, O, A, P keys)

Your ONLY task is to evaluate COMPLETENESS:
- FAIL if prediction omits KEY clinically relevant information present in the ground truth
- Key information includes: chief complaint, significant symptoms, critical findings, primary diagnosis, essential treatment plans
- Minor omissions of non-critical details are acceptable

ACCEPTABLE OMISSIONS:
- Stylistic details
- Redundant information
- Minor supporting details

NOT ACCEPTABLE OMISSIONS:
- Chief complaint or primary symptoms
- Key diagnostic findings
- Primary diagnosis
- Critical treatment decisions

After your analysis, output a JSON object with exactly these keys:
- "verdict": must be exactly "pass" or "fail" (lowercase)
- "reason": concise justification (1-2 sentences max)"""


JUDGE_CLINICAL_APPROPRIATENESS_SYSTEM_PROMPT = """You are a strict medical NLP evaluator assessing CLINICAL APPROPRIATENESS of SOAP notes.

You will receive:
- Model-predicted SOAP note (JSON with S, O, A, P keys)

Your ONLY task is to evaluate CLINICAL APPROPRIATENESS:
- Check if appropriate medical terminology is used
- Verify each section contains the right type of information:
  * S (Subjective): Patient-reported symptoms, concerns, history - NOT objective findings
  * O (Objective): Measurable/observed findings, test results - NOT patient complaints
  * A (Assessment): Clinical synthesis, diagnosis - NOT raw data
  * P (Plan): Treatment, follow-up, referrals - NOT assessment
- FAIL if sections contain misplaced information types
- FAIL if unprofessional or inappropriate language is used

ACCEPTABLE:
- Various levels of detail
- Different organizational styles
- Both formal and semi-formal medical language

NOT ACCEPTABLE:
- Objective findings in Subjective section
- Subjective complaints in Objective section
- Treatment plans in Assessment section
- Diagnoses only in Plan section

After your analysis, output a JSON object with exactly these keys:
- "verdict": must be exactly "pass" or "fail" (lowercase)
- "reason": concise justification (1-2 sentences max)"""


# Section-based judge system prompt for RL training (judges S, O, A, P separately)
# This is a streamlined version of JUDGE_SYSTEM_PROMPT optimized for RL reward computation
JUDGE_SECTION_SYSTEM_PROMPT = """You are a strict medical NLP evaluator assessing a single SOAP section.

You will receive:
- Original dialogue (source of all medical facts)
- A SOAP section key (S, O, A, or P)
- Ground-truth text for that section
- Model-predicted text for that section

CRITICAL PRE-CHECK (evaluate FIRST, before anything else):
- FAIL IMMEDIATELY if prediction is empty, blank, whitespace-only, or missing
- FAIL IMMEDIATELY if prediction is less than 10 characters
- An empty or near-empty prediction can NEVER pass, regardless of other criteria

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

Output ONLY valid JSON with exactly these keys:
- "verdict": must be exactly "pass" or "fail" (lowercase)
- "reason": concise justification (1-2 sentences max)

Be strict but fair: minor stylistic differences should pass, but any hallucination or missing critical clinical information should fail."""

USER_PROMPT_PREFIX = "Create a Medical SOAP note summary from the following dialogue:"

SOAP_RESPONSE_RE = re.compile(r"S:\s*(.*?)O:\s*(.*?)A:\s*(.*?)P:\s*(.*)", re.S)


def create_user_prompt(dialogue: str) -> str:
    """Create user prompt from dialogue."""
    return f"{USER_PROMPT_PREFIX}\n\n{dialogue}"


def extract_SOAP_response(response: str) -> Dict[str, str]:
    """Extract SOAP sections from a text response using regex pattern."""
    m = SOAP_RESPONSE_RE.match(response)
    if not m:
        raise ValueError("Could not parse SOAP record")
    return {
        "S": m.group(1).strip(),
        "O": m.group(2).strip(),
        "A": m.group(3).strip(),
        "P": m.group(4).strip(),
    }


def create_judge_prompt(
    dialogue: str, key: str, ground_truth: str, prediction: str
) -> str:
    """Create a prompt for LLM-as-judge evaluation of a SOAP section."""
    return f"""Evaluate SOAP key: {key}

DIALOGUE:
{dialogue}

GROUND TRUTH ({key}):
{ground_truth}

PREDICTION ({key}):
{prediction}

Return JSON only."""


def create_factual_accuracy_prompt(dialogue: str, prediction_json: str) -> str:
    """Create a prompt for evaluating factual accuracy of a SOAP note."""
    return f"""Evaluate the FACTUAL ACCURACY of the following SOAP note.

ORIGINAL DIALOGUE:
{dialogue}

MODEL PREDICTION:
{prediction_json}

Check if ALL information in the prediction can be traced back to the dialogue.
Return JSON only."""


def create_completeness_prompt(
    dialogue: str, ground_truth_json: str, prediction_json: str
) -> str:
    """Create a prompt for evaluating completeness of a SOAP note."""
    return f"""Evaluate the COMPLETENESS of the following SOAP note.

ORIGINAL DIALOGUE:
{dialogue}

GROUND TRUTH:
{ground_truth_json}

MODEL PREDICTION:
{prediction_json}

Check if key clinically relevant information from the ground truth is present.
Return JSON only."""


def create_clinical_appropriateness_prompt(prediction_json: str) -> str:
    """Create a prompt for evaluating clinical appropriateness of a SOAP note."""
    return f"""Evaluate the CLINICAL APPROPRIATENESS of the following SOAP note.

MODEL PREDICTION:
{prediction_json}

Check if each section (S, O, A, P) contains the appropriate type of information.
Return JSON only."""


def create_section_judge_prompt(
    dialogue: str, key: str, ground_truth: str, prediction: str
) -> str:
    """Create a prompt for LLM-as-judge evaluation of a single SOAP section.
    
    This version appends /no_think to disable Qwen3's thinking mode for faster
    inference during RL training.
    
    Args:
        dialogue: The original doctor-patient dialogue.
        key: The SOAP section key (S, O, A, or P).
        ground_truth: The ground truth text for this section.
        prediction: The model's predicted text for this section.
    
    Returns:
        The formatted judge prompt with /no_think suffix.
    """
    return f"""Evaluate SOAP section: {key}

DIALOGUE:
{dialogue}

GROUND TRUTH ({key}):
{ground_truth}

PREDICTION ({key}):
{prediction}

Return JSON only. /no_think"""
