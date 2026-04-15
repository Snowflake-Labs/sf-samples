"""Reward functions for Medical SOAP RL training.

This module implements reward functions for evaluating SOAP note generation,
including JSON parsing validation and optional LLM-as-judge evaluation.
"""

import json
import logging
import re

logger = logging.getLogger("MedicalSOAPReward")

# Required keys for valid SOAP JSON output
SOAP_KEYS = {"S", "O", "A", "P"}


def extract_json_from_response(response: str) -> dict | None:
    """Extract JSON object from model response.

    Handles common cases like markdown code fences and extra whitespace.

    Args:
        response: Raw model response string.

    Returns:
        Parsed JSON dict if successful, None otherwise.
    """
    # Try direct parsing first
    try:
        return json.loads(response.strip())
    except json.JSONDecodeError:
        pass

    # Try to extract JSON from markdown code fences
    # Pattern matches ```json ... ``` or ``` ... ```
    json_pattern = r"```(?:json)?\s*(\{[\s\S]*?\})\s*```"
    match = re.search(json_pattern, response)
    if match:
        try:
            return json.loads(match.group(1))
        except json.JSONDecodeError:
            pass

    # Try to find any JSON object in the response
    # Look for the first { and last }
    first_brace = response.find("{")
    last_brace = response.rfind("}")
    if first_brace != -1 and last_brace > first_brace:
        try:
            return json.loads(response[first_brace : last_brace + 1])
        except json.JSONDecodeError:
            pass

    return None


def validate_soap_json(parsed: dict) -> bool:
    """Validate that parsed JSON has exactly the required SOAP keys.

    Args:
        parsed: Parsed JSON dictionary.

    Returns:
        True if valid SOAP structure, False otherwise.
    """
    if not isinstance(parsed, dict):
        return False

    # Check for exact keys
    if set(parsed.keys()) != SOAP_KEYS:
        return False

    # Check that all values are strings (non-empty preferred but not required)
    for key in SOAP_KEYS:
        if not isinstance(parsed[key], str):
            return False

    return True


def medical_soap_reward_fn(
    prompt: str,
    completions: str,
    prompt_ids: list[int],
    completion_ids: list[int],
    pred_S: str,
    pred_O: str,
    pred_A: str,
    pred_P: str,
    **kwargs,
) -> float:
    """Compute reward for SOAP note generation.

    This reward function uses a two-stage approach:
    1. JSON parsing check: Verifies the output is valid JSON with S, O, A, P keys
    2. Content quality bonus: Provides additional reward for non-empty sections

    Args:
        prompt: The input prompt string.
        completions: The model's completion/response string.
        prompt_ids: Token IDs of the prompt.
        completion_ids: Token IDs of the completion.
        pred_S: Ground truth Subjective section.
        pred_O: Ground truth Objective section.
        pred_A: Ground truth Assessment section.
        pred_P: Ground truth Plan section.
        **kwargs: Additional keyword arguments (ignored).

    Returns:
        Reward value between 0.0 and 1.0:
        - 0.0: Invalid JSON or missing/wrong keys
        - 0.5: Valid JSON structure with correct keys
        - 0.75: Valid JSON with all non-empty sections
        - 1.0: Valid JSON with substantial content in all sections
    """
    try:
        # Stage 1: JSON parsing check
        parsed = extract_json_from_response(completions)

        if parsed is None:
            logger.debug("Failed to parse JSON from response")
            return 0.0

        if not validate_soap_json(parsed):
            logger.debug(f"Invalid SOAP structure. Keys found: {parsed.keys()}")
            return 0.0

        # Stage 2: Content quality bonus
        reward = 0.5  # Base reward for valid structure

        # Check for non-empty sections
        non_empty_count = sum(
            1 for key in SOAP_KEYS if parsed[key] and len(parsed[key].strip()) > 0
        )

        if non_empty_count == 4:
            reward = 0.75  # All sections have some content

            # Additional bonus for substantial content (at least 20 chars each)
            substantial_count = sum(
                1 for key in SOAP_KEYS if len(parsed[key].strip()) >= 20
            )
            if substantial_count == 4:
                reward = 1.0  # All sections have substantial content

        return reward

    except Exception as e:
        logger.warning(f"Exception in medical_soap_reward_fn: {e}", exc_info=True)
        return 0.0


def medical_soap_reward_fn_strict(
    prompt: str,
    completions: str,
    prompt_ids: list[int],
    completion_ids: list[int],
    pred_S: str,
    pred_O: str,
    pred_A: str,
    pred_P: str,
    **kwargs,
) -> float:
    """Strict binary reward function for SOAP note generation.

    Returns 1.0 only if the output is valid JSON with all SOAP keys and
    non-empty content in each section. Otherwise returns 0.0.

    This is useful for later stages of training when the model has learned
    the basic format and needs stricter feedback.

    Args:
        Same as medical_soap_reward_fn.

    Returns:
        1.0 if valid and complete, 0.0 otherwise.
    """
    try:
        parsed = extract_json_from_response(completions)

        if parsed is None:
            return 0.0

        if not validate_soap_json(parsed):
            return 0.0

        # Require all sections to have substantial content
        for key in SOAP_KEYS:
            if len(parsed[key].strip()) < 10:
                return 0.0

        return 1.0

    except Exception:
        return 0.0
