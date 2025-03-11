import json
from typing import Any, Callable, Dict, List, Optional, Tuple

import requests
import streamlit as st

from constants import API_ENDPOINT, API_TIMEOUT
from utils.db import _get_session_local
from utils.misc import is_local


def get_send_analyst_request_fnc() -> Callable[[List[Dict]], Optional[Dict]]:
    if is_local():
        return get_analyst_response__local
    else:
        return get_analyst_response__sis


def _build_error_msg_str(
    status: str, request_id: str, error_code: int, message: str
) -> str:
    return f"""
🚨 An Analyst API error has occurred 🚨

* response code: `{status}`
* request-id: `{request_id}`
* error code: `{error_code}`

Message:
```
{message}
```
        """


def get_analyst_response__local(
    messages: List[Dict],
) -> Tuple[Dict[str, Any], Optional[str]]:
    """
    Send chat history to the Cortex Analyst API and return the response in local setup.

    Args:
        messages (List[Dict]): The conversation history.

    Returns:
        Optional[Dict]: The response from the Cortex Analyst API.
    """
    # Prepare the request body with the user's prompt
    request_body = {
        "messages": messages,
        "semantic_model_file": f"@{st.session_state.selected_semantic_model_path}",
    }
    sf_session = _get_session_local()
    # In some cases it will be required to replace '_' with '-' here
    host_name = sf_session.conf.get("host")
    token = sf_session.conf.get("rest").token
    resp = requests.post(
        url=f"https://{host_name}{API_ENDPOINT}",
        json=request_body,
        headers={
            "Authorization": f'Snowflake Token="{token}"',
            "Content-Type": "application/json",
        },
    )
    request_id = resp.headers.get("X-Snowflake-Request-Id")
    try:
        out_obj = {**resp.json(), "request_id": request_id}
    except requests.exceptions.JSONDecodeError:
        return {"request_id": request_id}, _build_error_msg_str(
            resp.status_code,
            request_id,
            "None",
            "There is no message, as response content could not be parsed.",
        )

    if resp.status_code < 400:
        return out_obj, None  # type: ignore[arg-type]
    else:
        return out_obj, _build_error_msg_str(
            resp.status_code, request_id, out_obj["error_code"], ""
        )


def get_analyst_response__sis(
    messages: List[Dict],
) -> Tuple[Dict[str, Any], Optional[str]]:
    """
    Send chat history to the Cortex Analyst API and return the response.

    Args:
        messages (List[Dict]): The conversation history.

    Returns:
        Optional[Dict]: The response from the Cortex Analyst API.
    """
    # Prepare the request body with the user's prompt
    request_body = {
        "messages": messages,
        "semantic_model_file": f"@{st.session_state.selected_semantic_model_path}",
    }

    # Special package available only in SiS
    import _snowflake

    # Send a POST request to the Cortex Analyst API endpoint
    # Adjusted to use positional arguments as per the API's requirement
    resp = _snowflake.send_snow_api_request(
        "POST",  # method
        API_ENDPOINT,  # path
        {},  # headers
        {},  # params
        request_body,  # body
        None,  # request_guid
        API_TIMEOUT,  # timeout in milliseconds
    )

    # Content is a string with serialized JSON object
    parsed_content = json.loads(resp["content"])

    # Check if the response is successful
    if resp["status"] < 400:
        # Return the content of the response as a JSON object
        return parsed_content, None
    else:
        # Craft readable error message
        error_msg = _build_error_msg_str(
            resp["status"],
            parsed_content["request_id"],
            parsed_content["error_code"],
            parsed_content["message"],
        )
        return parsed_content, error_msg
