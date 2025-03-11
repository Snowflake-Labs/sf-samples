import streamlit as st


def is_local() -> bool:
    """
    Check if app is running locally.

    Returns:
        bool: True if running locally, else (if in SiS) False.
    """
    return st.experimental_user.get("user_name") is None
