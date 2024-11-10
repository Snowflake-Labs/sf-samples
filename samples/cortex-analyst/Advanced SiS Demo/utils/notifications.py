"""
This module provides a simple notification system for Streamlit applications.

It allows adding notifications to a queue and displaying them, even after
executing `st.rerun()`.
"""

from collections import deque
from typing import Deque, Literal, NamedTuple

import streamlit as st

NotificationType = Literal["info", "error"]
Notification = NamedTuple(
    "Notification", [("message", str), ("type", NotificationType)]
)


def handle_notification_queue():
    """Handle the notification queue by displaying each notification and then removing it from the queue."""
    if "notifications_queue" not in st.session_state:
        st.session_state["notifications_queue"] = deque()

    notification_queue: Deque[Notification] = st.session_state["notifications_queue"]
    while notification_queue:
        notification = notification_queue.popleft()
        if notification.type == "error":
            st.toast(notification.message, icon="🚨")
        else:
            st.toast(notification.message, icon="ℹ️")


def add_to_notification_queue(msg: str, _type: NotificationType):
    """
    Add a notification to the notification queue.

    Args:
        msg (str): The message to be displayed in the notification.
        _type (NotificationType): The type of the notification ('info' or 'error').
    """
    if "notifications_queue" not in st.session_state:
        st.session_state["notifications_queue"] = deque()

    st.session_state.notifications_queue.append(Notification(msg, _type))
