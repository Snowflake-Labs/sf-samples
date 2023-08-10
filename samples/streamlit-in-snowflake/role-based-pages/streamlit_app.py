from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import streamlit as st
from snowflake.snowpark.context import get_active_session
from snowflake.snowpark.exceptions import SnowparkSessionException
from streamlit.source_util import _on_pages_changed, get_pages
from streamlit.util import calc_md5


def running_in_sis() -> bool:
    try:
        get_active_session()
        return True
    except SnowparkSessionException:
        return False


# See sis-local for more details about this part
if running_in_sis():
    import streamlit as st

    session = get_active_session()
else:
    import streamlit_in_snowflake as st

    # This part is optional, and requires secrets.toml setup
    from streamlit_in_snowflake.local_session import get_local_session

    session = get_local_session()


def get_current_roles() -> list[str]:
    df = session.sql(
        """
        SELECT
            VALUE::string as ROLE
        FROM TABLE(FLATTEN(input => PARSE_JSON(CURRENT_AVAILABLE_ROLES())))
        """
    ).to_pandas()

    return list(df["ROLE"].values)


@dataclass
class Page:
    path: str
    name: str
    icon: str | None = None

    @property
    def page_hash(self) -> str:
        return calc_md5(str(Path(self.path).absolute()))

    def to_dict(self) -> dict[str, str | bool]:
        return {
            "page_script_hash": self.page_hash,
            "page_name": self.name,
            "icon": self.icon or "",
            "script_path": self.path,
        }


def show_pages(pages: list[Page]):
    """
    Given a list of Page objects, overwrite whatever pages are currently being
    shown in the sidebar, and overwrite them with this new set of pages.
    NOTE: This changes the list of pages globally, not just for the current user, so
    it is not appropriate for dymaically changing the list of pages.
    """
    current_pages: dict[str, dict[str, str | bool]] = get_pages("")  # type: ignore
    if set(current_pages.keys()) == set(p.page_hash for p in pages):
        return

    assert len(pages) > 0, "Must pass at least one page to show_pages"

    current_pages.clear()
    for page in pages:
        current_pages[page.page_hash] = page.to_dict()

    _on_pages_changed.send()


st.title("Role-Based Pages Example")

roles = get_current_roles()

pages = []
pages.append(Page("streamlit_app.py", "Home", "🏠"))
if "PUBLIC" in roles:
    pages.append(Page("public.py", "Public", "🌍"))

if "SUPER_ADMIN_ROLE" in roles:
    pages.append(Page("super_admin_role.py", "Admin Page", "💬"))


show_pages(pages)
