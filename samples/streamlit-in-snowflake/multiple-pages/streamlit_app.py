from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import streamlit as st
from streamlit.source_util import _on_pages_changed, get_pages
from streamlit.util import calc_md5


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


st.title("MultiPage App Example")
st.subheader("Simplified version of https://github.com/blackary/st_pages")

show_pages(
    [
        Page("streamlit_app.py", "Home", "🏠"),
        Page("secondary_page.py", "Secondary Page", "📄"),
    ]
)
