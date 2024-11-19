import streamlit as st

from components.query_gallery_entry import query_gallery_entry
from utils.db import get_sf_connection
from utils.notifications import handle_notification_queue
from utils.storage.saved_answers import SavedQuery, get_all_shared_queries


def _fetch_all_shared_queries_to_state():
    session = get_sf_connection()
    st.session_state["all_shared_queries"] = get_all_shared_queries(session=session)


def sidebar():
    with st.sidebar:
        refresh = st.button("Refresh", use_container_width=True, type="primary")
        if refresh:
            _fetch_all_shared_queries_to_state()


def single_gallery_tile(obj: SavedQuery, idx: int):
    _ = query_gallery_entry(
        obj,
        show_share_status=False,
        unique_id=f"page_2_{idx}",
        on_refresh=_fetch_all_shared_queries_to_state,
    )


st.set_page_config(layout="wide")
handle_notification_queue()

sidebar()

st.title("👥 Public charts")
st.write(
    "Welcome to the public charts gallery! Here, you can browse and discover queries shared by all app users, including your own."
)
st.divider()

if "all_shared_queries" not in st.session_state:
    with st.spinner("Fetching saved queries..."):
        _fetch_all_shared_queries_to_state()

# Render everyting in two columns layout
all_shared_queries = st.session_state["all_shared_queries"]
for pair_idx, (obj_1, obj_2) in enumerate(
    zip(all_shared_queries[::2], all_shared_queries[1::2])
):
    col1, col2 = st.columns(2)
    with col1:
        single_gallery_tile(obj_1, pair_idx * 2)
    with col2:
        single_gallery_tile(obj_2, pair_idx * 2 + 1)

if len(all_shared_queries) % 2 != 0:
    col1, _ = st.columns(2)
    last_tile_idx = len(all_shared_queries)
    with col1:
        single_gallery_tile(all_shared_queries[-1], last_tile_idx)


if not st.session_state["all_shared_queries"]:
    st.info(
        'It seems like there are no saved queries here. Go to "Talk to your data" tab, and add some!'
    )
