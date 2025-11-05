######### INITIAL SETUP #########

# Import packages #
import streamlit as st

# Must be the first Streamlit command
st.set_page_config(page_title="Cloud Costs", page_icon=":material/cloud:", layout="wide")

# ------------------------------------------------------------------------------------- #

######### PAGE NAVIGATION #########

page_definitions = {
    "Documentation": [
        ("pages/documentation.py", "Documentation", "home"),
    ],
    "Resource": [
        ("pages/compute_summary.py", "Compute Costs", "computer"),
        ("pages/storage_summary.py", "Storage Costs", "storage"),
        ("pages/network_summary.py", "Network Costs", "network_wifi"),
    ],
    "Internal": [
        ("pages/internal_summary.py", "Spend Summary", "insights"),
        ("pages/forecast_opportunities.py", "Forecast Opportunities", "lightbulb"),
    ],
}


# Create st.Page() objects from the dictionary
page_hierarchy = {
    section: [
        st.Page(filename, title=title, icon=f":material/{icon}:") for filename, title, icon in pages
    ]
    for section, pages in page_definitions.items()
}

# Render the navigation
pg = st.navigation(page_hierarchy)
pg.run()
