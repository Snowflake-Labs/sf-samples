# Import python packages
import streamlit as st
import pandas as pd
import altair as alt
from datetime import date, timedelta
from snowflake.snowpark.context import get_active_session
import yaml

st.set_page_config(layout="wide")

@st.cache_data
def get_df(_session, db, schema, view_name):
    df = _session.table([db, schema, view_name]).to_pandas()
    return df

def check_change(check_all,  multiselect_options, list_items ):
    if st.session_state[check_all]:
        st.session_state[multiselect_options]  = list_items
    else:
        st.session_state[multiselect_options] = []
    return

def multi_change(check_all, multiselect_options, list_items):
    if len(st.session_state[multiselect_options] ) == len(list_items):
        st.session_state[check_all] = True
    else:
        st.session_state[check_all] = False
    return

def build_dfs(db, schema, view_list):
    for i in view_list:
        i = get_df(db, schema, i)

def app():
    _session = get_active_session()

    st.title(f"**:blue[Storage Cost Attribution]**")
    # st.write("Requirements for entire page:")
    # st.markdown("- Download for entire page, cost center, etc.")
    # st.markdown("- Summary tiles")
    # st.markdown("- Page level filters")

    # Database, schema
    with open('snowflake.yml', 'r') as f:
        config = yaml.load(f, Loader=yaml.SafeLoader).get('env')

    db_name = config.get('finops_sis_db')
    schema_name = config.get('finops_acct_schema')
    costcenter_schema_name = config.get('finops_sis_usage_sc')

    # View list - update to dynamically build dfs
    STORAGE_CC_CREDITS_DAY_VW = 'STORAGE_CC_CREDITS_DAY'
    table_cost_center_vw = 'TABLE_RECREATION_SCHEMA_TABLETYPE_COUNT'


    storage_cost_center_df = get_df(_session, db_name, schema_name, STORAGE_CC_CREDITS_DAY_VW)
    table_cost_center_df = get_df(_session, db_name, schema_name, table_cost_center_vw)



    cost_center_tbl = 'COSTCENTER'

    cost_center_df = get_df(_session, db_name, costcenter_schema_name, cost_center_tbl)

    # Page Level filters
    filters = st.expander("**Page-Level Filters** :twisted_rightwards_arrows:", expanded=False)
    with filters:

        fil_1, fil_2 = st.columns(2)

        with fil_1:
            list_costcenters = cost_center_df["COSTCENTER_NAME"].unique().tolist()

            if "all_option_costcenter" not in st.session_state:
                st.session_state.all_option_costcenter = True
                st.session_state.selected_options_costcenter = list_costcenters

            selected_costcenter = st.multiselect(
                "Select Cost Center"
                ,list_costcenters
                ,key="selected_options_costcenter"
                ,on_change= multi_change
                ,args=("all_option_costcenter","selected_options_costcenter", list_costcenters )
                ,help = 'Select the Cost Center from button'
                #,default = list(['All'])
                ,max_selections=30
            )

            all_storeids = st.checkbox(
                "Select all"
                ,key='all_option_costcenter'
                ,on_change= check_change
                ,args=("all_option_costcenter","selected_options_costcenter", list_costcenters )
            )


        with fil_2:
            start_date, end_date = st.slider(
                "Date Range",
                value=(date.today() - timedelta(90), date.today())
            )

    # Compute & QAS by Cost Center (include WoW, radio button to view Compute & QAS individually or together as separate trendlines)
    storage_cost_center = st.container(border=True)

    with storage_cost_center:
        st.write(f"**:blue[Storage Trend by Type]**")

        filtered_df = storage_cost_center_df.loc[ ( storage_cost_center_df["COST_CENTER"].isin(selected_costcenter) )]
        filtered_df["DAY"] = pd.to_datetime(filtered_df["DAY"]).dt.date
        final_df = filtered_df[(filtered_df['DAY'] >= start_date) & (filtered_df['DAY'] <= end_date)]
        #st.table(final_df)

        compute_storage_line_chart = alt.Chart(final_df).mark_line(point=True).encode(
            x='DAY:T',
            y='STORAGE_COST_CURRENCY:Q',
            color='COST_CENTER',
        )

        st.altair_chart(compute_storage_line_chart, use_container_width=True)

        with st.expander("Storage Trend by Type - Data"):
            st.dataframe(final_df, use_container_width=True, hide_index=True)

    # Table by Cost Center - future state, remove and group with above visual with Compute / QAS toggle
    table_cost_center = st.container(border=True)

    with table_cost_center:
        st.write(f"**:blue[Table Re-Creation Counts by Schema by Table Type]**")

        table_filtered_df = table_cost_center_df.loc[ ( table_cost_center_df["COST_CENTER"].isin(selected_costcenter) )]

        storage_line_chart = alt.Chart(table_filtered_df).mark_bar(point=True).encode(
            x='TABLE_RECREATED_COUNT:Q',
            y='SCHEMA_NAME:O',
            color='COST_CENTER'
        )

        st.altair_chart(storage_line_chart, use_container_width=True)

        with st.expander("Table Re-Creation Counts by Schema by Table Type - Data"):

            st.dataframe(table_filtered_df, use_container_width=True, hide_index=True)
