# Import python packages
import streamlit as st
import pandas as pd
import altair as alt
from datetime import date, timedelta
from snowflake.snowpark.context import get_active_session
import yaml

# st.set_page_config(layout="wide")

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

    st.title(f"**:blue[Compute & QAS Cost Attribution]**")
    # st.write("Requirements for entire page:")
    # st.markdown("- Download for entire page, cost center, etc.")
    # st.markdown("- Summary tiles")
    # st.markdown("- Page level filters")

    # Database, schema
    with open('snowflake.yml', 'r') as f:
        config = yaml.load(f, Loader=yaml.SafeLoader).get('env')

    db_name = config.get('finops_acct_db')
    schema_name = config.get('finops_acct_schema')
    costcenter_schema_name = config.get('finops_sis_usage_sc')

    # View list - update to dynamically build dfs
    compute_qas_cost_center_vw = 'COMPUTE_AND_QAS_CC_CURRENCY_DAY'
    qas_cost_center_vw = 'QUERY_ACCELERATION_CC_CURRENCY_DAY'
    query_counts_cost_center_vw = 'QUERYCOUNTS_CC_DAY'

    compute_qas_cost_center_df = get_df(_session, db_name, schema_name, compute_qas_cost_center_vw)
    qas_cost_center_df = get_df(_session, db_name, schema_name, qas_cost_center_vw)
    query_counts_cost_center_df = get_df(_session, db_name, schema_name, query_counts_cost_center_vw)


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

            all_costcenters = st.checkbox(
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
    compute_qas_cost_center = st.container(border=True)
    with compute_qas_cost_center:
        st.write(f"**:blue[Compute & QAS by Cost Center]**")

        compute_qas_cost_center_df["DAY"] = pd.to_datetime(compute_qas_cost_center_df["DAY"]).dt.date

        filtered_df = compute_qas_cost_center_df.loc[ ( compute_qas_cost_center_df["COST_CENTER"].isin(selected_costcenter) )]

        final_df = filtered_df[(filtered_df['DAY'] >= start_date) & (filtered_df['DAY'] <= end_date)]

        compute_qas_line_chart = alt.Chart(final_df).mark_line(point=True).encode(
            x='DAY:T',
            y='WH_COST:Q',
            color='COST_CENTER',
        )

        st.altair_chart(compute_qas_line_chart, use_container_width=True)

        with st.expander("Compute & QAS by Cost Center - Data"):
            st.dataframe(final_df, use_container_width=True, hide_index=True)

    # QAS by Cost Center - future state, remove and group with above visual with Compute / QAS toggle
    qas_cost_center = st.container(border=True)

    with qas_cost_center:
        st.write(f"**:blue[QAS by Cost Center]**")

        # Initialize sample data
        if qas_cost_center_df.empty:
            data = {
                'DAY': ['2024-09-15','2024-09-15','2024-09-16','2024-09-16','2024-09-17','2024-09-17','2024-09-18','2024-09-18','2024-09-19','2024-09-19'],
                'COST_CENTER': ['PS', 'FINANCE','PS', 'FINANCE','PS', 'FINANCE','PS', 'FINANCE','PS', 'FINANCE'],
                'QAS_COST_CURRENCY': [5,3,3,1,10,3,4,1,9,2]
            }

            qas_cost_center_df = pd.DataFrame(data)

        qas_cost_center_df["DAY"] = pd.to_datetime(qas_cost_center_df["DAY"]).dt.date

        filtered_qas_cost_center_df = qas_cost_center_df.loc[ ( qas_cost_center_df["COST_CENTER"].isin(selected_costcenter) )]

        final_qas_cost_center_df = filtered_qas_cost_center_df[(filtered_qas_cost_center_df['DAY'] >= start_date) & (filtered_qas_cost_center_df['DAY'] <= end_date)]

        qas_line_chart = alt.Chart(final_qas_cost_center_df).mark_line(point=True).encode(
            x='DAY:T',
            y='QAS_COST_CURRENCY:Q',
            color='COST_CENTER',
        )

        st.altair_chart(qas_line_chart, use_container_width=True)

        with st.expander("QAS by Cost Center - Data"):
            st.dataframe(final_qas_cost_center_df, use_container_width=True, hide_index=True)

    # Query Counts by Cost Center
    query_counts_cost_center = st.container(border=True)

    with query_counts_cost_center:
        st.write(f"**:blue[Query Counts by Cost Center]**")

        # Unpivot df into appropriate format
        query_counts_cost_center_df = pd.melt(query_counts_cost_center_df,
                                              id_vars=['DAY', 'COST_CENTER'],
                                              value_vars=['NON_WH_QUERY_COUNT','WH_QUERY_COUNT','FAILURE_COUNT','SNOWFLAKE_WH_TASK'],
                                              var_name='QUERY_TYPE',
                                              value_name='QUERY_COUNT'
                                             )

        query_counts_cost_center_df["DAY"] = pd.to_datetime(query_counts_cost_center_df["DAY"]).dt.date
        filtered_query_cost_center_df = query_counts_cost_center_df.loc[ ( query_counts_cost_center_df["COST_CENTER"].isin(selected_costcenter) )]

        final_query_cost_center_df = filtered_query_cost_center_df[(filtered_query_cost_center_df['DAY'] >= start_date) & (filtered_query_cost_center_df['DAY'] <= end_date)]

        query_counts_line_chart = alt.Chart(final_query_cost_center_df).mark_line(point=True).encode(
            x='DAY:T',
            y='QUERY_COUNT:Q',
            color='QUERY_TYPE',
        )

        st.altair_chart(query_counts_line_chart, use_container_width=True)

        query_counts_cost_center_bar_chart = alt.Chart(filtered_query_cost_center_df).mark_bar().encode(
            x=alt.X('COST_CENTER').sort('-y'),
            y='sum(QUERY_COUNT)',
            color='QUERY_TYPE',
            order=alt.Order(
                'sum(QUERY_COUNT)',
                sort='descending'
            )
        )

        st.altair_chart(query_counts_cost_center_bar_chart, use_container_width=True)

        with st.expander("Compute & QAS by Cost Center - Data"):
            st.dataframe(final_query_cost_center_df, use_container_width=True, hide_index=True)

# app()
