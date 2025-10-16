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

    st.title(f"**:blue[Serverless Compute & Optimization Features]**")

    # Database, schema
    with open('snowflake.yml', 'r') as f:
        config = yaml.load(f, Loader=yaml.SafeLoader).get('env')

    db_name = config.get('finops_sis_db')
    schema_name = config.get('finops_acct_schema')
    costcenter_schema_name = config.get('finops_sis_usage_sc')

    # View list - update to dynamically build dfs
    ac_cost_center_vw = 'AUTOCLUSTERING_SCHEMA_CURRENCY_DAY'
    # cloud_services_cost_center_vw = 'CLOUD_SERVICES_BY_COST_CENTER'
    logging_events_cost_center_vw = 'LOGGING_EVENTS_CC_CREDITS_DAY'
    mv_usage_cost_center_vw = 'MATERIALIZED_VIEW_CC_CREDITS_DAY'
    rep_data_transfer_cost_center_vw = 'REPLICATION_DATA_TRANSFER_CC_CREDITS_DAY'
    serverless_task_cost_center_vw = 'SERVERLESS_TASK_CC_CREDITS_DAY'
    snowpipe_cost_center_vw = 'SNOWPIPE_COSTS_CC_CREDITS_DAY'
    sos_cost_center_vw = 'SOS_CC_CREDITS_DAY'

    ac_cost_center_df = get_df(_session, db_name, schema_name, ac_cost_center_vw)
    # cloud_services_cost_center_df = get_df(_session, db_name, schema_name, cloud_services_cost_center_vw)
    logging_events_cost_center_df = get_df(_session, db_name, schema_name, logging_events_cost_center_vw)
    mv_usage_cost_center_df = get_df(_session, db_name, schema_name, mv_usage_cost_center_vw)
    rep_data_transfer_cost_center_df = get_df(_session, db_name, schema_name, rep_data_transfer_cost_center_vw)
    serverless_task_cost_center_df = get_df(_session, db_name, schema_name, serverless_task_cost_center_vw)
    snowpipe_cost_center_df = get_df(_session, db_name, schema_name, snowpipe_cost_center_vw)
    sos_cost_center_df = get_df(_session, db_name, schema_name, sos_cost_center_vw)

    cost_center_tbl = 'COSTCENTER'

    cost_center_df = get_df(_session, db_name, costcenter_schema_name, cost_center_tbl)

    # Page Level filters
    filters = st.expander("**Page-Level Filters** :twisted_rightwards_arrows:", expanded=False)
    with filters:
        fil_1, fil_2 = st.columns(2)
        list_costcenters = cost_center_df["COSTCENTER_NAME"].unique().tolist()

        with fil_1:
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

    # download button for entire page as pdf placeholder here - currently not supported in SiS

    # 1. AC by Cost Center
    ac_cost_center = st.container(border=True)
    with ac_cost_center:
        st.write(f"**:blue[Automatic Clustering by Cost Center]**")

        # Initialize sample data
        if ac_cost_center_df.empty:
            data = [
                    ['2024-09-15', 'PS', 'FINOPS','FINOPS_ACCOUNT',10,223.3,0.2,1.23],
                    ['2024-09-16', 'PS', 'FINOPS','FINOPS_ACCOUNT',10,223.3,0.2,1.23],
                    ['2024-09-17', 'PS', 'ADMIN','ADMIN_TOOLS',10,223.3,0.2,1.23],
                   ]

            ac_cost_center_df = pd.DataFrame(data, columns=['DAY', 'COST_CENTER','DATABASE_NAME','SCHEMA_NAME','AC_COST_CURRENCY','RECLUSTER_GB','RECLUSTERED_TB','RECLUSTERED_ROWS_MM'])

        ac_cost_center_df["DAY"] = pd.to_datetime(ac_cost_center_df["DAY"]).dt.date

        filtered_df = ac_cost_center_df.loc[ ( ac_cost_center_df["COST_CENTER"].isin(selected_costcenter) )]

        final_df = filtered_df[(filtered_df['DAY'] >= start_date) & (filtered_df['DAY'] <= end_date)]

        ac_cost_center_line_chart = alt.Chart(final_df).mark_bar().encode(
            x='sum(AC_COST_CURRENCY)',
            y=alt.Y('COST_CENTER').sort('-x'),
            color='DATABASE_NAME'
        )

        st.altair_chart(ac_cost_center_line_chart, use_container_width=True)

        with st.expander("Automatic Clustering by Cost Center - Data"):
            st.dataframe(final_df, use_container_width=True, hide_index=True)

    # # 2. Cloud Services by Cost Center
    # cloud_services_cost_center = st.container(border=True)

    # with cloud_services_cost_center:
    #     st.write(f"**:blue[Cloud Services by Cost Center]**")

    #     cloud_services_cost_center_df["DAY"] = pd.to_datetime(cloud_services_cost_center_df["DAY"]).dt.date

    #     filtered_cloud_cost_center_df = cloud_services_cost_center_df.loc[ ( cloud_services_cost_center_df["COST_CENTER"].isin(selected_costcenter) )]

    #     final_cloud_cost_center_df = filtered_cloud_cost_center_df[(filtered_cloud_cost_center_df['DAY'] >= start_date) & (filtered_cloud_cost_center_df['DAY'] <= end_date)]

    #     cloud_services_line_chart = alt.Chart(final_cloud_cost_center_df).mark_line(point=True).encode(
    #         x='DAY:T',
    #         y='CREDITS_USED:Q',
    #         color='COST_CENTER',
    #     )

    #     st.altair_chart(cloud_services_line_chart, use_container_width=True)

    #     with st.expander("Cloud Services by Cost Center - Data"):
    #         st.dataframe(final_cloud_cost_center_df, use_container_width=True, hide_index=True)

    # 3. Logging Events by Cost Center
    logging_events_cost_center = st.container(border=True)

    with logging_events_cost_center:
        st.write(f"**:blue[Logging Events by Cost Center]**")

        # Initialize sample data
        if logging_events_cost_center_df.empty:
            data = [
                    ['2024-09-15', 'Professional Services', 'logging',2],
                    ['2024-09-16', 'Professional Services', 'logging',1.23],
                    ['2024-09-17', 'Professional Services', 'logging',5],
                   ]

            logging_events_cost_center_df = pd.DataFrame(data, columns=['DAY', 'COST_CENTER','SERVICE_TYPE','CREDITS_USED'])

        logging_events_cost_center_df["DAY"] = pd.to_datetime(logging_events_cost_center_df["DAY"]).dt.date

        filtered_logging_events_cost_center_df = logging_events_cost_center_df.loc[ ( logging_events_cost_center_df["COST_CENTER"].isin(selected_costcenter) )]

        final_cloud_cost_center_df = filtered_logging_events_cost_center_df[(filtered_logging_events_cost_center_df['DAY'] >= start_date) & (filtered_logging_events_cost_center_df['DAY'] <= end_date)]


        logging_events_line_chart = alt.Chart(final_cloud_cost_center_df).mark_line(point=True).encode(
            x='DAY:T',
            y='CREDITS_USED:Q',
            color='COST_CENTER',
        )

        st.altair_chart(logging_events_line_chart, use_container_width=True)

        with st.expander("Logging Events by Cost Center - Data"):
            st.dataframe(final_cloud_cost_center_df, use_container_width=True, hide_index=True)

    # 4. Materialized View Usage by Cost Center
    mv_usage_cost_center = st.container(border=True)
    with mv_usage_cost_center:
        st.write(f"**:blue[Materialized View Usage by Cost Center]**")

        mv_usage_cost_center_df["DAY"] = pd.to_datetime(mv_usage_cost_center_df["DAY"]).dt.date

        filtered_mv_usage_cost_center_df = mv_usage_cost_center_df.loc[ ( mv_usage_cost_center_df["COST_CENTER"].isin(selected_costcenter) )]

        final_cloud_cost_center_df = filtered_mv_usage_cost_center_df[(filtered_mv_usage_cost_center_df['DAY'] >= start_date) & (filtered_mv_usage_cost_center_df['DAY'] <= end_date)]


        mv_usage_line_chart = alt.Chart(final_cloud_cost_center_df).mark_line(point=True).encode(
            x='DAY:T',
            y='sum(CREDITS_USED)',
            color='COST_CENTER',
        )

        st.altair_chart(mv_usage_line_chart, use_container_width=True)

        mv_usage_bar_chart = alt.Chart(final_cloud_cost_center_df).mark_bar().encode(
            x='sum(CREDITS_USED)',
            y=alt.Y('COST_CENTER').sort('-x'),
            color='COST_CENTER'
        )

        st.altair_chart(mv_usage_bar_chart, use_container_width=True)


        with st.expander("Materialized View Usage by Cost Center - Data"):
            st.dataframe(final_cloud_cost_center_df, use_container_width=True, hide_index=True)

    # 5. Replication Data Transfer by Cost Center
    rep_data_transfer_cost_center = st.container(border=True)

    with rep_data_transfer_cost_center:
        st.write(f"**:blue[Replication Data Transfer by Cost Center]**")

        # Initialize sample data
        if rep_data_transfer_cost_center_df.empty:
            data = [
                    ['2024-09-15', 'Professional Services', 'replication',4],
                    ['2024-09-16', 'Professional Services', 'replication',6.23],
                    ['2024-09-17', 'Professional Services', 'replication',2],
                   ]

            rep_data_transfer_cost_center_df = pd.DataFrame(data, columns=['DAY', 'COST_CENTER','SERVICE_TYPE','CREDITS_USED'])

        rep_data_transfer_cost_center_df["DAY"] = pd.to_datetime(rep_data_transfer_cost_center_df["DAY"]).dt.date

        filtered_rep_data_transfer_cost_center_df = rep_data_transfer_cost_center_df.loc[ ( rep_data_transfer_cost_center_df["COST_CENTER"].isin(selected_costcenter) )]

        final_rep_data_transfer_cost_center_df = filtered_rep_data_transfer_cost_center_df[(filtered_rep_data_transfer_cost_center_df['DAY'] >= start_date) & (filtered_rep_data_transfer_cost_center_df['DAY'] <= end_date)]


        re_data_transfer_line_chart = alt.Chart(final_rep_data_transfer_cost_center_df).mark_line(point=True).encode(
            x='DAY:T',
            y='CREDITS_USED:Q',
            color='COST_CENTER',
        )

        st.altair_chart(re_data_transfer_line_chart, use_container_width=True)

        with st.expander("Replication Data Transfer by Cost Center - Data"):
            st.dataframe(final_rep_data_transfer_cost_center_df, use_container_width=True, hide_index=True)

    # 6.Serverless Task Usage by Cost Center
    serverless_task_cost_center = st.container(border=True)
    with serverless_task_cost_center:
        st.write(f"**:blue[Serverless Task by Cost Center]**")

        # Initialize sample data
        if serverless_task_cost_center_df.empty:
            data = [
                    ['2024-09-15', 'Professional Services', 'serverless tasks',2],
                    ['2024-09-16', 'Professional Services', 'serverless tasks',1.23],
                    ['2024-09-17', 'Professional Services', 'serverless tasks',5],
                    ['2024-09-15', 'Value Engineering', 'serverless tasks',4],
                    ['2024-09-16', 'Value Engineering', 'serverless tasks',10],
                    ['2024-09-17', 'Value Engineering', 'serverless tasks',11],
                   ]

            serverless_task_cost_center_df = pd.DataFrame(data, columns=['DAY', 'COST_CENTER','SERVICE_TYPE','CREDITS_USED'])

        serverless_task_cost_center_df["DAY"] = pd.to_datetime(serverless_task_cost_center_df["DAY"]).dt.date

        filtered_serverless_task_cost_center_df = serverless_task_cost_center_df.loc[ ( serverless_task_cost_center_df["COST_CENTER"].isin(selected_costcenter) )]

        final_serverless_task_cost_center_df = filtered_serverless_task_cost_center_df[(filtered_serverless_task_cost_center_df['DAY'] >= start_date) & (filtered_serverless_task_cost_center_df['DAY'] <= end_date)]


        serverless_task_line_chart = alt.Chart(final_serverless_task_cost_center_df).mark_line(point=True).encode(
            x='DAY:T',
            y='sum(CREDITS_USED)',
            color='COST_CENTER',
        )



        st.altair_chart(serverless_task_line_chart, use_container_width=True)

        with st.expander("Serverless Task by Cost Center - Data"):
            st.dataframe(final_serverless_task_cost_center_df, use_container_width=True, hide_index=True)

    # 7. Snowpipe by Cost Center
    snowpipe_cost_center = st.container(border=True)
    with snowpipe_cost_center:
        st.write(f"**:blue[Snowpipe by Cost Center]**")

        # Initialize sample data
        if snowpipe_cost_center_df.empty:
            data = [
                    ['2024-09-15', 'Professional Services', 'FINOPS.FINOPS_CONFIG',2],
                    ['2024-09-16', 'Professional Services', 'FINOPS.FINOPS_ACCOUNT',1.23],
                    ['2024-09-17', 'Professional Services', 'FINOPS.FINOPS_ACCOUNT',5],
                    ['2024-09-15', 'Value Engineering', 'ADMIN_DB.ADMIN_TOOLS',4],
                    ['2024-09-16', 'Value Engineering', 'ADMIN_DB.PUBLIC',10],
                    ['2024-09-17', 'Value Engineering', 'ADMIN_DB.ADMIN_TOOLS',11],
                   ]

            snowpipe_cost_center_df = pd.DataFrame(data, columns=['DAY', 'COST_CENTER','PIPE_SCHEMA','CREDITS_USED'])

        snowpipe_cost_center_df["DAY"] = pd.to_datetime(snowpipe_cost_center_df["DAY"]).dt.date

        filtered_snowpipe_cost_center_df = snowpipe_cost_center_df.loc[ ( snowpipe_cost_center_df["COST_CENTER"].isin(selected_costcenter) )]

        final_snowpipe_cost_center_df = filtered_snowpipe_cost_center_df[(filtered_snowpipe_cost_center_df['DAY'] >= start_date) & (filtered_snowpipe_cost_center_df['DAY'] <= end_date)]

        snowpipe_line_chart = alt.Chart(final_snowpipe_cost_center_df).mark_line(point=True).encode(
            x='DAY:T',
            y='sum(CREDITS_USED)',
            color='COST_CENTER',
        )

        st.altair_chart(snowpipe_line_chart, use_container_width=True)

        snowpipe_bar_chart = alt.Chart(final_snowpipe_cost_center_df).mark_bar().encode(
            x='sum(CREDITS_USED)',
            y=alt.Y('COST_CENTER').sort('-x'),
            color='PIPE_SCHEMA'
        )

        st.altair_chart(snowpipe_bar_chart, use_container_width=True)

        with st.expander("Snowpipe by Cost Center - Data"):
            st.dataframe(final_snowpipe_cost_center_df, use_container_width=True, hide_index=True)

    # 8. SOS by Cost Center
    sos_cost_center = st.container(border=True)
    with sos_cost_center:
        st.write(f"**:blue[Search Optimization Service by Cost Center]**")

        # Initialize sample data
        if sos_cost_center_df.empty:
            data = [
                    ['2024-09-15', 'Professional Services', 'FINOPS_DB.FINOPS_CONFIG',2],
                    ['2024-09-16', 'Professional Services', 'FINOPS_DB.FINOPS_ACCOUNT',1.23],
                    ['2024-09-17', 'Professional Services', 'FINOPS_DB.FINOPS_ACCOUNT',5],
                    ['2024-09-15', 'Value Engineering', 'ADMIN_DB.ADMIN_TOOLS',4],
                    ['2024-09-16', 'Value Engineering', 'ADMIN_DB.PUBLIC',10],
                    ['2024-09-17', 'Value Engineering', 'ADMIN_DB.ADMIN_TOOLS',11],
                   ]

            sos_cost_center_df = pd.DataFrame(data, columns=['DAY', 'COST_CENTER','TABLE_SCHEMA','CREDITS'])

        sos_cost_center_df["DAY"] = pd.to_datetime(sos_cost_center_df["DAY"]).dt.date

        filtered_sos_cost_center_df = sos_cost_center_df.loc[ ( sos_cost_center_df["COST_CENTER"].isin(selected_costcenter) )]

        final_sos_cost_center_df = filtered_sos_cost_center_df[(filtered_sos_cost_center_df['DAY'] >= start_date) & (filtered_sos_cost_center_df['DAY'] <= end_date)]


        sos_line_chart = alt.Chart(final_sos_cost_center_df).mark_line(point=True).encode(
            x='DAY:T',
            y='sum(CREDITS)',
            color='COST_CENTER',
        )

        st.altair_chart(sos_line_chart, use_container_width=True)

        sos_bar_chart = alt.Chart(final_sos_cost_center_df).mark_bar().encode(
            x='sum(CREDITS)',
            y=alt.Y('COST_CENTER').sort('-x'),
            color='TABLE_SCHEMA'
        )

        st.altair_chart(sos_bar_chart, use_container_width=True)

        with st.expander("Search Optimization Service by Cost Center - Data"):
            st.dataframe(final_snowpipe_cost_center_df, use_container_width=True, hide_index=True)

# app()
