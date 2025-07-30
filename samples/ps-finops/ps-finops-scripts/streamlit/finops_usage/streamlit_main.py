# Import python packages
import streamlit as st
import pandas as pd
import altair as alt
from datetime import date, timedelta
# Page Imports
import compute_qas_cost_attribution
import serverless_compute_optimization
import storage_cost_attribution
import streamlit.components.v1 as components
from fpdf import FPDF
import base64
from bs4 import BeautifulSoup
import random
import numpy as np
import yaml

from snowflake.snowpark.context import get_active_session

#st.set_page_config(layout="wide")


# Page calls
def compute_qas_cost_attribution_load():
    compute_qas_cost_attribution.app()

def serverless_compute_optimization_load():
    serverless_compute_optimization.app()

def storage_cost_attribution_load():
    storage_cost_attribution.app()

def get_df(_session, db, schema, view_name):
    df = _session.table([db, schema, view_name]).to_pandas()
    return df

def create_download_link(val, filename):
    b64 = base64.b64encode(val)  # val looks like b'...'
    return f'<br/> &nbsp; 💾 &nbsp; <a href="data:text/html;base64,{b64.decode()}" download="{filename}"><b>DOWNLOAD RESULTS</b> </a>'

class HTML_Report:

    def __init__(self, export_title, export_date) -> None:
        self.export_title = export_title
        self.export_date = export_date
        self.report_body = ""
        self.report_body_array = []

    def add_section(self, section_name: str) -> None:
        section_html = f'<div class="section"><h2>{section_name}</h2></div>'
        self.report_body_array.append(section_html)

    def add_container(self, html_content: str) -> None:
        container_html = f'<div id="container">{html_content}</div>'
        self.report_body += container_html

        self.report_body_array.append(self.report_body)
        self.report_body = ""


    def add_metric_visual(self, html_content: str, element_id: str) -> str:
        soup = BeautifulSoup(html_content, "html.parser")
        body = soup.find("body")
        graphs = body.find("script")
        graphs = str(graphs).replace('{"mode": "vega-lite"}', '{ "actions": false }')
        graphs = str(graphs).replace(
            "getElementById('vis')", f"getElementById('{element_id}')"
        )
        graphs = str(graphs).replace(
            'vegaEmbed("#vis", spec, embedOpt)',
            f'vegaEmbed("#{element_id}", spec, embedOpt)',
        )
        visual_html = f'<div class="graph"><div id="{element_id}"></div>{graphs}</div>'
        return visual_html
        # self.report_body += visual_html

        # self.report_body_array.append(self.report_body)
        # self.report_body = ""

    def add_tile(self, tile_name: str, html_metric: str, html_chart: str) -> str:
        tile_html = f"""<div class="tile">
        <h3>{tile_name}</h3>
        <div class="row">
            <div class="column metric">
            <h3>{html_metric}</h3>
            </div>
            <div class="column chart">
            {html_chart}
            </div>
        </div>

        </div>"""
        return tile_html


    def create_html(self,return_type:str) -> [str,bytes]:
        if return_type not in ["String", "Bytes"]:
            st.exception(Exception("Invalid return_type for function create_html()"))
        else:
            output=str()
            header = f"""
            <head>
            <title> Finops Usage</title>
            <script type="text/javascript" src="https://cdn.jsdelivr.net/npm/vega@5"></script>
            <script type="text/javascript" src="https://cdn.jsdelivr.net/npm/vega-lite@5.8.0"></script>
            <script type="text/javascript" src="https://cdn.jsdelivr.net/npm/vega-embed@6"></script>
            <style>
                html {{
                    display: table;
                    margin: auto;
                    margin-top: 3rem;
                    }}

                body {{
                    line-height: 1em;
                    font-weight: 100;
                    font-family: Inter, sans-serif;
                    display: table-cell;
                    width: 1200;
                    vertical-align: middle;
                    margin-top: 1rem;
                    background: rgb(25, 30, 36);
                    color: rgb(189, 196, 213);
                    }}

                .section {{
                    padding-left: 0;
                    padding: .5rem;
                    margin-top: 2rem;
                }}

                #container {{
                    display: flex;
                    flex-wrap: nowrap;
                    flex-direction: row;
                    justify-content: space-evenly;
                    align-items: center;
                }}

                .tile {{
                    border: 1px solid rgba(189, 196, 213, 0.2);
                    border-radius: 0.5rem;
                    padding: calc(1em - 1px);
                    /* height: 130px; */
                    overflow: auto;
                    background: transparent;
                    color: #60B4FF
                }}

                .row {{
                    display: flex;
                }}

                .column {{
                    float: left;
                    padding: 10px;
                    /* height: 100px; Should be removed. Only for demonstration */
                }}

                .metric {{
                    width: 25%;
                }}

                .chart {{
                    width: 75%;
                }}
                .row:after {{
                    content: "";
                    display: table;
                    clear: both;
                }}
            </style>

            </head>
            """
            output += header
            for v in self.report_body_array:
                output += f'{v}\n\n'

            if return_type == "String":
                return output
            if return_type == "Bytes":
                return bytes(output, encoding='utf-8')



    def compose_export():
        export_title = 'FinOps Snowflake Usage'
        export_date = date.today()
        export_date_line = 'Prepared on ' + export_date


def main():
    _session = get_active_session()

    if 'download_export' not in st.session_state:
        st.session_state['download_export'] = {}

    # Database, schema
    with open('snowflake.yml', 'r') as f:
        config = yaml.load(f, Loader=yaml.SafeLoader).get('env')

    db_name = config.get('finops_sis_db')
    schema_name = config.get('finops_sis_usage_sc')

    st.selectbox(
        "How would you like to view usage?",
        ("Account", "Organization")
    )

    cost_center_tbl = 'COSTCENTER'

    cost_center_df = get_df(_session, db_name, schema_name, cost_center_tbl)
    # Page Level filters
    filters = st.expander("**Page-Level Filters** :twisted_rightwards_arrows:", expanded=True)
    with filters:
        fil_1, fil_2 = st.columns(2)
        with fil_1:
            cost_center_selected = st.multiselect(
                "Cost Center"
                ,cost_center_df["COSTCENTER_NAME"]
            )

        with fil_2:
            start_date, end_date = st.slider(
                "Date Range",
                value=(date.today() - timedelta(30), date.today())
            )
            dates = pd.date_range(start_date, end_date)

    # Summary Tiles section
    st.write("**Compute & QAS Cost Attribution** :bar_chart:")



    # Row 1
    comp_1, comp_2, comp_3 = st.columns(3)

    with comp_1:
        comp_viz_1 = st.container(border=True, height=130)
        with comp_viz_1:
            comp_viz_1_metric, comp_viz_1_trend = st.columns([.35,.65])

            # Generate sample df
            compute_df = pd.DataFrame({'DAY': dates})
            compute_df['COST_CURRENCY'] = np.random.randint(8000, 10000, compute_df.shape[0])

            compute_value = "${:,.0f}K".format(compute_df['COST_CURRENCY'].sum() / 1000)

            with comp_viz_1_metric:
                compute_metric = st.metric(label=f"**:blue[Compute]**", value=compute_value, delta="{:.00%}".format(round(random.uniform(-.15, .15),2)), label_visibility='visible')

            compute_trend = alt.Chart(compute_df, padding={"left": 0, "top": 20, "right": 0, "bottom": 0}).encode(
                    x=alt.X('DAY', axis=alt.Axis(labels=False, title=None)),
                    y=alt.Y('COST_CURRENCY', axis=alt.Axis(labels=False, title=None))
                ).mark_line(
                    point=False
                ).configure_axis(
                    grid=False,
                    domain=False
                ).properties(
                    height=100
                )

            st.session_state['download_export']['compute'] = {
                "section": "Compute & QAS Cost Attribution",
                "metric": compute_value,
                "chart": compute_trend.to_html(),
                "id": "compute"
            }

            with comp_viz_1_trend:
                compute_chart = st.altair_chart(compute_trend, use_container_width=True)

    with comp_2:
        comp_viz_2 = st.container(border=True, height=130)
        with comp_viz_2:
            comp_viz_2_metric, comp_viz_2_trend = st.columns([.35,.65])

            # Generate sample df
            qas_df = pd.DataFrame({'DAY': dates})
            qas_df['COST_CURRENCY'] = np.random.randint(0, 500, qas_df.shape[0])

            qas_value = "${:,.0f}K".format(qas_df['COST_CURRENCY'].sum() / 1000)

            with comp_viz_2_metric:
                qas_metric = st.metric(label=f"**:blue[QAS]**", value=qas_value, delta="{:.00%}".format(round(random.uniform(-.15, .15),2)), label_visibility='visible')

            qas_trend = alt.Chart(qas_df, padding={"left": 0, "top": 20, "right": 0, "bottom": 0}).encode(
                    x=alt.X('DAY', axis=alt.Axis(labels=False, title=None)),
                    y=alt.Y('COST_CURRENCY', axis=alt.Axis(labels=False, title=None))
                ).mark_line(
                    point=False
                ).configure_axis(
                    grid=False,
                    domain=False
                ).properties(
                    height=100
                )

            st.session_state['download_export']['qas'] = {
                "section": "Compute & QAS Cost Attribution",
                "metric": qas_value,
                "chart": qas_trend.to_html(),
                "id": "qas"
            }

            with comp_viz_2_trend:
                qas_chart = st.altair_chart(qas_trend, use_container_width=True)

    with comp_3:
        comp_viz_3 = st.container(border=True, height=130)
        with comp_viz_3:
            comp_viz_3_metric, comp_viz_3_trend = st.columns([.35,.65])

            # Generate sample df
            query_count_df = pd.DataFrame({'DAY': dates})
            query_count_df['COST_CURRENCY'] = np.random.randint(12000, 15000, query_count_df.shape[0])

            query_count_value = "{:,.0f}K".format(query_count_df['COST_CURRENCY'].sum() / 1000)

            with comp_viz_3_metric:
                query_metric = st.metric(label=f"**:blue[Query Count]**", value=query_count_value, delta="{:.00%}".format(round(random.uniform(-.15, .15),2)), label_visibility='visible')

            query_count_trend = alt.Chart(query_count_df, padding={"left": 0, "top": 20, "right": 0, "bottom": 0}).encode(
                    x=alt.X('DAY', axis=alt.Axis(labels=False, title=None)),
                    y=alt.Y('COST_CURRENCY', axis=alt.Axis(labels=False, title=None))
                ).mark_line(
                    point=False
                ).configure_axis(
                    grid=False,
                    domain=False
                ).properties(
                    height=100
                )

            st.session_state['download_export']['query_count'] = {
                "section": "Compute & QAS Cost Attribution",
                "metric": query_count_value,
                "chart": query_count_trend.to_html(),
                "id": "query_count"
            }

            with comp_viz_3_trend:
                query_chart = st.altair_chart(query_count_trend, use_container_width=True)

    st.write("**Serverless Compute & Optimization Features** :bar_chart:")

    # Row 1
    serverless_1, serverless_2, serverless_3 = st.columns(3)

    with serverless_1:
        serverless_viz_1 = st.container(border=True, height=130)
        with serverless_viz_1:
            serverless_viz_1_metric, serverless_viz_1_trend = st.columns([.35,.65])

            # Generate sample df
            serverless_task_df = pd.DataFrame({'DAY': dates})
            serverless_task_df['COST_CURRENCY'] = np.random.randint(500, 800, serverless_task_df.shape[0])

            serverless_task_value = "${:,.0f}K".format(serverless_task_df['COST_CURRENCY'].sum() / 1000)

            with serverless_viz_1_metric:
                serverless_task_metric = st.metric(label=f"**:blue[Serverless Task]**", value=serverless_task_value, delta="{:.00%}".format(round(random.uniform(-.15, .15),2)), label_visibility='visible')

            serverless_task_trend = alt.Chart(serverless_task_df, padding={"left": 0, "top": 20, "right": 0, "bottom": 0}).encode(
                    x=alt.X('DAY', axis=alt.Axis(labels=False, title=None)),
                    y=alt.Y('COST_CURRENCY', axis=alt.Axis(labels=False, title=None))
                ).mark_line(
                    point=False
                ).configure_axis(
                    grid=False,
                    domain=False
                ).properties(
                    height=100
                )

            st.session_state['download_export']['serverless_task'] = {
                "section": "Serverless Compute & Optimization Features",
                "metric": serverless_task_value,
                "chart": serverless_task_trend.to_html(),
                "id": "serverless_task"
            }

            with serverless_viz_1_trend:
                serverless_task_chart = st.altair_chart(serverless_task_trend, use_container_width=True)

    with serverless_2:
        serverless_viz_2 = st.container(border=True, height=130)
        with serverless_viz_2:
            serverless_viz_2_metric, serverless_viz_2_trend = st.columns([.35,.65])

            # Generate sample df
            cloud_services_df = pd.DataFrame({'DAY': dates})
            cloud_services_df['COST_CURRENCY'] = np.random.randint(100, 500, cloud_services_df.shape[0])

            cloud_services_value = "${:,.0f}K".format(cloud_services_df['COST_CURRENCY'].sum() / 1000)

            with serverless_viz_2_metric:
                cloud_services_metric = st.metric(label=f"**:blue[Cloud Services]**", value=cloud_services_value, delta="{:.00%}".format(round(random.uniform(-.15, .15),2)), label_visibility='visible')

            cloud_services_trend = alt.Chart(cloud_services_df, padding={"left": 0, "top": 20, "right": 0, "bottom": 0}).encode(
                    x=alt.X('DAY', axis=alt.Axis(labels=False, title=None)),
                    y=alt.Y('COST_CURRENCY', axis=alt.Axis(labels=False, title=None))
                ).mark_line(
                    point=False
                ).configure_axis(
                    grid=False,
                    domain=False
                ).properties(
                    height=100
                )

            st.session_state['download_export']['cloud_services'] = {
                "section": "Serverless Compute & Optimization Features",
                "metric": cloud_services_value,
                "chart": cloud_services_trend.to_html(),
                "id": "cloud_services"
            }

            with serverless_viz_2_trend:
                cloud_services_chart = st.altair_chart(cloud_services_trend, use_container_width=True)

    with serverless_3:
        serverless_viz_3 = st.container(border=True, height=130)
        with serverless_viz_3:
            serverless_viz_3_metric, serverless_viz_3_trend = st.columns([.35,.65])

            # Generate sample df
            snowpipe_df = pd.DataFrame({'DAY': dates})
            snowpipe_df['COST_CURRENCY'] = np.random.randint(100, 500, snowpipe_df.shape[0])

            snowpipe_value = "${:,.0f}K".format(snowpipe_df['COST_CURRENCY'].sum() / 1000)

            with serverless_viz_3_metric:
                snowpipe_metric = st.metric(label=f"**:blue[Snowpipe]**", value=snowpipe_value, delta="{:.00%}".format(round(random.uniform(-.15, .15),2)), label_visibility='visible')

            snowpipe_trend = alt.Chart(snowpipe_df, padding={"left": 0, "top": 20, "right": 0, "bottom": 0}).encode(
                    x=alt.X('DAY', axis=alt.Axis(labels=False, title=None)),
                    y=alt.Y('COST_CURRENCY', axis=alt.Axis(labels=False, title=None))
                ).mark_line(
                    point=False
                ).configure_axis(
                    grid=False,
                    domain=False
                ).properties(
                    height=100
                )

            st.session_state['download_export']['snowpipe'] = {
                "section": "Serverless Compute & Optimization Features",
                "metric": snowpipe_value,
                "chart": snowpipe_trend.to_html(),
                "id": "snowpipe"
            }

            with serverless_viz_3_trend:
                snowpipe_chart = st.altair_chart(snowpipe_trend, use_container_width=True)

    # Row 2
    serverless_4, serverless_5, serverless_6 = st.columns(3)

    with serverless_4:
        serverless_viz_4 = st.container(border=True, height=130)
        with serverless_viz_4:
            serverless_viz_4_metric, serverless_viz_4_trend = st.columns([.35,.65])

            # Generate sample df
            ac_df = pd.DataFrame({'DAY': dates})
            ac_df['COST_CURRENCY'] = np.random.randint(400, 600, ac_df.shape[0])

            ac_value = "${:,.0f}K".format(ac_df['COST_CURRENCY'].sum() / 1000)

            with serverless_viz_4_metric:
                ac_metric = st.metric(label=f"**:blue[Auto-Clustering]**", value=ac_value, delta="{:.00%}".format(round(random.uniform(-.15, .15),2)), label_visibility='visible')

            ac_trend = alt.Chart(ac_df, padding={"left": 0, "top": 20, "right": 0, "bottom": 0}).encode(
                    x=alt.X('DAY', axis=alt.Axis(labels=False, title=None)),
                    y=alt.Y('COST_CURRENCY', axis=alt.Axis(labels=False, title=None))
                ).mark_line(
                    point=False
                ).configure_axis(
                    grid=False,
                    domain=False
                ).properties(
                    height=100
                )

            st.session_state['download_export']['ac'] = {
                "section": "Serverless Compute & Optimization Features",
                "metric": ac_value,
                "chart": ac_trend.to_html(),
                "id": "Auto-Clustering"
            }

            with serverless_viz_4_trend:
                ac_chart = st.altair_chart(ac_trend, use_container_width=True)

    with serverless_5:
        serverless_viz_5 = st.container(border=True, height=130)
        with serverless_viz_5:
            serverless_viz_5_metric, serverless_viz_5_trend = st.columns([.35,.65])

            # Generate sample df
            sos_df = pd.DataFrame({'DAY': dates})
            sos_df['COST_CURRENCY'] = np.random.randint(200, 300, sos_df.shape[0])

            sos_value = "${:,.0f}K".format(sos_df['COST_CURRENCY'].sum() / 1000)

            with serverless_viz_5_metric:
                sos_metric = st.metric(label=f"**:blue[Search Optimization]**", value=sos_value, delta="{:.00%}".format(round(random.uniform(-.15, .15),2)), label_visibility='visible')

            sos_trend = alt.Chart(sos_df, padding={"left": 0, "top": 20, "right": 0, "bottom": 0}).encode(
                    x=alt.X('DAY', axis=alt.Axis(labels=False, title=None)),
                    y=alt.Y('COST_CURRENCY', axis=alt.Axis(labels=False, title=None))
                ).mark_line(
                    point=False
                ).configure_axis(
                    grid=False,
                    domain=False
                ).properties(
                    height=100
                )

            st.session_state['download_export']['sos'] = {
                "section": "Serverless Compute & Optimization Features",
                "metric": sos_value,
                "chart": sos_trend.to_html(),
                "id": "search_optimization"
            }

            with serverless_viz_5_trend:
                sos_chart = st.altair_chart(sos_trend, use_container_width=True)

    with serverless_6:
        serverless_viz_6 = st.container(border=True, height=130)
        with serverless_viz_6:
            serverless_viz_6_metric, serverless_viz_6_trend = st.columns([.35,.65])

            # Generate sample df
            mv_df = pd.DataFrame({'DAY': dates})
            mv_df['COST_CURRENCY'] = np.random.randint(300, 400, mv_df.shape[0])

            mv_value = "${:,.0f}K".format(mv_df['COST_CURRENCY'].sum() / 1000)

            with serverless_viz_6_metric:
                mv_metric = st.metric(label=f"**:blue[Materialized View]**", value=mv_value, delta="{:.00%}".format(round(random.uniform(-.15, .15),2)), label_visibility='visible')

            mv_trend = alt.Chart(mv_df, padding={"left": 0, "top": 20, "right": 0, "bottom": 0}).encode(
                    x=alt.X('DAY', axis=alt.Axis(labels=False, title=None)),
                    y=alt.Y('COST_CURRENCY', axis=alt.Axis(labels=False, title=None))
                ).mark_line(
                    point=False
                ).configure_axis(
                    grid=False,
                    domain=False
                ).properties(
                    height=100
                )

            st.session_state['download_export']['mv'] = {
                "section": "Serverless Compute & Optimization Features",
                "metric": mv_value,
                "chart": mv_trend.to_html(),
                "id": "materialized_views"
            }

            with serverless_viz_6_trend:
                mv_chart = st.altair_chart(mv_trend, use_container_width=True)

    st.write("**Storage Cost Attribution** :bar_chart:")

    # Row 1
    storage_1, storage_2, storage_3 = st.columns(3)

    with storage_1:
        storage_viz_1 = st.container(border=True, height=130)
        with storage_viz_1:
            storage_viz_1_metric, storage_viz_1_trend = st.columns([.35,.65])

            # Generate sample df
            active_storage_df = pd.DataFrame({'DAY': dates})
            active_storage_df['COST_CURRENCY'] = np.random.randint(3000, 4000, active_storage_df.shape[0])

            active_storage_value = "${:,.0f}K".format(active_storage_df['COST_CURRENCY'].sum() / 1000)

            with storage_viz_1_metric:
                active_storage_metric = st.metric(label=f"**:blue[Active Storage]**", value=active_storage_value, delta="{:.00%}".format(round(random.uniform(-.15, .15),2)), label_visibility='visible')

            active_storage_trend = alt.Chart(active_storage_df, padding={"left": 0, "top": 20, "right": 0, "bottom": 0}).encode(
                    x=alt.X('DAY', axis=alt.Axis(labels=False, title=None)),
                    y=alt.Y('COST_CURRENCY', axis=alt.Axis(labels=False, title=None))
                ).mark_line(
                    point=False
                ).configure_axis(
                    grid=False,
                    domain=False
                ).properties(
                    height=100
                )

            st.session_state['download_export']['active_storage'] = {
                "section": "Storage Cost Attribution",
                "metric": active_storage_value,
                "chart": active_storage_trend.to_html(),
                "id": "active_storage"
            }

            with storage_viz_1_trend:
                active_storage_chart = st.altair_chart(active_storage_trend, use_container_width=True)

    with storage_2:
        storage_viz_2 = st.container(border=True, height=130)
        with storage_viz_2:
            storage_viz_2_metric, storage_viz_2_trend = st.columns([.35,.65])

            # Generate sample df
            fail_safe_df = pd.DataFrame({'DAY': dates})
            fail_safe_df['COST_CURRENCY'] = np.random.randint(150, 250, fail_safe_df.shape[0])

            fail_safe_value = "${:,.0f}K".format(fail_safe_df['COST_CURRENCY'].sum() / 1000)

            with storage_viz_2_metric:
                fail_safe_metric = st.metric(label=f"**:blue[Fail Safe]**", value=fail_safe_value, delta="{:.00%}".format(round(random.uniform(-.15, .15),2)), label_visibility='visible')

            fail_safe_trend = alt.Chart(fail_safe_df, padding={"left": 0, "top": 20, "right": 0, "bottom": 0}).encode(
                    x=alt.X('DAY', axis=alt.Axis(labels=False, title=None)),
                    y=alt.Y('COST_CURRENCY', axis=alt.Axis(labels=False, title=None))
                ).mark_line(
                    point=False
                ).configure_axis(
                    grid=False,
                    domain=False
                ).properties(
                    height=100
                )

            st.session_state['download_export']['fail_safe'] = {
                "section": "Storage Cost Attribution",
                "metric": fail_safe_value,
                "chart": fail_safe_trend.to_html(),
                "id": "fail_safe"
            }

            with storage_viz_2_trend:
                fail_safe_chart = st.altair_chart(fail_safe_trend, use_container_width=True)

    with storage_3:
        storage_viz_3 = st.container(border=True, height=130)
        with storage_viz_3:
            storage_viz_3_metric, storage_viz_3_trend = st.columns([.35,.65])

            # Generate sample df
            time_travel_df = pd.DataFrame({'DAY': dates})
            time_travel_df['COST_CURRENCY'] = np.random.randint(300, 400, time_travel_df.shape[0])

            time_travel_value = "${:,.0f}K".format(time_travel_df['COST_CURRENCY'].sum() / 1000)

            with storage_viz_3_metric:
                time_travel_metric = st.metric(label=f"**:blue[Time Travel]**", value=time_travel_value, delta="{:.00%}".format(round(random.uniform(-.15, .15),2)), label_visibility='visible')

            time_travel_trend = alt.Chart(time_travel_df, padding={"left": 0, "top": 20, "right": 0, "bottom": 0}).encode(
                    x=alt.X('DAY', axis=alt.Axis(labels=False, title=None)),
                    y=alt.Y('COST_CURRENCY', axis=alt.Axis(labels=False, title=None))
                ).mark_line(
                    point=False
                ).configure_axis(
                    grid=False,
                    domain=False
                ).properties(
                    height=100
                )

            st.session_state['download_export']['time_travel'] = {
                "section": "Storage Cost Attribution",
                "metric": time_travel_value,
                "chart": time_travel_trend.to_html(),
                "id": "time_travel"
            }

            with storage_viz_3_trend:
                time_travel_chart = st.altair_chart(time_travel_trend, use_container_width=True)

    # download button for entire page as pdf placeholder here - currently not supported in SiS

    # time_travel_trend.save('time_travel_trend.html')

    export_as_html = st.button(label="Download Export")

    report_text = "Report Text"

    if export_as_html:

        html_object = HTML_Report(
                export_title=f'FinOps Snowflake Usage',
                export_date=str(date.today())
            )

        file_name = "finops_download_" + str(date.today())

        # Compute & QAS Cost Attribution Section

        html_object.add_section(section_name="Compute & QAS Cost Attribution")

        compute_tile = html_object.add_tile(
            st.session_state['download_export']['compute'].get('id'),
            st.session_state['download_export']['compute'].get('metric'),
            html_object.add_metric_visual(
                html_content=st.session_state['download_export']['compute'].get('chart'),
                element_id=st.session_state['download_export']['compute'].get('id')
            )
        )

        qas_tile = html_object.add_tile(
            st.session_state['download_export']['qas'].get('id'),
            st.session_state['download_export']['qas'].get('metric'),
            html_object.add_metric_visual(
                html_content=st.session_state['download_export']['qas'].get('chart'),
                element_id=st.session_state['download_export']['qas'].get('id')
            )
        )

        query_count_tile = html_object.add_tile(
            st.session_state['download_export']['query_count'].get('id'),
            st.session_state['download_export']['query_count'].get('metric'),
            html_object.add_metric_visual(
                html_content=st.session_state['download_export']['query_count'].get('chart'),
                element_id=st.session_state['download_export']['query_count'].get('id')
            )
        )

        compute_qas_container = compute_tile + '\n' + qas_tile + '\n' + query_count_tile

        html_object.add_container(
            compute_qas_container
        )

        # Serverless Compute & Optimization Features

        html_object.add_section(section_name="Serverless Compute & Optimization Features")

        serverless_task_tile = html_object.add_tile(
            st.session_state['download_export']['serverless_task'].get('id'),
            st.session_state['download_export']['serverless_task'].get('metric'),
            html_object.add_metric_visual(
                html_content=st.session_state['download_export']['serverless_task'].get('chart'),
                element_id=st.session_state['download_export']['serverless_task'].get('id')
            )
        )

        cloud_services_tile = html_object.add_tile(
            st.session_state['download_export']['cloud_services'].get('id'),
            st.session_state['download_export']['cloud_services'].get('metric'),
            html_object.add_metric_visual(
                html_content=st.session_state['download_export']['cloud_services'].get('chart'),
                element_id=st.session_state['download_export']['cloud_services'].get('id')
            )
        )

        snowpipe_tile = html_object.add_tile(
            st.session_state['download_export']['snowpipe'].get('id'),
            st.session_state['download_export']['snowpipe'].get('metric'),
            html_object.add_metric_visual(
                html_content=st.session_state['download_export']['snowpipe'].get('chart'),
                element_id=st.session_state['download_export']['snowpipe'].get('id')
            )
        )

        serverless_compute_row1_container = serverless_task_tile + '\n' + cloud_services_tile + '\n' + snowpipe_tile

        html_object.add_container(
            serverless_compute_row1_container
        )

        # row 2

        ac_tile = html_object.add_tile(
            st.session_state['download_export']['ac'].get('id'),
            st.session_state['download_export']['ac'].get('metric'),
            html_object.add_metric_visual(
                html_content=st.session_state['download_export']['ac'].get('chart'),
                element_id=st.session_state['download_export']['ac'].get('id')
            )
        )

        sos_tile = html_object.add_tile(
            st.session_state['download_export']['sos'].get('id'),
            st.session_state['download_export']['sos'].get('metric'),
            html_object.add_metric_visual(
                html_content=st.session_state['download_export']['sos'].get('chart'),
                element_id=st.session_state['download_export']['sos'].get('id')
            )
        )

        mv_tile = html_object.add_tile(
            st.session_state['download_export']['mv'].get('id'),
            st.session_state['download_export']['mv'].get('metric'),
            html_object.add_metric_visual(
                html_content=st.session_state['download_export']['mv'].get('chart'),
                element_id=st.session_state['download_export']['mv'].get('id')
            )
        )

        serverless_compute_row2_container = ac_tile + '\n' + sos_tile + '\n' + mv_tile

        html_object.add_container(
            serverless_compute_row2_container
        )

        # Storage Cost Attribution Section

        html_object.add_section(section_name="Storage Cost Attribution")

        active_storage_tile = html_object.add_tile(
            st.session_state['download_export']['active_storage'].get('id'),
            st.session_state['download_export']['active_storage'].get('metric'),
            html_object.add_metric_visual(
                html_content=st.session_state['download_export']['active_storage'].get('chart'),
                element_id=st.session_state['download_export']['active_storage'].get('id')
            )
        )

        fail_safe_tile = html_object.add_tile(
            st.session_state['download_export']['fail_safe'].get('id'),
            st.session_state['download_export']['fail_safe'].get('metric'),
            html_object.add_metric_visual(
                html_content=st.session_state['download_export']['fail_safe'].get('chart'),
                element_id=st.session_state['download_export']['fail_safe'].get('id')
            )
        )

        time_travel_tile = html_object.add_tile(
            st.session_state['download_export']['time_travel'].get('id'),
            st.session_state['download_export']['time_travel'].get('metric'),
            html_object.add_metric_visual(
                html_content=st.session_state['download_export']['time_travel'].get('chart'),
                element_id=st.session_state['download_export']['time_travel'].get('id')
            )
        )

        storage_container = active_storage_tile + '\n' + fail_safe_tile + '\n' + time_travel_tile

        html_object.add_container(
            storage_container
        )

        str_result = html_object.create_html(return_type="String")

        #Return Bytes to be attached to a button or other actions.
        bt_result = html_object.create_html(return_type="Bytes")

        html = create_download_link(str_result.encode("utf-8"), file_name)
        st.markdown(html, unsafe_allow_html=True)

# Initialize multipage session state
if 'page_name' not in st.session_state:
    st.session_state['page_name'] = 'main'

# Sidebar
with st.sidebar:
    # Logo + Title
    st.image('bug-sno-blue.png',width=100)
    st.title(f"**:blue[FinOps Snowflake Usage]**")
    st.divider()

    # Pages
    if st.button("Home", use_container_width=True):
        st.session_state['page_name'] = 'main'

    if st.button("Compute & QAS Cost Attribution", use_container_width=True):
        st.session_state['page_name'] = 'compute_qas_cost_attribution'

    if st.button("Serverless Compute & Optimization Features", use_container_width=True):
        st.session_state['page_name'] = 'serverless_compute_optimization'

    if st.button("Storage Cost Attribution", use_container_width=True):
        st.session_state['page_name'] = 'storage_cost_attribution'

if st.session_state['page_name']:
    if st.session_state['page_name'] == 'main':
        main()

    if st.session_state['page_name'] == 'compute_qas_cost_attribution':
        compute_qas_cost_attribution_load()

    if st.session_state['page_name'] == 'serverless_compute_optimization':
        serverless_compute_optimization_load()

    if st.session_state['page_name'] == 'storage_cost_attribution':
        storage_cost_attribution_load()
