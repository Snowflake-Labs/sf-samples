import time

from snowflake.snowpark.session import Session
import streamlit as st
import pandas as pd
import logging ,sys
from util_fns import *
import requests

# Import the commonly defined utility scripts using
# dynamic path include
import sys

sys.path.append('src/python/lutils')
import sflk_base as L
sys.path.append('src/python/')
from preprocessing import pp_data, onnx_model
from functions import image_embedding, create_udf

# Define the project home directory, this is used for locating the config.ini file
PROJECT_HOME_DIR='.'

logging.basicConfig(stream=sys.stdout, level=logging.ERROR)
logger = logging.getLogger('exec_sql_script')

# Initialize a session with Snowflake
config = L.get_config(PROJECT_HOME_DIR)
sp_session = None
if "snowpark_session" not in st.session_state:
    sp_session = L.connect_to_snowflake(PROJECT_HOME_DIR)
    sp_session.use_role(f'''{config['SNOW_CONN']['role']}''')
    sp_session.use_schema(f'''{config['SNOW_CONN']['database']}.{config['SNOW_CONN']['schema']}''')
    sp_session.use_warehouse(f'''{config['SNOW_CONN']['warehouse']}''')
    st.session_state['snowpark_session'] = sp_session
else:
    sp_session = st.session_state['snowpark_session']

sp_session.add_packages("cachetools", "pillow", "torchvision", "onnxruntime")


def page_header():
    # Header.
    st.image("src/streamlit/images/logo-sno-blue.png", width=100)
    st.subheader("Media Advertising Campaign Optimization Demo")

def custom_page_styles():
    # page custom Styles
    st.markdown("""
                <style>
                    #MainMenu {visibility: hidden;} 
                    header {height:0px !important;}
                    footer {visibility: hidden;}
                    .block-container {
                        padding: 0px;
                        padding-top: 15px;
                        padding-left: 1rem;
                        max-width: 98%;
                    }
                    [data-testid="stVerticalBlock"] {
                        gap: 0;
                    }
                    [data-testid="stHorizontalBlock"] {
                        width: 99%;
                    }
                    .stApp {
                        width: 98% !important;
                    }
                    [data-testid="stMetricLabel"] {
                        font-size: 1.25rem;
                        font-weight: 700;
                    }
                    [data-testid="stMetricValue"] {
                        font-size: 1.5rem;
                        color: gray;
                    }
                    [data-testid="stCaptionContainer"] {
                        font-size: 1.25rem;
                    }
                    [data-testid="stMarkdownContainer"] {
                        font-size: 2rem;
                    }
                    div.stButton > button:first-child {
                        background-color: #50C878;color:white; border-color: none;
                </style>""", unsafe_allow_html=True)

def build_UI():
    custom_page_styles()
    page_header()
    st.markdown(
        """<hr style="height:2px; width:99%; border:none;color:lightgrey;background-color:lightgrey;" /> """,
        unsafe_allow_html=True)

    # st.write("""
    #     This page is used for running a sample SQL script. These SQL scripts would typically involve
    #     such activities like creating database, stored procs, roles ,stage etc..
    # """)

    st.markdown("""<hr style="height:40px; font-size:2px; width:99%; border:none;color:none;" /> """,
                unsafe_allow_html=True)
    st.caption("Snowflake Connection Information")

    st.markdown("""<hr style="height:20px; font-size:2px; width:99%; border:none;color:none;" /> """,
                unsafe_allow_html=True)
    c1, c2, c3, c4 = st.columns(4)
    with c1:
        st.metric("Database", f"{config['APP_DB']['database']}")

    with c2:
        st.metric("Schema", f"{config['APP_DB']['schema']}")

    with c3:
        st.metric("Warehouse", f"{config['SNOW_CONN']['warehouse']}")

    with c4:
        st.metric("Role", f"{config['APP_DB']['role']}")

    st.markdown("""<hr style="height:40px; font-size:2px; width:99%; border:none;color:none;" /> """,
                unsafe_allow_html=True)

    with st.expander("Step 1 - Database, Schema, Loading Model", expanded=True):
        script_output = st.empty()
        st11, st12 = st.columns([0.12, 0.88])

        with st11:
            create_db_btn = st.button(' ▶️  Create Database')
        with st12:
            if create_db_btn:
                with st.spinner("Snowflake environment is getting ready, please wait.."):
                    exec_sql_script('./src/sql-script/1_setup.sql', 'script_output')
                    sp_session.use_schema(f'''{config['APP_DB']['database']}.{config['APP_DB']['schema']}''')
                    sp_session.use_role(f'''{config['APP_DB']['role']}''')
                    pp_data(sp_session)
                    onnx_model(sp_session)
        if 'script_output' in st.session_state:
            st.markdown("""<hr style="height:30px; font-size:2px; width:99%; border:none;color:none;" /> """,
                        unsafe_allow_html=True)
            st.write("Script Output")
            st.json(st.session_state['script_output'])

    with st.expander("Step 2 - Functions & Procedures", False):
        script_output_2 = st.empty()

        st31, st32 = st.columns([0.18, 0.82])
        with st31:
            fns_btn = st.button(' ▶️  Load UDF')
        with st32:
            if fns_btn:
                with st.spinner("Required UDFs and Stored Procs are getting created , please wait.."):
                    create_udf(sp_session)
                    image_embedding(sp_session)

if __name__ == '__main__':
    build_UI()
