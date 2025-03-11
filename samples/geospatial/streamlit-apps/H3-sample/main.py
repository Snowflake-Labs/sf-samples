# This code of the Streamlit app visualizes data from the following dataset:
# https://app.snowflake.com/marketplace/listing/GZSVZ8ON6J/dataconsulting-pl-opencellid-open-database-of-cell-towers

import streamlit as st
import pandas as pd
import pydeck as pdk
import branca.colormap as cm
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col
import json
from PIL import Image

session = Session.builder.configs(st.secrets["geodemo"]).create()
# Replace the previous line with the following one in case you use ths code for Streamlit in Snowflake: 
# session = get_active_session()

col1, col2 = st.columns(2)

with col1:
    h3_resolution = st.slider("H3 resolution", min_value=1, max_value=6, value=3)

with col2:
    style_option = st.selectbox("Style schema", ("Contrast", "Snowflake"), index=1)

df = session.sql(f'select h3_latlng_to_cell_string(lat, lon, {h3_resolution}) as h3, count(*) as count\n'\
'from OPENCELLID.PUBLIC.RAW_CELL_TOWERS\n'\
'where mcc between 310 and 316\n'\
'group by 1').to_pandas()

if style_option == "Contrast":
    quantiles = df["COUNT"].quantile([0, 0.25, 0.5, 0.75, 1])
    colors = ['gray','blue','green','yellow','orange','red']
if style_option == "Snowflake":
    quantiles = df["COUNT"].quantile([0, 0.33, 0.66, 1])
    colors = ['#666666', '#24BFF2', '#126481', '#D966FF']

color_map = cm.LinearColormap(colors, vmin=quantiles.min(), vmax=quantiles.max(), index=quantiles)
df['COLOR'] = df['COUNT'].apply(color_map.rgb_bytes_tuple)
st.pydeck_chart(pdk.Deck(map_provider='carto', map_style='light',
                         initial_view_state=pdk.ViewState(
                             latitude=38.51405689475766,
                             longitude=-96.50284957885742, zoom=3),
                             layers=[pdk.Layer("H3HexagonLayer", df, get_hexagon="H3",
                                               get_fill_color="COLOR", 
                                               get_line_color="COLOR",
                                               opacity=0.5, extruded=False)]))