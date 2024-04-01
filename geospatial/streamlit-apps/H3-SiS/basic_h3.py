# This code of the Streamlit app visualizes data from the following dataset:
# https://app.snowflake.com/marketplace/listing/GZSVZ8ON6J/dataconsulting-pl-opencellid-open-database-of-cell-towers?search=opencellid
# The prerequisite for this app is to install that free dataset from the Marketplace to the 'opencellid' database

import streamlit as st
import pydeck as pdk
from snowflake.snowpark.context import get_active_session
from branca.colormap import LinearColormap

# Set up Streamlit and Snowflake
st.title("Cell towers density in the US")
session = get_active_session()

# Query data from Snowflake
query = '''
    SELECT H3_LATLNG_TO_CELL_STRING(lat, lon, 6) AS h3, COUNT(*) AS count
    FROM opencellid.public.raw_cell_towers
    WHERE mcc BETWEEN 310 AND 316
    GROUP BY 1
'''
df = session.sql(query).to_pandas()

# Create a color map for the data
quantiles = df["COUNT"].quantile([0, 0.25, 0.5, 0.75, 1])
colors = ['gray', 'blue', 'green', 'yellow', 'orange', 'red']
color_map = LinearColormap(colors, vmin=quantiles.min(), 
                           vmax=quantiles.max(), index=quantiles)
df['color'] = df['COUNT'].apply(color_map.rgb_bytes_tuple)

# Display the map with pydeck
view_state = pdk.ViewState(latitude=39.1166, longitude=-100.1833, zoom=3)
layer = pdk.Layer("H3HexagonLayer", df, get_hexagon="H3", 
                  get_fill_color="color", opacity=0.5, extruded=False)
st.pydeck_chart(pdk.Deck(map_style=None, initial_view_state=view_state, 
                         layers=[layer]))