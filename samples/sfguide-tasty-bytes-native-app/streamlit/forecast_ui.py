# Import python packages
import streamlit as st
from snowflake.snowpark.context import get_active_session
import pandas as pd
import datetime
import pydeck as pdk
import matplotlib
import snowflake.permissions as permission
import json

st.set_page_config(layout="wide")

# Get the current credentials
session = get_active_session()

# check if sales database has been granted reference permissions
app_name = session.sql("""
        select current_database()
    """).collect()[0]["CURRENT_DATABASE()"]
refs_json = session.sql(f"select system$get_reference_definitions('{app_name}') as REFS_JSON").collect()[0]["REFS_JSON"]
refs = json.loads(refs_json)
reference_set = True if refs[0]["bindings"] else False

if not reference_set:
   permission.request_reference('shift_sales')
else:
   # run the app once we have sales data
   st.title("Food Truck Sales Forecast")
   st.write(
      """The following data is from the Tasty Bytes sales forecast model. 
      It uses sales data from your account to ensure accuracy. 
      """
   )

   # only allow forecasts in cities where francisee operates
   # only pull from snowflake when the app is opened
   if 'city_list' not in st.session_state:
      distinct_cities = session.sql('''
                           select distinct city
                           from reference('shift_sales')
                           order by city
                           ''')

      st.session_state.city_list = pd.DataFrame(distinct_cities.collect())['CITY'].tolist()

   col1, col2, col3 = st.columns(3)

   with col1:
      st.session_state.city = st.selectbox("City: ", st.session_state.city_list)

   with col2:
      default_date = datetime.date.today() + datetime.timedelta(days=1)
      max_date = default_date + datetime.timedelta(days=7)
      st.session_state.input_date = st.date_input("Forecast Date: ", value=default_date, min_value=default_date, max_value=max_date)

   with col3:
      shift = st.selectbox("Shfit:", ["AM", "PM"])
      st.session_state.shift = 1 if shift == "AM" else 0

   if st.button("Get Forecast"):
      #  Call inference sproc
      data_frame = session.sql(f"CALL code_schema.inference('{st.session_state.city}', {st.session_state.shift}, DATE('{str(st.session_state.input_date)}'));")

      # convert results into a Pandas data frame
      queried_data = pd.DataFrame(data_frame.collect())

      # format the data for table
      formatted_data = queried_data[["LOCATION_NAME", "PREDICTION"]]
      formatted_data = formatted_data.rename(
         columns={
            "LOCATION_NAME": "Location",
            "PREDICTION": "Sales Forecast"
         }
      )
      formatted_data = formatted_data.style.background_gradient(
         axis=0, gmap=formatted_data["Sales Forecast"], 
         cmap="Greens"
      ).format({"Sales Forecast": "${:,.2f}"})
      
      col1, col2 = st.columns(2)

      # display dataframe
      with col1:
         st.dataframe(
            formatted_data,
            use_container_width=True,
         )
      
      # format data for map
      queried_data["Sales Forecast"] = queried_data["PREDICTION"].map('${:,.2f}'.format)
      queried_data["scaled_sales"] = (queried_data["PREDICTION"].max() - queried_data["PREDICTION"]) / (queried_data["PREDICTION"].max() - queried_data["PREDICTION"].min())

      # display map
      with col2:
         st.pydeck_chart(pdk.Deck(
            map_style=None,
            initial_view_state=pdk.data_utils.compute_view(
               queried_data[["LONGITUDE", "LATITUDE"]]
            ),
            layers=[
               pdk.Layer(
                  "ScatterplotLayer",
                  queried_data,
                  pickable=True,
                  opacity=0.8,
                  stroked=True,
                  filled=True,
                  radius_scale=6,
                  radius_min_pixels=1,
                  radius_max_pixels=15,
                  line_width_min_pixels=1,
                  get_position="[LONGITUDE, LATITUDE]",
                  get_radius="PREDICTION",
                  get_fill_color=['255 * scaled_sales', '1000 * scaled_sales + 60', '255 * scaled_sales'], # create a gradient to match rows
                  get_line_color=[0, 0, 0],
               )
            ],
            tooltip={
               "text": "{LOCATION_NAME}\n{Sales Forecast}"
            }
         ))
