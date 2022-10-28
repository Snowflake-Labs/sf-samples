import json
import pandas as pd
import geopandas as gpd
from shapely import wkb, wkt
from shapely.geometry import Polygon, shape
from folium import Map, Marker, GeoJson
import branca.colormap as cm
import geojson
from geojson.feature import *
from geojson import Feature, FeatureCollection
import streamlit as st
from streamlit_folium import folium_static
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col
import logging

def dataframe_to_geojson(df_geo, column_name, file_output = None):
    
    '''Produce the GeoJSON for a dataframe that has a geometry column in geojson format already'''
    
    list_features = []
    
    for i,row in df_geo.iterrows():
        feature = Feature(geometry = row["geometry"], properties = {column_name : row[column_name]})
        list_features.append(feature)
        
    feat_collection = FeatureCollection(list_features)
    
    geojson_result = json.dumps(feat_collection)
    
    #optionally write to file
    if file_output is not None:
        with open(file_output,"w") as f:
            json.dump(feat_collection,f)
    
    return geojson_result


def choropleth_map(df_aggreg, column_name, border_color = 'gray', fill_opacity = 0.5, initial_map = None, with_legend = True,
                   kind = "linear"):
    #colormap
    min_value = df_aggreg[column_name].min()
    max_value = df_aggreg[column_name].max()
    m = round ((min_value + max_value ) / 2 , 0)
    
    
    if initial_map is None:
        initial_map = Map(location= [51.608457629263995,4.271294826903612], zoom_start=12, tiles="cartodbpositron")

    #the colormap 
    #color names accepted https://github.com/python-visualization/branca/blob/master/branca/_cnames.json
    if kind == "linear":
        custom_cm = cm.LinearColormap(['gray','blue','green','yellow','orange','red'], vmin=min_value, vmax=max_value)
    elif kind == "outlier":
        #for outliers, values would be -11,0,1
        custom_cm = cm.LinearColormap(['white','gray','black'], vmin=min_value, vmax=max_value)
    elif kind == "filled_nulls":
        custom_cm = cm.LinearColormap(['sienna','green','yellow','red'], 
                                      index=[0,min_value,m,max_value],vmin=min_value,vmax=max_value)
   
    #create geojson data from dataframe
    geojson_data = dataframe_to_geojson(df_geo = df_aggreg, column_name = column_name)
    
    #plot on map
    name_layer = "Choropleth"
    if kind != "linear":
        name_layer = name_layer + kind
        
    GeoJson(
        geojson_data,
        style_function=lambda feature: {
            'fillColor': custom_cm(feature['properties'][column_name]),
            'color': border_color,
            'weight': 1,
            'fillOpacity': fill_opacity 
        }, 
        name = name_layer
    ).add_to(initial_map)

    #add legend (not recommended if multiple layers)
    if with_legend == True:
        custom_cm.add_to(initial_map)
        
    return initial_map

def app():

    st.title("Snowflake Search Optimisation Example")
    st.markdown("This map shows the geographic objects from GEOPLAYGROUND.GEO.OBJ_OF_INTEREST_SO table", unsafe_allow_html=True)
        # Create a session with Snowflake
    sess = Session.builder.configs(st.secrets["geo-demos"]).create()
        # Set geo data format to WKT
    sess.sql("ALTER SESSION SET GEOGRAPHY_OUTPUT_FORMAT='WKT'").collect()
        # SELECT FEATURE, GEO FROM GEOPLAYGROUND.GEO.OBJ_OF_INTEREST
    spatialfeatures = sess.table('OBJ_OF_INTEREST_SO').select(col("FEATURE"),col("GEO"))
        # Create DataFrame from query results
    df = pd.DataFrame(spatialfeatures.collect())
        # Convert DataFrame into the GeoDataFrame, and then convert to SRID WGS 84
    gs = gpd.GeoSeries.from_wkt(df['GEO'])
    gdf = gpd.GeoDataFrame(df[['FEATURE']], geometry=gs, crs=4326)
        # Convert FEATURE field into INTEGER
    gdf[['FEATURE']] = gdf[['FEATURE']].astype(int)
        # Create a Folium Map object
    map = choropleth_map(df_aggreg = gdf, column_name = "FEATURE")
        # Wrap Map object into Streamlit Component for rendering Folium maps
    folium_static(map)