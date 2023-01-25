import json
import pandas as pd
import geopandas as gpd
from shapely import wkt
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
        feature = Feature(geometry = row["GEOM"], properties = {column_name : row[column_name]})
        list_features.append(feature)
        
    feat_collection = FeatureCollection(list_features)
    
    geojson_result = json.dumps(feat_collection)
    
    #optionally write to file
    if file_output is not None:
        with open(file_output,"w") as f:
            json.dump(feat_collection,f)
    
    return geojson_result


def choropleth_map(df_aggreg, column_name, border_color = 'none', fill_opacity = 0.5, initial_map = None, with_legend = True,
                   kind = "linear"):
    #colormap
    min_value = df_aggreg[column_name].min()
    max_value = df_aggreg[column_name].max()
    m = round ((min_value + max_value ) / 2 , 0)
    
    
    if initial_map is None:
        initial_map = Map(location= [41.390333,2.1647773], zoom_start=11, tiles="cartodbpositron")

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
    name_layer = "Choropleth "
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

    st.title("Folium")
    "## H3 Index"
    st.markdown("Population density in Spain<sup>*</sup>", unsafe_allow_html=True)
    sess = Session.builder.configs(st.secrets["geo-playground"]).create()
    spatialfeatures = sess.table('DERIVED_SPATIALFEATURES_ESP_H3RES8_V1_YEARLY_V2').select(col("POPULATION"),col("GEOID"))
    geography = sess.table('GEOGRAPHY_ESP_H3RES8_V1').select(col("GEOID"),col("GEOM"))
    joined_df = spatialfeatures.join(geography, spatialfeatures.col("GEOID") == geography.col("GEOID"))
    joined_df = joined_df.select(col("GEOM"), col("POPULATION")).filter(col("POPULATION") > 500)
    logging.info('CUSTOM LOG: SnowPark DF is there')
    df = pd.DataFrame(joined_df.collect())
    logging.info('CUSTOM LOG: Pandas df is there')
    df['GEOM'] = df['GEOM'].apply(lambda x: geojson.loads(x))
    logging.info('CUSTOM LOG: GEOM is done')
    map = choropleth_map(df_aggreg = df, column_name = "POPULATION")
    logging.info('CUSTOM LOG: choropleth_map is done')
    folium_static(map)
    st.markdown("<small><sup>*</sup>For the areas with a population of more than 500 citizens per 0.7 km<sup>2</sup> (size of one hexagon)</small>", unsafe_allow_html=True)