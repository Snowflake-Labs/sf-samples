import pandas as pd
import geopandas as gpd
import streamlit as st
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col, call_builtin


def app():

    st.title("Various visualisations using Leafmap")

    row1_col1, row1_col2 = st.columns([2, 1])
    width = 950
    height = 600

    with row1_col2:
        backend = st.selectbox(
            "Select a plotting backend", ["folium", "kepler.gl", "pydeck"], index=2
        )
        if backend == "folium":
            import leafmap.foliumap as leafmap
        elif backend == "kepler.gl":
            import leafmap.kepler as leafmap
        elif backend == "pydeck":
            import leafmap.deck as leafmap

        sess = Session.builder.configs(st.secrets["geo-playground"]).create()
        spatialfeatures = sess.table('DERIVED_SPATIALFEATURES_ESP_H3RES8_V1_YEARLY_V2').select(col("POPULATION"),col("year_prec"), col("summer_min"), col("summer_max"),col("GEOID"))
        geography = sess.table('GEOGRAPHY_ESP_H3RES8_V1').select(col("GEOID").alias("HEXAGON"),col("GEOM"))
        joined_df = spatialfeatures.join(geography, spatialfeatures.col("GEOID") == geography.col("HEXAGON"))
        joined_df = joined_df.select(col("GEOM"), col("POPULATION"),col("YEAR_PREC"), col("summer_min"), col("SUMMER_MAX"), col("HEXAGON")).filter(col("POPULATION") > 500)
        joined_df = joined_df.select(call_builtin("st_aswkt", col("GEOM")).alias("GEOM"), col("HEXAGON"), col("POPULATION"),col("YEAR_PREC"), col("SUMMER_MIN"), col("SUMMER_MAX"))
        df = pd.DataFrame(joined_df.collect())
        df['geometry'] = gpd.GeoSeries.from_wkt(df['GEOM'])
        gdf = gpd.GeoDataFrame(df[["POPULATION","YEAR_PREC", "SUMMER_MIN", "SUMMER_MAX", "HEXAGON"]], crs="EPSG:4326", geometry=df['geometry'])
        lon, lat = leafmap.gdf_centroid(gdf)
        column_names = gdf.columns.values.tolist()
        layer_name = "Spain"

        with row1_col2:
            if backend == "pydeck":
                random_column = st.selectbox("Select a column to apply random colors", column_names)

#        random_column = column_names[0]

        with row1_col1:
            if backend == "pydeck":
                m = leafmap.Map(center=(lat, lon))
                m.add_gdf(gdf, random_color_column=random_column)
                st.pydeck_chart(m)

            else:
                map_style = eval(open("config_leafmap.json").read())
                m = leafmap.Map(center=(lat, lon), draw_export=True)
                m.add_gdf(gdf, layer_name=layer_name)
                if backend == "folium":
                    m.zoom_to_gdf(gdf)
                m.to_streamlit(width=width, height=height)