CREATE OR REPLACE FUNCTION PY_MAKELINE_AGG(g1 array)
returns geography
language python
runtime_version = 3.8
packages = ('geopandas','shapely')
handler = 'udf'
AS $$
import geopandas as gpd
from shapely.geometry import Point, LineString
from shapely.geometry import shape, mapping
def udf(g1):
    geo_object = gpd.GeoSeries([shape(i) for i in g1])
    final_shape = LineString(geo_object.tolist())
    return mapping(final_shape)
$$;

SELECT PY_MAKELINE_AGG((ARRAY_AGG(st_asgeojson(geo))) FROM OBJ_OF_INTEREST_SO;