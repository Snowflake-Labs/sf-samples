CREATE OR REPLACE FUNCTION PY_UNION_AGG(g1 array)
returns geography
language python
runtime_version = 3.8
packages = ('geopandas','shapely')
handler = 'udf'
AS $$
import geopandas as gpd
from shapely.ops import unary_union
from shapely.geometry import shape, mapping
def udf(g1):
    geo_object = gpd.GeoSeries([shape(i) for i in g1])
    shape_union = unary_union(geo_object)
    return mapping(shape_union)
$$;


SELECT PY_UNION_AGG(ARRAY_AGG(st_asgeojson(geo))) FROM OBJ_OF_INTEREST_SO;