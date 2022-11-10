CREATE OR REPLACE FUNCTION PY_MAKEVALID_WKB(geowkb BINARY) 
RETURNS BINARY 
LANGUAGE python 
runtime_version = 3.8 
packages = ('shapely') 
handler = 'udf' 
AS $$
import shapely
from shapely.geometry import shape, mapping
from shapely import wkb
from shapely.validation import make_valid
def udf(geowkb):
    g1 = wkb.loads(geowkb)
    if g1.is_valid == True:
        g1 = g1.buffer(0.000001, resolution = 1, join_style = 1)
        g1 = g1.simplify(0.000001)
        fixed_shape = g1
    else:
        fixed_shape = make_valid(g1)
    return  wkb.dumps(fixed_shape)
$$;




SELECT to_geography(PY_MAKEVALID_WKB(wkb), TRUE) AS geofixed,
       st_npoints(geofixed)
FROM raw_wkb
WHERE length(wkb) < 6000000
AND st_isvalid(geofixed) = TRUE;
