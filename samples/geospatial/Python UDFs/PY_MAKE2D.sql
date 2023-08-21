-- This UDF takes variant column with WKB as input and returns Geography. If input contained M-value it will be removed.

CREATE OR REPLACE FUNCTION PY_MAKE2D(g1 varchar)
returns geography
language python
runtime_version = 3.8
packages = ('shapely', 'gdal')
handler = 'udf'
AS $$
from osgeo import ogr
import shapely.wkb
from shapely.geometry import mapping
def udf(g1):
    geom = bytes.fromhex(g1)
    ogr_geom = ogr.CreateGeometryFromWkb(geom)
    ogr_geom.SetMeasured(False)
    shapely_geom = shapely.wkb.loads(ogr_geom.ExportToIsoWkb().hex(), hex = True)
    return mapping(shapely_geom)
$$;


SET shape_w_m = '01d50700000200000001d207000002000000000000000000000000000000000000000000000000004940000000000000f03f000000000000f03f0000000000406f4001d2070000020000000000000000000840000000000000f03f0000000000c062400000000000001040000000000000f03f0000000000003940';
SELECT PY_MAKE2D($shape_w_m);