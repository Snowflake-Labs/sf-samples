-- Returns the number of distinct geometries that constitute a composite geometry type 
-- (ST_GeomCollection, ST_MultiPoint, ST_MultiLineString, or ST_MultiPolygon).


CREATE OR REPLACE FUNCTION py_numgeographys(geo geography)
returns integer
language python
runtime_version = 3.8
packages = ('shapely')
handler = 'udf'
AS $$
from shapely.geometry import shape, mapping
def udf(geo):
    if geo['type'] not in ('MultiPoint', 'MultiLineString', 'MultiPolygon', 'GeometryCollection'):
        raise ValueError('Must be a composite geometry type')
    else:
        g1 = shape(geo)
        return len(g1.geoms)
$$;


select py_numgeographys(to_geography('MULTIPOLYGON(((-108.72070312499997 34.99400375757577,-100.01953124999997 46.58906908309183,
-90.79101562499996 34.92197103616377,-108.72070312499997 34.99400375757577),(-100.10742187499997 41.47566020027821,
-102.91992187499996 37.61423141542416,-96.85546874999996 37.54457732085582,-100.10742187499997 41.47566020027821)),
((-85.16601562499999 34.84987503195417,-80.771484375 28.497660832963476,-76.904296875 34.92197103616377,-85.16601562499999 34.84987503195417)))')) as num_geogs;