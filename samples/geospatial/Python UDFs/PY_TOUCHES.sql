-- Returns TRUE if g1 and g2 intersect, but their interiors do not intersect.

CREATE OR REPLACE FUNCTION py_touches(g1 geography, g2 geography)
returns boolean
language python
runtime_version = 3.8
packages = ('shapely')
handler = 'udf'
AS $$
from shapely.geometry import shape, mapping
def udf(g1, g2):
    geom1 = shape(g1)
    geom2 = shape(g2)
    return geom1.touches(geom2)
$$;

select py_touches(to_geography('POLYGON((-94.591555 39.042642,-94.591578 39.042381,-94.592080 39.042392,-94.591555 39.042642))'), 
to_geography('POLYGON((-94.591555 39.042642,-94.591578 39.042381,-94.591166 39.0423659,-94.591555 39.042642))')) ;