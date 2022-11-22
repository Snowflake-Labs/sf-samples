CREATE OR REPLACE FUNCTION PY_INTERSECTION(g1 geometry, g2 geometry)
returns geometry
language python
runtime_version = 3.8
packages = ('shapely')
handler = 'udf'
AS $$
import shapely
from shapely.geometry import shape, mapping
def udf(g1, g2):
    shape1 = shape(g1)
    shape2 = shape(g2)
    return mapping(shape1.intersection(shape2))
$$;


select py_intersection(to_geometry('POLYGON((-97.72482633590697 38.04944182145127,-84.24133479595181 38.03929418732139,-91.34215861558913 31.296177449497677,-97.72482633590697 38.04944182145127))'),
                      to_geometry('POLYGON((-91.79851233959197 36.206334232169056,-97.41053044795987 31.046008090324193,-83.82624685764311 30.701652827539803,-91.79851233959197 36.206334232169056))'));