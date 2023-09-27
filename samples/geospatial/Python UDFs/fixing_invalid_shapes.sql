-----------------------------------------
-- This code fixes invalid GEOGRAPHY shapes.
-- As an example we used table GEOLAB.PUBLIC.GEO_TABLE_SOURCE
-- which contains two columns:
-- id - Unique identifier
-- geog - GEOGRAPHY column with invalid shapes

-- Before using the code you need to update the name of DB/schema 
-- and the names of the source table and fields
-----------------------------------------

-----------------------------------------
-- UDFs
-----------------------------------------

----------------------------------------
-- This UDF takes as input an array of geospatial shapes, 
-- performs a union operation on them and returns the result 
-- as a binary object representing the union of the shapes 
-- in well-known binary (WKB) format.
-----------------------------------------
CREATE OR REPLACE FUNCTION GEOLAB.PUBLIC.PY_UNION_AGG(G1 ARRAY)
RETURNS BINARY
LANGUAGE PYTHON
RUNTIME_VERSION = '3.9'
PACKAGES = ('geopandas','shapely')
HANDLER = 'udf'
AS '
import geopandas as gpd
from shapely.ops import unary_union
from shapely.geometry import shape, mapping
from shapely import wkb
def udf(g1):
    geo_object = gpd.GeoSeries([shape(i) for i in g1])
    shape_union = unary_union(geo_object)
    return shape_union.wkb
';

-----------------------------------------
-- This UDF takes a geospatial object as input 
-- and returns a table of its interior rings (if any).
-----------------------------------------
CREATE OR REPLACE FUNCTION GEOLAB.PUBLIC.ST_GETINTERIORS(G OBJECT)
RETURNS TABLE (INTERIOR_RING OBJECT)
LANGUAGE JAVASCRIPT
AS '
{
processRow: function split_geom(row, rowWriter, context){
    let geojson = row.G;
    if (geojson.type === "Polygon") {
        coords = geojson.coordinates.slice(1);
    } else if ((geojson.type === "MultiPolygon")) {
        coords = geojson.coordinates.flatMap((poly) => poly.slice(1));
    } else {
        coords = [];
    }
    for (let i = 0; i < coords.length; i++) {
        rowWriter.writeRow({INTERIOR_RING: {
                "type" : "Polygon",
                "coordinates": [coords[i]]
            }
        });
    }
}
}
';

-----------------------------------------
-- This UDF takes a GEOGRAPHY object as input and returns
-- the exterior ring of the shape as a binary object 
-- in well-known binary (WKB) format
-----------------------------------------
CREATE OR REPLACE FUNCTION GEOLAB.PUBLIC.PY_GET_EXTERIOR(G1 GEOGRAPHY)
RETURNS BINARY(8388608)
LANGUAGE PYTHON 
RUNTIME_VERSION = '3.9'
PACKAGES = ('geopandas','shapely')
HANDLER = 'udf'
AS $$
import geopandas as gpd
from shapely.geometry import shape, mapping, Polygon, MultiPolygon
from shapely.validation import make_valid
from shapely import wkb
from shapely.ops import unary_union
def udf(g1):
    geom = shape(g1)
    exteriors = []
    if geom.geom_type == "MultiPolygon":
        exteriors = [Polygon(g.exterior) for g in geom.geoms]
        e_fixed = []
        for exterior in exteriors:
            e_fixed.append(exterior)
        return MultiPolygon(e_fixed).wkb
    else:
        exterior = Polygon(geom.exterior)
        return exterior.wkb
$$;

-----------------------------------------
--  This UDF is designed to take a GEOGRAPHY object, a distance, 
-- and a tolerance as input, and return the exterior of the object 
-- as a binary object in well-known binary (WKB) format, 
-- after performing certain operations to "repair" the exterior.
-----------------------------------------
CREATE OR REPLACE FUNCTION GEOLAB.PUBLIC.PY_FIX_EXTERIOR(G1 GEOGRAPHY, DIST float, TOL float)
RETURNS BINARY(8388608)
LANGUAGE PYTHON 
RUNTIME_VERSION = '3.9'
PACKAGES = ('geopandas','shapely')
HANDLER = 'udf'
AS $$
import geopandas as gpd
from shapely.geometry import shape, mapping, Polygon, MultiPolygon
from shapely.validation import make_valid
from shapely import wkb
from shapely.ops import unary_union
def udf(g1, dist, tol):
    geom = shape(g1)
    exteriors = []
    if geom.geom_type == "MultiPolygon":
        exteriors = [Polygon(g.exterior) for g in geom.geoms]
        e_fixed = []
        for exterior in exteriors:
            if exterior.is_valid == True:
                exterior = exterior.buffer(dist, resolution=1, join_style=1)
                exterior = exterior.simplify(tol)
            else:
                exterior = make_valid(exterior)
            e_fixed.append(exterior)
        return MultiPolygon(e_fixed).wkb
    else:
        exterior = Polygon(geom.exterior)
        if exterior.is_valid == True:
            exterior = exterior.buffer(dist, resolution=1, join_style=1)
            exterior = exterior.simplify(tol)
        else:
            exterior = make_valid(exterior)

        return exterior.wkb
$$;

-----------------------------------------
-- This UDF takes a GEOGRAPHY object (geo) and an integer (n)
-- as input and returns the n-th shape from the geo object 
-- if it is a composite geo type. 
-----------------------------------------
CREATE OR REPLACE FUNCTION GEOLAB.PUBLIC.PY_GEOGRAPHYN(geo geography, n integer)
returns geography
language python
runtime_version = '3.9'
packages = ('shapely')
handler = 'udf'
AS $$
from shapely.geometry import shape, mapping
def udf(geo, n):
    if geo['type'] not in ('MultiPoint', 'MultiLineString', 'MultiPolygon', 'GeometryCollection'):
        raise ValueError('Must be a composite geometry type')
    if n < 0:
        raise ValueError('N must be positive')
    g1 = shape(geo)
    if n > len(g1.geoms) - 1:
        raise ValueError('N out of range')
    else:
        return mapping(g1.geoms[n])
$$;

-----------------------------------------
-- This UDF takes a GEOGRAPHY object as input 
-- and returns the type of GEOGRAPHY as a string.
-----------------------------------------
CREATE OR REPLACE FUNCTION GEOLAB.PUBLIC.st_geographytype(geo GEOGRAPHY)
  RETURNS string
  AS
  $$
    ST_ASGEOJSON(geo):type::string
  $$
  ;

-----------------------------------------
-- This UDF takes a GEOGRAPHY object and returns the number 
-- of individual shapes within it, if it is a composite 
-- geo type (such as MultiPoint, MultiLineString, MultiPolygon, or GeometryCollection). 
-----------------------------------------
CREATE OR REPLACE FUNCTION GEOLAB.PUBLIC.PY_NUMGEOGRAPHYS(geo geography)
returns integer
language python
runtime_version = '3.9'
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

-----------------------------------------
--- FIXING STEPS
-----------------------------------------

-------------------------------------------------------------------------------
-- This section creates new tables for the exterior and interior polygons of invalid shapes. 
-- The exterior polygons represent the outside boundaries of the original shapes, and the 
-- interior polygons represent the holes in the original shapes. The exteriors are maintained 
-- as a single feature (shape), even if they are multipolygons. Interior polygons are 
-- extracted into individual features so they can be fixed separately. In some cases, 
-- the individual exterior or interior polygons are valid once they are extracted 
-- from the feature and do not require correction.
-------------------------------------------------------------------------------
-- Exteriors 
CREATE OR REPLACE TEMPORARY TABLE GEOLAB.PUBLIC.BAD_SHAPES 
(
    id  integer, 
    geog geography, 
    fix_buffer number(20,6), 
    fix_exterior geography
);

INSERT INTO GEOLAB.PUBLIC.BAD_SHAPES (id, geog, fix_exterior)
SELECT 
    id, 
    geog,
    to_geography(GEOLAB.PUBLIC.PY_GET_EXTERIOR(geog), True)
FROM GEOLAB.PUBLIC.GEO_TABLE_SOURCE
WHERE NOT st_isvalid(geog); 

-- Interiors  
CREATE OR REPLACE TEMPORARY TABLE GEOLAB.PUBLIC.BAD_INTERIOR LIKE GEOLAB.PUBLIC.BAD_SHAPES;
INSERT INTO GEOLAB.PUBLIC.BAD_INTERIOR  (id, geog, fix_exterior)
SELECT
    id, 
    to_geography(interior_ring, True) AS interior_loop,
    to_geography(GEOLAB.PUBLIC.PY_GET_EXTERIOR(interior_loop), True)
FROM GEOLAB.PUBLIC.BAD_SHAPES,
LATERAL GEOLAB.PUBLIC.ST_GETINTERIORS(st_asgeojson(geog))
ORDER BY random();

-------------------------------------------------------------------------------
-- This section fixes invalid exterior and interior polygons one at a time to introduce 
-- the smallest amount of change possible. To do this, it sets a buffer around the shape 
-- and then simplifies it. The buffer starts at 0.000001 (11 cm/4 inches) and doubles until 
-- all shapes are fixed or the buffer value exceeds 0.1 (11 km/6.8 miles). When we tested 
-- this script on our data with hudreds of complex invalid shapes, the largest buffer used for an 
-- exterior shape was 0.0001 (11 m/36 ft), with most exteriors requiring no simplification 
-- or the minimum of 0.000001. For the interior shapes, the maximum was 0.00001, again 
-- with most of the shapes requiring no correction or the minimum.
---
--- Details on the buffer value meaning: https://en.wikipedia.org/wiki/Decimal_degreeshttps://en.wikipedia.org/wiki/Decimal_degrees
-------------------------------------------------------------------------------
-- Exteriors 
EXECUTE IMMEDIATE $$
DECLARE
    dist numeric(20,6);
BEGIN
    dist := 0.000001;

    REPEAT
        UPDATE GEOLAB.PUBLIC.BAD_SHAPES 
        SET
            fix_buffer = :dist,
            fix_exterior = to_geography(GEOLAB.PUBLIC.PY_FIX_EXTERIOR(geog, :dist, 0.00001), True) 
        WHERE NOT st_isvalid(fix_exterior) ;
        
        dist := dist * 2; 
        UNTIL (dist > 0.1 OR SQLROWCOUNT=0)
    END REPEAT;
    RETURN dist/2;
END;
$$
;

--- Interiors
EXECUTE IMMEDIATE $$
DECLARE
    dist numeric(20,6);
BEGIN
    dist := 0.000001;

    REPEAT
        UPDATE GEOLAB.PUBLIC.bad_interior 
        SET
            fix_buffer = :dist,
            fix_exterior = to_geography(GEOLAB.PUBLIC.PY_FIX_EXTERIOR(geog, :dist, 0.00001), True) 
        WHERE NOT st_isvalid(fix_exterior) ;
        
        dist := dist * 2; 
        UNTIL (dist > 0.1 OR SQLROWCOUNT=0)
    END REPEAT;
    RETURN dist/2;
END;
$$
;
-------------------------------------------------------------------------------
--- This section combines all of the interior holes for each ID into a single multipolygon.
-------------------------------------------------------------------------------
CREATE OR REPLACE TEMPORARY TABLE GEOLAB.PUBLIC.FIX_INTERIOR AS 
SELECT
    id,
    to_geography(GEOLAB.PUBLIC.PY_UNION_AGG(array_agg(st_asgeojson(fix_exterior))), True) AS interior
FROM GEOLAB.PUBLIC.bad_interior
GROUP BY id;

-------------------------------------------------------------------------------
--- This section combines the fixed interior and exterior polygons back together 
-- by subtracting (using st_difference function) the fixed holes from the fixed exterior polygons.
-------------------------------------------------------------------------------
CREATE OR REPLACE TEMPORARY TABLE GEOLAB.PUBLIC.FIXED_SHAPES AS 
SELECT 
    bs.id,
    bs.geog, 
    iff(fi.interior is null, bs.fix_exterior, st_difference(bs.fix_exterior, fi.interior)) AS repaired_geog
FROM GEOLAB.PUBLIC.BAD_SHAPES bs 
LEFT OUTER JOIN GEOLAB.PUBLIC.FIX_INTERIOR fi 
ON fi.id = bs.id;

-------------------------------------------------------------------------------
--- This section removes any points that were introduced by the st_difference() function. 
-------------------------------------------------------------------------------
UPDATE GEOLAB.PUBLIC.FIXED_SHAPES t1
SET repaired_geog = geo
FROM
(
    SELECT
        id,
        GEOLAB.PUBLIC.PY_GEOGRAPHYN(repaired_geog, iff(GEOLAB.PUBLIC.ST_GEOGRAPHYTYPE(GEOLAB.PUBLIC.PY_GEOGRAPHYN(repaired_geog,0))='Point', 1, 0)) as geo
    FROM GEOLAB.PUBLIC.FIXED_SHAPES
    WHERE iff(GEOLAB.PUBLIC.ST_GEOGRAPHYTYPE(repaired_geog) = 'GeometryCollection', GEOLAB.PUBLIC.PY_NUMGEOGRAPHYS(repaired_geog) = 2, false)
) t2
WHERE t1.id = t2.id;


-------------------------------------------------------------------------------
--- This section updates the original data with the fixed shapes.
-------------------------------------------------------------------------------
UPDATE GEOLAB.PUBLIC.GEO_TABLE_SOURCE T1
SET geog = repaired_geog
FROM GEOLAB.PUBLIC.FIXED_SHAPES t2
WHERE NOT st_isvalid(t1.geog)
AND T1.ID = T2.ID;

