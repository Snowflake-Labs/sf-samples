CREATE OR REPLACE FUNCTION PY_CLOSESTPOINT(
    wktA STRING,
    wktB STRING
)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = 3.8
PACKAGES = ('shapely')
HANDLER = 'udf'
AS
$$
from shapely.wkt import loads
from shapely.ops import nearest_points
from shapely.validation import make_valid

def udf(wktA, wktB):
    # If either WKT is empty or NULL, return None
    if not wktA or not wktB:
        return None
    
    try:
        # 1) Parse WKT into Shapely geometries
        geomA = loads(wktA)
        geomB = loads(wktB)
        
        # 2) Validate geometries (optional but good practice)
        if not geomA.is_valid:
            geomA = make_valid(geomA)
        if not geomB.is_valid:
            geomB = make_valid(geomB)
        
        # 3) Find the pair of closest points
        #    The first returned point lies on geomA, the second on geomB
        point_on_A, point_on_B = nearest_points(geomA, geomB)
        
        # 4) Return the point on A as a WKT string
        return point_on_A.wkt

    except Exception as e:
        # Return the exception text for debugging;
        # in production, you might return None or re-raise
        return str(e)
$$;


---- How to call the function
SELECT PY_CLOSESTPOINT(
         'MULTIPOLYGON(((-108.72070312499997 34.99400375757577,-100.01953124999997 46.58906908309183,-90.79101562499996 34.92197103616377,-108.72070312499997 34.99400375757577),(-100.10742187499997 41.47566020027821,-102.91992187499996 37.61423141542416,-96.85546874999996 37.54457732085582,-100.10742187499997 41.47566020027821)),((-85.16601562499999 34.84987503195417,-80.771484375 28.497660832963476,-76.904296875 34.92197103616377,-85.16601562499999 34.84987503195417)))',
         'POLYGON((-120.06786346435545 42.017417046219435,-114.0413475036621 42.02978667407419,-114.02709960937499 36.15852843041614,-114.7697067260742 36.05645450949238,-114.68645095825194 35.0906979730151,-120.02511978149411 39.06744706095475,-120.06786346435545 42.017417046219435))'
       ) AS CLOSEST_POINT;

--- Example using a dataset from Carto Academy listing: https://app.snowflake.com/marketplace/listing/GZT0Z4CM1E9J2/carto-carto-academy-data-for-tutorials
SELECT TO_GEOGRAPHY(PY_CLOSESTPOINT(
         'MULTIPOLYGON(((-108.72070312499997 34.99400375757577,-100.01953124999997 46.58906908309183,-90.79101562499996 34.92197103616377,-108.72070312499997 34.99400375757577),(-100.10742187499997 41.47566020027821,-102.91992187499996 37.61423141542416,-96.85546874999996 37.54457732085582,-100.10742187499997 41.47566020027821)),((-85.16601562499999 34.84987503195417,-80.771484375 28.497660832963476,-76.904296875 34.92197103616377,-85.16601562499999 34.84987503195417)))',
         ST_ASWKT(GEOM))) AS CLOSEST_POINT
FROM CARTO_ACADEMY__DATA_FOR_TUTORIALS.CARTO.CELL_TOWERS_WORLDWIDE;


