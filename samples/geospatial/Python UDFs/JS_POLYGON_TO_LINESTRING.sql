-- Converts the GEOGRAPHY polygon into linestring

CREATE OR REPLACE FUNCTION ST_POLYGON_TO_LINESTRING(G GEOGRAPHY)
RETURNS OBJECT
LANGUAGE JAVASCRIPT
AS $$
    if (G.type !== "Polygon") {
        throw "Input must be a Polygon";
    }

    // Get the first (outer) ring of the polygon
    let outerRing = G.coordinates[0];
    let lineString = {
        "type": "LineString",
        "coordinates": outerRing
    };

    return lineString;
$$;


-- How to use:
SELECT ST_POLYGON_TO_LINESTRING(TO_GEOGRAPHY('{"type":"Polygon","coordinates":[[[-180,-72],[180,-72],[180,72],[-180,72],[-180,-72]]]}')) as poly_oriented_area;

SELECT st_area(ST_MAKEPOLYGONORIENTED(TO_GEOGRAPHY(ST_POLYGON_TO_LINESTRING(TO_GEOGRAPHY('{"type":"Polygon","coordinates":[[[-180,-72],[180,-72],[180,72],[-180,72],[-180,-72]]]}'))))) as poly_oriented_area, 
st_area(TO_GEOGRAPHY('{"type":"Polygon","coordinates":[[[-180,-72],[180,-72],[180,72],[-180,72],[-180,-72]]]}')) as poly_area;


-- Another option that takes VARIANT as an input
CREATE OR REPLACE FUNCTION ST_POLYGON_TO_LINESTRING(G OBJECT)
RETURNS OBJECT
LANGUAGE JAVASCRIPT
AS $$
    if (G.type !== "Polygon") {
        throw "Input must be a Polygon";
    }

    // Get the first (outer) ring of the polygon
    let outerRing = G.coordinates[0];
    let lineString = {
        "type": "LineString",
        "coordinates": outerRing
    };

    return lineString;
$$;

-- How to use:
SELECT st_area(ST_MAKEPOLYGONORIENTED(TO_GEOGRAPHY(ST_POLYGON_TO_LINESTRING('{"type":"Polygon","coordinates":[[[-180,-72],[180,-72],[180,72],[-180,72],[-180,-72]]]}'::VARIANT))));