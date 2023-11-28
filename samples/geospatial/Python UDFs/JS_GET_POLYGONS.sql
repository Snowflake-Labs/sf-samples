-- For given multipolygon returns the polygons

CREATE OR REPLACE FUNCTION ST_GETPOLYGONS(G OBJECT)
RETURNS TABLE (POLYGON OBJECT)
LANGUAGE JAVASCRIPT
AS '
{
processRow: function split_multipolygon(row, rowWriter, context){
    let geojson = row.G;
    let polygons = [];
    if (geojson.type === "Polygon") {
        polygons.push(geojson.coordinates);
    } else if (geojson.type === "MultiPolygon") {
        for (let i = 0; i < geojson.coordinates.length; i++) {
            polygons.push(geojson.coordinates[i]);
        }
    }
    for (let i = 0; i < polygons.length; i++) {
        rowWriter.writeRow({POLYGON: {
                "type" : "Polygon",
                "coordinates": polygons[i]
            }
        });
    }
}
}
';


-- How to use:
SELECT to_geography(polygon) as geog 
FROM TABLE (ST_GETPOLYGONS(st_asgeojson(to_geography('MULTIPOLYGON(((-108.72070312499997 34.99400375757577,
            -100.01953124999997 46.58906908309183,-90.79101562499996 34.92197103616377,-108.72070312499997 34.99400375757577),
            (-100.10742187499997 41.47566020027821,-102.91992187499996 37.61423141542416,-96.85546874999996 37.54457732085582,
            -100.10742187499997 41.47566020027821)),((-85.16601562499999 34.84987503195417,-80.771484375 28.497660832963476,
            -76.904296875 34.92197103616377,-85.16601562499999 34.84987503195417)))'))))