-- ST_CLUSTERDBSCAN: density-based spatial clustering (DBSCAN) for point data.
--
-- DBSCAN groups points that are packed closely together (many nearby neighbors)
-- and marks points in low-density regions as noise. Unlike k-means it does not
-- require the number of clusters up front and it detects outliers. This is a
-- pure-JavaScript implementation (no external libraries), wrapped by a SQL table
-- function so the result geometry is returned as native GEOGRAPHY.
--
-- Two objects are created:
--   1. _CLUSTERDBSCAN   scalar JavaScript UDF - runs the DBSCAN algorithm and
--                       returns an ARRAY of per-point objects.
--   2. ST_CLUSTERDBSCAN SQL table function (UDTF) - flattens that array and
--                       casts the geometry back to GEOGRAPHY, returning one row
--                       per input point. (Only SQL UDTFs may return GEOGRAPHY;
--                       JavaScript/Python/Java UDTFs cannot.)
--
-- Distance is the haversine great-circle distance in METERS, so `eps` is a real
-- ground distance rather than a raw lon/lat epsilon.
--
-- Arguments (ST_CLUSTERDBSCAN):
--   ids        ARRAY  parallel array of row identifiers (to join results back)
--   geojsons   ARRAY  parallel array of GeoJSON Point strings. From a GEOGRAPHY
--                     column build it with
--                     ARRAY_AGG(TO_VARCHAR(ST_ASGEOJSON(geog))) - note that
--                     ARRAY_AGG(GEOGRAPHY) is not allowed, so aggregate the
--                     GeoJSON text form.
--   eps        FLOAT  neighborhood radius in meters
--   min_points FLOAT  minimum points (incl. the point itself) to form a dense
--                     region / qualify a point as a cluster core
--
-- Returns a table:
--   id         VARCHAR    the passed-through identifier
--   geom       GEOGRAPHY  the point (ST_X / ST_Y / ST_DISTANCE etc. work on it)
--   cluster_id INT        cluster assignment 1..N, or NULL for noise
--   point_type VARCHAR    'core' | 'edge' | 'noise' (standard DBSCAN roles)

CREATE OR REPLACE FUNCTION _CLUSTERDBSCAN(
  ids ARRAY, geojsons ARRAY, eps DOUBLE, min_points DOUBLE)
RETURNS ARRAY
LANGUAGE JAVASCRIPT
IMMUTABLE
AS $$
    if (!GEOJSONS || EPS == null || MIN_POINTS == null) { return []; }
    const eps = Number(EPS);
    const minPoints = Number(MIN_POINTS);

    const geoms = GEOJSONS;
    const ids = IDS || [];
    const n = geoms.length;
    const coords = new Array(n);
    for (let i = 0; i < n; i++) {
        const g = (typeof geoms[i] === 'string') ? JSON.parse(geoms[i]) : geoms[i];
        const geom = (g && g.type === 'Feature') ? g.geometry : g;
        coords[i] = geom.coordinates; // [lon, lat]
    }

    // Haversine distance in meters (Earth radius = 6371008.8 m).
    const R = 6371008.8;
    const toRad = Math.PI / 180;
    function dist(a, b) {
        const dLat = (b[1]-a[1])*toRad, dLon = (b[0]-a[0])*toRad;
        const lat1 = a[1]*toRad, lat2 = b[1]*toRad;
        const h = Math.sin(dLat/2)**2 + Math.sin(dLon/2)**2*Math.cos(lat1)*Math.cos(lat2);
        return 2*R*Math.asin(Math.sqrt(h));
    }
    // Neighbors of point i within eps (includes i itself). O(n^2) overall.
    function regionQuery(i) {
        const out = [];
        for (let j = 0; j < n; j++) if (dist(coords[i], coords[j]) <= eps) out.push(j);
        return out;
    }

    // DBSCAN (Ester, Kriegel, Sander, Xu, 1996).
    const UNVISITED = 0, NOISE = -1;
    const label = new Array(n).fill(UNVISITED);
    const isCore = new Array(n).fill(false);
    let clusterId = 0;
    for (let i = 0; i < n; i++) {
        if (label[i] !== UNVISITED) continue;
        const neighbors = regionQuery(i);
        if (neighbors.length < minPoints) { label[i] = NOISE; continue; }
        clusterId++; label[i] = clusterId; isCore[i] = true;
        const seeds = neighbors.filter(x => x !== i);
        for (let s = 0; s < seeds.length; s++) {
            const q = seeds[s];
            if (label[q] === NOISE) label[q] = clusterId;   // border (edge) point
            if (label[q] !== UNVISITED) continue;
            label[q] = clusterId;
            const qn = regionQuery(q);
            if (qn.length >= minPoints) { isCore[q] = true; for (let k=0;k<qn.length;k++) seeds.push(qn[k]); }
        }
    }

    const result = new Array(n);
    for (let i = 0; i < n; i++) {
        const type = (label[i] === NOISE) ? 'noise' : (isCore[i] ? 'core' : 'edge');
        result[i] = {
            id: (i < ids.length) ? ids[i] : i,
            cluster: (label[i] === NOISE) ? null : label[i],
            point_type: type,
            geom: (typeof geoms[i] === 'string') ? geoms[i] : JSON.stringify(geoms[i])
        };
    }
    return result;
$$;


CREATE OR REPLACE FUNCTION ST_CLUSTERDBSCAN(
  ids ARRAY, geojsons ARRAY, eps FLOAT, min_points FLOAT)
RETURNS TABLE (id VARCHAR, geom GEOGRAPHY, cluster_id INT, point_type VARCHAR)
AS
$$
  SELECT
    f.value:id::VARCHAR                 AS id,
    TO_GEOGRAPHY(f.value:geom::VARCHAR) AS geom,
    f.value:cluster::INT                AS cluster_id,
    f.value:point_type::VARCHAR         AS point_type
  FROM TABLE(FLATTEN(input => _CLUSTERDBSCAN(ids, geojsons, eps, min_points))) f
$$;



---- How to call the function
--
-- Calling notes:
--   * Aggregate the GEOGRAPHY column as GeoJSON text with
--     ARRAY_AGG(TO_VARCHAR(ST_ASGEOJSON(<geog column>))); ARRAY_AGG(GEOGRAPHY)
--     is not supported.
--   * Pass the arrays as NON-CORRELATED scalar subqueries (as below). A lateral
--     join such as  FROM src, TABLE(ST_CLUSTERDBSCAN(src.ids, ...))  fails with
--     "Unsupported subquery type cannot be evaluated".
--   * Cast the eps / min_points literals ::FLOAT (NUMBER will not bind).

-- Simple inline example: two dense clusters plus one noise point.
SELECT t.id, ST_ASWKT(t.geom) AS geom, t.cluster_id, t.point_type
FROM TABLE(ST_CLUSTERDBSCAN(
       ARRAY_CONSTRUCT('a','b','c','d','e','f','z'),
       ARRAY_CONSTRUCT(
         '{"type":"Point","coordinates":[0,0]}',
         '{"type":"Point","coordinates":[0,0.0009]}',
         '{"type":"Point","coordinates":[0.0009,0]}',
         '{"type":"Point","coordinates":[10,10]}',
         '{"type":"Point","coordinates":[10,10.0009]}',
         '{"type":"Point","coordinates":[10.0009,10]}',
         '{"type":"Point","coordinates":[50,50]}'
       ),
       200::FLOAT,   -- eps = 200 m
       3::FLOAT))    -- min_points = 3
     AS t
ORDER BY t.cluster_id NULLS LAST, t.id;

-- Example on a dataset from the CARTO Academy listing:
-- https://app.snowflake.com/marketplace/listing/GZT0Z4CM1E9J2/carto-carto-academy-data-for-tutorials
-- Cluster blue whale Argos tracking points (15,545 points) into aggregation
-- areas. eps = 25 km, min_points = 10 finds foraging hotspots and marks sparse
-- transit pings as noise. The densest cluster centers near the Santa Barbara
-- Channel / Point Conception, a well-known blue whale foraging area.
SELECT
  t.cluster_id,
  COUNT(*)                       AS n_points,
  COUNT_IF(t.point_type='core')  AS core_points,
  COUNT_IF(t.point_type='edge')  AS edge_points,
  ROUND(AVG(ST_Y(t.geom)), 4)    AS centroid_lat,
  ROUND(AVG(ST_X(t.geom)), 4)    AS centroid_lon
FROM TABLE(ST_CLUSTERDBSCAN(
       (SELECT ARRAY_AGG(EVENT_ID::VARCHAR)
          FROM CARTO_ACADEMY__DATA_FOR_TUTORIALS.CARTO.BLUE_WHALES_POINT
          WHERE GEOM IS NOT NULL),
       (SELECT ARRAY_AGG(TO_VARCHAR(ST_ASGEOJSON(GEOM)))
          FROM CARTO_ACADEMY__DATA_FOR_TUTORIALS.CARTO.BLUE_WHALES_POINT
          WHERE GEOM IS NOT NULL),
       25000::FLOAT,   -- eps = 25 km
       10::FLOAT))     -- min_points = 10
     AS t
WHERE t.cluster_id IS NOT NULL
GROUP BY t.cluster_id
ORDER BY n_points DESC;
