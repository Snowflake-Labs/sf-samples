CREATE OR REPLACE FUNCTION ST_CLOSESTPOINT(
    geomA_wkt STRING,  -- The geometry on which we'll find the closest point
    geomB_wkt STRING   -- The "other" geometry, used for measuring distance
)
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$
// Access UDF arguments from Snowflake:
var wktA = arguments[0];
var wktB = arguments[1];

/******************************************************************************
 * 1) Parsing: Convert WKT strings to internal JavaScript objects.
 *    We'll handle POINT, LINESTRING, POLYGON, MULTI*, GEOMETRYCOLLECTION
 ******************************************************************************/

function parseWKTtoObject(wkt) {
  if (!wkt) {
    throw new Error("Empty WKT string");
  }
  wkt = wkt.trim();

  // Top-level geometry type
  let match = wkt.match(/^(\w+)\s*\(([\s\S]*)\)$/i);
  if (!match) {
    throw new Error("Invalid WKT (top-level parse fail): " + wkt);
  }

  let geomType = match[1].toUpperCase();
  let inner    = match[2].trim();

  switch (geomType) {
    case "POINT":
      return { type: "POINT", coords: parseCoordinateList(inner) }; 
      // coords => [[x,y]]

    case "LINESTRING":
      return { type: "LINESTRING", coords: parseCoordinateList(inner) };

    case "POLYGON":
      // e.g. "POLYGON((x1 y1, x2 y2,...),(innerRing),...)"
      return { type: "POLYGON", coords: parseMultiList(inner) };

    case "MULTIPOINT":
      // e.g. MULTIPOINT((10 40),(40 30)) or MULTIPOINT(10 40, 40 30)
      // We'll assume the fully parenthesized version
      return { type: "MULTIPOINT", coords: parseMultiList(inner) };

    case "MULTILINESTRING":
      return { type: "MULTILINESTRING", coords: parseMultiList(inner) };

    case "MULTIPOLYGON":
      return { type: "MULTIPOLYGON", coords: parsePolyList(inner) };

    case "GEOMETRYCOLLECTION":
      return { type: "GEOMETRYCOLLECTION", geometries: parseGeometryCollection(inner) };

    default:
      throw new Error("Unsupported geometry type: " + geomType);
  }
}

// Parse a list of "x y" pairs, e.g. "10 40, 40 30" -> [[10,40],[40,30]]
function parseCoordinateList(text) {
  return text.split(/\s*,\s*/).map(pair => {
    let coords = pair.trim().split(/\s+/).map(parseFloat);
    if (coords.length < 2) {
      throw new Error("Invalid coordinate: " + pair);
    }
    return coords.slice(0,2); // ignore Z/M
  });
}

// parseMultiList handles pairs of parentheses, e.g. LINESTRING((...),(...)) or POLYGON outer rings
function parseMultiList(text) {
  // Each group is ( x1 y1, x2 y2, ... )
  // We'll collect them into an array of arrays
  let results = [];
  let bracketDepth = 0;
  let startIndex = 0;

  for (let i=0; i<text.length; i++) {
    let c = text[i];
    if (c === '(') {
      bracketDepth++;
      if (bracketDepth === 1) {
        startIndex = i+1; 
      }
    } else if (c === ')') {
      bracketDepth--;
      if (bracketDepth === 0) {
        let chunk = text.substring(startIndex, i).trim();
        results.push(parseCoordinateList(chunk));
      }
    }
  }
  return results; // array of [ [x,y], [x,y], ... ]
}

// parsePolyList handles MULTIPOLYGON's outer parentheses
function parsePolyList(text) {
  let polygons = [];
  let bracketDepth = 0;
  let startIndex = 0;
  let inPolygon = false;

  for (let i=0; i<text.length; i++) {
    let c = text[i];
    if (c === '(') {
      bracketDepth++;
      if (!inPolygon) {
        inPolygon = true;
        startIndex = i+1;
      }
    } else if (c === ')') {
      bracketDepth--;
      if (bracketDepth === 0 && inPolygon) {
        let chunk = text.substring(startIndex, i).trim();
        polygons.push(parseMultiList(chunk));
        inPolygon = false;
      }
    }
  }
  return polygons;
}

// parseGeometryCollection: naive approach splitting top-level commas
function parseGeometryCollection(text) {
  let geoms = [];
  let bracketDepth = 0;
  let startPos = 0;
  for (let i=0; i<text.length; i++) {
    let c = text[i];
    if (c === '(') bracketDepth++;
    else if (c === ')') bracketDepth--;
    else if (c === ',' && bracketDepth === 0) {
      let chunk = text.substring(startPos, i).trim();
      geoms.push(parseWKTtoObject(chunk));
      startPos = i+1;
    }
  }
  let final = text.substring(startPos).trim();
  if (final) {
    geoms.push(parseWKTtoObject(final));
  }
  return geoms;
}

/******************************************************************************
 * 2) Decomposing geometries into simpler "components" so we can find a point 
 *    on A. For polygons, we use the exterior ring. For multi-*, we gather 
 *    multiple sub-components. 
 * 
 *    We'll define "decomposeGeometryForClosest(geomAObj)" = the geometry 
 *    from which we will *return* a point. i.e. the shape that will yield 
 *    the final "closest point."
 * 
 *    We'll define "decomposeGeometryForReference(geomBObj)" = the geometry 
 *    used for measuring distance. 
 ******************************************************************************/

function decomposeGeometryForClosest(geomObj) {
  // We will return an array of "components" describing 
  // lines or points within A. e.g.:
  //   { type: 'POINT', coords: [x, y] }
  //   { type: 'LINE', coords: [[x1,y1], [x2,y2], ...] }
  // For polygons, we treat the exterior ring as a line. 
  // For multi*, we gather sub-objects. 
  // This is so we can pick a final point *on A*.
  let results = [];

  switch(geomObj.type) {
    case 'POINT':
      results.push({ type: 'POINT', coords: geomObj.coords[0] });
      break;

    case 'LINESTRING':
      results.push({ type: 'LINE', coords: geomObj.coords });
      break;

    case 'POLYGON':
      // Use exterior ring only (the first ring).
      if (geomObj.coords.length > 0) {
        results.push({ type: 'LINE', coords: geomObj.coords[0] });
      }
      break;

    case 'MULTIPOINT':
      // e.g. coords => [ [[x1,y1]], [[x2,y2]] ]
      for (let arr of geomObj.coords) {
        if (arr.length > 0) {
          results.push({ type: 'POINT', coords: arr[0] });
        }
      }
      break;

    case 'MULTILINESTRING':
      for (let line of geomObj.coords) {
        results.push({ type: 'LINE', coords: line });
      }
      break;

    case 'MULTIPOLYGON':
      // coords => array of polygons, each polygon => array of rings
      for (let poly of geomObj.coords) {
        if (poly.length > 0) {
          // the first ring is exterior
          results.push({ type: 'LINE', coords: poly[0] });
        }
      }
      break;

    case 'GEOMETRYCOLLECTION':
      for (let g of geomObj.geometries) {
        results.push(...decomposeGeometryForClosest(g));
      }
      break;

    default:
      throw new Error("Unsupported geometry type for 'closest': " + geomObj.type);
  }

  return results;
}

function decomposeGeometryForReference(geomObj) {
  // The "other" geometry, from which we measure distance. 
  // We'll also break it into lines/points for simpler distance checks.
  let results = [];

  switch(geomObj.type) {
    case 'POINT':
      results.push({ type: 'POINT', coords: geomObj.coords[0] });
      break;

    case 'LINESTRING':
      results.push({ type: 'LINE', coords: geomObj.coords });
      break;

    case 'POLYGON':
      if (geomObj.coords.length > 0) {
        results.push({ type: 'LINE', coords: geomObj.coords[0] });
      }
      break;

    case 'MULTIPOINT':
      for (let arr of geomObj.coords) {
        if (arr.length > 0) {
          results.push({ type: 'POINT', coords: arr[0] });
        }
      }
      break;

    case 'MULTILINESTRING':
      for (let line of geomObj.coords) {
        results.push({ type: 'LINE', coords: line });
      }
      break;

    case 'MULTIPOLYGON':
      for (let poly of geomObj.coords) {
        if (poly.length > 0) {
          results.push({ type: 'LINE', coords: poly[0] });
        }
      }
      break;

    case 'GEOMETRYCOLLECTION':
      for (let g of geomObj.geometries) {
        results.push(...decomposeGeometryForReference(g));
      }
      break;

    default:
      throw new Error("Unsupported geometry type for 'reference': " + geomObj.type);
  }

  return results;
}

/******************************************************************************
 * 3) Distance / geometry functions
 ******************************************************************************/

function dist2(p1, p2) {
  let dx = p1[0] - p2[0];
  let dy = p1[1] - p2[1];
  return dx*dx + dy*dy;
}

/**
 * closestPointOnSegment(px,py, ax,ay, bx,by):
 * Returns the point on segment AB that is closest to P=(px,py).
 */
function closestPointOnSegment(px, py, ax, ay, bx, by) {
  let ABx = bx - ax;
  let ABy = by - ay;
  let APx = px - ax;
  let APy = py - ay;

  let dot = ABx*APx + ABy*APy;
  let len2 = ABx*ABx + ABy*ABy;
  if (len2 === 0) {
    return [ax, ay];
  }
  let t = dot / len2;
  if (t < 0) t = 0;
  if (t > 1) t = 1;
  return [ ax + t*ABx, ay + t*ABy ];
}

/******************************************************************************
 * 4) "Closest point on A to B" logic:
 *    - We'll decompose A into sub-components (points or lines).
 *    - For each sub-component, we figure out the place on it that yields
 *      the minimal distance to *any* sub-component of B.
 *    - Then pick the overall best among all sub-components in A.
 ******************************************************************************/

function findClosestPointOnAtoB(compA, subB) {
  // compA: { type: 'POINT' or 'LINE', coords: ... }
  // subB: an array of sub-components from B
  // We'll find the single [x,y] on compA that yields the minimal distance to B.

  let bestDist2 = Number.POSITIVE_INFINITY;
  let bestPt = null;

  if (compA.type === 'POINT') {
    // The only "point on A" is itself
    let candidate = compA.coords;
    // measure distance to each subcomponent in B
    let d2 = distanceFromB(subB, candidate);
    if (d2 < bestDist2) {
      bestDist2 = d2;
      bestPt = candidate;
    }
    return bestPt;
  }
  else if (compA.type === 'LINE') {
    // We can have multiple segments
    // We'll look for the location on this line that yields minimal distance to B
    // We'll do a brute-force approach: 
    //   for each segment in A, for each subcomponent in B, check endpoints or a few midpoints
    // This is not fully robust, but a decent approximation.
    let line = compA.coords;
    // We'll discretize each segment or do endpoint checks vs. subB
    // More robust approach: segment-segment formula. We'll keep it simpler.

    // We'll keep track of the best param t along each segment if we do point-segment tests
    for (let i=0; i<line.length-1; i++) {
      let [ax, ay] = line[i];
      let [bx, by] = line[i+1];

      // We'll test a few candidate points on AB: 
      //   - A itself
      //   - B itself
      //   - Possibly multiple subdivisions or direct segment-segment formula
      // For brevity, let's do a subfunction that tries "project each subB onto AB" and also "test endpoints"

      // Test endpoints A and B directly
      let d2A = distanceFromB(subB, [ax, ay]);
      if (d2A < bestDist2) {
        bestDist2 = d2A;
        bestPt = [ax, ay];
      }
      let d2B_ = distanceFromB(subB, [bx, by]);
      if (d2B_ < bestDist2) {
        bestDist2 = d2B_;
        bestPt = [bx, by];
      }

      // For each sub-component in B, if it's a point, project that point onto AB
      // if it's a line, project that line's endpoints onto AB, etc.
      for (let comp of subB) {
        if (comp.type === 'POINT') {
          let [px, py] = comp.coords;
          let cpt = closestPointOnSegment(px, py, ax, ay, bx, by);
          let d2C = distanceFromB(subB, cpt); 
          if (d2C < bestDist2) {
            bestDist2 = d2C;
            bestPt = cpt;
          }
        }
        else if (comp.type === 'LINE') {
          // line vs line segment => we can do segment-segment approach 
          // or just check endpoints + a few projected points. We'll do endpoints:
          let coordsB = comp.coords;
          for (let j=0; j<coordsB.length-1; j++) {
            let [bx1, by1] = coordsB[j];
            let [bx2, by2] = coordsB[j+1];
            // Project that segment's endpoints onto A's segment
            let cpt1 = closestPointOnSegment(bx1, by1, ax, ay, bx, by);
            let d2_1 = distanceFromB(subB, cpt1);
            if (d2_1 < bestDist2) { bestDist2 = d2_1; bestPt = cpt1; }

            let cpt2 = closestPointOnSegment(bx2, by2, ax, ay, bx, by);
            let d2_2 = distanceFromB(subB, cpt2);
            if (d2_2 < bestDist2) { bestDist2 = d2_2; bestPt = cpt2; }
          }
        }
      }
    }
    return bestPt;
  }
  else {
    throw new Error("Unsupported component type in A: " + compA.type);
  }
}

/**
 * distanceFromB(subB, pt): minimal distance^2 from `pt` to any subcomponent in B.
 */
function distanceFromB(subB, pt) {
  let minD2 = Number.POSITIVE_INFINITY;
  for (let comp of subB) {
    if (comp.type === 'POINT') {
      let d2_ = dist2(pt, comp.coords);
      if (d2_ < minD2) minD2 = d2_;
    } else if (comp.type === 'LINE') {
      // find closest approach from pt to each segment in comp
      let coords = comp.coords;
      for (let i=0; i<coords.length-1; i++) {
        let cpt = closestPointOnSegment(pt[0], pt[1], coords[i][0], coords[i][1], coords[i+1][0], coords[i+1][1]);
        let d2_ = dist2(pt, cpt);
        if (d2_ < minD2) minD2 = d2_;
      }
    }
  }
  return minD2;
}

/******************************************************************************
 * 5) Master function: "closest point on A to B"
 ******************************************************************************/

function closestPointOnAtoB(geomAObj, geomBObj) {
  // Decompose A into sub-components (where we can pick a final point).
  let subA = decomposeGeometryForClosest(geomAObj);
  // Decompose B into sub-components for measuring distance
  let subB = decomposeGeometryForReference(geomBObj);

  let overallBestDist2 = Number.POSITIVE_INFINITY;
  let overallBestPt = null;

  // For each sub-component in A, find that subcomponent's best point relative to B
  for (let compA of subA) {
    let cptA = findClosestPointOnAtoB(compA, subB);
    if (cptA) {
      // compute actual distance^2 to B
      let d2_ = distanceFromB(subB, cptA);
      if (d2_ < overallBestDist2) {
        overallBestDist2 = d2_;
        overallBestPt = cptA;
      }
    }
  }

  if (!overallBestPt) {
    throw new Error("No closest point found (unexpected).");
  }

  // Return WKT "POINT(x y)"
  return `POINT(${overallBestPt[0]} ${overallBestPt[1]})`;
}

// ------------------------------------------------------------------------------
// 6) Final UDF call
// ------------------------------------------------------------------------------
function runClosestPointAny(wktA, wktB) {
  let geomAObj = parseWKTtoObject(wktA);
  let geomBObj = parseWKTtoObject(wktB);
  return closestPointOnAtoB(geomAObj, geomBObj);
}

return runClosestPointAny(wktA, wktB);
$$;





---- How to call the function
SELECT ST_CLOSESTPOINT(
         'MULTIPOLYGON(((-108.72070312499997 34.99400375757577,-100.01953124999997 46.58906908309183,-90.79101562499996 34.92197103616377,-108.72070312499997 34.99400375757577),(-100.10742187499997 41.47566020027821,-102.91992187499996 37.61423141542416,-96.85546874999996 37.54457732085582,-100.10742187499997 41.47566020027821)),((-85.16601562499999 34.84987503195417,-80.771484375 28.497660832963476,-76.904296875 34.92197103616377,-85.16601562499999 34.84987503195417)))',
         'POLYGON((-120.06786346435545 42.017417046219435,-114.0413475036621 42.02978667407419,-114.02709960937499 36.15852843041614,-114.7697067260742 36.05645450949238,-114.68645095825194 35.0906979730151,-120.02511978149411 39.06744706095475,-120.06786346435545 42.017417046219435))'
       ) AS CLOSEST_POINT;

SELECT ST_CLOSESTPOINT(
         'MULTIPOLYGON(((-108.72070312499997 34.99400375757577,-100.01953124999997 46.58906908309183,-90.79101562499996 34.92197103616377,-108.72070312499997 34.99400375757577),(-100.10742187499997 41.47566020027821,-102.91992187499996 37.61423141542416,-96.85546874999996 37.54457732085582,-100.10742187499997 41.47566020027821)),((-85.16601562499999 34.84987503195417,-80.771484375 28.497660832963476,-76.904296875 34.92197103616377,-85.16601562499999 34.84987503195417)))',
         ST_ASWKT(POLYGON)) AS CLOSEST_POINT
FROM TEMP.PUBLIC.EA_H3_COVERAGE_VALID;