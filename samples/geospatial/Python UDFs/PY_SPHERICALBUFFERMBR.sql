-- Returns an MBR that represents the minimum and maximum latitude and longitude
-- of all points within a given distance from a point. The earth is modeled as a sphere.

CREATE OR REPLACE FUNCTION py_sphericalbuffermbr(longitude float, latitude float, radius_meters integer)
returns geography
language python
runtime_version = 3.8
packages = ('shapely', 'pyproj')
handler = 'udf'
AS $$
from shapely.geometry import box, mapping
import pyproj
def udf(longitude, latitude, radius_meters):
    geod = pyproj.Geod(ellps="WGS84")
    n_corner_long, n_corner_lat, _ = geod.fwd(longitude, latitude, 0, radius_meters)
    s_corner_long, s_corner_lat, _ = geod.fwd(longitude, latitude, 180, radius_meters)
    w_corner_long, w_corner_lat, _ = geod.fwd(longitude, latitude, -90, radius_meters)
    e_corner_long, e_corner_lat, _ = geod.fwd(longitude, latitude, 90, radius_meters)    
    mbr = box(w_corner_long, n_corner_lat, e_corner_long, s_corner_lat)
    return mapping(mbr)
	
$$;

-- How to call it
select py_sphericalbuffermbr(st_x(geo), st_y(geo), radius) as geog_reduced;

-- JS version, faster than Python one, but doesn't handle antimeridian properly

CREATE OR REPLACE FUNCTION js_sphericalbuffermbr(lng real, lat real, radius real)
  RETURNS geography
  LANGUAGE JAVASCRIPT
AS
$$
const earthR = 6371010;
const dlat = (180 / Math.PI) * RADIUS / earthR;
const dlng = dlat / Math.cos(LAT * Math.PI / 180);
return {
    "type": "Polygon",
    "coordinates": [[
        [LNG - dlng, LAT - dlat],
        [LNG - dlng, LAT + dlat],
        [LNG + dlng, LAT + dlat],
        [LNG + dlng, LAT - dlat],
        [LNG - dlng, LAT - dlat]
    ]]
};
$$;

-- How to call it
select py_sphericalbuffermbr(st_x(geo), st_y(geo), radius) as geog_reduced;