-- Returns the boundary polygon of a tile given its zoom level and its X and Y indices.

CREATE OR REPLACE FUNCTION JS_TILEENVELOPE(zoomLevel REAL, xTile REAL, yTile REAL)
  RETURNS GEOGRAPHY
  LANGUAGE JAVASCRIPT
  AS $$
      function tile2long(x, z)  {
      return (x / Math.pow(2, z) * 360 - 180);
    }

    function tile2lat(y, z) {
      const n = Math.PI - 2 * Math.PI * y / Math.pow(2, z);
      return (180 / Math.PI * Math.atan(0.5 * (Math.exp(n) - Math.exp(-n))));
    }

      minLon = tile2long(XTILE, ZOOMLEVEL);
      minLat = tile2lat(YTILE + 1, ZOOMLEVEL);
      maxLon = tile2long(XTILE + 1, ZOOMLEVEL);
      maxLat = tile2lat(YTILE, ZOOMLEVEL);

      return {
        type: "Polygon",
        coordinates: [
          [
            [minLon, minLat],
            [minLon, maxLat],
            [maxLon, maxLat],
            [maxLon, minLat],
            [minLon, minLat]
          ]
        ]
      };
$$;



SELECT ST_TILEENVELOPE(7, 6, 9);

-- Invered function,  to convert longitude and latitude to their corresponding x and y tile coordinates, respectively, for the given zoom level.


CREATE OR REPLACE FUNCTION JS_LNGLAT2TILEID(longitude FLOAT, latitude FLOAT, zoomLevel REAL)
  RETURNS OBJECT
  LANGUAGE JAVASCRIPT
  AS
  $$
    function long2tile(lon, z) {
      return Math.floor((lon + 180) / 360 * Math.pow(2, z));
    }

    function lat2tile(lat, z) {
      return Math.floor((1 - Math.log(Math.tan(lat * Math.PI / 180) + 1 / Math.cos(lat * Math.PI / 180)) / Math.PI) / 2 * Math.pow(2, z));
    }
      
      const xTile = long2tile(LONGITUDE, ZOOMLEVEL);
      const yTile = lat2tile(LATITUDE, ZOOMLEVEL);

      return {
        xTile: xTile,
        yTile: yTile
      };
  $$;

SELECT JS_LATLNG2TILEID(-161.71875, 82.116821929, 7)