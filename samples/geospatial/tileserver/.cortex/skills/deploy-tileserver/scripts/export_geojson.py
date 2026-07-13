#!/usr/bin/env python3
"""Export Overture countries+regions as newline-delimited GeoJSON for tippecanoe.

Reads from core Snowflake. Writes a GeoJSONL file (one GeoJSON Feature per line)
that tippecanoe bakes into PMTiles - no per-tile DB queries needed.

Portable: Snowflake connection is the SNOWFLAKE_CONNECTION env var (active `snow`
CLI connection; falls back to the default connection). SOURCE_TABLE and optional
COUNTRY narrow the export. Nothing account-specific is hardcoded.
"""
import json
import os
import sys

import snowflake.connector

OUT = os.environ.get("OUT", "viewer/features_full.geojsonl")
SOURCE_TABLE = os.environ.get("SOURCE_TABLE", "OVERTURE_MAPS__DIVISIONS.CARTO.DIVISION_AREA")
COUNTRY = os.environ.get("COUNTRY", "")  # e.g. 'US' to filter to one country

_country_clause = f"AND COUNTRY = '{COUNTRY}'" if COUNTRY else ""

# Per-subtype minimum zoom so tiles stay light: coarse levels show only when
# zoomed out, fine levels appear on zoom-in. tippecanoe honors feature.tippecanoe.minzoom.
SUBTYPE_MINZOOM = {
    "country": 0,
    "dependency": 0,
    "region": 3,
    "county": 5,
    "localadmin": 6,
    "macrohood": 7,
    "locality": 7,
    "neighborhood": 9,
    "microhood": 10,
}

QUERY = f"""
SELECT
  DIVISION_ID,
  NAMES:primary::string AS name,
  SUBTYPE,
  COUNTRY,
  ADMIN_LEVEL,
  ST_ASGEOJSON(GEOMETRY) AS gj
FROM {SOURCE_TABLE}
WHERE GEOMETRY IS NOT NULL {_country_clause}
"""


def sf_connect():
    name = os.environ.get("SNOWFLAKE_CONNECTION") or os.environ.get("SNOWFLAKE_DEFAULT_CONNECTION_NAME")
    if name:
        return snowflake.connector.connect(connection_name=name)
    return snowflake.connector.connect()


def main() -> int:
    conn = sf_connect()
    cur = conn.cursor()
    cur.execute(QUERY)
    n = 0
    os.makedirs(os.path.dirname(OUT) or ".", exist_ok=True)
    with open(OUT, "w") as f:
        for division_id, name, subtype, country, admin_level, gj in cur:
            if not gj:
                continue
            feat = {
                "type": "Feature",
                "geometry": json.loads(gj),
                "tippecanoe": {"minzoom": SUBTYPE_MINZOOM.get(subtype, 6)},
                "properties": {
                    "division_id": division_id,
                    "name": name,
                    "subtype": subtype,
                    "country": country,
                    "admin_level": int(admin_level) if admin_level is not None else None,
                },
            }
            f.write(json.dumps(feat) + "\n")
            n += 1
            if n % 500 == 0:
                print(f"  wrote {n} ...", flush=True)
    conn.close()
    print(f"Wrote {OUT}: {n} features, {os.path.getsize(OUT)} bytes")
    return 0


if __name__ == "__main__":
    sys.exit(main())
