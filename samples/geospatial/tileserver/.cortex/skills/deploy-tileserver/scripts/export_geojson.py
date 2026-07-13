#!/usr/bin/env python3
"""Export the owned SOURCE_FEATURES snapshot as newline-delimited GeoJSON for tippecanoe.

Reads the fixed-schema snapshot built by build_source_snapshot.py
(TILESERVER.CORE.SOURCE_FEATURES by default; override with SNAPSHOT_TABLE). Writes
a GeoJSONL file (one GeoJSON Feature per line) that tippecanoe bakes into PMTiles -
no per-tile DB queries needed.

Portable: Snowflake connection is the SNOWFLAKE_CONNECTION env var (active `snow`
CLI connection; falls back to the default connection). Reading the owned snapshot
(not the raw share) keeps this dataset-agnostic and immune to a mid-run share
lapse; the snapshot is already COUNTRY-scoped.
"""
import json
import os
import sys

import snowflake.connector

OUT = os.environ.get("OUT", "viewer/features_full.geojsonl")
SNAPSHOT_TABLE = os.environ.get("SNAPSHOT_TABLE", "TILESERVER.CORE.SOURCE_FEATURES")

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
SELECT division_id, name, subtype, country, admin_level, gj
FROM {SNAPSHOT_TABLE}
WHERE gj IS NOT NULL
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
