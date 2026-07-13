#!/usr/bin/env python3
"""Convert an MBTiles (SQLite) file to a real PMTiles v3 archive.

tippecanoe here emitted MBTiles even with a .pmtiles name, so convert it.
MBTiles tile_row is TMS (flipped); PMTiles uses XYZ tile ids. Tile data from
tippecanoe is gzipped MVT, so the header marks GZIP.
"""
import json
import os
import sqlite3
import sys

from pmtiles.writer import Writer
from pmtiles.tile import zxy_to_tileid, TileType, Compression

SRC = os.environ.get("SRC", "viewer/features_pmt.pmtiles")   # actually mbtiles
OUT = os.environ.get("OUT", "viewer/features_real.pmtiles")


def meta_get(cur, key, default=None):
    cur.execute("SELECT value FROM metadata WHERE name=?", (key,))
    r = cur.fetchone()
    return r[0] if r else default


def main() -> int:
    db = sqlite3.connect(SRC)
    cur = db.cursor()

    minzoom = int(meta_get(cur, "minzoom", "0"))
    maxzoom = int(meta_get(cur, "maxzoom", "7"))
    bounds = meta_get(cur, "bounds", "-180,-85.051129,180,83.336213")
    center = meta_get(cur, "center", "0,20,2")
    json_meta = meta_get(cur, "json", "{}")
    try:
        vlayers = json.loads(json_meta).get("vector_layers", [])
    except Exception:
        vlayers = []

    minlon, minlat, maxlon, maxlat = [float(v) for v in bounds.split(",")]
    cparts = center.split(",")
    clon, clat, cz = float(cparts[0]), float(cparts[1]), int(float(cparts[2]))
    e7 = lambda v: int(round(v * 1e7))

    cur.execute("SELECT zoom_level, tile_column, tile_row, tile_data FROM tiles")
    tiles = {}
    for z, x, row, data in cur:
        y = (2 ** z - 1) - row          # TMS -> XYZ
        tiles[zxy_to_tileid(z, x, y)] = bytes(data)

    print(f"Read {len(tiles)} tiles (z{minzoom}-{maxzoom}). Writing {OUT} ...", flush=True)
    with open(OUT, "wb") as f:
        w = Writer(f)
        for tid in sorted(tiles):
            w.write_tile(tid, tiles[tid])
        header = {
            "tile_type": TileType.MVT,
            "tile_compression": Compression.GZIP,
            "min_zoom": minzoom, "max_zoom": maxzoom,
            "min_lon_e7": e7(minlon), "min_lat_e7": e7(minlat),
            "max_lon_e7": e7(maxlon), "max_lat_e7": e7(maxlat),
            "center_zoom": cz, "center_lon_e7": e7(clon), "center_lat_e7": e7(clat),
        }
        metadata = {"name": meta_get(cur, "name", "Overture admin areas"),
                    "attribution": meta_get(cur, "attribution", "Overture Maps"),
                    "vector_layers": vlayers}
        w.finalize(header, metadata)
    db.close()
    print(f"Done. {OUT} = {os.path.getsize(OUT)} bytes")
    return 0


if __name__ == "__main__":
    sys.exit(main())
