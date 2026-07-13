#!/usr/bin/env python3
"""Bake public.features_mvt into a PMTiles archive (arm 2, direct/no-tippecanoe).

This is the alternate baker that queries the live features_mvt function tile by
tile. The default install path uses the tippecanoe pipeline (bake_pmtiles.sh),
which is faster for large datasets; keep this for small/dev bakes.

Uses a single, reused psycopg2 connection (with reconnect-on-failure) to be
gentle on the small Postgres instance's connection budget. Tiles are gzipped;
the header marks GZIP so MapLibre / Martin decompress transparently.

Portable: Postgres connection prefers PG_URL / DATABASE_URL (a full libpq URL),
else falls back to the libpq service named by PGSERVICE (default 'tileserver').
"""
import gzip
import os
import sys
import time

import psycopg2
from pmtiles.writer import Writer
from pmtiles.tile import zxy_to_tileid, TileType, Compression

MAXZOOM = int(os.environ.get("MAXZOOM", "6"))
OUT = os.environ.get("OUT", "viewer/features.pmtiles")

# data extent (lon/lat) from the sync step
MINLON, MINLAT, MAXLON, MAXLAT = -180.0, -85.0511289, 180.0, 83.3362128


def connect():
    url = os.environ.get("PG_URL") or os.environ.get("DATABASE_URL")
    last = None
    for attempt in range(6):
        try:
            if url:
                u = url if "sslmode=" in url else url + (("&" if "?" in url else "?") + "sslmode=require")
                c = psycopg2.connect(u, connect_timeout=15)
            else:
                c = psycopg2.connect(service=os.environ.get("PGSERVICE", "tileserver"), connect_timeout=15)
            c.autocommit = True
            return c
        except psycopg2.OperationalError as e:
            last = e
            time.sleep(min(2 ** attempt, 8))
    raise last


def fetch_zoom(conn_holder, z):
    """Fetch ALL non-empty tiles for a zoom in a single query (via generate_series),
    reconnecting and retrying the whole zoom on transient failure."""
    n = 2 ** z
    sql = (
        "SELECT x, y, features_mvt(%s, x, y) AS mvt "
        "FROM (SELECT g1.v AS x, g2.v AS y "
        "      FROM generate_series(0, %s) AS g1(v), generate_series(0, %s) AS g2(v)) c"
    )
    for attempt in range(5):
        try:
            out = []
            with conn_holder["conn"].cursor() as cur:
                cur.itersize = 2000
                cur.execute(sql, (z, n - 1, n - 1))
                for x, y, mvt in cur:
                    if mvt:
                        out.append((x, y, bytes(mvt)))
            return out
        except psycopg2.Error:
            try:
                conn_holder["conn"].close()
            except Exception:
                pass
            time.sleep(min(2 ** attempt, 8))
            conn_holder["conn"] = connect()
    return []


def main() -> int:
    e7 = lambda v: int(round(v * 1e7))
    conn_holder = {"conn": connect()}

    tiles = {}  # tileid -> gzipped mvt
    for z in range(MAXZOOM + 1):
        n = 2 ** z
        got = fetch_zoom(conn_holder, z)
        for x, y, data in got:
            tiles[zxy_to_tileid(z, x, y)] = gzip.compress(data, 6)
        print(f"  z{z}: kept {len(got)}/{n*n} tiles (running total {len(tiles)})", flush=True)

    print(f"{len(tiles)} non-empty tiles. Writing {OUT} ...", flush=True)
    os.makedirs(os.path.dirname(OUT) or ".", exist_ok=True)
    with open(OUT, "wb") as f:
        w = Writer(f)
        for tid in sorted(tiles.keys()):
            w.write_tile(tid, tiles[tid])
        header = {
            "tile_type": TileType.MVT,
            "tile_compression": Compression.GZIP,
            "min_zoom": 0,
            "max_zoom": MAXZOOM,
            "min_lon_e7": e7(MINLON), "min_lat_e7": e7(MINLAT),
            "max_lon_e7": e7(MAXLON), "max_lat_e7": e7(MAXLAT),
            "center_zoom": 2, "center_lon_e7": e7(0), "center_lat_e7": e7(20),
        }
        metadata = {
            "name": "Overture admin areas",
            "attribution": "Overture Maps",
            "vector_layers": [{
                "id": "features",
                "fields": {"name": "String", "subtype": "String",
                            "country": "String", "admin_level": "Number"},
                "minzoom": 0, "maxzoom": MAXZOOM,
            }],
        }
        w.finalize(header, metadata)

    print(f"Done. {OUT} = {os.path.getsize(OUT)} bytes", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
