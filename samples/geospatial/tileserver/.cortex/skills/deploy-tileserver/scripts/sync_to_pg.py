#!/usr/bin/env python3
"""ETL: Overture DIVISION_AREA (countries + regions) from Snowflake -> PostGIS.

Portable / account-agnostic:
  * Snowflake connection comes from the SNOWFLAKE_CONNECTION env var (the active
    `snow` CLI connection name). If unset, the default connection is used.
  * Source table comes from SOURCE_TABLE (default: the Overture divisions share);
    optional COUNTRY filter narrows the load.
  * Postgres side prefers the PG_URL / DATABASE_URL env (a full libpq URL); if
    absent it falls back to the libpq service named by PGSERVICE (default
    'tileserver'). No host, org, or account is hardcoded.
"""
import os
import sys

import snowflake.connector
import psycopg2
import psycopg2.extras

SOURCE_TABLE = os.environ.get("SOURCE_TABLE", "OVERTURE_MAPS__DIVISIONS.CARTO.DIVISION_AREA")
COUNTRY = os.environ.get("COUNTRY", "")  # e.g. 'US' to filter to one country

_country_clause = f"      AND COUNTRY = '{COUNTRY}'\n" if COUNTRY else ""

SF_QUERY = f"""
SELECT
  DIVISION_ID                         AS division_id,
  NAMES:primary::string               AS name,
  SUBTYPE                             AS subtype,
  COUNTRY                             AS country,
  ADMIN_LEVEL                         AS admin_level,
  ST_ASWKB(GEOMETRY)                  AS wkb
FROM {SOURCE_TABLE}
WHERE SUBTYPE IN ('country','region')
  AND GEOMETRY IS NOT NULL
{_country_clause}"""

DDL = [
    "DROP TABLE IF EXISTS public.features",
    """CREATE TABLE public.features (
         id           bigserial PRIMARY KEY,
         division_id  text,
         name         text,
         subtype      text,
         country      text,
         admin_level  int,
         geom         geometry(Geometry,4326)
       )""",
]

INSERT = "INSERT INTO public.features (division_id,name,subtype,country,admin_level,geom) VALUES %s"
TEMPLATE = "(%s,%s,%s,%s,%s,ST_MakeValid(ST_SetSRID(ST_GeomFromWKB(%s),4326)))"


def sf_connect():
    name = os.environ.get("SNOWFLAKE_CONNECTION") or os.environ.get("SNOWFLAKE_DEFAULT_CONNECTION_NAME")
    if name:
        return snowflake.connector.connect(connection_name=name)
    return snowflake.connector.connect()


def pg_connect():
    url = os.environ.get("PG_URL") or os.environ.get("DATABASE_URL")
    if url:
        if "sslmode=" not in url:
            url += ("&" if "?" in url else "?") + "sslmode=require"
        return psycopg2.connect(url, connect_timeout=20)
    service = os.environ.get("PGSERVICE", "tileserver")
    return psycopg2.connect(service=service, connect_timeout=20)


def main() -> int:
    print(f"Connecting to Snowflake ({os.environ.get('SNOWFLAKE_CONNECTION', 'default')}) ...", flush=True)
    sf = sf_connect()
    scur = sf.cursor()
    scur.execute(SF_QUERY)

    print("Connecting to Postgres ...", flush=True)
    pg = pg_connect()
    pg.autocommit = False
    pcur = pg.cursor()
    for stmt in DDL:
        pcur.execute(stmt)
    pg.commit()

    batch, total = [], 0
    for row in scur:
        division_id, name, subtype, country, admin_level, wkb = row
        if wkb is None:
            continue
        batch.append((division_id, name, subtype, country, admin_level,
                      psycopg2.Binary(bytes(wkb))))
        if len(batch) >= 200:
            psycopg2.extras.execute_values(pcur, INSERT, batch, template=TEMPLATE, page_size=200)
            total += len(batch)
            batch.clear()
            print(f"  inserted {total} ...", flush=True)
    if batch:
        psycopg2.extras.execute_values(pcur, INSERT, batch, template=TEMPLATE, page_size=200)
        total += len(batch)
    pg.commit()
    print(f"Inserted {total} rows. Building index ...", flush=True)

    pcur.execute("CREATE INDEX features_geom_gix ON public.features USING GIST (geom)")
    pcur.execute("CREATE INDEX features_subtype_idx ON public.features (subtype)")
    pcur.execute("ANALYZE public.features")
    pg.commit()

    pcur.execute("SELECT subtype, count(*) FROM public.features GROUP BY subtype ORDER BY 2 DESC")
    print("Row counts by subtype:")
    for r in pcur.fetchall():
        print(f"  {r[0]}: {r[1]}")
    pcur.execute("SELECT ST_XMin(e), ST_YMin(e), ST_XMax(e), ST_YMax(e) FROM (SELECT ST_Extent(geom) e FROM public.features) q")
    print("Extent (lon/lat):", pcur.fetchone())

    pg.close(); sf.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
