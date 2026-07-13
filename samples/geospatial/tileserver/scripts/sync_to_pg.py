#!/usr/bin/env python3
"""ETL: Overture DIVISION_AREA (countries + regions) from Snowflake -> PostGIS.

Snowflake side: connects via the 'fleet_test_evals' named connection (account
WGB26798), exports geometry as WKB. Postgres side: connects via the libpq
service 'tileserver' (~/.pg_service.conf + ~/.pgpass), loads into
public.features with a GiST index.
"""
import sys
import snowflake.connector
import psycopg2
import psycopg2.extras

SF_QUERY = """
SELECT
  DIVISION_ID                         AS division_id,
  NAMES:primary::string               AS name,
  SUBTYPE                             AS subtype,
  COUNTRY                             AS country,
  ADMIN_LEVEL                         AS admin_level,
  ST_ASWKB(GEOMETRY)                  AS wkb
FROM OVERTURE_MAPS__DIVISIONS.CARTO.DIVISION_AREA
WHERE SUBTYPE IN ('country','region')
  AND GEOMETRY IS NOT NULL
"""

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


def main() -> int:
    print("Connecting to Snowflake (fleet_test_evals) ...", flush=True)
    sf = snowflake.connector.connect(connection_name="fleet_test_evals")
    scur = sf.cursor()
    scur.execute(SF_QUERY)

    print("Connecting to Postgres (service=tileserver) ...", flush=True)
    pg = psycopg2.connect(service="tileserver", connect_timeout=20)
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
