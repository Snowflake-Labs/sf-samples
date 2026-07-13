#!/usr/bin/env python3
"""Resolve a source dataset and snapshot it into an owned table.

Why this exists (see logs/): the demo's source was a Marketplace share
(`OVERTURE_MAPS__DIVISIONS`) that (a) may not be acquired in a given account and
(b) can lapse *mid-install* - it was readable at the data step and gone by the
bake step, crashing the pipeline after a billable PG instance had been created.

This module makes the source robust and dataset-agnostic:

  * PROFILES - named column mappings for known free polygon datasets. Each maps
    an arbitrary source table into the fixed shape the pipeline expects.
  * probe/auto-detect - pick the first accessible profile (or an explicit
    --source-table / --source-profile). Used in preflight to FAIL FAST before any
    billable infra is created.
  * snapshot - CTAS the chosen source into TILESERVER.CORE.SOURCE_FEATURES with a
    fixed schema (division_id, name, subtype, country, admin_level, wkb, gj). Every
    downstream step reads this owned table, so a later share lapse cannot break the
    run.

Fixed snapshot schema (geom stored twice, format-agnostic):
    division_id STRING, name STRING, subtype STRING, country STRING,
    admin_level INT, wkb BINARY (ST_ASWKB), gj STRING (ST_ASGEOJSON)

Env: SNOWFLAKE_CONNECTION (active snow CLI conn), COUNTRY (US-scope; '' = all),
     SOURCE_TABLE / SOURCE_PROFILE (explicit overrides),
     SNAPSHOT_TABLE (default TILESERVER.CORE.SOURCE_FEATURES).
"""
import argparse
import os
import sys

import snowflake.connector

SNAPSHOT_TABLE = os.environ.get("SNAPSHOT_TABLE", "TILESERVER.CORE.SOURCE_FEATURES")

# Ordered candidate profiles. Each expression is evaluated against `table`.
#   country_col: name of a column to apply the COUNTRY filter on, or None if the
#                dataset is inherently single-country (COUNTRY is then a literal).
PROFILES = {
    "overture": {
        "table": "OVERTURE_MAPS__DIVISIONS.CARTO.DIVISION_AREA",
        "id": "DIVISION_ID",
        "name": "NAMES:primary::string",
        "subtype": "SUBTYPE",
        "country": "COUNTRY",
        "admin": "ADMIN_LEVEL",
        "geom": "GEOMETRY",
        "base_where": "SUBTYPE IN ('country','region') AND GEOMETRY IS NOT NULL",
        "country_col": "COUNTRY",
    },
    "carto_states": {
        "table": "CARTO_ACADEMY__DATA_FOR_TUTORIALS.CARTO.USA_STATES_BOUNDARIES",
        "id": "CODE_HASC",
        "name": "NAME",
        "subtype": "'region'",
        "country": "'US'",
        "admin": "1",
        "geom": "GEOM",
        "base_where": "GEOM IS NOT NULL",
        "country_col": None,  # inherently US-only
    },
}
CANDIDATE_ORDER = ["overture", "carto_states"]


def sf_connect():
    name = os.environ.get("SNOWFLAKE_CONNECTION") or os.environ.get("SNOWFLAKE_DEFAULT_CONNECTION_NAME")
    return snowflake.connector.connect(connection_name=name) if name else snowflake.connector.connect()


def _profile_from_env():
    """An explicit profile/table from env, or None to auto-detect."""
    prof = os.environ.get("SOURCE_PROFILE", "").strip().lower()
    tbl = os.environ.get("SOURCE_TABLE", "").strip()
    if prof and prof in PROFILES:
        p = dict(PROFILES[prof])
        if tbl:
            p["table"] = tbl
        return prof, p
    if tbl:
        # Explicit table with no profile: assume the Overture column mapping unless
        # the table looks like the CARTO states table.
        base = "carto_states" if "USA_STATES_BOUNDARIES" in tbl.upper() else "overture"
        p = dict(PROFILES[base]); p["table"] = tbl
        return base, p
    return None, None


def _accessible(cur, table, geom):
    try:
        cur.execute(f"SELECT {geom} FROM {table} LIMIT 1")
        cur.fetchone()
        return True
    except Exception:  # noqa: BLE001 - not authorized / does not exist / etc.
        return False


def resolve(cur):
    """Return (profile_name, profile_dict) for the first usable source, or raise."""
    name, prof = _profile_from_env()
    if prof is not None:
        if _accessible(cur, prof["table"], prof["geom"]):
            return name, prof
        raise SystemExit(
            f"ERROR: configured source '{prof['table']}' is not accessible. "
            "Acquire it (Marketplace) or pass a valid --source-table / --source-profile.")
    for cand in CANDIDATE_ORDER:
        prof = PROFILES[cand]
        if _accessible(cur, prof["table"], prof["geom"]):
            return cand, prof
    raise SystemExit(
        "ERROR: no source dataset is accessible. Acquire the free 'Overture Maps - "
        "Divisions' or 'CARTO Academy' Marketplace listing, or pass --source-table.")


def build_snapshot(conn, name, prof):
    country = os.environ.get("COUNTRY", "").strip()
    where = prof["base_where"]
    if country and prof["country_col"]:
        where += f" AND {prof['country_col']} = '{country}'"
    ctas = f"""
CREATE OR REPLACE TABLE {SNAPSHOT_TABLE}
  COMMENT = '{{"origin":"sf_sit-is","name":"oss-deploy-tileserver","version":{{"major":1,"minor":0}},"attributes":{{"is_quickstart":1,"source":"sql"}}}}'
AS
SELECT
  {prof['id']}::string        AS division_id,
  {prof['name']}              AS name,
  {prof['subtype']}::string   AS subtype,
  {prof['country']}::string   AS country,
  {prof['admin']}::int        AS admin_level,
  ST_ASWKB({prof['geom']})    AS wkb,
  ST_ASGEOJSON({prof['geom']})::string AS gj
FROM {prof['table']}
WHERE {where}
"""
    cur = conn.cursor()
    cur.execute(ctas)
    cur.execute(f"SELECT count(*), count(DISTINCT subtype) FROM {SNAPSHOT_TABLE}")
    n, nsub = cur.fetchone()
    print(f"Snapshot {SNAPSHOT_TABLE} built from profile '{name}' ({prof['table']}): "
          f"{n} rows, {nsub} subtype(s), country={country or 'ALL'}", flush=True)
    if not n:
        raise SystemExit("ERROR: snapshot is empty - check COUNTRY scope / source filter.")
    return n


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--probe-only", action="store_true",
                    help="only resolve/verify an accessible source; do not build the snapshot")
    args = ap.parse_args()

    conn = sf_connect()
    cur = conn.cursor()
    name, prof = resolve(cur)
    print(f"SOURCE_PROFILE={name}")
    print(f"SOURCE_TABLE_RESOLVED={prof['table']}")
    if args.probe_only:
        conn.close()
        return 0
    build_snapshot(conn, name, prof)
    conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
