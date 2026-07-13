#!/usr/bin/env python3
"""Arm 3: export H3 aggregation of Overture division areas to JSON for deck.gl.

Aggregates division-area centroids into H3 cells (core Snowflake, native H3),
writing viewer/h3.json = [{"h3": "<hex id>", "n": <count>}, ...]. No tiles, no
tile server - deck.gl renders these client-side.
"""
import json
import os
import sys

import snowflake.connector

RES = int(os.environ.get("H3_RES", "3"))
OUT = os.environ.get("OUT", "viewer/h3.json")

QUERY = f"""
SELECT H3_POINT_TO_CELL_STRING(ST_CENTROID(GEOMETRY), {RES}) AS h3, COUNT(*) AS n
FROM OVERTURE_MAPS__DIVISIONS.CARTO.DIVISION_AREA
WHERE GEOMETRY IS NOT NULL
GROUP BY 1
"""


def main() -> int:
    conn = snowflake.connector.connect(connection_name="fleet_test_evals")
    cur = conn.cursor()
    cur.execute(QUERY)
    rows = [{"h3": h3, "n": int(n)} for (h3, n) in cur if h3]
    conn.close()

    os.makedirs(os.path.dirname(OUT) or ".", exist_ok=True)
    with open(OUT, "w") as f:
        json.dump(rows, f)
    mx = max((r["n"] for r in rows), default=0)
    print(f"Wrote {OUT}: {len(rows)} cells (res {RES}), max n = {mx}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
