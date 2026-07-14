#!/usr/bin/env python3
"""Minimal Postgres SQL runner for the tileserver demo.

Reads the connection URL from the PG_URL environment variable (injected via the
bash tool's secret_env), never from disk or argv. Executes a single SQL string
passed as argv[1]. Prints rows as TSV. For bytea/tile output use --bytes to get
the length of the first column of the first row.
"""
import os
import sys


def main() -> int:
    url = os.environ.get("PG_URL")
    if url and "sslmode=" not in url:
        url += ("&" if "?" in url else "?") + "sslmode=require"

    arg = sys.argv[1] if len(sys.argv) > 1 else ""
    if arg.startswith("@"):
        with open(arg[1:], "r") as fh:
            sql = fh.read()
    else:
        sql = arg
    want_bytes = "--bytes" in sys.argv[2:]

    import psycopg2
    import time

    def _connect():
        last = None
        for attempt in range(6):
            try:
                if url:
                    return psycopg2.connect(url, connect_timeout=15)
                service = os.environ.get("PGSERVICE", "tileserver")
                return psycopg2.connect(service=service, connect_timeout=15)
            except psycopg2.OperationalError as e:
                last = e
                time.sleep(min(2 ** attempt, 8))
        raise last

    conn = _connect()
    try:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(sql)
            if cur.description is None:
                print(f"OK (rowcount={cur.rowcount})")
                return 0
            rows = cur.fetchall()
            if want_bytes and rows and rows[0]:
                val = rows[0][0]
                n = len(val) if val is not None else 0
                print(f"first_cell_bytes={n}")
                return 0
            cols = [d[0] for d in cur.description]
            print("\t".join(cols))
            for r in rows:
                print("\t".join("" if v is None else str(v) for v in r))
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
