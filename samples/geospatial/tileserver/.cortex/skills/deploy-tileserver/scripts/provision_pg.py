#!/usr/bin/env python3
"""Detect-reuse-else-create a Snowflake Postgres + PostGIS instance for the demo.

Strategy (idempotent):
  1. SHOW POSTGRES INSTANCES on the given `snow` connection. Reuse the instance
     named by --pg-instance, else a single existing one, else (only with
     --allow-create) CREATE a new STANDARD instance (billable) and wait for READY.
  2. Resolve the snowflake_admin password: prefer env PGPASSWORD, else a ~/.pgpass
     lookup for the host, else the credentials returned once by CREATE. (Existing
     instances do not expose the password via SQL - set PGPASSWORD or regenerate.)
  3. Wire the SPCS egress allowlist: a POSTGRES_INGRESS network rule + policy on the
     instance that admits the SPCS NAT pool 153.45.59.0/24 (plus optional DEV_IP).
  4. Connect with psycopg2 and CREATE EXTENSION IF NOT EXISTS postgis; smoke-test
     ST_AsMVT.
  5. Print machine-readable lines the orchestrator captures:
        PGHOST=<host>
        PG_URL=postgresql://snowflake_admin:<pw>@<host>:5432/postgres?sslmode=require

Nothing account-specific is hardcoded; the host comes from the instance metadata.
"""
import argparse
import json
import os
import subprocess
import sys
import time

DEFAULT_INSTANCE = "TILESERVER_PG"
SPCS_EGRESS_CIDR = "153.45.59.0/24"   # SPCS external-access NAT pool (IP rotates)


def snow_json(connection, sql):
    """Run a SQL statement via the snow CLI, return parsed JSON rows (list)."""
    out = subprocess.run(
        ["snow", "sql", "-c", connection, "--format", "JSON", "-q", sql],
        capture_output=True, text=True,
    )
    if out.returncode != 0:
        sys.stderr.write(out.stdout + out.stderr)
        raise SystemExit(f"snow sql failed: {sql}")
    txt = out.stdout.strip()
    if not txt:
        return []
    try:
        data = json.loads(txt)
    except json.JSONDecodeError:
        return []
    return data if isinstance(data, list) else [data]


def show_instances(connection):
    return snow_json(connection, "SHOW POSTGRES INSTANCES;")


def pick_instance(rows, wanted):
    by_name = {r.get("name", "").upper(): r for r in rows}
    if wanted and wanted.upper() in by_name:
        return by_name[wanted.upper()]
    ready = [r for r in rows if str(r.get("state", "")).upper() == "READY"]
    if len(ready) == 1:
        return ready[0]
    if len(rows) == 1:
        return rows[0]
    return None


def pgpass_lookup(host):
    path = os.path.expanduser("~/.pgpass")
    if not os.path.exists(path):
        return None
    try:
        with open(path) as fh:
            for line in fh:
                parts = line.rstrip("\n").split(":")
                if len(parts) == 5 and parts[0] == host:
                    return parts[4]
    except OSError:
        return None
    return None


def wait_ready(connection, name, timeout_s=1800):
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        rows = snow_json(connection, f"DESCRIBE POSTGRES INSTANCE {name};")
        state = (rows[0].get("state") if rows else "").upper()
        print(f"  instance {name} state={state}", flush=True)
        if state == "READY":
            return rows[0]
        time.sleep(20)
    raise SystemExit(f"instance {name} did not reach READY within {timeout_s}s")


def create_instance(connection, name):
    print(f"Creating Postgres instance {name} (STANDARD, billable) ...", flush=True)
    sql = (
        f"CREATE POSTGRES INSTANCE {name} "
        "COMPUTE_FAMILY = 'STANDARD' STORAGE_SIZE_GB = 20 "
        "AUTHENTICATION_AUTHORITY = POSTGRES "
        "COMMENT = '{\"origin\":\"sf_sit-is\",\"name\":\"oss-deploy-tileserver\","
        "\"version\":{\"major\":1,\"minor\":0},"
        "\"attributes\":{\"is_quickstart\":1,\"source\":\"sql\"}}';"
    )
    rows = snow_json(connection, sql)
    host, pw = None, None
    if rows:
        r = rows[0]
        host = r.get("host")
        access = r.get("access_roles")
        if access:
            try:
                blob = access if isinstance(access, dict) else json.loads(access)
                admin = blob.get("snowflake_admin", blob)
                pw = admin.get("password") if isinstance(admin, dict) else None
            except (json.JSONDecodeError, AttributeError):
                pw = None
    wait_ready(connection, name)
    if not host:
        rows = snow_json(connection, f"DESCRIBE POSTGRES INSTANCE {name};")
        host = rows[0].get("host") if rows else None
    return host, pw


def wire_egress(connection, dev_ip=""):
    ips = [SPCS_EGRESS_CIDR] + ([dev_ip] if dev_ip else [])
    value_list = ",".join(f"'{ip}'" for ip in ips)
    tag = ('{"origin":"sf_sit-is","name":"oss-deploy-tileserver","version":'
           '{"major":1,"minor":0},"attributes":{"is_quickstart":1,"source":"sql"}}')
    ddl = f"""
CREATE DATABASE IF NOT EXISTS TILESERVER;
CREATE SCHEMA IF NOT EXISTS TILESERVER.CORE;
CREATE OR REPLACE NETWORK RULE TILESERVER.CORE.PG_INGRESS
  MODE = POSTGRES_INGRESS TYPE = IPV4 VALUE_LIST = ({value_list})
  COMMENT = '{tag}';
CREATE NETWORK POLICY IF NOT EXISTS TILESERVER_PG_POLICY
  ALLOWED_NETWORK_RULE_LIST = ('TILESERVER.CORE.PG_INGRESS')
  COMMENT = '{tag}';
"""
    snow_json(connection, ddl)
    return "TILESERVER_PG_POLICY"


def attach_policy(connection, instance, policy):
    # Best-effort: attach the network policy to the instance so SPCS egress is admitted.
    try:
        snow_json(connection, f"ALTER POSTGRES INSTANCE {instance} SET NETWORK_POLICY = '{policy}';")
    except SystemExit:
        sys.stderr.write(f"WARN: could not attach {policy} to {instance}; set it manually if egress fails\n")


def ensure_postgis(url):
    import psycopg2
    last = None
    for attempt in range(6):
        try:
            conn = psycopg2.connect(url, connect_timeout=20)
            conn.autocommit = True
            with conn.cursor() as cur:
                cur.execute("CREATE EXTENSION IF NOT EXISTS postgis;")
                cur.execute("SELECT postgis_full_version();")
                ver = cur.fetchone()[0]
                cur.execute("SELECT length(ST_AsMVT(q)) FROM (SELECT 1 AS a) q;")
                _ = cur.fetchone()
            conn.close()
            print(f"  PostGIS ready: {ver[:60]} ...", flush=True)
            return
        except Exception as e:  # noqa: BLE001 - retry any connect/DDL error
            last = e
            time.sleep(min(2 ** attempt, 10))
    raise SystemExit(f"PostGIS setup failed after retries: {last}")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--connection", required=True)
    ap.add_argument("--pg-instance", default=os.environ.get("PG_INSTANCE", ""))
    ap.add_argument("--allow-create", action="store_true",
                    help="permit CREATE of a new (billable) instance if none is found")
    ap.add_argument("--dev-ip", default=os.environ.get("DEV_IP", ""))
    args = ap.parse_args()

    rows = show_instances(args.connection)
    inst = pick_instance(rows, args.pg_instance)

    if inst is None:
        if not args.allow_create:
            sys.stderr.write(
                "ERROR: no reusable Postgres instance found and --allow-create not set.\n"
                "       Pass --pg-instance <name> to reuse one, or --allow-create to make one.\n")
            return 3
        name = args.pg_instance or DEFAULT_INSTANCE
        host, pw = create_instance(args.connection, name)
        instance_name = name
    else:
        instance_name = inst.get("name")
        host = inst.get("host")
        pw = None
        print(f"Reusing Postgres instance {instance_name} (host {host}).", flush=True)

    if not host:
        sys.stderr.write("ERROR: could not resolve instance host.\n")
        return 4

    pw = pw or os.environ.get("PGPASSWORD") or pgpass_lookup(host)
    if not pw:
        sys.stderr.write(
            f"ERROR: no password for {host}. Existing instances do not expose it via SQL.\n"
            "       Set PGPASSWORD, add a ~/.pgpass entry, or regenerate credentials.\n")
        return 5

    url = f"postgresql://snowflake_admin:{pw}@{host}:5432/postgres?sslmode=require"

    policy = wire_egress(args.connection, args.dev_ip)
    attach_policy(args.connection, instance_name, policy)
    ensure_postgis(url)

    # machine-readable output for the orchestrator to capture
    print(f"PGHOST={host}")
    print(f"PG_URL={url}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
