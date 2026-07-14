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


def _row_for(rows, name):
    for r in rows:
        if str(r.get("name", "")).upper() == name.upper():
            return r
    return None


def wait_ready(connection, name, timeout_s=1800):
    # Poll SHOW (reliable lowercase "state" column) rather than DESCRIBE, whose
    # row shape can omit "state" and crash a naive .get(...).upper().
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        rows = snow_json(connection, "SHOW POSTGRES INSTANCES;")
        row = _row_for(rows, name)
        state = (str(row.get("state")) if row and row.get("state") else "").upper()
        print(f"  instance {name} state={state or '(pending)'}", flush=True)
        if state == "READY":
            return row
        time.sleep(20)
    raise SystemExit(f"instance {name} did not reach READY within {timeout_s}s")


def _extract_admin_pw(access):
    """Pull the snowflake_admin password out of the CREATE access_roles column,
    tolerating string/dict/list shapes."""
    if not access:
        return None
    blob = access
    if isinstance(blob, str):
        try:
            blob = json.loads(blob)
        except json.JSONDecodeError:
            return None
    if isinstance(blob, dict):
        admin = blob.get("snowflake_admin") or blob.get("SNOWFLAKE_ADMIN")
        # access_roles maps role name directly to the password string, e.g.
        #   {"application": "<pw>", "snowflake_admin": "<pw>"}
        if isinstance(admin, str):
            return admin
        if isinstance(admin, dict):
            return admin.get("password") or admin.get("PASSWORD")
        if "password" in blob:
            return blob.get("password")
    if isinstance(blob, list):
        for item in blob:
            if isinstance(item, dict) and str(item.get("role", "")).lower() == "snowflake_admin":
                return item.get("password") or item.get("PASSWORD")
    return None


def create_instance(connection, name):
    print(f"Creating Postgres instance {name} (STANDARD, billable) ...", flush=True)
    # STANDARD_M is the smallest general-purpose family available on AWS
    # (BURST_* is Azure-only; bare 'STANDARD' is not a valid family).
    compute_family = os.environ.get("PG_COMPUTE_FAMILY", "STANDARD_M")
    sql = (
        f"CREATE POSTGRES INSTANCE {name} "
        f"COMPUTE_FAMILY = '{compute_family}' STORAGE_SIZE_GB = 20 "
        "AUTHENTICATION_AUTHORITY = POSTGRES "
        "COMMENT = '{\"origin\":\"sf_sit-is\",\"name\":\"oss-deploy-tileserver\","
        "\"version\":{\"major\":1,\"minor\":0},"
        "\"attributes\":{\"is_quickstart\":1,\"source\":\"sql\"}}';"
    )
    rows = snow_json(connection, sql)
    host, pw = None, None
    if rows:
        r = rows[0]
        sys.stderr.write(f"DEBUG create row keys={list(r.keys())}\n")
        host = r.get("host")
        pw = _extract_admin_pw(r.get("access_roles"))
    if not pw:
        sys.stderr.write(
            "WARN: could not parse snowflake_admin password from CREATE output; "
            "reuse/connection will need PGPASSWORD or ~/.pgpass.\n")
    wait_ready(connection, name)
    if not host:
        rows = snow_json(connection, "SHOW POSTGRES INSTANCES;")
        row = _row_for(rows, name)
        host = row.get("host") if row else None
    # Persist creds to ~/.pgpass (best-effort) so idempotent re-runs can reuse.
    if host and pw:
        _write_pgpass(host, pw)
    return host, pw


def _write_pgpass(host, pw, user="snowflake_admin", db="postgres"):
    try:
        path = os.path.expanduser("~/.pgpass")
        line = f"{host}:5432:{db}:{user}:{pw}"
        existing = ""
        if os.path.exists(path):
            with open(path) as fh:
                existing = fh.read()
        if host not in existing:
            with open(path, "a") as fh:
                if existing and not existing.endswith("\n"):
                    fh.write("\n")
                fh.write(line + "\n")
            os.chmod(path, 0o600)
    except OSError:
        pass


def _detect_public_ip():
    """Best-effort public IP of this machine, so local psql/ETL is admitted by the
    instance's POSTGRES_INGRESS allowlist. Returns '' on failure."""
    import urllib.request
    for url in ("https://api.ipify.org", "https://checkip.amazonaws.com"):
        try:
            with urllib.request.urlopen(url, timeout=8) as resp:
                ip = resp.read().decode().strip()
                if ip:
                    return ip
        except Exception:  # noqa: BLE001
            continue
    return ""


def wire_egress(connection, cidrs):
    # Set the POSTGRES_INGRESS allowlist to exactly `cidrs`. Always includes the
    # SPCS egress NAT pool (so the Martin service reaches PG); the caller adds the
    # operator's dev IP so local PostGIS setup / data load / bake reach PG too.
    value_list = ",".join(f"'{c}'" for c in cidrs)
    tag = ('{"origin":"sf_sit-is","name":"oss-deploy-tileserver","version":'
           '{"major":1,"minor":0},"attributes":{"is_quickstart":1,"source":"sql"}}')
    ddl = f"""
CREATE DATABASE IF NOT EXISTS TILESERVER;
CREATE SCHEMA IF NOT EXISTS TILESERVER.CORE;
CREATE NETWORK RULE IF NOT EXISTS TILESERVER.CORE.PG_INGRESS
  MODE = POSTGRES_INGRESS TYPE = IPV4 VALUE_LIST = ({value_list})
  COMMENT = '{tag}';
ALTER NETWORK RULE TILESERVER.CORE.PG_INGRESS SET VALUE_LIST = ({value_list});
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


def _poll_connect(url, timeout_s=60):
    """Return a live psycopg2 connection once PG is reachable, else None on timeout.
    Tolerates the seconds-to-tens-of-seconds propagation delay after an ingress
    rule / network-policy change."""
    import psycopg2
    deadline = time.time() + timeout_s
    delay = 3
    while time.time() < deadline:
        try:
            conn = psycopg2.connect(url, connect_timeout=10)
            conn.autocommit = True
            return conn
        except Exception:  # noqa: BLE001 - timeout/refused while ingress propagates
            time.sleep(delay)
            delay = min(delay + 2, 10)
    return None


def establish_ingress(connection, url, dev_ip, instance_name):
    """Make PG reachable from THIS machine for local ETL, then leave the ingress
    allowlist tight.

    The operator's public IP as seen on the :5432 path is not reliably the same as
    an external HTTP probe reports (split egress by destination/port). So:
      1. Try the best-effort detected dev IP (+ SPCS pool).
      2. If that cannot connect, briefly widen ingress to 0.0.0.0/0 (password + TLS
         still required) so the install never wedges on a wrong /32.
      3. Once connected, learn the TRUE client IP from Postgres (inet_client_addr)
         and re-tighten the allowlist to exactly [SPCS pool, real dev IP /32].
    Set DEV_INGRESS_CIDR to pin a known egress CIDR and skip the 0.0.0.0/0 step.
    Returns the resolved real dev IP (or None).
    """
    pinned = os.environ.get("DEV_INGRESS_CIDR", "").strip()
    if pinned:
        wire_egress(connection, [SPCS_EGRESS_CIDR, pinned])
    else:
        wire_egress(connection, [SPCS_EGRESS_CIDR] + ([f"{dev_ip}/32"] if dev_ip else []))
    attach_policy(connection, instance_name, "TILESERVER_PG_POLICY")

    conn = _poll_connect(url, timeout_s=45)
    if conn is None and not pinned:
        sys.stderr.write(
            "WARN: PG not reachable with the detected dev IP; the local egress IP "
            "likely differs from the HTTP-probe IP. Briefly widening ingress to "
            "0.0.0.0/0 (password + TLS still required) to learn the real client IP.\n")
        wire_egress(connection, ["0.0.0.0/0"])
        conn = _poll_connect(url, timeout_s=120)
    if conn is None:
        raise SystemExit(
            "PG unreachable from this machine. Set DEV_INGRESS_CIDR to your egress "
            "CIDR, or check outbound :5432 is not blocked by a local firewall/VPN.")

    real_ip = None
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT host(inet_client_addr())")
            real_ip = cur.fetchone()[0]
    except Exception:  # noqa: BLE001
        pass
    finally:
        conn.close()

    # Re-tighten precisely: SPCS pool + the real client /32 (or the pinned CIDR).
    final = [SPCS_EGRESS_CIDR, pinned] if pinned else \
            [SPCS_EGRESS_CIDR] + ([f"{real_ip}/32"] if real_ip else [])
    wire_egress(connection, final)
    if real_ip:
        print(f"  dev ingress = {real_ip}/32 (true egress IP via inet_client_addr)", flush=True)
    # Confirm the tightened rule still admits us before handing off downstream ETL.
    conn2 = _poll_connect(url, timeout_s=45)
    if conn2 is None:
        # Fall back to leaving the broad rule so the install completes; warn loudly.
        sys.stderr.write("WARN: tightened ingress blocked reconnect; leaving 0.0.0.0/0 for the install.\n")
        wire_egress(connection, ["0.0.0.0/0"])
    else:
        conn2.close()
    return real_ip


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
                # Confirm the MVT builders exist (arm 1 depends on them) without
                # executing ST_AsMVT (which requires a real geometry column).
                cur.execute("SELECT count(*) FROM pg_proc WHERE proname IN ('st_asmvt','st_asmvtgeom','st_tileenvelope');")
                nfn = cur.fetchone()[0]
                if nfn < 3:
                    raise RuntimeError(f"PostGIS MVT functions missing (found {nfn}/3)")
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
        # A reused instance may still be building (or resuming); ensure it is
        # READY before we try to connect for PostGIS setup.
        if str(inst.get("state", "")).upper() != "READY":
            wait_ready(args.connection, instance_name)

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

    dev_ip = args.dev_ip or _detect_public_ip()
    establish_ingress(args.connection, url, dev_ip, instance_name)
    ensure_postgis(url)

    # machine-readable output for the orchestrator to capture
    print(f"PGHOST={host}")
    print(f"PG_URL={url}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
