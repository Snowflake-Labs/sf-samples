import io
import csv
import json
import time
import os
import glob
import psycopg2

PG_HOST = os.environ.get("PG_HOST", "<your-pg-host>")
PG_PORT = int(os.environ.get("PG_PORT", "5432"))
PG_DB = os.environ.get("PG_DB", "postgres")
PG_USER = os.environ.get("PG_USER", "snowflake_admin")
PG_PASS = os.environ.get("PG_PASSWORD", "<your-pg-password>")

def get_pg():
    return psycopg2.connect(
        host=PG_HOST, port=PG_PORT, dbname=PG_DB,
        user=PG_USER, password=PG_PASS, sslmode="require",
    )

def load_jsonl(path):
    rows = []
    with open(path, "r") as f:
        for line in f:
            obj = json.loads(line)
            if isinstance(obj, list):
                rows.append(obj)
    return rows

def main():
    import sys
    if len(sys.argv) < 2:
        print("Usage: load_pg.py <command>")
        print("  create   - Create tables in Postgres")
        print("  load     - Load JSONL files into Postgres")
        print("  bench    - Run benchmark queries")
        print("  status   - Show current row counts")
        return

    cmd = sys.argv[1]

    pg = get_pg()
    pg.autocommit = True
    cur = pg.cursor()

    if cmd == "create":
        print("Creating tables...", flush=True)
        cur.execute("DROP TABLE IF EXISTS customer_propensity")
        cur.execute("""
            CREATE TABLE customer_propensity (
                customer_id   INTEGER NOT NULL,
                category_id   TEXT    NOT NULL,
                propensity_score DOUBLE PRECISION NOT NULL,
                PRIMARY KEY (customer_id, category_id)
            )
        """)
        cur.execute("CREATE INDEX idx_propensity_cust ON customer_propensity (customer_id)")
        cur.execute("DROP TABLE IF EXISTS inventory")
        cur.execute("""
            CREATE TABLE inventory (
                product_id      INTEGER NOT NULL,
                region          TEXT    NOT NULL,
                qty_on_hand     INTEGER NOT NULL,
                est_delivery_days INTEGER NOT NULL,
                PRIMARY KEY (region, product_id)
            )
        """)
        cur.execute("CREATE INDEX idx_inventory_region ON inventory (region)")
        cur.execute("DROP TABLE IF EXISTS customer_features")
        cur.execute("""
            CREATE TABLE customer_features (
                customer_id          INTEGER PRIMARY KEY,
                age                  INTEGER,
                tenure_months        INTEGER,
                loyalty_tier_num     INTEGER,
                total_orders         INTEGER,
                lifetime_spend       DOUBLE PRECISION,
                avg_order_value      DOUBLE PRECISION,
                days_since_purchase  INTEGER,
                categories_purchased INTEGER,
                total_browse_events  INTEGER,
                total_sessions       INTEGER,
                avg_browsed_price    DOUBLE PRECISION,
                add_to_cart_count    INTEGER,
                cart_abandonment_rate DOUBLE PRECISION,
                store_ratio          DOUBLE PRECISION,
                engagement_score     DOUBLE PRECISION
            )
        """)
        print("Done.", flush=True)

    elif cmd == "load":
        target = sys.argv[2] if len(sys.argv) > 2 else ""
        jsonl_path = sys.argv[3] if len(sys.argv) > 3 else ""

        if not target or not jsonl_path:
            print("Usage: load_pg.py load <propensity|inventory|features> <jsonl_path>")
            return

        rows = load_jsonl(jsonl_path)
        print(f"Loaded {len(rows)} rows from {jsonl_path}", flush=True)

        if target == "propensity":
            buf = io.StringIO()
            writer = csv.writer(buf, delimiter="\t")
            for r in rows:
                writer.writerow(r)
            buf.seek(0)
            cur.copy_from(buf, "customer_propensity", sep="\t",
                         columns=("customer_id", "category_id", "propensity_score"))
            print(f"Inserted {len(rows)} propensity rows", flush=True)

        elif target == "inventory":
            buf = io.StringIO()
            writer = csv.writer(buf, delimiter="\t")
            for r in rows:
                writer.writerow(r)
            buf.seek(0)
            cur.copy_from(buf, "inventory", sep="\t",
                         columns=("product_id", "region", "qty_on_hand", "est_delivery_days"))
            print(f"Inserted {len(rows)} inventory rows", flush=True)

        elif target == "features":
            buf = io.StringIO()
            writer = csv.writer(buf, delimiter="\t")
            for r in rows:
                writer.writerow(r)
            buf.seek(0)
            cur.copy_from(buf, "customer_features", sep="\t",
                         columns=("customer_id", "age", "tenure_months", "loyalty_tier_num",
                                  "total_orders", "lifetime_spend", "avg_order_value",
                                  "days_since_purchase", "categories_purchased",
                                  "total_browse_events", "total_sessions", "avg_browsed_price",
                                  "add_to_cart_count", "cart_abandonment_rate", "store_ratio",
                                  "engagement_score"))
            print(f"Inserted {len(rows)} customer_features rows", flush=True)

    elif cmd == "bench":
        print("Running benchmark queries...", flush=True)
        for _ in range(5):
            t0 = time.perf_counter()
            cur.execute(
                "SELECT category_id, propensity_score FROM customer_propensity "
                "WHERE customer_id = 1025000 ORDER BY propensity_score DESC LIMIT 20"
            )
            rows = cur.fetchall()
            t1 = time.perf_counter()
            print(f"  Propensity lookup: {len(rows)} rows in {(t1-t0)*1000:.1f}ms")

        for _ in range(5):
            t0 = time.perf_counter()
            cur.execute(
                "SELECT product_id, qty_on_hand, est_delivery_days FROM inventory "
                "WHERE region = 'Northeast'"
            )
            rows = cur.fetchall()
            t1 = time.perf_counter()
            print(f"  Inventory lookup: {len(rows)} rows in {(t1-t0)*1000:.1f}ms")

    elif cmd == "status":
        for t in ["customer_features", "customer_propensity", "inventory"]:
            cur.execute(f"SELECT count(*) FROM {t}")
            print(f"  {t}: {cur.fetchone()[0]} rows")

    cur.close()
    pg.close()

if __name__ == "__main__":
    main()
