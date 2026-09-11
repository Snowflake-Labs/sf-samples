import os
import sys
import time
import json
import numpy as np
import pandas as pd
import requests
import psycopg2
import psycopg2.pool
import snowflake.connector
from concurrent.futures import ThreadPoolExecutor, as_completed
from fastapi import FastAPI, Query
from fastapi.responses import HTMLResponse
from contextlib import asynccontextmanager

state = {}

CUST_SQL = (
    "SELECT AGE, TENURE_MONTHS, LOYALTY_TIER_NUM, TOTAL_ORDERS, "
    "LIFETIME_SPEND, AVG_ORDER_VALUE, DAYS_SINCE_PURCHASE, "
    "CATEGORIES_PURCHASED, TOTAL_BROWSE_EVENTS, TOTAL_SESSIONS, "
    "AVG_BROWSED_PRICE, ADD_TO_CART_COUNT, CART_ABANDONMENT_RATE, "
    "STORE_RATIO, ENGAGEMENT_SCORE "
    'FROM FEATURE_STORE."CUSTOMER_FEATURES$v1" WHERE CUSTOMER_ID = %s'
)

PG_PROPENSITY_SQL = (
    "SELECT category_id, propensity_score "
    "FROM customer_propensity WHERE customer_id = %s "
    "ORDER BY propensity_score DESC LIMIT 20"
)

PG_INVENTORY_SQL = (
    "SELECT product_id, qty_on_hand, est_delivery_days "
    "FROM inventory WHERE region = %s"
)

PG_FEATURES_SQL = (
    "SELECT age, tenure_months, loyalty_tier_num, total_orders, "
    "lifetime_spend, avg_order_value, days_since_purchase, "
    "categories_purchased, total_browse_events, total_sessions, "
    "avg_browsed_price, add_to_cart_count, cart_abandonment_rate, "
    "store_ratio, engagement_score "
    "FROM customer_features WHERE customer_id = %s"
)


def _token():
    return open("/snowflake/session/token").read()


def _new_conn(warehouse=None):
    params = dict(
        host=os.environ["SNOWFLAKE_HOST"],
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        authenticator="oauth",
        token=_token(),
        database="RETAIL_RECOMMENDATION_QS",
    )
    if warehouse:
        params["warehouse"] = warehouse
    return snowflake.connector.connect(**params)


def _refresh_conn(key, warehouse=None):
    try:
        state[key].cursor().execute("SELECT 1").fetchone()
    except Exception:
        print(f"Reconnecting {key}...", flush=True)
        try:
            state[key].close()
        except Exception:
            pass
        state[key] = _new_conn(warehouse=warehouse)
    return state[key]


def _get_hybrid_conn():
    key = "hybrid_conn_0"
    wh = os.environ.get("SNOWFLAKE_WAREHOUSE", "RETAIL_QS_WH")
    if key not in state:
        state[key] = _new_conn(warehouse=wh)
    return _refresh_conn(key, warehouse=wh)


def _get_wh_conn():
    return _refresh_conn("wh_conn", warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "RETAIL_QS_WH"))


def _get_pg_conn():
    pool = state.get("pg_pool")
    if pool:
        return pool.getconn()
    return None


def _put_pg_conn(conn):
    pool = state.get("pg_pool")
    if pool:
        pool.putconn(conn)


ZIP_TO_REGION = {
    "0": "Northeast", "1": "Northeast",
    "2": "Southeast", "3": "Southeast",
    "4": "Midwest", "5": "Midwest",
    "6": "South", "7": "South",
    "8": "West", "9": "West",
}


def _zip_to_region(zip_code: str) -> str:
    if zip_code and len(zip_code) >= 1:
        return ZIP_TO_REGION.get(zip_code[0], "Northeast")
    return "Northeast"


def _read_pg_password():
    secret_path = os.environ.get("PG_PASSWORD_PATH", "")
    if secret_path and os.path.exists(secret_path):
        return open(secret_path).read().strip()
    return os.environ.get("PG_PASSWORD", "")


@asynccontextmanager
async def lifespan(app: FastAPI):
    wh_conn = _new_conn(warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "RETAIL_QS_WH"))

    cur = wh_conn.cursor()
    cur.execute(
        "SELECT PRODUCT_ID, PRODUCT_NAME, CATEGORY_ID, DEPARTMENT, "
        "CATEGORY_NAME, BRAND, PRICE, WEIGHT_LBS, PRODUCT_TYPE "
        "FROM RAW.PRODUCT_CATALOG"
    )
    cols = [d[0] for d in cur.description]
    products = pd.DataFrame(cur.fetchall(), columns=cols)
    cur.close()

    pg_host = os.environ.get("PG_HOST", "")
    pg_pass = _read_pg_password()
    pg_user = os.environ.get("PG_USER", "snowflake_admin")
    pg_db = os.environ.get("PG_DB", "postgres")
    pg_port = int(os.environ.get("PG_PORT", "5432"))

    pg_pool = None
    if pg_host and pg_pass:
        try:
            pg_pool = psycopg2.pool.ThreadedConnectionPool(
                minconn=2,
                maxconn=6,
                host=pg_host,
                port=pg_port,
                dbname=pg_db,
                user=pg_user,
                password=pg_pass,
                sslmode="require",
                connect_timeout=10,
            )
            test_conn = pg_pool.getconn()
            test_cur = test_conn.cursor()
            test_cur.execute("SELECT count(*) FROM customer_features")
            feat_count = test_cur.fetchone()[0]
            test_cur.execute("SELECT count(*) FROM customer_propensity")
            prop_count = test_cur.fetchone()[0]
            test_cur.execute("SELECT count(*) FROM inventory")
            inv_count = test_cur.fetchone()[0]
            test_cur.close()
            pg_pool.putconn(test_conn)
            print(f"Postgres connected: {feat_count} features, {prop_count} propensity, {inv_count} inventory rows", flush=True)
        except Exception as e:
            print(f"Postgres connection failed: {e}, will use warehouse queries as fallback", flush=True)
            pg_pool = None
    else:
        print("No PG_HOST/PG_PASSWORD configured, using warehouse queries", flush=True)

    state["wh_conn"] = wh_conn
    state["hybrid_conn_0"] = _new_conn(warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "RETAIL_QS_WH"))
    state["pg_pool"] = pg_pool
    state["products"] = products
    state["product_map"] = products.set_index("PRODUCT_ID").to_dict("index")
    state["executor"] = ThreadPoolExecutor(max_workers=3)

    state["model_url"] = os.environ.get("MODEL_ENDPOINT", "")
    if not state["model_url"]:
        try:
            cur = wh_conn.cursor()
            cur.execute(
                "SELECT DNS_NAME FROM INFORMATION_SCHEMA.SERVICES "
                "WHERE SERVICE_NAME = 'RANKING_MODEL_SERVICE' "
                "AND SERVICE_SCHEMA = 'ML_REGISTRY'"
            )
            row = cur.fetchone()
            if row:
                dns = row[0]
                state["model_url"] = f"http://{dns}:5000/predict"
                print(f"Discovered model DNS: {dns}", flush=True)
            else:
                raise ValueError("Service not found in INFORMATION_SCHEMA")
            cur.close()
        except Exception as e:
            state["model_url"] = "http://ranking-model-service.ml-registry.svc.spcs.internal:5000/predict"
            print(f"DNS discovery failed ({e}), using fallback", flush=True)
    state["token_path"] = "/snowflake/session/token"
    state["http_session"] = requests.Session()

    backend = "postgres" if pg_pool else "warehouse"
    print(f"Ready - {len(products)} products cached, model_url={state['model_url']}, backend={backend}", flush=True)
    yield
    state["executor"].shutdown(wait=False)
    state["http_session"].close()
    if pg_pool:
        pg_pool.closeall()
    wh_conn.close()
    try:
        state["hybrid_conn_0"].close()
    except Exception:
        pass


app = FastAPI(lifespan=lifespan, docs_url=None, redoc_url=None)


def _get_auth_headers():
    token = open(state["token_path"]).read()
    return {
        "Authorization": f'Snowflake Token="{token}"',
        "Content-Type": "application/json",
    }


@app.get("/health")
def health():
    pg_ok = state.get("pg_pool") is not None
    return {
        "status": "ok",
        "products_cached": len(state.get("products", [])),
        "backend": "postgres" if pg_ok else "warehouse",
    }


@app.get("/", response_class=HTMLResponse)
def test_page():
    return """<!DOCTYPE html><html><head><title>Recommendation Orchestrator</title>
<style>body{font-family:system-ui,sans-serif;max-width:800px;margin:40px auto;padding:0 20px;background:#f5f5f5}
h1{color:#1a73e8}input,button{padding:8px 12px;font-size:14px;margin:4px}button{background:#1a73e8;color:#fff;border:none;border-radius:4px;cursor:pointer}
button:hover{background:#155ab6}pre{background:#222;color:#0f0;padding:16px;border-radius:6px;overflow-x:auto;max-height:600px;overflow-y:auto}
.card{background:#fff;padding:20px;border-radius:8px;margin:16px 0;box-shadow:0 1px 3px rgba(0,0,0,.1)}</style></head>
<body><h1>Real-Time Recommendation Engine</h1>
<div class="card"><h3>Get Recommendations</h3>
<label>Customer ID: <input id="cid" type="number" value="1000000"></label>
<label>Zip Code: <input id="zip" value="10001" size="6"></label>
<label>Top N: <input id="topn" type="number" value="5" min="1" max="20"></label>
<button id="btn-rec">Get Recommendations</button></div>
<div class="card"><h3>Health Check</h3><button id="btn-hc">Check Health</button></div>
<pre id="out">Results will appear here...</pre>
<script>
document.getElementById('btn-rec').addEventListener('click',async function(){
  var o=document.getElementById('out');o.textContent='Loading...';
  try{var r=await fetch('/recommend',{method:'POST',headers:{'Content-Type':'application/json'},
    body:JSON.stringify({customer_id:+document.getElementById('cid').value,
      zip_code:document.getElementById('zip').value,top_n:+document.getElementById('topn').value})});
    var txt=await r.text();try{o.textContent=JSON.stringify(JSON.parse(txt),null,2)}catch(pe){o.textContent='Status: '+r.status+'\\n'+txt}}catch(e){o.textContent='Error: '+e}});
document.getElementById('btn-hc').addEventListener('click',async function(){
  var o=document.getElementById('out');
  try{var r=await fetch('/health');var txt=await r.text();try{o.textContent=JSON.stringify(JSON.parse(txt),null,2)}catch(pe){o.textContent='Status: '+r.status+'\\n'+txt}}catch(e){o.textContent='Error: '+e}});
</script></body></html>"""


@app.get("/recommend")
def recommend_get(
    customer_id: int = Query(...),
    zip_code: str = Query("10001"),
    top_n: int = Query(5),
):
    return _do_recommend({"customer_id": customer_id, "zip_code": zip_code, "top_n": top_n})


def _fetch_customer(customer_id):
    pg = state.get("pg_pool")
    if pg:
        conn = pg.getconn()
        try:
            cur = conn.cursor()
            cur.execute(PG_FEATURES_SQL, (customer_id,))
            row = cur.fetchone()
            cur.close()
            return row
        finally:
            pg.putconn(conn)
    conn = _get_hybrid_conn()
    cur = conn.cursor()
    cur.execute(CUST_SQL, (customer_id,))
    row = cur.fetchone()
    cur.close()
    return row


def _fetch_propensity(customer_id):
    pg = state.get("pg_pool")
    if pg:
        conn = pg.getconn()
        try:
            cur = conn.cursor()
            cur.execute(PG_PROPENSITY_SQL, (customer_id,))
            rows = cur.fetchall()
            cur.close()
            return {row[0]: float(row[1]) for row in rows}
        finally:
            pg.putconn(conn)
    conn = _get_hybrid_conn()
    cur = conn.cursor()
    cur.execute(
        "SELECT CATEGORY_ID, PROPENSITY_SCORE "
        "FROM GOLD.CUSTOMER_PROPENSITY_HYBRID WHERE CUSTOMER_ID = %s "
        "ORDER BY PROPENSITY_SCORE DESC LIMIT 20",
        (customer_id,),
    )
    rows = cur.fetchall()
    cur.close()
    return {row[0]: float(row[1]) for row in rows}


def _fetch_inventory(region):
    pg = state.get("pg_pool")
    if pg:
        conn = pg.getconn()
        try:
            cur = conn.cursor()
            cur.execute(PG_INVENTORY_SQL, (region,))
            rows = cur.fetchall()
            cur.close()
            return rows
        finally:
            pg.putconn(conn)
    conn = _get_hybrid_conn()
    cur = conn.cursor()
    cur.execute(
        "SELECT PRODUCT_ID, QTY_ON_HAND, EST_DELIVERY_DAYS "
        "FROM RAW.INVENTORY_HYBRID WHERE REGION = %s",
        (region,),
    )
    rows = cur.fetchall()
    cur.close()
    return rows


def _parse_score(p):
    val = p[1] if len(p) > 1 else p[0]
    if isinstance(val, dict):
        if "output_feature_1" in val:
            return float(val["output_feature_1"])
        if "output_feature_0" in val:
            return float(val["output_feature_0"])
        return float(list(val.values())[0])
    if isinstance(val, list):
        return float(val[1]) if len(val) > 1 else float(val[0])
    return float(val)


def _extract_params(obj):
    if isinstance(obj, dict) and "customer_id" in obj:
        return obj
    if isinstance(obj, dict):
        for v in obj.values():
            if isinstance(v, dict) and "customer_id" in v:
                return v
            if isinstance(v, str):
                try:
                    parsed = json.loads(v)
                    if isinstance(parsed, dict) and "customer_id" in parsed:
                        return parsed
                except (json.JSONDecodeError, TypeError):
                    pass
    if isinstance(obj, list):
        for item in obj:
            r = _extract_params(item)
            if r is not None:
                return r
    return obj


@app.post("/recommend")
def recommend(request: dict):
    if "data" in request and isinstance(request["data"], list):
        results = []
        for row in request["data"]:
            row_idx = row[0]
            params = row[1] if len(row) > 1 else {}
            if isinstance(params, str):
                params = json.loads(params)
            params = _extract_params(params)
            result = _do_recommend(params)
            results.append([row_idx, result])
        return {"data": results}
    return _do_recommend(request)


def _do_recommend(request: dict):
    timings = {}
    t0 = time.perf_counter()

    customer_id = int(request["customer_id"])
    zip_code = str(request.get("zip_code", "10001"))
    top_n = int(request.get("top_n", 5))
    region = _zip_to_region(zip_code)

    t1 = time.perf_counter()
    executor = state["executor"]
    fut_cust = executor.submit(_fetch_customer, customer_id)
    fut_prop = executor.submit(_fetch_propensity, customer_id)
    fut_inv = executor.submit(_fetch_inventory, region)

    cust_row = fut_cust.result(timeout=10)
    timings["features_ms"] = round((time.perf_counter() - t1) * 1000, 1)

    if cust_row is None:
        fut_prop.cancel()
        fut_inv.cancel()
        return {"error": "customer_not_found", "customer_id": customer_id}

    (age, tenure, tier_num, total_orders, lifetime_spend, avg_order_val,
     days_since, cats_purchased, browse_events, sessions,
     avg_browsed_price, atc_count, cart_abandon, store_ratio, engagement) = cust_row

    propensity_map = fut_prop.result(timeout=10)
    timings["propensity_ms"] = round((time.perf_counter() - t1) * 1000, 1)

    inv_rows = fut_inv.result(timeout=10)
    timings["inventory_ms"] = round((time.perf_counter() - t1) * 1000, 1)
    timings["candidates"] = len(inv_rows)

    MAX_RANKING_CATEGORIES = 6
    if propensity_map:
        top_cats = sorted(propensity_map, key=propensity_map.get, reverse=True)[:MAX_RANKING_CATEGORIES]
        top_cat_set = set(top_cats)
        inv_rows = [r for r in inv_rows if product_map.get(r[0], {}).get("CATEGORY_ID", "") in top_cat_set]
    timings["filtered_candidates"] = len(inv_rows)

    if not inv_rows:
        return {
            "customer_id": customer_id,
            "zip_code": zip_code,
            "recommendations": [],
            "timings": timings,
            "message": "No inventory available for region"
        }

    t4 = time.perf_counter()
    product_map = state["product_map"]

    prod_ids = [row[0] for row in inv_rows]
    prod_prices = np.array([product_map.get(pid, {}).get("PRICE", 0) for pid in prod_ids], dtype=np.float64)
    prod_types = np.array([
        {"daily_staple": 0, "mid_range": 1, "premium": 2}.get(
            product_map.get(pid, {}).get("PRODUCT_TYPE", ""), 0)
        for pid in prod_ids
    ], dtype=np.float64)

    avg_bp = max(float(avg_browsed_price or 0), 1.0)
    avg_ov = max(float(avg_order_val or 0), 1.0)
    n = len(inv_rows)

    feats = np.column_stack([
        np.full(n, float(age or 30)),
        np.full(n, float(tenure or 0)),
        np.full(n, float(tier_num or 0)),
        np.full(n, float(total_orders or 0)),
        np.full(n, float(lifetime_spend or 0)),
        np.full(n, float(avg_order_val or 0)),
        np.full(n, float(days_since or 999)),
        np.full(n, float(engagement or 0)),
        np.full(n, avg_bp),
        prod_prices,
        prod_types,
        np.zeros(n),
        prod_prices / avg_bp,
        np.clip(prod_prices / avg_ov, 0, 5),
    ])
    timings["pair_features_ms"] = round((time.perf_counter() - t4) * 1000, 1)

    t5 = time.perf_counter()
    data = {"data": [[i] + feats[i].tolist() for i in range(n)]}
    headers = _get_auth_headers()
    model_ok = False
    try:
        resp = state["http_session"].post(
            state["model_url"], json=data, headers=headers, timeout=30
        )
        resp.raise_for_status()
        body = resp.json()
        predictions = body["data"]
        if predictions:
            print(f"Model response sample: {predictions[0]}", flush=True)
        scores = np.array([_parse_score(p) for p in predictions])
        model_ok = True
    except Exception as e:
        print(f"Model call failed: {e}, using propensity-only fallback", flush=True)
        scores = np.array([
            propensity_map.get(product_map.get(pid, {}).get("CATEGORY_ID", ""), 0.5)
            for pid in prod_ids
        ])
    timings["model_ms"] = round((time.perf_counter() - t5) * 1000, 1)
    timings["model_ok"] = model_ok

    propensity_boost = np.array([
        propensity_map.get(product_map.get(pid, {}).get("CATEGORY_ID", ""), 0.5)
        for pid in prod_ids
    ])
    final_scores = 0.7 * scores + 0.3 * propensity_boost

    k = min(top_n, len(final_scores))
    top_k = np.argpartition(final_scores, -k)[-k:]
    top_k = top_k[np.argsort(final_scores[top_k])[::-1]]

    recommendations = []
    for rank, li in enumerate(top_k, 1):
        pid = prod_ids[li]
        pinfo = product_map.get(pid, {})
        inv = inv_rows[li]
        recommendations.append({
            "rank": rank,
            "product_id": int(pid),
            "product_name": str(pinfo.get("PRODUCT_NAME", "")),
            "category": str(pinfo.get("CATEGORY_NAME", "")),
            "department": str(pinfo.get("DEPARTMENT", "")),
            "price": float(pinfo.get("PRICE", 0)),
            "score": round(float(final_scores[li]), 4),
            "in_stock_qty": int(inv[1]),
            "est_delivery_days": int(inv[2]),
        })

    timings["total_ms"] = round((time.perf_counter() - t0) * 1000, 1)
    timings["backend"] = "postgres" if state.get("pg_pool") else "warehouse"

    return {
        "customer_id": customer_id,
        "zip_code": zip_code,
        "region": region,
        "recommendations": recommendations,
        "timings": timings,
    }
