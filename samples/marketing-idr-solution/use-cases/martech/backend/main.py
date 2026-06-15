"""
Martech Backend (FastAPI on SPCS) — SSP-shape-compatible

Mirrors the SSP backend response surface so the cloned SSP frontend can bind
to martech data without touching api.ts.

NFR-1: DB name read from MARTECH_DB env var (default: MARTECH); never
hard-coded in queries.
Auth: SPCS OAuth via /snowflake/session/token; falls back to local named
connection for dev.
"""

from __future__ import annotations

import json
import logging
import os
import queue
import threading
import uuid
from contextlib import contextmanager
from datetime import datetime
from typing import Any, Iterator

import snowflake.connector  # type: ignore
from fastapi import Body, FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from martech_data_generator import MartechDataGenerator  # type: ignore


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

MARTECH_DB = os.getenv("MARTECH_DB", "MARTECH")
MARTECH_WAREHOUSE = os.getenv("MARTECH_WAREHOUSE", "MARTECH_WH")
MARTECH_ROLE = os.getenv("MARTECH_ROLE", "MARTECH_APP_ROLE")
SPCS_TOKEN_PATH = "/snowflake/session/token"
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
APP_VERSION = os.getenv("APP_VERSION", "0.1.0")
FRONTEND_INDUSTRY = os.getenv("FRONTEND_INDUSTRY", "MARTECH")

logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("martech")


# ---------------------------------------------------------------------------
# Connection pool
# ---------------------------------------------------------------------------

class ConnectionPool:
    def __init__(self, size: int = 5) -> None:
        self._q: queue.Queue = queue.Queue(maxsize=size)
        self._lock = threading.Lock()
        self._size = size
        self._created = 0

    def _new_connection(self) -> Any:
        if os.path.exists(SPCS_TOKEN_PATH):
            with open(SPCS_TOKEN_PATH) as f:
                token = f.read()
            log.info(f"Opening Snowflake SPCS connection (db={MARTECH_DB})")
            return snowflake.connector.connect(
                token=token,
                authenticator="oauth",
                host=os.environ["SNOWFLAKE_HOST"],
                account=os.environ["SNOWFLAKE_ACCOUNT"],
                database=MARTECH_DB,
                warehouse=MARTECH_WAREHOUSE,
                role=MARTECH_ROLE,
                client_session_keep_alive=True,
            )
        conn_name = os.getenv("SNOWFLAKE_CONNECTION_NAME", "default_connection_name")
        log.info(f"Opening Snowflake connection via named connection: {conn_name}")
        conn = snowflake.connector.connect(
            connection_name=conn_name,
            client_session_keep_alive=True,
        )
        cur = conn.cursor()
        try:
            cur.execute(f"USE ROLE {MARTECH_ROLE}")
            cur.execute(f"USE WAREHOUSE {MARTECH_WAREHOUSE}")
            cur.execute(f"USE DATABASE {MARTECH_DB}")
        finally:
            cur.close()
        return conn

    @contextmanager
    def acquire(self) -> Iterator[Any]:
        try:
            conn = self._q.get_nowait()
        except queue.Empty:
            with self._lock:
                if self._created < self._size:
                    conn = self._new_connection()
                    self._created += 1
                else:
                    conn = self._q.get(timeout=30)
        try:
            yield conn
        finally:
            self._q.put(conn)


pool = ConnectionPool(size=5)


def fetch(sql: str, params: tuple | None = None) -> list[dict]:
    with pool.acquire() as conn:
        cur = conn.cursor(snowflake.connector.DictCursor)
        try:
            cur.execute(sql, params or ())
            return list(cur.fetchall())
        finally:
            cur.close()


def execute(sql: str, params: tuple | None = None) -> int:
    with pool.acquire() as conn:
        cur = conn.cursor()
        try:
            cur.execute(sql, params or ())
            return cur.rowcount or 0
        finally:
            cur.close()


def parse_json_field(value: Any) -> Any:
    if value is None or not isinstance(value, str):
        return value
    try:
        return json.loads(value)
    except Exception:
        return value


# ---------------------------------------------------------------------------
# FastAPI app
# ---------------------------------------------------------------------------

app = FastAPI(title="Martech IDR Backend", version=APP_VERSION)
app.add_middleware(
    CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"]
)


# ---- health & runtime config ----------------------------------------------

@app.get("/health")
def health() -> dict:
    return {"status": "ok", "db": MARTECH_DB, "version": APP_VERSION}


@app.get("/api/config")
def get_config() -> dict:
    return {
        "db_name": MARTECH_DB,
        "frontend_industry": FRONTEND_INDUSTRY,
        "deployment_id": os.getenv("DEPLOYMENT_ID", "local"),
        "version": APP_VERSION,
    }


# ---- /api/identity-filter-sources -----------------------------------------

@app.get("/api/identity-filter-sources")
def identity_filter_sources() -> dict:
    rows = fetch(f"""
        SELECT SOURCE_TABLE_NAME, FRIENDLY_NAME, SOURCE_PRIORITY
        FROM {MARTECH_DB}.CONFIG.IDR_SOURCE_CONFIG
        WHERE IS_ACTIVE = TRUE
        ORDER BY SOURCE_PRIORITY
    """)
    return {
        "sources": [
            {"value": r["SOURCE_TABLE_NAME"], "label": r["FRIENDLY_NAME"], "priority": r["SOURCE_PRIORITY"]}
            for r in rows
        ],
        "ui_mode": "checkbox",
        "checkbox_threshold": 6,
    }


# ---- /api/dashboard -------------------------------------------------------

@app.get("/api/dashboard")
def dashboard() -> dict:
    cluster_count = fetch(f"SELECT COUNT(*) AS C FROM {MARTECH_DB}.SILVER.IDR_CORE_CLUSTER")[0]["C"]
    edge_count = fetch(f"SELECT COUNT(*) AS C FROM {MARTECH_DB}.SILVER.IDR_CORE_MATCH_RESULTS WHERE IS_CURRENT")[0]["C"]
    src_counts = {
        "POS_TRANSACTION_RAW": fetch(f"SELECT COUNT(*) AS C FROM {MARTECH_DB}.BRONZE.POS_TRANSACTION_RAW")[0]["C"],
        "LOYALTY_MEMBER_RAW": fetch(f"SELECT COUNT(*) AS C FROM {MARTECH_DB}.BRONZE.LOYALTY_MEMBER_RAW")[0]["C"],
        "WEB_CLICKSTREAM_RAW": fetch(f"SELECT COUNT(*) AS C FROM {MARTECH_DB}.BRONZE.WEB_CLICKSTREAM_RAW")[0]["C"],
        "SHOPIFY_ORDER_RAW": fetch(f"SELECT COUNT(*) AS C FROM {MARTECH_DB}.BRONZE.SHOPIFY_ORDER_RAW")[0]["C"],
    }
    total_records = sum(src_counts.values())

    id_counts = fetch(f"""
        SELECT IDENTIFIER_TYPE AS T, COUNT(*) AS C
        FROM {MARTECH_DB}.SILVER.IDR_CORE_ENTITY_IDENTIFIERS
        GROUP BY IDENTIFIER_TYPE
    """)
    id_breakdown = {r["T"]: r["C"] for r in id_counts}

    rule_drivers = fetch(f"""
        SELECT RULE_NAME AS LABEL, COUNT(*) AS CNT
        FROM {MARTECH_DB}.SILVER.IDR_CORE_MATCH_RESULTS
        WHERE IS_CURRENT = TRUE
        GROUP BY RULE_NAME
        ORDER BY CNT DESC
        LIMIT 10
    """)

    return {
        "total_identities": cluster_count,
        "total_clusters": cluster_count,
        "total_source_records": total_records,
        "pipeline_flow_window_minutes": 0,
        "pipeline_events_ingested": total_records,
        "pipeline_clusters_created": cluster_count,
        "global_dedupe_ratio": (1 - cluster_count / total_records) if total_records else None,
        "avg_confidence": 0.85,
        "source_breakdown": src_counts,
        "id_type_breakdown": id_breakdown,
        "confidence_buckets": {"high": cluster_count, "medium": 0, "low": 0},
        "run_delta": {"run_id": None, "new_clusters": cluster_count, "updated_clusters": 0, "merged_clusters": 0, "candidate_matches": 0},
        "merge_source_shares": [
            {"label": k.replace("_RAW", "").replace("_", " ").title(), "pct": (v / total_records * 100) if total_records else 0, "weight": v}
            for k, v in src_counts.items()
        ],
        "rule_drivers": [{"rule_label": r["LABEL"], "count": r["CNT"]} for r in rule_drivers],
        "alerts": [],
        "health": {"score": 95, "summary": "All systems nominal"},
        "timings_ms": None,
        "dashboard_wall_ms": None,
    }


# ---- /api/pipeline-sankey -------------------------------------------------

@app.get("/api/pipeline-sankey")
def pipeline_sankey(minutes: int = 60, run_id: str | None = None) -> dict:
    """Return sankey flow data from IDR_COMPLETE events.
    When run_id is set, returns stats for that single run.
    Otherwise aggregates all runs in the time window."""
    if run_id:
        rows = fetch(f"""
            SELECT TO_VARCHAR(EVENT_DETAILS) AS D
            FROM {MARTECH_DB}.SILVER.IDR_CORE_EVENT_LOG
            WHERE EVENT_TYPE = 'IDR_COMPLETE'
              AND RUN_ID = %s
            LIMIT 1
        """, (run_id,))
    else:
        rows = fetch(f"""
            SELECT TO_VARCHAR(EVENT_DETAILS) AS D
            FROM {MARTECH_DB}.SILVER.IDR_CORE_EVENT_LOG
            WHERE EVENT_TYPE = 'IDR_COMPLETE'
              AND EVENT_TIMESTAMP >= DATEADD('minute', -{minutes}, CURRENT_TIMESTAMP())
            ORDER BY EVENT_TIMESTAMP DESC
            LIMIT 100
        """)

    totals = {"inserts": 0, "deletes": 0, "created": 0, "updated": 0, "merged": 0, "matches": 0, "ml_pairs": 0}
    by_source: dict[str, int] = {}
    by_match_rule: dict[str, int] = {}
    run_count = 0

    for r in rows:
        raw = r.get("D")
        if not raw:
            continue
        try:
            details = json.loads(raw)
        except (json.JSONDecodeError, TypeError):
            continue
        run_count += 1
        sr = details.get("totals", {}).get("source_records", {})
        totals["inserts"] += sr.get("total_inserts", 0)
        totals["deletes"] += sr.get("total_deletes", 0)

        profiles = details.get("totals", {}).get("profiles", {})
        totals["created"] += profiles.get("created", 0)
        totals["updated"] += profiles.get("updated", 0)
        totals["merged"] += profiles.get("merged", 0)

        matches_section = details.get("totals", {}).get("matches", {})
        by_rule_data = matches_section.get("by_rule", {})
        for rule, cnt in by_rule_data.items():
            by_match_rule[rule] = by_match_rule.get(rule, 0) + cnt

        totals["matches"] += sum(by_rule_data.values()) if by_rule_data else 0

        by_table = sr.get("by_table", {})
        for tbl, counts in by_table.items():
            label = tbl.replace("_RAW", "")
            ins = counts.get("inserts", 0) if isinstance(counts, dict) else 0
            by_source[label] = by_source.get(label, 0) + ins

        ml_results = details.get("stages", {}).get("ml_scoring", {}).get("results", {})
        scoring = ml_results.get("scoring", {})
        totals["ml_pairs"] += scoring.get("scored", 0)

    return {
        "totals": totals,
        "by_source": by_source,
        "by_match_rule": by_match_rule,
        "run_count": run_count,
    }


# ---- /api/identities ------------------------------------------------------

def _identity_profile_row(r: dict) -> dict:
    """Map a GOLD.DT_CUSTOMER_PROFILE row into SSP-shape IdentityProfile."""
    source_set = parse_json_field(r.get("SOURCE_SET")) or []
    device_ids = parse_json_field(r.get("DEVICE_IDS")) or []
    uid2_ids = parse_json_field(r.get("UID2_IDS")) or []
    rampid_ids = parse_json_field(r.get("RAMPID_IDS")) or []
    device_ids = [x for x in device_ids if x]
    uid2_ids = [x for x in uid2_ids if x]
    rampid_ids = [x for x in rampid_ids if x]
    confidence = min(1.0, 0.4 + 0.15 * len(source_set) + (0.1 if r.get("LOYALTY_MEMBER_ID") else 0))
    name_parts = [r.get("PRIMARY_FIRST_NAME"), r.get("PRIMARY_LAST_NAME")]
    display_name = " ".join(p for p in name_parts if p) or r.get("PRIMARY_EMAIL") or r.get("CLUSTER_ID")
    return {
        "identity_id": r["CLUSTER_ID"],
        "display_name": display_name,
        "primary_hem": r.get("PRIMARY_EMAIL_HEM") or None,
        "primary_uid2": uid2_ids[0] if uid2_ids else None,
        "primary_rampid": rampid_ids[0] if rampid_ids else None,
        "primary_ppid": None,
        "primary_device_id": device_ids[0] if device_ids else None,
        "primary_cookie": None,
        "device_count": len(device_ids),
        "id_type_count": len([x for x in [r.get("PRIMARY_EMAIL"), r.get("PRIMARY_PHONE"), r.get("LOYALTY_MEMBER_ID")] + device_ids + uid2_ids + rampid_ids if x]),
        "source_count": len(source_set),
        "source_types": source_set,
        "household_id": r.get("HOUSEHOLD_ID"),
        "geo_country": "US",
        "geo_metro": r.get("BILLING_CITY"),
        "connected_cluster_count": 0,
        "confidence_score": confidence,
        "first_seen": str(r["FIRST_SEEN_TS"]) if r.get("FIRST_SEEN_TS") else None,
        "last_seen": str(r["LAST_SEEN_TS"]) if r.get("LAST_SEEN_TS") else None,
    }


@app.get("/api/identities")
def list_identities(
    q: str | None = Query(None),
    page: int = Query(1, ge=1),
    page_size: int = Query(25, ge=1, le=500),
    sort_by: str = Query("confidence"),
    sort_dir: str = Query("desc"),
    confidence_band: str | None = Query(None),
    connected_clusters: str | None = Query(None),
    filter_source_types: str | None = Query(None),
    min_devices: int | None = Query(None),
) -> dict:
    where: list[str] = []
    params: list = []
    if q:
        where.append("(CLUSTER_ID ILIKE %s OR PRIMARY_EMAIL ILIKE %s OR PRIMARY_PHONE ILIKE %s OR LOYALTY_MEMBER_ID ILIKE %s OR PRIMARY_FIRST_NAME ILIKE %s OR PRIMARY_LAST_NAME ILIKE %s OR (PRIMARY_FIRST_NAME || ' ' || PRIMARY_LAST_NAME) ILIKE %s)")
        like = f"%{q}%"
        params.extend([like, like, like, like, like, like, like])

    where_sql = (" WHERE " + " AND ".join(where)) if where else ""

    sort_col = {
        "confidence": "p.LIFETIME_TOTAL_SPEND",
        "last_seen": "p.LAST_SEEN_TS",
        "first_seen": "p.FIRST_SEEN_TS",
        "display_name": "p.PRIMARY_LAST_NAME",
        "identity_id": "p.CLUSTER_ID",
        "source_count": "ARRAY_SIZE(p.SOURCE_SET)",
        "device_count": "ARRAY_SIZE(p.DEVICE_IDS)",
        "id_type_count": "ARRAY_SIZE(p.SOURCE_SET)",
        "connected_clusters": "p.LIFETIME_TOTAL_SPEND",
        "household_id": "h.HOUSEHOLD_ID",
    }.get(sort_by, "p.LIFETIME_TOTAL_SPEND")
    direction = "DESC NULLS LAST" if sort_dir == "desc" else "ASC NULLS LAST"

    # The list page surfaces a household_id alongside the individual profile.
    # LEFT JOIN to HOUSEHOLD_MEMBERSHIP keeps non-household identities visible.
    where_qualified = where_sql.replace("PRIMARY_EMAIL", "p.PRIMARY_EMAIL") \
        .replace("PRIMARY_PHONE", "p.PRIMARY_PHONE") \
        .replace("LOYALTY_MEMBER_ID", "p.LOYALTY_MEMBER_ID") \
        .replace("PRIMARY_FIRST_NAME", "p.PRIMARY_FIRST_NAME") \
        .replace("PRIMARY_LAST_NAME", "p.PRIMARY_LAST_NAME") \
        .replace("CLUSTER_ID", "p.CLUSTER_ID")

    total = fetch(
        f"SELECT COUNT(*) AS C FROM {MARTECH_DB}.GOLD.DT_CUSTOMER_PROFILE p{where_qualified}",
        tuple(params),
    )[0]["C"]
    offset = (page - 1) * page_size
    rows = fetch(f"""
        SELECT p.*, h.HOUSEHOLD_ID
        FROM {MARTECH_DB}.GOLD.DT_CUSTOMER_PROFILE p
        LEFT JOIN {MARTECH_DB}.SILVER.IDR_CORE_HOUSEHOLD_MEMBERSHIP h
          ON h.INDIVIDUAL_CLUSTER_ID = p.CLUSTER_ID
        {where_qualified}
        ORDER BY {sort_col} {direction}
        LIMIT {page_size} OFFSET {offset}
    """, tuple(params))
    return {
        "identities": [_identity_profile_row(r) for r in rows],
        "total": total,
        "showing": len(rows),
        "page": page,
        "page_size": page_size,
    }


@app.get("/api/identities/{identity_id}")
def get_identity(identity_id: str) -> dict:
    profile_rows = fetch(f"""
        SELECT * FROM {MARTECH_DB}.GOLD.DT_CUSTOMER_PROFILE WHERE CLUSTER_ID = %s
    """, (identity_id,))
    if not profile_rows:
        raise HTTPException(404, "Identity not found")
    profile = _identity_profile_row(profile_rows[0])
    # Identifier inventory: mirror SSP's pattern — one DISTINCT row per
    # (type, value, source_system) sourced from the engine-maintained
    # IDENTIFIER_LINK x ENTITY_IDENTIFIERS x CLUSTER_MEMBERSHIP join. The
    # frontend's aggregateLinkedIdentifiersByValue() collapses multi-source
    # values into a single row with multiple source pills.
    ident_rows = fetch(f"""
        SELECT DISTINCT e.IDENTIFIER_TYPE,
                        e.IDENTIFIER_VALUE,
                        l.SOURCE_TYPE AS SOURCE_SYSTEM,
                        CASE WHEN e.IDENTIFIER_TYPE IN ('EMAIL','PHONE','LOYALTY_ID','UID2','RAMPID')
                             THEN TRUE ELSE FALSE END AS IS_PRIMARY
        FROM {MARTECH_DB}.SILVER.IDR_CORE_CLUSTER_MEMBERSHIP cm
        JOIN {MARTECH_DB}.SILVER.IDR_CORE_IDENTIFIER_LINK   l ON cm.SOURCE_RECORD_ID = l.SOURCE_RECORD_ID AND l.IS_ACTIVE = TRUE
        JOIN {MARTECH_DB}.SILVER.IDR_CORE_ENTITY_IDENTIFIERS e ON l.IDENTIFIER_ID    = e.IDENTIFIER_ID    AND e.IS_ACTIVE = TRUE
        WHERE cm.CLUSTER_ID = %s
        ORDER BY e.IDENTIFIER_TYPE, IS_PRIMARY DESC
    """, (identity_id,))
    identifiers = [
        {"identifier_type": r["IDENTIFIER_TYPE"],
         "identifier_value": r["IDENTIFIER_VALUE"],
         "source_system": r["SOURCE_SYSTEM"],
         "is_primary": bool(r["IS_PRIMARY"])}
        for r in ident_rows
    ]
    return {
        "profile": profile,
        "identifiers": identifiers,
        "tapad_person_ids": None,
        "experian_person_ids": None,
        "household_ids": None,
        "audience_segments": None,
    }


@app.get("/api/identities/{identity_id}/idr-explanation")
def idr_explanation(identity_id: str) -> dict:
    matches = fetch(f"""
        SELECT m.IDENTIFIER_1, m.IDENTIFIER_2, m.MATCH_SCORE,
               m.RULE_ID, m.RULE_NAME, r.RULE_DESCRIPTION,
               m.NEW_SOURCE_RECORD_ID, m.MATCHED_SOURCE_RECORD_ID,
               m.UPDATED_AT, m.CREATED_AT
        FROM {MARTECH_DB}.SILVER.IDR_CORE_MATCH_RESULTS m
        LEFT JOIN {MARTECH_DB}.CONFIG.IDR_MATCHING_RULES r ON r.RULE_ID = m.RULE_ID
        JOIN {MARTECH_DB}.SILVER.IDR_CORE_CLUSTER_MEMBERSHIP cm
          ON cm.SOURCE_RECORD_ID = m.NEW_SOURCE_RECORD_ID
          OR cm.SOURCE_RECORD_ID = m.MATCHED_SOURCE_RECORD_ID
        WHERE cm.CLUSTER_ID = %s AND m.IS_CURRENT = TRUE
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY LEAST(m.NEW_SOURCE_RECORD_ID, m.MATCHED_SOURCE_RECORD_ID),
                         GREATEST(m.NEW_SOURCE_RECORD_ID, m.MATCHED_SOURCE_RECORD_ID)
            ORDER BY COALESCE(r.RULE_PRIORITY, 999), m.MATCH_SCORE DESC
        ) = 1
    """, (identity_id,))

    members = fetch(f"""
        SELECT cm.SOURCE_RECORD_ID,
               COALESCE(MAX(il.SOURCE_TYPE), 'UNKNOWN') AS SRC
        FROM {MARTECH_DB}.SILVER.IDR_CORE_CLUSTER_MEMBERSHIP cm
        LEFT JOIN {MARTECH_DB}.SILVER.IDR_CORE_IDENTIFIER_LINK il
          ON il.SOURCE_RECORD_ID = cm.SOURCE_RECORD_ID
        WHERE cm.CLUSTER_ID = %s
        GROUP BY cm.SOURCE_RECORD_ID
    """, (identity_id,))

    return {
        "identity_id": identity_id,
        "cluster_confidence": (sum(m.get("MATCH_SCORE") or 0 for m in matches) / len(matches)) if matches else 0.9,
        "rules_applied": sorted({m["RULE_ID"] for m in matches if m.get("RULE_ID")}),
        "total_identifiers": len(members),
        "source_systems": sorted({m["SRC"] for m in members}),
        "matches": [
            {
                "identifier_1": m.get("NEW_SOURCE_RECORD_ID") or "",
                "identifier_2": m.get("MATCHED_SOURCE_RECORD_ID") or "",
                "match_score": m.get("MATCH_SCORE") or 0,
                "match_rule": m.get("RULE_ID"),
                "rule_description": m.get("RULE_DESCRIPTION") or m.get("RULE_NAME"),
                "match_details": {
                    "identifier_1": m.get("IDENTIFIER_1"),
                    "identifier_2": m.get("IDENTIFIER_2"),
                },
                "last_seen": (str(m.get("UPDATED_AT") or m.get("CREATED_AT")) if (m.get("UPDATED_AT") or m.get("CREATED_AT")) else None),
            }
            for m in matches
        ],
        "source_records": [
            {"record_id": m["SOURCE_RECORD_ID"], "source": m["SRC"], "name": None, "identifiers": None}
            for m in members
        ],
    }


@app.get("/api/identities/{identity_id}/weak-links")
def weak_links(identity_id: str) -> list:
    return []


@app.get("/api/identities/{identity_id}/source-records")
def source_records(identity_id: str) -> list:
    """Mirror SSP shape: one row per source_record_id with all identifiers
    aggregated into the `identifiers` array. Driven by IDR_CORE_CLUSTER's
    SOURCE_RECORD_IDS variant for the active cluster."""
    cluster_rows = fetch(f"""
        SELECT SOURCE_RECORD_IDS
        FROM {MARTECH_DB}.SILVER.IDR_CORE_CLUSTER
        WHERE CLUSTER_ID = %s AND STATUS = 'ACTIVE'
    """, (identity_id,))
    if not cluster_rows:
        return []
    raw_ids = parse_json_field(cluster_rows[0].get("SOURCE_RECORD_IDS")) or []
    if not isinstance(raw_ids, list) or not raw_ids:
        return []

    # Quote each id; safe because source record IDs are alphanumeric/UUID-shaped
    src_list = "','".join(str(x) for x in raw_ids if x)

    rows = fetch(f"""
        WITH record_first_seen AS (
            SELECT l.SOURCE_RECORD_ID, MIN(l.CREATED_AT) AS FIRST_SEEN
            FROM {MARTECH_DB}.SILVER.IDR_CORE_IDENTIFIER_LINK l
            WHERE l.IS_ACTIVE = TRUE
              AND l.SOURCE_RECORD_ID IN ('{src_list}')
            GROUP BY l.SOURCE_RECORD_ID
        )
        SELECT l.SOURCE_RECORD_ID, l.SOURCE_TYPE,
               e.IDENTIFIER_TYPE, e.IDENTIFIER_VALUE,
               rfs.FIRST_SEEN AS CREATED_AT
        FROM {MARTECH_DB}.SILVER.IDR_CORE_IDENTIFIER_LINK l
        JOIN {MARTECH_DB}.SILVER.IDR_CORE_ENTITY_IDENTIFIERS e
          ON l.IDENTIFIER_ID = e.IDENTIFIER_ID AND e.IS_ACTIVE = TRUE
        JOIN record_first_seen rfs
          ON rfs.SOURCE_RECORD_ID = l.SOURCE_RECORD_ID
        WHERE l.IS_ACTIVE = TRUE
          AND l.SOURCE_RECORD_ID IN ('{src_list}')
        ORDER BY rfs.FIRST_SEEN ASC NULLS LAST,
                 l.SOURCE_RECORD_ID,
                 e.IDENTIFIER_TYPE
    """)

    sr_map: dict[str, dict] = {}
    for r in rows:
        rid = r["SOURCE_RECORD_ID"]
        if rid not in sr_map:
            sr_map[rid] = {
                "record_id": rid,
                "source_type": r.get("SOURCE_TYPE") or "UNKNOWN",
                "identifiers": [],
                "created_at": str(r["CREATED_AT"]) if r.get("CREATED_AT") else None,
                "raw_payload_json": None,
                "raw_payload_origin": None,
                "source_attributes": None,
            }
        if r.get("IDENTIFIER_TYPE"):
            sr_map[rid]["identifiers"].append({
                "type": r["IDENTIFIER_TYPE"],
                "value": r.get("IDENTIFIER_VALUE") or "",
            })

    # Include any source records that exist in the cluster but have no
    # rows in IDR_CORE_IDENTIFIER_LINK (defensive — shouldn't happen but
    # keeps the UI consistent if extraction missed a record).
    for rid in raw_ids:
        if rid not in sr_map:
            sr_map[rid] = {
                "record_id": rid,
                "source_type": "UNKNOWN",
                "identifiers": [],
                "created_at": None,
                "raw_payload_json": None,
                "raw_payload_origin": None,
                "source_attributes": None,
            }

    return list(sr_map.values())


@app.get("/api/source-record/{record_id}/payload")
def source_record_payload(record_id: str, source_type: str = Query(...)) -> dict:
    return {"raw_payload_json": None, "raw_payload_origin": None, "source_attributes": None}


@app.get("/api/identities/{identity_id}/lineage")
def lineage(identity_id: str) -> dict:
    rows = fetch(f"""
        SELECT LOG_ID, CLUSTER_ID, EVENT_TYPE,
               TO_VARCHAR(SOURCE_RECORD_IDS) AS SOURCE_RECORD_IDS,
               TO_VARCHAR(PREVIOUS_SOURCE_RECORD_IDS) AS PREVIOUS_SOURCE_RECORD_IDS,
               TO_VARCHAR(MERGED_FROM_CLUSTERS) AS MERGED_FROM_CLUSTERS,
               EVENT_DETAILS, CREATED_AT, MATCH_ID, MATCH_DETAILS
        FROM {MARTECH_DB}.SILVER.IDR_CORE_CLUSTER_LOG
        WHERE CLUSTER_ID = %s
        ORDER BY CREATED_AT ASC
        LIMIT 100
    """, (identity_id,))

    # Build the source_records map the frontend uses to label record IDs
    # by their source system (Record<record_id, {record_id, source, name}>).
    src_record_ids: set[str] = set()
    for r in rows:
        for raw in (r.get("SOURCE_RECORD_IDS"), r.get("PREVIOUS_SOURCE_RECORD_IDS")):
            if not raw:
                continue
            try:
                ids = json.loads(raw)
                if isinstance(ids, list):
                    src_record_ids.update(str(x) for x in ids if x)
            except Exception:
                pass

    src_records_map: dict[str, dict] = {}
    if src_record_ids:
        ids_csv = "','".join(src_record_ids)
        link_rows = fetch(f"""
            SELECT DISTINCT SOURCE_RECORD_ID, SOURCE_TYPE
            FROM {MARTECH_DB}.SILVER.IDR_CORE_IDENTIFIER_LINK
            WHERE SOURCE_RECORD_ID IN ('{ids_csv}')
        """)
        for lr in link_rows:
            rid = lr.get("SOURCE_RECORD_ID")
            if rid and rid not in src_records_map:
                src_records_map[rid] = {
                    "record_id": rid,
                    "source": lr.get("SOURCE_TYPE") or "UNKNOWN",
                    "name": rid,
                }

    return {
        "cluster_id": identity_id,
        "lineage": [
            {
                "log_id": r["LOG_ID"],
                "cluster_id": r["CLUSTER_ID"],
                "event_type": r["EVENT_TYPE"],
                # Strings — frontend JSON.parses these
                "source_record_ids": r.get("SOURCE_RECORD_IDS"),
                "previous_source_record_ids": r.get("PREVIOUS_SOURCE_RECORD_IDS"),
                "merged_from_clusters": r.get("MERGED_FROM_CLUSTERS"),
                # Objects — frontend reads keys directly
                "event_details": parse_json_field(r.get("EVENT_DETAILS")),
                "match_details": parse_json_field(r.get("MATCH_DETAILS")),
                "created_at": str(r["CREATED_AT"]) if r.get("CREATED_AT") else None,
                "match_id": r.get("MATCH_ID"),
            }
            for r in rows
        ],
        "merged_clusters": [],
        "source_records": src_records_map,
    }


@app.get("/api/identities/{identity_id}/ai-summary")
def ai_summary(identity_id: str) -> dict:
    return {"summary": "AI summary not yet computed for this customer profile."}


@app.get("/api/identities/{identity_id}/connected-clusters-graph")
def connected_clusters_graph(identity_id: str, depth: int = 1) -> dict:
    return {"ego_cluster_id": identity_id, "cluster_depth": {identity_id: 0}, "cluster_parent": {identity_id: None}, "pairs": []}


# ---- LLM reviews ----------------------------------------------------------

def _llm_review_row(r: dict) -> dict:
    # Map DB verdict to frontend decision labels
    verdict = r.get("LLM_VERDICT") or ""
    decision_map = {"MATCH": "MERGE_PERSON", "NOT_MATCH": "NO_MATCH", "UNCLEAR": "UNCLEAR"}
    llm_decision = decision_map.get(verdict.upper(), verdict or None)
    # Map DB status to frontend status
    status = r.get("STATUS") or "PENDING_HUMAN"
    status_map = {"PENDING_HUMAN": "PENDING", "ACCEPTED": "ACCEPTED", "REJECTED": "REJECTED"}
    mapped_status = status_map.get(status, status)
    return {
        "review_id": r.get("REVIEW_ID") or r.get("PAIR_ID"),
        "pair_id": r.get("PAIR_ID") or "",
        "cluster_id_a": r.get("CLUSTER_ID_A") or r.get("SOURCE_RECORD_ID_A") or "",
        "cluster_id_b": r.get("CLUSTER_ID_B") or r.get("SOURCE_RECORD_ID_B") or "",
        "source_type_a": r.get("SOURCE_TYPE_A"),
        "source_type_b": r.get("SOURCE_TYPE_B"),
        "ml_score": float(r["ML_SCORE"]) if r.get("ML_SCORE") is not None else None,
        "ml_tier": "GREY_LLM",
        "llm_decision": llm_decision,
        "llm_confidence": 0.9 if verdict == "MATCH" else (0.3 if verdict == "NOT_MATCH" else None),
        "llm_reasoning": r.get("LLM_RATIONALE"),
        "llm_raw_output": r.get("LLM_RAW_OUTPUT"),
        "llm_parse_error": r.get("LLM_PARSE_ERROR"),
        "status": mapped_status,
        "created_at": str(r["CREATED_AT"]) if r.get("CREATED_AT") else None,
        "model_used": r.get("LLM_MODEL"),
        # ML features (populated when joined)
        "features": {
            "email_eq": r.get("EMAIL_EQ"),
            "phone_eq": r.get("PHONE_EQ"),
            "email_handle_eq": r.get("EMAIL_HANDLE_EQ"),
            "phone_last7_eq": r.get("PHONE_LAST7_EQ"),
            "loyalty_id_eq": r.get("LOYALTY_ID_EQ"),
            "device_id_eq": r.get("DEVICE_ID_EQ"),
            "jw_first_name": float(r["JW_FIRST_NAME"]) if r.get("JW_FIRST_NAME") is not None else None,
            "jw_last_name": float(r["JW_LAST_NAME"]) if r.get("JW_LAST_NAME") is not None else None,
            "jw_street": float(r["JW_STREET"]) if r.get("JW_STREET") is not None else None,
            "postal_eq": r.get("POSTAL_EQ"),
            "state_eq": r.get("STATE_EQ"),
            "nickname_first_eq": r.get("NICKNAME_FIRST_EQ"),
        } if r.get("JW_FIRST_NAME") is not None else None,
    }


@app.get("/api/llm-reviews")
def list_llm_reviews(status: str = "ALL", limit: int = 50, offset: int = 0) -> list:
    # Map frontend status to DB status
    status_reverse = {"PENDING": "PENDING_HUMAN", "ACCEPTED": "ACCEPTED", "REJECTED": "REJECTED"}
    where_clause = ""
    params: tuple = ()
    if status != "ALL":
        db_status = status_reverse.get(status, status)
        where_clause = "WHERE q.STATUS = %s"
        params = (db_status,)
    rows = fetch(f"""
        SELECT q.REVIEW_ID, q.PAIR_ID, q.SOURCE_RECORD_ID_A, q.SOURCE_TYPE_A,
               q.SOURCE_RECORD_ID_B, q.SOURCE_TYPE_B,
               q.ML_SCORE, q.LLM_VERDICT, q.LLM_RATIONALE, q.LLM_RAW_OUTPUT,
               q.LLM_PARSE_ERROR, q.LLM_MODEL, q.STATUS, q.CREATED_AT,
               f.EMAIL_EQ, f.PHONE_EQ, f.EMAIL_HANDLE_EQ, f.PHONE_LAST7_EQ,
               f.LOYALTY_ID_EQ, f.DEVICE_ID_EQ,
               f.JW_FIRST_NAME, f.JW_LAST_NAME, f.JW_STREET,
               f.POSTAL_EQ, f.STATE_EQ, f.NICKNAME_FIRST_EQ,
               cma.CLUSTER_ID AS CLUSTER_ID_A,
               cmb.CLUSTER_ID AS CLUSTER_ID_B
        FROM {MARTECH_DB}.SILVER.LLM_REVIEW_QUEUE q
        LEFT JOIN {MARTECH_DB}.SILVER.ML_PAIR_FEATURES f ON f.PAIR_ID = q.PAIR_ID
        LEFT JOIN {MARTECH_DB}.SILVER.IDR_CORE_CLUSTER_MEMBERSHIP cma ON cma.SOURCE_RECORD_ID = q.SOURCE_RECORD_ID_A
        LEFT JOIN {MARTECH_DB}.SILVER.IDR_CORE_CLUSTER_MEMBERSHIP cmb ON cmb.SOURCE_RECORD_ID = q.SOURCE_RECORD_ID_B
        {where_clause}
        ORDER BY q.ML_SCORE DESC
        LIMIT {limit} OFFSET {offset}
    """, params)
    return [_llm_review_row(r) for r in rows]


@app.get("/api/llm-reviews/stats")
def llm_review_stats() -> dict:
    rows = fetch(f"""
        SELECT STATUS, COUNT(*) AS C
        FROM {MARTECH_DB}.SILVER.LLM_REVIEW_QUEUE
        GROUP BY STATUS
    """)
    by_status = {r["STATUS"]: r["C"] for r in rows}
    total = sum(by_status.values())
    # Map to frontend-expected keys
    return {
        "total": total,
        "unfiltered_total": total,
        "PENDING": by_status.get("PENDING_HUMAN", 0),
        "REVIEWED": 0,
        "ACCEPTED": by_status.get("ACCEPTED", 0),
        "REJECTED": by_status.get("REJECTED", 0),
    }


def _fetch_record_attributes(source_record_id: str, source_type: str) -> dict:
    """Fetch actual attribute values from the appropriate STD table."""
    attrs = {"email": None, "phone": None, "first_name": None, "last_name": None,
             "street": None, "city": None, "state": None, "postal_code": None,
             "loyalty_id": None, "device_id": None}
    if source_type == "LOYALTY_MEMBER_RAW":
        rows = fetch(f"""
            SELECT EMAIL_STD, PHONE_E164, PHONE, FIRST_NAME_STD, LAST_NAME_STD,
                   STREET_ADDRESS_STD, CITY_STD, STATE_STD, POSTAL_CODE_STD, MEMBER_ID_STD
            FROM {MARTECH_DB}.SILVER.STD_LOYALTY_MEMBER_RAW
            WHERE MEMBER_ID = %s LIMIT 1
        """, (source_record_id,))
        if rows:
            r = rows[0]
            attrs = {"email": r.get("EMAIL_STD"), "phone": r.get("PHONE_E164") or r.get("PHONE"),
                     "first_name": r.get("FIRST_NAME_STD"), "last_name": r.get("LAST_NAME_STD"),
                     "street": r.get("STREET_ADDRESS_STD"), "city": r.get("CITY_STD"),
                     "state": r.get("STATE_STD"), "postal_code": r.get("POSTAL_CODE_STD"),
                     "loyalty_id": r.get("MEMBER_ID_STD"), "device_id": None}
    elif source_type == "SHOPIFY_ORDER_RAW":
        rows = fetch(f"""
            SELECT EMAIL_STD, PHONE_E164, PHONE, FIRST_NAME_STD, LAST_NAME_STD,
                   BILLING_STREET_ADDRESS, BILLING_CITY, BILLING_STATE, BILLING_POSTAL_CODE,
                   LOYALTY_MEMBER_NUMBER_STD
            FROM {MARTECH_DB}.SILVER.STD_SHOPIFY_ORDER_RAW
            WHERE ORDER_ID = %s LIMIT 1
        """, (source_record_id,))
        if rows:
            r = rows[0]
            attrs = {"email": r.get("EMAIL_STD"), "phone": r.get("PHONE_E164") or r.get("PHONE"),
                     "first_name": r.get("FIRST_NAME_STD"), "last_name": r.get("LAST_NAME_STD"),
                     "street": r.get("BILLING_STREET_ADDRESS"), "city": r.get("BILLING_CITY"),
                     "state": r.get("BILLING_STATE"), "postal_code": r.get("BILLING_POSTAL_CODE"),
                     "loyalty_id": r.get("LOYALTY_MEMBER_NUMBER_STD"), "device_id": None}
    elif source_type == "POS_TRANSACTION_RAW":
        rows = fetch(f"""
            SELECT EMAIL_STD, PHONE_E164, CUSTOMER_PHONE, FIRST_NAME_STD, LAST_NAME_STD,
                   LOYALTY_MEMBER_NUMBER_STD
            FROM {MARTECH_DB}.SILVER.STD_POS_TRANSACTION_RAW
            WHERE TXN_ID = %s LIMIT 1
        """, (source_record_id,))
        if rows:
            r = rows[0]
            attrs = {"email": r.get("EMAIL_STD"), "phone": r.get("PHONE_E164") or r.get("CUSTOMER_PHONE"),
                     "first_name": r.get("FIRST_NAME_STD"), "last_name": r.get("LAST_NAME_STD"),
                     "street": None, "city": None, "state": None, "postal_code": None,
                     "loyalty_id": r.get("LOYALTY_MEMBER_NUMBER_STD"), "device_id": None}
    elif source_type == "WEB_CLICKSTREAM_RAW":
        rows = fetch(f"""
            SELECT EMAIL_STD, PHONE_E164, LOGGED_IN_PHONE, DEVICE_ID_STD
            FROM {MARTECH_DB}.SILVER.STD_WEB_CLICKSTREAM_RAW
            WHERE EVENT_ID = %s LIMIT 1
        """, (source_record_id,))
        if rows:
            r = rows[0]
            attrs = {"email": r.get("EMAIL_STD"), "phone": r.get("PHONE_E164") or r.get("LOGGED_IN_PHONE"),
                     "first_name": None, "last_name": None,
                     "street": None, "city": None, "state": None, "postal_code": None,
                     "loyalty_id": None, "device_id": r.get("DEVICE_ID_STD")}
    return attrs


@app.get("/api/llm-reviews/{review_id}/record-attributes")
def get_review_record_attributes(review_id: str) -> dict:
    """Fetch actual attribute values for both records in a pair."""
    rows = fetch(f"""
        SELECT SOURCE_RECORD_ID_A, SOURCE_TYPE_A, SOURCE_RECORD_ID_B, SOURCE_TYPE_B
        FROM {MARTECH_DB}.SILVER.LLM_REVIEW_QUEUE
        WHERE REVIEW_ID = %s OR PAIR_ID = %s
        LIMIT 1
    """, (review_id, review_id))
    if not rows:
        return {"record_a": {}, "record_b": {}}
    row = rows[0]
    record_a = _fetch_record_attributes(row["SOURCE_RECORD_ID_A"], row["SOURCE_TYPE_A"])
    record_b = _fetch_record_attributes(row["SOURCE_RECORD_ID_B"], row["SOURCE_TYPE_B"])
    return {
        "record_a": record_a,
        "record_b": record_b,
        "source_id_a": row["SOURCE_RECORD_ID_A"],
        "source_id_b": row["SOURCE_RECORD_ID_B"],
        "source_type_a": row["SOURCE_TYPE_A"],
        "source_type_b": row["SOURCE_TYPE_B"],
    }


def _household_payload(household_id: str, current_cluster_id: str | None) -> dict:
    """Hydrate a household record with members and edges. Shared between the
    per-identity and per-household endpoints. `current_cluster_id` flags one
    member as the focal identity (None when called from /api/households/...).
    """
    h_rows = fetch(f"""
        SELECT HOUSEHOLD_ID, HOUSEHOLD_TYPE, SHARED_STREET, SHARED_CITY, SHARED_STATE, SHARED_POSTAL,
               MEMBER_COUNT, PRIMARY_LAST_NAME, FIRST_SEEN, LAST_SEEN
        FROM {MARTECH_DB}.SILVER.IDR_CORE_HOUSEHOLD_CLUSTER
        WHERE HOUSEHOLD_ID = %s AND STATUS = 'ACTIVE'
    """, (household_id,))
    if not h_rows:
        return None
    h = h_rows[0]
    member_rows = fetch(f"""
        SELECT m.INDIVIDUAL_CLUSTER_ID,
               p.PRIMARY_FIRST_NAME, p.PRIMARY_LAST_NAME,
               p.PRIMARY_EMAIL, p.PRIMARY_PHONE,
               p.LIFETIME_TOTAL_SPEND, p.CONFIDENCE_SCORE
        FROM {MARTECH_DB}.SILVER.IDR_CORE_HOUSEHOLD_MEMBERSHIP m
        LEFT JOIN {MARTECH_DB}.GOLD.DT_CUSTOMER_PROFILE p
          ON p.CLUSTER_ID = m.INDIVIDUAL_CLUSTER_ID
        WHERE m.HOUSEHOLD_ID = %s
        ORDER BY p.PRIMARY_LAST_NAME, p.PRIMARY_FIRST_NAME
    """, (household_id,))
    members = [
        {
            "cluster_id": r["INDIVIDUAL_CLUSTER_ID"],
            "primary_first_name": r.get("PRIMARY_FIRST_NAME"),
            "primary_last_name": r.get("PRIMARY_LAST_NAME"),
            "primary_email": r.get("PRIMARY_EMAIL"),
            "primary_phone": r.get("PRIMARY_PHONE"),
            "lifetime_total_spend": float(r["LIFETIME_TOTAL_SPEND"]) if r.get("LIFETIME_TOTAL_SPEND") is not None else None,
            "confidence_score": float(r["CONFIDENCE_SCORE"]) if r.get("CONFIDENCE_SCORE") is not None else None,
            "is_current": r["INDIVIDUAL_CLUSTER_ID"] == current_cluster_id,
        }
        for r in member_rows
    ]
    member_ids = [m["cluster_id"] for m in members]
    edges: list[dict] = []
    if len(member_ids) >= 2:
        ids_sql = ", ".join("%s" for _ in member_ids)
        edge_rows = fetch(f"""
            SELECT A_CLUSTER_ID, B_CLUSTER_ID, RULE_ID, MATCH_SCORE, JW_LAST
            FROM {MARTECH_DB}.SILVER.IDR_CORE_HOUSEHOLD_MATCH_RESULTS
            WHERE IS_ACTIVE = TRUE AND IS_CURRENT = TRUE
              AND A_CLUSTER_ID IN ({ids_sql})
              AND B_CLUSTER_ID IN ({ids_sql})
        """, tuple(member_ids + member_ids))
        edges = [
            {
                "a_cluster_id": r["A_CLUSTER_ID"],
                "b_cluster_id": r["B_CLUSTER_ID"],
                "rule_id": r["RULE_ID"],
                "match_score": float(r["MATCH_SCORE"]) if r.get("MATCH_SCORE") is not None else None,
                "jw_last": float(r["JW_LAST"]) if r.get("JW_LAST") is not None else None,
            }
            for r in edge_rows
        ]
    spend_total = sum(m["lifetime_total_spend"] or 0 for m in members)
    edge_scores = [e["match_score"] for e in edges if e["match_score"] is not None]
    avg_conf = (sum(edge_scores) / len(edge_scores)) if edge_scores else None
    return {
        "household_id": h["HOUSEHOLD_ID"],
        "household_type": h["HOUSEHOLD_TYPE"],
        "shared_street": h.get("SHARED_STREET"),
        "shared_city": h.get("SHARED_CITY"),
        "shared_state": h.get("SHARED_STATE"),
        "shared_postal": h.get("SHARED_POSTAL"),
        "member_count": h["MEMBER_COUNT"],
        "primary_last_name": h.get("PRIMARY_LAST_NAME"),
        "household_lifetime_spend": round(spend_total, 2),
        "avg_member_confidence": round(avg_conf, 4) if avg_conf is not None else None,
        "first_seen": str(h["FIRST_SEEN"]) if h.get("FIRST_SEEN") else None,
        "last_seen": str(h["LAST_SEEN"]) if h.get("LAST_SEEN") else None,
        "members": members,
        "edges": edges,
    }


@app.get("/api/identities/{identity_id}/household")
def identity_household(identity_id: str):
    """Return the household this identity is part of, or null."""
    rows = fetch(f"""
        SELECT HOUSEHOLD_ID
        FROM {MARTECH_DB}.SILVER.IDR_CORE_HOUSEHOLD_MEMBERSHIP
        WHERE INDIVIDUAL_CLUSTER_ID = %s
        LIMIT 1
    """, (identity_id,))
    if not rows:
        return None
    return _household_payload(rows[0]["HOUSEHOLD_ID"], identity_id)


@app.get("/api/households/{household_id}")
def get_household(household_id: str):
    """Return a household by id (used by the future /households page)."""
    payload = _household_payload(household_id, None)
    if payload is None:
        raise HTTPException(404, "Household not found")
    return payload


@app.get("/api/households/{household_id}/ai-risk-signals")
def household_ai_risk_signals(household_id: str, cluster_id: str | None = None):
    """Use Cortex AI_COMPLETE to generate risk signals for a household."""
    payload = _household_payload(household_id, cluster_id)
    if payload is None:
        raise HTTPException(404, "Household not found")

    # Build context for the prompt
    members_ctx = []
    for m in payload["members"]:
        members_ctx.append(
            f"- {m['primary_first_name']} {m['primary_last_name']}: "
            f"email={m.get('primary_email','N/A')}, phone={m.get('primary_phone','N/A')}"
        )

    # Look up rule descriptions for edges
    edge_rule_ids = list({e['rule_id'] for e in payload.get("edges", []) if e.get('rule_id')})
    rule_desc_map: dict[str, str] = {}
    if edge_rule_ids:
        placeholders = ", ".join(["%s"] * len(edge_rule_ids))
        rule_rows = fetch(
            f"SELECT RULE_ID, RULE_DESCRIPTION FROM {MARTECH_DB}.CONFIG.IDR_MATCHING_RULES WHERE RULE_ID IN ({placeholders})",
            tuple(edge_rule_ids)
        )
        for r in rule_rows:
            rule_desc_map[r["RULE_ID"]] = r.get("RULE_DESCRIPTION") or ""

    edges_ctx = []
    for e in payload.get("edges", []):
        desc = rule_desc_map.get(e['rule_id'], '')
        edges_ctx.append(
            f"- {e['rule_id']}: score={e.get('match_score','N/A')}, "
            f"jw_last_name={e.get('jw_last','N/A')}, "
            f"description=\"{desc}\""
        )

    prompt = f"""You are an identity resolution risk analyst for a martech customer data platform.
Analyze this household grouping and produce risk signals as JSON.

HOUSEHOLD CONTEXT:
- Type: {payload['household_type']}
- Member count: {payload['member_count']}
- Shared address: {payload.get('shared_street','N/A')}, {payload.get('shared_city','N/A')}, {payload.get('shared_state','N/A')} {payload.get('shared_postal','N/A')}
- Primary last name: {payload.get('primary_last_name','N/A')}
- Avg edge confidence: {payload.get('avg_member_confidence', 0):.1%}

MEMBERS:
{chr(10).join(members_ctx)}

LINKING EDGES:
{chr(10).join(edges_ctx) if edges_ctx else 'No edges found.'}

Produce ONLY valid JSON (no markdown, no code fences) with this structure:
{{
  "risk_level": "Low" or "Medium" or "High",
  "risk_score": number between 0.0 and 1.0,
  "signals": [
    {{"signal": "short title", "severity": "info" or "warning" or "critical", "detail": "explanation"}}
  ],
  "recommendation": "one sentence summary recommendation"
}}

Consider these risk dimensions:
1. Address sharing patterns (is this a normal household or suspicious aggregation point?)
2. Name similarity anomalies (fuzzy vs exact matches, maiden/married name plausibility)
3. Strength of linking evidence (deterministic vs probabilistic edges)

Return 3-6 signals. Be specific and cite numbers from the data.
Order signals with concerns/risks (severity "critical" or "warning") FIRST, followed by neutral observations (severity "info") LAST."""

    try:
        rows = fetch(
            "SELECT SNOWFLAKE.CORTEX.COMPLETE('claude-4-sonnet', %s) AS RESULT",
            (prompt,)
        )
        if not rows:
            return {"error": "No response from AI model"}
        raw = rows[0]["RESULT"]
        import json as _json
        # Strip markdown fences if present
        text = raw.strip()
        if text.startswith("```"):
            text = text.split("\n", 1)[1] if "\n" in text else text[3:]
            if text.endswith("```"):
                text = text[:-3]
            text = text.strip()
        result = _json.loads(text)
        result["prompt"] = prompt
        return result
    except Exception as e:
        return {
            "risk_level": "Unknown",
            "risk_score": None,
            "signals": [{"signal": "AI analysis unavailable", "severity": "info", "detail": str(e)[:200]}],
            "recommendation": "Fallback to rule-based risk assessment.",
        }


@app.get("/api/identities/{identity_id}/llm-reviews")
def identity_llm_reviews(identity_id: str) -> list:
    rows = fetch(f"""
        SELECT q.REVIEW_ID, q.PAIR_ID, q.SOURCE_RECORD_ID_A, q.SOURCE_RECORD_ID_B,
               q.ML_SCORE, q.LLM_VERDICT, q.LLM_RATIONALE, q.LLM_MODEL, q.STATUS, q.CREATED_AT
        FROM {MARTECH_DB}.SILVER.LLM_REVIEW_QUEUE q
        JOIN {MARTECH_DB}.SILVER.IDR_CORE_CLUSTER_MEMBERSHIP cm
          ON cm.SOURCE_RECORD_ID = q.SOURCE_RECORD_ID_A
        WHERE cm.CLUSTER_ID = %s
        ORDER BY q.ML_SCORE DESC LIMIT 50
    """, (identity_id,))
    return [_llm_review_row(r) for r in rows]


class LLMThresholdsBody(BaseModel):
    score_low: float
    score_high: float
    batch_size: int
    model: str
    available_models: list[str] = []


@app.get("/api/config/llm-thresholds")
def get_llm_thresholds() -> dict:
    rows = fetch(f"""
        SELECT CONFIG_KEY, CONFIG_VALUE FROM {MARTECH_DB}.CONFIG.IDR_ML_AI_EVALUATION_CONFIG
        WHERE CONFIG_KEY IN ('LLM_REVIEW_SCORE_LOW','LLM_REVIEW_SCORE_HIGH','LLM_REVIEW_BATCH_SIZE','LLM_REVIEW_MODEL')
    """)
    cfg = {r["CONFIG_KEY"]: r["CONFIG_VALUE"] for r in rows}
    return {
        "score_low": float(cfg.get("LLM_REVIEW_SCORE_LOW", "0.55")),
        "score_high": float(cfg.get("LLM_REVIEW_SCORE_HIGH", "0.85")),
        "batch_size": int(cfg.get("LLM_REVIEW_BATCH_SIZE", "50")),
        "model": cfg.get("LLM_REVIEW_MODEL", "claude-4-sonnet"),
        "available_models": ["claude-4-sonnet", "mistral-large2", "llama3.1-70b"],
    }


@app.post("/api/config/llm-thresholds")
def update_llm_thresholds(body: LLMThresholdsBody) -> dict:
    for key, val in [
        ("LLM_REVIEW_SCORE_LOW", str(body.score_low)),
        ("LLM_REVIEW_SCORE_HIGH", str(body.score_high)),
        ("LLM_REVIEW_BATCH_SIZE", str(body.batch_size)),
        ("LLM_REVIEW_MODEL", body.model),
    ]:
        execute(f"""
            UPDATE {MARTECH_DB}.CONFIG.IDR_ML_AI_EVALUATION_CONFIG
            SET CONFIG_VALUE = %s, UPDATED_AT = CURRENT_TIMESTAMP()
            WHERE CONFIG_KEY = %s
        """, (val, key))
    return get_llm_thresholds()


@app.post("/api/llm-reviews/{review_id}/accept")
def accept_review(review_id: str) -> dict:
    execute(f"""
        UPDATE {MARTECH_DB}.SILVER.LLM_REVIEW_QUEUE
        SET HUMAN_VERDICT='ACCEPT', STATUS='ACCEPTED', UPDATED_AT=CURRENT_TIMESTAMP()
        WHERE REVIEW_ID=%s
    """, (review_id,))
    execute(f"""
        INSERT INTO {MARTECH_DB}.SILVER.IDR_CORE_MATCH_RESULTS
            (SOURCE_RECORD_ID_A, SOURCE_RECORD_ID_B, NEW_SOURCE_RECORD_ID, MATCHED_SOURCE_RECORD_ID,
             RULE_ID, RULE_NAME, MATCH_SCORE, MATCHED_ON, IS_ACTIVE, IS_CURRENT)
        SELECT SOURCE_RECORD_ID_A, SOURCE_RECORD_ID_B, SOURCE_RECORD_ID_A, SOURCE_RECORD_ID_B,
               'MARTECH_R17', 'LLM Adjudicated MATCH', 0.90, 'LLM_HUMAN', TRUE, TRUE
        FROM {MARTECH_DB}.SILVER.LLM_REVIEW_QUEUE WHERE REVIEW_ID=%s
    """, (review_id,))
    return {"review_id": review_id, "status": "ACCEPTED"}


@app.post("/api/llm-reviews/{review_id}/reject")
def reject_review(review_id: str) -> dict:
    execute(f"""
        UPDATE {MARTECH_DB}.SILVER.LLM_REVIEW_QUEUE
        SET HUMAN_VERDICT='REJECT', STATUS='REJECTED', UPDATED_AT=CURRENT_TIMESTAMP()
        WHERE REVIEW_ID=%s
    """, (review_id,))
    return {"review_id": review_id, "status": "REJECTED"}


@app.post("/api/demo/llm-review/{pair_id}")
def demo_llm_review(pair_id: str) -> dict:
    return {"review_id": pair_id, "pair_id": pair_id, "status": "PENDING_HUMAN",
            "cluster_id_a": "", "cluster_id_b": ""}


# ---- Configuration --------------------------------------------------------

@app.get("/api/config/matching-rules")
def matching_rules() -> list:
    rows = fetch(f"""
        SELECT RULE_ID, RULE_NAME, RULE_DESCRIPTION, RULE_PRIORITY, MATCH_TYPE,
               ANCHOR_FIELD, EXACT_MATCH_FIELDS, FUZZY_MATCH_FIELD, FUZZY_ALGORITHM,
               FUZZY_THRESHOLD, BASE_SCORE, REQUIRE_CROSS_SOURCE,
               REQUIRE_DIFFERENT_RECORD, IS_ACTIVE
        FROM {MARTECH_DB}.CONFIG.IDR_MATCHING_RULES
        ORDER BY RULE_PRIORITY
    """)
    return [
        {
            "rule_id": r["RULE_ID"], "rule_name": r["RULE_NAME"],
            "rule_description": r.get("RULE_DESCRIPTION"),
            "rule_priority": r.get("RULE_PRIORITY"),
            "match_type": r.get("MATCH_TYPE"),
            "anchor_field": r.get("ANCHOR_FIELD"),
            "exact_match_fields": r.get("EXACT_MATCH_FIELDS"),
            "fuzzy_match_field": r.get("FUZZY_MATCH_FIELD"),
            "fuzzy_algorithm": r.get("FUZZY_ALGORITHM"),
            "fuzzy_threshold": r.get("FUZZY_THRESHOLD"),
            "use_fuzzy_score": (r.get("FUZZY_ALGORITHM") or "NONE") != "NONE",
            "base_score": r.get("BASE_SCORE"),
            "auto_match_threshold": r.get("BASE_SCORE"),
            "require_cross_source": r.get("REQUIRE_CROSS_SOURCE"),
            "require_different_record": r.get("REQUIRE_DIFFERENT_RECORD"),
            "is_active": r.get("IS_ACTIVE"),
        } for r in rows
    ]


@app.get("/api/config/sources")
def sources() -> list:
    rows = fetch(f"""
        SELECT SOURCE_TABLE_NAME, PRIMARY_KEY_COLUMN, FRIENDLY_NAME, SOURCE_PREFIX,
               SOURCE_PRIORITY, HAS_LOCATION, IS_ACTIVE
        FROM {MARTECH_DB}.CONFIG.IDR_SOURCE_CONFIG
        ORDER BY SOURCE_PRIORITY
    """)
    desc_rows = fetch(f"""
        SELECT SOURCE_SYSTEM, DESCRIPTION FROM {MARTECH_DB}.CONFIG.SOURCE_PRIORITY
    """)
    desc_map = {r["SOURCE_SYSTEM"]: r["DESCRIPTION"] for r in desc_rows}
    return [
        {
            "source": r["SOURCE_TABLE_NAME"],
            "priority": r["SOURCE_PRIORITY"],
            "description": desc_map.get(r["SOURCE_TABLE_NAME"].replace("_RAW", ""), ""),
            "pk_column": r["PRIMARY_KEY_COLUMN"],
            "prefix": r["SOURCE_PREFIX"],
            "has_location": r["HAS_LOCATION"],
        } for r in rows
    ]


@app.get("/api/config/pipeline-tasks")
def pipeline_tasks() -> list:
    try:
        rows = fetch(f"SHOW TASKS IN SCHEMA {MARTECH_DB}.APP")
    except Exception:
        rows = []
    return [
        {
            "task_name": r.get("name", ""),
            "display_name": (r.get("name") or "").replace("_", " ").title(),
            "schedule": r.get("schedule") or "",
            "warehouse": r.get("warehouse") or MARTECH_WAREHOUSE,
            "state": r.get("state") or "suspended",
        }
        for r in rows
    ]


@app.post("/api/config/pipeline-tasks/{task_name}/resume")
def resume_task(task_name: str) -> dict:
    execute(f"ALTER TASK {MARTECH_DB}.APP.{task_name} RESUME")
    return {"task": task_name, "state": "RESUMED"}


@app.post("/api/config/pipeline-tasks/{task_name}/suspend")
def suspend_task(task_name: str) -> dict:
    execute(f"ALTER TASK {MARTECH_DB}.APP.{task_name} SUSPEND")
    return {"task": task_name, "state": "SUSPENDED"}


@app.get("/api/config/infrastructure")
def infrastructure() -> dict:
    return {
        "warehouse": {"name": MARTECH_WAREHOUSE, "size": "MEDIUM", "auto_suspend": 60, "status": "running"},
        "compute_pool": None,
        "service": None,
    }


@app.get("/api/config/display-thresholds")
def display_thresholds() -> dict:
    return {
        "confidence_high": 0.9,
        "confidence_medium": 0.6,
        "health_low_confidence_weight": 0.3,
        "health_vendor_tension_weight": 0.2,
        "health_probabilistic_weight": 0.5,
        "page_size_default": 25,
    }


@app.get("/api/table-ddl")
def table_ddl(object: str = Query(...)) -> dict:
    try:
        rows = fetch(f"SELECT GET_DDL('TABLE', %s) AS DDL", (object,))
        return {"ddl": rows[0]["DDL"] if rows else ""}
    except Exception as e:
        return {"ddl": f"-- error: {e}"}


# ---- Pipeline run / generate ----------------------------------------------
# SSP-compatible singleton pattern: one generation at a time, no run_id needed.

_generate_job: dict[str, Any] = {"status": "idle"}
_generate_lock = threading.Lock()


def _set_stage(stage: str) -> None:
    with _generate_lock:
        _generate_job["stage"] = stage


@app.post("/api/generate", status_code=202)
def trigger_generate(body: dict = Body(default={})) -> dict:
    with _generate_lock:
        if _generate_job.get("status") == "running":
            raise HTTPException(409, "Generation already in progress")
        _generate_job.clear()
        _generate_job.update({"status": "running", "started_at": datetime.utcnow().isoformat()})
    scale = max(1, min(int(body.get("record_scale", 1)), 50))
    threading.Thread(target=_generate_worker, args=(scale,), daemon=True).start()
    return {"status": "running"}


def _generate_worker(scale: int = 1) -> None:
    try:
        gen = MartechDataGenerator(
            db_name=MARTECH_DB,
            # None in SPCS (no SNOWFLAKE_CONNECTION_NAME env) -> _connect() falls
            # through to the SPCS OAuth token at /snowflake/session/token. A bogus
            # default here forces the named-connection path and fails in-container.
            connection_name=os.getenv("SNOWFLAKE_CONNECTION_NAME"),
            num_customers=10_000 * scale,
            num_events=100_000 * scale,
        )
        summary = gen.run_all(stage_callback=_set_stage)

        with _generate_lock:
            _generate_job.update({
                "status": "complete",
                "message": f"Generated and loaded {summary.pos_rows + summary.loyalty_rows + summary.web_rows + summary.shopify_rows} records across 4 sources",
                "stage": "complete",
                "generated": {
                    "pos_transactions": summary.pos_rows,
                    "loyalty_members": summary.loyalty_rows,
                    "web_clickstream": summary.web_rows,
                    "shopify_orders": summary.shopify_rows,
                    "total": summary.pos_rows + summary.loyalty_rows + summary.web_rows + summary.shopify_rows,
                },
            })
    except Exception as e:
        log.exception("Data generation failed")
        with _generate_lock:
            _generate_job.update({
                "status": "failed",
                "error": "Data generation failed. Check backend logs.",
            })


@app.get("/api/generate/status")
def generate_status() -> dict:
    with _generate_lock:
        return dict(_generate_job)


# ---- Run history / task runs ----------------------------------------------

@app.get("/api/run-history")
def run_history(minutes: int = 1440, limit: int = 50) -> list:
    """Aggregate IDR_COMPLETE events into a per-run summary matching the
    SSP RunRecord shape the frontend expects."""
    rows = fetch(f"""
        SELECT
            e.RUN_ID,
            e.EVENT_TIMESTAMP AS COMPLETED_AT,
            e.PROCESSING_TIME_MS,
            e.EVENT_DETAILS,
            (SELECT MIN(EVENT_TIMESTAMP)
             FROM {MARTECH_DB}.SILVER.IDR_CORE_EVENT_LOG s
             WHERE s.RUN_ID = e.RUN_ID) AS STARTED_AT
        FROM {MARTECH_DB}.SILVER.IDR_CORE_EVENT_LOG e
        WHERE e.EVENT_TYPE = 'IDR_COMPLETE'
          AND e.EVENT_TIMESTAMP >= DATEADD('minute', -{minutes}, CURRENT_TIMESTAMP())
        ORDER BY e.EVENT_TIMESTAMP DESC
        LIMIT {limit}
    """)
    out = []
    for r in rows:
        details = parse_json_field(r.get("EVENT_DETAILS")) or {}
        if not isinstance(details, dict):
            details = {}
        totals = details.get("totals", {}) if isinstance(details, dict) else {}
        profiles = totals.get("profiles", {}) or {}
        records = totals.get("records", {}) or {}
        matches = totals.get("matches", {}) or {}
        source_recs = totals.get("source_records", {}) or {}
        duration_ms = r.get("PROCESSING_TIME_MS") or details.get("total_duration_ms") or 0
        started = r.get("STARTED_AT")
        completed = r.get("COMPLETED_AT")
        stages = details.get("stages", {}) or {}
        ml_candidates = (stages.get("ml_scoring", {}) or {}).get("results", {}).get("candidates", {}).get("totals", {})
        ml_pairs_count = ml_candidates.get("pairs_inserted", 0) + ml_candidates.get("pairs_appended", 0)
        ml_scoring = (stages.get("ml_scoring", {}) or {}).get("results", {}).get("scoring", {})
        ml_upgraded_count = ml_scoring.get("edges_emitted", 0) if ml_scoring else 0
        out.append({
            "run_id": r.get("RUN_ID"),
            "status": "DONE",
            "started_at": str(started) if started else None,
            "completed_at": str(completed) if completed else None,
            "records_processed": (source_recs.get("total_inserts") or 0) + (source_recs.get("total_deletes") or 0),
            "clusters_created": profiles.get("created") or records.get("created") or 0,
            "clusters_updated": profiles.get("updated") or records.get("updated") or 0,
            "clusters_merged": profiles.get("merged") or records.get("merged") or 0,
            "duration_seconds": (int(duration_ms) // 1000) if duration_ms else 0,
            "ml_pairs": ml_pairs_count,
            "ml_upgraded": ml_upgraded_count,
        })
    return out


@app.get("/api/task-runs")
def task_runs(minutes: int = 1440, limit: int = 50) -> list:
    """Mirror SSP TaskRun shape using TASK_HISTORY information_schema view."""
    # INFORMATION_SCHEMA.TASK_HISTORY only supports up to 7 days lookback
    capped_minutes = min(minutes, 10080)
    try:
        rows = fetch(f"""
            SELECT NAME, STATE, SCHEDULED_TIME, COMPLETED_TIME,
                   DATEDIFF('second', QUERY_START_TIME, COMPLETED_TIME) AS DUR_S,
                   QUERY_ID, ERROR_CODE, ERROR_MESSAGE, RETURN_VALUE
            FROM TABLE({MARTECH_DB}.INFORMATION_SCHEMA.TASK_HISTORY(
                TASK_NAME => 'IDR_INCREMENTAL_TASK',
                SCHEDULED_TIME_RANGE_START => DATEADD('minute', -{capped_minutes}, CURRENT_TIMESTAMP()),
                RESULT_LIMIT => {limit}
            ))
            ORDER BY SCHEDULED_TIME DESC
        """)
    except Exception:
        rows = []
    return [
        {
            "task_name": r.get("NAME"),
            "state": r.get("STATE"),
            "scheduled_time": str(r["SCHEDULED_TIME"]) if r.get("SCHEDULED_TIME") else None,
            "completed_time": str(r["COMPLETED_TIME"]) if r.get("COMPLETED_TIME") else None,
            "duration_seconds": int(r.get("DUR_S") or 0),
            "query_id": r.get("QUERY_ID"),
            "error_code": r.get("ERROR_CODE"),
            "error_message": r.get("ERROR_MESSAGE"),
            "return_value": r.get("RETURN_VALUE"),
        }
        for r in rows
        if r.get("STATE") in ("SUCCEEDED", "FAILED", "EXECUTING")
    ]


@app.get("/api/task-runs/{query_id}/stages")
def task_run_stages(query_id: str) -> list:
    """Fetch per-stage timing breakdown for a pipeline run.
    
    The task QUERY_ID doesn't directly appear in the event log (the SP has its own
    session query ID). We correlate by finding the RUN_ID whose IDR_COMPLETE falls
    within the task's execution window using TASK_HISTORY.
    """
    # Find the RUN_ID by matching the task's time window
    rows = fetch(f"""
        WITH task_window AS (
            SELECT QUERY_START_TIME, COMPLETED_TIME
            FROM TABLE({MARTECH_DB}.INFORMATION_SCHEMA.TASK_HISTORY(
                TASK_NAME => 'IDR_INCREMENTAL_TASK',
                RESULT_LIMIT => 50
            ))
            WHERE QUERY_ID = %s
            LIMIT 1
        )
        SELECT el.EVENT_DETAILS:stage::VARCHAR AS STAGE,
               el.EVENT_TYPE,
               el.PROCESSING_TIME_MS,
               el.EVENT_TIMESTAMP
        FROM {MARTECH_DB}.SILVER.IDR_CORE_EVENT_LOG el, task_window tw
        WHERE el.EVENT_TYPE IN ('IDR_STAGE_COMPLETE', 'IDR_STAGE_START', 'IDR_COMPLETE')
          AND el.EVENT_TIMESTAMP BETWEEN tw.QUERY_START_TIME AND COALESCE(tw.COMPLETED_TIME, CURRENT_TIMESTAMP())
        ORDER BY el.EVENT_TIMESTAMP
    """, (query_id,))
    stages = []
    for r in rows:
        if r.get("EVENT_TYPE") == "IDR_STAGE_COMPLETE":
            stages.append({
                "stage": r.get("STAGE") or "complete",
                "duration_ms": r.get("PROCESSING_TIME_MS") or 0,
            })
        elif r.get("EVENT_TYPE") == "IDR_COMPLETE":
            stages.append({
                "stage": "total",
                "duration_ms": r.get("PROCESSING_TIME_MS") or 0,
            })
    # For EXECUTING runs, also find stages that have started but not completed
    started_stages = [r.get("STAGE") for r in rows if r.get("EVENT_TYPE") == "IDR_STAGE_START"]
    completed_stages = [s["stage"] for s in stages]
    for s in started_stages:
        if s and s not in completed_stages:
            stages.append({"stage": s, "duration_ms": None, "in_progress": True})
    return stages


@app.get("/api/pipeline/run-clusters")
def pipeline_run_clusters(
    event_type: str = Query("CREATED"),
    minutes: int | None = Query(None),
    run_id: str | None = Query(None),
    timestamp: str | None = Query(None),
    window_ms: int = Query(180_000),
) -> list:
    et = event_type.upper()
    if et not in ("CREATED", "MEMBERS_ADDED", "UPDATED", "MERGED", "ML_PAIRS"):
        return []
    if et == "UPDATED":
        et = "MEMBERS_ADDED"

    if et == "ML_PAIRS":
        rows = fetch(f"""
            SELECT PAIR_ID AS LOG_ID, CLUSTER_A AS CLUSTER_ID, 'ML_PAIRS' AS EVENT_TYPE,
                   SCORE AS SOURCE_RECORD_COUNT, SCORED_AT AS CREATED_AT,
                   CLUSTER_A AS IDENTITY_ID,
                   COALESCE(p.PRIMARY_FIRST_NAME || ' ' || p.PRIMARY_LAST_NAME, '') AS NAME
            FROM {MARTECH_DB}.SILVER.ML_PAIR_FEATURES f
            LEFT JOIN {MARTECH_DB}.GOLD.DT_CUSTOMER_PROFILE p ON p.CLUSTER_ID = f.CLUSTER_A
            WHERE DECISION = 'AUTO_MERGE'
            ORDER BY SCORED_AT DESC
            LIMIT 200
        """)
    else:
        event_types = ("UPDATED", "MEMBERS_ADDED") if et == "MEMBERS_ADDED" else (et,)
        et_placeholders = ",".join(["%s"] * len(event_types))
        time_filter = ""
        params: tuple = ()
        if run_id:
            run_window = fetch(f"""
                SELECT MIN(EVENT_TIMESTAMP) AS STARTED, MAX(EVENT_TIMESTAMP) AS ENDED
                FROM {MARTECH_DB}.SILVER.IDR_CORE_EVENT_LOG
                WHERE RUN_ID = %s
            """, (run_id,))
            if run_window and run_window[0].get("STARTED"):
                time_filter = "AND cl.CREATED_AT >= DATEADD('minute', -5, %s::TIMESTAMP_NTZ) AND cl.CREATED_AT <= DATEADD('minute', 5, %s::TIMESTAMP_NTZ)"
                params = (*event_types, str(run_window[0]["STARTED"]), str(run_window[0]["ENDED"]))
            else:
                return []
        elif timestamp:
            window_sec = max(window_ms // 1000, 10)
            time_filter = f"AND cl.CREATED_AT BETWEEN DATEADD('second', -{window_sec}, %s::TIMESTAMP_NTZ) AND DATEADD('second', {window_sec}, %s::TIMESTAMP_NTZ)"
            params = (*event_types, timestamp, timestamp)
        elif minutes:
            time_filter = f"AND cl.CREATED_AT >= DATEADD('minute', -{minutes}, CURRENT_TIMESTAMP())"
            params = (*event_types,)
        else:
            time_filter = "AND cl.CREATED_AT >= DATEADD('minute', -60, CURRENT_TIMESTAMP())"
            params = (*event_types,)

        rows = fetch(f"""
            SELECT cl.LOG_ID, cl.CLUSTER_ID, cl.EVENT_TYPE,
                   COALESCE(ARRAY_SIZE(PARSE_JSON(cl.SOURCE_RECORD_IDS)), 0) AS SOURCE_RECORD_COUNT,
                   cl.CREATED_AT,
                   cl.CLUSTER_ID AS IDENTITY_ID,
                   COALESCE(p.PRIMARY_FIRST_NAME || ' ' || p.PRIMARY_LAST_NAME, '') AS NAME
            FROM {MARTECH_DB}.SILVER.IDR_CORE_CLUSTER_LOG cl
            LEFT JOIN {MARTECH_DB}.GOLD.DT_CUSTOMER_PROFILE p ON p.CLUSTER_ID = cl.CLUSTER_ID
            WHERE cl.EVENT_TYPE IN ({et_placeholders})
            {time_filter}
            ORDER BY cl.CREATED_AT DESC
            LIMIT 200
        """, params)

    return [
        {
            "log_id": r.get("LOG_ID"),
            "cluster_id": r.get("CLUSTER_ID"),
            "event_type": r.get("EVENT_TYPE"),
            "source_record_count": r.get("SOURCE_RECORD_COUNT", 0),
            "created_at": str(r.get("CREATED_AT")) if r.get("CREATED_AT") else None,
            "identity_id": r.get("IDENTITY_ID"),
            "name": r.get("NAME"),
        }
        for r in rows
    ]


# ---- AI cost --------------------------------------------------------------

@app.get("/api/ai-cost")
def ai_cost() -> dict:
    return {"total_credits": 0, "by_model": {}, "by_day": []}


# =============================================================================
# v1 — IDHH Adjudication: dispute queue, ML explainability, pipeline status,
# blocking-strategy admin (read-only)
# =============================================================================

@app.get("/api/disputes")
def list_disputes(
    severity: str = Query("STRONG", description="STRONG | MILD | ALL"),
    status: str = Query("PENDING", description="PENDING | RESOLVED_KEEP | RESOLVED_SPLIT | ALL"),
    limit: int = Query(50, ge=1, le=500),
    cursor: str | None = Query(None, description="DISPUTE_ID to paginate after"),
) -> list:
    """List ML-vs-deterministic disputes. Default: STRONG severity, PENDING status."""
    where = []
    if severity.upper() != "ALL":
        where.append(f"q.DISPUTE_SEVERITY = '{severity.upper()}'")
    if status.upper() != "ALL":
        where.append(f"q.STATUS = '{status.upper()}'")
    if cursor:
        where.append(f"q.DISPUTE_ID > '{cursor}'")
    where_sql = ("WHERE " + " AND ".join(where)) if where else ""

    rows = fetch(f"""
        SELECT q.DISPUTE_ID, q.PAIR_ID,
               q.SOURCE_RECORD_ID_A, q.SOURCE_RECORD_ID_B,
               q.DETERMINISTIC_RULE_ID,
               r.RULE_NAME AS DETERMINISTIC_RULE_NAME,
               q.ML_PROB, q.ML_MODEL_VERSION,
               q.DISPUTE_SEVERITY, q.DISPUTE_REASON,
               q.STATUS, q.CREATED_AT
        FROM {MARTECH_DB}.SILVER.IDR_INDIVIDUAL_MATCH_DISPUTE_QUEUE q
        LEFT JOIN {MARTECH_DB}.CONFIG.IDR_MATCHING_RULES r
            ON r.RULE_ID = q.DETERMINISTIC_RULE_ID
        {where_sql}
        ORDER BY
            CASE q.DISPUTE_SEVERITY WHEN 'STRONG' THEN 0 ELSE 1 END,
            q.CREATED_AT ASC
        LIMIT {limit}
    """)
    return [
        {
            "dispute_id": r.get("DISPUTE_ID"),
            "pair_id": r.get("PAIR_ID"),
            "source_record_id_a": r.get("SOURCE_RECORD_ID_A"),
            "source_record_id_b": r.get("SOURCE_RECORD_ID_B"),
            "deterministic_rule_id": r.get("DETERMINISTIC_RULE_ID"),
            "deterministic_rule_name": r.get("DETERMINISTIC_RULE_NAME"),
            "ml_prob": float(r.get("ML_PROB")) if r.get("ML_PROB") is not None else None,
            "ml_model_version": r.get("ML_MODEL_VERSION"),
            "severity": r.get("DISPUTE_SEVERITY"),
            "reason": r.get("DISPUTE_REASON"),
            "status": r.get("STATUS"),
            "created_at": str(r["CREATED_AT"]) if r.get("CREATED_AT") else None,
        }
        for r in rows
    ]


@app.get("/api/disputes/{dispute_id}")
def get_dispute(dispute_id: str) -> dict:
    """Detail view: includes both source records, deterministic match, ML negative-feature explanation."""
    rows = fetch(f"""
        SELECT q.*, r.RULE_NAME AS DETERMINISTIC_RULE_NAME, r.RULE_DESCRIPTION
        FROM {MARTECH_DB}.SILVER.IDR_INDIVIDUAL_MATCH_DISPUTE_QUEUE q
        LEFT JOIN {MARTECH_DB}.CONFIG.IDR_MATCHING_RULES r
            ON r.RULE_ID = q.DETERMINISTIC_RULE_ID
        WHERE q.DISPUTE_ID = %s
    """, (dispute_id,))
    if not rows:
        raise HTTPException(status_code=404, detail=f"dispute {dispute_id} not found")
    r = rows[0]
    return {
        "dispute_id": r.get("DISPUTE_ID"),
        "pair_id": r.get("PAIR_ID"),
        "source_record_id_a": r.get("SOURCE_RECORD_ID_A"),
        "source_record_id_b": r.get("SOURCE_RECORD_ID_B"),
        "deterministic_match_id": r.get("DETERMINISTIC_MATCH_ID"),
        "deterministic_rule_id": r.get("DETERMINISTIC_RULE_ID"),
        "deterministic_rule_name": r.get("DETERMINISTIC_RULE_NAME"),
        "deterministic_rule_description": r.get("RULE_DESCRIPTION"),
        "ml_prob": float(r.get("ML_PROB")) if r.get("ML_PROB") is not None else None,
        "ml_model_version": r.get("ML_MODEL_VERSION"),
        "ml_top_negative_features": parse_json_field(r.get("ML_TOP_NEGATIVE_FEATURES")),
        "severity": r.get("DISPUTE_SEVERITY"),
        "reason": r.get("DISPUTE_REASON"),
        "llm_verdict": r.get("LLM_VERDICT"),
        "llm_reasoning": r.get("LLM_REASONING"),
        "status": r.get("STATUS"),
        "steward_decision": r.get("STEWARD_DECISION"),
        "steward_reason": r.get("STEWARD_REASON"),
        "steward_user": r.get("STEWARD_USER"),
        "decided_at": str(r["DECIDED_AT"]) if r.get("DECIDED_AT") else None,
        "created_at": str(r["CREATED_AT"]) if r.get("CREATED_AT") else None,
    }


@app.post("/api/disputes/{dispute_id}/decide")
def decide_dispute(dispute_id: str, payload: dict) -> dict:
    """
    Steward decision on a dispute.
    Body: {"decision": "KEEP_MERGE" | "SPLIT", "reason": "...", "user": "..."}
    SPLIT: also inserts a VETO edge into IDR_CORE_MATCH_RESULTS for the disputed pair.
    """
    decision = (payload.get("decision") or "").upper()
    if decision not in ("KEEP_MERGE", "SPLIT"):
        raise HTTPException(status_code=400, detail="decision must be KEEP_MERGE or SPLIT")
    reason = payload.get("reason")
    user = payload.get("user") or "anonymous"
    if decision == "SPLIT" and not reason:
        raise HTTPException(status_code=400, detail="SPLIT requires a reason")

    # Look up the dispute first to get the source record IDs
    drows = fetch(f"""
        SELECT SOURCE_RECORD_ID_A, SOURCE_RECORD_ID_B, STATUS
        FROM {MARTECH_DB}.SILVER.IDR_INDIVIDUAL_MATCH_DISPUTE_QUEUE
        WHERE DISPUTE_ID = %s
    """, (dispute_id,))
    if not drows:
        raise HTTPException(status_code=404, detail=f"dispute {dispute_id} not found")
    if drows[0].get("STATUS") != "PENDING":
        raise HTTPException(status_code=409, detail=f"dispute already resolved with status {drows[0].get('STATUS')}")

    src_a = drows[0]["SOURCE_RECORD_ID_A"]
    src_b = drows[0]["SOURCE_RECORD_ID_B"]
    new_status = "RESOLVED_KEEP" if decision == "KEEP_MERGE" else "RESOLVED_SPLIT"

    # Update dispute row
    execute(f"""
        UPDATE {MARTECH_DB}.SILVER.IDR_INDIVIDUAL_MATCH_DISPUTE_QUEUE
        SET STATUS = %s,
            STEWARD_DECISION = %s,
            STEWARD_REASON = %s,
            STEWARD_USER = %s,
            DECIDED_AT = CURRENT_TIMESTAMP()
        WHERE DISPUTE_ID = %s
    """, (new_status, decision, reason, user, dispute_id))

    # If SPLIT, insert a VETO edge so SP_UPDATE_CLUSTERS will block future merging
    if decision == "SPLIT":
        execute(f"""
            INSERT INTO {MARTECH_DB}.SILVER.IDR_CORE_MATCH_RESULTS
                (SOURCE_RECORD_ID_A, SOURCE_RECORD_ID_B,
                 NEW_SOURCE_RECORD_ID, MATCHED_SOURCE_RECORD_ID,
                 RULE_ID, RULE_NAME, MATCH_SCORE, MATCHED_ON,
                 IS_ACTIVE, IS_CURRENT, EDGE_POLARITY, EDGE_TARGET)
            VALUES (%s, %s, %s, %s,
                    'STEWARD_VETO', %s, NULL, 'STEWARD',
                    TRUE, TRUE, 'VETO', 'INDIVIDUAL')
        """, (src_a, src_b, src_a, src_b, f"Steward dispute split: {reason[:200]}"))

    return {"dispute_id": dispute_id, "status": new_status, "decision": decision}


def _source_type_from_record_id(record_id: str) -> str:
    """Derive source table type from record ID prefix."""
    if record_id.startswith("LM-"):
        return "LOYALTY_MEMBER_RAW"
    elif record_id.startswith("SHO-"):
        return "SHOPIFY_ORDER_RAW"
    elif record_id.startswith("TXN-"):
        return "POS_TRANSACTION_RAW"
    elif record_id.startswith("EVT-"):
        return "WEB_CLICKSTREAM_RAW"
    return "UNKNOWN"


def _source_label_from_record_id(record_id: str) -> str:
    if record_id.startswith("LM-"):
        return "Loyalty"
    elif record_id.startswith("SHO-"):
        return "Shopify"
    elif record_id.startswith("TXN-"):
        return "POS"
    elif record_id.startswith("EVT-"):
        return "Web"
    return "Unknown"


@app.get("/api/match-pair-detail")
def match_pair_detail(record_a: str = Query(...), record_b: str = Query(...), cluster_id: str = Query(...)) -> dict:
    """Fetch full pair detail for a match edge: record attributes + ML/LLM provenance."""
    # Fetch edge from match results
    edges = fetch(f"""
        SELECT mr.MATCH_ID, mr.RULE_ID, r.RULE_NAME, r.MATCH_TYPE,
               mr.MATCH_SCORE, mr.MATCHED_ON, mr.EDGE_POLARITY,
               x.ML_PROB, x.ML_MODEL_VERSION,
               x.TOP_POSITIVE_FEATURES, x.TOP_NEGATIVE_FEATURES,
               x.BLOCKING_KEYS_HIT,
               q.LLM_VERDICT, q.LLM_RATIONALE, q.ML_SCORE AS LLM_ML_SCORE,
               q.STATUS AS LLM_STATUS
        FROM {MARTECH_DB}.SILVER.IDR_CORE_MATCH_RESULTS mr
        LEFT JOIN {MARTECH_DB}.CONFIG.IDR_MATCHING_RULES r ON r.RULE_ID = mr.RULE_ID
        LEFT JOIN {MARTECH_DB}.SILVER.IDR_ML_INDIVIDUAL_MATCH_EXPLAIN x ON x.MATCH_ID = mr.MATCH_ID
        LEFT JOIN {MARTECH_DB}.SILVER.LLM_REVIEW_QUEUE q
            ON LEAST(q.SOURCE_RECORD_ID_A, q.SOURCE_RECORD_ID_B) = LEAST(mr.NEW_SOURCE_RECORD_ID, mr.MATCHED_SOURCE_RECORD_ID)
           AND GREATEST(q.SOURCE_RECORD_ID_A, q.SOURCE_RECORD_ID_B) = GREATEST(mr.NEW_SOURCE_RECORD_ID, mr.MATCHED_SOURCE_RECORD_ID)
        WHERE LEAST(mr.NEW_SOURCE_RECORD_ID, mr.MATCHED_SOURCE_RECORD_ID) = LEAST(%s, %s)
          AND GREATEST(mr.NEW_SOURCE_RECORD_ID, mr.MATCHED_SOURCE_RECORD_ID) = GREATEST(%s, %s)
          AND mr.IS_ACTIVE = TRUE
        LIMIT 1
    """, (record_a, record_b, record_a, record_b))

    edge_data = {}
    if edges:
        e = edges[0]
        edge_data = {
            "rule_id": e.get("RULE_ID"),
            "rule_name": e.get("RULE_NAME"),
            "match_type": e.get("MATCH_TYPE"),
            "score": float(e["MATCH_SCORE"]) if e.get("MATCH_SCORE") is not None else None,
            "matched_on": e.get("MATCHED_ON"),
            "ml_prob": float(e["ML_PROB"]) if e.get("ML_PROB") is not None else None,
            "ml_model_version": e.get("ML_MODEL_VERSION"),
            "top_positive_features": parse_json_field(e.get("TOP_POSITIVE_FEATURES")),
            "top_negative_features": parse_json_field(e.get("TOP_NEGATIVE_FEATURES")),
            "blocking_keys_hit": parse_json_field(e.get("BLOCKING_KEYS_HIT")),
            "llm_verdict": e.get("LLM_VERDICT"),
            "llm_rationale": e.get("LLM_RATIONALE"),
            "llm_ml_score": float(e["LLM_ML_SCORE"]) if e.get("LLM_ML_SCORE") is not None else None,
        }

    # Fetch record attributes
    src_a = _source_type_from_record_id(record_a)
    src_b = _source_type_from_record_id(record_b)
    attrs_a = _fetch_record_attributes(record_a, src_a)
    attrs_b = _fetch_record_attributes(record_b, src_b)

    return {
        "record_a": {"id": record_a, "source": _source_label_from_record_id(record_a), **attrs_a},
        "record_b": {"id": record_b, "source": _source_label_from_record_id(record_b), **attrs_b},
        "edge": edge_data,
    }


@app.get("/api/clusters/{cluster_id}/explain")
def cluster_explain(cluster_id: str) -> dict:
    """All edges in a cluster with provenance: rule, ML explainability, LLM reasoning."""
    edges = fetch(f"""
        SELECT mr.MATCH_ID, mr.SOURCE_RECORD_ID_A, mr.SOURCE_RECORD_ID_B,
               mr.RULE_ID, r.RULE_NAME, r.MATCH_TYPE,
               mr.MATCH_SCORE, mr.MATCHED_ON, mr.EDGE_POLARITY, mr.CREATED_AT,
               x.ML_PROB, x.ML_MODEL_VERSION,
               x.TOP_POSITIVE_FEATURES, x.TOP_NEGATIVE_FEATURES,
               x.BLOCKING_KEYS_HIT,
               q.STEWARD_DECISION, q.STEWARD_REASON, q.LLM_REASONING
        FROM {MARTECH_DB}.SILVER.IDR_CORE_CLUSTER_MEMBERSHIP cm_a
        JOIN {MARTECH_DB}.SILVER.IDR_CORE_CLUSTER_MEMBERSHIP cm_b
            ON cm_a.CLUSTER_ID = cm_b.CLUSTER_ID
            AND cm_a.SOURCE_RECORD_ID < cm_b.SOURCE_RECORD_ID
        JOIN {MARTECH_DB}.SILVER.IDR_CORE_MATCH_RESULTS mr
            ON LEAST(mr.SOURCE_RECORD_ID_A, mr.SOURCE_RECORD_ID_B) = cm_a.SOURCE_RECORD_ID
            AND GREATEST(mr.SOURCE_RECORD_ID_A, mr.SOURCE_RECORD_ID_B) = cm_b.SOURCE_RECORD_ID
            AND mr.IS_ACTIVE = TRUE
        LEFT JOIN {MARTECH_DB}.CONFIG.IDR_MATCHING_RULES r ON r.RULE_ID = mr.RULE_ID
        LEFT JOIN {MARTECH_DB}.SILVER.IDR_ML_INDIVIDUAL_MATCH_EXPLAIN x ON x.MATCH_ID = mr.MATCH_ID
        LEFT JOIN {MARTECH_DB}.SILVER.LLM_REVIEW_QUEUE q
            ON LEAST(q.SOURCE_RECORD_ID_A, q.SOURCE_RECORD_ID_B) = LEAST(mr.SOURCE_RECORD_ID_A, mr.SOURCE_RECORD_ID_B)
           AND GREATEST(q.SOURCE_RECORD_ID_A, q.SOURCE_RECORD_ID_B) = GREATEST(mr.SOURCE_RECORD_ID_A, mr.SOURCE_RECORD_ID_B)
        WHERE cm_a.CLUSTER_ID = %s
        ORDER BY mr.CREATED_AT
    """, (cluster_id,))

    return {
        "cluster_id": cluster_id,
        "edges": [
            {
                "match_id": e.get("MATCH_ID"),
                "source_record_id_a": e.get("SOURCE_RECORD_ID_A"),
                "source_record_id_b": e.get("SOURCE_RECORD_ID_B"),
                "rule_id": e.get("RULE_ID"),
                "rule_name": e.get("RULE_NAME"),
                "match_type": e.get("MATCH_TYPE"),
                "edge_source": _classify_edge_source(e.get("MATCH_TYPE"), e.get("EDGE_POLARITY")),
                "edge_polarity": e.get("EDGE_POLARITY") or "MATCH",
                "match_score": float(e["MATCH_SCORE"]) if e.get("MATCH_SCORE") is not None else None,
                "matched_on": e.get("MATCHED_ON"),
                "ml_prob": float(e["ML_PROB"]) if e.get("ML_PROB") is not None else None,
                "ml_model_version": e.get("ML_MODEL_VERSION"),
                "top_positive_features": parse_json_field(e.get("TOP_POSITIVE_FEATURES")),
                "top_negative_features": parse_json_field(e.get("TOP_NEGATIVE_FEATURES")),
                "blocking_keys_hit": parse_json_field(e.get("BLOCKING_KEYS_HIT")),
                "llm_reasoning": e.get("LLM_REASONING"),
                "steward_decision": e.get("STEWARD_DECISION"),
                "steward_reason": e.get("STEWARD_REASON"),
                "created_at": str(e["CREATED_AT"]) if e.get("CREATED_AT") else None,
            }
            for e in edges
        ],
    }


def _classify_edge_source(match_type: str | None, polarity: str | None) -> str:
    if (polarity or "MATCH") == "VETO":
        return "STEWARD_VETO"
    mt = (match_type or "").upper()
    if mt == "ML":
        return "ML"
    if mt == "LLM":
        return "LLM"
    return "DETERMINISTIC"


@app.get("/api/admin/blocking")
def admin_blocking() -> dict:
    """Read-only view of v1 blocking config: strategies + stoplist."""
    strategies = fetch(f"""
        SELECT STRATEGY_ID, STRATEGY_NAME, STRATEGY_DESCRIPTION,
               BLOCK_KEY_EXPR, MAX_BLOCK_SIZE, PRIORITY, IS_ACTIVE
        FROM {MARTECH_DB}.CONFIG.IDR_ML_INDIVIDUAL_BLOCKING_STRATEGIES
        ORDER BY PRIORITY
    """)
    stoplist = fetch(f"""
        SELECT ATTRIBUTE, PATTERN_TYPE, PATTERN, REASON, IS_ACTIVE
        FROM {MARTECH_DB}.CONFIG.IDR_ML_INDIVIDUAL_BLOCKING_STOPLIST
        WHERE IS_ACTIVE = TRUE
        ORDER BY ATTRIBUTE, PATTERN_TYPE, PATTERN
    """)
    return {
        "strategies": [
            {
                "strategy_id": s.get("STRATEGY_ID"),
                "name": s.get("STRATEGY_NAME"),
                "description": s.get("STRATEGY_DESCRIPTION"),
                "block_key_expr": s.get("BLOCK_KEY_EXPR"),
                "max_block_size": int(s["MAX_BLOCK_SIZE"]) if s.get("MAX_BLOCK_SIZE") is not None else None,
                "priority": int(s["PRIORITY"]) if s.get("PRIORITY") is not None else None,
                "is_active": bool(s.get("IS_ACTIVE")),
            }
            for s in strategies
        ],
        "stoplist": [
            {
                "attribute": s.get("ATTRIBUTE"),
                "pattern_type": s.get("PATTERN_TYPE"),
                "pattern": s.get("PATTERN"),
                "reason": s.get("REASON"),
            }
            for s in stoplist
        ],
    }


@app.get("/api/pipeline/status")
def pipeline_gate_status() -> dict:
    """Reports the current ML-active and LLM-enabled gate states."""
    ml_rows = fetch(f"""
        SELECT COUNT(*) AS C
        FROM {MARTECH_DB}.CONFIG.IDR_MATCHING_RULES
        WHERE MATCH_TYPE = 'ML'
          AND COALESCE(TARGET, 'INDIVIDUAL') = 'INDIVIDUAL'
          AND IS_ACTIVE = TRUE
    """)
    ml_active = bool(ml_rows and int(ml_rows[0].get("C") or 0) > 0)

    llm_rows = fetch(f"""
        SELECT CONFIG_VALUE
        FROM {MARTECH_DB}.CONFIG.IDR_ML_AI_EVALUATION_CONFIG
        WHERE CONFIG_KEY = 'LLM_ADJUDICATION_ENABLED'
    """)
    llm_enabled = bool(llm_rows and str(llm_rows[0].get("CONFIG_VALUE", "")).upper() == "TRUE")

    return {
        "ml_active": ml_active,
        "llm_enabled": llm_enabled,
        "effective_pipeline": (
            "deterministic + ML + LLM + cluster" if (ml_active and llm_enabled)
            else "deterministic + ML + cluster" if ml_active
            else "deterministic + cluster"
        ),
    }


# ─── Test Runner ─────────────────────────────────────────────────────────────
_test_lock = threading.Lock()
_test_status: dict[str, dict] = {}


@app.post("/api/tests/run", status_code=202)
def trigger_tests(skip_llm: bool = True, cleanup: bool = True) -> dict:
    run_id = str(uuid.uuid4())
    with _test_lock:
        _test_status[run_id] = {"status": "RUNNING", "started_at": datetime.utcnow().isoformat()}
    threading.Thread(target=_test_worker, args=(run_id, skip_llm, cleanup), daemon=True).start()
    return {"run_id": run_id, "status": "RUNNING"}


def _test_worker(run_id: str, skip_llm: bool, cleanup: bool) -> None:
    try:
        rows = fetch(
            f"CALL {MARTECH_DB}.APP.SP_RUN_IDR_TESTS(%s, %s)",
            (skip_llm, cleanup),
        )
        result_json = rows[0][list(rows[0].keys())[0]] if rows else "{}"
        result = json.loads(result_json) if isinstance(result_json, str) else result_json
        with _test_lock:
            _test_status[run_id] = {
                "status": "DONE",
                "started_at": _test_status[run_id]["started_at"],
                "finished_at": datetime.utcnow().isoformat(),
                "result": result,
            }
    except Exception as e:
        log.exception(f"Test run failed: {run_id}")
        with _test_lock:
            _test_status[run_id]["status"] = "FAILED"
            _test_status[run_id]["error"] = str(e)


@app.get("/api/tests/status")
def test_status(run_id: str) -> dict:
    with _test_lock:
        if run_id not in _test_status:
            raise HTTPException(404, "Test run not found")
        return _test_status[run_id]


if __name__ == "__main__":
    import uvicorn  # type: ignore
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", "8000")))
