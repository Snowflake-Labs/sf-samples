#!/usr/bin/env python3
"""Render or install the Overture sample through the user's Snowflake CLI connection."""

from __future__ import annotations

import argparse
from decimal import Decimal
import hashlib
import json
import math
from pathlib import Path
import re
import subprocess
import sys
from string import Template
import tomllib
from typing import Any
import uuid

ROOT = Path(__file__).resolve().parents[1]
VERSION = "1.1.0"
MARKER_PREFIX = "overture-maps-semantic-layer:"
# Name Snowsight gives the account's single CoWork object.
COWORK_OBJECT = "SNOWFLAKE_INTELLIGENCE_OBJECT_DEFAULT"
THEMES = ("place", "division", "division_area", "division_boundary")
# Opt-in themes. Each is too large for an unbounded scan (buildings: a bbox
# filter scanned 15 GB in 1.8 s; the same box as a geometry predicate scanned
# 644 GB in 14.5 min), so their views expose BBOX_* columns as required filters.
OPTIONAL_THEMES = ("address", "segment", "building")
S3_THEMES = {"place": "places", "division": "divisions", "division_area": "divisions",
             "division_boundary": "divisions", "address": "addresses",
             "segment": "transportation", "building": "buildings"}
# CARTO Marketplace listings, one per Overture theme family.
LISTINGS = {"places": "GZT0Z4CM1E9KR", "divisions": "GZT0Z4CM1E9M9",
            "addresses": "GZT0Z4CM1E9NQ", "transportation": "GZT0Z4CM1E9KJ",
            "buildings": "GZT0Z4CM1E9KN"}
STAC_URL = "https://stac.overturemaps.org/catalog.json"
RELEASE = re.compile(r"\d{4}-\d{2}-\d{2}\.\d+\Z")
GEOMETRY_DIMENSIONS = {"place": 0, "division": 0, "division_area": 2,
                       "division_boundary": 1, "address": 0, "segment": 1,
                       "building": 2}
IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z0-9_$]*\Z")
REQUIRED = {
    # Overture replaced CATEGORIES with TAXONOMY and BASIC_CATEGORY; the CARTO
    # Marketplace share no longer carries CATEGORIES at all.
    "place": {"ID": "text", "GEOMETRY": "geo", "NAMES": "object",
              "TAXONOMY": "object", "BASIC_CATEGORY": "text", "ADDRESSES": "array"},
    "division": {"ID": "text", "GEOMETRY": "geo", "NAMES": "object",
                 "COUNTRY": "text", "REGION": "text", "SUBTYPE": "text",
                 "POPULATION": "number"},
    "division_area": {"ID": "text", "DIVISION_ID": "text", "GEOMETRY": "geo",
                      "NAMES": "object", "COUNTRY": "text", "REGION": "text",
                      "SUBTYPE": "text", "CLASS": "text"},
    "division_boundary": {"ID": "text", "GEOMETRY": "geo", "COUNTRY": "text",
                          "REGION": "text", "SUBTYPE": "text", "CLASS": "text",
                          "IS_LAND": "boolean"},
    "address": {"ID": "text", "GEOMETRY": "geo", "STREET": "text", "NUMBER": "text",
                "POSTCODE": "text", "POSTAL_CITY": "text", "ADDRESS_LEVELS": "array",
                "COUNTRY": "text", "BBOX": "object"},
    "segment": {"ID": "text", "GEOMETRY": "geo", "NAMES": "object", "SUBTYPE": "text",
                "CLASS": "text", "SUBCLASS": "text", "SPEED_LIMITS": "array",
                "BBOX": "object"},
    "building": {"ID": "text", "GEOMETRY": "geo", "NAMES": "object", "SUBTYPE": "text",
                 "CLASS": "text", "HEIGHT": "float", "NUM_FLOORS": "number",
                 "BBOX": "object"},
}
TYPE_PREFIXES = {
    "text": ("VARCHAR", "TEXT", "STRING"), "geo": ("GEOGRAPHY",),
    "object": ("VARIANT", "OBJECT"), "array": ("VARIANT", "ARRAY"),
    "number": ("NUMBER", "DECIMAL", "INT", "FLOAT", "DOUBLE", "REAL"),
    "float": ("FLOAT", "DOUBLE", "REAL", "NUMBER", "DECIMAL"),
    "boolean": ("BOOLEAN",),
}
ALL_THEMES = THEMES + OPTIONAL_THEMES
OBJECTS = {
    "VIEW": [theme.upper() for theme in ALL_THEMES],
    "SEMANTIC VIEW": ["OVERTURE_MAPS_SV"],
    "CORTEX SEARCH SERVICE": ["CATEGORY_SEARCH"],
    "AGENT": ["OVERTURE_MAPS_AGENT"],
    "TABLE": ["DATASET_INFO", "CATEGORY_VOCAB"] + [f"RAW_{theme.upper()}" for theme in ALL_THEMES],
    "STAGE": ["OVERTURE_S3"],
    "FILE FORMAT": ["OVERTURE_PARQUET"],
}


def identifier(value: str, parts: int = 1) -> str:
    """Accept explicit unquoted identifiers; reject SQL expressions and ambiguous names."""
    if not isinstance(value, str):
        raise ValueError("Identifiers must be strings")
    components = value.split(".")
    if len(components) != parts or any(not IDENTIFIER.fullmatch(part) for part in components):
        raise ValueError(f"Expected {parts} unquoted identifier component(s): {value!r}")
    return ".".join(f'"{part.upper()}"' for part in components)


def literal(value: str) -> str:
    return "'" + value.replace("\\", "\\\\").replace("'", "''") + "'"


def load_config(path: Path, require_connection: bool = True) -> dict[str, Any]:
    config = tomllib.loads(path.read_text())
    for key in ("role", "warehouse", "database", "schema", "viewer_role"):
        identifier(config[key])
    if require_connection and (
        not isinstance(config.get("connection"), str) or not config["connection"].strip()
    ):
        raise ValueError("A named Snowflake CLI connection is required")
    source = config["source"]
    if source["mode"] not in {"existing", "s3"}:
        raise ValueError("source.mode must be existing or s3")
    for key in ("release", "coverage"):
        if not isinstance(source[key], str) or not source[key].strip():
            raise ValueError(f"source.{key} must be a non-empty string")
    target = f'{config["database"]}.{config["schema"]}'
    optional = source.setdefault("optional_themes", [])
    if (not isinstance(optional, list) or len(set(optional)) != len(optional)
            or not set(optional) <= set(OPTIONAL_THEMES)):
        raise ValueError(f"source.optional_themes must be a subset of {list(OPTIONAL_THEMES)}")
    for theme in THEMES + tuple(optional):
        if source["mode"] == "s3":
            source[theme] = f"{target}.RAW_{theme.upper()}"
        identifier(source[theme], 3)
        if source["mode"] == "existing" and source[theme].upper().rsplit(".", 1)[0] == target.upper():
            raise ValueError("Existing sources must be outside the sample-owned schema")
    integration = config.get("s3", {}).get("storage_integration", "")
    if integration:
        identifier(integration)
    release = config.get("s3", {}).get("release", "latest")
    if release != "latest" and not RELEASE.fullmatch(release):
        raise ValueError("s3.release must be latest or YYYY-MM-DD.N")
    bbox = config.get("s3", {}).get("place_bbox", [])
    if source["mode"] == "s3" and optional and not bbox:
        raise ValueError("Optional themes loaded from S3 require place_bbox; global copies are too large")
    if bbox:
        if len(bbox) != 4 or any(
            isinstance(value, bool) or not isinstance(value, (int, float))
            or not math.isfinite(value) for value in bbox
        ):
            raise ValueError("place_bbox must contain four finite numbers")
        west, south, east, north = bbox
        if not (-180 <= west < east <= 180 and -90 <= south < north <= 90):
            raise ValueError("place_bbox must be [west, south, east, north], no dateline crossing")
    for key, default, maximum in (("statement_timeout_seconds", 1800, 7200),
                                  ("map_rows", 1000, 10000)):
        value = config.setdefault("limits", {}).setdefault(key, default)
        if type(value) is not int or not 1 <= value <= maximum:
            raise ValueError(f"limits.{key} must be an integer from 1 to {maximum}")
    return config


def namespace(config: dict[str, Any]) -> str:
    return identifier(config["database"]) + "." + identifier(config["schema"])


def active_themes(config: dict[str, Any]) -> tuple[str, ...]:
    return THEMES + tuple(config["source"].get("optional_themes", []))


def available_prompts(config: dict[str, Any]) -> list[dict[str, Any]]:
    """Prompts whose required themes are installed; others would reference missing views."""
    themes = set(active_themes(config))
    return [prompt for prompt in json.loads((ROOT / "examples/prompts.json").read_text())
            if set(prompt["requires"]) <= themes]


def marker(config: dict[str, Any]) -> str:
    content = {key: value for key, value in config.items() if key != "connection"}
    digest_builder = hashlib.sha256(json.dumps(content, sort_keys=True).encode())
    for folder in ("sql", "sql/themes", "semantic", "semantic/themes", "agent", "examples",
                   "scripts"):
        for path in sorted((ROOT / folder).glob("*")):
            if path.is_file():
                digest_builder.update(path.relative_to(ROOT).as_posix().encode())
                digest_builder.update(path.read_bytes())
    digest = digest_builder.hexdigest()[:16]
    return f"{MARKER_PREFIX}{VERSION}:{digest}"


def owned(comment: Any) -> bool:
    """Any version of this sample owns the object; the full marker only records which."""
    return isinstance(comment, str) and comment.startswith(MARKER_PREFIX)


def context(config: dict[str, Any]) -> dict[str, str]:
    return {
        "ns": namespace(config),
        "warehouse": identifier(config["warehouse"]),
        "owner_comment": literal(marker(config)),
        "map_rows": str(config["limits"]["map_rows"]),
        **{f"source_{theme}": identifier(config["source"][theme], 3)
           for theme in active_themes(config)},
    }


BBOX_GUIDANCE = (
    " Tables {tables} are global and very large. Every query on them must filter "
    "bbox_xmin, bbox_xmax, bbox_ymin and bbox_ymax with literal coordinates spanning "
    "a small area before any geometry predicate; a geometry predicate alone scans the "
    "whole table. If no area is given, ask for one. Their height, floors and subtype "
    "fields are often NULL, so never present a NULL-heavy count as absence."
)


def theme_sections(theme: str) -> dict[str, str]:
    text = (ROOT / "semantic/themes" / f"{theme}.sql").read_text()
    sections = {}
    for block in text.split("@@")[1:]:
        name, _, body = block.partition("\n")
        sections[name.strip()] = body.rstrip()
    if set(sections) != {"tables", "facts", "dimensions", "metrics"}:
        raise ValueError(f"semantic/themes/{theme}.sql must define all four sections")
    return sections


def semantic_extras(config: dict[str, Any]) -> dict[str, str]:
    optional = config["source"].get("optional_themes", [])
    extras = {f"extra_{name}": "" for name in ("tables", "facts", "dimensions", "metrics")}
    for theme in optional:
        for name, body in theme_sections(theme).items():
            # Fragments are inserted as values, so render their own placeholders first.
            extras[f"extra_{name}"] += ",\n" + Template(body).substitute(context(config))
    extras["extra_guidance"] = (
        BBOX_GUIDANCE.format(tables=", ".join(theme.upper() for theme in optional))
        if optional else "")
    return extras


def install_files(config: dict[str, Any]) -> list[str]:
    return (["sql/10_sources.sql"]
            + [f"sql/themes/{theme}.sql" for theme in config["source"].get("optional_themes", [])]
            + ["semantic/overture.sql", "sql/30_category_search.sql"])


def render_file(name: str, config: dict[str, Any], **values: str) -> str:
    if name == "semantic/overture.sql":
        values["verified_queries"] = verified_queries(config)
        values.update(semantic_extras(config))
    return Template((ROOT / name).read_text()).substitute(context(config), **values).strip()


class SnowCLI:
    def __init__(self, config: dict[str, Any]):
        self.config = config

    def query(self, sql: str) -> list[dict[str, Any]]:
        settings = (
            "use secondary roles none;\n"
            f"alter session set statement_timeout_in_seconds = "
            f"{self.config['limits']['statement_timeout_seconds']};\n"
        )
        result = subprocess.run(
            ["snow", "sql", "--connection", self.config["connection"],
             "--role", self.config["role"], "--warehouse", self.config["warehouse"],
             "--database", self.config["database"], "--format", "JSON",
             "--enhanced-exit-codes", "--enable-templating", "NONE",
             "--query", settings + sql],
            text=True, capture_output=True, check=True,
        )
        decoder = json.JSONDecoder()
        remaining = result.stdout.strip()
        payload: Any = []
        while remaining:
            payload, end = decoder.raw_decode(remaining)
            remaining = remaining[end:].strip()
        if not isinstance(payload, list):
            raise ValueError("Unexpected Snowflake CLI JSON result; expected a list")
        if payload and isinstance(payload[0], list):
            payload = payload[-1]
        if any(not isinstance(row, dict) for row in payload):
            raise ValueError("Unexpected Snowflake CLI result rows")
        return [{key.upper(): value for key, value in row.items()} for row in payload]


def validate_columns(theme: str, rows: list[dict[str, Any]]) -> None:
    columns = {row["NAME"]: row["TYPE"].upper() for row in rows}
    for name, kind in REQUIRED[theme].items():
        if name not in columns or not columns[name].startswith(TYPE_PREFIXES[kind]):
            raise ValueError(f"{theme}.{name}: expected {kind}, got {columns.get(name, 'missing')}")


def preflight(config: dict[str, Any], client: SnowCLI) -> None:
    rows = client.query("select current_account() as account, current_role() as role")
    print(json.dumps(rows), file=sys.stderr)
    for theme in active_themes(config):
        source = identifier(config["source"][theme], 3)
        validate_columns(theme, client.query(f"describe table {source}"))
        sample = client.query(f"select id from {source} limit 1")
        if not sample:
            raise ValueError(f"{source} is empty; missing data is not an installed theme")
        expected_dimension = GEOMETRY_DIMENSIONS[theme]
        geometry = client.query(
            "select count_if(geometry is null "
            f"or st_dimension(geometry) != {expected_dimension}) as invalid "
            f"from (select geometry from {source} limit 100)"
        )[0]
        if int(geometry["INVALID"] or 0):
            raise ValueError(f"{source}: null or unexpected geometry in the first 100 rows")
    place = identifier(config["source"]["place"], 3)
    sample = client.query(
        f"select typeof(to_variant(names)) as names_type, "
        f"typeof(to_variant(taxonomy)) as taxonomy_type, "
        f"typeof(to_variant(addresses)) as addresses_type from {place} "
        "where names is not null and taxonomy is not null and addresses is not null limit 1"
    )
    if not sample or sample[0] != {
        "NAMES_TYPE": "OBJECT", "TAXONOMY_TYPE": "OBJECT", "ADDRESSES_TYPE": "ARRAY"
    }:
        raise ValueError("Expected Overture objects for names/taxonomy and an addresses array")
    print("Source contract passed (metadata and sample, not a full quality audit).", file=sys.stderr)


def ensure_namespace(config: dict[str, Any], client: SnowCLI, create: bool = False) -> None:
    database = identifier(config["database"])
    rows = client.query(
        f"select schema_name, comment from {database}.information_schema.schemata "
        f"where schema_name = {literal(config['schema'].upper())}"
    )
    if not rows:
        if not create:
            raise ValueError("Sample schema not found")
        client.query(f"create schema {namespace(config)} comment = {literal(marker(config))}")
    elif not owned(rows[0].get("COMMENT")):
        raise ValueError("Schema exists but was not created by this sample; choose another schema")
    elif create and rows[0]["COMMENT"] != marker(config):
        # Upgrade in place: the one schema keeps its name and records the new build.
        client.query(f"alter schema {namespace(config)} set comment = {literal(marker(config))}")


def check_owned_objects(config: dict[str, Any], client: SnowCLI,
                        allow_candidates: bool = False) -> None:
    kinds = {"VIEW": "VIEWS", "TABLE": "TABLES", "SEMANTIC VIEW": "SEMANTIC VIEWS",
             "AGENT": "AGENTS", "CORTEX SEARCH SERVICE": "CORTEX SEARCH SERVICES",
             "STAGE": "STAGES", "FILE FORMAT": "FILE FORMATS"}
    for kind, plural in kinds.items():
        for row in client.query(f"show {plural} in schema {namespace(config)}"):
            name = row["NAME"]
            candidate = (allow_candidates and kind == "TABLE"
                         and re.fullmatch(r"LOAD_(" + "|".join(t.upper() for t in ALL_THEMES)
                                          + r")_[A-F0-9]{8}", name))
            if (name not in OBJECTS[kind] and not candidate) or not owned(row.get("COMMENT")):
                raise ValueError(f"Unrecognized or modified {kind} {name}; refusing to mutate schema")


def published_release(document: Any) -> str:
    """Return the release Overture marks as published in its STAC catalog."""
    release = document.get("latest") if isinstance(document, dict) else None
    if not isinstance(release, str) or not RELEASE.fullmatch(release):
        raise ValueError("STAC catalog has no valid published latest release")
    return release


def fetch_published_release() -> str:
    # A fixed public metadata GET; no account data or credentials leave here.
    from urllib.request import urlopen
    try:
        with urlopen(STAC_URL, timeout=30) as response:
            return published_release(json.load(response))
    except OSError as error:
        raise ValueError(f"Could not read {STAC_URL} ({error}); pin s3.release instead") from error


def resolve_release(rows: list[dict[str, Any]], requested: str,
                    themes: tuple[str, ...] = THEMES) -> str:
    """Check the requested release is complete in the bucket for every theme.

    "latest" is resolved from STAC by the caller, never from bucket listing order:
    a newer release directory can exist in the bucket before it is published.
    """
    releases: dict[str, set[str]] = {}
    pattern = r"release/(\d{4}-\d{2}-\d{2}\.\d+)/theme=\w+/type=(\w+)/"
    for row in rows:
        match = re.search(pattern, row["NAME"])
        if match:
            releases.setdefault(match[1], set()).add(match[2])
    if requested in releases and set(themes) <= releases[requested]:
        return requested
    raise ValueError(f"Release {requested} does not have all required types "
                     f"{list(themes)} in the bucket; nothing will be loaded")


def choose_source(existing: dict[str, Any] | None, listing: dict[str, Any] | None) -> str:
    """Pick a theme source, failing closed when Marketplace readiness is unknown.

    existing is the SHOW DATABASES row for the theme database, listing the
    SHOW AVAILABLE LISTINGS row. A permission or unknown readiness state must not
    be mistaken for unavailability and silently trigger an S3 load.
    """
    if existing:
        if str(existing.get("KIND", "")).upper() == "IMPORTED DATABASE" or existing.get("ORIGIN"):
            return "imported"
        return "existing"
    if listing is None:
        return "s3"
    ready = str(listing.get("IS_READY_FOR_IMPORT", "")).lower()
    if ready in {"true", "yes", "1"}:
        return "marketplace"
    if ready in {"false", "no", "0"}:
        return "s3"
    raise ValueError("Listing readiness is unknown; refusing to infer an S3 fallback")


def projection(theme: str) -> str:
    # allow_invalid keeps parseable but self-intersecting shapes, as the
    # Marketplace share does; strict conversion silently dropped 7 of 1,084,259
    # division areas. Corrupt WKB still yields NULL and fails the load check.
    types = {"text": "varchar", "object": "variant", "array": "variant",
             "number": "number", "boolean": "boolean", "float": "float"}
    return ",\n       ".join(
        "try_to_geography($1:geometry::binary, true) as GEOMETRY" if kind == "geo"
        else f"$1:{name.lower()}::{types[kind]} as {name}"
        for name, kind in REQUIRED[theme].items()
    )


def load_s3(config: dict[str, Any], client: SnowCLI, load_id: str | None = None,
            release: str | None = None) -> None:
    if config["source"]["mode"] != "s3":
        raise ValueError("load-s3 requires source.mode = 's3'")
    if load_id is not None and not re.fullmatch(r"[A-F0-9]{8}", load_id):
        raise ValueError("load_id must be eight uppercase hexadecimal characters")
    requested = config.get("s3", {}).get("release", "latest")
    if requested != "latest" and release and release != requested:
        raise ValueError("Release override conflicts with the pinned s3.release")
    themes = active_themes(config)
    ensure_namespace(config, client, create=True)
    check_owned_objects(config, client)
    ns = namespace(config)
    existing = client.query(f"show tables in schema {ns}")
    if any(row["NAME"].startswith("RAW_") or row["NAME"] == "DATASET_INFO" for row in existing):
        raise ValueError("S3 data is already loaded in this schema. Install reuses it; "
                         "to load a different release or area, run cleanup first.")
    integration = config.get("s3", {}).get("storage_integration", "")
    client.query(render_file("sql/05_s3_setup.sql", config,
                            integration=f"storage_integration = {identifier(integration)}"
                            if integration else ""))
    if requested != "latest":
        release = requested
    else:
        release = published_release({"latest": release}) if release else fetch_published_release()
    files = client.query(
        f"list @{ns}.OVERTURE_S3/{release}/ "
        f"pattern = '.*/type=({'|'.join(themes)})/part-00000.*[.]parquet'"
    )
    release = resolve_release(files, release, themes)
    candidates: list[tuple[str, str]] = []
    bbox = config.get("s3", {}).get("place_bbox", [])
    for theme in themes:
        suffix = load_id or uuid.uuid4().hex[:8].upper()
        candidate = f"{ns}.LOAD_{theme.upper()}_{suffix}"
        candidates.append((candidate, f"{ns}.RAW_{theme.upper()}"))
        where = ""
        if (theme == "place" or theme in OPTIONAL_THEMES) and bbox:
            west, south, east, north = bbox
            where = (f"where $1:bbox:xmin::float <= {east} and $1:bbox:xmax::float >= {west} "
                     f"and $1:bbox:ymin::float <= {north} and $1:bbox:ymax::float >= {south}")
        print(f"Loading {theme}, release {release}", file=sys.stderr)
        client.query(render_file("sql/06_load_table.sql", config, candidate=candidate,
                                projection=projection(theme), release=release,
                                theme=S3_THEMES[theme],
                                type=theme, filter=where))
        quality = client.query(
            f"select count(*) as row_count, count_if(geometry is null "
            f"or st_dimension(geometry) != {GEOMETRY_DIMENSIONS[theme]}) "
            f"as invalid, count_if(geometry is not null and not st_isvalid(geometry)) "
            f"as self_intersecting, count_if(id is null) as null_ids, "
            f"count(distinct id) as ids from {candidate}"
        )[0]
        if (not int(quality["ROW_COUNT"]) or int(quality["INVALID"] or 0)
                or int(quality["NULL_IDS"] or 0)
                or int(quality["ROW_COUNT"]) != int(quality["IDS"])):
            raise ValueError(f"Load validation failed for {candidate}: {quality}. "
                             "Candidates retained for inspection; no views published.")
        if int(quality.get("SELF_INTERSECTING") or 0):
            print(f"{theme}: kept {quality['SELF_INTERSECTING']} invalid but parseable "
                  "geometries, matching the Marketplace share", file=sys.stderr)
    orphan = client.query(
        f"select count(*) as orphan_count from {candidates[2][0]} areas "
        f"left join {candidates[1][0]} divisions on areas.division_id = divisions.id "
        "where areas.division_id is not null and divisions.id is null"
    )[0]
    if int(orphan["ORPHAN_COUNT"]):
        raise ValueError("Area-to-division relationship has orphan keys; candidates retained")
    for candidate, target in candidates:
        client.query(f"alter table {candidate} rename to {target}")
    optional = config["source"].get("optional_themes", [])
    coverage = f"Places bbox {bbox or 'global'}; divisions global"
    if optional:
        coverage += f"; {', '.join(optional)} bbox {bbox}"
    create_dataset_info(config, client, release, coverage)


def create_dataset_info(config: dict[str, Any], client: SnowCLI,
                        release: str, coverage: str) -> None:
    client.query(dataset_sql(config, release, coverage))


def dataset_sql(config: dict[str, Any], release: str, coverage: str) -> str:
    return (
        f"create or replace table {namespace(config)}.DATASET_INFO "
        f"comment = {literal(marker(config))} as select {literal(release)} as release, "
        f"{literal(coverage)} as coverage, {literal(config['source']['mode'])} as source_mode, "
        "current_timestamp() as recorded_at;"
    )


def verified_queries(config: dict[str, Any]) -> str:
    queries = []
    for prompt in available_prompts(config):
        sql = Template(prompt["reference_sql"]).substitute(ns=namespace(config))
        queries.append(f"{prompt['id']} as (question {literal(prompt['question'])} "
                       f"sql {literal(sql)})")
    return "ai_verified_queries (\n" + ",\n".join(queries) + "\n)"


def agent_sql(config: dict[str, Any], dataset: dict[str, Any]) -> str:
    ns = namespace(config)
    prompts = available_prompts(config)
    instructions = (ROOT / "agent/instructions.md").read_text()
    optional = config["source"].get("optional_themes", [])
    if optional:
        instructions += "\n" + (ROOT / "agent/large_themes.md").read_text()
    instructions += (f"\nInstalled source metadata: {json.dumps(dataset)}\n"
                     f"Return at most {config['limits']['map_rows']} map rows.")
    spec = {
        "models": {"orchestration": "auto"},
        "orchestration": {"budget": {"seconds": 120, "tokens": 16000}},
        "instructions": {
            "orchestration": instructions,
            "response": "Lead with the answer. State units, scope and relevant data limitations.",
            "sample_questions": [{"question": item["question"]} for item in prompts
                                 if not (config["source"]["mode"] == "s3"
                                         and config.get("s3", {}).get("place_bbox")
                                         and item["id"] == "berlin_hexagons")],
        },
        "tools": [
            {"tool_spec": {"type": "cortex_analyst_text_to_sql", "name": "overture_maps",
                           "description": "Query Overture places and administrative geography. "
                           "Supports literal-point distance, containment, H3 density and map shapes. "
                           "No modeled place-to-area spatial join."
                           + (f" Also {', '.join(optional)} with mandatory bbox filters."
                              if optional else "")}},
            {"tool_spec": {"type": "cortex_search", "name": "category_vocabulary",
                           "description": "Resolve place types to primary category tags present "
                           "in this installation. Search first for category questions. "
                           "An absent search match does not prove a category does not exist."}},
        ],
        "tool_resources": {
            "overture_maps": {"semantic_view": f"{ns}.OVERTURE_MAPS_SV",
                              "execution_environment": {
                                  "type": "warehouse", "warehouse": config["warehouse"].upper()}},
            "category_vocabulary": {"search_service": f"{ns}.CATEGORY_SEARCH",
                                    "max_results": 8, "title_column": "CATEGORY",
                                    "id_column": "CATEGORY"},
        },
    }
    return (f"create or replace agent {ns}.OVERTURE_MAPS_AGENT "
            f"comment = {literal(marker(config))} "
            "profile = '{\"display_name\": \"Overture Maps\"}' "
            f"from specification {literal(json.dumps(spec))};")


def grant_sql(config: dict[str, Any], cowork: str | None = None) -> str:
    if config["role"].upper() == config["viewer_role"].upper():
        return ""
    ns = namespace(config)
    role = identifier(config["viewer_role"])
    statements = [f"grant usage on database {identifier(config['database'])} to role {role}",
                  f"grant usage on schema {ns} to role {role}",
                  f"grant usage on warehouse {identifier(config['warehouse'])} to role {role}"]
    for name in [theme.upper() for theme in active_themes(config)] + ["DATASET_INFO"]:
        statements.append(f"grant select on view {ns}.{name} to role {role}" if name != "DATASET_INFO"
                          else f"grant select on table {ns}.{name} to role {role}")
    statements += [f"grant select on semantic view {ns}.OVERTURE_MAPS_SV to role {role}",
                   f"grant usage on agent {ns}.OVERTURE_MAPS_AGENT to role {role}",
                   f"grant usage on cortex search service {ns}.CATEGORY_SEARCH to role {role}"]
    # Without USAGE on the CoWork object the viewer sees an empty agent list.
    statements.append(f"grant usage on snowflake intelligence {cowork or identifier(COWORK_OBJECT)} "
                      f"to role {role}")
    return ";\n".join(statements) + ";"


def cowork_object(client: Any, create: bool = False) -> str | None:
    """Return the account's single CoWork object, creating the default one if asked."""
    rows = client.query("show snowflake intelligences")
    if rows:
        return identifier(rows[0]["NAME"])
    if not create:
        return None
    client.query(f"create snowflake intelligence if not exists {identifier(COWORK_OBJECT)}")
    return identifier(COWORK_OBJECT)


def cowork_listed(config: dict[str, Any], client: Any, cowork: str) -> bool:
    return any(row.get("DATABASE_NAME") == config["database"].upper()
               and row.get("SCHEMA_NAME") == config["schema"].upper()
               and row.get("NAME") == "OVERTURE_MAPS_AGENT"
               for row in client.query(f"show agents in snowflake intelligence {cowork}"))


def register_cowork(config: dict[str, Any], client: Any) -> str:
    """Add the agent to CoWork; rerun after every agent replace.

    When an account has a CoWork object, only agents added to it appear in
    CoWork. ADD AGENT fails if the agent is already present, so check first
    instead of catching errors (the agent path cannot run scripting blocks).
    """
    try:
        cowork = cowork_object(client, create=True)
        if not cowork_listed(config, client, cowork):
            client.query(f"alter snowflake intelligence {cowork} add agent "
                         f"{namespace(config)}.OVERTURE_MAPS_AGENT")
    except subprocess.CalledProcessError as error:
        raise ValueError(
            "Could not add the agent to Snowflake CoWork. The installer role needs "
            "CREATE SNOWFLAKE INTELLIGENCE ON ACCOUNT (if no CoWork object exists) or "
            f"MODIFY on the CoWork object. {error.stderr or ''}".strip()) from error
    return cowork


def verify(config: dict[str, Any], client: SnowCLI) -> None:
    ns = namespace(config)
    first_place_id = None
    for theme in active_themes(config):
        rows = client.query(f"select id from {ns}.{theme.upper()} limit 1")
        if not rows:
            raise ValueError(f"Normalized {theme} is empty")
        if theme == "place":
            first_place_id = rows[0]["ID"]
    if first_place_id is None:
        raise ValueError("PLACE has a null ID")
    semantic_rows = client.query(
        f"select * from semantic_view({ns}.OVERTURE_MAPS_SV "
        "dimensions places.place_id, places.name metrics places.map_latitude, "
        f"places.map_longitude where places.place_id = {literal(first_place_id)}) limit 1"
    )
    if not semantic_rows:
        raise ValueError("Semantic view did not return the sampled place")
    client.query(f"describe agent {ns}.OVERTURE_MAPS_AGENT")
    search = client.query(f"describe cortex search service {ns}.CATEGORY_SEARCH")[0]
    if search.get("SERVING_STATE") != "ACTIVE" or search.get("INDEXING_ERROR"):
        raise ValueError(f"Search service is not ready: {search}")
    cowork = cowork_object(client)
    if cowork is None or not cowork_listed(config, client, cowork):
        raise ValueError("The agent is not registered in Snowflake CoWork, so users "
                         "cannot see it there. Rerun install with a role that has "
                         "MODIFY on the CoWork object.")
    print(f"Data/model/search checks passed. Agent: {ns}.OVERTURE_MAPS_AGENT")
    print(f"CoWork: agent registered in {cowork}.")
    print("Agent responses and native maps: NOT TESTED. Run examples in CoWork as the viewer.")


def finite_number(value: Any) -> bool:
    return (not isinstance(value, bool) and isinstance(value, (int, float, Decimal))
            and math.isfinite(value))


def valid_polygon(value: Any) -> bool:
    """Check complete display GeoJSON structure, not polygon topology or rendering."""
    if not isinstance(value, str):
        return False
    try:
        shape = json.loads(value)
    except (ValueError, RecursionError):
        return False
    if not isinstance(shape, dict):
        return False
    coordinates = shape.get("coordinates")
    if shape.get("type") == "Polygon":
        polygons = [coordinates]
    elif shape.get("type") == "MultiPolygon":
        polygons = coordinates
    else:
        return False
    if not isinstance(polygons, list) or not polygons:
        return False
    for polygon in polygons:
        if not isinstance(polygon, list) or not polygon:
            return False
        for ring in polygon:
            if not isinstance(ring, list) or len(ring) < 4 or ring[0] != ring[-1]:
                return False
            for point in ring:
                if (not isinstance(point, list) or len(point) != 2
                        or not all(finite_number(value) for value in point)
                        or not -180 <= point[0] <= 180 or not -90 <= point[1] <= 90):
                    return False
    return True


def valid_lines(value: Any) -> bool:
    """Check complete LineString/MultiLineString GeoJSON structure, not rendering."""
    if not isinstance(value, str):
        return False
    try:
        shape = json.loads(value)
    except (ValueError, RecursionError):
        return False
    if not isinstance(shape, dict):
        return False
    coordinates = shape.get("coordinates")
    if shape.get("type") == "LineString":
        lines = [coordinates]
    elif shape.get("type") == "MultiLineString":
        lines = coordinates
    else:
        return False
    if not isinstance(lines, list) or not lines:
        return False
    for line in lines:
        if not isinstance(line, list) or len(line) < 2:
            return False
        for point in line:
            if (not isinstance(point, list) or len(point) != 2
                    or not all(finite_number(value) for value in point)
                    or not -180 <= point[0] <= 180 or not -90 <= point[1] <= 90):
                return False
    return True


def validate_map_rows(prompt: dict[str, Any], rows: list[dict[str, Any]],
                      client: Any) -> None:
    """Validate the bounded reference result; never scan the source again."""
    layer = prompt["layer"]
    if layer not in {"latlon", "h3", "geojson", "lines"}:
        raise ValueError(f"Unknown map layer for {prompt['id']}")
    seen = set()
    for index, row in enumerate(rows, 1):
        context = f"{prompt['id']}: row {index}"
        if not set(prompt["columns"]) <= set(row):
            raise ValueError(f"{context}: missing output columns")
        key = row["H3_CELL" if layer == "h3" else "ID"]
        if not isinstance(key, str) or not key.strip() or key in seen:
            raise ValueError(f"{context}: expected a unique nonempty string ID")
        seen.add(key)
        for column in ("PLACE_COUNT", "AREA_SQKM", "DISTANCE_M", "LENGTH_KM", "HEIGHT_M"):
            if column in prompt["columns"] and not finite_number(row[column]):
                raise ValueError(f"{context}: {column} must be finite and numeric")
        if layer == "latlon":
            for column, bound in (("LATITUDE", 90), ("LONGITUDE", 180)):
                if not finite_number(row[column]) or not -bound <= row[column] <= bound:
                    raise ValueError(f"{context}: {column} must be numeric and in range")
        elif layer == "geojson" and not valid_polygon(row["GEOJSON"]):
            raise ValueError(f"{context}: expected complete Polygon/MultiPolygon GeoJSON")
        elif layer == "lines" and not valid_lines(row["GEOJSON"]):
            raise ValueError(f"{context}: expected complete LineString/MultiLineString GeoJSON")
    if layer == "h3" and rows:
        cells = ",".join(f"({literal(row['H3_CELL'])})" for row in rows)
        checks = client.query(
            "select count(*) as checked, coalesce(count_if(case "
            "when h3_is_valid_cell(column1) then h3_get_resolution(column1) != 8 "
            f"else true end), 0) as invalid from values {cells}"
        )
        if (len(checks) != 1 or checks[0].get("CHECKED") != len(rows)
                or checks[0].get("INVALID") != 0):
            raise ValueError(f"{prompt['id']}: H3 validity/resolution-8 check failed")


def verify_prompts(config: dict[str, Any], client: SnowCLI) -> None:
    for prompt in available_prompts(config):
        sql = Template(prompt["reference_sql"]).substitute(ns=namespace(config))
        rows = client.query(sql)
        if not rows:
            print(f"{prompt['id']}: EMPTY; check {prompt['coverage']}")
            continue
        validate_map_rows(prompt, rows, client)
        print(f"{prompt['id']}: reference SQL and map contract passed for {len(rows)} rows. "
              "Agent equivalence and map rendering require separate checks.")


def run(command: str, config: dict[str, Any], execute: bool) -> None:
    files = install_files(config)
    dataset = {"RELEASE": config["source"]["release"], "COVERAGE": config["source"]["coverage"]}
    if command == "render":
        print("-- Review only. Preflight and schema ownership checks require the installer.")
        print(f"use role {identifier(config['role'])};")
        print(f"use warehouse {identifier(config['warehouse'])};")
        print(f"create schema {namespace(config)} comment = {literal(marker(config))};")
        if config["source"]["mode"] == "s3":
            print("-- S3 loading is a separate command. Read actual release/coverage from DATASET_INFO.")
            print("-- Agent below uses placeholder metadata; use install after load-s3 for execution.")
        else:
            print(dataset_sql(config, dataset["RELEASE"], dataset["COVERAGE"]))
        for name in files:
            print(render_file(name, config))
        print(agent_sql(config, dataset))
        print(f"-- Then add the agent to the account's Snowflake CoWork object if it is not listed:\n"
              f"alter snowflake intelligence {identifier(COWORK_OBJECT)} add agent "
              f"{namespace(config)}.OVERTURE_MAPS_AGENT;")
        print(grant_sql(config))
        return
    if command in {"install", "load-s3", "cleanup"} and not execute:
        raise ValueError(f"{command} changes Snowflake objects; review first, then pass --execute")
    execute_workflow(command, config, SnowCLI(config))


def drop_unused_theme_views(config: dict[str, Any], client: Any) -> None:
    """Remove views for optional themes no longer configured, before the model is replaced."""
    unused = {theme.upper() for theme in OPTIONAL_THEMES} - {
        theme.upper() for theme in active_themes(config)}
    for row in client.query(f"show views in schema {namespace(config)}"):
        if row["NAME"] in unused and owned(row.get("COMMENT")):
            client.query(f"drop view {namespace(config)}.{identifier(row['NAME'])}")


def execute_workflow(command: str, config: dict[str, Any], client: Any,
                     load_id: str | None = None, release: str | None = None) -> None:
    files = install_files(config)
    dataset = {"RELEASE": config["source"]["release"], "COVERAGE": config["source"]["coverage"]}
    if command == "preflight":
        preflight(config, client)
    elif command == "load-s3":
        load_s3(config, client, load_id=load_id, release=release)
    elif command == "install":
        preflight(config, client)
        ensure_namespace(config, client, create=True)
        check_owned_objects(config, client)
        if config["source"]["mode"] == "existing":
            create_dataset_info(config, client, dataset["RELEASE"], dataset["COVERAGE"])
        dataset = client.query(f"select release, coverage from {namespace(config)}.DATASET_INFO")[0]
        drop_unused_theme_views(config, client)
        for name in files:
            client.query(render_file(name, config))
        client.query(agent_sql(config, dataset))
        cowork = register_cowork(config, client)
        grants = grant_sql(config, cowork)
        if grants:
            client.query(grants)
        verify(config, client)
    elif command == "verify":
        verify(config, client)
    elif command == "verify-prompts":
        verify_prompts(config, client)
    elif command == "cleanup":
        ensure_namespace(config, client)
        check_owned_objects(config, client, allow_candidates=True)
        ns = namespace(config)
        # Remove only this agent from CoWork; the CoWork object is account-wide.
        cowork = cowork_object(client)
        if cowork and cowork_listed(config, client, cowork):
            client.query(f"alter snowflake intelligence {cowork} drop agent {ns}.OVERTURE_MAPS_AGENT")
        for kind in ("AGENT", "CORTEX SEARCH SERVICE", "SEMANTIC VIEW", "VIEW", "TABLE",
                     "STAGE", "FILE FORMAT"):
            for name in OBJECTS[kind]:
                client.query(f"drop {kind} if exists {ns}.{identifier(name)}")
        for row in client.query(f"show tables in schema {ns}"):
            if row["NAME"].startswith("LOAD_") and owned(row.get("COMMENT")):
                client.query(f"drop table {ns}.{identifier(row['NAME'])}")
        client.query(f"drop schema {ns} restrict")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("render", "preflight", "install", "load-s3",
                                             "verify", "verify-prompts", "cleanup"))
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--execute", action="store_true", help="Authorize mutating commands")
    args = parser.parse_args()
    config = load_config(args.config)
    run(args.command, config, args.execute)


if __name__ == "__main__":
    try:
        main()
    except (ValueError, KeyError, OSError, subprocess.CalledProcessError) as error:
        print(f"ERROR: {error}", file=sys.stderr)
        if isinstance(error, subprocess.CalledProcessError):
            print(error.stderr, file=sys.stderr)
        sys.exit(1)