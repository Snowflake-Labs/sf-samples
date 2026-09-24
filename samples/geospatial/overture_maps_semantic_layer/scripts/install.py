#!/usr/bin/env python3
"""Render or install the Overture sample through the user's Snowflake CLI connection."""

from __future__ import annotations

import argparse
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
VERSION = "1.0.0"
THEMES = ("place", "division", "division_area", "division_boundary")
GEOMETRY_DIMENSIONS = {"place": 0, "division": 0, "division_area": 2,
                       "division_boundary": 1}
IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z0-9_$]*\Z")
REQUIRED = {
    "place": {"ID": "text", "GEOMETRY": "geo", "NAMES": "object",
              "CATEGORIES": "object", "ADDRESSES": "array"},
    "division": {"ID": "text", "GEOMETRY": "geo", "NAMES": "object",
                 "COUNTRY": "text", "REGION": "text", "SUBTYPE": "text",
                 "POPULATION": "number"},
    "division_area": {"ID": "text", "DIVISION_ID": "text", "GEOMETRY": "geo",
                      "NAMES": "object", "COUNTRY": "text", "REGION": "text",
                      "SUBTYPE": "text", "CLASS": "text"},
    "division_boundary": {"ID": "text", "GEOMETRY": "geo", "COUNTRY": "text",
                          "REGION": "text", "SUBTYPE": "text", "CLASS": "text",
                          "IS_LAND": "boolean"},
}
TYPE_PREFIXES = {
    "text": ("VARCHAR", "TEXT", "STRING"), "geo": ("GEOGRAPHY",),
    "object": ("VARIANT", "OBJECT"), "array": ("VARIANT", "ARRAY"),
    "number": ("NUMBER", "DECIMAL", "INT", "FLOAT", "DOUBLE", "REAL"),
    "boolean": ("BOOLEAN",),
}
OBJECTS = {
    "VIEW": [theme.upper() for theme in THEMES],
    "SEMANTIC VIEW": ["OVERTURE_MAPS_SV"],
    "CORTEX SEARCH SERVICE": ["CATEGORY_SEARCH"],
    "AGENT": ["OVERTURE_MAPS_AGENT"],
    "TABLE": ["DATASET_INFO", "CATEGORY_VOCAB"] + [f"RAW_{theme.upper()}" for theme in THEMES],
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


def load_config(path: Path) -> dict[str, Any]:
    config = tomllib.loads(path.read_text())
    for key in ("role", "warehouse", "database", "schema", "viewer_role"):
        identifier(config[key])
    if not isinstance(config["connection"], str) or not config["connection"].strip():
        raise ValueError("A named Snowflake CLI connection is required")
    source = config["source"]
    if source["mode"] not in {"existing", "s3"}:
        raise ValueError("source.mode must be existing or s3")
    for key in ("release", "coverage"):
        if not isinstance(source[key], str) or not source[key].strip():
            raise ValueError(f"source.{key} must be a non-empty string")
    target = f'{config["database"]}.{config["schema"]}'
    for theme in THEMES:
        if source["mode"] == "s3":
            source[theme] = f"{target}.RAW_{theme.upper()}"
        identifier(source[theme], 3)
        if source["mode"] == "existing" and source[theme].upper().rsplit(".", 1)[0] == target.upper():
            raise ValueError("Existing sources must be outside the sample-owned schema")
    integration = config.get("s3", {}).get("storage_integration", "")
    if integration:
        identifier(integration)
    release = config.get("s3", {}).get("release", "latest")
    if release != "latest" and not re.fullmatch(r"\d{4}-\d{2}-\d{2}\.\d+", release):
        raise ValueError("s3.release must be latest or YYYY-MM-DD.N")
    bbox = config.get("s3", {}).get("place_bbox", [])
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


def marker(config: dict[str, Any]) -> str:
    content = {key: value for key, value in config.items() if key != "connection"}
    digest_builder = hashlib.sha256(json.dumps(content, sort_keys=True).encode())
    for folder in ("sql", "semantic", "agent", "examples", "scripts"):
        for path in sorted((ROOT / folder).glob("*")):
            if path.is_file():
                digest_builder.update(path.relative_to(ROOT).as_posix().encode())
                digest_builder.update(path.read_bytes())
    digest = digest_builder.hexdigest()[:16]
    return f"overture-maps-semantic-layer:{VERSION}:{digest}"


def context(config: dict[str, Any]) -> dict[str, str]:
    return {
        "ns": namespace(config),
        "warehouse": identifier(config["warehouse"]),
        "owner_comment": literal(marker(config)),
        "map_rows": str(config["limits"]["map_rows"]),
        **{f"source_{theme}": identifier(config["source"][theme], 3) for theme in THEMES},
    }


def render_file(name: str, config: dict[str, Any], **values: str) -> str:
    if name == "semantic/overture.sql":
        values["verified_queries"] = verified_queries(config)
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
    for theme in THEMES:
        source = identifier(config["source"][theme], 3)
        validate_columns(theme, client.query(f"describe table {source}"))
        sample = client.query(f"select id from {source} limit 1")
        if not sample:
            raise ValueError(f"{source} is empty; missing data is not an installed theme")
        expected_dimension = GEOMETRY_DIMENSIONS[theme]
        geometry = client.query(
            "select count_if(geometry is null or not st_isvalid(geometry) "
            f"or st_dimension(geometry) != {expected_dimension}) as invalid "
            f"from (select geometry from {source} limit 100)"
        )[0]
        if int(geometry["INVALID"] or 0):
            raise ValueError(f"{source}: invalid or unexpected geometry in the first 100 rows")
    place = identifier(config["source"]["place"], 3)
    sample = client.query(
        f"select typeof(to_variant(names)) as names_type, "
        f"typeof(to_variant(categories)) as categories_type, "
        f"typeof(to_variant(addresses)) as addresses_type from {place} "
        "where names is not null and categories is not null and addresses is not null limit 1"
    )
    if not sample or sample[0] != {
        "NAMES_TYPE": "OBJECT", "CATEGORIES_TYPE": "OBJECT", "ADDRESSES_TYPE": "ARRAY"
    }:
        raise ValueError("Expected Overture objects for names/categories and an addresses array")
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
    elif rows[0].get("COMMENT") != marker(config):
        raise ValueError("Schema is not owned by this configuration/version; choose a new schema")


def check_owned_objects(config: dict[str, Any], client: SnowCLI,
                        allow_candidates: bool = False) -> None:
    kinds = {"VIEW": "VIEWS", "TABLE": "TABLES", "SEMANTIC VIEW": "SEMANTIC VIEWS",
             "AGENT": "AGENTS", "CORTEX SEARCH SERVICE": "CORTEX SEARCH SERVICES",
             "STAGE": "STAGES", "FILE FORMAT": "FILE FORMATS"}
    for kind, plural in kinds.items():
        for row in client.query(f"show {plural} in schema {namespace(config)}"):
            name = row["NAME"]
            candidate = (allow_candidates and kind == "TABLE"
                         and re.fullmatch(r"LOAD_(PLACE|DIVISION|DIVISION_AREA|DIVISION_BOUNDARY)_[A-F0-9]{8}", name))
            if (name not in OBJECTS[kind] and not candidate) or row.get("COMMENT") != marker(config):
                raise ValueError(f"Unrecognized or modified {kind} {name}; refusing to mutate schema")


def resolve_release(rows: list[dict[str, Any]], requested: str) -> str:
    releases: dict[str, set[str]] = {}
    pattern = r"release/(\d{4}-\d{2}-\d{2}\.\d+)/theme=\w+/type=(\w+)/"
    for row in rows:
        match = re.search(pattern, row["NAME"])
        if match:
            releases.setdefault(match[1], set()).add(match[2])
    complete = sorted(
        (release for release, types in releases.items() if set(THEMES) <= types),
        key=lambda release: (release.rsplit(".", 1)[0], int(release.rsplit(".", 1)[1])),
    )
    if requested == "latest" and complete:
        return complete[-1]
    if requested in complete:
        return requested
    raise ValueError("No available release has all four required types; nothing will be loaded")


def projection(theme: str) -> str:
    types = {"text": "varchar", "object": "variant", "array": "variant",
             "number": "number", "boolean": "boolean"}
    return ",\n       ".join(
        "try_to_geography($1:geometry::binary) as GEOMETRY" if kind == "geo"
        else f"$1:{name.lower()}::{types[kind]} as {name}"
        for name, kind in REQUIRED[theme].items()
    )


def load_s3(config: dict[str, Any], client: SnowCLI) -> None:
    if config["source"]["mode"] != "s3":
        raise ValueError("load-s3 requires source.mode = 's3'")
    ensure_namespace(config, client, create=True)
    check_owned_objects(config, client)
    ns = namespace(config)
    existing = client.query(f"show tables in schema {ns}")
    if any(row["NAME"].startswith("RAW_") or row["NAME"] == "DATASET_INFO" for row in existing):
        raise ValueError("Load already started or completed. Use a new schema for a new load.")
    integration = config.get("s3", {}).get("storage_integration", "")
    client.query(render_file("sql/05_s3_setup.sql", config,
                            integration=f"storage_integration = {identifier(integration)}"
                            if integration else ""))
    files = client.query(
        f"list @{ns}.OVERTURE_S3 "
        "pattern = '.*/type=(place|division|division_area|division_boundary)/part-00000.*[.]parquet'"
    )
    release = resolve_release(files, config.get("s3", {}).get("release", "latest"))
    candidates: list[tuple[str, str]] = []
    for theme in THEMES:
        candidate = f"{ns}.LOAD_{theme.upper()}_{uuid.uuid4().hex[:8].upper()}"
        candidates.append((candidate, f"{ns}.RAW_{theme.upper()}"))
        bbox = config.get("s3", {}).get("place_bbox", [])
        where = ""
        if theme == "place" and bbox:
            west, south, east, north = bbox
            where = (f"where $1:bbox:xmin::float <= {east} and $1:bbox:xmax::float >= {west} "
                     f"and $1:bbox:ymin::float <= {north} and $1:bbox:ymax::float >= {south}")
        print(f"Loading {theme}, release {release}", file=sys.stderr)
        client.query(render_file("sql/06_load_table.sql", config, candidate=candidate,
                                projection=projection(theme), release=release,
                                theme="places" if theme == "place" else "divisions",
                                type=theme, filter=where))
        quality = client.query(
            f"select count(*) as row_count, count_if(geometry is null or not st_isvalid(geometry) "
            f"or st_dimension(geometry) != {GEOMETRY_DIMENSIONS[theme]}) "
            f"as invalid, count_if(id is null) as null_ids, count(distinct id) as ids "
            f"from {candidate}"
        )[0]
        if (not int(quality["ROW_COUNT"]) or int(quality["INVALID"] or 0)
                or int(quality["NULL_IDS"] or 0)
                or int(quality["ROW_COUNT"]) != int(quality["IDS"])):
            raise ValueError(f"Load validation failed for {candidate}: {quality}. "
                             "Candidates retained for inspection; no views published.")
    orphan = client.query(
        f"select count(*) as orphan_count from {candidates[2][0]} areas "
        f"left join {candidates[1][0]} divisions on areas.division_id = divisions.id "
        "where areas.division_id is not null and divisions.id is null"
    )[0]
    if int(orphan["ORPHAN_COUNT"]):
        raise ValueError("Area-to-division relationship has orphan keys; candidates retained")
    for candidate, target in candidates:
        client.query(f"alter table {candidate} rename to {target}")
    coverage = f"Places bbox {config.get('s3', {}).get('place_bbox', []) or 'global'}; divisions global"
    create_dataset_info(config, client, release, coverage)


def create_dataset_info(config: dict[str, Any], client: SnowCLI,
                        release: str, coverage: str) -> None:
    client.query(dataset_sql(config, release, coverage))


def dataset_sql(config: dict[str, Any], release: str, coverage: str) -> str:
    return (
        f"create table if not exists {namespace(config)}.DATASET_INFO "
        f"comment = {literal(marker(config))} as select {literal(release)} as release, "
        f"{literal(coverage)} as coverage, {literal(config['source']['mode'])} as source_mode, "
        "current_timestamp() as recorded_at;"
    )


def verified_queries(config: dict[str, Any]) -> str:
    prompts = json.loads((ROOT / "examples/prompts.json").read_text())
    queries = []
    for prompt in prompts:
        sql = Template(prompt["reference_sql"]).substitute(ns=namespace(config))
        queries.append(f"{prompt['id']} as (question {literal(prompt['question'])} "
                       f"sql {literal(sql)})")
    return "ai_verified_queries (\n" + ",\n".join(queries) + "\n)"


def agent_sql(config: dict[str, Any], dataset: dict[str, Any]) -> str:
    ns = namespace(config)
    prompts = json.loads((ROOT / "examples/prompts.json").read_text())
    instructions = (ROOT / "agent/instructions.md").read_text()
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
                           "No modeled place-to-area spatial join."}},
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
    return (f"create agent if not exists {ns}.OVERTURE_MAPS_AGENT "
            f"comment = {literal(marker(config))} "
            "profile = '{\"display_name\": \"Overture Maps\"}' "
            f"from specification {literal(json.dumps(spec))};")


def grant_sql(config: dict[str, Any]) -> str:
    ns = namespace(config)
    role = identifier(config["viewer_role"])
    statements = [f"grant usage on database {identifier(config['database'])} to role {role}",
                  f"grant usage on schema {ns} to role {role}",
                  f"grant usage on warehouse {identifier(config['warehouse'])} to role {role}"]
    for name in OBJECTS["VIEW"] + ["DATASET_INFO"]:
        statements.append(f"grant select on view {ns}.{name} to role {role}" if name != "DATASET_INFO"
                          else f"grant select on table {ns}.{name} to role {role}")
    statements += [f"grant select on semantic view {ns}.OVERTURE_MAPS_SV to role {role}",
                   f"grant usage on agent {ns}.OVERTURE_MAPS_AGENT to role {role}",
                   f"grant usage on cortex search service {ns}.CATEGORY_SEARCH to role {role}"]
    return ";\n".join(statements) + ";"


def verify(config: dict[str, Any], client: SnowCLI) -> None:
    ns = namespace(config)
    first_place_id = None
    for theme in THEMES:
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
    print(f"Data/model/search checks passed. Agent: {ns}.OVERTURE_MAPS_AGENT")
    print("Agent responses and native maps: NOT TESTED. Run examples in CoWork as the viewer.")


def verify_prompts(config: dict[str, Any], client: SnowCLI) -> None:
    for prompt in json.loads((ROOT / "examples/prompts.json").read_text()):
        sql = Template(prompt["reference_sql"]).substitute(ns=namespace(config))
        rows = client.query(sql)
        if not rows:
            print(f"{prompt['id']}: EMPTY; check {prompt['coverage']}")
            continue
        if not set(prompt["columns"]) <= set(rows[0]):
            raise ValueError(f"Unexpected output columns for {prompt['id']}")
        print(f"{prompt['id']}: reference SQL returned {len(rows)} rows. "
              "Agent equivalence and map rendering require separate checks.")


def run(command: str, config: dict[str, Any], execute: bool) -> None:
    files = ("sql/10_sources.sql", "semantic/overture.sql", "sql/30_category_search.sql")
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
        print(grant_sql(config))
        return
    if command in {"install", "load-s3", "cleanup"} and not execute:
        raise ValueError(f"{command} changes Snowflake objects; review first, then pass --execute")
    client = SnowCLI(config)
    if command == "preflight":
        preflight(config, client)
    elif command == "load-s3":
        load_s3(config, client)
    elif command == "install":
        preflight(config, client)
        ensure_namespace(config, client, create=True)
        check_owned_objects(config, client)
        if config["source"]["mode"] == "existing":
            create_dataset_info(config, client, dataset["RELEASE"], dataset["COVERAGE"])
        dataset = client.query(f"select release, coverage from {namespace(config)}.DATASET_INFO")[0]
        for name in files:
            client.query(render_file(name, config))
        client.query(agent_sql(config, dataset))
        client.query(grant_sql(config))
        verify(config, client)
    elif command == "verify":
        verify(config, client)
    elif command == "verify-prompts":
        verify_prompts(config, client)
    elif command == "cleanup":
        ensure_namespace(config, client)
        check_owned_objects(config, client, allow_candidates=True)
        ns = namespace(config)
        for kind in ("AGENT", "CORTEX SEARCH SERVICE", "SEMANTIC VIEW", "VIEW", "TABLE",
                     "STAGE", "FILE FORMAT"):
            for name in OBJECTS[kind]:
                client.query(f"drop {kind} if exists {ns}.{identifier(name)}")
        for row in client.query(f"show tables in schema {ns}"):
            if row["NAME"].startswith("LOAD_") and row.get("COMMENT") == marker(config):
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