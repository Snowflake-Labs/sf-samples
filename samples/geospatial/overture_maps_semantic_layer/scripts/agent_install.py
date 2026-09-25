#!/usr/bin/env python3
"""Offline SQL step renderer for an agent using its authenticated Snowflake SQL tool."""

from __future__ import annotations

import argparse
from contextlib import redirect_stdout, redirect_stderr
import io
import json
from pathlib import Path
import re
from typing import Any

import install

IDENTITY_SQL = (
    "select current_account() as account, current_role() as role, "
    "current_warehouse() as warehouse, current_database() as database, current_user() as user"
)


def normalize_rows(rows: Any) -> list[dict[str, Any]]:
    if not isinstance(rows, list) or any(not isinstance(row, dict) for row in rows):
        raise ValueError("Expected SQL result rows as a JSON array of objects")
    return [{key.upper(): value for key, value in row.items()} for row in rows]


def discovery_sql(database: str, schema: str | None = None) -> str:
    scope = ""
    if schema:
        install.identifier(schema)
        scope = f" and table_schema = {install.literal(schema.upper())}"
    return (
        f"select table_catalog, table_schema, table_name, table_type "
        f"from {install.identifier(database)}.information_schema.tables "
        "where (upper(table_name) like '%PLACE%' or upper(table_name) like '%DIVISION%')"
        f"{scope} order by table_schema, table_name limit 100"
    )


def select_sources(candidates: list[dict[str, Any]]) -> dict[str, Any]:
    """Validate explicitly assembled source sets without combining unrelated datasets."""
    compatible = []
    rejected = []
    for candidate in candidates:
        try:
            sources = candidate["sources"]
            for theme in install.THEMES:
                install.identifier(sources[theme], 3)
                install.validate_columns(theme, normalize_rows(candidate["columns"][theme]))
            compatible.append({"label": candidate["label"], "sources": sources,
                               "release": candidate.get("release", "unknown"),
                               "coverage": candidate.get("coverage", "unknown")})
        except (ValueError, KeyError) as error:
            rejected.append({"label": candidate.get("label", "unnamed"), "reason": str(error)})
    return {"status": "selected" if len(compatible) == 1 else "ambiguous" if compatible else "missing",
            "candidates": compatible, "rejected": rejected,
            "next": "Run source preflight; column compatibility alone does not prove access or coverage."}


def propose_settings(identity: dict[str, Any]) -> dict[str, Any]:
    identity = {key.upper(): value for key, value in identity.items()}
    role = identity.get("ROLE")
    elevated = role in {"PUBLIC", "ACCOUNTADMIN", "SECURITYADMIN", "ORGADMIN", "GLOBALORGADMIN"}
    return {"account": identity.get("ACCOUNT"),
            "role": None if elevated else role,
            "viewer_role": None if elevated else role,
            "database": identity.get("DATABASE"), "warehouse": identity.get("WAREHOUSE"),
            "schema": "OVERTURE_SEMANTIC_V1",
            "approval_required": True,
            "note": "Proposed from current context only. Verify privileges and schema availability. "
                    "Current role may be shared by other users; ask before broadening the audience."}


def split_statements(sql: str) -> list[str]:
    """Split the shipped SQL subset, preserving semicolons in SQL string literals."""
    statements = []
    current = []
    quote = None
    index = 0
    while index < len(sql):
        char = sql[index]
        following = sql[index:index + 2]
        if quote:
            current.append(char)
            if char == "\\" and quote == "'" and index + 1 < len(sql):
                index += 1
                current.append(sql[index])
            elif char == quote:
                if following == quote * 2:
                    index += 1
                    current.append(sql[index])
                else:
                    quote = None
        elif char in {"'", '"'}:
            quote = char
            current.append(char)
        elif following in {"--", "/*", "$$"}:
            raise ValueError("Comments and dollar-quoted blocks are not part of the shipped SQL subset")
        elif char == ";":
            if "".join(current).strip():
                statements.append("".join(current).strip())
            current = []
        else:
            current.append(char)
        index += 1
    if quote:
        raise ValueError("Unterminated SQL quote")
    if "".join(current).strip():
        statements.append("".join(current).strip())
    return statements


def mutates(sql: str) -> bool:
    return not re.match(r"^(select|show|describe|list)\b", sql.lstrip(), re.IGNORECASE)


class NextStatement(Exception):
    def __init__(self, sql: str, index: int):
        self.sql = sql
        self.index = index


class ReplayClient:
    """Replay successful tool results until the existing workflow needs another statement."""

    def __init__(self, history: list[dict[str, Any]]):
        self.history = history
        self.index = 0

    def query(self, sql: str) -> list[dict[str, Any]]:
        rows = []
        for statement in split_statements(sql):
            if self.index == len(self.history):
                raise NextStatement(statement, self.index)
            event = self.history[self.index]
            if event.get("sql") != statement or event.get("ok") is not True:
                raise ValueError("Receipt mismatch or failed query: inspect live state before resuming")
            rows = normalize_rows(event["rows"])
            self.index += 1
        return rows


def next_statement(command: str, config: dict[str, Any], receipt: dict[str, Any]) -> dict[str, Any]:
    if command not in {"preflight", "install", "load-s3", "verify", "verify-prompts"}:
        raise ValueError("Unsupported agent command; cleanup is deliberately not automated")
    expected = receipt["identity"]
    current = receipt["current_identity"]
    for field in ("ACCOUNT", "ROLE", "WAREHOUSE"):
        if not expected.get(field) or current.get(field) != expected[field]:
            raise ValueError(f"Session identity mismatch for {field}")
    if expected["ROLE"] != config["role"].upper() or expected["WAREHOUSE"] != config["warehouse"].upper():
        raise ValueError("Approved configuration does not match session role/warehouse")
    marker = install.marker(config)
    if receipt.get("marker") != marker or receipt.get("command") != command:
        raise ValueError("Receipt belongs to another configuration, code revision or command")
    if command == "load-s3" and not re.fullmatch(r"[A-F0-9]{8}", receipt.get("load_id", "")):
        raise ValueError("S3 receipt needs a stable eight-digit hexadecimal load_id")
    history = receipt.get("history", [])
    if not isinstance(history, list):
        raise ValueError("Receipt history must be a list")
    client = ReplayClient(history)
    logs = io.StringIO()
    try:
        with redirect_stdout(logs), redirect_stderr(logs):
            install.execute_workflow(command, config, client, load_id=receipt.get("load_id"))
    except NextStatement as pending:
        mutation = mutates(pending.sql)
        approved = receipt.get("approved_marker") == marker
        result = {
            "status": "approval_required" if mutation and not approved else "next",
            "command": command, "index": pending.index, "sql": pending.sql,
            "mutates": mutation, "marker": marker, "identity_sql": IDENTITY_SQL,
            "expected_identity": expected,
        }
        if mutation:
            result["context_requirements"] = {
                "secondary_roles": "NONE",
                "statement_timeout_in_seconds": config["limits"]["statement_timeout_seconds"],
            }
        return result
    if client.index != len(history):
        raise ValueError("Receipt has trailing events from a different execution")
    return {"status": "complete", "command": command, "statements": client.index,
            "output": logs.getvalue().strip(),
            "agent_execution": "not tested", "viewer_access": "not tested",
            "cowork_registration": "not tested", "native_maps": "not tested"}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="action", required=True)
    subparsers.add_parser("identity")
    discover = subparsers.add_parser("discover")
    discover.add_argument("--database", required=True)
    discover.add_argument("--schema")
    sources = subparsers.add_parser("select-sources")
    sources.add_argument("--candidates", type=Path, required=True)
    settings = subparsers.add_parser("propose-settings")
    settings.add_argument("--identity", type=Path, required=True)
    next_step = subparsers.add_parser("next")
    next_step.add_argument("--config", type=Path, required=True)
    next_step.add_argument("--receipt", type=Path, required=True)
    next_step.add_argument("--command", required=True)
    proposal = subparsers.add_parser("proposal")
    proposal.add_argument("--config", type=Path, required=True)
    args = parser.parse_args()
    if args.action == "identity":
        result = {"sql": IDENTITY_SQL, "mutates": False}
    elif args.action == "discover":
        result = {"sql": discovery_sql(args.database, args.schema), "mutates": False}
    elif args.action == "select-sources":
        result = select_sources(json.loads(args.candidates.read_text()))
    elif args.action == "propose-settings":
        result = propose_settings(normalize_rows(json.loads(args.identity.read_text()))[0])
    elif args.action == "proposal":
        config = install.load_config(args.config, require_connection=False)
        result = {"marker": install.marker(config), "target": install.namespace(config),
                  "role": config["role"], "warehouse": config["warehouse"],
                  "viewer_role": config["viewer_role"], "source": config["source"],
                  "approval_required": True}
    else:
        config = install.load_config(args.config, require_connection=False)
        result = next_statement(args.command, config, json.loads(args.receipt.read_text()))
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    try:
        main()
    except (ValueError, KeyError, OSError, IndexError) as error:
        print(json.dumps({"status": "blocked", "error": str(error)}))
        raise SystemExit(1)