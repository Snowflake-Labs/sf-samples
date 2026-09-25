from __future__ import annotations

from copy import deepcopy
import json
from pathlib import Path
import sys
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))
import agent_install as agent
import install


class AgentInstallerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.config = install.load_config(ROOT / "config.example.toml")
        self.identity = {"ACCOUNT": "TEST123", "ROLE": self.config["role"],
                         "WAREHOUSE": self.config["warehouse"]}

    def receipt(self, command: str, approved: bool = False) -> dict:
        marker = install.marker(self.config)
        return {"command": command, "marker": marker, "identity": self.identity,
                "current_identity": deepcopy(self.identity), "history": [],
                "approved_marker": marker if approved else None, "load_id": "AB123456"}

    def candidate(self, label: str = "Loaded data") -> dict:
        return {"label": label,
                "sources": {theme: f"CUSTOM_DB.MY_DATA.{theme.upper()}" for theme in install.THEMES},
                "columns": {theme: [{"name": name, "type": install.TYPE_PREFIXES[kind][0]}
                                    for name, kind in columns.items()]
                            for theme, columns in install.REQUIRED.items()}}

    def test_discovery_scope_is_quoted_and_bounded(self) -> None:
        sql = agent.discovery_sql("my_db", "carto")
        self.assertIn('"MY_DB".information_schema.tables', sql)
        self.assertIn("table_schema = 'CARTO'", sql)
        self.assertIn("limit 100", sql)
        with self.assertRaises(ValueError):
            agent.discovery_sql("x;drop database y")

    def test_renamed_sources_selected_without_inventing_coverage(self) -> None:
        result = agent.select_sources([self.candidate()])
        self.assertEqual(result["status"], "selected")
        self.assertEqual(result["candidates"][0]["coverage"], "unknown")

    def test_ambiguous_sources_require_choice(self) -> None:
        result = agent.select_sources([self.candidate(), self.candidate("Marketplace")])
        self.assertEqual(result["status"], "ambiguous")
        self.assertEqual(len(result["candidates"]), 2)

    def test_missing_and_incompatible_sources(self) -> None:
        self.assertEqual(agent.select_sources([])["status"], "missing")
        candidate = self.candidate()
        candidate["columns"]["place"][1]["type"] = "BINARY"
        result = agent.select_sources([candidate])
        self.assertEqual(result["status"], "missing")
        self.assertIn("expected geo", result["rejected"][0]["reason"])

    def test_settings_do_not_select_elevated_role(self) -> None:
        for role in ("PUBLIC", "ACCOUNTADMIN", "SECURITYADMIN"):
            result = agent.propose_settings({**self.identity, "ROLE": role})
            self.assertIsNone(result["role"])
            self.assertIsNone(result["viewer_role"])
        self.assertTrue(agent.propose_settings(self.identity)["approval_required"])

    def test_config_without_cli_connection(self) -> None:
        config = deepcopy(self.config)
        del config["connection"]
        with patch.object(install.tomllib, "loads", return_value=config):
            self.assertNotIn("connection", install.load_config(ROOT / "config.example.toml", False))

    def test_sql_split_preserves_literals(self) -> None:
        sql = "select 'a;b', 'it''s;ok', 'a\\\'b;c'; select \"semi;column\" from t;"
        statements = agent.split_statements(sql)
        self.assertEqual(len(statements), 2)
        self.assertIn("it''s;ok", statements[0])
        for sql in ("select 'unclosed", "select 1 -- comment", "select $$foo$$"):
            with self.assertRaises(ValueError):
                agent.split_statements(sql)

    def test_replay_consumes_results_without_execution(self) -> None:
        client = agent.ReplayClient([{"sql": "select 1", "ok": True, "rows": [{"value": 1}]}])
        self.assertEqual(client.query("select 1;"), [{"VALUE": 1}])
        with self.assertRaises(agent.NextStatement) as pending:
            client.query("select 2")
        self.assertEqual(pending.exception.index, 1)

    def test_replay_rejects_changed_or_failed_queries(self) -> None:
        for event in ({"sql": "select 2", "ok": True, "rows": []},
                      {"sql": "select 1", "ok": False, "rows": []}):
            with self.assertRaises(ValueError):
                agent.ReplayClient([event]).query("select 1")

    def test_next_never_calls_cli(self) -> None:
        with patch.object(install.subprocess, "run", side_effect=AssertionError("CLI forbidden")):
            result = agent.next_statement("preflight", self.config, self.receipt("preflight"))
        self.assertEqual(result["status"], "next")
        self.assertFalse(result["mutates"])

    def test_account_role_and_warehouse_mismatch(self) -> None:
        for field in ("ACCOUNT", "ROLE", "WAREHOUSE"):
            receipt = self.receipt("preflight")
            receipt["current_identity"][field] = "DIFFERENT"
            with self.assertRaisesRegex(ValueError, "identity mismatch"):
                agent.next_statement("preflight", self.config, receipt)

    def test_changed_config_requires_new_receipt(self) -> None:
        receipt = self.receipt("preflight", True)
        self.config["schema"] = "ANOTHER_SCHEMA"
        with self.assertRaisesRegex(ValueError, "another configuration"):
            agent.next_statement("preflight", self.config, receipt)

    def test_mutation_requires_approval(self) -> None:
        def workflow(command, config, client, load_id=None):
            client.query("create schema TEST.SAMPLE")
        with patch.object(install, "execute_workflow", workflow):
            result = agent.next_statement("install", self.config, self.receipt("install"))
            self.assertEqual(result["status"], "approval_required")
            result = agent.next_statement("install", self.config, self.receipt("install", True))
            self.assertEqual(result["status"], "next")

    def test_cleanup_not_supported_by_agent(self) -> None:
        with self.assertRaisesRegex(ValueError, "cleanup"):
            agent.next_statement("cleanup", self.config, self.receipt("cleanup", True))

    def test_self_audience_does_not_grant_to_self(self) -> None:
        self.config["viewer_role"] = self.config["role"]
        self.assertEqual(install.grant_sql(self.config), "")

    def test_all_templates_split(self) -> None:
        self.assertEqual(len(agent.split_statements(install.render_file("sql/10_sources.sql", self.config))), 4)
        self.assertEqual(len(agent.split_statements(install.render_file("semantic/overture.sql", self.config))), 1)
        self.assertEqual(len(agent.split_statements(install.agent_sql(self.config, {"COVERAGE": "a;b"}))), 1)

    def test_existing_installation_replay(self) -> None:
        receipt = self.receipt("install", True)
        seen_mutations = []
        for _ in range(100):
            result = agent.next_statement("install", self.config, receipt)
            if result["status"] == "complete":
                self.assertEqual(result["native_maps"], "not tested")
                self.assertEqual(result["agent_execution"], "not tested")
                self.assertTrue(any("create agent" in sql for sql in seen_mutations))
                self.assertFalse(any("create or replace" in sql for sql in seen_mutations))
                break
            self.assertEqual(result["status"], "next")
            sql = result["sql"]
            rows = []
            if sql.startswith("select current_account"):
                rows = [self.identity]
            elif sql.startswith("describe table"):
                theme = sql.rsplit('"', 2)[1].lower()
                rows = self.candidate()["columns"][theme]
            elif sql.startswith("select id"):
                rows = [{"ID": "demo-id"}]
            elif "as invalid" in sql:
                rows = [{"INVALID": 0}]
            elif "as names_type" in sql:
                rows = [{"NAMES_TYPE": "OBJECT", "CATEGORIES_TYPE": "OBJECT", "ADDRESSES_TYPE": "ARRAY"}]
            elif sql.startswith("select release"):
                rows = [{"RELEASE": "unknown", "COVERAGE": "unknown"}]
            elif sql.startswith("select * from semantic_view"):
                rows = [{"PLACE_ID": "demo-id"}]
            elif sql.startswith("describe cortex search"):
                rows = [{"SERVING_STATE": "ACTIVE"}]
            if result["mutates"]:
                seen_mutations.append(sql)
            receipt["history"].append({"sql": sql, "ok": True, "rows": rows})
        else:
            self.fail("Workflow did not terminate")

    def test_schema_collision_stops_before_mutation(self) -> None:
        receipt = self.receipt("load-s3", True)
        self.config["source"]["mode"] = "s3"
        receipt["marker"] = receipt["approved_marker"] = install.marker(self.config)
        result = agent.next_statement("load-s3", self.config, receipt)
        receipt["history"].append({"sql": result["sql"], "ok": True,
                                   "rows": [{"COMMENT": "someone else's schema"}]})
        with self.assertRaisesRegex(ValueError, "not owned"):
            agent.next_statement("load-s3", self.config, receipt)

    def test_scenario_fixture_has_failure_cases(self) -> None:
        scenarios = json.loads((ROOT / "tests/installation_scenarios.json").read_text())
        required = {"renamed_share", "no_cli", "account_mismatch", "declined_load",
                    "partial_install", "unknown_coverage", "maps_unavailable"}
        self.assertTrue(required <= {scenario["id"] for scenario in scenarios})


if __name__ == "__main__":
    unittest.main()