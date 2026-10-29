from pathlib import Path
import json
import re
import sys
import unittest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))
from agent_install import split_statements


class MapGuideTests(unittest.TestCase):
    def test_map_scenarios_reference_gallery_and_remain_unverified(self):
        prompts = json.loads((ROOT / "examples/prompts.json").read_text())
        scenarios = json.loads((ROOT / "tests/map_scenarios.json").read_text())
        prompt_ids = {prompt["id"] for prompt in prompts}
        scenario_ids = {scenario["id"] for scenario in scenarios}
        self.assertEqual(len(scenario_ids), len(scenarios))
        self.assertTrue({"table_to_map", "wrong_result_type", "missing_result",
                         "subagent_handoff", "logical_geometry", "default_h3_map",
                         "unsupported_spatial_join", "maps_unavailable"} <= scenario_ids)
        for scenario in scenarios:
            self.assertIn(scenario["prompt_id"], prompt_ids)
            self.assertEqual(scenario["status"], "not tested")
            self.assertTrue(scenario["setup"])
            self.assertIsInstance(scenario["follow_ups"], list)
            self.assertGreaterEqual(len(scenario["expected"]), 2)

    def test_examples_are_six_read_only_statements(self):
        sql = (ROOT / "docs/examples/map_queries.sql").read_text()
        names = re.findall(r"^-- example: (\w+)$", sql, re.MULTILINE)
        self.assertEqual(names, ["points", "geography_points", "lines", "polygons",
                                 "h3", "aggregate_then_join"])
        without_comments = "\n".join(line for line in sql.splitlines()
                                      if not line.lstrip().startswith("--"))
        statements = split_statements(without_comments)
        self.assertEqual(len(statements), 6)
        for statement in statements:
            self.assertRegex(statement, r"^(select|with)\b")
            self.assertNotRegex(statement.lower(), r"\b(create|alter|drop|insert|delete|update|merge|call)\b")

    def test_guide_local_links_exist(self):
        for relative in ("README.md", "docs/map-testing-guide.md", "docs/validation.md"):
            path = ROOT / relative
            for target in re.findall(r"\]\(([^)]+)\)", path.read_text()):
                if "://" in target or target.startswith("#"):
                    continue
                linked_path = target.split("#", 1)[0]
                self.assertTrue((path.parent / linked_path).exists(),
                                f"Broken link in {relative}: {target}")

    def test_guide_covers_field_questions_without_internal_links(self):
        guide = (ROOT / "docs/map-testing-guide.md").read_text()
        for heading in ("Preview access", "Workspaces", "CoWork", "Dashboards",
                        "Safe geographic aggregation", "MCP", "FAQ", "Troubleshooting"):
            self.assertIn(f"## {heading}\n", guide)
        for private_reference in ("snowflake.slack.com", "snowflake.enterprise.slack.com",
                                  "snowflake-eng/", "COPILOT_ORCHESTRATOR_PARAM_"):
            self.assertNotIn(private_reference, guide)
        self.assertIn("does not yet contain a verified", guide)


if __name__ == "__main__":
    unittest.main()