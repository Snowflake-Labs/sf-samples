from pathlib import Path
import re
import sys
import unittest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))
from agent_install import split_statements


class MapGuideTests(unittest.TestCase):
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