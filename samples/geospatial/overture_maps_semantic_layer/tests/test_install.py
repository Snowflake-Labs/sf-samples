from __future__ import annotations

from copy import deepcopy
import importlib.util
import json
from pathlib import Path
import subprocess
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location("install", ROOT / "scripts/install.py")
install = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(install)


class InstallerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.config = install.load_config(ROOT / "config.example.toml")

    def test_identifier_quoting(self) -> None:
        self.assertEqual(install.identifier("my_db.carto.place", 3), '"MY_DB"."CARTO"."PLACE"')
        for value in ('db.s.t;drop schema x', 'db."mixed".t', 'db.s', 'db..t'):
            with self.assertRaises(ValueError):
                install.identifier(value, 3)

    def test_literal_escaping(self) -> None:
        self.assertEqual(install.literal("a'b\\c"), "'a''b\\\\c'")

    def test_source_columns(self) -> None:
        rows = [{"NAME": name, "TYPE": install.TYPE_PREFIXES[kind][0]}
                for name, kind in install.REQUIRED["place"].items()]
        install.validate_columns("place", rows)
        with self.assertRaises(ValueError):
            install.validate_columns("place", rows[:-1])
        rows[1]["TYPE"] = "BINARY"
        with self.assertRaises(ValueError):
            install.validate_columns("place", rows)

    def test_release_requires_all_types(self) -> None:
        rows = [{"NAME": f"s3://overturemaps-us-west-2/release/2026-08-19.0/theme=x/type={theme}/part-00000.parquet"}
                for theme in install.THEMES]
        rows.append({"NAME": "release/2026-09-23.0/theme=places/type=place/part-00000.parquet"})
        self.assertEqual(install.resolve_release(rows, "latest"), "2026-08-19.0")
        with self.assertRaises(ValueError):
            install.resolve_release(rows, "2026-09-23.0")

    def test_marker_tracks_configuration(self) -> None:
        updated = deepcopy(self.config)
        updated["source"]["place"] = "OTHER.CARTO.PLACE"
        self.assertNotEqual(install.marker(updated), install.marker(self.config))
        updated = deepcopy(self.config)
        updated["connection"] = "another_connection"
        self.assertEqual(install.marker(updated), install.marker(self.config))

    def test_all_templates_render(self) -> None:
        for file in ("sql/10_sources.sql", "semantic/overture.sql", "sql/30_category_search.sql"):
            rendered = install.render_file(file, self.config)
            self.assertNotIn("$ns", rendered)
            self.assertNotIn("create or replace", rendered.lower())
        rendered = install.render_file("sql/06_load_table.sql", self.config,
            candidate='"DB"."SCHEMA".LOAD_PLACE_12345678',
            projection=install.projection("place"), release="2026-08-19.0",
            theme="places", type="place", filter="")
        self.assertIn("$1:geometry", rendered)
        self.assertIn("try_to_geography", rendered)

    def test_source_independence(self) -> None:
        updated = deepcopy(self.config)
        updated["source"]["place"] = "MY_LOADED_DATA.OVERTURE.PLACE"
        original = install.render_file("semantic/overture.sql", self.config)
        changed = install.render_file("semantic/overture.sql", updated)
        self.assertEqual(original.replace(install.marker(self.config), "MARKER"),
                         changed.replace(install.marker(updated), "MARKER"))

    def test_geometry_grain_and_no_spatial_relationship(self) -> None:
        sql = install.render_file("semantic/overture.sql", self.config)
        self.assertIn("areas.area_id as ID", sql)
        self.assertIn("places.place_id as ID", sql)
        self.assertIn("st_union_agg", sql)
        self.assertIn("area_to_division", sql)
        self.assertNotIn("places(GEOM)", sql)

    def test_prompt_contract(self) -> None:
        prompts = json.loads((ROOT / "examples/prompts.json").read_text())
        self.assertEqual(len(prompts), 5)
        for prompt in prompts:
            self.assertTrue(prompt["reference_sql"].startswith("select "))
            self.assertIn("limit ", prompt["reference_sql"])
            self.assertIn(prompt["layer"], {"latlon", "h3", "geojson"})
            self.assertTrue(prompt["caveat"])
        self.assertIn("ai_verified_queries", install.verified_queries(self.config))

    def test_agent_json_roundtrip(self) -> None:
        sql = install.agent_sql(self.config, {"RELEASE": "unknown", "COVERAGE": "test"})
        encoded = sql.split("from specification ", 1)[1][1:-2]
        decoded = encoded.replace("''", "'").replace("\\\\", "\\")
        spec = json.loads(decoded)
        self.assertEqual(spec["models"]["orchestration"], "auto")
        self.assertEqual(len(spec["instructions"]["sample_questions"]), 5)
        self.assertNotIn("data_to_map", str(spec["tools"]))

    def test_cli_multiple_json_results(self) -> None:
        result = subprocess.CompletedProcess([], 0, '[{"status":"ok"}]\n[{"name":"PLACE"}]', '')
        with patch.object(install.subprocess, "run", return_value=result) as mocked:
            self.assertEqual(install.SnowCLI(self.config).query("show tables"), [{"NAME": "PLACE"}])
            self.assertIn("NONE", mocked.call_args.args[0])

    def test_execute_required(self) -> None:
        for command in ("install", "load-s3", "cleanup"):
            with self.assertRaises(ValueError):
                install.run(command, self.config, False)

    def test_unowned_schema_rejected(self) -> None:
        client = unittest.mock.Mock()
        client.query.return_value = [{"SCHEMA_NAME": self.config["schema"], "COMMENT": "someone else"}]
        with self.assertRaises(ValueError):
            install.ensure_namespace(self.config, client, create=True)
        self.assertEqual(client.query.call_count, 1)

    def test_no_broad_grants_or_drop(self) -> None:
        grants = install.grant_sql(self.config).lower()
        self.assertNotIn("all privileges", grants)
        self.assertNotIn("raw_place", grants)
        self.assertNotIn("to role public", grants)

    def test_invalid_configuration(self) -> None:
        for changes in ({"place_bbox": [0, 0, 0, 1]}, {"place_bbox": [0, 0, float('nan'), 1]},
                        {"release": "latest; drop schema test"},
                        {"storage_integration": 'x;drop table y'}):
            config = deepcopy(self.config)
            config["s3"].update(changes)
            with patch.object(install.tomllib, "loads", return_value=config):
                with self.assertRaises(ValueError):
                    install.load_config(ROOT / "config.example.toml")

    def test_s3_config_derives_raw_sources(self) -> None:
        config = deepcopy(self.config)
        config["source"]["mode"] = "s3"
        with patch.object(install.tomllib, "loads", return_value=config):
            result = install.load_config(ROOT / "config.example.toml")
        self.assertEqual(result["source"]["place"], "MY_DATABASE.OVERTURE_SEMANTIC_V1.RAW_PLACE")

    def test_modified_object_blocks_cleanup(self) -> None:
        client = unittest.mock.Mock()
        client.query.return_value = [{"NAME": "PLACE", "COMMENT": "not our object"}]
        with self.assertRaises(ValueError):
            install.check_owned_objects(self.config, client, allow_candidates=True)

    def test_load_failure_does_not_publish(self) -> None:
        config = deepcopy(self.config)
        config["source"]["mode"] = "s3"
        client = unittest.mock.Mock()
        files = [{"NAME": f"release/2026-08-19.0/theme=x/type={theme}/part-00000.parquet"}
                 for theme in install.THEMES]
        client.query.side_effect = [[], [], files, [],
            [{"ROW_COUNT": "10", "INVALID": "1", "NULL_IDS": "0", "IDS": "10"}]]
        with patch.object(install, "ensure_namespace"), patch.object(install, "check_owned_objects"):
            with self.assertRaises(ValueError):
                install.load_s3(config, client)
        statements = " ".join(call.args[0] for call in client.query.call_args_list)
        self.assertNotIn("rename to", statements)
        self.assertNotIn("create view", statements)

    def test_complete_load_validates_before_rename(self) -> None:
        config = deepcopy(self.config)
        config["source"]["mode"] = "s3"
        client = unittest.mock.Mock()
        files = [{"NAME": f"release/2026-08-19.0/theme=x/type={theme}/part-00000.parquet"}
                 for theme in install.THEMES]
        quality = [{"ROW_COUNT": "10", "INVALID": "0", "NULL_IDS": "0", "IDS": "10"}]
        client.query.side_effect = [[], [], files] + [[], quality] * 4 + [
            [{"ORPHAN_COUNT": "0"}], [], [], [], [], []]
        with patch.object(install, "ensure_namespace"), patch.object(install, "check_owned_objects"):
            install.load_s3(config, client)
        statements = [call.args[0] for call in client.query.call_args_list]
        validation_end = next(index for index, sql in enumerate(statements) if "orphan_count" in sql)
        rename_start = next(index for index, sql in enumerate(statements) if "rename to" in sql)
        self.assertLess(validation_end, rename_start)
        self.assertIn("DATASET_INFO", statements[-1])

    def test_existing_source_inside_target_rejected(self) -> None:
        config = deepcopy(self.config)
        config["source"]["place"] = "MY_DATABASE.OVERTURE_SEMANTIC_V1.PLACE"
        with patch.object(install.tomllib, "loads", return_value=config):
            with self.assertRaises(ValueError):
                install.load_config(ROOT / "config.example.toml")

    def test_matching_namespace_does_not_recreate(self) -> None:
        client = unittest.mock.Mock()
        client.query.return_value = [{"SCHEMA_NAME": self.config["schema"],
                                      "COMMENT": install.marker(self.config)}]
        install.ensure_namespace(self.config, client, create=True)
        self.assertEqual(client.query.call_count, 1)
        self.assertTrue(client.query.call_args.args[0].startswith("select "))

    def test_load_refuses_existing_raw_tables(self) -> None:
        config = deepcopy(self.config)
        config["source"]["mode"] = "s3"
        client = unittest.mock.Mock()
        client.query.return_value = [{"NAME": "RAW_PLACE"}]
        with patch.object(install, "ensure_namespace"), patch.object(install, "check_owned_objects"):
            with self.assertRaises(ValueError):
                install.load_s3(config, client)
        self.assertEqual(client.query.call_count, 1)

    def test_verify_filters_semantic_smoke_query(self) -> None:
        client = unittest.mock.Mock()
        client.query.side_effect = [[{"ID": "test-place"}]] * 4 + [
            [{"PLACE_ID": "test-place"}], [], [{"SERVING_STATE": "ACTIVE"}]]
        with patch("builtins.print"):
            install.verify(self.config, client)
        semantic_sql = client.query.call_args_list[4].args[0]
        self.assertIn("where places.place_id = 'test-place'", semantic_sql)


if __name__ == "__main__":
    unittest.main()