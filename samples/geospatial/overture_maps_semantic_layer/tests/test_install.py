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
        self.assertEqual(install.resolve_release(rows, "2026-08-19.0"), "2026-08-19.0")
        with self.assertRaises(ValueError):
            install.resolve_release(rows, "2026-09-23.0")
        with self.assertRaises(ValueError):
            install.resolve_release(rows, "2026-08-19.0", install.THEMES + ("building",))

    def test_stac_release_parsing(self) -> None:
        self.assertEqual(install.published_release({"latest": "2026-10-21.0"}), "2026-10-21.0")
        for document in ({}, {"latest": "main"}, {"latest": "2026-10-21.0; drop"}, [], None):
            with self.assertRaises(ValueError):
                install.published_release(document)

    def test_unreachable_stac_fails_closed(self) -> None:
        with patch("urllib.request.urlopen", side_effect=OSError("offline")):
            with self.assertRaisesRegex(ValueError, "pin s3.release"):
                install.fetch_published_release()

    def test_choose_source_fails_closed(self) -> None:
        imported = {"KIND": "IMPORTED DATABASE", "ORIGIN": "X.Y.SHARE"}
        self.assertEqual(install.choose_source(imported, None), "imported")
        self.assertEqual(install.choose_source({"KIND": "STANDARD"}, None), "existing")
        self.assertEqual(install.choose_source(None, None), "s3")
        self.assertEqual(install.choose_source(None, {"IS_READY_FOR_IMPORT": "true"}), "marketplace")
        self.assertEqual(install.choose_source(None, {"IS_READY_FOR_IMPORT": "false"}), "s3")
        for listing in ({}, {"IS_READY_FOR_IMPORT": None}, {"IS_READY_FOR_IMPORT": "pending"}):
            with self.assertRaisesRegex(ValueError, "unknown"):
                install.choose_source(None, listing)

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
            self.assertNotIn("if not exists", rendered.lower())
            self.assertIn("create or replace", rendered.lower())
        rendered = install.render_file("sql/06_load_table.sql", self.config,
            candidate='"DB"."SCHEMA".LOAD_PLACE_12345678',
            projection=install.projection("place"), release="2026-08-19.0",
            theme="places", type="place", filter="")
        self.assertIn("$1:geometry", rendered)
        self.assertIn("try_to_geography($1:geometry::binary, true)", rendered)

    def optional_config(self, themes=install.OPTIONAL_THEMES, mode="existing"):
        config = deepcopy(self.config)
        config["source"]["optional_themes"] = list(themes)
        config["source"]["mode"] = mode
        with patch.object(install.tomllib, "loads", return_value=config):
            return install.load_config(ROOT / "config.example.toml")

    def test_optional_themes_render_into_one_semantic_view(self) -> None:
        base = install.render_file("semantic/overture.sql", self.config)
        self.assertNotIn("BBOX", base)
        self.assertNotIn("tallest_buildings", base)
        config = self.optional_config()
        sql = install.render_file("semantic/overture.sql", config)
        for expected in ("addresses as", "segments as", "buildings as", "buildings.bbox_xmin",
                         "segments.speed_limit_kmh", "tallest_buildings as",
                         "must filter bbox_xmin"):
            self.assertIn(expected, sql)
        self.assertNotIn("$extra", sql)
        self.assertNotIn("$ns", sql)
        self.assertIn('"OVERTURE_SEMANTIC".BUILDING primary key', sql)
        self.assertEqual(install.install_files(config)[1:4],
                         [f"sql/themes/{theme}.sql" for theme in install.OPTIONAL_THEMES])
        for name in install.install_files(config):
            rendered = install.render_file(name, config)
            self.assertNotIn("--", rendered)
            self.assertNotIn("if not exists", rendered.lower())

    def test_optional_prompts_follow_installed_themes(self) -> None:
        ids = {prompt["id"] for prompt in install.available_prompts(self.optional_config(["building"]))}
        self.assertIn("tallest_buildings", ids)
        self.assertNotIn("market_street_addresses", ids)
        self.assertNotIn("tallest_buildings", {p["id"] for p in install.available_prompts(self.config)})
        grants = install.grant_sql(self.optional_config(["building"])).lower()
        self.assertIn('view "my_database"."overture_semantic".building', grants.replace('"building"', "building"))
        self.assertNotIn("address", install.grant_sql(self.config).lower())

    def test_optional_theme_configuration(self) -> None:
        for themes in (["connector"], ["building", "building"], "building"):
            with self.assertRaises(ValueError):
                self.optional_config(themes)
        config = deepcopy(self.config)
        config["s3"]["place_bbox"] = []
        config["source"].update(mode="s3", optional_themes=["building"])
        with patch.object(install.tomllib, "loads", return_value=config):
            with self.assertRaisesRegex(ValueError, "too large"):
                install.load_config(ROOT / "config.example.toml")
        loaded = self.optional_config(["segment"], "s3")
        self.assertEqual(loaded["source"]["segment"], "MY_DATABASE.OVERTURE_SEMANTIC.RAW_SEGMENT")

    def test_optional_s3_load_is_clipped(self) -> None:
        config = self.optional_config(["building"], "s3")
        client = unittest.mock.Mock()
        files = [{"NAME": f"release/2026-08-19.0/theme=x/type={theme}/part-00000.parquet"}
                 for theme in install.THEMES + ("building",)]
        quality = [{"ROW_COUNT": "10", "INVALID": "0", "NULL_IDS": "0", "IDS": "10"}]
        client.query.side_effect = [[], [], files] + [[], quality] * 5 + [
            [{"ORPHAN_COUNT": "0"}], [], [], [], [], [], []]
        with patch.object(install, "ensure_namespace"), patch.object(install, "check_owned_objects"), \
                patch.object(install, "fetch_published_release", return_value="2026-08-19.0"):
            install.load_s3(config, client)
        building = next(call.args[0] for call in client.query.call_args_list
                        if "theme=buildings/type=building" in call.args[0])
        self.assertIn("$1:bbox:xmin::float <=", building)
        self.assertIn("$1:bbox::variant as BBOX", building)

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
        self.assertEqual(len(prompts), 9)
        for prompt in prompts:
            self.assertTrue(prompt["reference_sql"].startswith("select "))
            self.assertIn("limit ", prompt["reference_sql"])
            self.assertIn(prompt["layer"], {"latlon", "h3", "geojson", "lines"})
            self.assertTrue(prompt["caveat"])
            self.assertTrue(set(prompt["requires"]) <= set(install.ALL_THEMES))
            if set(prompt["requires"]) & set(install.OPTIONAL_THEMES):
                for column in ("bbox_xmin", "bbox_xmax", "bbox_ymin", "bbox_ymax"):
                    self.assertIn(column, prompt["reference_sql"])
        self.assertIn("ai_verified_queries", install.verified_queries(self.config))

    def test_agent_json_roundtrip(self) -> None:
        sql = install.agent_sql(self.config, {"RELEASE": "unknown", "COVERAGE": "test"})
        encoded = sql.split("from specification ", 1)[1][1:-2]
        decoded = encoded.replace("''", "'").replace("\\\\", "\\")
        spec = json.loads(decoded)
        self.assertEqual(spec["models"]["orchestration"], "auto")
        self.assertEqual(len(spec["instructions"]["sample_questions"]), 6)
        self.assertNotIn("data_to_map", str(spec["tools"]))
        self.assertEqual(spec["tool_resources"]["overture_maps"]["execution_environment"],
                         {"type": "warehouse", "warehouse": self.config["warehouse"].upper()})
        instructions = spec["instructions"]["orchestration"]
        self.assertIn("at most\nonce", instructions)
        self.assertIn("not physical GEOM or CLASS", instructions)
        self.assertIn("Never\ninvent a result ID", instructions)

    def test_default_s3_keeps_sf_h3_example(self) -> None:
        self.config["source"]["mode"] = "s3"
        sql = install.agent_sql(self.config, {"COVERAGE": "SF"})
        encoded = sql.split("from specification ", 1)[1][1:-2]
        spec = json.loads(encoded.replace("''", "'").replace("\\\\", "\\"))
        questions = [item["question"] for item in spec["instructions"]["sample_questions"]]
        self.assertEqual(len(questions), 5)
        self.assertTrue(any("San Francisco" in text and "hexagons" in text
                            for text in questions))
        self.assertFalse(any("Berlin" in text for text in questions))

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
        self.assertEqual(result["source"]["place"], "MY_DATABASE.OVERTURE_SEMANTIC.RAW_PLACE")

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
        with patch.object(install, "ensure_namespace"), patch.object(install, "check_owned_objects"), \
                patch.object(install, "fetch_published_release", return_value="2026-08-19.0"):
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
        with patch.object(install, "ensure_namespace"), patch.object(install, "check_owned_objects"), \
                patch.object(install, "fetch_published_release", return_value="2026-08-19.0"):
            install.load_s3(config, client)
        statements = [call.args[0] for call in client.query.call_args_list]
        self.assertIn("list @", statements[2])
        self.assertIn("/2026-08-19.0/", statements[2])
        validation_end = next(index for index, sql in enumerate(statements) if "orphan_count" in sql)
        rename_start = next(index for index, sql in enumerate(statements) if "rename to" in sql)
        self.assertLess(validation_end, rename_start)
        self.assertIn("DATASET_INFO", statements[-1])

    def test_existing_source_inside_target_rejected(self) -> None:
        config = deepcopy(self.config)
        config["source"]["place"] = "MY_DATABASE.OVERTURE_SEMANTIC.PLACE"
        with patch.object(install.tomllib, "loads", return_value=config):
            with self.assertRaises(ValueError):
                install.load_config(ROOT / "config.example.toml")

    def test_older_sample_schema_is_upgraded_in_place(self) -> None:
        client = unittest.mock.Mock()
        client.query.return_value = [{"SCHEMA_NAME": self.config["schema"],
                                      "COMMENT": "overture-maps-semantic-layer:1.0.0:e401a0fe112a9d5a"}]
        install.ensure_namespace(self.config, client, create=True)
        update = client.query.call_args_list[-1].args[0]
        self.assertTrue(update.startswith("alter schema"))
        self.assertIn(install.marker(self.config), update)
        client.reset_mock()
        install.ensure_namespace(self.config, client)
        self.assertEqual(client.query.call_count, 1)

    def test_older_sample_objects_accepted_foreign_rejected(self) -> None:
        client = unittest.mock.Mock()
        views = lambda row: (lambda sql: [row] if sql.startswith("show VIEWS") else [])
        client.query.side_effect = views({"NAME": "PLACE", "COMMENT": "overture-maps-semantic-layer:0.9:x"})
        install.check_owned_objects(self.config, client)
        for row in ({"NAME": "PLACE", "COMMENT": "my overture view"},
                    {"NAME": "MY_VIEW", "COMMENT": install.marker(self.config)}):
            client.query.side_effect = views(row)
            with self.assertRaises(ValueError):
                install.check_owned_objects(self.config, client)
        self.assertFalse(install.owned(None))
        self.assertFalse(install.owned("xoverture-maps-semantic-layer:"))

    def test_removed_theme_view_dropped_before_model(self) -> None:
        client = unittest.mock.Mock()
        client.query.return_value = [
            {"NAME": "BUILDING", "COMMENT": "overture-maps-semantic-layer:1.1.0:old"},
            {"NAME": "SEGMENT", "COMMENT": "someone else"},
            {"NAME": "PLACE", "COMMENT": "overture-maps-semantic-layer:1.1.0:old"}]
        install.drop_unused_theme_views(self.config, client)
        drops = [call.args[0] for call in client.query.call_args_list if call.args[0].startswith("drop")]
        self.assertEqual(drops, ['drop view "MY_DATABASE"."OVERTURE_SEMANTIC"."BUILDING"'])
        client.reset_mock()
        install.drop_unused_theme_views(self.optional_config(["building"]), client)
        self.assertEqual(client.query.call_count, 1)

    def test_install_prunes_before_replacing_model(self) -> None:
        statements = []
        def query(sql):
            statements.append(sql)
            if sql.startswith("select release"):
                return [{"RELEASE": "r", "COVERAGE": "c"}]
            return []
        client = unittest.mock.Mock(query=query)
        with patch.object(install, "preflight"), patch.object(install, "ensure_namespace"), \
                patch.object(install, "check_owned_objects"), patch.object(install, "verify"):
            install.execute_workflow("install", self.config, client)
        prune = next(i for i, sql in enumerate(statements) if sql.startswith("show views"))
        model = next(i for i, sql in enumerate(statements) if "semantic view" in sql)
        self.assertLess(prune, model)

    def test_load_into_loaded_schema_points_to_cleanup(self) -> None:
        config = deepcopy(self.config)
        config["source"]["mode"] = "s3"
        client = unittest.mock.Mock()
        client.query.return_value = [{"NAME": "RAW_PLACE"}]
        with patch.object(install, "ensure_namespace"), patch.object(install, "check_owned_objects"):
            with self.assertRaisesRegex(ValueError, "run cleanup first"):
                install.load_s3(config, client)

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