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
        self.cowork_row = {"NAME": "OVERTURE_MAPS_AGENT", "DATABASE_NAME": "MY_DATABASE",
                           "SCHEMA_NAME": "OVERTURE_SEMANTIC"}

    def cowork_client(self, objects, agents):
        statements = []
        def query(sql):
            statements.append(sql)
            if sql == "show snowflake intelligences":
                return objects
            if sql.startswith("show agents in snowflake intelligence"):
                return agents
            return []
        return unittest.mock.Mock(query=query), statements

    def test_cowork_registration_adds_only_when_absent(self) -> None:
        client, statements = self.cowork_client([{"NAME": "SNOWFLAKE_INTELLIGENCE_OBJECT_DEFAULT"}], [])
        self.assertEqual(install.register_cowork(self.config, client),
                         '"SNOWFLAKE_INTELLIGENCE_OBJECT_DEFAULT"')
        self.assertIn('alter snowflake intelligence "SNOWFLAKE_INTELLIGENCE_OBJECT_DEFAULT" add agent '
                      '"MY_DATABASE"."OVERTURE_SEMANTIC".OVERTURE_MAPS_AGENT', statements)
        self.assertFalse(any(sql.startswith("create snowflake intelligence") for sql in statements))
        client, statements = self.cowork_client([{"NAME": "SNOWFLAKE_INTELLIGENCE_OBJECT_DEFAULT"}],
                                                [self.cowork_row])
        install.register_cowork(self.config, client)
        self.assertFalse(any("add agent" in sql for sql in statements))
        other = dict(self.cowork_row, SCHEMA_NAME="ELSEWHERE")
        client, statements = self.cowork_client([{"NAME": "SNOWFLAKE_INTELLIGENCE_OBJECT_DEFAULT"}], [other])
        install.register_cowork(self.config, client)
        self.assertTrue(any("add agent" in sql for sql in statements))

    def test_no_cowork_object_registers_nothing(self) -> None:
        # Creating a CoWork object would hide every other agent from every user.
        client, statements = self.cowork_client([], [])
        self.assertIsNone(install.register_cowork(self.config, client))
        self.assertEqual(statements, ["show snowflake intelligences"])
        self.assertNotIn("snowflake intelligence", install.grant_sql(self.config, None))
        client, statements = self.cowork_client([{"NAME": "COMPANY_AI"}], [])
        self.assertEqual(install.register_cowork(self.config, client), '"COMPANY_AI"')
        self.assertIn('alter snowflake intelligence "COMPANY_AI" add agent '
                      '"MY_DATABASE"."OVERTURE_SEMANTIC".OVERTURE_MAPS_AGENT', statements)
        self.assertIn('grant usage on snowflake intelligence "COMPANY_AI" to role "MY_VIEWER_ROLE"',
                      install.grant_sql(self.config, '"COMPANY_AI"'))

    def test_install_and_render_never_create_cowork_object(self) -> None:
        for objects in ([], [{"NAME": "SNOWFLAKE_INTELLIGENCE_OBJECT_DEFAULT"}]):
            client, statements = self.cowork_client(objects, [])
            with patch.object(install, "preflight"), patch.object(install, "pre_deploy"), \
                    patch.object(install, "deploy_model"), patch.object(install, "verify"):
                install.execute_workflow("install", self.config, client)
            self.assertFalse(any(sql.lower().startswith("create snowflake intelligence")
                                 for sql in statements))
            self.assertEqual(any("add agent" in sql for sql in statements), bool(objects))
        with patch("builtins.print") as printed:
            install.run("render", self.config, execute=False)
        rendered = "\n".join(str(call.args[0]) for call in printed.call_args_list if call.args)
        executable = [line for line in rendered.splitlines() if not line.lstrip().startswith("--")]
        self.assertFalse(any("snowflake intelligence" in line.lower() for line in executable))

    def test_cowork_privilege_error_names_grant(self) -> None:
        error = install.subprocess.CalledProcessError(1, "snow", stderr="Insufficient privileges")
        client = unittest.mock.Mock()
        client.query.side_effect = error
        with self.assertRaisesRegex(ValueError, "MODIFY on the CoWork object"):
            install.register_cowork(self.config, client)

    def test_verify_requires_cowork_listing_only_with_object(self) -> None:
        checks = [[{"ID": "p"}]] * 4 + [[{"PLACE_ID": "p"}], [], [{"SERVING_STATE": "ACTIVE"}]]
        client = unittest.mock.Mock()
        client.query.side_effect = checks + [[{"NAME": "SNOWFLAKE_INTELLIGENCE_OBJECT_DEFAULT"}], []]
        with patch("builtins.print"), self.assertRaisesRegex(ValueError, "not added to the CoWork object"):
            install.verify(self.config, client)
        client.query.side_effect = checks + [[{"NAME": "SNOWFLAKE_INTELLIGENCE_OBJECT_DEFAULT"}],
                                             [self.cowork_row]]
        with patch("builtins.print"):
            self.assertEqual(install.verify(self.config, client), "passed")
        client.query.side_effect = checks + [[]]
        with patch("builtins.print"):
            self.assertEqual(install.verify(self.config, client), "not required")

    def test_cleanup_removes_agent_from_cowork_only(self) -> None:
        client, statements = self.cowork_client([{"NAME": "SNOWFLAKE_INTELLIGENCE_OBJECT_DEFAULT"}],
                                                [self.cowork_row])
        install.drop_cowork_and_agent(self.config, client)
        drop_cowork = next(i for i, sql in enumerate(statements) if "drop agent " in sql
                           and sql.startswith("alter snowflake intelligence"))
        drop_agent = next(i for i, sql in enumerate(statements) if sql.startswith("drop agent"))
        self.assertLess(drop_cowork, drop_agent)
        self.assertFalse(any(sql.startswith("drop snowflake intelligence") for sql in statements))

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
        rendered = install.render_file("sql/06_load_table.sql", self.config,
            candidate='"DB"."SCHEMA".LOAD_PLACE_12345678',
            projection=install.projection("place"), release="2026-08-19.0",
            theme="places", type="place", filter="")
        self.assertIn("$1:geometry", rendered)
        self.assertIn("try_to_geography($1:geometry::binary, true)", rendered)
        setup = install.render_file("sql/05_s3_setup.sql", self.config, integration="")
        self.assertIn('"MY_DATABASE"."OVERTURE_SEMANTIC_RAW".OVERTURE_S3', setup)
        search = install.search_sql(self.config)
        self.assertIn('"OVERTURE_SEMANTIC".CATEGORY_SEARCH', search)
        self.assertIn('from "MY_DATABASE"."OVERTURE_SEMANTIC".CATEGORY_VOCAB', search)

    def optional_config(self, themes=install.OPTIONAL_THEMES, mode="existing"):
        config = deepcopy(self.config)
        config["source"]["optional_themes"] = list(themes)
        config["source"]["mode"] = mode
        with patch.object(install.tomllib, "loads", return_value=config):
            return install.load_config(ROOT / "config.example.toml")

    def test_optional_prompts_follow_installed_themes(self) -> None:
        ids = {prompt["id"] for prompt in install.available_prompts(self.optional_config(["building"]))}
        self.assertIn("tallest_buildings", ids)
        self.assertNotIn("market_street_addresses", ids)
        self.assertNotIn("tallest_buildings", {p["id"] for p in install.available_prompts(self.config)})
        variables = install.dcm_variables(self.optional_config(["building"]), {"RELEASE": "r", "COVERAGE": "c"})
        self.assertEqual(variables["optional_themes"], ["building"])
        self.assertNotIn("source_address", variables)

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
        self.assertEqual(loaded["source"]["segment"], "MY_DATABASE.OVERTURE_SEMANTIC_RAW.RAW_SEGMENT")

    def test_optional_s3_load_is_clipped(self) -> None:
        config = self.optional_config(["building"], "s3")
        client = unittest.mock.Mock()
        files = [{"NAME": f"release/2026-08-19.0/theme=x/type={theme}/part-00000.parquet"}
                 for theme in install.THEMES + ("building",)]
        quality = [{"ROW_COUNT": "10", "INVALID": "0", "NULL_IDS": "0", "IDS": "10"}]
        client.query.side_effect = [[], [], files] + [[], quality] * 5 + [
            [{"ORPHAN_COUNT": "0"}], [], [], [], [], [], []]
        with patch.object(install, "ensure_owned_schema"), patch.object(install, "check_raw_objects"), \
                patch.object(install, "fetch_published_release", return_value="2026-08-19.0"):
            install.load_s3(config, client)
        building = next(call.args[0] for call in client.query.call_args_list
                        if "theme=buildings/type=building" in call.args[0])
        self.assertIn("$1:bbox:xmin::float <=", building)
        self.assertIn("$1:bbox::variant as BBOX", building)

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
        macro = (ROOT / "dcm/sources/macros/verified_queries.sql").read_text()
        self.assertIn("ai_verified_queries", macro)
        for prompt in prompts:
            self.assertIn(f"{prompt['id']} as (question ", macro)

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
        for command in sorted(install.MUTATING):
            with self.subTest(command=command), self.assertRaises(ValueError):
                install.run(command, self.config, False)
        self.assertEqual(set(install.SQL_COMMANDS) & set(install.CLI_COMMANDS), set())

    def test_unowned_schema_rejected(self) -> None:
        client = unittest.mock.Mock()
        client.query.return_value = [{"SCHEMA_NAME": self.config["schema"], "COMMENT": "someone else"}]
        with self.assertRaisesRegex(ValueError, "not created by this sample"):
            install.pre_deploy(self.config, client)
        self.assertEqual(client.query.call_count, 1)
        client.reset_mock()
        with self.assertRaisesRegex(ValueError, "not created by this sample"):
            install.ensure_owned_schema(self.config, client, "project_schema")
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
        self.assertEqual(result["source"]["place"], "MY_DATABASE.OVERTURE_SEMANTIC_RAW.RAW_PLACE")
        self.assertEqual(result["project_schema"], "OVERTURE_SEMANTIC_DCM")

    def test_schemas_must_differ(self) -> None:
        for key in ("project_schema", "raw_schema"):
            config = deepcopy(self.config)
            config[key] = config["schema"]
            with self.subTest(key=key), patch.object(install.tomllib, "loads", return_value=config):
                with self.assertRaisesRegex(ValueError, "must all differ"):
                    install.load_config(ROOT / "config.example.toml")

    def test_modified_object_blocks_cleanup(self) -> None:
        client = unittest.mock.Mock()
        client.query.return_value = [{"NAME": "RAW_PLACE", "COMMENT": "not our object"}]
        with self.assertRaises(ValueError):
            install.check_raw_objects(self.config, client, allow_candidates=True)

    def test_load_failure_does_not_publish(self) -> None:
        config = deepcopy(self.config)
        config["source"]["mode"] = "s3"
        client = unittest.mock.Mock()
        files = [{"NAME": f"release/2026-08-19.0/theme=x/type={theme}/part-00000.parquet"}
                 for theme in install.THEMES]
        client.query.side_effect = [[], [], files, [],
            [{"ROW_COUNT": "10", "INVALID": "1", "NULL_IDS": "0", "IDS": "10"}]]
        with patch.object(install, "ensure_owned_schema"), patch.object(install, "check_raw_objects"), \
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
        with patch.object(install, "ensure_owned_schema"), patch.object(install, "check_raw_objects"), \
                patch.object(install, "fetch_published_release", return_value="2026-08-19.0"):
            install.load_s3(config, client)
        statements = [call.args[0] for call in client.query.call_args_list]
        self.assertIn("list @", statements[2])
        self.assertIn("/2026-08-19.0/", statements[2])
        validation_end = next(index for index, sql in enumerate(statements) if "orphan_count" in sql)
        rename_start = next(index for index, sql in enumerate(statements) if "rename to" in sql)
        self.assertLess(validation_end, rename_start)
        self.assertIn("LOAD_INFO", statements[-1])

    def test_existing_source_inside_target_rejected(self) -> None:
        config = deepcopy(self.config)
        config["source"]["place"] = "MY_DATABASE.OVERTURE_SEMANTIC.PLACE"
        with patch.object(install.tomllib, "loads", return_value=config):
            with self.assertRaises(ValueError):
                install.load_config(ROOT / "config.example.toml")

    def test_legacy_schema_requires_migration(self) -> None:
        client = unittest.mock.Mock()
        client.query.return_value = [{"SCHEMA_NAME": self.config["schema"],
                                      "COMMENT": "overture-maps-semantic-layer:1.1.0:936812ea263f3824"}]
        self.assertEqual(install.model_state(self.config, client), "legacy")
        with self.assertRaisesRegex(ValueError, "migrate-legacy"):
            install.pre_deploy(self.config, client)
        client.query.return_value = [{"SCHEMA_NAME": self.config["schema"],
                                      "COMMENT": install.marker(self.config)}]
        self.assertEqual(install.model_state(self.config, client), "dcm")
        client.query.return_value = []
        self.assertEqual(install.model_state(self.config, client), "absent")
        self.assertTrue(install.marker(self.config).startswith("overture-maps-semantic-layer:2."))

    def test_migrate_legacy_refuses_foreign_objects(self) -> None:
        legacy = [{"SCHEMA_NAME": "OVERTURE_SEMANTIC", "COMMENT": "overture-maps-semantic-layer:1.1.0:x"}]
        for row in ({"NAME": "PLACE", "COMMENT": "my overture view"},
                    {"NAME": "MY_VIEW", "COMMENT": "overture-maps-semantic-layer:1.1.0:x"}):
            statements = []
            def query(sql, row=row):
                statements.append(sql)
                if "information_schema.schemata" in sql:
                    return legacy
                return [row] if sql.startswith("show VIEWS") or sql.startswith("show views") else []
            with self.subTest(row=row), self.assertRaisesRegex(ValueError, "refusing to migrate"):
                install.migrate_legacy(self.config, unittest.mock.Mock(query=query))
            self.assertFalse(any(sql.startswith(("drop", "alter")) for sql in statements))
        self.assertFalse(install.owned(None))
        self.assertFalse(install.owned("xoverture-maps-semantic-layer:"))

    def test_migrate_legacy_moves_raw_data_then_clears_schema(self) -> None:
        legacy = "overture-maps-semantic-layer:1.1.0:x"
        statements = []
        def query(sql):
            statements.append(sql)
            if "information_schema.schemata" in sql:
                return [{"SCHEMA_NAME": "S", "COMMENT": legacy}] if "'OVERTURE_SEMANTIC'" in sql else []
            if sql.startswith("show tables in schema \"MY_DATABASE\".\"OVERTURE_SEMANTIC\""):
                return [{"NAME": "RAW_PLACE", "COMMENT": legacy}, {"NAME": "DATASET_INFO", "COMMENT": legacy}]
            return []
        install.migrate_legacy(self.config, unittest.mock.Mock(query=query))
        load_info = next(i for i, sql in enumerate(statements) if "LOAD_INFO" in sql)
        move = next(i for i, sql in enumerate(statements) if "rename to" in sql)
        drop_info = next(i for i, sql in enumerate(statements)
                         if sql.startswith("drop TABLE") and "DATASET_INFO" in sql)
        self.assertLess(load_info, move)
        self.assertLess(move, drop_info)
        self.assertIn('rename to "MY_DATABASE"."OVERTURE_SEMANTIC_RAW"."RAW_PLACE"', statements[move])
        self.assertTrue(statements[-1].startswith('drop schema "MY_DATABASE"."OVERTURE_SEMANTIC" restrict'))

    def test_install_runs_phases_in_order(self) -> None:
        calls = []
        def record(name):
            return lambda *args, **kwargs: calls.append(name)
        with patch.object(install, "preflight", record("preflight")), \
                patch.object(install, "pre_deploy", record("pre-deploy")), \
                patch.object(install, "deploy_model", lambda c, cl, command: calls.append(command)), \
                patch.object(install, "post_deploy", record("post-deploy")):
            install.execute_workflow("install", self.config, unittest.mock.Mock())
        self.assertEqual(calls, ["preflight", "pre-deploy", "deploy", "post-deploy"])

    def test_pre_deploy_creates_project_container_only(self) -> None:
        statements = []
        client = unittest.mock.Mock(query=lambda sql: statements.append(sql) or [])
        install.pre_deploy(self.config, client)
        executed = [sql for sql in statements if not sql.startswith("select")]
        self.assertEqual(executed[0].split(" comment")[0],
                         'create schema "MY_DATABASE"."OVERTURE_SEMANTIC_DCM"')
        self.assertTrue(executed[1].startswith(
            'create dcm project if not exists "MY_DATABASE"."OVERTURE_SEMANTIC_DCM".'
            "OVERTURE_MAPS_SEMANTIC_LAYER"))
        self.assertEqual(len(executed), 2)

    def test_post_deploy_builds_search_before_agent(self) -> None:
        statements = []
        client, statements = self.cowork_client([], [])
        with patch.object(install, "verify", return_value="not required"):
            self.assertEqual(install.post_deploy(self.config, client), "not required")
        search = next(i for i, sql in enumerate(statements) if "cortex search service" in sql)
        agent = next(i for i, sql in enumerate(statements) if sql.startswith("create or replace agent"))
        self.assertLess(search, agent)
        self.assertFalse(any("DEFINE" in sql or "semantic view" in sql.lower() for sql in statements))

    def test_run_dcm_arguments(self) -> None:
        version = subprocess.CompletedProcess([], 0, "Snowflake CLI version: 3.31.0\n", "")
        result = subprocess.CompletedProcess([], 0, "Deployed 12 entities.\n", "")
        with patch.object(install.subprocess, "run", side_effect=[version, version, result]) as mocked:
            install.run_dcm("deploy", self.config, Path("/tmp/x"))
        arguments = mocked.call_args.args[0]
        self.assertEqual(arguments[1:4], ["dcm", "deploy",
                                          "MY_DATABASE.OVERTURE_SEMANTIC_DCM.OVERTURE_MAPS_SEMANTIC_LAYER"])
        for flag, value in (("--from", "/tmp/x"), ("--connection", "my_connection"),
                            ("--role", "MY_INSTALLER_ROLE")):
            self.assertEqual(arguments[arguments.index(flag) + 1], value)
        self.assertTrue(arguments[arguments.index("--alias") + 1].startswith(
            install.marker(self.config) + ":deploy:"))
        old = subprocess.CompletedProcess([], 0, "Snowflake CLI version: 3.16.0\n", "")
        with patch.object(install.subprocess, "run", return_value=old):
            with self.assertRaisesRegex(ValueError, "SNOW_CLI"):
                install.run_dcm("plan", self.config, Path("/tmp/x"))

    def test_cleanup_purges_model_before_dropping_project(self) -> None:
        statements = []
        def query(sql):
            statements.append(sql)
            if "information_schema.schemata" in sql:
                return [{"SCHEMA_NAME": "S", "COMMENT": install.marker(self.config)}]
            if sql.startswith("show dcm projects"):
                return [{"NAME": install.DCM_PROJECT}]
            return []
        with patch.object(install, "run_dcm", side_effect=lambda *a, **k: statements.append("PURGE")), \
                patch.object(install, "cowork_object", return_value=None):
            install.cleanup(self.config, unittest.mock.Mock(query=query))
        order = [next(i for i, sql in enumerate(statements) if needle in sql)
                 for needle in ("drop agent if exists", "PURGE", "drop dcm project",
                                'drop schema "MY_DATABASE"."OVERTURE_SEMANTIC_DCM"',
                                'drop schema "MY_DATABASE"."OVERTURE_SEMANTIC_RAW"')]
        self.assertEqual(order, sorted(order))
        self.assertFalse(any('drop schema "MY_DATABASE"."OVERTURE_SEMANTIC" ' in sql for sql in statements))

    def test_s3_dataset_comes_from_load_info(self) -> None:
        config = self.optional_config([], "s3")
        client = unittest.mock.Mock()
        client.query.side_effect = [[{"SCHEMA_NAME": "R", "COMMENT": "m"}],
                                    [{"RELEASE": "2026-10-21.0", "COVERAGE": "Places bbox"}]]
        self.assertEqual(install.dataset(config, client),
                         {"RELEASE": "2026-10-21.0", "COVERAGE": "Places bbox"})
        self.assertIn('"OVERTURE_SEMANTIC_RAW".LOAD_INFO', client.query.call_args.args[0])
        client.query.side_effect = [[]]
        with self.assertRaisesRegex(ValueError, "run load-s3 first"):
            install.dataset(config, client)

    def test_load_into_loaded_schema_points_to_cleanup(self) -> None:
        config = deepcopy(self.config)
        config["source"]["mode"] = "s3"
        client = unittest.mock.Mock()
        client.query.return_value = [{"NAME": "RAW_PLACE"}]
        with patch.object(install, "ensure_owned_schema"), patch.object(install, "check_raw_objects"):
            with self.assertRaisesRegex(ValueError, "run cleanup first"):
                install.load_s3(config, client)

    def test_matching_namespace_does_not_recreate(self) -> None:
        client = unittest.mock.Mock()
        client.query.return_value = [{"SCHEMA_NAME": self.config["project_schema"],
                                      "COMMENT": install.marker(self.config)}]
        install.ensure_owned_schema(self.config, client, "project_schema")
        self.assertEqual(client.query.call_count, 1)
        self.assertTrue(client.query.call_args.args[0].startswith("select "))

    def test_load_refuses_existing_raw_tables(self) -> None:
        config = deepcopy(self.config)
        config["source"]["mode"] = "s3"
        client = unittest.mock.Mock()
        client.query.return_value = [{"NAME": "RAW_PLACE"}]
        with patch.object(install, "ensure_owned_schema"), patch.object(install, "check_raw_objects"):
            with self.assertRaises(ValueError):
                install.load_s3(config, client)
        self.assertEqual(client.query.call_count, 1)

    def test_verify_filters_semantic_smoke_query(self) -> None:
        client = unittest.mock.Mock()
        client.query.side_effect = [[{"ID": "test-place"}]] * 4 + [
            [{"PLACE_ID": "test-place"}], [], [{"SERVING_STATE": "ACTIVE"}],
            [{"NAME": "SNOWFLAKE_INTELLIGENCE_OBJECT_DEFAULT"}], [self.cowork_row]]
        with patch("builtins.print"):
            install.verify(self.config, client)
        semantic_sql = client.query.call_args_list[4].args[0]
        self.assertIn("where places.place_id = 'test-place'", semantic_sql)


if __name__ == "__main__":
    unittest.main()