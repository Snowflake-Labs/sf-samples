from __future__ import annotations

from copy import deepcopy
from decimal import Decimal
import json
from pathlib import Path
import sys
import unittest
from unittest.mock import Mock, patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))
import agent_install
import install


class MapResultTests(unittest.TestCase):
    def setUp(self) -> None:
        self.prompts = {item["id"]: item for item in
                        json.loads((ROOT / "examples/prompts.json").read_text())}
        self.point = {"ID": "point-1", "NAME": "Example", "LATITUDE": 37.79,
                      "LONGITUDE": -122.39}
        self.ring = [[-122.4, 37.7], [-122.3, 37.7], [-122.3, 37.8], [-122.4, 37.7]]
        self.polygon = {"ID": "area-1", "NAME": "Example", "AREA_SQKM": Decimal("1.2"),
                        "GEOJSON": json.dumps({"type": "Polygon", "coordinates": [self.ring]})}
        self.cell = {"H3_CELL": "8828308281fffff", "PLACE_COUNT": 2}
        self.client = Mock()

    def validate(self, prompt_id: str, rows: list[dict]) -> None:
        install.validate_map_rows(self.prompts[prompt_id], rows, self.client)

    def test_points_accept_finite_numeric_coordinates(self) -> None:
        self.point["LATITUDE"] = Decimal("37.79")
        self.validate("hospital_points", [self.point])
        self.client.query.assert_not_called()

    def test_points_reject_null_text_boolean_nonfinite_and_out_of_range(self) -> None:
        for column, value in (("LATITUDE", None), ("LATITUDE", "37.79"),
                              ("LATITUDE", True), ("LATITUDE", float("nan")),
                              ("LONGITUDE", float("inf")), ("LATITUDE", 91),
                              ("LONGITUDE", -181)):
            with self.subTest(column=column, value=value), self.assertRaises(ValueError):
                self.validate("hospital_points", [{**self.point, column: value}])

    def test_every_row_requires_columns_and_unique_string_ids(self) -> None:
        for bad_row in ({**self.point, "ID": None}, {**self.point, "ID": ""},
                        {**self.point, "ID": 123}, self.point,
                        {"ID": "other", "NAME": "Missing coordinates"}):
            with self.subTest(row=bad_row), self.assertRaises(ValueError):
                self.validate("hospital_points", [self.point, bad_row])

    def test_numeric_measures_reject_invalid_values(self) -> None:
        for value in (None, "2", True, float("nan"), float("inf")):
            with self.subTest(value=value), self.assertRaises(ValueError):
                self.validate("california_counties", [{**self.polygon, "AREA_SQKM": value}])
        with self.assertRaises(ValueError):
            self.validate("nearby_cafes", [{**self.point, "DISTANCE_M": "near"}])

    def test_polygon_and_multipolygon_pass_without_sql(self) -> None:
        self.validate("california_counties", [self.polygon])
        self.polygon["GEOJSON"] = json.dumps({"type": "MultiPolygon",
                                               "coordinates": [[self.ring]]})
        self.validate("reverse_geocode", [self.polygon])
        self.client.query.assert_not_called()

    def test_polygon_rejects_incomplete_or_wrong_geometry(self) -> None:
        invalid_shapes = [None, '{"type":"Polygon",', "null", "[]", "{}",
                          json.dumps({"type": "Polygon", "coordinates": []}),
                          json.dumps({"type": "Point", "coordinates": [-122, 37]}),
                          json.dumps({"type": "Polygon", "coordinates": [self.ring[:-1]]}),
                          json.dumps({"type": "Polygon", "coordinates": [[[0, 91]] * 4]}),
                          json.dumps({"type": "Polygon", "coordinates": [[[0, float("nan")]] * 4]})]
        for shape in invalid_shapes:
            with self.subTest(shape=shape), self.assertRaises(ValueError):
                self.validate("california_counties", [{**self.polygon, "GEOJSON": shape}])

    def test_h3_checks_returned_cells_only(self) -> None:
        self.client.query.return_value = [{"CHECKED": 1, "INVALID": 0}]
        self.validate("sf_hexagons", [self.cell])
        sql = self.client.query.call_args.args[0]
        self.assertIn("h3_is_valid_cell(column1)", sql)
        self.assertIn("h3_get_resolution(column1) != 8", sql)
        self.assertIn("from values ('8828308281fffff')", sql)
        self.assertNotIn(".PLACE", sql)
        self.assertFalse(agent_install.mutates(sql))

    def test_h3_requires_strings_and_valid_resolution_result(self) -> None:
        with self.assertRaises(ValueError):
            self.validate("sf_hexagons", [{**self.cell, "H3_CELL": 613196570331971583}])
        self.client.query.assert_not_called()
        for response in ([], [{"CHECKED": 1, "INVALID": 1}],
                         [{"CHECKED": 0, "INVALID": 0}], [{"CHECKED": 1, "INVALID": None}]):
            self.client.query.return_value = response
            with self.subTest(response=response), self.assertRaises(ValueError):
                self.validate("sf_hexagons", [self.cell])

    def test_h3_literals_are_escaped(self) -> None:
        self.client.query.return_value = [{"CHECKED": 1, "INVALID": 1}]
        with self.assertRaises(ValueError):
            self.validate("sf_hexagons", [{**self.cell, "H3_CELL": "bad'cell"}])
        self.assertIn("('bad''cell')", self.client.query.call_args.args[0])

    def test_empty_reference_results_are_not_passed(self) -> None:
        self.client.query.return_value = []
        config = install.load_config(ROOT / "config.example.toml")
        with patch("builtins.print") as output:
            install.verify_prompts(config, self.client)
        self.assertEqual(self.client.query.call_count, len(self.prompts))
        self.assertTrue(all("EMPTY" in call.args[0] for call in output.call_args_list))

    def test_verify_prompts_runs_all_contracts(self) -> None:
        self.client.query.side_effect = [
            [self.point], [self.cell], [{"CHECKED": 1, "INVALID": 0}],
            [self.cell], [{"CHECKED": 1, "INVALID": 0}], [self.polygon],
            [{**self.point, "DISTANCE_M": 100}], [self.polygon],
        ]
        config = install.load_config(ROOT / "config.example.toml")
        with patch("builtins.print") as output:
            install.verify_prompts(config, self.client)
        self.assertEqual(self.client.query.call_count, 8)
        self.assertEqual(len(output.call_args_list), 6)

    def test_receipt_replay_completes_all_prompt_checks_without_cli(self) -> None:
        config = install.load_config(ROOT / "config.example.toml")
        identity = {"ACCOUNT": "TEST123", "ROLE": config["role"],
                    "WAREHOUSE": config["warehouse"]}
        receipt = {"command": "verify-prompts", "marker": install.marker(config),
                   "identity": identity, "current_identity": deepcopy(identity), "history": []}
        results = [[self.point], [self.cell], [{"CHECKED": 1, "INVALID": 0}],
                   [self.cell], [{"CHECKED": 1, "INVALID": 0}], [self.polygon],
                   [{**self.point, "DISTANCE_M": 100}], [self.polygon]]
        with patch.object(install.subprocess, "run", side_effect=AssertionError("CLI forbidden")):
            for rows in results:
                step = agent_install.next_statement("verify-prompts", config, receipt)
                self.assertEqual(step["status"], "next")
                self.assertFalse(step["mutates"])
                receipt["history"].append({"sql": step["sql"], "ok": True, "rows": rows})
            result = agent_install.next_statement("verify-prompts", config, receipt)
        self.assertEqual(result["status"], "complete")
        self.assertEqual(result["statements"], 8)
        self.assertEqual(result["native_maps"], "not tested")
        self.assertEqual(result["agent_execution"], "not tested")
        self.assertEqual(result["output"].count("map contract passed"), 6)

    def test_receipt_replay_includes_h3_validation(self) -> None:
        config = install.load_config(ROOT / "config.example.toml")
        identity = {"ACCOUNT": "TEST123", "ROLE": config["role"],
                    "WAREHOUSE": config["warehouse"]}
        receipt = {"command": "verify-prompts", "marker": install.marker(config),
                   "identity": identity, "current_identity": deepcopy(identity), "history": []}
        first = agent_install.next_statement("verify-prompts", config, receipt)
        receipt["history"].append({"sql": first["sql"], "ok": True, "rows": [self.point]})
        second = agent_install.next_statement("verify-prompts", config, receipt)
        receipt["history"].append({"sql": second["sql"], "ok": True, "rows": [self.cell]})
        check = agent_install.next_statement("verify-prompts", config, receipt)
        self.assertIn("h3_is_valid_cell", check["sql"])
        self.assertFalse(check["mutates"])
        receipt["history"].append({"sql": check["sql"], "ok": True,
                                   "rows": [{"CHECKED": 1, "INVALID": 1}]})
        with self.assertRaisesRegex(ValueError, "H3 validity"):
            agent_install.next_statement("verify-prompts", config, receipt)


if __name__ == "__main__":
    unittest.main()