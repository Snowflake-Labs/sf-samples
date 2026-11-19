"""Offline checks for the DCM project in dcm/.

Rendering needs jinja2 (pip install jinja2); without it these tests are skipped.
DCM makes macros in sources/macros/ available to every definition file; the
renderer below does the same by prepending them.
"""

from __future__ import annotations

from copy import deepcopy
import importlib.util
import json
from pathlib import Path
import re
import sys
import unittest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))
SPEC = importlib.util.spec_from_file_location("install", ROOT / "scripts/install.py")
install = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(install)
import build_dcm  # noqa: E402

try:
    import jinja2
except ImportError:  # pragma: no cover - exercised only without the test dependency
    jinja2 = None

MODEL_FILES = ("sources.sql", "themes.sql", "semantic.sql")
FIXTURES = ROOT / "tests/fixtures/legacy_render"
MARKER = re.compile(r"overture-maps-semantic-layer:[0-9][0-9.]*[0-9](:[0-9a-f]+)?")


def render_definitions(variables: dict, names=None) -> dict[str, str]:
    macros = "".join(path.read_text() for path in sorted((ROOT / "dcm/sources/macros").glob("*.sql")))
    environment = jinja2.Environment(undefined=jinja2.StrictUndefined)
    rendered = {}
    for path in sorted((ROOT / "dcm/sources/definitions").glob("*.sql")):
        if names is None or path.name in names:
            rendered[path.name] = environment.from_string(macros + path.read_text()).render(**variables)
    return rendered


def statements(text: str) -> list[str]:
    """Split on statement-ending semicolons; the sample's SQL has none inside strings."""
    text = re.sub(r"(?im)^DEFINE (SEMANTIC VIEW|VIEW)\b",
                  lambda m: "create or replace " + m[1].lower(), text)
    return [MARKER.sub("MARKER", part.strip().rstrip(";"))
            for part in re.split(r";\s*\n", text) if part.strip()]


@unittest.skipUnless(jinja2, "jinja2 is required to render dcm/ (pip install jinja2)")
class DcmProjectTests(unittest.TestCase):
    def setUp(self) -> None:
        self.config = install.load_config(ROOT / "config.example.toml")
        self.dataset = {"RELEASE": "unknown", "COVERAGE": "Global"}

    def variant(self, name: str) -> dict:
        config = deepcopy(self.config)
        if name in {"existing_all_themes"}:
            config["source"]["optional_themes"] = ["address", "segment", "building"]
        if name == "s3_building":
            config["source"]["mode"] = "s3"
            config["source"]["optional_themes"] = ["building"]
        variables = install.dcm_variables(config, self.dataset)
        if name == "s3_building":
            # The fixture predates the raw schema: S3 tables then lived in the model schema.
            for theme in install.active_themes(config):
                variables[f"source_{theme}"] = f"MY_DATABASE.OVERTURE_SEMANTIC.RAW_{theme.upper()}"
        return variables

    def test_model_matches_pre_dcm_renderer(self) -> None:
        for name in ("existing_core", "existing_all_themes", "s3_building"):
            with self.subTest(variant=name):
                rendered = render_definitions(self.variant(name), MODEL_FILES)
                text = "\n".join(rendered[file] for file in MODEL_FILES)
                self.assertEqual(statements(text), statements((FIXTURES / f"{name}.sql").read_text()))

    def semantic(self, config) -> str:
        return render_definitions(install.dcm_variables(config, self.dataset),
                                  ["semantic.sql"])["semantic.sql"]

    def test_optional_themes_render_into_one_semantic_view(self) -> None:
        base = self.semantic(self.config)
        self.assertNotIn("BBOX", base)
        self.assertNotIn("tallest_buildings", base)
        config = deepcopy(self.config)
        config["source"]["optional_themes"] = list(install.OPTIONAL_THEMES)
        sql = self.semantic(config)
        for expected in ("addresses as", "segments as", "buildings as", "buildings.bbox_xmin",
                         "segments.speed_limit_kmh", "tallest_buildings as",
                         "must filter bbox_xmin", "Tables ADDRESS, SEGMENT, BUILDING are global"):
            self.assertIn(expected, sql)
        self.assertEqual(sql.count("DEFINE SEMANTIC VIEW"), 1)
        self.assertIn('"OVERTURE_SEMANTIC".BUILDING primary key', sql)
        self.assertNotIn("{", sql)

    def test_semantic_view_independent_of_sources(self) -> None:
        updated = deepcopy(self.config)
        updated["source"]["place"] = "MY_LOADED_DATA.OVERTURE.PLACE"
        self.assertEqual(MARKER.sub("M", self.semantic(self.config)), MARKER.sub("M", self.semantic(updated)))
        views = render_definitions(install.dcm_variables(updated, self.dataset), ["sources.sql"])
        self.assertIn('from "MY_LOADED_DATA"."OVERTURE"."PLACE"', views["sources.sql"])

    def test_geometry_grain_and_no_spatial_relationship(self) -> None:
        sql = self.semantic(self.config)
        self.assertIn("areas.area_id as ID", sql)
        self.assertIn("places.place_id as ID", sql)
        self.assertIn("st_union_agg", sql)
        self.assertIn("area_to_division", sql)
        self.assertNotIn("places(GEOM)", sql)

    def test_definitions_hold_only_dcm_statements(self) -> None:
        config = deepcopy(self.config)
        config["source"]["optional_themes"] = list(install.OPTIONAL_THEMES)
        for name, text in render_definitions(install.dcm_variables(config, self.dataset)).items():
            for statement in re.split(r";\s*\n", text):
                if statement.strip():
                    self.assertRegex(statement.strip(), r"^(DEFINE|GRANT) ", name)

    def test_verified_queries_macro_is_current(self) -> None:
        self.assertEqual((ROOT / "dcm/sources/macros/verified_queries.sql").read_text(),
                         build_dcm.render(), "Run scripts/build_dcm.py after editing prompts.json")

    def test_every_installed_prompt_is_a_verified_query(self) -> None:
        for themes in ([], ["address"], ["address", "segment", "building"]):
            config = deepcopy(self.config)
            config["source"]["optional_themes"] = themes
            semantic = render_definitions(install.dcm_variables(config, self.dataset),
                                          ["semantic.sql"])["semantic.sql"]
            expected = {prompt["id"] for prompt in install.available_prompts(config)}
            found = set(re.findall(r"^(\w+) as \(question ", semantic, re.MULTILINE))
            self.assertEqual(found, expected)

    def test_optional_theme_removal_drops_only_its_definitions(self) -> None:
        config = deepcopy(self.config)
        config["source"]["optional_themes"] = ["address", "building"]
        both = render_definitions(install.dcm_variables(config, self.dataset))
        config["source"]["optional_themes"] = ["building"]
        one = render_definitions(install.dcm_variables(config, self.dataset))
        self.assertIn(".ADDRESS comment", both["themes.sql"])
        self.assertNotIn("ADDRESS", one["themes.sql"])
        self.assertNotIn(".ADDRESS ", one["access.sql"])
        self.assertIn(".BUILDING comment", one["themes.sql"])

    def test_grants_cover_managed_objects_only(self) -> None:
        access = render_definitions(install.dcm_variables(self.config, self.dataset),
                                    ["access.sql"])["access.sql"]
        self.assertIn('GRANT SELECT ON SEMANTIC VIEW "MY_DATABASE"."OVERTURE_SEMANTIC".OVERTURE_MAPS_SV '
                      'TO ROLE "MY_VIEWER_ROLE"', access)
        for shared in ("ON DATABASE", "ON WAREHOUSE", "ON AGENT", "CORTEX SEARCH", "SNOWFLAKE INTELLIGENCE"):
            self.assertNotIn(shared, access)
        config = deepcopy(self.config)
        config["viewer_role"] = config["role"]
        self.assertNotIn("GRANT", render_definitions(install.dcm_variables(config, self.dataset),
                                                     ["access.sql"])["access.sql"])

    def test_literals_escape_like_the_installer(self) -> None:
        dataset = {"RELEASE": "r", "COVERAGE": "O'Brien \\ coverage"}
        info = render_definitions(install.dcm_variables(self.config, dataset),
                                  ["dataset_info.sql"])["dataset_info.sql"]
        self.assertIn(install.literal("O'Brien \\ coverage"), info)

    def test_generated_manifest_is_json_with_every_variable(self) -> None:
        directory = install.write_dcm_project(self.config, self.dataset)
        manifest = json.loads((directory / "manifest.yml").read_text())
        target = manifest["targets"][manifest["default_target"]]
        self.assertEqual(target["project_name"],
                         "MY_DATABASE.OVERTURE_SEMANTIC_DCM.OVERTURE_MAPS_SEMANTIC_LAYER")
        variables = manifest["templating"]["defaults"]
        render_definitions(variables)  # StrictUndefined fails on any missing variable
        committed = (ROOT / "dcm/manifest.yml").read_text()
        for key in variables:
            self.assertIn(f"    {key}:", committed, f"dcm/manifest.yml lacks a default for {key}")


if __name__ == "__main__":
    unittest.main()
