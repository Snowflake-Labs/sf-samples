# makevalid-geography

A standalone Snowflake Java UDF that repairs invalid `GEOGRAPHY` polygons using
the [JTS Topology Suite](https://github.com/locationtech/jts) `GeometryFixer`
applied in a gnomonic projection centred on the input's bounding box.

Unlike `TO_GEOGRAPHY(..., TRUE)` (which only tolerates invalid shapes at parse
time), this UDF actually produces a new, valid `GEOGRAPHY` that Snowflake's
spatial functions (`ST_AREA`, `ST_INTERSECTS`, `ST_BUFFER`, joins, etc.) can
operate on reliably.

## Package contents

| File                           | Purpose                                            |
|--------------------------------|----------------------------------------------------|
| `makevalid-java-1.0.0.jar`     | Shaded fat JAR (JTS + GeographicLib + handler).    |
| `install.sql`                  | One-shot SQL to stage the JAR and create the UDFs. |
| `src/` + `pom.xml`             | Source (Apache 2.0) + Maven build for rebuilds.    |
| `README.md`                    | This file.                                         |

## Prerequisites

- A role with `CREATE DATABASE` / `CREATE SCHEMA` / `CREATE STAGE` /
  `CREATE FUNCTION` privileges (ACCOUNTADMIN or equivalent).
- Snowflake account with Java UDFs enabled (default on all accounts).
- SnowSQL or the [`snow` CLI](https://docs.snowflake.com/en/developer-guide/snowflake-cli-v2/index)
  for uploading the JAR.

## Installation (3 steps)

### 1. Upload the JAR to a Snowflake stage

From the directory that contains `makevalid-java-1.0.0.jar`:

```bash
# Using the `snow` CLI
snow sql -q "CREATE DATABASE IF NOT EXISTS MY_DB;
             CREATE SCHEMA IF NOT EXISTS MY_DB.GEO;
             CREATE STAGE IF NOT EXISTS MY_DB.GEO.UDFS;"

snow sql -q "PUT file://makevalid-java-1.0.0.jar @MY_DB.GEO.UDFS
             AUTO_COMPRESS=FALSE OVERWRITE=TRUE"
```

Or in Snowsight: Data → Databases → MY_DB → GEO → Stages → UDFS → "Upload files".

### 2. Edit placeholders in `install.sql`

Replace `<DB>`, `<SCHEMA>`, `<STAGE>` with your choices (e.g. `MY_DB`, `GEO`, `UDFS`).

### 3. Run `install.sql`

```bash
snow sql -f install.sql
```

Or paste the file into a Snowsight worksheet. The final `SELECT` at the bottom
is a smoke test that should return a valid repaired polygon.

## Functions

| Signature                                           | Description                                                 |
|----------------------------------------------------|-------------------------------------------------------------|
| `MAKEVALID(geom GEOGRAPHY) -> GEOGRAPHY`            | Repair with default 1 mm gnomonic-plane grid snap.          |
| `MAKEVALID(geom GEOGRAPHY, grid_meters DOUBLE)`     | Repair with custom grid. `0` disables snapping.             |
| `MAKEVALID_STRICT(geom GEOGRAPHY[, grid_meters])`   | Same, but raises an exception instead of returning NULL.    |

### Choosing `grid_meters`

The UDF projects your polygon onto a gnomonic tangent plane, runs
`GeometryFixer`, snaps coordinates to a grid to remove floating-point noise,
then reverse-projects. `grid_meters` is the snap grid size in that metre-scale
plane.

| Use case                                         | Suggested `grid_meters` |
|--------------------------------------------------|-------------------------|
| Default / real-world parcels with jitter          | `0.001` (1 mm) — default|
| Very small synthetic shapes, preserve detail     | `1e-6`                  |
| Aggressively clean messy digitised data          | `0.01` to `1.0`         |
| Never snap (may still be spherically invalid)    | `0`                     |

## Usage examples

```sql
-- Repair a self-intersecting "bowtie" polygon
SELECT ST_ASWKT(MY_DB.GEO.MAKEVALID(
    TO_GEOGRAPHY('POLYGON((0 0, 1 1, 1 0, 0 1, 0 0))', TRUE)
));
-- -> MULTIPOLYGON(((...),(...))) with two triangles

-- Batch-repair a table of potentially invalid geographies
CREATE OR REPLACE TABLE parcels_fixed AS
SELECT id,
       MY_DB.GEO.MAKEVALID(geom) AS geom
FROM   parcels_raw;

-- Now downstream spatial functions work
SELECT id, ST_AREA(geom) AS area_m2
FROM   parcels_fixed
WHERE  geom IS NOT NULL;

-- Aggressive snap for highly jittered field data
SELECT MY_DB.GEO.MAKEVALID(geom, 0.01) FROM messy_shapes;

-- Debug a specific bad row by using the strict variant
SELECT MY_DB.GEO.MAKEVALID_STRICT(geom) FROM parcels_raw WHERE id = 42;
```

### Repair rows in place

Update only the invalid rows of an existing table, filtering with `ST_ISVALID`:

```sql
-- Fix only the invalid rows, in place
UPDATE my_schema.parcels
SET    geom = MY_DB.GEO.MAKEVALID(geom)
WHERE  NOT ST_ISVALID(geom);

-- Verify no invalid shapes remain
SELECT COUNT_IF(NOT ST_ISVALID(geom)) AS still_invalid,
       COUNT_IF(geom IS NULL)         AS null_count
FROM   my_schema.parcels;
```

Notes:

- `NOT ST_ISVALID(geom)` is equivalent to `ST_ISVALID(geom) = FALSE`.
- The lenient `MAKEVALID` returns `NULL` on unrecoverable shapes; swap in
  `MAKEVALID_STRICT` to surface the underlying exception while debugging,
  then switch back to the lenient variant for bulk runs.

## How it works

```
input GEOGRAPHY
      |
      v
parse GeoJSON (Snowflake default marshalling)
      |
      v
dim < 2 ? --yes--> return as-is (points, lines never invalid)
      |
      no
      v
DouglasPeucker pre-simplify (tol = 1e-9 deg, removes duplicate vertices)
      |
      v
envelope wider than 180 deg ? --yes--> shift eastern vertices by -360 (antimeridian)
      |
      v
envelope still > 90 deg ? --yes--> planar GeometryFixer.fix fallback
      |
      no
      v
gnomonic Forward (centred on envelope centre)
      |
      v
GeometryFixer.fix (planar repair)
      |
      v
GeometryPrecisionReducer (grid_meters snap)
      |
      v
gnomonic Reverse
      |
      v
GeometryFixer.fix (second pass, catches reverse-projection jitter)
      |
      v
GeoJsonWriter -> GEOGRAPHY
```

## Limitations

- The input envelope should be `<= 90 deg` in width and height. Larger inputs
  fall back to planar fixing (less accurate but still valid).
- `GEOMETRY` type is not supported (UDF is geography-specific by design).
- Requires Snowflake Java runtime 11.

## Rebuilding from source

```bash
# requires JDK 11+ and Maven
mvn clean package
# new jar at target/makevalid-java-1.0.0.jar
```

## License

Apache License 2.0 — see header in `src/main/java/com/snowflake/geo/MakeValid.java`.

Core projection-based repair algorithm adapted from
[Snowflake-Labs/jts-udf](https://github.com/Snowflake-Labs/jts-udf).
