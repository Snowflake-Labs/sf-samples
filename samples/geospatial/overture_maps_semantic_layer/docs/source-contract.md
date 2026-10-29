# Source contract

The adapters accept four Overture tables or views with uppercase column names.
All sources must expose WGS84 `GEOGRAPHY` in `GEOMETRY`, not planar `GEOMETRY`, WKB,
or GeoJSON. Each ID must be non-null and unique within its table. The customer
must verify these guarantees for existing sources; preflight checks metadata and
a sample, not every row.

Required columns:

- `PLACE`: ID, GEOMETRY, NAMES, TAXONOMY, BASIC_CATEGORY, ADDRESSES.
- `DIVISION`: ID, GEOMETRY, NAMES, COUNTRY, REGION, SUBTYPE, POPULATION.
- `DIVISION_AREA`: ID, DIVISION_ID, GEOMETRY, NAMES, COUNTRY, REGION, SUBTYPE, CLASS.
- `DIVISION_BOUNDARY`: ID, GEOMETRY, COUNTRY, REGION, SUBTYPE, CLASS, IS_LAND.

NAMES and TAXONOMY are objects (VARIANT or OBJECT) with a `primary` field.
Overture replaced the older CATEGORIES column with TAXONOMY and BASIC_CATEGORY,
and the CARTO Marketplace share no longer carries CATEGORIES (checked on
2026-10-25). The place view uses `taxonomy:primary` and falls back to
BASIC_CATEGORY; sources that only have CATEGORIES fail preflight and need a
compatible view first.
ADDRESSES is an array (VARIANT or ARRAY); the adapter uses the first address's
`country`, `region`, and `locality`. Null values remain null. Parquet wrappers
such as `list[].element` are not supported: expose a compatible source view first.
The S3 loader reads raw fields directly and does not introduce those wrappers.

ID and location labels are strings; POPULATION is numeric; IS_LAND is boolean.
PLACE and DIVISION geometry must be points. DIVISION_AREA must contain polygons;
DIVISION_BOUNDARY must contain lines. Preflight checks for null geometry and the
expected dimension in the first 100 rows. It does not reject geometries that
fail ST_ISVALID: Overture ships a few self-intersecting division polygons and the
Marketplace share keeps them, so the S3 loader converts with
`TRY_TO_GEOGRAPHY(..., TRUE)` to keep them too. Corrupt WKB still becomes NULL
and fails the load. Strict conversion silently dropped 7 of 1,084,259 division
areas. None of this establishes full-table geometry type
or nested-field coverage. MultiPoint values must be normalized to single points
before using the place adapter's ST_X/ST_Y expressions.

`DIVISION_AREA.DIVISION_ID` refers to `DIVISION.ID`. The semantic model declares
this equality relationship; sources must uphold its uniqueness and integrity.
There is no relationship between PLACE and DIVISION_AREA.

Normalized views expose scalar labels and retain native geometry as GEOM.
Places keep bare region codes such as CA; divisions/areas keep ISO codes such as
US-CA. The model explains the difference rather than silently changing source
semantics. Land and maritime areas stay separate and retain their own IDs.

S3 staging tables contain only the columns this sample needs. They are not a
complete replacement for all columns in a Marketplace share.

The four places and divisions themes are required. The agent-driven workflow can
discover candidate sources, then validates explicit mappings for renamed
databases and existing copies. The manual CLI requires those mappings in
configuration. A missing share never triggers an automatic load, and an unknown
Marketplace listing readiness is an error rather than a reason to load from S3.
The destination schema must not contain existing sources.

## Optional themes

List any of `address`, `segment` and `building` in `source.optional_themes` to
add them. Each needs these columns:

- `ADDRESS`: ID, GEOMETRY (points), STREET, NUMBER, POSTCODE, POSTAL_CITY,
  ADDRESS_LEVELS, COUNTRY, BBOX.
- `SEGMENT`: ID, GEOMETRY (lines), NAMES, SUBTYPE, CLASS, SUBCLASS, SPEED_LIMITS,
  BBOX.
- `BUILDING`: ID, GEOMETRY (polygons), NAMES, SUBTYPE, CLASS, HEIGHT, NUM_FLOORS,
  BBOX.

BBOX is the Overture object with `xmin`, `xmax`, `ymin` and `ymax`. The views
expose it as four float columns because the shares prune on them and do not
prune on geometry predicates. The segment view keeps only the first
unconditional speed limit, converted to km/h; conditional limits become NULL.
Segments are mapped reference geometry, not a routing network. In S3 mode these
themes require `place_bbox` and are loaded clipped to it, because global copies
run from hundreds of millions to billions of rows.