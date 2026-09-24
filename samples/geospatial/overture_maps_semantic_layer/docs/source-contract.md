# Source contract

The adapters accept four Overture tables or views with uppercase column names.
All sources must expose WGS84 `GEOGRAPHY` in `GEOMETRY`, not planar `GEOMETRY`, WKB,
or GeoJSON. Each ID must be non-null and unique within its table. The customer
must verify these guarantees for existing sources; preflight checks metadata and
a sample, not every row.

Required columns:

- `PLACE`: ID, GEOMETRY, NAMES, CATEGORIES, ADDRESSES.
- `DIVISION`: ID, GEOMETRY, NAMES, COUNTRY, REGION, SUBTYPE, POPULATION.
- `DIVISION_AREA`: ID, DIVISION_ID, GEOMETRY, NAMES, COUNTRY, REGION, SUBTYPE, CLASS.
- `DIVISION_BOUNDARY`: ID, GEOMETRY, COUNTRY, REGION, SUBTYPE, CLASS, IS_LAND.

NAMES and CATEGORIES are objects (VARIANT or OBJECT) with a `primary` field.
ADDRESSES is an array (VARIANT or ARRAY); the adapter uses the first address's
`country`, `region`, and `locality`. Null values remain null. Parquet wrappers
such as `list[].element` are not supported: expose a compatible source view first.
The S3 loader reads raw fields directly and does not introduce those wrappers.

ID and location labels are strings; POPULATION is numeric; IS_LAND is boolean.
PLACE and DIVISION geometry must be points. DIVISION_AREA must contain polygons;
DIVISION_BOUNDARY must contain lines. Preflight checks geometry validity and
dimension in the first 100 rows. This does not establish full-table geometry type
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

All four themes are required for v1. Explicit mappings support renamed databases
and existing copies without automatic discovery. A missing share never triggers
an automatic load. The destination schema must not contain existing sources.