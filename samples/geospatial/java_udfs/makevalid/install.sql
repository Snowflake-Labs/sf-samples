-- =============================================================================
-- makevalid-geography 1.0.0 - Snowflake Java UDF installation script
-- =============================================================================
--
-- Prerequisite: you have run (from the directory containing this file):
--   snow sql -q "PUT file://makevalid-java-1.0.0.jar @<your_stage> AUTO_COMPRESS=FALSE OVERWRITE=TRUE"
-- OR the equivalent Snowsight "Upload to stage" action.
--
-- Then edit the two placeholders below (<DB>.<SCHEMA>, <STAGE>) and run the
-- whole file once as a role with CREATE FUNCTION privileges.
-- =============================================================================

-- 1. Pick / create target database and schema ---------------------------------
CREATE DATABASE IF NOT EXISTS <DB>;
CREATE SCHEMA   IF NOT EXISTS <DB>.<SCHEMA>;

-- 2. Pick / create stage for the JAR ------------------------------------------
CREATE STAGE    IF NOT EXISTS <DB>.<SCHEMA>.<STAGE>;

-- 3. Upload the JAR (run from SnowSQL / snow CLI on your laptop) --------------
-- PUT file://makevalid-java-1.0.0.jar @<DB>.<SCHEMA>.<STAGE>
--     AUTO_COMPRESS = FALSE OVERWRITE = TRUE;

-- 4. Register the UDFs --------------------------------------------------------

-- Lenient variant: returns NULL on any unrecoverable error.
CREATE OR REPLACE FUNCTION <DB>.<SCHEMA>.MAKEVALID(geom GEOGRAPHY)
RETURNS GEOGRAPHY
LANGUAGE JAVA
RUNTIME_VERSION = '11'
HANDLER = 'com.snowflake.geo.MakeValid.makeValidGeography'
IMPORTS = ('@<DB>.<SCHEMA>.<STAGE>/makevalid-java-1.0.0.jar')
COMMENT = 'Repair invalid GEOGRAPHY polygons via gnomonic projection + JTS GeometryFixer. Default 1mm grid snap.';

CREATE OR REPLACE FUNCTION <DB>.<SCHEMA>.MAKEVALID(geom GEOGRAPHY, grid_meters DOUBLE)
RETURNS GEOGRAPHY
LANGUAGE JAVA
RUNTIME_VERSION = '11'
HANDLER = 'com.snowflake.geo.MakeValid.makeValidGeography'
IMPORTS = ('@<DB>.<SCHEMA>.<STAGE>/makevalid-java-1.0.0.jar')
COMMENT = 'Repair invalid GEOGRAPHY polygons. grid_meters controls post-fix snap: 0 disables, smaller preserves detail, larger snaps harder.';

-- Strict variant: propagates Java exceptions (useful for debugging bad rows).
CREATE OR REPLACE FUNCTION <DB>.<SCHEMA>.MAKEVALID_STRICT(geom GEOGRAPHY)
RETURNS GEOGRAPHY
LANGUAGE JAVA
RUNTIME_VERSION = '11'
HANDLER = 'com.snowflake.geo.MakeValid.makeValidGeographyStrict'
IMPORTS = ('@<DB>.<SCHEMA>.<STAGE>/makevalid-java-1.0.0.jar')
COMMENT = 'Same as MAKEVALID but raises an exception on failure instead of returning NULL.';

CREATE OR REPLACE FUNCTION <DB>.<SCHEMA>.MAKEVALID_STRICT(geom GEOGRAPHY, grid_meters DOUBLE)
RETURNS GEOGRAPHY
LANGUAGE JAVA
RUNTIME_VERSION = '11'
HANDLER = 'com.snowflake.geo.MakeValid.makeValidGeographyStrict'
IMPORTS = ('@<DB>.<SCHEMA>.<STAGE>/makevalid-java-1.0.0.jar')
COMMENT = 'Same as MAKEVALID(geom, grid_meters) but raises an exception on failure instead of returning NULL.';

-- 5. Smoke test ---------------------------------------------------------------
SELECT ST_ASWKT(<DB>.<SCHEMA>.MAKEVALID(
    TO_GEOGRAPHY('POLYGON((0 0, 1 1, 1 0, 0 1, 0 0))', TRUE)
)) AS bowtie_repaired;
