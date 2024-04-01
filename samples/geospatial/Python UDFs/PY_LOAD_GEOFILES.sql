-- === Loading shape files using PY_LOAD_GEOEFILE ===
-- The GEO file load reader is built as a User Defined Table Function (UDTF)
-- and returns a result set from the several external GEO data file types. Using a UDTF provides
-- the greatest flexibility for loading data, since the user can SELECT only the attributes they need
-- from the file.

-- === Requirements ===
--  - The file must be packaged as a standard zip file, where the set of shape files are zipped
--    together at a single level, not the zipped up folder
--  - The zipped file must sit on share that the role has access to
--  - Role must have the ability to execute the function
--  - Depending on file and warehouse sizes the STATEMENT_TIMEOUT_IN_SECONDS will
--    need to be increased. Testing with one of the Ookla files on a medium warehouse,
--    required the timeout to be increased to over 10 minutes.

-- === Parameters ===
--  - ZIP_FILE_NAME - The scoped URL to the zipped file on the Snowflake share and is
--    generated using the build_scoped_file_url function. This function requires two
--    parameters:
--    - Snowflake share of where the file is located
--    - Name of the file, including path
--  - GEOFILE_NAME - The name of the GEO file inside the package zip that needs to be loaded

-- === UDTF Code ===
CREATE OR REPLACE FUNCTION PY_LOAD_GEOFILE(PATH_TO_FILE string, filename string)
returns table (wkb binary, properties object)
language python
runtime_version = 3.8
packages = ('fiona', 'shapely', 'snowflake-snowpark-python')
handler = 'GeoFileReader'
AS $$
from shapely.geometry import shape
from snowflake.snowpark.files import SnowflakeFile
from fiona.io import ZipMemoryFile
import fiona
class GeoFileReader:
    def process(self, PATH_TO_FILE: str, filename: str):
        fiona.drvsupport.supported_drivers['libkml'] = 'rw'
        fiona.drvsupport.supported_drivers['LIBKML'] = 'rw'
        with SnowflakeFile.open(PATH_TO_FILE, 'rb') as f:
            with ZipMemoryFile(f) as zip:
                with zip.open(filename) as collection:
                    for record in collection:
                        if (not (record['geometry'] is None)):
                            yield ((shape(record['geometry']).wkb, dict(record['properties'])))
$$;

-- === Example execution (ESRI ShapeFile) ===
create or replace table GEOLAB.GEOGRAPHY.TABLE_NAME as
SELECT properties:Field_1::string as field_1,
properties:Field_2::string as Field_2,
to_geography(wkb, True) as geometry FROM table(PY_LOAD_GEOFILE(build_scoped_file_url(@stage_name, 'ZIP_FILE_NAME.zip'), 'GEOFILE_NAME.shp'));

-- === Example execution (MapInfo TAB File) ===
create or replace table GEOLAB.GEOGRAPHY.TABLE_NAME as
SELECT properties:Field_1::string as field_1,
properties:Field_2::string as Field_2,
to_geography(wkb, True) as geometry FROM table(PY_LOAD_GEOFILE(build_scoped_file_url(@stage_name, 'ZIP_FILE_NAME.zip'), 'GEOFILE_NAME.tab'));

-- === Example execution (Google Earth KML File) ===
select
to_geography(wkb, True) as geometry,
properties:Name::string as Name,
properties:altitudeMode::string as altitudeMode
from table(PY_LOAD_GEOFILE(build_scoped_file_url(@tmobile, 'ZIP_FILE_NAME.zip'), 'GEOFILE_NAME.kml'));

-- === Example execution (OGC GeoPackage) ===
-- Note: A specific layer in the .gpkg file is opened by specifying layer name in UDTF `with zip.open(filename,layer=layername) as collection:`
SELECT 
    properties:Field_1::string as field_1,
    properties:Field_2::string as Field_2,
    to_geography(wkb, True) as geometry 
FROM table(PY_LOAD_GEOFILE(build_scoped_file_url(@stage_name, 'ZIP_FILE_NAME.zip'), 'GEOFILE_NAME.gpkg'));
