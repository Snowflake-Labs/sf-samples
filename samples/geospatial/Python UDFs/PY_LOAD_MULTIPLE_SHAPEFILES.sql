-- === Loading all shape files from .zip using SHAPE_MULTI_FILE ===
-- The GEO file load reader is built as a User Defined Table Function (UDTF)
-- and returns a result set from the the zip archive with shapefiles. Using a UDTF provides
-- the greatest flexibility for loading data, since the user can SELECT only the attributes they need
-- from the file.

-- === Requirements ===
--  - The file must be packaged as a standard zip file
--  - The zipped file must sit on share that the role has access to
--  - Role must have the ability to execute the function
--  - Depending on file and warehouse sizes the STATEMENT_TIMEOUT_IN_SECONDS will
--    need to be increased. Testing with one of the Ookla files on a medium warehouse,
--    required the timeout to be increased to over 10 minutes.

-- === Parameters ===
--  - PATH_TO_FILE - The scoped URL to the zipped file on the Snowflake share and is
--    generated using the build_scoped_file_url function. This function requires two
--    parameters:
--    - @stage_name - Snowflake stage of where the file is located
--    - ZIP_FILE_NAME - Name of the file, including path

-- === UDTF Code ===
CREATE OR REPLACE FUNCTION SHAPE_MULTI_FILE(PATH_TO_FILE string)
returns table (wkb binary, properties object)
language python
runtime_version = 3.8
packages = ('fiona', 'shapely', 'snowflake-snowpark-python','zipfile-deflate64')
handler = 'GeoFileReader'
AS $$
import io
from zipfile import ZipFile
from shapely.geometry import shape
from snowflake.snowpark.files import SnowflakeFile
from fiona.io import ZipMemoryFile
class GeoFileReader:    
    def _is_shapefile(self, filename: str) -> bool:
        return filename.endswith('.shp')
    def _read_zip_content(self, path_to_file: str) -> bytes:
        with SnowflakeFile.open(path_to_file, 'rb') as f:
            return f.read()
    def _extract_shapefiles_from_zip(self, zip_content: bytes) -> list:
        with ZipFile(io.BytesIO(zip_content), 'r') as shp_zip:
            return [name for name in shp_zip.namelist() if self._is_shapefile(name)]
    def process(self, PATH_TO_FILE: str):
        zip_content = self._read_zip_content(PATH_TO_FILE)
        shapefiles = self._extract_shapefiles_from_zip(zip_content)       
        for shp in shapefiles:
            with ZipMemoryFile(zip_content) as zip:
                with zip.open(shp) as collection:
                    for record in collection:
                        if record['geometry']:
                            yield shape(record['geometry']).wkb, dict(record['properties'])
$$;

-- === Example execution (ESRI ShapeFile) ===
create or replace table GEOLAB.GEOGRAPHY.TABLE_NAME as
SELECT properties:Field_1::string as field_1,
properties:Field_2::string as Field_2,
to_geography(wkb, True) as geometry FROM table(shape_multi_file(build_scoped_file_url(@stage_name, 'ZIP_FILE_NAME.zip')));
