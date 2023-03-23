-- 1. Upload the zip archive with Shapefiles files to pre-created stage
put file:///Users/<LOCAL_PATH>/<FILENAME>.zip @geostage AUTO_COMPRESS = FALSE OVERWRITE = TRUE

-- 2. Create a function to read Shapefiles from the stage and return it as a table
CREATE OR REPLACE FUNCTION PY_LOAD_SHAPEFILE(PATH_TO_FILE string)
returns table (wkt binary, properties object)
language python
runtime_version = 3.8
imports=('@geostage/archive.zip')
packages = ('fiona', 'shapely')
handler = 'ShapeFileReader'
AS $$
import fiona
from shapely.geometry import shape
import sys
IMPORT_DIRECTORY_NAME = "snowflake_import_directory"
import_dir = sys._xoptions[IMPORT_DIRECTORY_NAME]

class ShapeFileReader:        
    def process(self, PATH_TO_FILE: str):
      shapefile = fiona.open(f"zip://{import_dir}/archive.zip/{PATH_TO_FILE}")
      for record in shapefile:
        yield ((shape(record['geometry']).wkt, dict(record['properties'])))
$$;

-- 3. Crate a table from the staged file
SELECT * FROM table(PY_LOAD_SHAPEFILE('<PATH_TO_FILE>'));