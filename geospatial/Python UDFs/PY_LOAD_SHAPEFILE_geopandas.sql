-- 1. Upload the zip archive with Shapefiles files to pre-created stage
put file:///Users/<LOCAL_PATH>/<FILENAME>.zip @geostage AUTO_COMPRESS = FALSE OVERWRITE = TRUE

-- 2. Create a function to read Shapefiles from the stage and return it as a table
CREATE OR REPLACE FUNCTION  PY_LOAD_SHAPEFILE(PATH_TO_FILE string)
returns table (osm_id string, lastchange string, code integer, fclass string, 
               geomtype string, postalcode string, name string, geometry string)
language python
runtime_version=3.8
packages = ('geopandas')
imports=('@geostage/archive.zip')
handler='ReadShapefile'
as $$
import sys
import geopandas as gpd
import_dir = sys._xoptions["snowflake_import_directory"]

class ReadShapefile:
    def process(self, PATH_TO_FILE: str):
        gdf = gpd.read_file(f"zip://{import_dir}/archive.zip/{PATH_TO_FILE}")
        return tuple(gdf.itertuples(index=False, name=None))
$$;

-- 3. Crate a table from the staged file
SELECT * FROM table(PY_LOAD_SHAPEFILE('<PATH_TO_FILE>'));