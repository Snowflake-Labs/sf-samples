CREATE OR REPLACE FUNCTION PY_LOAD_ZARRFILE(
    PATH_TO_FILE STRING, mydata STRING
)
RETURNS TABLE (
    dim_index string,
    value string
)
LANGUAGE PYTHON
RUNTIME_VERSION = 3.8
PACKAGES = (
    'zarr',
    'numpy',
    'snowflake-snowpark-python'
)
HANDLER = 'ZarrFileReader'
AS
$$
import zarr
import numpy as np
import io
import os
from snowflake.snowpark.files import SnowflakeFile

class ZarrFileReader:
    def process(self, PATH_TO_FILE: str, mydata: str):
        # 1. Read entire ZIP from Snowflake stage into memory
        with SnowflakeFile.open(PATH_TO_FILE, 'rb') as f:
            zip_data = f.read()  # bytes of the entire Zarr ZIP

        # 2. Write bytes to a temporary file in the Snowflake Python sandbox
        temp_path = "/tmp/zarr_temp.zip"
        with open(temp_path, "wb") as tmp:
            tmp.write(zip_data)

        # 3. Now open the Zarr store from that local path
        store = zarr.ZipStore(temp_path, mode='r')
        root = zarr.open(store, mode='r')

        # 4. For example, assume an array named mydata is inside
        if mydata not in root.array_keys():
            return

        arr = root[mydata]

        # 5. Yield rows (dim_index, value)
        it = np.ndenumerate(arr[:])  # watch out for very large data
        for idx, val in it:
            yield (list(idx), float(val))
$$;

-------How to test it

--- Prepare test database and test stage (extenal which contains a test package, and internal)
CREATE OR REPLACE DATABASE zarr_test;
CREATE OR REPLACE STAGE zarr_test.public.stage_external URL='s3://large-objects-testing';
CREATE OR REPLACE STAGE zarr_test.public.stage_internal;

--- Createing a UDTF for reading ZARR files
CREATE OR REPLACE FUNCTION ZARR_TEST.PUBLIC.PY_LOAD_ZARRFILE(
    PATH_TO_FILE STRING, mydata STRING
)
RETURNS TABLE (
    dim_index string,
    value string
)
LANGUAGE PYTHON
RUNTIME_VERSION = 3.8
PACKAGES = (
    'zarr',
    'numpy',
    'snowflake-snowpark-python'
)
HANDLER = 'ZarrFileReader'
AS
$$
import zarr
import numpy as np
import io
import os
from snowflake.snowpark.files import SnowflakeFile

class ZarrFileReader:
    def process(self, PATH_TO_FILE: str, mydata: str):
        # 1. Read entire ZIP from Snowflake stage into memory
        with SnowflakeFile.open(PATH_TO_FILE, 'rb') as f:
            zip_data = f.read()  # bytes of the entire Zarr ZIP

        # 2. Write bytes to a temporary file in the Snowflake Python sandbox
        temp_path = "/tmp/zarr_temp.zip"
        with open(temp_path, "wb") as tmp:
            tmp.write(zip_data)

        # 3. Now open the Zarr store from that local path
        store = zarr.ZipStore(temp_path, mode='r')
        root = zarr.open(store, mode='r')

        # 4. For example, assume an array named mydata is inside
        if mydata not in root.array_keys():
            return

        arr = root[mydata]

        # 5. Yield rows (dim_index, value)
        it = np.ndenumerate(arr[:])  # watch out for very large data
        for idx, val in it:
            yield (list(idx), float(val))
$$;

-- Creating a function to copy files from external stage to internal stage
create or replace procedure ZARR_TEST.PUBLIC.COPY_BIN(file_url string, dest_stage string, dest_filename string)
returns variant
language python
runtime_version=3.8
packages = ('snowflake-snowpark-python')
handler = 'x'
execute as caller
AS
$$
from snowflake.snowpark.files import SnowflakeFile
import io
def x(session, file_url, dest_stage, dest_filename):
    file = io.BytesIO(SnowflakeFile.open(file_url, 'rb').read())
    session._conn.upload_stream(file, dest_stage, dest_filename, compress_data=False)
    return 'file copied to stage [%s] with name [%s]' % (dest_stage, dest_filename)
$$;

-- Copying a test package from external stage to the intenal one
call COPY_BIN(build_scoped_file_url(@ZARR_TEST.PUBLIC.STAGE_EXTERNAL, 'pre_maldi.zip'), '@ZARR_TEST.PUBLIC.STAGE_INTERNAL' , 'pre_maldi.zip');

-- Converting data from ZARR file into Table format and output a top 10 rows
SELECT TOP 10 *
FROM TABLE(ZARR_TEST.PUBLIC.PY_LOAD_ZARRFILE(
        build_scoped_file_url(@ZARR_TEST.PUBLIC.STAGE_INTERNAL, 'pre_maldi.zip'), '2'));