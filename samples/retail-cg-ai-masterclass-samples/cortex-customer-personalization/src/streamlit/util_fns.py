
import configparser ,json ,logging
import os ,sys ,subprocess
from snowflake.snowpark.session import Session
from pathlib import Path
import logging ,sys ,os 
import streamlit as st
import pandas as pd
import requests ,zipfile, tarfile

# Import the commonly defined utility scripts using
# dynamic path include
import sys
sys.path.append('./src/python/lutils')
import sflk_base as L


logger = logging.getLogger('app')

def exec_sql_script(p_sqlscript: str ,p_cache_id) -> bool:
    '''
        Executes a sql script and writes the output to a textbox.
    '''
    script_executed = False
    logger.info(f'Executing sql script: {p_sqlscript} ...')
    
    # Capture script output.
    script_output = []

    process = subprocess.Popen(
        ['./bin/exec_sqlscript.sh'
        ,p_sqlscript]
        ,stdout=subprocess.PIPE
        ,universal_newlines=True)

    while True:
        output = process.stdout.readline()
        # st.write(output)
        script_output.append(output)
        return_code = process.poll()
        
        if return_code is not None:
            # st.write(f'RETURN CODE: {return_code} \n')
            script_output.append(f'RETURN CODE: {return_code} \n')
            script_executed = True

            # Process has finished, read rest of the output 
            for output in process.stdout.readlines():
                # st.write(output)
                script_output.append(output)

            break

    script_output.append('\n --- Finished executing script ---')
    if 'output' not in st.session_state:
        # Write the Script Output to the Session.
        st.session_state[p_cache_id] = script_output

    return script_executed


def load_sample_and_display_table(p_session: Session ,p_table: str ,p_sample_rowcount :int):
    '''
    Utility function to display sample records 
    '''
    st.write(f'sampling target table {p_table} ...')
    tbl_df = (p_session
        .table(p_table)
        .limit(p_sample_rowcount)
        #.sample(n=p_sample_rowcount)
        .to_pandas())

    st.dataframe(tbl_df ,use_container_width=True)
    st.write(f'')

def list_stage(p_session: Session ,p_stage :str):
    '''
    Utility function to display contents of a stage
    '''
    rows = p_session.sql(f''' list @{p_stage}; ''').collect()
    data = []
    for r in rows:
        data.append({
            'name': r['name']
            ,'size': r['size']
            ,'last_modified': r['last_modified']
        })

    df = pd.json_normalize(data)
    return df

def download_library_frompypi_tolocal(p_pypi_url :str ,p_library_file :str ,p_download_dir :str):
    """ Download library to a local folder. Typically used for downloading third party
        libraries from PyPI

        :param p_pypi_url: The PyPi download url
        :param p_library_file: The local filename for the downloaded library,
            usually this is the library name without the '.tar.gz' extension
        :param p_download_dir: The local directory where this file would be downloaded to.

        :returns: Status of downloading
    """
    print(f'  Downloading library: {p_library_file} ...')
    local_lib_fl = f'{p_download_dir}/{p_library_file}'

    # Create a local directory to store data, library etc..
    Path(p_download_dir).mkdir(parents=True, exist_ok=True)
    print(f'  Create local dir: {p_download_dir}')

    print(f'  Downloading library from PyPI to {p_download_dir} ...')
    with open(local_lib_fl, "wb") as f:
        r = requests.get(p_pypi_url)
        f.write(r.content)

    return True

# -------------------------------------
def upload_locallibraries_to_p_stage(p_sflk_session ,p_local_dir ,p_db ,p_sch ,p_stage ,p_stage_dir):
    """ Used for uploading third party libraries from a local directory to an stage. 

        :param p_sflk_session: The snowflake session
        :param p_local_dir: The local directory where the third party libraries are kept
        :param p_db: The database for the stage
        :param p_sch: The schema for the stage
        :param p_stage: The stage to upload too
        :param p_stage_dir: The stage folder under which the library will be stored. Typically 'lib'
        
        :returns: Nothing
    """
    print(f" Uploading library to stage: {p_db}.{p_sch}.{p_stage} ")

    for path, currentDirectory, files in os.walk(p_local_dir):
        for file in files:
            # build the relative paths to the file
            local_file = os.path.join(path, file)

            # build the path to where the file will be staged
            stage_dir = path.replace(p_local_dir , p_stage_dir)

            print(f'    {local_file} => @{p_stage}/{stage_dir}')
            p_sflk_session.file.put(
                local_file_name = local_file
                ,stage_location = f'{p_db}.{p_sch}.{p_stage}/{stage_dir}'
                ,auto_compress=False ,overwrite=True)
    
    p_sflk_session.sql(f'alter stage {p_db}.{p_sch}.{p_stage} refresh; ').collect()