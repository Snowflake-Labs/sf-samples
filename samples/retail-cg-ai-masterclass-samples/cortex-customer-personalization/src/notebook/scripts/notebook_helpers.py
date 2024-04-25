
#
# These are various functions, meant to be imported into the notebooks and used to aid execution or deployment
#

from pathlib import Path
import os ,json ,requests ,zipfile, tarfile
from IPython.display import display, HTML , Markdown
import os ,configparser ,json ,logging ,sys
from snowflake.snowpark.session import Session

# logging.basicConfig(stream=sys.stdout, level=logging.ERROR)
# logger = logging.getLogger('util')

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

# -------------------------------------
def read_source_and_format(p_scriptfl :str ,p_substitues :dict):
    """ Reads a scripts source file, which is templatized with specific placeholders for database, schema etc..
        The file content is formulated/substituted with the appropriate values. This function is used for 
        deploying scripts (javascript/python), which are UDF for ex into Snowflake. 

        :param p_scriptfl: The source file that is templatized
        :param p_substitues: A dictonary with key/value pairs indicating the replacement values
        
        :returns: de-templatized string
    """
    file_content = ''
    with open(p_scriptfl, 'r') as file:
        file_content = file.read()

    from string import Template
    fomatted_content = Template(file_content).substitute(p_substitues)

    return fomatted_content
    
# -------------------------------------
def display_code(p_title ,p_code ,p_background_color = '#E2EC9A;'):
    """ Used for highlighing and displaying the content. Typically used to show dynamic generated code/statements. 

        :param p_title: The Title
        :param p_code: The code to show
        
        :returns: None
    """
    display(HTML(f'''
        <html> <body>
        <h4> {p_title} </h4>
        <pre style="background-color:{p_background_color}">
        {p_code}
        </pre>
        </body> </html>
        '''))

# -------------------------------------
def set_cell_background(color):
    '''
        This is an utility fn to set the background color of code cell.
        Ref: https://stackoverflow.com/questions/49429585/how-to-change-the-background-color-of-a-single-cell-in-a-jupyter-notebook-jupy
    '''
    script = (
        "var cell = this.closest('.jp-CodeCell');"
        "var editor = cell.querySelector('.jp-Editor');"
        "editor.style.background='{}';"
        "this.parentNode.removeChild(this)"
    ).format(color)

    display(HTML('<img src onerror="{}" style="display:none">'.format(script)))

from IPython.core.magic import register_cell_magic

@register_cell_magic
def background(color, cell): 
    set_cell_background(color) 
    return eval(cell)