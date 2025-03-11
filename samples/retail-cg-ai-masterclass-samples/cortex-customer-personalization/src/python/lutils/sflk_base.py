import os ,configparser ,json ,logging ,sys
from snowflake.snowpark.session import Session

logger = logging.getLogger('util')


def get_config(p_project_home_dir: str) -> configparser.ConfigParser:
    config_fl = f'{p_project_home_dir}/config.ini'
    config = configparser.ConfigParser()
    
    with open(config_fl) as f:
        config.read(config_fl)
    
    return config

def get_snowflake_conn_information(p_project_home_dir: str) -> json:
    # logger.debug('Fetching connection information ...')
    snow_conn_info = None

    config = get_config(p_project_home_dir)

    connection_info = config['SNOW_CONN']['connection_info']
    connection_env_var = (connection_info.split('|')[1]).strip()
    snow_conn_flpath =  f'''{p_project_home_dir}/{(connection_info.split('|')[0]).strip()}'''
    
    # Are we running in the codespace docker environment ?
    # When creating the dockerfile, we purpose created this hidden file to indicate
    # this
    if os.path.exists(f'{p_project_home_dir}/.is_codespace_env.txt'):
        #Is the connection_info pointing to an environment variable ?
        if connection_env_var in os.environ:
            logger.debug('connection_info is pointing to an environment variable')
            val = os.environ.get( (connection_info.split('|')[1]).strip() , {} )
            snow_conn_info = json.loads( val )
        else:
            raise Exception(f'''
                The codespace secret '{connection_env_var}' is not configured, refer to doc 'running_codespace' on setting this github secret.
            ''')
    
    elif os.path.exists(snow_conn_flpath):
        logger.debug('connection_info is pointing to a local file')
        with open(snow_conn_flpath) as conn_f:
            snow_conn_info = json.load(conn_f)

    else:
        raise Exception(f'''connection_info is not pointing to an environment variable or to a local file: {config['SNOW_CONN']['connection_info']}''')
    
    return snow_conn_info
    
def connect_to_snowflake(p_project_home_dir: str) -> Session:
    logger.info('Connecting to snowflake ...')
    snow_conn_info = None

    config = get_config(p_project_home_dir)
    snow_conn_info = get_snowflake_conn_information(p_project_home_dir)
    
    sp_session = Session.builder.configs(snow_conn_info).create()
    return sp_session
