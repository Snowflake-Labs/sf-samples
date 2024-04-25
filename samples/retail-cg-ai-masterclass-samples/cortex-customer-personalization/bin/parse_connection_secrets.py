#!/usr/bin/python

'''
This program is solely used as a helper script for the shell scripts to parse the snowflake
connection and configuration file. It is not to be used other than this functionality.
'''

import os ,configparser ,json ,logging

# Import the commonly defined utility scripts using
# dynamic path include
import sys
sys.path.append('src/python/lutils')
import sflk_base as L

# Define the project home directory, this is used for locating the config.ini file
PROJECT_HOME_DIR='.'

CONFIG_FL = f'{PROJECT_HOME_DIR}/config.ini'
config = configparser.ConfigParser()
logger = logging.getLogger('parse_connection_secrets')
snow_conn_info = None

def store_configs_for_sqlscript(p_config):
    '''
        We would need to use some of the configurations defined in 'config.ini' (ex: APP_DB.ROLE)
        inside SQL scripts. To facilitate this, we store the variables inside a local config file 
        and will use them when connecting to snowflake
    '''
    out_file = os.path.join('.app_store' ,'snowsql_config.ini')
    #automatically create parent folders if it does not exists to avoid errors
    os.makedirs(os.path.dirname(out_file), exist_ok=True)

    pre_defined_config = '''
[options]\n
variable_substitution = True \n

[variables]\n
'''

    sections_toignore = ['DATA']
    with open(out_file, "w") as out_fl:
        out_fl.write(pre_defined_config)
        
        for conf_section in p_config.sections():
            if conf_section in sections_toignore:
                continue

            for i ,(k ,v) in enumerate(p_config.items(conf_section)):

                # Ignore from storing these keys
                if k in ['connection_info']:
                    continue

                out_fl.write(f'{conf_section}_{k}={v}\n')

    return out_file

snow_conn_info = None
with open(CONFIG_FL) as f:
    config.read(CONFIG_FL)
    
    store_configs_for_sqlscript(config)

    snow_conn_info = L.get_snowflake_conn_information(PROJECT_HOME_DIR)
    
key = sys.argv[1]
print(snow_conn_info[key])

