import boto3, os, logging, sys
sys.path.append('./src/python/lutils')
import sflk_base as L

logging.basicConfig(stream=sys.stdout, level=logging.ERROR)

# Define the project home directory, this is used for locating the config.ini file
PROJECT_HOME_DIR = './'
config = L.get_config(PROJECT_HOME_DIR)
session = L.connect_to_snowflake(PROJECT_HOME_DIR)

if not os.path.isdir('data_raw/'):
    #make folder
    os.mkdir('data_raw/')
    os.mkdir('data_raw/train')
    os.mkdir('data_raw/val')
    os.mkdir('data_raw/images_original')
    
    # set aws credentials 
    s3r = boto3.resource('s3', aws_access_key_id=config['AWS_S3']['access_key'],
        aws_secret_access_key=config['AWS_S3']['secret_key'])
    bucket = s3r.Bucket(config['AWS_S3']['bucket'])

    # downloading folder 
    prefix = 'data_raw/'
    for object in bucket.objects.filter(Prefix = prefix):
        if object.key == prefix:
            os.makedirs(os.path.dirname(object.key), exist_ok=True)
            continue;
        if not os.path.isfile(object.key):
            try:
                bucket.download_file(object.key, object.key)
            except: 
                os.mkdir(object.key.rsplit('/',1)[0])
                bucket.download_file(object.key, object.key)