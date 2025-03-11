# Download files from public S3 bucket without having to provide AWS creds
bucket_name = 'sfquickstarts'
download_dir = '/Users/jprusa/Downloads'
categories = ["blazer" ,"blouse" ,"body" ,"dress" ,"hat" ,"hoodie" ,"longsleeve" ,"not_sure" ,"other" ,"outwear", "pants" ,"polo" ,"shirt" ,"shoes" ,"shorts" ,"skirt" ,"t_shirt", "top" ,"undershirt"]
prefixes = ['data_preprocessed/','data_raw/images_original/']

# Add image category prefixes for data_raw/train and data_raw/val folders
for category in categories:
    prefixes.append('data_raw/train/'+category+"/")
    prefixes.append('data_raw/val/'+category+"/")
    
import boto3
from botocore import UNSIGNED
from botocore.config import Config
s3 = boto3.client('s3', config=Config(signature_version=UNSIGNED))

for prefix in prefixes:
    response = s3.list_objects(Bucket=bucket_name,Prefix='advanced_personalization_recommendation_engine/'+prefix)
    for key_obj in response['Contents']:
        if 'jpg' in key_obj["Key"]:
            key_obj_name = key_obj["Key"]
            print(f'Downloading {key_obj_name}')
            download_filename = download_dir + key_obj_name.replace("/","_")
            s3.download_file(bucket_name, key_obj_name, download_filename)