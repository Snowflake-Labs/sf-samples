from IPython.display import display, HTML , Markdown
from snowflake.snowpark.session import Session
from PIL import Image
import pandas as pd
import logging
import tqdm
import glob
import numpy as np
import base64
from io import BytesIO
import os

def pp_data(session, img_path="./data_preprocessed", table_name="IMAGES_ENCODED"):
    """
    b64 encodes data from local and uploads to a table in snowflake
    inputs:
    - img_path: local path to preprocessed images
    - table_name: table name to use for table created in snowflake
    outputs:
    - None
    """
    try:
        session.table(table_name).limit(1).to_pandas()
    except:
        # encode data into table
        images = glob.glob(img_path+"/*.jpg")
        labels = []
        names = []
        data = []
        # b64 encodes images 
        for img in tqdm.tqdm(images):
            names.append(img.split('/')[-1])
            labels.append(img.split('/')[-1].split('_')[0])
            with open(img, "rb") as image_file:
                data.append(base64.b64encode(image_file.read()).decode("utf-8"))
        # create pandas DF with image name, label and b64 string
        df_base64 = pd.DataFrame(
            {
                'LABEL': labels,
                'NAME': names,
                'DATA': data
            }
        )
        # write DF into snowflake table
        sdf = session.create_dataframe(df_base64)
        sdf.write.save_as_table(table_name=table_name, mode='overwrite')
        
def onnx_model(session: Session, model_path="./notebook/model.onnx", model_stg="MODEL_STG"):
    """
    uploads onnx model to staging
    inputs:
    - model_path: local path to model
    - model_stg: snowflake stage name 
    outputs:
    - None
    """
    sql_stmt = f'PUT file://{model_path} @{model_stg} OVERWRITE = TRUE AUTO_COMPRESS = FALSE;'
    session.sql(sql_stmt).collect();