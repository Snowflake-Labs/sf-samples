from IPython.display import display, HTML , Markdown
from snowflake.snowpark.session import Session
from snowflake.snowpark.functions import udf, col
from torchvision.models._utils import IntermediateLayerGetter
from cachetools import cached
from io import BytesIO
from PIL import Image
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
import base64, json ,logging
import torch
import snowflake.snowpark.functions as F

def create_udf(session: Session) -> None:
    """creates and registers UDF using cached model"""
    @cached(cache={})
    def load_model(model_path: str) -> object:
        """Load ONNX model, set onnx runtime session options"""
        import onnxruntime as rt
        # Adjust session options (use only 1 Thread for best performance)
        opts = rt.SessionOptions()
        opts.intra_op_num_threads = 1
        providers = ['CPUExecutionProvider']
        model = rt.InferenceSession(model_path, providers=providers, sess_options=opts)
        return model

    @cached(cache={})
    def load_transform() -> object:
        """
        create data transformer to conduct nessisary preprocessing 
        on images for model ingestion
        """
        from torchvision import transforms
        # resizes, center crops and normalizes image
        data_transforms = transforms.Compose([
            transforms.Resize(256),
            transforms.CenterCrop(224),
            transforms.ToTensor(),
            transforms.Normalize([0.485, 0.456, 0.406], [0.229, 0.224, 0.225])
        ])
        return data_transforms
    
    def udf_onnx_embedding_model(data: str) -> str:
        """
        computes embedding for an input consisting of a b64 encoded image 
        """
        import sys
        import json
        import base64
        from io import BytesIO
        from PIL import Image

        # load model and image transform
        IMPORT_DIRECTORY_NAME = "snowflake_import_directory"
        import_dir = sys._xoptions[IMPORT_DIRECTORY_NAME]
        model_name = 'model.onnx'
        model = load_model(import_dir+model_name)
        transform = load_transform()

        # decode b64 encoded image into bytes, read bytes to image
        img = Image.open(BytesIO(base64.b64decode(data)))
        # apply transform to prepare input for embedding model
        img = transform(img).unsqueeze(0) 
        # compute embedding for input
        embedding = model.run(None, {"input": img.numpy()})[0].squeeze()
        output = json.dumps([round(float(x), 3) for x in embedding])
        return output
    
    # register utf 
    udf_onnx_embedding_model = session.udf.register(func=udf_onnx_embedding_model, 
                                                   name="udf_onnx_embedding_model", 
                                                   stage_location='@UDF_STG',
                                                   replace=True, 
                                                   is_permanent=True, 
                                                   imports=['@MODEL_STG/model.onnx'],
                                                   packages=["cachetools", "pillow", "torchvision", "onnxruntime"], 
                                                   session=session)


def image_embedding(session, table_name="IMAGES_ENCODED", encodings_col="DATA"):
    """helper function to generate image embeddings for the given table"""
    sdf = session.table(table_name)
    scored_sdf = sdf.with_column('EMBEDDING', F.call_udf("udf_onnx_embedding_model", F.col(encodings_col)))
    scored_sdf.write.save_as_table(table_name=table_name, mode='overwrite')