from snowflake.snowpark.session import Session
import streamlit as st
import logging, sys
import matplotlib.pyplot as plt
import snowflake.snowpark.functions as F
from snowflake.snowpark.functions import udf, col

sys.path.append('src/python/lutils')
import sflk_base as L
import pandas as pd
import numpy as np
from sklearn.metrics.pairwise import cosine_similarity
from PIL import Image, ImageOps
from io import BytesIO
import base64, json ,logging, io
import random
from cachetools import cached
# Define the project home directory, this is used for locating the config.ini file
PROJECT_HOME_DIR='.'

# Initialize a session with Snowflake
config = L.get_config(PROJECT_HOME_DIR)
sp_session = None
if "snowpark_session" not in st.session_state:
    sp_session = L.connect_to_snowflake(PROJECT_HOME_DIR)
    sp_session.use_role(f'''{config['APP_DB']['role']}''')
    sp_session.use_schema(f'''{config['APP_DB']['database']}.{config['APP_DB']['schema']}''')
    sp_session.use_warehouse(f'''{config['SNOW_CONN']['warehouse']}''')
    st.session_state['snowpark_session'] = sp_session
else:
    sp_session = st.session_state['snowpark_session']

def page_header():
    # Header.
    st.image("src/streamlit/images/logo-sno-blue.png", width=100)
    st.subheader("Fashion Snow-ke Show")

def custom_page_styles():
    # page custom Styles
    st.markdown("""
                <style>
                    #MainMenu {visibility: hidden;} 
                    header {height:0px !important;}
                    footer {visibility: hidden;}
                    .block-container {
                        padding: 0px;
                        padding-top: 15px;
                        padding-left: 1rem;
                        max-width: 98%;
                    }
                    [data-testid="stVerticalBlock"] {
                        gap: 0;
                    }
                    [data-testid="stHorizontalBlock"] {
                        width: 99%;
                    }
                    .stApp {
                        width: 98% !important;
                    }
                    [data-testid="stMetricLabel"] {
                        font-size: 1.25rem;
                        font-weight: 700;
                    }
                    [data-testid="stMetricValue"] {
                        font-size: 1.5rem;
                        color: gray;
                    }
                    [data-testid="stCaptionContainer"] {
                        font-size: 1.25rem;
                    }
                    div.stButton > button:first-child {
                        background-color: #50C878;color:white; border-color: none;
                </style>""", unsafe_allow_html=True)
key_counter=0
data_table = "IMAGES_ENCODED"
data_col = 'DATA'
name_col = 'NAME'
label_col = 'LABEL'
embedding_col = "EMBEDDING"
sdf1 = sp_session.table(data_table)

def get_image(img_name):
    data = sdf1.filter(col(name_col) == img_name).select(label_col, data_col).to_pandas()
    img = Image.open(BytesIO(base64.b64decode(data[data_col][0])))
    label = data[label_col][0]
    return img, label

def visualize_model(item1,df, num_images=6):
    global key_counter
    images_so_far = 0
    fig = plt.figure()
    for row in df.iterrows():
        item = row[0]
        sim_score = row[1].values[0]
        
        img, label = get_image(item)
        img = np.asarray(img)
        st.session_state[item1+str(images_so_far)]=img
        #assert False, item+str(images_so_far)
        images_so_far += 1
        ax = plt.subplot(num_images//2, 2, images_so_far)
        ax.axis('off')
        ax.set_title(f'Label: {label}, Similarity: {sim_score:.2}')
        st.image(img,width=100)
        double=st.button(' ▶️ ADD TO CART!',key=key_counter)
        key_counter+=1
        if images_so_far == num_images:
            return
        
# Finds top N most similar items for an input item and can display them
    
def display_item(item):
    img, label = get_image(item)
    st.image(img, width=300);
    
@cached(cache={})
def load_transform() -> object:
    from torchvision import transforms
    data_transforms = transforms.Compose([
        transforms.Resize(256),
        transforms.CenterCrop(224),
        transforms.ToTensor(),
        transforms.Normalize([0.485, 0.456, 0.406], [0.229, 0.224, 0.225])
    ])
    return data_transforms
def top_n(sim_df, item, n=10, plot_n=None, visualize = True, best=True):
    if best:
        df = sim_df[[item]].sort_values(item, ascending=False)[1:].head(n) # top n
    else: 
        df = sim_df[[item]].sort_values(item, ascending=True).head(n) # bottom n
    if plot_n is None:
        plot_n = n
    visualize_model(item,df, num_images=plot_n)
    return df

def build_UI():
    global key_counter
    global cart_value
    custom_page_styles()
    page_header()
    st.markdown(
        """<hr style="height:2px; width:99%; border:none;color:lightgrey;background-color:lightgrey;" /> """,
        unsafe_allow_html=True)

    st.markdown("""<hr style="height:40px; font-size:2px; width:99%; border:none;color:none;" /> """,
                unsafe_allow_html=True)
    
    data_table = "IMAGES_ENCODED"
    data_col = 'DATA'
    name_col = 'NAME'
    label_col = 'LABEL'
    embedding_col = "EMBEDDING"

    sdf = sp_session.table(data_table)
    img_names = sdf.select(name_col).to_pandas()

    embeddings = sdf.select(embedding_col, name_col).to_pandas()

    uploaded_file = st.file_uploader("Upload your jpeg here...")
    # read file as base64
    if uploaded_file is not None:
        bytes_data = uploaded_file.getvalue()
        base64_img = base64.b64encode(bytes_data).decode("utf-8")
        # preprocess image
        buffer = io.BytesIO()
        imgdata = base64.b64decode(base64_img)
        img = Image.open(io.BytesIO(imgdata))
        img =ImageOps.exif_transpose(img)
        if np.asarray(img).shape[-1] != 3:
            img = img.convert('RGB')
        w,h = img.size
        scale_factor = min(w/256, h/256)
        new_size = (int(w/scale_factor), int(h/scale_factor))
        img = img.resize(new_size)
        img.save(buffer, format="jpeg")
        base64_img = base64.b64encode(buffer.getvalue()).decode("utf-8")
        # get embedding
        sdf = sp_session.create_dataframe(['new_image'], schema=["NAME"])
        scored_sdf = sdf.with_column('EMBEDDING', F.call_udf("udf_torch_embedding_model", base64_img))
        embedding = scored_sdf.to_pandas()
        embeddings=pd.concat([embeddings, embedding], axis=0)
        embeddings_np = list(embeddings[embedding_col].apply(lambda x: np.array(json.loads(x))))

        similarity=cosine_similarity(np.vstack(embeddings_np))
        sim_df = pd.DataFrame(similarity, 
                            columns=embeddings.NAME, 
                            index=embeddings.NAME)

        item = 'new_image'
        st.image(imgdata,width=300)
        top_n(sim_df,item)
    
if __name__ == '__main__':
    build_UI()
