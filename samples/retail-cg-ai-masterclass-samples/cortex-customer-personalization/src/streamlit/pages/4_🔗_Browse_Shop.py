from snowflake.snowpark.session import Session
import streamlit as st
import logging, sys
import matplotlib.pyplot as plt
from snowflake.snowpark.functions import udf, col

sys.path.append('src/python/lutils')
import sflk_base as L
import pandas as pd
import numpy as np
from sklearn.metrics.pairwise import cosine_similarity
from PIL import Image
from io import BytesIO
import base64, json ,logging
import random

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

sdf = sp_session.table(data_table)
img_names = sdf.select(name_col).to_pandas()

embeddings = sdf.select(embedding_col, name_col).to_pandas()
embeddings_np = list(embeddings[embedding_col].apply(lambda x: np.array(json.loads(x))))
similarity=cosine_similarity(np.vstack(embeddings_np))


sim_df = pd.DataFrame(similarity, 
                      columns=embeddings.NAME, 
                      index=embeddings.NAME)

def get_image(img_name):
    data = sdf.filter(col(name_col) == img_name).select(label_col, data_col).to_pandas()
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
def top_n(sim_df, item, n=10, plot_n=None, visualize = True, best=True):
    if best:
        df = sim_df[[item]].sort_values(item, ascending=False)[1:].head(n) # top n
    else: 
        df = sim_df[[item]].sort_values(item, ascending=True).head(n) # bottom n
    if plot_n is None:
        plot_n = n
    visualize_model(item,df, num_images=plot_n)
    return df
index=[1000,2000,3000,4504,3980,200]
st.session_state['cart_value']=0
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
    c1, c2, c3 = st.columns(3)
    
    with c1:
        item = img_names.NAME[index[0]]
        df = display_item(item)
        btn1 = st.button(' ▶️ ADD TO CART!', key=key_counter)
        key_counter+=1
        with st.expander('You might also like...', expanded=True):
            if btn1:
                top_n(sim_df,item)
            else:
                for i in range(6):
                    if item+str(i) in st.session_state:
                        st.image(st.session_state[item+str(i)],width=100)
                        double=st.button(' ▶️ ADD TO CART!',key=key_counter)
                        key_counter+=1
        
        item = img_names.NAME[index[1]]
        df = display_item(item)
        btn2 = st.button(' ▶️ ADD TO CART!', key=key_counter)
        key_counter+=1
        with st.expander('You might also like...', expanded=True):
            if btn2:
                top_n(sim_df,item)
            else:
                for i in range(6):
                    if item+str(i) in st.session_state:
                        st.image(st.session_state[item+str(i)],width=100)
                        double=st.button(' ▶️ ADD TO CART!',key=key_counter)
                        key_counter+=1

    with c2:
        item = img_names.NAME[index[2]]
        df = display_item(item)
        btn3 = st.button(' ▶️ ADD TO CART!', key=key_counter)
        key_counter+=1
        with st.expander('You might also like...', expanded=True):
            if btn3:
                top_n(sim_df,item)
            else:
                for i in range(6):
                    if item+str(i) in st.session_state:
                        st.image(st.session_state[item+str(i)],width=100)
                        double=st.button(' ▶️ ADD TO CART!',key=key_counter)
                        key_counter+=1
        
        item = img_names.NAME[index[3]]
        df = display_item(item)
        btn4 = st.button(' ▶️ ADD TO CART!', key=key_counter)
        key_counter+=1
        with st.expander('You might also like...', expanded=True):
            if btn4:
                top_n(sim_df,item)
            else:
                for i in range(6):
                    if item+str(i) in st.session_state:
                        st.image(st.session_state[item+str(i)],width=100)
                        double=st.button(' ▶️ ADD TO CART!',key=key_counter)
                        key_counter+=1
    
    with c3:
        item = img_names.NAME[index[4]]
        df = display_item(item)
        btn5 = st.button(' ▶️ ADD TO CART!', key=key_counter)
        key_counter+=1
        with st.expander('You might also like...', expanded=True):
            if btn5:
                top_n(sim_df,item)
            else:
                for i in range(6):
                    if item+str(i) in st.session_state:
                        st.image(st.session_state[item+str(i)],width=100)
                        double=st.button(' ▶️ ADD TO CART!',key=key_counter)
                        key_counter+=1
        
        item = img_names.NAME[index[5]]
        df = display_item(item)
        btn6 = st.button(' ▶️ ADD TO CART!', key=key_counter)
        key_counter+=1
        with st.expander('You might also like...', expanded=True):
            if btn6:
                top_n(sim_df,item)
            else:
                for i in range(6):
                    if item+str(i) in st.session_state:
                        st.image(st.session_state[item+str(i)],width=100)
                        double=st.button(' ▶️ ADD TO CART!',key=key_counter)
                        key_counter+=1
if __name__ == '__main__':
    build_UI()
