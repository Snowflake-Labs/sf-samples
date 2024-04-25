import streamlit as st

def custom_page_styles():
    # page custom Styles
    st.markdown("""
                <style>
                    #MainMenu {visibility: hidden;} 
                    header {height:0px !important;}
                    footer {visibility: hidden;}
                    .block-container {
                        padding: 0px;
                        padding-top: 35px;
                        padding-left: 2rem;
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
                </style>""", unsafe_allow_html=True)

def page_header():
    # Header.
    st.image("src/streamlit/images/logo-sno-blue.png", width=100)
    st.subheader("Retail Recommender Demo")

def display_readme():
    with open("README.md", 'r') as f:
        readme_line = f.readlines()
        readme_buffer = []

    for line in readme_line:
        readme_buffer.append(line)

        if line.startswith('<img '):
            src = line.split(' src="')[1].split('"')[0]
            st.markdown(''.join(readme_buffer[:-1]))

            c1, c2, c3 = st.columns([0.1, 0.8, 0.1])
            with c2:
                st.image(f'{src}')

            readme_buffer.clear()

    st.markdown(''.join(readme_buffer))

def build_UI():
    st.set_page_config(
        page_title="AD campaign effectiveness",
        page_icon="🧊",
        layout="wide",
        initial_sidebar_state="expanded")

    custom_page_styles()
    page_header()
    st.markdown(
        """<hr style="height:2px; width:99%; border:none;color:lightgrey;background-color:lightgrey;" /> """,
        unsafe_allow_html=True)

    display_readme()


if __name__ == '__main__':
    build_UI()