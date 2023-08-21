import sys

import streamlit as st

sys.path.append(".")
from file2 import some_handy_function

st.write("# Multi-File App Example")

some_handy_function()
