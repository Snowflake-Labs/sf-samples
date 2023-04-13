import streamlit as st
import zipimport

importer = zipimport.zipimporter('utils.zip')

module=importer.load_module('utils/helper')

module.helper_function()