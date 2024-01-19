#!/bin/bash

# Prompt user for input
read -p "What is the image repository URL (SHOW IMAGE REPOSITORIES IN SCHEMA)? " repository_url
read -p "What warehouse can the app use? " warehouse

# Paths to the files
makefile="./Makefile"
streamlit_file="./bb_node.yaml"

# Copy files
cp $makefile.template $makefile
cp $streamlit_file.template $streamlit_file

# Replace placeholders in Makefile file using | as delimiter
sed -i "" "s|<<repository_url>>|$repository_url|g" $makefile

# Replace placeholders in Streamlit file using | as delimiter
sed -i "" "s|<<repository_url>>|$repository_url|g" $streamlit_file
sed -i "" "s|<<warehouse_name>>|$warehouse|g" $streamlit_file

echo "Placeholder values have been replaced!"
