#!/bin/bash

#
# This script gets executed at the end of the codespace creation. This is
# a place to run any configurations or command line utilities.
#
# NOTE: If you add lot of utilities here, it might take a longer time to initialize the final 
# environment. 
#
#

# Used by execution script to verify if we are running in a codespace docker environment 
echo "running in codespace" >> .is_codespace_env.txt 

# The following will install additional packages in the 'pyspark' conda enviroment
# enable this if you want them to be added during the startup
#
# WARN: This would increase the startup time, test it out and see if you would be ok
#       on the timeline
#
#conda env update -f ./environment.yml

#This needs to be run to ensure the local utility scripts are add to path
# python ./bin/setup.py install