# Ref: https://www.datasciencelearner.com/importerror-attempted-relative-import-parent-package/

# Utility script to ensure that the local packages are defined in the
# path, so that these utility functions can be re-used across multiple 
# artifacts.

from setuptools import setup, find_packages  
setup(name = 'lutils', packages = find_packages())