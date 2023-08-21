import sys

sys.path.append("utils.zip")

# If the folder 'utils' exists, then this will import from that folder. Otherwise
# it will import from the zip file.
from utils.plotting import this_is_a_cool_function

this_is_a_cool_function()
