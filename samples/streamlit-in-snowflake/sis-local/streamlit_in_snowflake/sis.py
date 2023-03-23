from typing import Callable

from streamlit import *  # noqa: F403
from streamlit import (
    audio,
    bokeh_chart,
    cache,
    camera_input,
    download_button,
    experimental_get_query_params,
    experimental_set_query_params,
    experimental_user,
    file_uploader,
    image,
    video,
    warning,
)
from streamlit import write as original_write

SIS_FAQ = "https://docs.google.com/document/d/1XY0wPp5tJQHivkzkcxYKjwKiW8-rreHsUwdYFmWEuwg/edit"

# TODO: st.markdown should also warn when using unsafe html
# TODO: warn for custom components
# TODO: warn for components API
# TODO: st.info for cache_data and cache_resource? Since they won't crash but just won't work

UNSUPPORTED_STREAMLIT_FEATURES = [
    file_uploader,
    cache,
    camera_input,
    image,
    video,
    audio,
    experimental_get_query_params,
    experimental_set_query_params,
    experimental_user,
    download_button,
    bokeh_chart,
]


def create_st_command_with_warning(st_command: Callable):
    def command(args, **kwargs):
        warning(
            icon="⚠️",
            body=f"""**You used `st.{st_command.__name__}` but it is not
            supported in SiS.** Read more in the [SiS FAQ]({SIS_FAQ})""",
        )
        return st_command(args, **kwargs)

    return command


file_uploader = create_st_command_with_warning(file_uploader)
cache = create_st_command_with_warning(cache)
camera_input = create_st_command_with_warning(camera_input)
image = create_st_command_with_warning(image)
video = create_st_command_with_warning(video)
audio = create_st_command_with_warning(audio)
experimental_get_query_params = create_st_command_with_warning(experimental_get_query_params)
experimental_set_query_params = create_st_command_with_warning(experimental_set_query_params)
experimental_user = create_st_command_with_warning(experimental_user)
download_button = create_st_command_with_warning(download_button)
bokeh_chart = create_st_command_with_warning(bokeh_chart)


def write(args, **kwargs):
    if "unsafe_allow_html" in kwargs:
        warning(
            icon="⚠️",
            body="""**You used `unsafe_allow_html` in `st.write` but it is not supported
            in SiS.** Read more in the [SiS FAQ]({SIS_FAQ})""",
        )
    original_write(args, **kwargs)
