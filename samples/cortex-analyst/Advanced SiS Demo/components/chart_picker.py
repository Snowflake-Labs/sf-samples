from typing import Callable, Dict, List, Optional

import pandas as pd
import streamlit as st

from utils.plots import (
    ALL_SUPPORTED_OPTIONS,
    AVAILABLE_CHARTS,
    ChartConfigDict,
    ChartDefinition,
)


def _format_barmode(x: str) -> str:
    return {
        "relative": "Relative",
        "group": "Group",
        "stack": "Stack",
        "overlay": "Overlay",
    }.get(x, x)


def _pick_barmode(component_idx: int = -1, default: str = "relative") -> str:
    options = ALL_SUPPORTED_OPTIONS["barmode"]
    default_idx = options.index(default)
    return st.selectbox(
        "Barmode",
        options=options,
        index=default_idx,
        format_func=_format_barmode,
        help="Choose how bars are displayed",
        key=f"barmode_picker_{component_idx}",
    )


def _format_orientation(x: str) -> Optional[str]:
    return "Vertical" if x == "v" else "Horizontal"


def _pick_orientation(component_idx: int = -1, default: str = "v") -> str:
    options = ALL_SUPPORTED_OPTIONS["orientation"]
    default_idx = options.index(default)
    return st.selectbox(
        "Orientation",
        options=options,
        index=default_idx,
        format_func=_format_orientation,
        help="Choose the orientation of the plot",
        key=f"orientation_picker_{component_idx}",
    )


def _pick_nbins(component_idx: int = -1, default: int = 10) -> int:
    return st.slider(
        "Number of bins",
        min_value=1,
        max_value=100,
        value=default,
        help="Choose the number of bins for the histogram",
        key=f"nbins_picker_{component_idx}",
    )


def _format_line_shape(x: str) -> str:
    return {
        "linear": "Linear",
        "spline": "Spline",
        "hv": "Horizontal-Vertical",
        "vh": "Vertical-Horizontal",
        "hvh": "Horizontal-Vertical-Horizontal",
        "vhv": "Vertical-Horizontal-Vertical",
    }.get(x, x)


def _pick_line_shape(component_idx: int = -1, default: str = "linear") -> Optional[str]:
    options = ALL_SUPPORTED_OPTIONS["line_shape"]
    default_idx = options.index(default)
    return st.selectbox(
        "Line shape",
        options=options,
        index=default_idx,
        format_func=_format_line_shape,
        help="Choose the shape of the lines in the plot",
        key=f"line_shape_picker_{component_idx}",
    )


# map extra param name to function rendering UI element
extra_params_pickers: Dict[str, Callable[[int, str], str]] = {
    "barmode": _pick_barmode,
    "orientation": _pick_orientation,
    "nbins": _pick_nbins,
    "line_shape": _pick_line_shape,
}


def _get_default_idx(option: Optional[str], all_options: List[str], idx: int) -> int:
    if option is None:
        return idx
    try:
        return all_options.index(option)
    except ValueError:
        return idx


def chart_picker(
    df: pd.DataFrame,
    default_config: Optional[ChartConfigDict] = None,
    component_idx: int = -1,
) -> Dict:
    """
    Based on provided dataframe and selected chart type will render chart picking controls, and return chart config.

    Args:
        df (pd.DataFrame): The query results.
        default_config Optional[ChartConfigDict] : default config describing how the default values picks for all controls
        component_idx (int): Unique component for index - handy for spawning widgets with unique keys when spawning this component multiple times.

    Returns:
        (Dict): Dict config defining chart
    """
    if not default_config:
        default_config = {"type": "Bar", "params": {}}
    all_col_names = list(df.columns)
    all_charts_names = list(AVAILABLE_CHARTS.keys())
    df_has_at_least_two_columns = len(df.columns) >= 2

    plot_cfg: Dict = {"type": None, "params": {}}
    def_params = default_config["params"]

    # Get default chart type index
    default_chart_name = default_config.get("type")
    if default_chart_name is None:
        default_chart_name = all_charts_names[0]
    default_chart_idx = all_charts_names.index(default_chart_name)

    # Pick chart type
    picked_chart = st.selectbox(
        "Select chart type",
        options=all_charts_names,
        format_func=lambda chart_name: AVAILABLE_CHARTS[chart_name].get_pretty_name(),
        key=f"chart_type_{component_idx}",
        index=default_chart_idx,
        disabled=(not df_has_at_least_two_columns),
    )
    if not df_has_at_least_two_columns:
        st.info("At least two columns are required to plot a chart")
        return plot_cfg

    plot_cfg["type"] = picked_chart
    picked_params = {}
    picked_chart_setings: ChartDefinition = AVAILABLE_CHARTS[picked_chart]

    # We always need 2 columns for x and y
    col1, col2 = st.columns(2)
    col1_name, col2_name = picked_chart_setings.base_column_args

    # Get default indexes
    col1_default_idx = _get_default_idx(def_params.get(col1_name), all_col_names, 0)
    col2_default_idx = _get_default_idx(def_params.get(col2_name), all_col_names, 1)

    picked_params[col1_name] = col1.selectbox(
        col1_name,
        all_col_names,
        key=f"x_col_select_{component_idx}",
        index=col1_default_idx,
    )

    picked_params[col2_name] = col2.selectbox(
        col2_name,
        all_col_names,
        key=f"y_col_select_{component_idx}",
        index=col2_default_idx,
    )

    # Now we can pick more columns
    if len(df.columns) > 2:
        for col_param_name in picked_chart_setings.extra_column_args:
            def_idx = _get_default_idx(
                def_params.get(col_param_name), all_col_names, None
            )
            selectbox_args = {
                "label": col_param_name,
                "options": all_col_names,
                "key": f"{col_param_name}_select_{component_idx}",
            }
            if def_idx is not None:
                selectbox_args["index"] = def_idx
            picked_params[col_param_name] = st.selectbox(**selectbox_args)

    # Other, non-column args
    for param_name in picked_chart_setings.additional_params:
        picker_fnc = extra_params_pickers.get(param_name)
        if picker_fnc is None:
            continue
        fnc_args = {"component_idx": component_idx}
        default_val = default_config["params"].get(param_name)
        if default_val is not None:
            fnc_args["default"] = default_val
        picked_params[param_name] = picker_fnc(**fnc_args)

    plot_cfg["params"] = picked_params
    return plot_cfg
