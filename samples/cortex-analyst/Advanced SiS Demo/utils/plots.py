"""
This module provides utilities and definitions related to chart creation using the Plotly Python package.

It includes a data class for chart configurations and a dictionary of available chart types. Additionally,
it provides a function to generate Plotly figures based on the provided configuration.
"""

from dataclasses import dataclass, field
from typing import Callable, Dict, List, Literal, Optional, TypedDict

import pandas as pd
import plotly.express as px
from plotly.graph_objs._figure import Figure


@dataclass
class ChartDefinition:
    """
    A data class to store configuration details for a chart.

    This class is designed to encapsulate the configuration details required to generate
    charts using plotly.express functions. It stores parameter names and other relevant
    information needed to create various types of charts.

    Attributes:
        name (str): The name of the chart.
        plotly_fnc (Callable): The Plotly function to generate the chart.
        icon (str): An icon representing the chart type.
        base_column_args (List[str]): Names of the base Plotly arguments that take column names as input.
        extra_column_args (List[str]): Additional Plotly arguments that take column names as input.
        additional_params (List[str]): Other plot-specific parameter names.
    """

    name: str
    plotly_fnc: Callable
    icon: str
    base_column_args: List[str] = field(default_factory=lambda: ["x", "y"])
    extra_column_args: List[str] = field(default_factory=list)
    additional_params: List[str] = field(default_factory=list)

    def get_pretty_name(self) -> str:
        """Get name with icon."""
        return f"{self.name} {self.icon}"


class ChartParams(TypedDict, total=False):
    """
    A dict containing all supported parameters for configuring charts using Plotly Express.

    This dictionary is used to store the parameters required by various Plotly Express chart plotting functions.
    Each key represents a parameter name, and the corresponding value represents the parameter value.
    """

    data_frame: Optional[pd.DataFrame]

    x: Optional[str]
    y: Optional[str]
    names: Optional[str]
    values: Optional[str]
    color: Optional[str]

    barmode: Optional[str]
    orientation: Optional[str]
    nbins: Optional[int]
    line_shape: Optional[str]


class ChartConfigDict(TypedDict):
    """A dict containing all configuration required to draw a chart."""

    type: str
    params: ChartParams


ALL_SUPPORTED_ARGS: Dict[str, Literal["column", "number", "option"]] = {
    "x": "column",
    "y": "column",
    "names": "column",
    "values": "column",
    "color": "column",
    "barmode": "option",
    "orientation": "option",
    "line_shape": "option",
    "nbins": "number",
}

ALL_SUPPORTED_OPTIONS: Dict[str, List[str]] = {
    "barmode": ["relative", "group", "stack", "overlay"],
    "orientation": ["v", "h"],
    "line_shape": ["linear", "spline", "hv", "vh", "hvh", "vhv"],
}

# A dictionary of all currently supported charts
AVAILABLE_CHARTS: Dict[str, ChartDefinition] = {
    "Bar": ChartDefinition(
        name="Bar Chart",
        plotly_fnc=px.bar,
        icon="📊",
        extra_column_args=["color"],
        additional_params=["barmode", "orientation"],
    ),
    "Line": ChartDefinition(
        name="Line Chart",
        plotly_fnc=px.line,
        icon="📈",
        extra_column_args=["color"],
        additional_params=["line_shape"],
    ),
    "Pie": ChartDefinition(
        name="Pie Chart",
        plotly_fnc=px.pie,
        icon="🥧",
        base_column_args=["names", "values"],
        extra_column_args=["color"],
    ),
    "Histogram": ChartDefinition(
        name="Histogram",
        plotly_fnc=px.histogram,
        icon="📊",
        extra_column_args=["color"],
        additional_params=["nbins"],
    ),
}


def get_all_supported_plotly_args() -> Dict[str, List[str]]:
    """Get all supported plotly args based on all supported charts definitions."""
    base_columns_args = {
        arg for c in AVAILABLE_CHARTS.values() for arg in c.base_column_args
    }
    extra_columns_args = {
        arg for c in AVAILABLE_CHARTS.values() for arg in c.extra_column_args
    }
    additional_params_args = {
        arg for c in AVAILABLE_CHARTS.values() for arg in c.additional_params
    }
    return {
        "base_columns": list(base_columns_args),
        "extra_columns": list(extra_columns_args),
        "additional_params": list(additional_params_args),
    }


def plotly_fig_from_config(df: pd.DataFrame, cfg: ChartConfigDict) -> Figure:
    """
    Generate a Plotly figure based on the provided configuration.

    This function takes a DataFrame and a configuration dictionary, extracts the chart type and parameters,
    and uses the corresponding Plotly function to generate the chart.

    Args:
        df (pd.DataFrame): The DataFrame containing the data to be plotted.
        cfg (Dict): A dictionary containing the chart configuration, including the chart type and parameters.

    Returns:
       Figure: The generated Plotly figure.
    """
    chart_name = cfg["type"]
    plt_args = cfg["params"].copy()
    plt_args["data_frame"] = df
    chart_cfg = AVAILABLE_CHARTS[chart_name]
    return chart_cfg.plotly_fnc(**plt_args)
