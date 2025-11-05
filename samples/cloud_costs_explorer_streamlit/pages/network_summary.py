"""Network cost summary and analysis page."""

from __future__ import annotations

from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import plotly.express as px
import streamlit as st

# Import functions from shared modules to match cpc.py style
try:
    from shared_modules.dashboard_functions import (
        format_subcat_change as shared_format_subcat_change,
        plotly_stacked_bar,
    )
except ImportError:
    # Fallback if shared modules not available
    plotly_stacked_bar = None
    shared_format_subcat_change = None


# Helper function to aggregate data by granularity
def aggregate_by_granularity(df: pd.DataFrame, granularity: str) -> pd.DataFrame:
    """Aggregate daily data to the specified granularity."""
    df = df.copy()
    df["date"] = pd.to_datetime(df["date"])

    # Create period grouping based on granularity
    if granularity == "Daily":
        df["period"] = df["date"]
    elif granularity == "Weekly":
        df["period"] = df["date"].dt.to_period("W").dt.start_time
    elif granularity == "Monthly":
        df["period"] = df["date"].dt.to_period("M").dt.start_time
    elif granularity == "Quarterly":
        df["period"] = df["date"].dt.to_period("Q").dt.start_time
    else:
        df["period"] = df["date"]  # Default to daily

    # Group by period and other dimensions, then aggregate
    group_cols = [
        "period",
        "network_cost_subcategory",
        "network_category",
        "region",
        "cloud",
        "traffic_type",
        "deployment",
    ]

    aggregated_df = (
        df.groupby(group_cols)
        .agg(
            {
                "cost": "sum",
                "normalized_cost": "sum",
                "gb_transferred": "sum",
                "tb_transferred": "sum",
                "cost_per_tb": "mean",
            }
        )
        .reset_index()
    )

    # Rename period back to date for consistency
    aggregated_df["date"] = aggregated_df["period"]
    aggregated_df = aggregated_df.drop("period", axis=1)
    aggregated_df["granularity"] = granularity

    return aggregated_df


st.title("Network Cost Summary")

# ----------------------------------------------------------------------------- #
#### DEFINE SESSION STATE DEFAULTS ####
if "network_lookback_select" not in st.session_state:
    st.session_state.network_lookback_select = (
        540  # Default value - show all available data (6 quarters)
    )

if "network_granularity_select" not in st.session_state:
    st.session_state.network_granularity_select = "Monthly"

if "network_normalized_select" not in st.session_state:
    st.session_state.network_normalized_select = "Raw"


# ----------------------------------------------------------------------------- #
#### HELPER FUNCTIONS ####
def subcategory_detail_prep(df: pd.DataFrame, metric_type: str, growth_type: str, metric_col: str):
    """Create detailed breakdown table for network buckets (adapted from original cpc.py)"""
    # Create list of subcategories sorted by metric in latest period
    period_list = sorted(df["date"].unique())
    if not period_list:
        st.warning("No data available for detailed analysis")
        return

    # Determine metric column to sort by
    sort_col = "cptt" if metric_type == "CPTT" else "total_cost"
    df_max_period = df[df["date"] == period_list[-1]].sort_values([sort_col], ascending=False)
    sub_cat_list = list(df_max_period[metric_col].unique())

    # Create detailed breakdown table
    sub_cat_df = pd.DataFrame()

    for sub_cat in sub_cat_list:
        temp_df = pd.DataFrame()
        temp_df2 = df[df[metric_col] == sub_cat].copy()

        # Calculate growth metrics
        if metric_type == "CPTT":
            temp_df2["cptt"] = temp_df2["display_cost"] / temp_df2["tb_transferred"]
            temp_df2["prior_cptt"] = temp_df2.groupby(metric_col)["cptt"].shift(1)
            temp_df2["cptt_growth_amount"] = temp_df2["cptt"] - temp_df2["prior_cptt"]
            temp_df2["cptt_growth_percent"] = temp_df2["cptt"] / temp_df2["prior_cptt"] - 1
        else:
            temp_df2["prior_cost"] = temp_df2.groupby(metric_col)["total_cost"].shift(1)
            temp_df2["cost_growth_amount"] = temp_df2["total_cost"] - temp_df2["prior_cost"]
            temp_df2["cost_growth_percent"] = temp_df2["total_cost"] / temp_df2["prior_cost"] - 1

        temp_df["Sub Category"] = [sub_cat, ""]
        temp_df["Metric"] = [f"{metric_type}", f"     {metric_type} Growth {growth_type}"]

        for _, row in temp_df2.iterrows():
            period = row["date"].strftime("%Y-%m-%d")

            if metric_type == "Total Cost":
                metric = f"${row['total_cost']:,.0f}"
                change_percent = row.get("cost_growth_percent", 0)
                if growth_type == "($)":
                    growth_val = row.get("cost_growth_amount", 0)
                    if not pd.isna(growth_val):
                        symbol = "+" if growth_val >= 0 else "-"
                        change = f"{symbol}${abs(growth_val):,.0f}"
                    else:
                        change = "$0"
                else:
                    if not pd.isna(change_percent):
                        change = f"{change_percent*100:+.1f}"
                    else:
                        change = "0.0"
                        change_percent = 0
                # Apply color formatting
                change = format_subcat_change(change, change_percent, metric_type, growth_type)
            else:  # CPTT
                cptt_val = row.get("cptt", 0)
                metric = f"${cptt_val:.3f}" if not pd.isna(cptt_val) else "$0.000"
                change_percent = row.get("cptt_growth_percent", 0)
                if growth_type == "($)":
                    growth_val = row.get("cptt_growth_amount", 0)
                    if not pd.isna(growth_val):
                        symbol = "+" if growth_val >= 0 else "-"
                        change = f"{symbol}${abs(growth_val):.3f}"
                    else:
                        change = "$0.000"
                else:
                    if not pd.isna(change_percent):
                        change = f"{change_percent*100:+.1f}"
                    else:
                        change = "0.0"
                        change_percent = 0
                # Apply color formatting
                change = format_subcat_change(change, change_percent, metric_type, growth_type)

            temp_df[period] = [metric, change]

        sub_cat_df = pd.concat([sub_cat_df, temp_df], ignore_index=True)

    # Display the table
    if not sub_cat_df.empty:
        df_height = len(sub_cat_df) * 35 + 35
        df_height = min(df_height, 600)  # Cap at 600px
        st.dataframe(
            sub_cat_df.set_index("Sub Category"), use_container_width=True, height=df_height
        )
    else:
        st.warning("No data available for the selected network bucket")


# Color formatting function for growth indicators (matches cpc.py formatting)
def format_subcat_change(change, change_percent, metric_type, growth_type):
    """Format change values with color-coded symbols matching cpc.py style."""
    # Use shared function if available, otherwise use local implementation
    if shared_format_subcat_change is not None:
        # Convert metric_type to match shared function expectations
        shared_metric_type = "Cost" if metric_type == "Total Cost" else metric_type
        return shared_format_subcat_change(change, change_percent, shared_metric_type, growth_type)

    # Local fallback implementation
    if metric_type == "Total Cost":
        color_bounds = [0.1, 0.01, 0, -0.01, -0.1]
    else:  # CPTT
        color_bounds = [0.1, 0.05, 0, -0.05, -0.1]

    if change_percent >= color_bounds[0]:
        symbol = "🔴"  # Red for high positive changes (bad)
    elif change_percent > color_bounds[1] and change_percent < color_bounds[0]:
        symbol = "🟡"  # Yellow for moderate positive changes
    elif change_percent > color_bounds[3] and change_percent < color_bounds[1]:
        symbol = "🔘"  # White/gray for neutral changes
    else:
        symbol = "🟢"  # Green for negative changes (good)

    if growth_type == "($)":
        formatted_change = f"{symbol} {change}"
    else:
        formatted_change = f"{symbol} {change}"
    return formatted_change


# Define all network categories (buckets) and cost subcategories
network_categories = ["External Traffic", "Infrastructure", "Replication", "Data Services"]
network_cost_subcategories = [
    "Data Egress",
    "Disaster Recovery",
    "VPC",
    "Load Balancer",
    "Network Traffic",
    "Data Connections",
    "DB Replication",
    "Storage Replication",
    "APIs",
    "Ingestion",
    "CDNs",
]


# Sample data generation for demo
@st.cache_data
def get_network_data() -> pd.DataFrame:
    """Generate sample network cost data."""
    dates = pd.date_range(end=datetime.now(), periods=540, freq="D")  # 18 months for 6 quarters
    regions = ["us-east-1", "us-west-2", "eu-west-1", "ap-southeast-1"]
    clouds = ["AWS", "GCP", "Azure"]
    traffic_types = [
        "Data Transfer Out",
        "Data Transfer In",
        "CloudFront",
        "NAT Gateway",
        "Load Balancer",
        "VPC Endpoints",
    ]
    deployments = ["Production", "Development", "Staging"]

    # Network category mapping for logical grouping
    network_category_mapping = {
        "Data Egress": "External Traffic",
        "Network Traffic": "External Traffic",
        "APIs": "External Traffic",
        "CDNs": "External Traffic",
        "VPC": "Infrastructure",
        "Load Balancer": "Infrastructure",
        "DB Replication": "Replication",
        "Storage Replication": "Replication",
        "Disaster Recovery": "Data Services",
        "Data Connections": "Data Services",
        "Ingestion": "Data Services",
    }

    ### DUMMY DATA FOR DEMO ###

    data = []
    for date in dates:
        for network_category in network_categories:  # All bucket categories
            for (
                network_subcategory
            ) in network_cost_subcategories:  # All subcategories in each bucket
                for region in regions:
                    for cloud in clouds:
                        # Different cost patterns for different network subcategories
                        cost_multipliers = {
                            "Data Egress": 3.0,
                            "Disaster Recovery": 2.5,
                            "VPC": 1.0,
                            "Load Balancer": 1.5,
                            "Network Traffic": 2.8,
                            "Data Connections": 1.8,
                            "DB Replication": 2.2,
                            "Storage Replication": 2.0,
                            "APIs": 1.2,
                            "Ingestion": 1.6,
                            "CDNs": 2.3,
                        }

                        # Category-based cost adjustments
                        category_multipliers = {
                            "External Traffic": 1.3,  # Higher costs for external traffic
                            "Infrastructure": 1.0,  # Baseline infrastructure costs
                            "Replication": 1.1,  # Slight premium for replication
                            "Data Services": 1.2,  # Premium for data services
                        }

                        base_cost = (
                            100
                            * cost_multipliers.get(network_subcategory, 1.0)
                            * category_multipliers.get(network_category, 1.0)
                        )
                        cost = (
                            base_cost
                            * np.random.uniform(0.7, 1.3)
                            * (1 + 0.1 * np.sin(date.dayofyear / 365 * 2 * np.pi))
                        )

                        # Different transfer patterns for different network subcategories
                        if network_subcategory in ["Data Egress", "Network Traffic", "CDNs"]:
                            gb_transferred = np.random.uniform(5000, 50000)  # High transfer volumes
                        elif network_subcategory in ["VPC", "Load Balancer"]:
                            gb_transferred = np.random.uniform(
                                1000, 10000
                            )  # Medium transfer volumes
                        else:
                            gb_transferred = np.random.uniform(500, 5000)  # Lower transfer volumes

                        # Category-based transfer adjustments
                        if network_category == "External Traffic":
                            gb_transferred *= 1.5  # Higher transfer volumes for external traffic
                        elif network_category == "Replication":
                            gb_transferred *= 0.8  # Lower volumes for replication

                        tb_transferred = gb_transferred / 1024  # Convert to TB

                        # Normalized cost (accounting for different month lengths)
                        # Simple approach: use the same cost for normalized view
                        normalized_cost = cost

                        data.append(
                            {
                                "date": date,
                                "network_cost_subcategory": network_subcategory,
                                "network_category": network_category,
                                "region": region,
                                "cloud": cloud,
                                "traffic_type": np.random.choice(traffic_types),
                                "deployment": np.random.choice(deployments),
                                "cost": cost,
                                "normalized_cost": normalized_cost,
                                "gb_transferred": gb_transferred,
                                "tb_transferred": tb_transferred,
                                "cost_per_tb": cost / tb_transferred,
                                "granularity": "Daily",
                            }
                        )

    ### END DUMMY DATA FOR DEMO ###

    return pd.DataFrame(data)


# Load data
df = get_network_data()

# ----------------------------------------------------------------------------- #
#### CREATE SIDEBAR FILTERS ####
with st.sidebar:
    st.write("**Filters**")

    granularity_select = st.selectbox(
        "Granularity",
        options=["Quarterly", "Monthly", "Weekly", "Daily"],
        key="network_granularity_select",
    )

    # Lookback window with units based on granularity
    if granularity_select == "Daily":
        lookback_label = "Lookback window (days)"
        lookback_min = 1
        lookback_max = 540
        default_lookback = min(st.session_state.network_lookback_select, lookback_max)
    elif granularity_select == "Weekly":
        lookback_label = "Lookback window (weeks)"
        lookback_min = 1
        lookback_max = 77  # ~18 months of weeks
        default_lookback = min(st.session_state.network_lookback_select // 7, lookback_max)
    elif granularity_select == "Monthly":
        lookback_label = "Lookback window (months)"
        lookback_min = 1
        lookback_max = 18  # 18 months for 6 quarters
        default_lookback = min(st.session_state.network_lookback_select // 30, lookback_max)
    else:  # Quarterly
        lookback_label = "Lookback window (quarters)"
        lookback_min = 1
        lookback_max = 6  # 6 quarters
        default_lookback = min(st.session_state.network_lookback_select // 90, lookback_max)

    new_lookback = st.number_input(
        lookback_label,
        value=max(default_lookback, lookback_min),
        min_value=lookback_min,
        max_value=lookback_max,
    )

    if st.button("Run", key="network_update"):
        # Convert to days for storage
        if granularity_select == "Daily":
            lookback_days = new_lookback
        elif granularity_select == "Weekly":
            lookback_days = new_lookback * 7
        elif granularity_select == "Monthly":
            lookback_days = new_lookback * 30
        else:  # Quarterly
            lookback_days = new_lookback * 90

        st.session_state.network_lookback_select = lookback_days
        st.rerun()

    st.markdown("---")

    # Value type selection
    value_type_select = st.selectbox("Select Total Cost or CPTT", options=["CPTT", "Total Cost"])

    # Normalized vs raw cost
    normalized_select = st.selectbox(
        "Select Raw or Normalized Cost",
        options=["Normalized", "Raw"],
        key="network_normalized_select",
        help="Normalized costs account for different month lengths to show accurate trends",
    )

# ----------------------------------------------------------------------------- #
#### APPLY FILTERS TO DATA ####

# Apply lookback window first (on daily data)
# The lookback is already in days from the sidebar conversion
adjusted_lookback = st.session_state.network_lookback_select
cutoff_date = df["date"].max() - timedelta(days=adjusted_lookback)
df_filtered = df[df["date"] >= cutoff_date].copy()

# Aggregate data by selected granularity
df_filtered = aggregate_by_granularity(df_filtered, granularity_select)

# Select cost column based on normalization choice
cost_column = "normalized_cost" if normalized_select == "Normalized" else "cost"
df_filtered["display_cost"] = df_filtered[cost_column]

# Display info about current aggregation
if granularity_select != "Daily":
    # Show lookback in appropriate units
    if granularity_select == "Weekly":
        lookback_display = f"{adjusted_lookback // 7} weeks ({adjusted_lookback} days)"
    elif granularity_select == "Monthly":
        lookback_display = f"{adjusted_lookback // 30} months ({adjusted_lookback} days)"
    elif granularity_select == "Quarterly":
        lookback_display = f"{adjusted_lookback // 90} quarters ({adjusted_lookback} days)"
    else:
        lookback_display = f"{adjusted_lookback} days"

    st.info(
        f"Data is aggregated by {granularity_select.lower()} periods. Using {lookback_display} lookback for visualization."
    )

# ----------------------------------------------------------------------------- #
#### APPLY FILTERS TO CREATE NETWORK BUCKET DATAFRAME ####
# Create network bucket data using broader categories as subcategories
network_bucket_df = df_filtered.copy()
network_bucket_df["network_bucket"] = network_bucket_df[
    "network_category"
]  # Use the network category as the bucket
network_bucket_df["cptt_cost"] = network_bucket_df[
    "display_cost"
]  # Cost per transferred terabyte calculation
network_bucket_df["total_cost"] = network_bucket_df["display_cost"]

# Group by date and network_bucket (category) to match the original cpc.py structure
network_bucket_df = (
    network_bucket_df.groupby(["date", "network_bucket", "cloud"])
    .agg(
        {
            "display_cost": "sum",
            "total_cost": "sum",
            "cptt_cost": "sum",
            "tb_transferred": "sum",
            "cost_per_tb": "mean",
        }
    )
    .reset_index()
)

# ----------------------------------------------------------------------------- #
#### COST PER TRANSFERRED TERABYTE BY SUBCATEGORY ####
with st.container():
    c1, c2 = st.columns([1, 0.5])
    with c1:
        st.subheader("Trended CPTT by Subcategory")
    with c2:
        cloud_options = ["All"] + sorted(network_bucket_df["cloud"].unique(), reverse=False)
        cloud_select = st.selectbox(
            "Select Cloud To View",
            options=cloud_options,
            index=0,  # Default to "All"
            key="main_cloud_select",
        )

        if cloud_select != "All":
            network_bucket_df = network_bucket_df[network_bucket_df["cloud"] == cloud_select]

# Recreate plotly_stacked_bar functionality for Cost Per Transferred Terabyte
if not network_bucket_df.empty:
    # Calculate cost per transferred terabyte (CPTT)
    network_bucket_df["cptt"] = (
        network_bucket_df["display_cost"] / network_bucket_df["tb_transferred"]
    )

    # Prepare data for stacked bar chart
    chart_data = (
        network_bucket_df.groupby(["date", "network_bucket"])
        .agg({"cptt": "mean", "display_cost": "sum", "tb_transferred": "sum"})
        .reset_index()
    )

    # Use the plotly_stacked_bar function for consistent styling with cpc.py
    if plotly_stacked_bar is not None:
        plotly_stacked_bar(
            df=chart_data,
            y_axis="cptt",
            y_axis_label="Cost Per Transferred Terabyte",
            color_col="network_bucket",
            color_label="Network Bucket",
            text_size=15,
            hide_legend=False,
            use_tooltip=True,
            round_values=False,
            granularity_select=granularity_select,
        )
    else:
        # Fallback to basic chart if shared modules not available
        fig = px.bar(
            chart_data,
            x="date",
            y="cptt",
            color="network_bucket",
            title="Cost Per Transferred Terabyte",
            labels={
                "cptt": "Cost Per Transferred Terabyte",
                "date": "Date",
                "network_bucket": "Network Bucket",
            },
        )
        fig.update_layout(height=500)
        st.plotly_chart(fig, use_container_width=True)

with st.expander("Expand Network Category Detail"):
    c1, c2 = st.columns(2)
    with c1:
        subcat_metric_select = st.selectbox(
            "Select whether you would like to view CPTT or Cost", options=["CPTT", "Total Cost"]
        )
    with c2:
        subcat_growth_select = st.selectbox(
            "Select whether you would like to view absolute or percent growth",
            options=["($)", "(%)"],
        )
    subcategory_detail_prep(
        network_bucket_df, subcat_metric_select, subcat_growth_select, "network_bucket"
    )
st.markdown("------------")

# ----------------------------------------------------------------------------- #
#### CREATE VALUE TYPE LIST ####
period_list = sorted(df_filtered["date"].unique(), reverse=True)

network_max_period = network_bucket_df[network_bucket_df["date"] == period_list[0]].sort_values(
    "display_cost", ascending=False
)
sub_cat_list = network_max_period["network_bucket"].unique()

# ----------------------------------------------------------------------------- #
#### CHART LOOP ####
sub_cat_select = st.selectbox("Choose Network Bucket", options=sub_cat_list)

# network_bucket is already in the data as network_category
df_filtered_with_bucket = df_filtered.copy()
df_filtered_with_bucket["network_bucket"] = df_filtered_with_bucket["network_category"]

# Filter by selected network bucket
filtered_df = df_filtered_with_bucket[df_filtered_with_bucket["network_bucket"] == sub_cat_select]

c1, c3 = st.columns([4, 6])
with c1:
    st.title(sub_cat_select)
with c3:
    # Chart series selection above the filters
    series_list = ["Cost Class", "Deployment"]
    series_select_display = st.selectbox("Select Chart Series", series_list)

    # Map display names to actual column names
    if series_select_display == "Cost Class":
        series_select = (
            "network_cost_subcategory"  # Show individual network types within the bucket
        )
    else:  # Deployment
        series_select = "deployment"

    with st.expander("Expand to Apply Filters"):
        c1, c2 = st.columns(2)
        with c1:
            # Cloud filter (multiselect_and_filter equivalent)
            cloud_options = sorted(filtered_df["cloud"].unique())
            selected_clouds = st.multiselect("Select Cloud", cloud_options, default=cloud_options)
            if selected_clouds:
                filtered_df = filtered_df[filtered_df["cloud"].isin(selected_clouds)]

            # Deployment filter
            deployment_options = sorted(filtered_df["deployment"].unique())
            selected_deployments = st.multiselect(
                "Select Deployment", deployment_options, default=deployment_options
            )
            if selected_deployments:
                filtered_df = filtered_df[filtered_df["deployment"].isin(selected_deployments)]

        with c2:
            # Cost class filter (use all available network cost subcategories when Cost Class is selected)
            if series_select_display == "Cost Class":
                # Get available network cost subcategories for the selected bucket
                available_cost_subcategories = sorted(
                    filtered_df["network_cost_subcategory"].unique()
                )
                selected_cost_subcategories = st.multiselect(
                    "Select Cost Class",
                    available_cost_subcategories,
                    default=available_cost_subcategories,
                )
                if selected_cost_subcategories:
                    filtered_df = filtered_df[
                        filtered_df["network_cost_subcategory"].isin(selected_cost_subcategories)
                    ]
            else:
                # For Deployment view, still show traffic_type filter
                traffic_type_options = sorted(filtered_df["traffic_type"].unique())
                selected_traffic_types = st.multiselect(
                    "Select Traffic Type", traffic_type_options, default=traffic_type_options
                )
                if selected_traffic_types:
                    filtered_df = filtered_df[
                        filtered_df["traffic_type"].isin(selected_traffic_types)
                    ]

terabytes_df_pivot = filtered_df.groupby(["date", "cloud"])["tb_transferred"].mean().reset_index()
terabytes_df_pivot = terabytes_df_pivot.groupby("date")["tb_transferred"].sum().reset_index()

filtered_df_pivot = (
    filtered_df.groupby(["date", "cloud", series_select])
    .agg({"display_cost": "sum", "tb_transferred": "sum"})
    .reset_index()
)

filtered_df_pivot = (
    filtered_df_pivot.groupby(["date", series_select])
    .agg({"display_cost": "sum", "tb_transferred": "sum"})
    .reset_index()
)

filtered_df_pivot = pd.merge(filtered_df_pivot, terabytes_df_pivot, on="date", how="left")

# Calculate CPTT and growth metrics
filtered_df_pivot["cptt"] = (
    filtered_df_pivot["display_cost"] / filtered_df_pivot["tb_transferred_x"]
)
filtered_df_pivot["prior_cost"] = filtered_df_pivot.groupby(series_select)["display_cost"].shift(1)
filtered_df_pivot["prior_cptt"] = filtered_df_pivot.groupby(series_select)["cptt"].shift(1)

filtered_df_pivot["cost_growth_percent"] = (
    filtered_df_pivot["display_cost"] / filtered_df_pivot["prior_cost"] - 1
)
filtered_df_pivot["cptt_growth_percent"] = (
    filtered_df_pivot["cptt"] / filtered_df_pivot["prior_cptt"] - 1
)

filtered_df_pivot["cost_growth_amount"] = (
    filtered_df_pivot["display_cost"] - filtered_df_pivot["prior_cost"]
)
filtered_df_pivot["cptt_growth_amount"] = (
    filtered_df_pivot["cptt"] - filtered_df_pivot["prior_cptt"]
)
filtered_df_pivot = filtered_df_pivot.fillna(0).sort_values(
    ["date", "display_cost"], ascending=[1, 0]
)

# Create charts (adapted from cpc_charts_stacked functionality)
if not filtered_df_pivot.empty:
    # Create charts based on value type selection
    if value_type_select == "Total Cost":
        chart_data = filtered_df_pivot.copy()
        chart_col = "display_cost"
        chart_title = f"Total cost for {sub_cat_select}"
    else:  # CPTT
        chart_data = filtered_df_pivot.copy()
        chart_col = "cptt"
        chart_title = f"CPTT for {sub_cat_select}"

    # Create charts based on value type selection
    if value_type_select == "Total Cost":
        y_axis_label = "Total Cost"
    else:  # CPTT
        y_axis_label = "CPTT"

    # Use the same plotly_stacked_bar function as the first graph for consistent styling
    if plotly_stacked_bar is not None:
        plotly_stacked_bar(
            df=chart_data,
            y_axis=chart_col,
            y_axis_label=y_axis_label,
            color_col=series_select,
            color_label=series_select_display,
            text_size=15,
            hide_legend=False,
            use_tooltip=True,
            round_values=False,
            granularity_select=granularity_select,
        )
    else:
        # Fallback to basic chart if shared modules not available
        fig_stacked = px.bar(
            chart_data, x="date", y=chart_col, color=series_select, title=chart_title
        )
        fig_stacked.update_layout(
            xaxis_title="Date",
            yaxis_title="Cost ($)" if chart_col == "display_cost" else "CPTT ($)",
            legend_title=series_select_display,
            height=500,
        )
        st.plotly_chart(fig_stacked, use_container_width=True)

    # This matches the cpc_charts_stacked call from original cpc.py
    # cptt_charts_stacked(
    #     sub_cat_select, filtered_df_pivot, value_type_select, series_select, True, granularity_select
    # )

    # ----------------------------------------------------------------------------- #
    #### NETWORK BUCKET DETAIL SECTION ####
    st.markdown("---")
    with st.expander("Expand Network Bucket Detail"):
        c1, c2 = st.columns(2)
        with c1:
            subcat_metric_select = st.selectbox(
                "Select whether you would like to view CPTT or Cost",
                options=["CPTT", "Total Cost"],
                key="bucket_metric",
            )
        with c2:
            subcat_growth_select = st.selectbox(
                "Select whether you would like to view absolute or percent growth",
                options=["($)", "(%)"],
                key="bucket_growth",
            )
        # Prepare data for subcategory detail function
        detail_prep_df = filtered_df.copy()

        # Add required columns for subcategory_detail_prep function
        detail_prep_df["total_cost"] = detail_prep_df["display_cost"]
        detail_prep_df["cptt_cost"] = detail_prep_df["display_cost"]
        detail_prep_df["cptt"] = detail_prep_df["display_cost"] / detail_prep_df["tb_transferred"]

        # Group by date and network_cost_subcategory to match expected structure
        detail_prep_df = (
            detail_prep_df.groupby(["date", "network_cost_subcategory"])
            .agg(
                {
                    "display_cost": "sum",
                    "total_cost": "sum",
                    "cptt_cost": "sum",
                    "tb_transferred": "sum",
                    "cptt": "mean",
                }
            )
            .reset_index()
        )

        # Recalculate CPTT after aggregation
        detail_prep_df["cptt"] = detail_prep_df["display_cost"] / detail_prep_df["tb_transferred"]

        subcategory_detail_prep(
            detail_prep_df, subcat_metric_select, subcat_growth_select, "network_cost_subcategory"
        )

else:
    st.warning("No data available for the selected period.")
