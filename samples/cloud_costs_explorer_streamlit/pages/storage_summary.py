"""Storage cost summary and analysis page."""

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
        "storage_cost_class",
        "storage_category",
        "region",
        "cloud",
        "storage_type",
        "deployment",
    ]

    aggregated_df = (
        df.groupby(group_cols)
        .agg(
            {
                "cost": "sum",
                "normalized_cost": "sum",
                "size_gb": "sum",
                "size_tb": "sum",
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


st.title("Storage Cost Summary")

# ----------------------------------------------------------------------------- #
#### DEFINE SESSION STATE DEFAULTS ####
if "storage_lookback_select" not in st.session_state:
    st.session_state.storage_lookback_select = (
        540  # Default value - show all available data (6 quarters)
    )

if "storage_granularity_select" not in st.session_state:
    st.session_state.storage_granularity_select = "Monthly"

if "storage_normalized_select" not in st.session_state:
    st.session_state.storage_normalized_select = "Raw"


# ----------------------------------------------------------------------------- #
#### HELPER FUNCTIONS ####
def subcategory_detail_prep(df: pd.DataFrame, metric_type: str, growth_type: str, metric_col: str):
    """Create detailed breakdown table for storage buckets (adapted from original cpc.py)"""
    # Create list of subcategories sorted by metric in latest period
    period_list = sorted(df["date"].unique())
    if not period_list:
        st.warning("No data available for detailed analysis")
        return

    # Determine metric column to sort by
    sort_col = "cpt" if metric_type == "CPT" else "total_cost"
    df_max_period = df[df["date"] == period_list[-1]].sort_values([sort_col], ascending=False)
    sub_cat_list = list(df_max_period[metric_col].unique())

    # Create detailed breakdown table
    sub_cat_df = pd.DataFrame()

    for sub_cat in sub_cat_list:
        temp_df = pd.DataFrame()
        temp_df2 = df[df[metric_col] == sub_cat].copy()

        # Calculate growth metrics
        if metric_type == "CPT":
            temp_df2["cpt"] = temp_df2["display_cost"] / temp_df2["size_tb"]
            temp_df2["prior_cpt"] = temp_df2.groupby(metric_col)["cpt"].shift(1)
            temp_df2["cpt_growth_amount"] = temp_df2["cpt"] - temp_df2["prior_cpt"]
            temp_df2["cpt_growth_percent"] = temp_df2["cpt"] / temp_df2["prior_cpt"] - 1
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
            else:  # CPT
                cpt_val = row.get("cpt", 0)
                metric = f"${cpt_val:.3f}" if not pd.isna(cpt_val) else "$0.000"
                change_percent = row.get("cpt_growth_percent", 0)
                if growth_type == "($)":
                    growth_val = row.get("cpt_growth_amount", 0)
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
        st.warning("No data available for the selected storage bucket")


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
    else:  # CPT
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


# Define all storage categories (buckets) and cost classes
storage_categories = ["Standard", "Infrequent Access", "Intelligent Tiering", "Tags"]
storage_cost_classes = [
    "Logs",
    "Operational Data",
    "Early Retrieval",
    "Early Deletion",
    "Hybrid Tables",
    "Jenkins Storage",
    "Home",
    "Temp",
    "Stage",
    "Replication",
    "AI/ML Storage",
]


# Sample data generation for demo
@st.cache_data
def get_storage_data() -> pd.DataFrame:
    """Generate sample storage cost data."""
    dates = pd.date_range(end=datetime.now(), periods=540, freq="D")  # 18 months for 6 quarters
    regions = ["us-east-1", "us-west-2", "eu-west-1"]
    clouds = ["AWS", "GCP", "Azure"]
    storage_types = ["S3 Standard", "S3 IA", "S3 Glacier", "EBS gp3", "EBS gp2", "EFS"]
    deployments = ["Production", "Development", "Staging"]

    ### DUMMY DATA FOR DEMO ###

    data = []
    for date in dates:
        for storage_category in storage_categories:  # All bucket categories
            for storage_class in storage_cost_classes:  # All cost classes in each bucket
                for region in regions:
                    for cloud in clouds:
                        # Different cost patterns for different storage classes
                        cost_multipliers = {
                            "Logs": 2.5,
                            "Operational Data": 3.0,
                            "Early Retrieval": 1.2,
                            "Early Deletion": 0.8,
                            "Hybrid Tables": 4.0,
                            "Jenkins Storage": 1.5,
                            "Home": 1.0,
                            "Temp": 0.5,
                            "Stage": 1.8,
                            "Replication": 2.2,
                            "AI/ML Storage": 3.5,
                        }

                        # Category-based cost adjustments
                        category_multipliers = {
                            "Standard": 1.0,
                            "Infrequent Access": 0.7,  # Cheaper for infrequent access
                            "Intelligent Tiering": 1.2,  # Slightly more for intelligent features
                            "Tags": 1.1,  # Small premium for tagging features
                        }

                        base_cost = (
                            100
                            * cost_multipliers.get(storage_class, 1.0)
                            * category_multipliers.get(storage_category, 1.0)
                        )
                        cost = (
                            base_cost
                            * np.random.uniform(0.7, 1.3)
                            * (1 + 0.1 * np.sin(date.dayofyear / 365 * 2 * np.pi))
                        )

                        # Different size patterns for different storage classes
                        if storage_class in ["Logs", "Operational Data"]:
                            size_gb = np.random.uniform(
                                1000, 10000
                            )  # Larger sizes for operational data
                        elif storage_class in ["Temp", "Early Deletion"]:
                            size_gb = np.random.uniform(
                                100, 1000
                            )  # Smaller sizes for temporary data
                        else:
                            size_gb = np.random.uniform(500, 5000)  # Medium sizes for others

                        # Category-based size adjustments
                        if storage_category == "Infrequent Access":
                            size_gb *= 0.8  # Slightly smaller sizes for infrequent access
                        elif storage_category == "Tags":
                            size_gb *= 1.1  # Slightly larger for tagged storage

                        size_tb = size_gb / 1024  # Convert to TB

                        # Normalized cost (accounting for different month lengths)
                        # Simple approach: use the same cost for normalized view
                        normalized_cost = cost

                        data.append(
                            {
                                "date": date,
                                "storage_cost_class": storage_class,
                                "storage_category": storage_category,
                                "region": region,
                                "cloud": cloud,
                                "storage_type": np.random.choice(storage_types),
                                "deployment": np.random.choice(deployments),
                                "cost": cost,
                                "normalized_cost": normalized_cost,
                                "size_gb": size_gb,
                                "size_tb": size_tb,
                                "cost_per_tb": cost / size_tb,
                                "granularity": "Daily",
                            }
                        )

    ### END DUMMY DATA FOR DEMO ###

    return pd.DataFrame(data)


# Load data
df = get_storage_data()

# ----------------------------------------------------------------------------- #
#### CREATE SIDEBAR FILTERS ####
with st.sidebar:
    st.write("**Filters**")

    granularity_select = st.selectbox(
        "Granularity",
        options=["Quarterly", "Monthly", "Weekly", "Daily"],
        key="storage_granularity_select",
    )

    # Lookback window with units based on granularity
    if granularity_select == "Daily":
        lookback_label = "Lookback window (days)"
        lookback_min = 1
        lookback_max = 540
        default_lookback = min(st.session_state.storage_lookback_select, lookback_max)
    elif granularity_select == "Weekly":
        lookback_label = "Lookback window (weeks)"
        lookback_min = 1
        lookback_max = 77  # ~18 months of weeks
        default_lookback = min(st.session_state.storage_lookback_select // 7, lookback_max)
    elif granularity_select == "Monthly":
        lookback_label = "Lookback window (months)"
        lookback_min = 1
        lookback_max = 18  # 18 months for 6 quarters
        default_lookback = min(st.session_state.storage_lookback_select // 30, lookback_max)
    else:  # Quarterly
        lookback_label = "Lookback window (quarters)"
        lookback_min = 1
        lookback_max = 6  # 6 quarters
        default_lookback = min(st.session_state.storage_lookback_select // 90, lookback_max)

    new_lookback = st.number_input(
        lookback_label,
        value=max(default_lookback, lookback_min),
        min_value=lookback_min,
        max_value=lookback_max,
    )

    if st.button("Run", key="storage_update"):
        # Convert to days for storage
        if granularity_select == "Daily":
            lookback_days = new_lookback
        elif granularity_select == "Weekly":
            lookback_days = new_lookback * 7
        elif granularity_select == "Monthly":
            lookback_days = new_lookback * 30
        else:  # Quarterly
            lookback_days = new_lookback * 90

        st.session_state.storage_lookback_select = lookback_days
        st.rerun()

    st.markdown("---")

    # Value type selection
    value_type_select = st.selectbox("Select Total Cost or CPT", options=["CPT", "Total Cost"])

    # Normalized vs raw cost
    normalized_select = st.selectbox(
        "Select Raw or Normalized Cost",
        options=["Normalized", "Raw"],
        key="storage_normalized_select",
        help="Normalized costs account for different month lengths to show accurate trends",
    )

# ----------------------------------------------------------------------------- #
#### APPLY FILTERS TO DATA ####

# Apply lookback window first (on daily data)
# The lookback is already in days from the sidebar conversion
adjusted_lookback = st.session_state.storage_lookback_select
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
#### APPLY FILTERS TO CREATE STORAGE BUCKET DATAFRAME ####
# Create storage bucket data using broader categories as subcategories
storage_bucket_df = df_filtered.copy()
storage_bucket_df["storage_bucket"] = storage_bucket_df[
    "storage_category"
]  # Use the storage category as the bucket
storage_bucket_df["cpt_cost"] = storage_bucket_df["display_cost"]  # Cost per terabyte calculation
storage_bucket_df["total_cost"] = storage_bucket_df["display_cost"]

# Group by date and storage_bucket (category) to match the original cpc.py structure
storage_bucket_df = (
    storage_bucket_df.groupby(["date", "storage_bucket", "cloud"])
    .agg(
        {
            "display_cost": "sum",
            "total_cost": "sum",
            "cpt_cost": "sum",
            "size_tb": "sum",
            "cost_per_tb": "mean",
        }
    )
    .reset_index()
)

# ----------------------------------------------------------------------------- #
#### COST PER TERABYTE BY SUBCATEGORY ####
with st.container():
    c1, c2 = st.columns([1, 0.5])
    with c1:
        st.subheader("Trended CPT by Subcategory")
    with c2:
        cloud_options = ["All"] + sorted(storage_bucket_df["cloud"].unique(), reverse=False)
        cloud_select = st.selectbox(
            "Select Cloud To View",
            options=cloud_options,
            index=0,  # Default to "All"
            key="main_cloud_select",
        )

        if cloud_select != "All":
            storage_bucket_df = storage_bucket_df[storage_bucket_df["cloud"] == cloud_select]

# Recreate plotly_stacked_bar functionality for Cost Per Terabyte
if not storage_bucket_df.empty:
    # Calculate cost per terabyte (CPT)
    storage_bucket_df["cpt"] = storage_bucket_df["display_cost"] / storage_bucket_df["size_tb"]

    # Prepare data for stacked bar chart
    chart_data = (
        storage_bucket_df.groupby(["date", "storage_bucket"])
        .agg({"cpt": "mean", "display_cost": "sum", "size_tb": "sum"})
        .reset_index()
    )

    # Use the plotly_stacked_bar function for consistent styling with cpc.py
    if plotly_stacked_bar is not None:
        plotly_stacked_bar(
            df=chart_data,
            y_axis="cpt",
            y_axis_label="Cost Per Terabyte",
            color_col="storage_bucket",
            color_label="Storage Bucket",
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
            y="cpt",
            color="storage_bucket",
            title="Cost Per Terabyte",
            labels={"cpt": "Cost Per Terabyte", "date": "Date", "storage_bucket": "Storage Bucket"},
        )
        fig.update_layout(height=500)
        st.plotly_chart(fig, use_container_width=True)

with st.expander("Expand Storage Category Detail"):
    c1, c2 = st.columns(2)
    with c1:
        subcat_metric_select = st.selectbox(
            "Select whether you would like to view CPT or Cost", options=["CPT", "Total Cost"]
        )
    with c2:
        subcat_growth_select = st.selectbox(
            "Select whether you would like to view absolute or percent growth",
            options=["($)", "(%)"],
        )
    subcategory_detail_prep(
        storage_bucket_df, subcat_metric_select, subcat_growth_select, "storage_bucket"
    )
st.markdown("------------")

# ----------------------------------------------------------------------------- #
#### CREATE VALUE TYPE LIST ####
period_list = sorted(df_filtered["date"].unique(), reverse=True)

storage_max_period = storage_bucket_df[storage_bucket_df["date"] == period_list[0]].sort_values(
    "display_cost", ascending=False
)
sub_cat_list = storage_max_period["storage_bucket"].unique()

# ----------------------------------------------------------------------------- #
#### CHART LOOP ####
sub_cat_select = st.selectbox("Choose Storage Bucket", options=sub_cat_list)

# storage_bucket is already in the data as storage_category
df_filtered_with_bucket = df_filtered.copy()
df_filtered_with_bucket["storage_bucket"] = df_filtered_with_bucket["storage_category"]

# Filter by selected storage bucket
filtered_df = df_filtered_with_bucket[df_filtered_with_bucket["storage_bucket"] == sub_cat_select]

c1, c3 = st.columns([4, 6])
with c1:
    st.title(sub_cat_select)
with c3:
    # Chart series selection above the filters
    series_list = ["Cost Class", "Deployment"]
    series_select_display = st.selectbox("Select Chart Series", series_list)

    # Map display names to actual column names
    if series_select_display == "Cost Class":
        series_select = "storage_cost_class"  # Show individual storage types within the bucket
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
            # Cost class filter (use all available storage cost classes when Cost Class is selected)
            if series_select_display == "Cost Class":
                # Get available storage cost classes for the selected bucket
                available_cost_classes = sorted(filtered_df["storage_cost_class"].unique())
                selected_cost_classes = st.multiselect(
                    "Select Cost Class", available_cost_classes, default=available_cost_classes
                )
                if selected_cost_classes:
                    filtered_df = filtered_df[
                        filtered_df["storage_cost_class"].isin(selected_cost_classes)
                    ]
            else:
                # For Deployment view, still show storage_type filter
                storage_type_options = sorted(filtered_df["storage_type"].unique())
                selected_storage_types = st.multiselect(
                    "Select Storage Type", storage_type_options, default=storage_type_options
                )
                if selected_storage_types:
                    filtered_df = filtered_df[
                        filtered_df["storage_type"].isin(selected_storage_types)
                    ]


terabytes_df_pivot = filtered_df.groupby(["date", "cloud"])["size_tb"].mean().reset_index()
terabytes_df_pivot = terabytes_df_pivot.groupby("date")["size_tb"].sum().reset_index()

filtered_df_pivot = (
    filtered_df.groupby(["date", "cloud", series_select])
    .agg({"display_cost": "sum", "size_tb": "sum"})
    .reset_index()
)

filtered_df_pivot = (
    filtered_df_pivot.groupby(["date", series_select])
    .agg({"display_cost": "sum", "size_tb": "sum"})
    .reset_index()
)

filtered_df_pivot = pd.merge(filtered_df_pivot, terabytes_df_pivot, on="date", how="left")

# Calculate CPT and growth metrics
filtered_df_pivot["cpt"] = filtered_df_pivot["display_cost"] / filtered_df_pivot["size_tb_x"]
filtered_df_pivot["prior_cost"] = filtered_df_pivot.groupby(series_select)["display_cost"].shift(1)
filtered_df_pivot["prior_cpt"] = filtered_df_pivot.groupby(series_select)["cpt"].shift(1)

filtered_df_pivot["cost_growth_percent"] = (
    filtered_df_pivot["display_cost"] / filtered_df_pivot["prior_cost"] - 1
)
filtered_df_pivot["cpt_growth_percent"] = (
    filtered_df_pivot["cpt"] / filtered_df_pivot["prior_cpt"] - 1
)

filtered_df_pivot["cost_growth_amount"] = (
    filtered_df_pivot["display_cost"] - filtered_df_pivot["prior_cost"]
)
filtered_df_pivot["cpt_growth_amount"] = filtered_df_pivot["cpt"] - filtered_df_pivot["prior_cpt"]
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
    else:  # CPT
        chart_data = filtered_df_pivot.copy()
        chart_col = "cpt"
        chart_title = f"CPT for {sub_cat_select}"

    # Create charts based on value type selection
    if value_type_select == "Total Cost":
        y_axis_label = "Total Cost"
    else:  # CPT
        y_axis_label = "CPT"

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
            yaxis_title="Cost ($)" if chart_col == "display_cost" else "CPT ($)",
            legend_title=series_select_display,
            height=500,
        )
        st.plotly_chart(fig_stacked, use_container_width=True)

    # This matches the cpc_charts_stacked call from original cpc.py
    # cpt_charts_stacked(
    #     sub_cat_select, filtered_df_pivot, value_type_select, series_select, True, granularity_select
    # )

    # ----------------------------------------------------------------------------- #
    #### STORAGE BUCKET DETAIL SECTION ####
    st.markdown("---")
    with st.expander("Expand Storage Bucket Detail"):
        c1, c2 = st.columns(2)
        with c1:
            subcat_metric_select = st.selectbox(
                "Select whether you would like to view CPT or Cost",
                options=["CPT", "Total Cost"],
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
        detail_prep_df["cpt_cost"] = detail_prep_df["display_cost"]
        detail_prep_df["cpt"] = detail_prep_df["display_cost"] / detail_prep_df["size_tb"]

        # Group by date and storage_cost_class to match expected structure
        detail_prep_df = (
            detail_prep_df.groupby(["date", "storage_cost_class"])
            .agg(
                {
                    "display_cost": "sum",
                    "total_cost": "sum",
                    "cpt_cost": "sum",
                    "size_tb": "sum",
                    "cpt": "mean",
                }
            )
            .reset_index()
        )

        # Recalculate CPT after aggregation
        detail_prep_df["cpt"] = detail_prep_df["display_cost"] / detail_prep_df["size_tb"]

        subcategory_detail_prep(
            detail_prep_df, subcat_metric_select, subcat_growth_select, "storage_cost_class"
        )

else:
    st.warning("No data available for the selected period.")
