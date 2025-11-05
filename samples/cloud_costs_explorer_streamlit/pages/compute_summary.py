"""Compute cost summary and analysis page."""

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
        # Use the first day of each month for proper monthly grouping
        df["period"] = df["date"].dt.to_period("M").dt.start_time
    elif granularity == "Quarterly":
        df["period"] = df["date"].dt.to_period("Q").dt.start_time
    else:
        df["period"] = df["date"]  # Default to daily

    # Group by period and other dimensions, then aggregate
    group_cols = [
        "period",
        "compute_subcategory",
        "compute_category",
        "region",
        "cloud",
        "cost_class",
        "cost_sub_class",
        "deployment",
    ]

    aggregated_df = (
        df.groupby(group_cols)
        .agg({"cost": "sum", "normalized_cost": "sum", "hours": "sum", "cost_per_hour": "mean"})
        .reset_index()
    )

    # Rename period back to date for consistency
    aggregated_df["date"] = aggregated_df["period"]
    aggregated_df = aggregated_df.drop("period", axis=1)
    aggregated_df["granularity"] = granularity

    return aggregated_df


st.title("Compute Cost Summary")

# ----------------------------------------------------------------------------- #
#### DEFINE SESSION STATE DEFAULTS ####
if "compute_lookback_select" not in st.session_state:
    st.session_state.compute_lookback_select = (
        540  # Default value - show all available data (6 quarters)
    )

if "compute_granularity_select" not in st.session_state:
    st.session_state.compute_granularity_select = "Monthly"

if "compute_normalized_select" not in st.session_state:
    st.session_state.compute_normalized_select = "Raw"


# ----------------------------------------------------------------------------- #
#### HELPER FUNCTIONS ####
def subcategory_detail_prep(df: pd.DataFrame, metric_type: str, growth_type: str, metric_col: str):
    """Create detailed breakdown table for compute buckets (adapted from original cpc.py)"""
    # Create list of subcategories sorted by metric in latest period
    period_list = sorted(df["date"].unique())
    if not period_list:
        st.warning("No data available for detailed analysis")
        return

    # Determine metric column to sort by
    sort_col = "cph" if metric_type == "CPH" else "total_cost"
    df_max_period = df[df["date"] == period_list[-1]].sort_values([sort_col], ascending=False)
    sub_cat_list = list(df_max_period[metric_col].unique())

    # Create detailed breakdown table
    sub_cat_df = pd.DataFrame()

    for sub_cat in sub_cat_list:
        temp_df = pd.DataFrame()
        temp_df2 = df[df[metric_col] == sub_cat].copy()

        # Calculate growth metrics
        if metric_type == "CPH":
            temp_df2["cph"] = temp_df2["display_cost"] / temp_df2["hours"]
            temp_df2["prior_cph"] = temp_df2.groupby(metric_col)["cph"].shift(1)
            temp_df2["cph_growth_amount"] = temp_df2["cph"] - temp_df2["prior_cph"]
            temp_df2["cph_growth_percent"] = temp_df2["cph"] / temp_df2["prior_cph"] - 1
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
            else:  # CPH
                cph_val = row.get("cph", 0)
                metric = f"${cph_val:.3f}" if not pd.isna(cph_val) else "$0.000"
                change_percent = row.get("cph_growth_percent", 0)
                if growth_type == "($)":
                    growth_val = row.get("cph_growth_amount", 0)
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
        st.warning("No data available for the selected compute bucket")


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
    else:  # CPH
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


# Define compute category groupings (moved outside function for global access)
compute_category_mapping = {
    "Warehouse Compute": "Core Compute",
    "Database Compute": "Core Compute",
    "Kubernetes Compute": "Core Compute",
    "Container Instances": "Core Compute",
    "ML/AI Compute": "Advanced Compute",
    "Data Processing": "Advanced Compute",
    "API Calls": "API & Microservices",
    "Serverless Functions": "API & Microservices",
    "Queue Processing": "API & Microservices",
    "Storage Requests": "Storage & Cache",
    "Cache Operations": "Storage & Cache",
    "Traffic Processing": "Storage & Cache",
}


# Sample data generation for demo
@st.cache_data
def get_compute_data() -> pd.DataFrame:
    """Generate sample compute cost data."""
    dates = pd.date_range(end=datetime.now(), periods=540, freq="D")  # 18 months for 6 quarters
    # Updated to realistic compute subcategories
    compute_subcategories = [
        "Warehouse Compute",
        "API Calls",
        "Storage Requests",
        "Kubernetes Compute",
        "Traffic Processing",
        "Database Compute",
        "Serverless Functions",
        "Container Instances",
        "Data Processing",
        "ML/AI Compute",
        "Cache Operations",
        "Queue Processing",
    ]
    regions = ["us-east-1", "us-west-2", "eu-west-1"]
    clouds = ["AWS", "GCP", "Azure"]
    cost_classes = ["Compute", "Storage", "Network"]
    cost_sub_classes = ["Warehouse", "API", "Storage", "Kubernetes", "Serverless", "Database"]
    deployments = ["Production", "Development", "Staging"]

    data = []
    for date in dates:
        for compute_subcat in compute_subcategories:
            for region in regions:
                for cloud in clouds:
                    # Different cost patterns for different compute types
                    cost_multipliers = {
                        "Warehouse Compute": 2.5,
                        "API Calls": 0.3,
                        "Storage Requests": 0.1,
                        "Kubernetes Compute": 2.0,
                        "Traffic Processing": 1.2,
                        "Database Compute": 3.0,
                        "Serverless Functions": 0.8,
                        "Container Instances": 1.5,
                        "Data Processing": 2.2,
                        "ML/AI Compute": 4.0,
                        "Cache Operations": 0.5,
                        "Queue Processing": 0.7,
                    }

                    ### DUMMY DATA FOR DEMO ###

                    base_cost = 100 * cost_multipliers.get(compute_subcat, 1.0)
                    cost = (
                        base_cost
                        * np.random.uniform(0.7, 1.3)
                        * (1 + 0.1 * np.sin(date.dayofyear / 365 * 2 * np.pi))
                    )

                    # Different hour patterns for different compute types
                    if compute_subcat in ["API Calls", "Storage Requests"]:
                        hours = np.random.uniform(0.1, 2.0)  # Lower hours for API/requests
                    elif compute_subcat in ["Warehouse Compute", "Database Compute"]:
                        hours = np.random.uniform(8, 24)  # Higher hours for warehouses
                    else:
                        hours = np.random.uniform(2, 16)  # Medium hours for others

                    # Normalized cost (accounting for different month lengths)
                    # Simple approach: use the same cost for normalized view
                    normalized_cost = cost

                    data.append(
                        {
                            "date": date,
                            "compute_subcategory": compute_subcat,
                            "compute_category": compute_category_mapping[compute_subcat],
                            "region": region,
                            "cloud": cloud,
                            "cost_class": np.random.choice(cost_classes),
                            "cost_sub_class": np.random.choice(cost_sub_classes),
                            "deployment": np.random.choice(deployments),
                            "cost": cost,
                            "normalized_cost": normalized_cost,
                            "hours": hours,
                            "cost_per_hour": cost / hours,
                            "granularity": "Daily",
                        }
                    )

                    ### END DUMMY DATA FOR DEMO ###

    return pd.DataFrame(data)


# Load data
df = get_compute_data()

# ----------------------------------------------------------------------------- #
#### CREATE SIDEBAR FILTERS ####
with st.sidebar:
    st.write("**Filters**")

    granularity_select = st.selectbox(
        "Granularity",
        options=["Quarterly", "Monthly", "Weekly", "Daily"],
        key="compute_granularity_select",
    )

    # Lookback window with units based on granularity
    if granularity_select == "Daily":
        lookback_label = "Lookback window (days)"
        lookback_min = 1
        lookback_max = 540
        default_lookback = min(st.session_state.compute_lookback_select, lookback_max)
    elif granularity_select == "Weekly":
        lookback_label = "Lookback window (weeks)"
        lookback_min = 1
        lookback_max = 77  # ~18 months of weeks
        default_lookback = min(st.session_state.compute_lookback_select // 7, lookback_max)
    elif granularity_select == "Monthly":
        lookback_label = "Lookback window (months)"
        lookback_min = 1
        lookback_max = 18  # 18 months for 6 quarters
        default_lookback = min(st.session_state.compute_lookback_select // 30, lookback_max)
    else:  # Quarterly
        lookback_label = "Lookback window (quarters)"
        lookback_min = 1
        lookback_max = 6  # 6 quarters
        default_lookback = min(st.session_state.compute_lookback_select // 90, lookback_max)

    new_lookback = st.number_input(
        lookback_label,
        value=max(default_lookback, lookback_min),
        min_value=lookback_min,
        max_value=lookback_max,
    )

    if st.button("Run", key="compute_update"):
        # Convert to days for storage
        if granularity_select == "Daily":
            lookback_days = new_lookback
        elif granularity_select == "Weekly":
            lookback_days = new_lookback * 7
        elif granularity_select == "Monthly":
            lookback_days = new_lookback * 30
        else:  # Quarterly
            lookback_days = new_lookback * 90

        st.session_state.compute_lookback_select = lookback_days
        st.rerun()

    st.markdown("---")

    # Value type selection
    value_type_select = st.selectbox("Select Total Cost or CPH", options=["CPH", "Total Cost"])

    # Normalized vs raw cost
    normalized_select = st.selectbox(
        "Select Raw or Normalized Cost",
        options=["Normalized", "Raw"],
        key="compute_normalized_select",
        help="Normalized costs account for different month lengths to show accurate trends",
    )

# ----------------------------------------------------------------------------- #
#### APPLY FILTERS TO DATA ####

# Apply lookback window first (on daily data)
# The lookback is already in days from the sidebar conversion
adjusted_lookback = st.session_state.compute_lookback_select
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
#### APPLY FILTERS TO CREATE COMPUTE BUCKET DATAFRAME ####
# Create compute bucket data using broader categories as subcategories
compute_bucket_df = df_filtered.copy()
compute_bucket_df["compute_bucket"] = compute_bucket_df[
    "compute_category"
]  # Use the compute category as the bucket
compute_bucket_df["cph_cost"] = compute_bucket_df["display_cost"]  # Cost per hour calculation
compute_bucket_df["total_cost"] = compute_bucket_df["display_cost"]

# Group by date and compute_bucket (category) to match the original cpc.py structure
compute_bucket_df = (
    compute_bucket_df.groupby(["date", "compute_bucket", "cloud"])
    .agg(
        {
            "display_cost": "sum",
            "total_cost": "sum",
            "cph_cost": "sum",
            "hours": "sum",
            "cost_per_hour": "mean",
        }
    )
    .reset_index()
)

# ----------------------------------------------------------------------------- #
#### COST PER COMPUTE HOURS BY SUBCATEGORY ####
with st.container():
    c1, c2 = st.columns([1, 0.5])
    with c1:
        st.subheader("Trended CPH by Subcategory")
    with c2:
        cloud_options = ["All"] + sorted(compute_bucket_df["cloud"].unique(), reverse=False)
        cloud_select = st.selectbox(
            "Select Cloud To View",
            options=cloud_options,
            index=0,  # Default to "All"
            key="main_cloud_select",
        )

        if cloud_select != "All":
            compute_bucket_df = compute_bucket_df[compute_bucket_df["cloud"] == cloud_select]

# Use plotly_stacked_bar function to match cpc.py style
if not compute_bucket_df.empty:
    # Calculate cost per compute hours (CPH)
    compute_bucket_df["cph"] = compute_bucket_df["display_cost"] / compute_bucket_df["hours"]

    # Prepare data for stacked bar chart - ensure proper structure for plotly_stacked_bar
    # The plotly_stacked_bar function expects individual rows, not pre-aggregated data
    chart_data = (
        compute_bucket_df.groupby(["date", "compute_bucket"])
        .agg({"display_cost": "sum", "hours": "sum"})
        .reset_index()
    )

    # Recalculate CPH after aggregation for accuracy
    chart_data["cph"] = chart_data["display_cost"] / chart_data["hours"]

    # Sort by date to ensure proper chronological order
    chart_data = chart_data.sort_values("date")

    # Use the same plotly_stacked_bar function as cpc.py for consistent styling
    if plotly_stacked_bar is not None:
        plotly_stacked_bar(
            df=chart_data,
            y_axis="cph",
            y_axis_label="CPH",
            color_col="compute_bucket",
            color_label="Compute Bucket",
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
            y="cph",
            color="compute_bucket",
            title="Cost Per Compute Hours",
            labels={
                "cph": "Cost Per Compute Hours",
                "date": "Date",
                "compute_bucket": "Compute Bucket",
            },
        )
        fig.update_layout(height=400)
        st.plotly_chart(fig, use_container_width=True)

with st.expander("Expand Compute Category Detail"):
    c1, c2 = st.columns(2)
    with c1:
        subcat_metric_select = st.selectbox(
            "Select whether you would like to view CPH or Cost", options=["CPH", "Total Cost"]
        )
    with c2:
        subcat_growth_select = st.selectbox(
            "Select whether you would like to view absolute or percent growth",
            options=["($)", "(%)"],
        )
    subcategory_detail_prep(
        compute_bucket_df, subcat_metric_select, subcat_growth_select, "compute_bucket"
    )
st.markdown("------------")

# ----------------------------------------------------------------------------- #
#### CREATE VALUE TYPE LIST ####
period_list = sorted(df_filtered["date"].unique(), reverse=True)

compute_max_period = compute_bucket_df[compute_bucket_df["date"] == period_list[0]].sort_values(
    "display_cost", ascending=False
)
sub_cat_list = compute_max_period["compute_bucket"].unique()

# ----------------------------------------------------------------------------- #
#### CHART LOOP ####
sub_cat_select = st.selectbox("Choose Compute Bucket", options=sub_cat_list)

# Add compute_bucket column to df_filtered by mapping from compute_subcategory
df_filtered_with_bucket = df_filtered.copy()
df_filtered_with_bucket["compute_bucket"] = df_filtered_with_bucket["compute_subcategory"].map(
    compute_category_mapping
)

# Filter by selected compute bucket
filtered_df = df_filtered_with_bucket[df_filtered_with_bucket["compute_bucket"] == sub_cat_select]

c1, c3 = st.columns([4, 6])
with c1:
    st.title(sub_cat_select)
with c3:
    # Chart series selection above the filters
    series_list = ["Cost Class", "Deployment"]
    series_select_display = st.selectbox("Select Chart Series", series_list)

    # Map display names to actual column names
    if series_select_display == "Cost Class":
        series_select = "compute_subcategory"  # Show individual compute types within the bucket
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
            # Cost class filter (use keys from compute_category_mapping when Cost Class is selected)
            if series_select_display == "Cost Class":
                # Get available compute subcategories from the mapping keys
                available_subcategories = [
                    subcat
                    for subcat in compute_category_mapping.keys()
                    if subcat in filtered_df["compute_subcategory"].unique()
                ]
                cost_class_options = sorted(available_subcategories)
                selected_cost_classes = st.multiselect(
                    "Select Cost Class", cost_class_options, default=cost_class_options
                )
                if selected_cost_classes:
                    filtered_df = filtered_df[
                        filtered_df["compute_subcategory"].isin(selected_cost_classes)
                    ]
            else:
                # For Deployment view, still show cost_class filter
                cost_class_options = sorted(filtered_df["cost_class"].unique())
                selected_cost_classes = st.multiselect(
                    "Select Cost Class", cost_class_options, default=cost_class_options
                )
                if selected_cost_classes:
                    filtered_df = filtered_df[filtered_df["cost_class"].isin(selected_cost_classes)]


credits_df_pivot = filtered_df.groupby(["date", "cloud"])["hours"].mean().reset_index()
credits_df_pivot = credits_df_pivot.groupby("date")["hours"].sum().reset_index()

filtered_df_pivot = (
    filtered_df.groupby(["date", "cloud", series_select])
    .agg({"display_cost": "sum", "hours": "sum"})
    .reset_index()
)

filtered_df_pivot = (
    filtered_df_pivot.groupby(["date", series_select])
    .agg({"display_cost": "sum", "hours": "sum"})
    .reset_index()
)

filtered_df_pivot = pd.merge(filtered_df_pivot, credits_df_pivot, on="date", how="left")

# Calculate CPH and growth metrics
filtered_df_pivot["cph"] = filtered_df_pivot["display_cost"] / filtered_df_pivot["hours_x"]
filtered_df_pivot["prior_cost"] = filtered_df_pivot.groupby(series_select)["display_cost"].shift(1)
filtered_df_pivot["prior_cph"] = filtered_df_pivot.groupby(series_select)["cph"].shift(1)

filtered_df_pivot["cost_growth_percent"] = (
    filtered_df_pivot["display_cost"] / filtered_df_pivot["prior_cost"] - 1
)
filtered_df_pivot["cph_growth_percent"] = (
    filtered_df_pivot["cph"] / filtered_df_pivot["prior_cph"] - 1
)

filtered_df_pivot["cost_growth_amount"] = (
    filtered_df_pivot["display_cost"] - filtered_df_pivot["prior_cost"]
)
filtered_df_pivot["cph_growth_amount"] = filtered_df_pivot["cph"] - filtered_df_pivot["prior_cph"]
filtered_df_pivot = filtered_df_pivot.fillna(0).sort_values(
    ["date", "display_cost"], ascending=[1, 0]
)

# Create charts using the same plotly_stacked_bar function as the first graph
if not filtered_df_pivot.empty:
    # Create charts based on value type selection
    if value_type_select == "Total Cost":
        chart_data = filtered_df_pivot.copy()
        chart_col = "display_cost"
        y_axis_label = "Total Cost"
    else:  # CPH
        chart_data = filtered_df_pivot.copy()
        chart_col = "cph"
        y_axis_label = "CPH"

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
            chart_data,
            x="date",
            y=chart_col,
            color=series_select,
            title=f"{y_axis_label} for {sub_cat_select}",
        )
        fig_stacked.update_layout(
            xaxis_title="Date",
            yaxis_title=y_axis_label,
            legend_title=series_select_display,
            height=500,
        )
        st.plotly_chart(fig_stacked, use_container_width=True)

    # This matches the cpc_charts_stacked call from original cpc.py
    # cph_charts_stacked(
    #     sub_cat_select, filtered_df_pivot, value_type_select, series_select, True, granularity_select
    # )

    # ----------------------------------------------------------------------------- #
    #### COMPUTE BUCKET DETAIL SECTION ####
    st.markdown("---")
    with st.expander("Expand Compute Bucket Detail"):
        c1, c2 = st.columns(2)
        with c1:
            subcat_metric_select = st.selectbox(
                "Select whether you would like to view CPH or Cost",
                options=["CPH", "Total Cost"],
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
        detail_prep_df["cph_cost"] = detail_prep_df["display_cost"]
        detail_prep_df["cph"] = detail_prep_df["display_cost"] / detail_prep_df["hours"]

        # Group by date and compute_subcategory to match expected structure
        detail_prep_df = (
            detail_prep_df.groupby(["date", "compute_subcategory"])
            .agg(
                {
                    "display_cost": "sum",
                    "total_cost": "sum",
                    "cph_cost": "sum",
                    "hours": "sum",
                    "cph": "mean",
                }
            )
            .reset_index()
        )

        # Recalculate CPH after aggregation
        detail_prep_df["cph"] = detail_prep_df["display_cost"] / detail_prep_df["hours"]

        subcategory_detail_prep(
            detail_prep_df, subcat_metric_select, subcat_growth_select, "compute_subcategory"
        )

else:
    st.warning("No data available for the selected period.")
