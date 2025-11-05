"""Internal budget tracking and annual spend analysis page."""

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
    shared_format_subcat_change = None
    plotly_stacked_bar = None


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
        "functional_area",
        "internal_category",
        "region",
        "cloud",
        "environment",
        "team",
    ]

    aggregated_df = (
        df.groupby(group_cols)
        .agg(
            {
                "actual_cost": "sum",
                "normalized_cost": "sum",
                "budget_amount": "sum",
                "annual_budget": "mean",
                "variance_amount": "sum",
                "variance_percent": "mean",
            }
        )
        .reset_index()
    )

    # Recalculate variance_percent after aggregation
    aggregated_df["variance_percent"] = (
        (aggregated_df["actual_cost"] - aggregated_df["budget_amount"])
        / aggregated_df["budget_amount"]
    ) * 100

    # Rename period back to date for consistency
    aggregated_df["date"] = aggregated_df["period"]
    aggregated_df = aggregated_df.drop("period", axis=1)
    aggregated_df["granularity"] = granularity

    return aggregated_df


st.title("Internal Budget Summary")

# ----------------------------------------------------------------------------- #
#### DEFINE SESSION STATE DEFAULTS ####
if "internal_lookback_select" not in st.session_state:
    st.session_state.internal_lookback_select = (
        540  # Default value - show all available data (6 quarters)
    )

if "internal_granularity_select" not in st.session_state:
    st.session_state.internal_granularity_select = "Monthly"

if "internal_normalized_select" not in st.session_state:
    st.session_state.internal_normalized_select = "Raw"


# ----------------------------------------------------------------------------- #
#### HELPER FUNCTIONS ####
def subcategory_detail_prep(df: pd.DataFrame, metric_type: str, growth_type: str, metric_col: str):
    """Create detailed breakdown table for budget buckets (adapted from original cpc.py)"""
    # Create list of subcategories sorted by metric in latest period
    period_list = sorted(df["date"].unique())
    if not period_list:
        st.warning("No data available for detailed analysis")
        return

    # Determine metric column to sort by
    sort_col = "variance_percent" if metric_type == "Budget Variance" else "total_cost"
    df_max_period = df[df["date"] == period_list[-1]].sort_values([sort_col], ascending=False)
    sub_cat_list = list(df_max_period[metric_col].unique())

    # Create detailed breakdown table
    sub_cat_df = pd.DataFrame()

    for sub_cat in sub_cat_list:
        temp_df = pd.DataFrame()
        temp_df2 = df[df[metric_col] == sub_cat].copy()

        # Calculate growth metrics
        if metric_type == "Budget Variance":
            temp_df2["variance_percent"] = (
                (temp_df2["display_cost"] - temp_df2["budget_amount"]) / temp_df2["budget_amount"]
            ) * 100
            temp_df2["prior_variance"] = temp_df2.groupby(metric_col)["variance_percent"].shift(1)
            temp_df2["variance_growth_amount"] = (
                temp_df2["variance_percent"] - temp_df2["prior_variance"]
            )
            temp_df2["variance_growth_percent"] = (
                temp_df2["variance_percent"] / temp_df2["prior_variance"] - 1
            )
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
            else:  # Budget Variance
                variance_val = row.get("variance_percent", 0)
                metric = f"{variance_val:+.1f}%" if not pd.isna(variance_val) else "0.0%"
                change_percent = row.get("variance_growth_percent", 0)
                if growth_type == "($)":
                    growth_val = row.get("variance_growth_amount", 0)
                    if not pd.isna(growth_val):
                        symbol = "+" if growth_val >= 0 else "-"
                        change = f"{symbol}{abs(growth_val):.1f}pp"
                    else:
                        change = "0.0pp"
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
        st.warning("No data available for the selected budget category")


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
    else:  # Budget Variance
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


# Define all internal categories (buckets) and functional areas
internal_categories = ["Development", "Operations", "Support", "Infrastructure"]
internal_functional_areas = [
    "Application Development",
    "Data Engineering",
    "DevOps",
    "Testing",
    "Security",
    "Monitoring",
    "Production Support",
    "Customer Support",
    "Training",
    "Disaster Recovery",
    "Compliance",
]


# Sample data generation for demo
@st.cache_data
def get_internal_budget_data() -> pd.DataFrame:
    """Generate sample internal budget tracking data."""
    dates = pd.date_range(end=datetime.now(), periods=540, freq="D")  # 18 months for 6 quarters
    regions = ["us-east-1", "us-west-2", "eu-west-1"]
    clouds = ["AWS", "GCP", "Azure"]
    environments = ["Production", "Development", "Testing", "Staging"]
    teams = ["Frontend Team", "Backend Team", "Data Team", "DevOps Team", "Security Team"]

    # Internal category mapping for logical grouping
    internal_category_mapping = {
        "Application Development": "Development",
        "Data Engineering": "Development",
        "DevOps": "Operations",
        "Testing": "Operations",
        "Security": "Operations",
        "Monitoring": "Infrastructure",
        "Production Support": "Support",
        "Customer Support": "Support",
        "Training": "Support",
        "Disaster Recovery": "Infrastructure",
        "Compliance": "Infrastructure",
    }

    ### DUMMY DATA FOR DEMO ###

    data = []
    for date in dates:
        for internal_category in internal_categories:  # All bucket categories
            for functional_area in internal_functional_areas:  # All functional areas in each bucket
                for region in regions:
                    for cloud in clouds:
                        # Different budget patterns for different functional areas
                        budget_multipliers = {
                            "Application Development": 3.5,
                            "Data Engineering": 2.8,
                            "DevOps": 2.2,
                            "Testing": 1.5,
                            "Security": 2.0,
                            "Monitoring": 1.8,
                            "Production Support": 2.5,
                            "Customer Support": 1.2,
                            "Training": 0.8,
                            "Disaster Recovery": 1.6,
                            "Compliance": 1.0,
                        }

                        # Category-based budget adjustments
                        category_multipliers = {
                            "Development": 1.2,  # Higher budgets for development
                            "Operations": 1.0,  # Baseline operational costs
                            "Support": 0.8,  # Lower support costs
                            "Infrastructure": 1.1,  # Slight premium for infrastructure
                        }

                        # Annual budget calculation
                        annual_budget_base = (
                            100000
                            * budget_multipliers.get(functional_area, 1.0)
                            * category_multipliers.get(internal_category, 1.0)
                        )
                        monthly_budget = annual_budget_base / 12
                        daily_budget = monthly_budget / 30

                        # Actual spend with some variance from budget
                        budget_variance_factor = np.random.uniform(0.85, 1.15)  # ±15% variance
                        actual_cost = daily_budget * budget_variance_factor

                        # Add seasonal patterns
                        seasonal_factor = 1 + 0.1 * np.sin((date.dayofyear / 365) * 2 * np.pi)
                        actual_cost *= seasonal_factor

                        # Normalized cost (accounting for different month lengths)
                        # Simple approach: use the same cost for normalized view
                        normalized_cost = actual_cost

                        data.append(
                            {
                                "date": date,
                                "functional_area": functional_area,
                                "internal_category": internal_category,
                                "region": region,
                                "cloud": cloud,
                                "environment": np.random.choice(environments),
                                "team": np.random.choice(teams),
                                "actual_cost": actual_cost,
                                "normalized_cost": normalized_cost,
                                "budget_amount": daily_budget,
                                "annual_budget": annual_budget_base,
                                "variance_amount": actual_cost - daily_budget,
                                "variance_percent": ((actual_cost - daily_budget) / daily_budget)
                                * 100,
                                "granularity": "Daily",
                            }
                        )

    ### END DUMMY DATA FOR DEMO ###

    return pd.DataFrame(data)


# Load data
df = get_internal_budget_data()

# ----------------------------------------------------------------------------- #
#### CREATE SIDEBAR FILTERS ####
with st.sidebar:
    st.write("**Filters**")

    granularity_select = st.selectbox(
        "Granularity",
        options=["Quarterly", "Monthly", "Weekly", "Daily"],
        key="internal_granularity_select",
    )

    # Lookback window with units based on granularity
    if granularity_select == "Daily":
        lookback_label = "Lookback window (days)"
        lookback_min = 1
        lookback_max = 540
        default_lookback = min(st.session_state.internal_lookback_select, lookback_max)
    elif granularity_select == "Weekly":
        lookback_label = "Lookback window (weeks)"
        lookback_min = 1
        lookback_max = 77  # ~18 months of weeks
        default_lookback = min(st.session_state.internal_lookback_select // 7, lookback_max)
    elif granularity_select == "Monthly":
        lookback_label = "Lookback window (months)"
        lookback_min = 1
        lookback_max = 18  # 18 months for 6 quarters
        default_lookback = min(st.session_state.internal_lookback_select // 30, lookback_max)
    else:  # Quarterly
        lookback_label = "Lookback window (quarters)"
        lookback_min = 1
        lookback_max = 6  # 6 quarters
        default_lookback = min(st.session_state.internal_lookback_select // 90, lookback_max)

    new_lookback = st.number_input(
        lookback_label,
        value=max(default_lookback, lookback_min),
        min_value=lookback_min,
        max_value=lookback_max,
    )

    if st.button("Run", key="internal_update"):
        # Convert to days for storage
        if granularity_select == "Daily":
            lookback_days = new_lookback
        elif granularity_select == "Weekly":
            lookback_days = new_lookback * 7
        elif granularity_select == "Monthly":
            lookback_days = new_lookback * 30
        else:  # Quarterly
            lookback_days = new_lookback * 90

        st.session_state.internal_lookback_select = lookback_days
        st.rerun()

    st.markdown("---")

    # Value type selection
    value_type_select = st.selectbox(
        "Select Total Cost or Budget Variance", options=["Budget Variance", "Total Cost"]
    )

    # Normalized vs raw cost
    normalized_select = st.selectbox(
        "Select Raw or Normalized Cost",
        options=["Normalized", "Raw"],
        key="internal_normalized_select",
        help="Normalized costs account for different month lengths to show accurate trends",
    )

# ----------------------------------------------------------------------------- #
#### APPLY FILTERS TO DATA ####

# Apply lookback window first (on daily data)
# The lookback is already in days from the sidebar conversion
adjusted_lookback = st.session_state.internal_lookback_select
cutoff_date = df["date"].max() - timedelta(days=adjusted_lookback)
df_filtered = df[df["date"] >= cutoff_date].copy()

# Aggregate data by selected granularity
df_filtered = aggregate_by_granularity(df_filtered, granularity_select)

# Select cost column based on normalization choice
cost_column = "normalized_cost" if normalized_select == "Normalized" else "actual_cost"
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
#### APPLY FILTERS TO CREATE INTERNAL BUCKET DATAFRAME ####
# Create internal bucket data using broader categories as subcategories
internal_bucket_df = df_filtered.copy()
internal_bucket_df["internal_bucket"] = internal_bucket_df[
    "internal_category"
]  # Use the internal category as the bucket
internal_bucket_df["total_cost"] = internal_bucket_df["display_cost"]

# Group by date and internal_bucket (category) to match the original cpc.py structure
internal_bucket_df = (
    internal_bucket_df.groupby(["date", "internal_bucket", "cloud"])
    .agg(
        {
            "display_cost": "sum",
            "total_cost": "sum",
            "budget_amount": "sum",
            "variance_amount": "sum",
            "annual_budget": "mean",
        }
    )
    .reset_index()
)

# Calculate variance percentage after aggregation
internal_bucket_df["variance_percent"] = (
    (internal_bucket_df["display_cost"] - internal_bucket_df["budget_amount"])
    / internal_bucket_df["budget_amount"]
) * 100

# ----------------------------------------------------------------------------- #
#### PERFORMANCE BY CATEGORY ####
with st.container():
    c1, c2 = st.columns([1, 0.5])
    with c1:
        st.subheader("Performance by Category")
    with c2:
        cloud_options = ["All"] + sorted(internal_bucket_df["cloud"].unique(), reverse=False)
        cloud_select = st.selectbox(
            "Select Cloud To View",
            options=cloud_options,
            index=0,  # Default to "All"
            key="main_cloud_select",
        )

        if cloud_select != "All":
            internal_bucket_df = internal_bucket_df[internal_bucket_df["cloud"] == cloud_select]

# Create performance visualization
if not internal_bucket_df.empty:
    # Prepare data for stacked bar chart
    chart_data = (
        internal_bucket_df.groupby(["date", "internal_bucket"])
        .agg({"variance_percent": "mean", "display_cost": "sum", "budget_amount": "sum"})
        .reset_index()
    )

    # Create chart using plotly_stacked_bar for consistent formatting with cpc.py
    if plotly_stacked_bar is not None:
        if value_type_select == "Budget Variance":
            plotly_stacked_bar(
                df=chart_data,
                y_axis="variance_percent",
                y_axis_label="Budget Variance (%)",
                color_col="internal_bucket",
                color_label="Internal Category",
                text_size=15,
                hide_legend=False,
                use_tooltip=True,
                round_values=False,
                granularity_select=granularity_select,
            )
        else:  # Total Cost
            plotly_stacked_bar(
                df=chart_data,
                y_axis="display_cost",
                y_axis_label="Total Cost",
                color_col="internal_bucket",
                color_label="Internal Category",
                text_size=15,
                hide_legend=False,
                use_tooltip=True,
                round_values=False,
                granularity_select=granularity_select,
            )
    else:
        # Fallback to basic chart if shared modules not available
        if value_type_select == "Budget Variance":
            fig = px.bar(
                chart_data,
                x="date",
                y="variance_percent",
                color="internal_bucket",
                title="Budget Variance by Category (%)",
                labels={
                    "variance_percent": "Budget Variance (%)",
                    "date": "Date",
                    "internal_bucket": "Internal Category",
                },
            )
        else:  # Total Cost
            fig = px.bar(
                chart_data,
                x="date",
                y="display_cost",
                color="internal_bucket",
                title="Total Cost by Category",
                labels={
                    "display_cost": "Total Cost ($)",
                    "date": "Date",
                    "internal_bucket": "Internal Category",
                },
            )
        fig.update_layout(height=500)
        st.plotly_chart(fig, use_container_width=True)

with st.expander("Expand Internal Category Detail"):
    c1, c2 = st.columns(2)
    with c1:
        subcat_metric_select = st.selectbox(
            "Select whether you would like to view Budget Variance or Cost",
            options=["Budget Variance", "Total Cost"],
        )
    with c2:
        subcat_growth_select = st.selectbox(
            "Select whether you would like to view absolute or percent growth",
            options=["($)", "(%)"],
        )
    subcategory_detail_prep(
        internal_bucket_df, subcat_metric_select, subcat_growth_select, "internal_bucket"
    )
st.markdown("------------")

# ----------------------------------------------------------------------------- #
#### CREATE VALUE TYPE LIST ####
period_list = sorted(df_filtered["date"].unique(), reverse=True)

internal_max_period = internal_bucket_df[internal_bucket_df["date"] == period_list[0]].sort_values(
    "display_cost", ascending=False
)
sub_cat_list = internal_max_period["internal_bucket"].unique()

# ----------------------------------------------------------------------------- #
#### CHART LOOP ####
sub_cat_select = st.selectbox("Choose Internal Category", options=sub_cat_list)

# internal_bucket is already in the data as internal_category
df_filtered_with_bucket = df_filtered.copy()
df_filtered_with_bucket["internal_bucket"] = df_filtered_with_bucket["internal_category"]

# Filter by selected internal bucket
filtered_df = df_filtered_with_bucket[df_filtered_with_bucket["internal_bucket"] == sub_cat_select]

c1, c3 = st.columns([4, 6])
with c1:
    st.title(sub_cat_select)
with c3:
    # Chart series selection above the filters
    series_list = ["Functional Area", "Team"]
    series_select_display = st.selectbox("Select Chart Series", series_list)

    # Map display names to actual column names
    if series_select_display == "Functional Area":
        series_select = "functional_area"  # Show individual functional areas within the bucket
    else:  # Team
        series_select = "team"

    with st.expander("Expand to Apply Filters"):
        c1, c2 = st.columns(2)
        with c1:
            # Cloud filter (multiselect_and_filter equivalent)
            cloud_options = sorted(filtered_df["cloud"].unique())
            selected_clouds = st.multiselect("Select Cloud", cloud_options, default=cloud_options)
            if selected_clouds:
                filtered_df = filtered_df[filtered_df["cloud"].isin(selected_clouds)]

            # Environment filter
            environment_options = sorted(filtered_df["environment"].unique())
            selected_environments = st.multiselect(
                "Select Environment", environment_options, default=environment_options
            )
            if selected_environments:
                filtered_df = filtered_df[filtered_df["environment"].isin(selected_environments)]

        with c2:
            # Functional area filter (use all available functional areas when Functional Area is selected)
            if series_select_display == "Functional Area":
                # Get available functional areas for the selected bucket
                available_functional_areas = sorted(filtered_df["functional_area"].unique())
                selected_functional_areas = st.multiselect(
                    "Select Functional Area",
                    available_functional_areas,
                    default=available_functional_areas,
                )
                if selected_functional_areas:
                    filtered_df = filtered_df[
                        filtered_df["functional_area"].isin(selected_functional_areas)
                    ]
            else:
                # For Team view, still show team filter
                team_options = sorted(filtered_df["team"].unique())
                selected_teams = st.multiselect("Select Team", team_options, default=team_options)
                if selected_teams:
                    filtered_df = filtered_df[filtered_df["team"].isin(selected_teams)]


budget_df_pivot = filtered_df.groupby(["date", "cloud"])["budget_amount"].mean().reset_index()
budget_df_pivot = budget_df_pivot.groupby("date")["budget_amount"].sum().reset_index()

filtered_df_pivot = (
    filtered_df.groupby(["date", "cloud", series_select])
    .agg({"display_cost": "sum", "budget_amount": "sum"})
    .reset_index()
)

filtered_df_pivot = (
    filtered_df_pivot.groupby(["date", series_select])
    .agg({"display_cost": "sum", "budget_amount": "sum"})
    .reset_index()
)

filtered_df_pivot = pd.merge(filtered_df_pivot, budget_df_pivot, on="date", how="left")

# Calculate variance and growth metrics
filtered_df_pivot["variance_percent"] = (
    (filtered_df_pivot["display_cost"] - filtered_df_pivot["budget_amount_x"])
    / filtered_df_pivot["budget_amount_x"]
) * 100
filtered_df_pivot["prior_cost"] = filtered_df_pivot.groupby(series_select)["display_cost"].shift(1)
filtered_df_pivot["prior_variance"] = filtered_df_pivot.groupby(series_select)[
    "variance_percent"
].shift(1)

filtered_df_pivot["cost_growth_percent"] = (
    filtered_df_pivot["display_cost"] / filtered_df_pivot["prior_cost"] - 1
)
filtered_df_pivot["variance_growth_percent"] = (
    filtered_df_pivot["variance_percent"] / filtered_df_pivot["prior_variance"] - 1
)

filtered_df_pivot["cost_growth_amount"] = (
    filtered_df_pivot["display_cost"] - filtered_df_pivot["prior_cost"]
)
filtered_df_pivot["variance_growth_amount"] = (
    filtered_df_pivot["variance_percent"] - filtered_df_pivot["prior_variance"]
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
    else:  # Budget Variance
        chart_data = filtered_df_pivot.copy()
        chart_col = "variance_percent"
        chart_title = f"Budget variance for {sub_cat_select}"

    # Use plotly_stacked_bar for consistent formatting with cpc.py
    if plotly_stacked_bar is not None:
        y_axis_label = "Total Cost" if chart_col == "display_cost" else "Budget Variance (%)"
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
        fig_stacked = px.bar(chart_data, x="date", y=chart_col, color=series_select, title=chart_title)
        fig_stacked.update_layout(
            xaxis_title="Date",
            yaxis_title="Cost ($)" if chart_col == "display_cost" else "Budget Variance (%)",
            legend_title=series_select_display,
            height=500,
        )
        st.plotly_chart(fig_stacked, use_container_width=True)

    # This matches the cpc_charts_stacked call from original cpc.py
    # budget_charts_stacked(
    #     sub_cat_select, filtered_df_pivot, value_type_select, series_select, True, granularity_select
    # )

    # ----------------------------------------------------------------------------- #
    #### INTERNAL BUCKET DETAIL SECTION ####
    st.markdown("---")
    with st.expander("Expand Internal Category Detail"):
        c1, c2 = st.columns(2)
        with c1:
            subcat_metric_select = st.selectbox(
                "Select whether you would like to view Budget Variance or Cost",
                options=["Budget Variance", "Total Cost"],
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
        detail_prep_df["variance_percent"] = (
            (detail_prep_df["display_cost"] - detail_prep_df["budget_amount"])
            / detail_prep_df["budget_amount"]
        ) * 100

        # Group by date and functional_area to match expected structure
        detail_prep_df = (
            detail_prep_df.groupby(["date", "functional_area"])
            .agg(
                {
                    "display_cost": "sum",
                    "total_cost": "sum",
                    "budget_amount": "sum",
                    "variance_percent": "mean",
                }
            )
            .reset_index()
        )

        # Recalculate variance after aggregation
        detail_prep_df["variance_percent"] = (
            (detail_prep_df["display_cost"] - detail_prep_df["budget_amount"])
            / detail_prep_df["budget_amount"]
        ) * 100

        subcategory_detail_prep(
            detail_prep_df, subcat_metric_select, subcat_growth_select, "functional_area"
        )

else:
    st.warning("No data available for the selected period.")
