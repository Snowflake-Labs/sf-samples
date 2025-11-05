"""Forecast and cost optimization opportunities analysis page."""

from __future__ import annotations

from datetime import date

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

# Import plotly_stacked_bar function from shared modules for consistent formatting
try:
    from shared_modules.dashboard_functions import plotly_stacked_bar
except ImportError:
    # Fallback if shared modules not available
    plotly_stacked_bar = None

# ----------------------------------------------------------------------------- #
# Streamlit Page Setup
# st.set_page_config(page_title="🔮 Forecast & Opportunities", layout="wide")
st.title("Forecast & Opportunities")

# ----------------------------------------------------------------------------- #
# Page Description and Target Metrics
desc_col, metric_col = st.columns([2, 1])

with desc_col:
    st.markdown(
        """
        **How to Use This Page**

        Use this page to apply **custom scenarios** for one-time expenses or savings, and  
        **monthly scenarios** for recurring ones, to the forecasted data. These scenarios  
        help model potential cost savings or additional costs across compute, storage, and  
        network services. View their impact on Cost Per Hour (CPH), Cost Per Terabyte (CPT),  
        and Cost Per Transferred Terabyte (CPTT) metrics.
        """
    )

with metric_col:
    st.metric(label="🎯 Target CPH", value="$2.45")
    st.metric(label="🎯 Target CPT", value="$15.80")
    st.metric(label="🎯 Target CPTT", value="$8.25")

# ----------------------------------------------------------------------------- #
# Service Categories and Cost Classes
compute_categories = ["Core Compute", "Advanced Compute", "API & Microservices", "Storage & Cache"]
compute_cost_classes = [
    "Warehouse Compute",
    "Database Compute",
    "Kubernetes Compute",
    "Container Instances",
    "ML/AI Compute",
    "Data Processing",
    "API Calls",
    "Serverless Functions",
    "Queue Processing",
    "Storage Requests",
    "Cache Operations",
    "Traffic Processing",
]

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

network_categories = ["External Traffic", "Infrastructure", "Replication", "Data Services"]
network_cost_classes = [
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

# Combined lists for scenario inputs
all_service_types = ["Compute", "Storage", "Network"]
all_categories = compute_categories + storage_categories + network_categories
all_cost_classes = compute_cost_classes + storage_cost_classes + network_cost_classes

cloud_names_dict = ["All", "AWS", "GCP", "Azure"]

# ----------------------------------------------------------------------------- #
# Date Setup
today = date.today()
fiscal_start = pd.Timestamp(today.year, today.month, 1)
fiscal_end = pd.Timestamp(today.year + 1, 1, 31)  # Include full January 2026
months = pd.date_range(start=fiscal_start, end=fiscal_end, freq="MS")
month_cols = [m.strftime("%Y-%m-%d") for m in months]


# ----------------------------------------------------------------------------- #
# Sample Data Generation
@st.cache_data
def get_forecast_data() -> pd.DataFrame:
    """Generate sample forecast data for compute, storage, and network services."""
    # Set seed for consistent data generation across all months
    np.random.seed(42)

    # Ensure we cover full months including January 2026
    dates = pd.date_range(start=fiscal_start, end=fiscal_end, freq="D")
    clouds = ["AWS", "GCP", "Azure"]
    regions = ["us-east-1", "us-west-2", "eu-west-1"]

    data = []
    for date in dates:
        month = date.replace(day=1)

        ### DUMMY DATA FOR DEMO ###

        # Reset seed for each date to ensure consistency
        np.random.seed(42 + date.day + date.month + date.year)

        # Add seasonal and trend variations
        month_multiplier = 1.0 + 0.15 * np.sin(2 * np.pi * date.month / 12)  # Seasonal variation
        trend_multiplier = 1.0 + 0.02 * (date.month - 1) / 12  # Slight upward trend
        daily_variation = 0.95 + 0.1 * np.random.random()  # Daily randomness

        for cloud in clouds:
            for region in regions:
                # Compute data
                for category in compute_categories:
                    for cost_class in compute_cost_classes:
                        if (
                            cost_class
                            in [
                                "Warehouse Compute",
                                "Database Compute",
                                "Kubernetes Compute",
                                "Container Instances",
                            ]
                            and category == "Core Compute"
                        ):
                            hours = np.random.uniform(10, 60)
                            base_cost = hours * np.random.uniform(
                                2.50, 2.90
                            )  # CPH range around $2.45 target
                        elif (
                            cost_class in ["ML/AI Compute", "Data Processing"]
                            and category == "Advanced Compute"
                        ):
                            hours = np.random.uniform(8, 45)
                            base_cost = hours * np.random.uniform(
                                2.40, 2.85
                            )  # CPH range around $2.45 target
                        elif (
                            cost_class in ["API Calls", "Serverless Functions", "Queue Processing"]
                            and category == "API & Microservices"
                        ):
                            hours = np.random.uniform(2, 25)
                            base_cost = hours * np.random.uniform(
                                2.35, 2.80
                            )  # CPH range around $2.45 target
                        elif (
                            cost_class
                            in ["Storage Requests", "Cache Operations", "Traffic Processing"]
                            and category == "Storage & Cache"
                        ):
                            hours = np.random.uniform(5, 35)
                            base_cost = hours * np.random.uniform(
                                2.45, 2.85
                            )  # CPH range around $2.45 target
                        else:
                            continue

                        data.append(
                            {
                                "date": date,
                                "month": month,
                                "cloud": cloud,
                                "region": region,
                                "service_type": "Compute",
                                "category": category,
                                "cost_class": cost_class,
                                "amount": base_cost
                                * month_multiplier
                                * trend_multiplier
                                * daily_variation
                                * np.random.uniform(0.85, 1.15),
                                "volume": hours * month_multiplier * daily_variation,
                                "unit": "hours",
                            }
                        )

                # Storage data
                for category in storage_categories:
                    for cost_class in storage_cost_classes:
                        if category == "Standard":
                            size_tb = np.random.uniform(5, 80)
                            base_cost = size_tb * np.random.uniform(
                                15.20, 17.00
                            )  # CPT range around $15.80 target
                        elif category == "Infrequent Access":
                            size_tb = np.random.uniform(2, 50)
                            base_cost = size_tb * np.random.uniform(
                                15.00, 16.80
                            )  # CPT range around $15.80 target
                        elif category == "Intelligent Tiering":
                            size_tb = np.random.uniform(8, 100)
                            base_cost = size_tb * np.random.uniform(
                                15.40, 17.20
                            )  # CPT range around $15.80 target
                        else:  # Tags
                            size_tb = np.random.uniform(3, 70)
                            base_cost = size_tb * np.random.uniform(
                                15.30, 16.90
                            )  # CPT range around $15.80 target

                        data.append(
                            {
                                "date": date,
                                "month": month,
                                "cloud": cloud,
                                "region": region,
                                "service_type": "Storage",
                                "category": category,
                                "cost_class": cost_class,
                                "amount": base_cost
                                * month_multiplier
                                * trend_multiplier
                                * daily_variation
                                * np.random.uniform(0.85, 1.15),
                                "volume": size_tb * month_multiplier * daily_variation,
                                "unit": "terabytes",
                            }
                        )

                # Network data
                for category in network_categories:
                    for cost_class in network_cost_classes:
                        if category == "External Traffic":
                            tb_transferred = np.random.uniform(10, 150)
                            base_cost = tb_transferred * np.random.uniform(
                                7.80, 9.20
                            )  # CPTT range around $8.25 target
                        elif category == "Infrastructure":
                            tb_transferred = np.random.uniform(5, 90)
                            base_cost = tb_transferred * np.random.uniform(
                                7.70, 9.00
                            )  # CPTT range around $8.25 target
                        elif category == "Replication":
                            tb_transferred = np.random.uniform(2, 60)
                            base_cost = tb_transferred * np.random.uniform(
                                7.90, 8.80
                            )  # CPTT range around $8.25 target
                        else:  # Data Services
                            tb_transferred = np.random.uniform(8, 120)
                            base_cost = tb_transferred * np.random.uniform(
                                8.00, 9.10
                            )  # CPTT range around $8.25 target

                        data.append(
                            {
                                "date": date,
                                "month": month,
                                "cloud": cloud,
                                "region": region,
                                "service_type": "Network",
                                "category": category,
                                "cost_class": cost_class,
                                "amount": base_cost
                                * month_multiplier
                                * trend_multiplier
                                * daily_variation
                                * np.random.uniform(0.85, 1.15),
                                "volume": tb_transferred * month_multiplier * daily_variation,
                                "unit": "tb_transferred",
                            }
                        )

        ### END DUMMY DATA FOR DEMO ###

    return pd.DataFrame(data)


# Load forecast data
forecast_data = get_forecast_data()

# ----------------------------------------------------------------------------- #
# Initialize Session State
columns = ["Service Type", "Category", "Cost Class", "Cloud"] + month_cols
st.session_state.setdefault("override_list", [])
st.session_state.setdefault("editor_initialized", False)

# ----------------------------------------------------------------------------- #
# Monthly Scenarios Input
with st.expander("📅 Monthly Scenarios Input", expanded=False):
    with st.form("monthly_scenarios_form"):
        auto_service_type = st.selectbox(
            "Service Type",
            all_service_types,
            key="auto_service_type",
            help="Select which service type to apply the scenario to.",
        )

        # Dynamic category selection based on service type
        if auto_service_type == "Compute":
            available_categories = compute_categories
            available_cost_classes = compute_cost_classes
        elif auto_service_type == "Storage":
            available_categories = storage_categories
            available_cost_classes = storage_cost_classes
        else:  # Network
            available_categories = network_categories
            available_cost_classes = network_cost_classes

        auto_category = st.selectbox(
            "Category",
            available_categories,
            key="auto_category",
            help="Select which category to apply the scenario to.",
        )
        auto_cost_class = st.selectbox(
            "Cost Class",
            available_cost_classes,
            key="auto_cost_class",
            help="Choose the specific cost class this scenario affects.",
        )
        auto_cloud = st.selectbox(
            "Cloud",
            cloud_names_dict,
            key="auto_cloud",
            help="Select which cloud to apply the scenario to. 'All' applies to every cloud.",
        )
        auto_amount = st.number_input(
            "Amount per Month",
            step=100.0,
            help="Dollar amount to add/subtract each month (use negative values for cost savings).",
        )
        auto_start = st.selectbox(
            "Start Month", month_cols, key="start_month", help="First month to apply this scenario."
        )
        auto_end = st.selectbox(
            "End Month",
            month_cols,
            key="end_month",
            help="Last month to apply this scenario (inclusive).",
        )
        submit_auto = st.form_submit_button("Apply Monthly Scenario Changes")

    if submit_auto:
        start_idx = month_cols.index(auto_start)
        end_idx = month_cols.index(auto_end)
        selected_months = month_cols[start_idx : end_idx + 1]

        row_data = {
            "Service Type": auto_service_type,
            "Category": auto_category,
            "Cost Class": auto_cost_class,
            "Cloud": auto_cloud,
            **{m: 0.0 for m in month_cols},
        }
        for m in selected_months:
            row_data[m] = auto_amount

        st.session_state.override_list.append(row_data)
        st.session_state.editor_initialized = False
        st.success(
            f"Added monthly scenario: {auto_service_type} - {auto_category} - {auto_cost_class}"
        )
        st.rerun()

# ----------------------------------------------------------------------------- #
# Custom Scenarios Editor
st.subheader("✏️ Custom Scenarios Input Table")

st.info(
    "**How to use this table:** This table contains **all scenarios** that will be applied "
    "to forecasted data. You can add new scenarios using the + button below or "
    "edit existing ones. Make sure to save changes after editing or deleting rows "
    "to ensure your updates are applied correctly."
)

initial_data = pd.DataFrame(
    st.session_state.override_list if st.session_state.override_list else [], columns=columns
)
edited_df = st.data_editor(
    initial_data,
    column_config={
        "Service Type": st.column_config.SelectboxColumn(
            "Service Type", options=all_service_types, help="Service type for this scenario"
        ),
        "Category": st.column_config.SelectboxColumn(
            "Category", options=all_categories, help="Category affected by this scenario"
        ),
        "Cost Class": st.column_config.SelectboxColumn(
            "Cost Class", options=all_cost_classes, help="Specific cost class affected"
        ),
        "Cloud": st.column_config.SelectboxColumn(
            "Cloud", options=cloud_names_dict, help="Cloud provider for this scenario"
        ),
    },
    num_rows="dynamic",
    key="override_editor",
    use_container_width=True,
)

if st.button("Save Custom Changes", key="save_manual"):
    st.session_state.override_list = (
        edited_df.to_dict(orient="records") if not edited_df.empty else []
    )
    st.success("Custom changes saved!")
combined_overrides_df = edited_df.copy()

# ----------------------------------------------------------------------------- #
# Combined Filters Section
with st.expander("🔍 Expand to Apply Filters", expanded=False):
    st.markdown("### ☁️ Select Cloud")
    selected_cloud = st.selectbox(
        "Select Cloud",
        cloud_names_dict,
        help="⚠️ This filter affects ALL charts and calculations below",
    )

    if not combined_overrides_df.empty:
        st.markdown("### 🎯 Select Override Scenarios")
        combined_overrides_df["Row Label"] = (
            combined_overrides_df["Service Type"]
            + " | "
            + combined_overrides_df["Category"]
            + " | "
            + combined_overrides_df["Cost Class"]
            + " | "
            + combined_overrides_df["Cloud"]
        )
        row_labels = combined_overrides_df["Row Label"].tolist()
        selected_rows = st.multiselect(
            "Select override rows to apply in graphs",
            options=row_labels,
            default=row_labels,
            help="⚠️ Only checked scenarios will be applied to forecast calculations",
        )
        combined_overrides_df = combined_overrides_df[
            combined_overrides_df["Row Label"].isin(selected_rows)
        ]

# ----------------------------------------------------------------------------- #
# Process Forecast Data
forecasted_summary = (
    forecast_data.groupby(["month", "cloud", "service_type"])
    .agg({"amount": "sum", "volume": "sum"})
    .reset_index()
)

# Add "all" cloud aggregation
all_clouds_agg = (
    forecasted_summary.groupby(["month", "service_type"])
    .agg({"amount": "sum", "volume": "sum"})
    .reset_index()
)
all_clouds_agg["cloud"] = "All"
forecasted_summary = pd.concat([forecasted_summary, all_clouds_agg], ignore_index=True)

# ----------------------------------------------------------------------------- #
# Apply Overrides
if not combined_overrides_df.empty:
    for _, row in combined_overrides_df.iterrows():
        service_type = row.get("Service Type")
        category = row.get("Category")
        cost_class = row.get("Cost Class")
        cloud = row.get("Cloud")

        if not all([service_type, category, cost_class, cloud]):
            continue

        target_clouds = [cloud] if cloud != "All" else ["AWS", "GCP", "Azure"]

        for target in target_clouds:
            for month_str in month_cols:
                value = pd.to_numeric(row.get(month_str, 0), errors="coerce")
                if pd.isna(value) or value == 0:
                    continue

                month = pd.to_datetime(month_str)
                mask = (
                    (forecasted_summary["cloud"] == target)
                    & (forecasted_summary["month"] == month)
                    & (forecasted_summary["service_type"] == service_type)
                )

                if not mask.any():
                    new_row = {
                        "cloud": target,
                        "month": month,
                        "service_type": service_type,
                        "amount": 0.0,
                        "volume": 0.0,
                    }
                    forecasted_summary = pd.concat(
                        [forecasted_summary, pd.DataFrame([new_row])], ignore_index=True
                    )
                    mask = (
                        (forecasted_summary["cloud"] == target)
                        & (forecasted_summary["month"] == month)
                        & (forecasted_summary["service_type"] == service_type)
                    )

                forecasted_summary.loc[mask, "amount"] += value

    # Recalculate "All" cloud aggregation after overrides
    forecasted_summary = forecasted_summary[forecasted_summary["cloud"] != "All"]
    all_updated = (
        forecasted_summary.groupby(["month", "service_type"])
        .agg({"amount": "sum", "volume": "sum"})
        .reset_index()
    )
    all_updated["cloud"] = "All"
    forecasted_summary = pd.concat([forecasted_summary, all_updated], ignore_index=True)

# ----------------------------------------------------------------------------- #
# Cloud Filter
filtered_forecast = forecasted_summary[forecasted_summary["cloud"] == selected_cloud]

# ----------------------------------------------------------------------------- #
# Calculate Metrics
compute_data = filtered_forecast[filtered_forecast["service_type"] == "Compute"]
storage_data = filtered_forecast[filtered_forecast["service_type"] == "Storage"]
network_data = filtered_forecast[filtered_forecast["service_type"] == "Network"]

# Calculate ratios
compute_ratios = compute_data.copy()
compute_ratios["CPH"] = compute_ratios["amount"] / compute_ratios["volume"]

storage_ratios = storage_data.copy()
storage_ratios["CPT"] = storage_ratios["amount"] / storage_ratios["volume"]

network_ratios = network_data.copy()
network_ratios["CPTT"] = network_ratios["amount"] / network_ratios["volume"]

# ----------------------------------------------------------------------------- #
# Plot Charts
col1, col2, col3 = st.columns(3)


def create_metric_chart(data, metric_col, title, target_value, color):
    if data.empty:
        return go.Figure().add_annotation(
            text=f"No data for {selected_cloud}", xref="paper", yref="paper", x=0.5, y=0.5
        )

    fig = px.line(data, x="month", y=metric_col, title=title)
    fig.add_hline(
        y=target_value,
        line_dash="dash",
        line_color="red",
        annotation_text=f"Target: ${target_value}",
    )
    fig.update_traces(line_color=color, line_width=3)
    fig.update_layout(height=400)
    return fig


with col1:
    st.subheader("💻 Cost Per Hour (CPH)")
    if not compute_ratios.empty:
        fig_cph = create_metric_chart(compute_ratios, "CPH", "Cost Per Hour", 2.45, "#1f77b4")
        st.plotly_chart(fig_cph, use_container_width=True)
    else:
        st.info(f"No compute data available for {selected_cloud}")

with col2:
    st.subheader("💾 Cost Per Terabyte (CPT)")
    if not storage_ratios.empty:
        fig_cpt = create_metric_chart(storage_ratios, "CPT", "Cost Per Terabyte", 15.80, "#ff7f0e")
        st.plotly_chart(fig_cpt, use_container_width=True)
    else:
        st.info(f"No storage data available for {selected_cloud}")

with col3:
    st.subheader("🌐 Cost Per Transferred TB (CPTT)")
    if not network_ratios.empty:
        fig_cptt = create_metric_chart(
            network_ratios, "CPTT", "Cost Per Transferred TB", 8.25, "#2ca02c"
        )
        st.plotly_chart(fig_cptt, use_container_width=True)
    else:
        st.info(f"No network data available for {selected_cloud}")

# ----------------------------------------------------------------------------- #
# Service Type Breakdown
with st.expander("📊 Cost Breakdown by Service Type", expanded=False):
    st.subheader("💰 Total Cost by Service Type")

    # Prepare breakdown data
    breakdown_data = filtered_forecast.copy()
    # Rename month column to date for plotly_stacked_bar compatibility
    breakdown_data["date"] = breakdown_data["month"]

    if not breakdown_data.empty:
        # Use plotly_stacked_bar for consistent formatting with other pages
        if plotly_stacked_bar is not None:
            plotly_stacked_bar(
                df=breakdown_data,
                y_axis="amount",
                y_axis_label="Cost ($)",
                color_col="service_type",
                color_label="Service Type",
                text_size=15,
                hide_legend=False,
                use_tooltip=True,
                round_values=False,
                granularity_select="Monthly",  # Default to Monthly for forecast data
            )
        else:
            # Fallback to basic chart if shared modules not available
            breakdown_data["month_str"] = breakdown_data["month"].dt.strftime("%Y-%m")
            fig_breakdown = px.bar(
                breakdown_data,
                x="month_str",
                y="amount",
                color="service_type",
                title=f"Monthly Cost Breakdown - {selected_cloud}",
                labels={"amount": "Cost ($)", "month_str": "Month", "service_type": "Service Type"},
            )
            fig_breakdown.update_layout(height=500)
            st.plotly_chart(fig_breakdown, use_container_width=True)

        # Summary table
        st.subheader("📋 Summary by Service Type")
        summary_table = (
            breakdown_data.groupby("service_type")
            .agg({"amount": "sum", "volume": "sum"})
            .reset_index()
        )

        summary_table["amount"] = summary_table["amount"].apply(lambda x: f"${x:,.0f}")
        summary_table["volume"] = summary_table["volume"].apply(lambda x: f"{x:,.1f}")
        summary_table.columns = ["Service Type", "Total Cost", "Total Volume"]

        st.dataframe(summary_table, use_container_width=True, hide_index=True)
    else:
        st.info(f"No breakdown data available for {selected_cloud}")
