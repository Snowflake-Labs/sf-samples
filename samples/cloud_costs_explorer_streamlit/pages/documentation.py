"""Documentation page for cloud costs template."""

from __future__ import annotations

import streamlit as st

# All CSS in one consolidated block
st.markdown(
    """
<style>
    /* Common table styling for all tables */
    .cloudcosts-table {
        width: 100%;
        border-collapse: collapse;
    }
    
    .cloudcosts-table th {
        background-color: #105780;
        color: white;
        padding: 10px;
        text-align: left;
        font-size: 18px;
        border: 1px solid black;
    }
    
    .cloudcosts-table td {
        padding: 8px 15px;
        border: 1px solid #29B5e8;
        vertical-align: top;
        background-color: white;
        border: 1px solid black;
        color: black;
    }
    /* Table of contents specific styling - override common styling */
    .toc-table td {
        background-color: white;
    }
    
    .toc-table tr:nth-child(odd) {
        background-color: rgba(16, 87, 128, 0.1);
    }
    
    .toc-table tr:nth-child(even) {
        background-color: #f8f8f8;
    }
    
    .toc-table a {
        color: black;
        text-decoration: none;
        display: block;
        padding: 10px 0;
        border-bottom: 1px solid #29B5e8;
    }
    
    .toc-table a:last-child {
        border-bottom: none;
    }
    
    .toc-table a:hover {
        color: #29B5e8;
        background-color: rgba(41, 181, 232, 0.1);
    }
    
    /* Feature list styling */
    .feature-list {
        text-align: left;
        list-style-type: none;
        padding-left: 0;
        margin: 0;
    }
    
    .feature-list li {
        position: relative;
        padding-left: 20px;
        margin-bottom: 12px;
        padding-bottom: 0;
        border-bottom: none;
    }
    
    .feature-list li:last-child {
        margin-bottom: 0;
    }
    
    .feature-list li:before {
        content: "•";
        position: absolute;
        left: 5px;
        color: #29B5e8;
        font-weight: bold;
    }
    
    /* Section heading */
    .section-heading {
        font-weight: bold;
        color: black;
    }
    
    /* Flip card container */
    .flip-card {
        background-color: transparent;
        width: 100%;
        height: 200px;
        perspective: 1000px;
        margin-bottom: 20px;
    }
    
    /* Card container to control flip animation */
    .flip-card-inner {
        position: relative;
        width: 100%;
        height: 100%;
        text-align: center;
        transition: transform 0.8s;
        transform-style: preserve-3d;
        box-shadow: 0 4px 8px 0 rgba(0,0,0,0.2);
        border-radius: 5px;
    }
    
    /* Add flip effect on hover */
    .flip-card:hover .flip-card-inner {
        transform: rotateY(180deg);
    }
    
    /* Position front and back sides */
    .flip-card-front, .flip-card-back {
        position: absolute;
        width: 100%;
        height: 100%;
        -webkit-backface-visibility: hidden; /* Safari */
        backface-visibility: hidden;
        display: flex;
        flex-direction: column;
        justify-content: center;
        align-items: center;
        padding: 15px;
    }
    
    /* Front side styling */
    .flip-card-front {
        background-color: #105780;
        color: white;
    }
    
    /* Back side styling - rotated */
    .flip-card-back {
        background-color: #29B5e8;
        color: white;
        transform: rotateY(180deg);
    }
    
    /* Text styling */
    .flip-card h3 {
        margin-top: 0;
        font-size: 20px;
        font-weight: bold;
        color: #29B5e8;
        height: 50px;
        display: flex;
        align-items: center;
        justify-content: center;
    }
    
    .flip-card p {
        font-size: 16px;
        padding: 0 5px;
        color: white;
    }
    
    /* Expander equal height */
    .equal-height {
        min-height: 300px;
    }
    
    /* Miscellaneous styles */
    mark { 
        background-color: #e5e7e9;
        color: black;
    }
</style>
""",
    unsafe_allow_html=True,
)

# Welcome section
st.markdown(
    """
<h1>Welcome to Cloud Costs Dashboard</h1>
<p>This dashboard provides a comprehensive view of cloud costs across compute, storage, and network services 
with advanced scenario planning and budget tracking capabilities. Monitor efficiency metrics, 
track spending patterns, and optimize your cloud infrastructure costs through data-driven insights.
""",
    unsafe_allow_html=True,
)

# Five flip cards in a single row
col1, col2, col3, col4, col5 = st.columns(5)

# Function to create a flip card to avoid repetition
def create_flip_card(container, title, front_text, back_text):
    container.markdown(
        f"""
    <div class="flip-card">
        <div class="flip-card-inner">
            <div class="flip-card-front">
                <h3>{title}</h3>
                <p>{front_text}</p>
            </div>
            <div class="flip-card-back">
                <h3>{title}</h3>
                <p>{back_text}</p>
            </div>
        </div>
    </div>
    """,
        unsafe_allow_html=True,
    )

# Create cards using the function
create_flip_card(
    col1,
    "Multi-Level Cost Analysis",
    "Provides detailed cost breakdowns across categories and cost classes for comprehensive analysis.",
    "Drill down from high-level categories to specific cost classes to identify optimization opportunities and spending patterns."
)

create_flip_card(
    col2,
    "Efficiency Metric Tracking",
    "Monitors CPH, CPT, and CPTT metrics against established targets for performance optimization.",
    "Track Cost Per Hour ($2.45), Cost Per Terabyte ($15.80), and Cost Per Transferred TB ($8.25) targets."
)

create_flip_card(
    col3,
    "Advanced Scenario Planning",
    "Model different cost scenarios and optimization strategies in real-time.",
    "Create monthly recurring scenarios and one-time adjustments to forecast the impact of infrastructure changes."
)

create_flip_card(
    col4,
    "Granular Time Analysis",
    "Analyze costs across daily, weekly, monthly, and quarterly time periods.",
    "Adjust lookback windows dynamically and view historical trends with multiple aggregation levels."
)

create_flip_card(
    col5,
    "Budget Variance Monitoring",
    "Track internal spending against budgets with variance analysis and alerts.",
    "Monitor actual vs. target spend across functional areas with seasonal pattern recognition."
)

st.divider()

# Dashboard Page Navigation
st.markdown("### 📊 **Summary Pages**")

col1, col2, col3 = st.columns(3)

with col1:
    if st.button("💻 Compute Summary", use_container_width=True):
        st.switch_page("pages/compute_summary.py")

with col2:
   if st.button("💾 Storage Summary", use_container_width=True):
        st.switch_page("pages/storage_summary.py")
    
with col3:
    if st.button("🌐 Network Summary", use_container_width=True):
        st.switch_page("pages/network_summary.py")

html_input = """
<hr>

<h1>Dashboard Pages</h1>
<h4 id="compute-summary">💻 Compute Summary</h4>
<p><strong>Metric:</strong> Cost Per Hour (CPH) - Target: $2.45</p>
"""
st.markdown(html_input, unsafe_allow_html=True)

# Create expandable sections for each page
col1, col2 = st.columns(2)

with col1:
    with st.expander("Categories & Cost Classes", expanded=True):
        st.markdown(
            """
            <div class="equal-height">
            <ul class="feature-list">
                <li><strong>Core Compute:</strong> Warehouse, Database, Kubernetes, Containers</li>
                <li><strong>Advanced Compute:</strong> ML/AI, Data Processing</li>
                <li><strong>API & Microservices:</strong> APIs, Serverless, Queue Processing</li>
                <li><strong>Storage & Cache:</strong> Storage Requests, Cache Operations</li>
            </ul>
            </div>
            """,
            unsafe_allow_html=True,
        )

with col2:
    with st.expander("Key Features", expanded=True):
        st.markdown(
            """
            <div class="equal-height">
            <ul class="feature-list">
                <li>Two-level analysis: Category → Cost Class breakdown</li>
                <li>CPH efficiency tracking with target monitoring</li>
                <li>Filter by cloud, deployment, and cost classes</li>
                <li>Expandable detail tables for subcategory analysis</li>
                <li>Color-coded performance indicators</li>
            </ul>
            </div>
            """,
            unsafe_allow_html=True,
    )

html_input = """
<h4 id="storage-summary">💾 Storage Summary</h4>
<p><strong>Metric:</strong> Cost Per Terabyte (CPT) - Target: $15.80</p>
"""
st.markdown(html_input, unsafe_allow_html=True)

col1, col2 = st.columns(2)

with col1:
    with st.expander("Categories & Storage Types", expanded=True):
        st.markdown(
            """
            <div class="equal-height">
            <ul class="feature-list">
                <li><strong>Standard:</strong> General purpose storage</li>
                <li><strong>Infrequent Access:</strong> Long-term archival storage</li>
                <li><strong>Intelligent Tiering:</strong> Automated tier optimization</li>
                <li><strong>Tags:</strong> Custom categorization</li>
            </ul>
            </div>
            """,
            unsafe_allow_html=True,
        )

with col2:
    with st.expander("Cost Classes", expanded=True):
        st.markdown(
        """
            <div class="equal-height">
            <ul class="feature-list">
                <li>Logs, Operational Data, Early Retrieval, Early Deletion</li>
                <li>Hybrid Tables, Jenkins Storage, Home, Temp, Stage</li>
                <li>Replication, AI/ML Storage</li>
                <li>Storage tier optimization analysis</li>
                <li>CPT efficiency across different storage types</li>
            </ul>
            </div>
            """,
            unsafe_allow_html=True,
        )

html_input = """
<h4 id="network-summary">🌐 Network Summary</h4>
<p><strong>Metric:</strong> Cost Per Transferred Terabyte (CPTT) - Target: $8.25</p>
"""
st.markdown(html_input, unsafe_allow_html=True)

col1, col2 = st.columns(2)

with col1:
    with st.expander("Network Categories", expanded=True):
        st.markdown(
            """
            <div class="equal-height">
            <ul class="feature-list">
                <li><strong>External Traffic:</strong> Data egress and ingress</li>
                <li><strong>Infrastructure:</strong> VPC, Load Balancer</li>
                <li><strong>Replication:</strong> DB/Storage replication</li>
                <li><strong>Data Services:</strong> APIs, Ingestion, CDNs</li>
            </ul>
            </div>
            """,
            unsafe_allow_html=True,
    )

with col2:
    with st.expander("Network Features", expanded=True):
        st.markdown(
        """
            <div class="equal-height">
            <ul class="feature-list">
                <li>Network efficiency monitoring</li>
                <li>Transfer cost optimization opportunities</li>
                <li>Category-based network cost analysis</li>
                <li>Data transfer pattern identification</li>
                <li>Egress cost reduction strategies</li>
            </ul>
            </div>
            """,
            unsafe_allow_html=True,
        )

html_input = """
<h4 id="internal-summary">📊 Internal Summary</h4>
<p><strong>Metric:</strong> Total Cost vs Budget Variance</p>
"""
st.markdown(html_input, unsafe_allow_html=True)

col1, col2 = st.columns(2)

with col1:
    with st.expander("Functional Areas", expanded=True):
        st.markdown(
            """
            <div class="equal-height">
            <ul class="feature-list">
                <li><strong>Development:</strong> Application Development, Data Engineering</li>
                <li><strong>Operations:</strong> DevOps, Testing, Security</li>
                <li><strong>Support:</strong> Production Support, Customer Support</li>
                <li><strong>Infrastructure:</strong> Monitoring, Disaster Recovery, Compliance</li>
            </ul>
            </div>
            """,
            unsafe_allow_html=True,
        )

with col2:
    with st.expander("Budget Features", expanded=True):
        st.markdown(
        """
            <div class="equal-height">
            <ul class="feature-list">
                <li>Budget performance tracking by category</li>
                <li>Variance analysis (actual vs target spend)</li>
                <li>Functional area cost breakdown</li>
                <li>Annual target monitoring with seasonal patterns</li>
                <li>Color-coded variance indicators</li>
            </ul>
            </div>
            """,
            unsafe_allow_html=True,
        )

html_input = """
<h4 id="forecast-opportunities">🔮 Forecast & Opportunities</h4>
<p><strong>Purpose:</strong> Scenario Planning & Cost Optimization</p>
"""
st.markdown(html_input, unsafe_allow_html=True)

col1, col2 = st.columns(2)

with col1:
    with st.expander("Scenario Types", expanded=True):
        st.markdown(
            """
            <div class="equal-height">
            <ul class="feature-list">
                <li><strong>Monthly Recurring:</strong> Ongoing cost adjustments</li>
                <li><strong>One-time Custom:</strong> Project-specific scenarios</li>
                <li><strong>Service-specific:</strong> Optimization modeling</li>
                <li><strong>Multi-dimensional:</strong> Service Type + Category + Cost Class + Cloud</li>
            </ul>
            </div>
            """,
            unsafe_allow_html=True,
        )

with col2:
    with st.expander("Optimization Features", expanded=True):
        st.markdown(
        """
            <div class="equal-height">
            <ul class="feature-list">
                <li>Impact visualization on CPH, CPT, CPTT metrics</li>
                <li>Target achievement tracking</li>
                <li>Seasonal and trend analysis</li>
                <li>Dynamic scenario editor with validation</li>
                <li>Cost breakdown by service type</li>
            </ul>
            </div>
            """,
            unsafe_allow_html=True,
    )

st.divider()

st.markdown("## Key Features")

# Create tabs for different feature categories
efficiency_tab, filtering_tab, analysis_tab, optimization_tab = st.tabs([
    "🎯 Efficiency Metrics",
    "🔍 Filtering Options", 
    "📊 Analysis Tools",
    "💡 Optimization"
])

with efficiency_tab:
    st.markdown(
        """
        <table class="cloudcosts-table">
            <tr>
                <th>Metric</th>
                <th>Target</th>
                <th>Purpose</th>
                <th>Best Practice</th>
            </tr>
            <tr>
                <td>Cost Per Hour (CPH)</td>
                <td>$2.45</td>
                <td>Compute efficiency optimization</td>
                <td>Monitor trends and identify high-cost periods</td>
            </tr>
            <tr>
                <td>Cost Per Terabyte (CPT)</td>
                <td>$15.80</td>
                <td>Storage tier optimization</td>
                <td>Compare across storage categories for optimization</td>
            </tr>
            <tr>
                <td>Cost Per Transferred TB (CPTT)</td>
                <td>$8.25</td>
                <td>Network efficiency monitoring</td>
                <td>Identify egress optimization opportunities</td>
            </tr>
            <tr>
                <td>Budget Variance</td>
                <td>0%</td>
                <td>Internal cost control</td>
                <td>Track seasonal patterns and reallocate resources</td>
            </tr>
        </table>
        """,
        unsafe_allow_html=True,
    )

with filtering_tab:
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown(
            """
            **Time Period Controls**
            - Daily, Weekly, Monthly, Quarterly granularity
            - Dynamic lookback window adjustment
            - Historical trend analysis
            - Seasonal pattern recognition
            
            **Cloud Provider Selection**
            - AWS, GCP, Azure filtering
            - Multi-cloud cost comparison
            - Provider-specific optimization
            """
        )
    
    with col2:
        st.markdown(
            """
            **Category & Cost Class Filters**
            - Multi-select category filtering
            - Cost class drill-down capabilities
            - Service type segmentation
            - Deployment environment filtering
            
            **Functional Area Controls**
            - Department-based filtering
            - Cost center allocation
            - Project-specific analysis
            """
        )

with analysis_tab:
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown(
            """
            **Multi-Level Analysis**
            - Category-level overview with drill-down capabilities
            - Cost class breakdown for granular insights
            - Expandable detail tables for deep analysis
            - Color-coded performance indicators
            
            **Trend Analysis**
            - Year-over-year growth tracking
            - Seasonal pattern recognition
            - Budget variance monitoring
            - Efficiency metric trending
            """
        )
    
    with col2:
        st.markdown(
        """
            **Comparative Analysis**
            - Category efficiency comparisons
            - Target achievement gaps
            - Cross-service cost analysis
            - Performance benchmarking
            
            **Data Visualization**
            - Interactive charts and graphs
            - Cost distribution breakdowns
            - Time-series trend visualization
            - Performance dashboard views
            """
        )

with optimization_tab:
    st.markdown(
        """
        ### 🎯 Cost Optimization Workflow
        
        1. **Baseline Assessment**: Start with summary pages to understand current cost distribution
        2. **Target Analysis**: Identify services exceeding efficiency targets
        3. **Category Deep Dive**: Use expandable details to find specific cost classes driving inefficiency
        4. **Scenario Modeling**: Model optimization scenarios in Forecast & Opportunities
        5. **Impact Measurement**: Track improvements against targets over time
        
        ### 💡 Optimization Opportunities
        
        - **Compute**: Move workloads from Advanced to Core Compute categories
        - **Storage**: Migrate data to appropriate tiers (Standard → Infrequent Access)
        - **Network**: Optimize data transfer patterns and reduce egress costs
        - **Internal**: Reallocate resources between functional areas based on performance
        
        ### 🔮 Scenario Planning Features
        
    - Monthly recurring cost scenarios
    - One-time custom adjustments
    - Multi-dimensional scenario modeling
    - Impact visualization on all metrics
    - Resource optimization recommendations
    """
    )

st.divider()

st.markdown("## Getting Started")

st.markdown(
    """
### 🚀 Quick Start Guide

1. **Start with Summary Pages**: Navigate to Compute, Storage, or Network summaries for service-specific analysis
2. **Apply Filters**: Use sidebar controls to focus on specific time periods, clouds, or categories
3. **Drill Down**: Click "Expand Category Detail" for granular cost class analysis
4. **Monitor Targets**: Review efficiency metrics against established targets
5. **Plan Scenarios**: Use Forecast & Opportunities for cost optimization modeling
6. **Track Budgets**: Monitor internal spend against targets in Internal Summary

### 📊 Navigation Tips
- **Summary Pages**: Service-specific cost analysis with two-level breakdowns
- **Forecast Page**: Scenario planning and optimization opportunities
- **Internal Page**: Budget tracking and functional area performance
- **Documentation**: This guide with detailed feature explanations
    """
    )

st.divider()

st.markdown("## Technical Details")

col1, col2 = st.columns(2)

with col1:
    st.markdown(
        """
        **Data Sources & Updates**
        - Real-time cost calculations
        - Cached data for performance optimization
    - Dynamic filtering capabilities
        - Multi-dimensional cost categories
    """
    )

with col2:
    st.markdown(
        """
        **System Architecture** 
        - Cloud provider cost API integration
        - Time-series historical data storage
        - Regional cost allocation logic
        - Automated efficiency metric calculations
"""
)
