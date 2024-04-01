# Mandatory : Create the following Streamlit in Snowflake application with Invest_analyst role"

# Imports
import streamlit as st
from snowflake.snowpark.context import get_active_session
import pandas as pd
import plotly.express as px
from PIL import Image  # Import des images

# Settings
session = get_active_session()
st.set_page_config(layout="wide", page_title="Investment Insight Accelerator")

# Fonction pour l'onglet "Home"
def home_tab():
    st.title("Welcome to the Investment Insight Accelerator")
    """
    
    As we embark on 2024, Asset management industrie faces a **pivotal era** marked by intensifying **fee pressures** and **higher regulatory landscapes**, exemplified by initiatives like the **Sustainable Finance Disclosure Regulation (SFDR)**. 
    
    In these evolving dynamics **Technology & IA** appear as indispensable **allies** by improving **Operational efficiency** of companies, enabling a more agile answer to complex challenges.
    ### Challenges in Asset Management
    
    """
    st.image("https://i.postimg.cc/LshSTrSy/Capture-d-e-cran-2024-02-20-a-09-12-30.png", width=800)
    st.image("https://i.postimg.cc/02TH8D1p/Capture-d-e-cran-2024-02-21-a-11-42-53.png", width=800)
    """
    ### 0. Scenario
    - Leading a **French investment fund with billions in assets under management**, we excel in navigating global markets to uncover transformative opportunities.
    - Launch of our **"Sustainable Retail Fund"** marks our commitment to invest in companies that not only offer promising returns but also meet our strong **sustainability criteria**.

    ### 1. Market Intelligence
    Our investment journey begins with a comprehensive market analysis. This phase is dedicated to identifying emerging trends, technological advancements, and potential investment targets that align with our fund's objectives. **SkiGear Co.**, recognized for its exceptional quality and innovation in the outdoor gear textiles sector, has surfaced as a prime candidate for our portfolio, exemplifying our pursuit of combining financial performance with sustainability.
    
    ### 2. Due Dilligence
    **Prior to making any investment decisions**, we delve deeper into market studies, annual reports, and **conduct a meticulous due diligence process**.
    
    This includes **assessing risks associated with investing in SkiGear Co.**, by examining their industrial assets.
    
    ### 3. Post-Investment Strategy and Governance
    After the investment, our focus shifts towards portfolio monitoring. This phase is vital for maintaining transparency with our Limited Partners (LPs), providing them with real-time insights into their investments and the overall performance of the fund.
    
    Through advanced data-sharing capabilities, we aim to foster trust and alignment with our LPs' expectations. This monitoring not only optimizes portfolio performance but also validates the effectiveness of our sustainable investment strategy, ensuring that we remain true to our commitments to sustainability and long-term profitability.
    """
    st.image ('https://informationage-production.s3.amazonaws.com/uploads/2022/10/ai-investment-to-increase-but-challenges-remain-around-delivering-roi.jpeg', width= 600)
    

@st.cache_data()
def load_data_df(table_name, column):
    df = session.table(table_name).select(column).distinct().to_pandas()
    return df[column].tolist()

@st.cache_data()
def query_data_df(selected_lp, selected_fund):
    df_filtered = session.table("SNOW_INVEST.SILVER.LP_INVESTMENTS")
    if selected_lp:
        df_filtered = df_filtered.filter(df_filtered["LP"].isin(selected_lp))
    if selected_fund:
        df_filtered = df_filtered.filter(df_filtered["FUND"].isin(selected_fund))
    return df_filtered.to_pandas()

# Création vue sécurisée

def share_view(view_name):
    share_name = "SNOW_INVEST_INSIGHT"
    recipient_account = 'ASB94895'
    
    try:
        # Tente de créer le partage s'il n'existe pas déjà
        session.sql(f"CREATE SHARE IF NOT EXISTS {share_name}").collect()
        st.info(f"Share `{share_name}` is ready (either newly created or already existed).")
        
        # Accorde les privilèges d'accès à la base de données au SHARE
        session.sql(f"GRANT USAGE ON DATABASE SNOW_INVEST TO SHARE {share_name}").collect()
        
        # Ajoute la vue au partage en accordant le privilège SELECT
        session.sql(f"GRANT SELECT ON VIEW SNOW_INVEST.GOLD.{view_name} TO SHARE {share_name}").collect()
        
        # Confirme que la vue a été ajoutée au partage
        st.success(f"View `{view_name}` has been successfully added to the share `{share_name}`.")
        
        # Ajoute le compte destinataire au partage
        session.sql(f"ALTER SHARE IDENTIFIER ('SNOW_INVEST_INSIGHT') ADD ACCOUNTS = {recipient_account}").collect()
        
        # Affiche un message confirmant que la vue a été ajoutée au partage
        st.info(f"The view `{view_name}` has been successfully updated in the share listing `{share_name}`.")
        
        st.success(f"Congratulations! Your dataset `{view_name}` has been shared with locator `{recipient_account}`.")
    except Exception as e:
        st.error(f"Error during the share operation: {str(e)}")

def create_secure_view(selected_lp):
    if not selected_lp:
        st.error("Please select at least one LP.")
        return

    lp_names_clean = ["'" + lp.replace("'", "''") + "'" for lp in selected_lp]
    lp_names_joined = ", ".join(lp_names_clean)

    if len(selected_lp) > 1:
        view_name = "MULTI_LP_INVEST"
    else:
        single_lp_name = selected_lp[0].replace(" ", "_").upper().replace("#", "")  # Remove any '#' characters to avoid syntax error
        view_name = f"{single_lp_name}_INVEST"

    sql_query = f"""
    CREATE OR REPLACE SECURE VIEW SNOW_INVEST.GOLD.{view_name} AS 
    SELECT * FROM SNOW_INVEST.SILVER.LP_PF_MONITORING
    WHERE LP IN ({lp_names_joined});
    """

    try:
        session.sql(sql_query).collect()
        st.success(f"The secured view `{view_name}` has been successfully created for the selected LP(s).")
        st.info("""
        Your secured view has been created. You can directly share these insights via data sharing with your Snowflake or non-Snowflake partners!
        
        - **DIRECT SHARE**: Share individually with other Snowflake customers.
        - **MANAGED ACCOUNTS**: Share with non-Snowflake entities (Reader or Full*).
        """)
        return view_name
    except Exception as e:
        st.error(f"Error creating the secured view: {str(e)}")
        return None

def initiate_share():
    if 'view_name' in st.session_state:
        view_name = st.session_state['view_name']
        if view_name:
            share_view(view_name)
        else:
            st.error("Error: Secure view creation failed. Please try again.")
    else:
        st.error("Please create a secure view first.")

def investment_journey_tab():
    st.title("Investment Journey")
    """
    Our journey with SkiGear Co. takes us from in-depth due diligence to ongoing portfolio monitoring.
    """
    tab1, tab2 = st.tabs(["AI Due Diligence", "LP Monitoring"])
    
    with tab1:
        ai_due_diligence_tab()
    
    with tab2:
        lp_monitoring_tab()


# Fonction pour l'onglet "LPs Monitoring"
def lp_monitoring_tab():
    st.header("Portfolio Monitoring: Enhancing LP Engagement")
    """
    ### Observation
    Challenges in Asset Management
    Maintaining transparency and effective communication with Limited Partners (LPs) is crucial, yet the process of data sharing is often complex and inefficient. This can hinder the ability to provide LPs with timely, relevant insights into their investments, impacting trust and decision-making.

    ### What You Will See
    A transformative shift in LP engagement, where data sharing becomes seamless, fostering a transparent and trusting relationship. Asset managers can now provide LPs with real-time insights and detailed performance analytics effortlessly.
    """

    # Assuming load_data and query_data functions are defined elsewhere in your code.
    df_lp, df_fund = load_data_df("SNOW_INVEST.SILVER.LP_INVESTMENTS", "LP"), load_data_df("SNOW_INVEST.SILVER.LP_INVESTMENTS", "FUND")
    col1, col2 = st.columns(2)
    with col1:
        selected_lp = st.multiselect("Select Limited Partner(s):", options=df_lp, default=["Swiss Life"])
    with col2:
        selected_fund = st.multiselect("Select Fund(s):", options=df_fund)
    
    df_filtered = query_data_df(selected_lp, selected_fund)
    
    create_plots(df_filtered)
    
    """
    **Data Exploration**: Below is an editable data table. You can add your comments directly in the 'Description' column to note observations or insights about specific LP investments.
    """
    if not df_filtered.empty:
        st.experimental_data_editor(data=df_filtered, use_container_width=True)
    else:
        st.write("No data available for selected filters.")

    # Bouton pour créer une vue sécurisée
    if st.button("Create a secure view based on the selected LP(s)"):
        view_name = create_secure_view(selected_lp)
        if view_name:
            st.session_state['view_name'] = view_name

    # Bouton pour partager la vue sécurisée créée
    if st.button("Share the created secure view"):
        initiate_share()

    """
    ### Snowflake Benefits
    Snowflake data sharing streamlines the process of engaging with LPs. 
    By leveraging Snowflake, asset managers can ensure that LPs have immediate access to critical investment performance data and insights, enhancing transparency and trust in the partnership. 

    It not only simplifies cooperation but also enables more informed decision-making by LPs based on the latest data.
    """

# Fonction pour créer des graphiques
def create_plots(df_filtered):
    if not df_filtered.empty:
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.markdown("**Commitment per Year**")
            line_data = df_filtered.groupby('VINTAGE')['TOTAL_COMMITMENT_EU'].sum().reset_index()
            fig_line = px.line(line_data, x='VINTAGE', y='TOTAL_COMMITMENT_EU')
            st.plotly_chart(fig_line, use_container_width=True)
            
        with col2:
            st.markdown("**Commitment Summary**")
            df_summary = df_filtered.groupby('LP').agg(
                Total_Commitment=pd.NamedAgg(column='TOTAL_COMMITMENT_EU', aggfunc='sum'),
                Commitment_Called=pd.NamedAgg(column='COMMITMENT_CALLED_EU', aggfunc='sum'),
                Commitment_Uncalled=pd.NamedAgg(column='COMMITMENT_UNCALLED_EU', aggfunc='sum'),
                Distributed=pd.NamedAgg(column='DISTRIBUTED_EU', aggfunc='sum'),
                Recallable=pd.NamedAgg(column='RECALLABLE_EU', aggfunc='sum'),
                NAV=pd.NamedAgg(column='NAV_EU', aggfunc='sum')
            ).reset_index()
            df_melted = df_summary.melt(id_vars=['LP'], 
                                        value_vars=['Total_Commitment', 'Commitment_Called', 'Commitment_Uncalled', 'Distributed', 'Recallable', 'NAV'],
                                        var_name='Metric', value_name='Amount')
            fig_summary = px.bar(df_melted, x='Amount', y='Metric', color='LP', orientation='h', 
                                 labels={'Amount': 'Amount (EU)', 'Metric': 'Metric', 'LP': 'Client'})
            fig_summary.update_layout(showlegend=False)
            st.plotly_chart(fig_summary, use_container_width=True)

        with col3:
            st.markdown("**Private Debt/Equity Ratio**")
            asset_class_counts = df_filtered['ASSET_CLASS'].value_counts().reset_index(name='count')
            fig_pie = px.pie(asset_class_counts, values='count', names='ASSET_CLASS')
            st.plotly_chart(fig_pie, use_container_width=True)


@st.cache_data()
def load_data(view_name, column, filter_column=None, filter_value=None):
    query = f"SELECT DISTINCT {column} FROM {view_name}"
    if filter_column and filter_value:
        query += f" WHERE {filter_column} = '{filter_value}'"
    df = session.sql(query).to_pandas()
    options = sorted(df[column].dropna().unique().tolist())
    return options

@st.cache_data()
def query_data(view_name, selected_lp="All", selected_fund="All", selected_company="All", selected_year="All"):
    where_conditions = []
    if selected_lp != "All":
        where_conditions.append(f"LP = '{selected_lp}'")
    if selected_fund != "All":
        where_conditions.append(f"FUND = '{selected_fund}'")
    if selected_company != "All":
        where_conditions.append(f"COMPANY_NAME = '{selected_company}'")
    if selected_year != "All":
        where_conditions.append(f"REPORTING_YEAR = TO_DATE('{selected_year}-01-01', 'YYYY-MM-DD')")
    
    where_clause = " AND ".join(where_conditions) if where_conditions else "1=1"
    query = f"SELECT * FROM {view_name} WHERE {where_clause};"
    df = session.sql(query).to_pandas()
    return df

@st.cache_data()
def query_data_gender_distribution(view_name, selected_lp="All", selected_fund="All", selected_company="All"):
    where_conditions = []
    if selected_lp != "All":
        where_conditions.append(f"LP = '{selected_lp}'")
    if selected_fund != "All":
        where_conditions.append(f"FUND = '{selected_fund}'")
    if selected_company != "All":
        where_conditions.append(f"COMPANY_NAME = '{selected_company}'")
    
    where_clause = " AND ".join(where_conditions) if where_conditions else "1=1"
    query = f"SELECT * FROM {view_name} WHERE {where_clause};"
    df = session.sql(query).to_pandas()
    return df


def gender_distribution(view_name, selected_lp, selected_fund, selected_company):
    df = query_data_gender_distribution(view_name, selected_lp, selected_fund, selected_company)
    if df is not None:
        genders = ['Male', 'Female']
        values = [df['TOTAL_MALE_EMPLOYEES'].sum(), df['TOTAL_FEMALE_EMPLOYEES'].sum()]
        if sum(values) > 0:
            fig = px.pie(names=genders, values=values, title="Gender Distribution")
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.write("No gender data available for selected filters.")
    else:
        st.write("No data available for selected filters.")


selected_lp = "Swiss Life"
selected_fund = "Global Cross Asset Fund"

def conformity_reporting_content():
    st.title("ESG Conformity Reporting")
    st.subheader("Observation")
    st.markdown("""
    Conformity reporting, especially under regulations such as SFDR in Europe, presents a significant challenge for asset managers, requiring accurate, timely reporting on ESG factors and compliance.
    """)

    st.subheader("What You'll See")
    st.markdown("""
    In this section, we dive into our portfolio's ESG performance, pulling data directly from marketplaces and aggregating it to provide a comprehensive view of our efforts. 
    

    You'll see how we:
    
    - Retrieve in **one-click** esg datasets on **Marketplace** and aggregate data with **our Portfolio Data**
    - **Analyze our investments against ESG criteria** to ensure compliance and performance.

    We used data from **GITS Impact**, a **leading provider of impact data and intelligence**, harnessing the power of impact economics and technology to discover the full value contribution a business makes to the world. 
    """)

    
    # Création des filtres sur la même ligne
    col1, col2, col3, col4 = st.columns([3, 2, 2, 2])
    lp_names = load_data("SNOW_INVEST.GOLD.EQUITY_ESG_GHG_EMISSIONS_SUMMARY_JOINED", "LP")
    selected_lp = col1.selectbox("Select LP:", lp_names, index=lp_names.index("Swiss Life") if "Swiss Life" in lp_names else 0, key='select_lp')

    # Load Fund names but pre-select "Global Cross Asset Fund", assuming load_data can filter based on LP
    fund_names = load_data("SNOW_INVEST.GOLD.EQUITY_ESG_GHG_EMISSIONS_SUMMARY_JOINED", "FUND", "LP", selected_lp)
    fund_names_with_all = ["All"] + fund_names  # Ensure "All" is an option
    selected_fund = col2.selectbox("Select Fund:", fund_names_with_all, index=fund_names_with_all.index("Global Cross Asset Fund") if "Global Cross Asset Fund" in fund_names_with_all else 0, key='select_fund')

    # Load Company names based on pre-selected Fund
    if selected_fund != "All":
        company_names = load_data("SNOW_INVEST.GOLD.EQUITY_ESG_GHG_EMISSIONS_SUMMARY_JOINED", "COMPANY_NAME", "FUND", selected_fund)
    else:
        company_names = load_data("SNOW_INVEST.GOLD.EQUITY_ESG_GHG_EMISSIONS_SUMMARY_JOINED", "COMPANY_NAME")
    company_names_with_all = ["All"] + company_names  # Ensure "All" is at the beginning
    selected_company = col3.selectbox("Select Company:", company_names_with_all, key='select_company')

    # Load Reporting Year based on pre-selected Fund
    if selected_fund != "All":
        year_names = load_data("SNOW_INVEST.GOLD.EQUITY_ESG_GHG_EMISSIONS_SUMMARY_JOINED", "REPORTING_YEAR", "FUND", selected_fund)
    else:
        year_names = load_data("SNOW_INVEST.GOLD.EQUITY_ESG_GHG_EMISSIONS_SUMMARY_JOINED", "REPORTING_YEAR")
    year_names_with_all = ["All"] + year_names
    selected_year = col4.selectbox("Select Year:", year_names_with_all, key='select_year')

    col1, col2, col3 = st.columns(3)
    with col1:
        df_scope1 = query_data("SNOW_INVEST.GOLD.EQUITY_ESG_GHG_EMISSIONS_SUMMARY_JOINED", selected_lp, selected_fund, selected_company)
        if not df_scope1.empty:
            st.subheader("Scope 1 GHG Emissions")
            fig_scope1 = px.bar(df_scope1, x='REPORTING_YEAR', y='SCOPE_1_EMISSIONS_TOTAL', color='COMPANY_NAME', title="Scope 1 GHG Emissions by Reporting Year")
            st.plotly_chart(fig_scope1, use_container_width=True)

    with col2:
        df_scope2 = query_data("SNOW_INVEST.GOLD.EQUITY_ESG_GHG_EMISSIONS_SUMMARY_JOINED", selected_lp, selected_fund, selected_company)
        st.subheader("Scope 2 GHG Emissions")
        fig_scope2 = px.bar(df_scope2, x='REPORTING_YEAR', y='SCOPE_2_EMISSIONS_LOCATION_BASED',color='COMPANY_NAME', title="Scope 2 GHG Emissions")
        st.plotly_chart(fig_scope2, use_container_width=True)

    with col3:
        df_water = query_data("SNOW_INVEST.GOLD.EQUITY_ESG_ENERGY_WATER_CONSUMPTION_JOINED", selected_lp, selected_fund, selected_company)
        st.subheader("Water Consumption")
        fig_water = px.bar(df_water, x='REPORTING_YEAR', y='CONSUMPTION_TOTAL_WATER',color='COMPANY_NAME', title="Total Water Consumption")
        st.plotly_chart(fig_water, use_container_width=True)

    col4, col5, col6 = st.columns(3)
    with col4:
        df_waste = query_data("SNOW_INVEST.GOLD.EQUITY_ESG_WASTE_MANAGEMENT_JOINED", selected_lp, selected_fund, selected_company)
        st.subheader("Waste Management")
        fig_waste = px.bar(df_waste, x='REPORTING_YEAR', y='TOTAL_WASTE_GENERATED',color='COMPANY_NAME', title="Waste Management Overview")
        st.plotly_chart(fig_waste, use_container_width=True)

    with col5:
        df_energy = query_data("SNOW_INVEST.GOLD.EQUITY_ESG_ENERGY_WATER_CONSUMPTION_JOINED", selected_lp, selected_fund, selected_company)
        st.subheader("Energy Consumption")
        fig_energy = px.bar(df_energy, x='REPORTING_YEAR', y='CONSUMPTION_TOTAL_ENERGY',color='COMPANY_NAME', title="Total Energy Consumption")
        st.plotly_chart(fig_energy, use_container_width=True)

    with col6:
        st.subheader("Gender Distribution")
        gender_distribution("SNOW_INVEST.GOLD.EQUITY_ESG_GENDER_DISTRIBUTION_JOINED", selected_lp, selected_fund, selected_company) 

    
    st.subheader("Snowflake Benefits")
    st.markdown("""
    Snowflake facilitates seamless ESG data integration and analysis, enabling asset managers to efficiently meet SFDR requirements and other regulatory standards. 

    It not only enhances regulatory compliance but also supports sustainable investment strategies, driving long-term value.
    """)

    # Logo de la Marketplace Snowflake et lien vers la Marketplace
    st.subheader("Discover all available public listings here")
    st.markdown("[![Snowflake Marketplace](https://app.snowflake.com/static/sf_marketplace-757b05c140da9c3752ba.png)](https://app.snowflake.com/marketplace/?_ga=2.263345996.1133863339.1707690978-666559001.1702648177)")



@st.cache_data(show_spinner=True)
def query_data_sql(query):
    df = session.sql(query).to_pandas()
    return df

@st.cache_data()
def load_esg_data():
    query = """
    SELECT a.*, b.* 
    FROM SNOW_INVEST.GOLD.LP_EQUITY as a
    LEFT JOIN ESG_RISK_DATA_PUBLIC_COMPANIES_SAMPLE.REPRISK_ESG_DATA_SAMPLE_PUBLIC_COMPANIES.RR_METRICS_PUBLIC_COMPANIES_SAMPLE as b
    ON b.reprisk_company_id = a.company_id;
    """
    return load_data(query)


def operations_tab():
    st.title("3rd Step: Operations - Governance and Reporting")
    
    # Define tabs for detailed operational aspects
    tab1, tab2 = st.tabs(["Data Governance", "Conformity Reporting"])
    
    with tab1:
        data_governance_tab()

    with tab2:
        conformity_reporting_content()
    
def market_intelligence_tab():
    st.title("Making the right decisions by identifying weak signals in real-time")
    
    # Define tabs for detailed analysis at the start
    tab1, tab2 = st.tabs(["Competitive Intelligence via Latest News", "In-depth Market Analysis"])

    with tab1:
        st.header("Competitive Intelligence via Latest News")
        """
        ### Observation
        Asset managers face hurdles such as:
        
        - **Delayed Market Insights**: Struggling to access up-to-date market information.
        - **Manual Data Synthesis**: Spending excessive time manually compiling market insights.

        ### What You Will See
        - An advanced approach to market intelligence that goes beyond traditional data synthesis to include real-time competitive insights. This includes leveraging the latest news and market developments to inform investment strategies. Below, **last news from Financial Times** that could impact our investment strategy and understanding of the sector.
        """
        st.image("https://groupe-perspective.fr/wp-content/uploads/2022/03/Logo-Financial-Times.png", width=400)
        try:
            query = "SELECT TITLE, LINK FROM SNOW_INVEST.silver.financialtimes_articles_structured LIMIT 3"
            result = session.sql(query).to_pandas()
            if not result.empty:
                for index, row in result.iterrows():
                    st.write(f"**{row['TITLE']}**: [Read Article]({row['LINK']})")
            else:
                st.write("No recent articles are available at the moment.")
        except Exception as e:
            st.error(f"An error occurred while fetching the latest articles: {str(e)}")

        st.markdown("_Discover more on_ [Financial Times Wealth Management](https://www.ft.com/ft-wealth-management)")

        # Understand the Logic Behind Recommendations
        with st.expander("Understand the Logic Behind Recommendations"):
            st.markdown("""
             View `ft_wh_response` provides a synthesis of recommendations based on the analysis of recent Financial Times articles focusing on Wealth Management. It has been generated using Snowflake Cortex function directly available directly through SQL.
     ```SELECT SNOWFLAKE.CORTEX.COMPLETE(
    'mixtral-8x7b',
    CONCAT(
        'Prompt: Objective is to provide a synthesis of recommendations based on the analysis of recent Financial Times articles focusing on Wealth Management. Reason is that we raised money for a new sustainable retail fund "Sustainable Retail Fund". Weare looking to invest in companies related to this fund. In Your response should adhere to the following format: ',
        'Dear Analyst, below you will find your investment advisory synthesis dated ', CURRENT_DATE(), '. ',
        'Analyze market trends, technological advancements, and corporate strategic actions to identify relevant investment opportunities. ',
        'You will explain that you find relevant opportunity that need to be deep-dive with SkiGear Co, a family-owned French enterprise established in 1987, specializes in high-quality outdoor gear textiles. Give some fake financial KPI to justify these choice',
        'Response: '
        -- You can leave this part empty for LLaMA to fill with its response. Ensure the response does not exceed 4096 tokens.
    )
) AS response
FROM SNOW_INVEST.bronze.financialtimes_articles_wh;
           ```
  """)

        # Button for getting investment recommendations
        if st.button("Get Investment Recommendations based on the latest articles", key="get_reco_ci"):
            try:
                query = "SELECT * FROM SNOW_INVEST.bronze.ft_wh_response"
                result = session.sql(query).to_pandas()
                if not result.empty:
                    for index, row in result.iterrows():
                        for col in result.columns:
                            st.write(f"{col}: {row[col]}")
                        st.write("-" * 30)
                else:
                    st.write("No investment recommendations available at the moment.")
            except Exception as e:
                st.error(f"An error occurred while fetching investment recommendations: {str(e)}")


    with tab2:
        # Call the function that contains the content for In-depth Market Analysis
        document_analysis_content()

# Function for in-depth market analysis content
def document_analysis_content():
    st.header("In-depth Market Analysis for SkiGear Co.")
    """
    ### Observation
    Gathering, centralizing, and synthesizing vast amounts of data for market intelligence is daunting. Asset managers often struggle with not just the volume of data but also the need for timely competitive intelligence, leveraging current events and market shifts to stay ahead.

    ### What You Will See
    **Questions**:
    - In what ways can sustainability metrics be embedded into decision-making processes to drive actions that align with sustainability commitments?
    - How can retailers effectively prioritize their actions to ensure both immediate and long-term impact on ESG goals?
    
    """
    # Details
    with st.expander("Show Details"):
        model = st.selectbox('Select your model for analysis', ('mixtral-8x7b','llama2-70b-chat','llama2-7b-chat'))
        df_summary = session.table('SNOW_INVEST.SILVER.PDF_SUMMARIZED_TEXT')
        st.dataframe(df_summary, use_container_width=True)

    # Chatbox
    prompt = st.text_input("Enter your question here", placeholder="Type your question related to document insights and press enter", label_visibility="collapsed")
    if prompt:
        quest_q = f"""
        select snowflake.cortex.complete(
            '{model}', 
            concat( 
            'Answer the question based on the context only.','Context: ',
            (
                select to_varchar(array_agg(chunk)) 
                from (
                select chunk from SNOW_INVEST.GOLD.PDF_VECTOR_STORE 
                    order by vector_l2_distance(
                    snowflake.cortex.embed_text('e5-base-v2', 
                    '{prompt.replace("'", "''")}'
                    ), chunk_embedding
                    ) limit 10) 
            ),
                'Question: ', 
                '{prompt.replace("'", "''")}',
                'Answer: '
            )
        ) as response;
        """
        df_query = session.sql(quest_q).to_pandas()
        st.write(df_query['RESPONSE'][0])

    # Explanation
    with st.expander("Explanation"):
        explain_sql = f"""select 
                        REPORT, 
                        CHUNK,
                        vector_l2_distance(snowflake.cortex.embed_text('e5-base-v2', '{prompt.replace("'", "''")}'), chunk_embedding) as VECTOR_L2_DISTANCE
                        from SNOW_INVEST.GOLD.PDF_VECTOR_STORE 
                        order by VECTOR_L2_DISTANCE limit 10;
                    """
        df_explain_sql = session.sql(explain_sql).to_pandas()
        st.dataframe(df_explain_sql, use_container_width=True)

    """
    ### Snowflake Benefits
    Implementing RAG within Snowflake becomes child's play thanks to Cortex and platform's vector search capabilities. 

    **Fully Integrated AI/ML approach** significantly **reduces time and complexity** involved in document research, allowing for **efficient information retrieval** and insight generation directly within Snowflake.
    """
    st.image('https://i.postimg.cc/qB5kw52Z/Capture-d-e-cran-2024-02-20-a-14-38-25.png')


# Fonction pour l'onglet "AI Due Diligence"
def ai_due_diligence_tab():
    st.title("Enhancing Due Diligence with AI")
    st.subheader("Observation")
    st.markdown("""
    - Usual due diligence process is fraught with challenges, notably the **manual nature of data collection and analysis**, which not only **slows down the process** but significantly **increases the risk of errors**. 
    - These errors can lead to **misinformed decisions and potential oversights**, impacting the overall **investment strategy and outcomes**.""")
    
    st.subheader("What You Will See")
    st.markdown("""
    - Revolutionized due diligence process leveraging **Snowflake Document AI** 
   
    **Company**: SkiGear Co.
    **Equipment**: Injection Molder
    **Serial Number**: SGMM-12345
    """)

    # Sélection du fichier
    option = get_list_files_duedil()

    col1, col2 = st.columns([3, 1])
    get_result1 = session.file.get(f"@DOC_AI_DB.DOC_AI_SCHEMA.DUE_DILLIGENCE/{option}", "tmp")
    image = Image.open(f"tmp/{option}")
    col1.image(image)

    format_option = f"/{option[:-4]}.png"
    query = f"""
                SELECT * 
                FROM DOC_AI_DB.DOC_AI_SCHEMA.DUE_DILLIGENCE_TRANSFORMED
                where RELATIVE_PATH = '{format_option}';
            """
    result = session.sql(query).to_pandas()
    result = result.transpose()
    col2.experimental_data_editor(result, use_container_width=True, height=500)

    if col2.button("Update results"):
        col2.success('Results updated!')

    # Section pour identifier l'entreprise qui n'a pas passé le test
    """
    ### Company Analysis Results
    After analyzing the due diligence documents and assessing the risks using AI, it's essential to review the analysis results.
    """

    # Code pour identifier l'entreprise
    query_failed_company = """
    SELECT COMPANY,MACHINE_NAME,SERIAL_NUMBER, GRADE, INSPECTION_DATE
    FROM DOC_AI_DB.DOC_AI_SCHEMA.DUE_DILLIGENCE_TRANSFORMED
    WHERE GRADE = 'FAIL'
    """

    failed_company_df = session.sql(query_failed_company).to_pandas()
    st.dataframe(failed_company_df)

    # Conclusion
    """
    ### Snowflake Benefits
    Snowflake's Document AI, accelerates and simplifies the due diligence landscape : 
    - **Automating** and enhancing **data collection, extraction, and analysis**
    - **Minimizing the risk of errors** and ensurinf a more accurate and efficient evaluation**
    - Allowing asset managers to make **more informed and confident investment decisions.**
    """

# Fonction pour obtenir la liste des fichiers de due diligence
def get_list_files_duedil():
    df = session.sql("""
    SELECT REGEXP_SUBSTR(relative_path, '^[^/]+') AS FOLDER,  
    relative_path, 
    GET_PRESIGNED_URL(@DOC_AI_DB.DOC_AI_SCHEMA.DUE_DILLIGENCE, relative_path) as URL
    from DIRECTORY(@DOC_AI_DB.DOC_AI_SCHEMA.DUE_DILLIGENCES)
    """).to_pandas()
    option = st.selectbox('Choose the invoice to visualize:', df['RELATIVE_PATH'])
    return option


# Fonction pour charger la liste des bases de données
@st.cache_data
def load_db_list():
    df = session.sql("SELECT database_name FROM snowflake.information_schema.databases").to_pandas()
    return df.iloc[:, 0].tolist() 

# Fonction pour charger la liste des schémas
@st.cache_data
def load_schema_list(database):
    df = session.sql(f"SELECT schema_name FROM {database}.INFORMATION_SCHEMA.SCHEMATA").to_pandas()
    return df.iloc[:, 0].tolist()

# Fonction pour charger la liste des tables
@st.cache_data
def load_table_list(database, schema):
    df = session.sql(f"SELECT TABLE_NAME FROM {database}.INFORMATION_SCHEMA.TABLES WHERE table_schema = '{schema}'").to_pandas()
    return df.iloc[:, 0].tolist()  # Modification pour extraire la première colonne

# Fonction pour détecter automatiquement les catégories sémantiques PII
def query_detect_semantics(database_choice, schema_choice, table_choice):
    query = f"""
    SELECT 
        f.key AS COLUMN_NAME, 
        f.value:recommendation.semantic_category::string AS SEMANTIC_CATEGORY, 
        f.value:recommendation.privacy_category::string AS PRIVACY_CATEGORY,
        f.value:recommendation.confidence::string AS CONFIDENCE,
        f.value:recommendation.coverage::float AS COVERAGE_SAMPLE
    FROM 
        TABLE(
            FLATTEN(EXTRACT_SEMANTIC_CATEGORIES('{database_choice}.{schema_choice}.{table_choice}')::VARIANT)
        ) AS f 
    WHERE f.value:recommendation IS NOT NULL
    """
    return query

# Fonction pour identifier les tags GDPR
def identify_gdpr_tag(data_preview_df, database_choice, schema_choice, table_choice):
    list_tag = []
    list_GDPR = []
    for i in data_preview_df.columns:
        text = f"""SELECT SYSTEM$GET_TAG('SNOW_INVEST.SILVER.GDPR', '{database_choice}.{schema_choice}.{table_choice}."{i}"', 'column') AS TAG"""
        current_tag = session.sql(text).to_pandas()
        list_tag.append(current_tag["TAG"][0])
        list_GDPR.append("GDPR")
    
    df_gdpr = pd.DataFrame(list(zip(data_preview_df.columns, list_GDPR, list_tag)),
                        columns=['COLUMN_NAME', "TAG_NAME", 'TAG_VALUE'])
    return df_gdpr

# MAIN APPLICATION
def data_governance_tab():
    st.title("Securing Foundations for Investments")
    """
    ### Observation
    - Navigating  complex landscape of **data governance is increasingly challenging** for asset managers, especially with **high requirements of regulations like GDPR**. 

    - The need to ensure data privacy, manage consent, and respond to data subject requests **adds layers of complexity to data management practices**. 

    - **Traditional systems** struggle to keep up, leading to **potential compliance risks and inefficiencies**.
    
    ### What you will see
    - A comprehensive, streamlined approach to data governance that not only ensures compliance with regulations like GDPR but also enhances operational efficiency and data security.
    """

    # Fonction pour identifier les tags GDPR
    def identify_gdpr_tag(data_preview_df, database_choice, schema_choice, table_choice):
        list_tag = []
        list_GDPR = []
        for i in data_preview_df.columns:
            text = f"""SELECT SYSTEM$GET_TAG('SNOW_INVEST.SILVER.GDPR', '{database_choice}.{schema_choice}.{table_choice}."{i}"', 'column') AS TAG"""
            current_tag = session.sql(text).to_pandas()
            list_tag.append(current_tag["TAG"][0])
            list_GDPR.append("GDPR")
        
        df_gdpr = pd.DataFrame(list(zip(data_preview_df.columns, list_GDPR, list_tag)),
                            columns=['COLUMN_NAME', "TAG_NAME", 'TAG_VALUE'])
        return df_gdpr

    # MAIN APPLICATION
    st.subheader("1. Select a table to analyze")
    st.write("1. Choose a database, schema, and table to analyze")

    col1, col2, col3 = st.columns(3)
    with col1:
        database_choices = load_db_list()
        default_db_index = database_choices.index('SNOW_INVEST') if 'SNOW_INVEST' in database_choices else 0
        database_choice = st.selectbox('Database', database_choices, index=default_db_index)  

    with col2:
        schema_choices = load_schema_list(database_choice)
        default_schema_index = schema_choices.index('SILVER') if 'SILVER' in schema_choices else 0
        schema_choice = st.selectbox('Schema', schema_choices, index=default_schema_index) 

    with col3:
        table_choices = load_table_list(database_choice, schema_choice)
        default_table_index = table_choices.index('LPS_REFERENTIAL') if 'LPS_REFERENTIAL' in table_choices else 0
        table_choice = st.selectbox('Table', table_choices, index=default_table_index)

    if database_choice and schema_choice and table_choice:
        st.write("2. Data preview")
        data_preview_df = session.sql(f"SELECT * FROM {database_choice}.{schema_choice}.{table_choice} LIMIT 100").to_pandas()
        st.dataframe(data_preview_df)

        st.write("3. Current Column tagging:")
        df_gdpr = identify_gdpr_tag(data_preview_df, database_choice, schema_choice, table_choice)
        st.dataframe(df_gdpr)

        st.button('Refresh current GDPR tag usage')

        st.subheader("2. Apply tag manually")

        with st.form("my_form"):
            options = st.multiselect(
                'Please select columns to tag GDPR',
                data_preview_df.columns)

            tag_value = st.radio(
                "Choose the value to apply",
                ('EMAIL', 'NAME', 'GENDER', 'BANK_ACCOUNT'))

            submitted = st.form_submit_button("Apply tag")
            if submitted:
                st.write("tag applied")
                for i in options:
                    query = f'''ALTER TABLE {database_choice}.{schema_choice}.{table_choice} MODIFY COLUMN "{i}" SET TAG SNOW_INVEST.SILVER.GDPR = '{tag_value}' '''
                    session.sql(query).collect()
                    st.success('TAG APPLIED !')

        st.subheader("3. Detect automatically PII data")

        if st.button('Detect PII data'):
            df_PII = session.sql(query_detect_semantics(database_choice, schema_choice, table_choice)).to_pandas()
            st.dataframe(df_PII)
            session.create_dataframe(df_PII).write.mode("overwrite").save_as_table("SNOW_INVEST.SILVER.CLASSIFICATION_RESULT")

        st.subheader("4. Protect PII data")

        if st.button('Apply tag to protect data'):
            session.sql("CALL SNOW_INVEST.SILVER.associate_tag('CLASSIFICATION_RESULT','LPS_REFERENTIAL')").to_pandas()
            st.success('Tag applied with success !')

        st.subheader("5. Reset tag")

        if st.button('RESET data tagging'):
            session.sql(f"""ALTER TABLE {database_choice}.{schema_choice}.{table_choice} MODIFY COLUMN "EMAIL" UNSET TAG SNOW_INVEST.SILVER.GDPR ; """).collect()
            session.sql(f"""ALTER TABLE {database_choice}.{schema_choice}.{table_choice} MODIFY COLUMN "PRENOM_CONTACT" UNSET TAG SNOW_INVEST.SILVER.GDPR ; """).collect()
            session.sql(f"""ALTER TABLE {database_choice}.{schema_choice}.{table_choice} MODIFY COLUMN "NOM_CONTACT" UNSET TAG SNOW_INVEST.SILVER.GDPR ; """).collect()
            session.sql(f"""ALTER TABLE {database_choice}.{schema_choice}.{table_choice} MODIFY COLUMN "BIRTH_DATE" UNSET TAG SNOW_INVEST.SILVER.GDPR ; """).collect()
            session.sql(f"""ALTER TABLE {database_choice}.{schema_choice}.{table_choice} MODIFY COLUMN "GENDER" UNSET TAG SNOW_INVEST.SILVER.GDPR ; """).collect()
            session.sql(f"""ALTER TABLE {database_choice}.{schema_choice}.{table_choice} MODIFY COLUMN "IBAN" UNSET TAG SNOW_INVEST.SILVER.GDPR ; """).collect()
            st.success('RESET DONE !')

    """
    ### Snowflake Benefits
    Snowflake's platform is built with the complexities of modern asset management in mind, offering a robust set of features that enhance data governance and ensure compliance with regulations like GDPR:

    - **Role-Based Access Control (RBAC)**: Implement fine-grained access control to ensure that sensitive data is only accessible by authorized personnel, reducing the risk of data breaches.

    - **Federated Authentication**: Enhance security with federated authentication mechanisms that allow users to access Snowflake using their organizational credentials, simplifying access management without compromising security.

    - **Advanced Data Security and Anonymization**: Leverage End-to-end encryption, dynamic data masking, and anonymization to protect sensitive information, ensuring data privacy while enabling strategic use of the data.
    """
    

# Fonction pour l'onglet "Conclusion"
def conclusion_tab():
    st.title("Conclusion")
    st.image("https://i.postimg.cc/hPBdNmYD/Capture-d-e-cran-2024-02-14-a-23-06-32.png", use_column_width=True)
    st.image("https://i.postimg.cc/8zyfF0zc/Capture-d-e-cran-2024-02-14-a-23-06-52.png", use_column_width=True)

def main():
    app_mode = st.sidebar.radio(
        "Navigation",
        [
            "Home",
            "1. Market Intelligence",
            "2. Investment Journey",
            "3. Operations",
            "Conclusion"
        ]
    )
    
    if app_mode == "Home":
        home_tab()
    elif app_mode == "1. Market Intelligence":
        market_intelligence_tab()
    elif app_mode == "2. Investment Journey":
        investment_journey_tab()
    elif app_mode == "3. Operations":
        operations_tab() 
    elif app_mode == "Conclusion":
        conclusion_tab()


# Call the main function to run the app
if __name__ == "__main__":
    main()
