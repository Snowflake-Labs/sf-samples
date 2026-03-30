# Snowflake Samples

Working examples for every corner of the Snowflake platform — SQL snippets, Python apps, full-stack demos, quickstart companions, dbt projects, Native Apps, Cortex agents, and everything in between. Built by the community, maintained in the open.

**55+ samples** across AI, data engineering, Snowpark, Streamlit, Iceberg, ML, SPCS, geospatial, and more. Each one lives in its own directory under [`samples/`](samples/) with a README explaining how to run it.

> Many of these samples are companion code for [quickstarts](https://quickstarts.snowflake.com), blog posts, and conference talks.

## Looking for something specific?

Some product areas have their own dedicated repos with more focused, maintained collections:

| If you're looking for... | Go here |
|--------------------------|---------|
| **Snowflake Notebooks** (demos, tutorials, data science) | [snowflake-demo-notebooks](https://github.com/Snowflake-Labs/snowflake-demo-notebooks) |
| **Cortex AI Toolkit** (agents, RAG, search, fine-tuning, AI playground) | [Snowflake-AI-Toolkit](https://github.com/Snowflake-Labs/Snowflake-AI-Toolkit) |
| **Streamlit in Snowflake** (app patterns, deployment, components) | [snowflake-demo-streamlit](https://github.com/Snowflake-Labs/snowflake-demo-streamlit) |
| **SPCS templates** (container service blueprints) | [spcs-templates](https://github.com/Snowflake-Labs/spcs-templates) |
| **Quickstart guides** (step-by-step walkthroughs) | [sfquickstarts](https://github.com/Snowflake-Labs/sfquickstarts) |
| **Emerging Solutions Toolbox** (SIT team frameworks and accelerators) | [emerging-solutions-toolbox](https://github.com/Snowflake-Labs/emerging-solutions-toolbox) |

If it doesn't fit neatly into one of those — or crosses multiple areas — this repo is the right home.

---

## What's Inside

### Cortex AI and LLM

| Sample | Description |
|--------|-------------|
| [aisql](samples/aisql) | AISQL use cases across marketing, e-commerce, healthcare, finance with Cortex AI functions |
| [cortex-analyst](samples/cortex-analyst) | Industry-specific Cortex Analyst semantic model templates |
| [cortex_analyst_native_app](samples/cortex_analyst_native_app) | Native App for natural language querying of Marketplace data via Cortex Analyst |
| [cortex_analyst_salesforce_salescloud_template](samples/cortex_analyst_salesforce_salescloud_template) | Semantic YAML templates for querying Salesforce Sales Cloud data |
| [cortex_ai_agents_local_streamlit](samples/cortex_ai_agents_local_streamlit) | Cortex Agents demo analyzing sales conversations with Cortex Search and Analyst |
| [cortex-nemoguardrails](samples/cortex-nemoguardrails) | NeMo Guardrails integration with Cortex LLMs on SPCS |
| [snowfake-cortex](samples/snowfake-cortex) | Snippets and scripts for Snowflake Cortex functions |
| [operation_cortex](samples/operation_cortex) | Spy Agency data scavenger hunt demo with Cortex Analyst and Streamlit |
| [sow_llm](samples/sow_llm) | Open-source LLMs (FLAN-T5) on Snowpark Optimized Warehouses via UDFs |

### Agentic AI

| Sample | Description |
|--------|-------------|
| [cloudflare-r2-logpush-agent](samples/cloudflare-r2-logpush-agent) | Cloudflare HTTP log pipeline from R2 into Snowflake with a natural-language Cortex Agent |
| [create_ai_agents_on_snowflake_with_lang_ai](samples/create_ai_agents_on_snowflake_with_lang_ai) | Retention AI Agent using Lang.ai's Native App |
| [vehicle-insurance-agentic-ai](samples/vehicle-insurance-agentic-ai) | End-to-end agentic insurance claim processor using Cortex Document AI and LangGraph |

### RAG and Document Intelligence

| Sample | Description |
|--------|-------------|
| [RAG-to-Rich-Apps](samples/RAG-to-Rich-Apps) | RAG pipeline with Cortex: upload PDFs, query via notebook and Streamlit |
| [pdf-analysis](samples/pdf-analysis) | SQL-based PDF parsing pipeline using stages, streams, tasks, and a parse UDF |
| [govern-your-lakehouse-for-ai-vhol](samples/govern-your-lakehouse-for-ai-vhol) | Hands-on lab: Iceberg + governance + data quality DMFs + Cortex Agent |

### ML and Data Science

| Sample | Description |
|--------|-------------|
| [ml](samples/ml) | Comprehensive ML samples: feature store, model registry, ML jobs, HPO, distributed training |
| [custom_ml_pipelines](samples/custom_ml_pipelines) | Scikit-learn custom preprocessing with Snowflake Model Registry |
| [ML Powered Functions](samples/ML%20Powered%20Functions) | Built-in ML functions: anomaly detection, forecasting, contribution explorer |
| [energy-price-forecasting-native-app](samples/energy-price-forecasting-native-app) | Time-series electricity price forecasting with Native App deployment |
| [Sagemaker-to-Snowflake](samples/Sagemaker-to-Snowflake) | XGBoost, PyTorch, image classification migration from SageMaker to Snowflake ML |
| [Distributed Training with Ray and AutoGluon](samples/Distributed%20Training%20with%20Ray%20and%20AutoGluon) | Distributed ML training with Ray and AutoGluon on Snowflake notebooks |
| [leveraging_kumo_for_smarter_recommendations](samples/leveraging_kumo_for_smarter_recommendations) | Product recommendations for high-LTV customers using Kumo's Native App |
| [prescriptive-maintenance-demo](samples/prescriptive-maintenance-demo) | Prescriptive maintenance with scikit-learn and Snowpark |
| [multimodal-process-analytics](samples/multimodal-process-analytics) | Multimodal process analytics notebook using ML |

### Snowpark

| Sample | Description |
|--------|-------------|
| [snowpark-pandas](samples/snowpark-pandas) | Snowpark pandas API (Modin) for running pandas natively on Snowflake |
| [snowpark_connect](samples/snowpark_connect) | Snowpark Connect for migrating and running Spark workloads on Snowflake |
| [vectorized_udtfs](samples/vectorized_udtfs) | Vectorized UDTFs for high-performance time-series processing |
| [rdkit](samples/rdkit) | Cheminformatics with RDKit and Snowpark Python |

### Streamlit

| Sample | Description |
|--------|-------------|
| [streamlit-in-snowflake](samples/streamlit-in-snowflake) | Patterns collection: multi-page apps, role-based pages, deployment, telemetry |
| [cloud_costs_explorer_streamlit](samples/cloud_costs_explorer_streamlit) | Dashboard template for cloud cost exploration and forecasting |
| [FSI_asset_management_demo](samples/FSI_asset_management_demo) | Investment Insight Accelerator with Document AI and Cortex |

### Snowpark Container Services (SPCS)

| Sample | Description |
|--------|-------------|
| [spcs](samples/spcs) | SPCS samples including DeepSeek-R1 deployment and connection helpers |
| [snowflake_cli_snowpark_container_services](samples/snowflake_cli_snowpark_container_services) | Deploying containerized apps to SPCS with the Snowflake CLI |

### Iceberg

| Sample | Description |
|--------|-------------|
| [iceberg_cortex](samples/iceberg_cortex) | Cortex LLM functions on Iceberg tables with data sharing |
| [dynamic_iceberg_tables](samples/dynamic_iceberg_tables) | Creating and managing dynamic Iceberg tables |
| [vhol-iceberg-snowflake-catalog](samples/vhol-iceberg-snowflake-catalog) | Hands-on lab for Iceberg tables with Snowflake-managed catalog |

### Data Engineering

| Sample | Description |
|--------|-------------|
| [dcdf_incremental_processing](samples/dcdf_incremental_processing) | DCDF ELT patterns for incremental processing with logical partitions |
| [intro_to_de_python](samples/intro_to_de_python) | Introductory data engineering notebook with Python |
| [snowpipe_streaming_dts_summit_demo-main](samples/snowpipe_streaming_dts_summit_demo-main) | Real-time audio-to-insights pipeline: Snowpipe Streaming + Dynamic Tables |
| [braze-cdi-pipeline](samples/braze-cdi-pipeline) | Braze Cloud Data Ingestion automation with streams, tasks, and stored procedures |
| [openflow-cdc-sqlserver-demo](samples/openflow-cdc-sqlserver-demo) | OpenFlow CDC replication from SQL Server to Snowflake |
| [git-integration](samples/git-integration) | Snowflake Git integration feature samples |
| [notebooks](samples/notebooks) | Snowflake Notebooks: Anaconda webinar, container runtime, Iceberg ingestion |

### Data Sharing and Marketplace

| Sample | Description |
|--------|-------------|
| [data-sharing-consent-management](samples/data-sharing-consent-management) | Consent management framework for data sharing with mock consumer UI |
| [cordial_data_share](samples/cordial_data_share) | Queries for Cordial's Secure Data Share from Snowflake Marketplace |
| [sfguide-tasty-bytes-native-app](samples/sfguide-tasty-bytes-native-app) | Tasty Bytes sales forecast Native App for Marketplace distribution |

### Industry Solutions

| Sample | Description |
|--------|-------------|
| [retail-cg-ai-masterclass-samples](samples/retail-cg-ai-masterclass-samples) | Retail/CPG AI masterclass: voice assistant, cost governance, personalization, RAG |
| [tasty_bytes](samples/tasty_bytes) | Modular quickstarts: cost management, transformations, governance, geospatial |
| [sap_accounts_receivable_dbt](samples/sap_accounts_receivable_dbt) | dbt project for SAP accounts receivable with Tableau dashboards |
| [lakehouse_analytics](samples/lakehouse_analytics) | Lakehouse analytics with product review analysis and Cortex Analyst |

### Geospatial

| Sample | Description |
|--------|-------------|
| [geospatial](samples/geospatial) | Geospatial Python UDFs and Streamlit apps for location analytics |

### External Integrations

| Sample | Description |
|--------|-------------|
| [openai-nb](samples/openai-nb) | Azure OpenAI + AzureML Prompt Flow integration with Snowflake |
| [analyze_data_with_r_using_posit_workbench_and_snowflake](samples/analyze_data_with_r_using_posit_workbench_and_snowflake) | R-based analysis using Posit Workbench Native App with dbplyr |
| [sfguide-snowflake-python-adtag](samples/sfguide-snowflake-python-adtag) | Simple ad tag powered by Snowflake, deployed on AWS Lambda |

### FinOps and Performance

| Sample | Description |
|--------|-------------|
| [ps-finops](samples/ps-finops) | Professional Services FinOps scripts and Account Usage queries |
| [workload-optimization-queries](samples/workload-optimization-queries) | Account Usage and Information Schema queries for workload optimization |
| [reach-and-frequency-queries](samples/reach-and-frequency-queries) | SQL for advertising reach and frequency analysis |

---

## Contributing

This repo thrives on community contributions. Got a snippet, demo, or proof-of-concept that runs on Snowflake? Add it.

1. Fork the repo and create a branch
2. Create a new directory under `samples/` with a descriptive kebab-case name
3. Include all source code, SQL scripts, notebooks, and config files
4. Add a `README.md` explaining what it does, prerequisites, and how to run it
5. Open a pull request

If your sample is primarily a Snowflake Notebook, consider contributing to [snowflake-demo-notebooks](https://github.com/Snowflake-Labs/snowflake-demo-notebooks) instead. If it's a Streamlit app pattern, [snowflake-demo-streamlit](https://github.com/Snowflake-Labs/snowflake-demo-streamlit) might be a better fit. When in doubt, submit here — we can always move it later.

## Legal

Licensed under the [Apache License 2.0](LICENSE). See [LEGAL.md](LEGAL.md) for additional terms. These samples are not part of the Snowflake Service and are provided as-is.
