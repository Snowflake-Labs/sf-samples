[Amperity](https://www.amperity.com/) is a data management platform designed for enterprise companies to turn raw data into the best data asset for running their business using AI to resolve identities and a suite of models to generate assets for any use case.

Amperity turns Bronze data into Silver and Gold data in record time with infinite flexibility.

**The Amperity Impact**

Using Amperity to standardize and manage customer profiles is 86% faster than building and managing a cascade of ETLs. A trained Amperity user can turn raw data into a great unified data model in hours instead of months.

This can accelerate timelines and get your data teams working on the interesting work of building models and solving use cases.

Amperity's biggest differentiation that makes this possible is our AI identity resolution. For more information on this process see the following

1. [Site Page](https://amperity.com/solutions/use-cases/identity-resolution)
2. [Deep dive of the Stitch process](https://docs.amperity.com/stitch.html)

**Integrating with Amperity**

Plug your Snowflake data into Amperity using a first class Secure Data Sharing integration.

Amperity also offers a large library of connectors to common SAAS tools and can [function as an ETL](https://docs.amperity.com/sources.html) and [Reverse ETL platform](https://docs.amperity.com/destinations.html) for your Snowflake infrastructure.

**Empower your Marketing Team**

[Amperity provides AmpIQ](https://docs.amperity.com/ampiq.html), a marketer-friendly way for your busines teams to build lists and campaigns to integrate with your marketing platforms and paid media channels.

**This Dataset**

These datasets and workbooks are meant to provide a simple tour of the dataset you could have by adopting Amperity into your architecture. The datasets are the default models Amperity generates from raw data, with examples of how each is used to gain insights into your data.

**Gold Tables**

These are the simple, powerful tables any marketer or analyst could quickly use to understand customers and segment them based on common marketing use cases.

- [Customer_360](https://docs.amperity.com/datagrid/table_customer_360.html) | The "Gold" table. A single Amperity ID per person with the best available PII as well as many simple fields to understand who that person is and how they interact with your brand.
- [Customer_Attributes](https://docs.amperity.com/reference/data_tables.html#customer-attributes) | Table that stores flags identifying what type of customer this is, the best PII to use to interact with them, and whether they are contactable by channel.
- [Predicted_CLV_Attributes](https://docs.amperity.com/datagrid/table_predicted_clv_attributes.html) | Amperity uses ML to predict the customer lifetime value (CLV) of your customers. This table contains those predictions.
- [Predicted_Affinity_Product_Subcategory](https://docs.amperity.com/datagrid/table_predicted_affinity.html) | Amperity uses ML to which products to recommend and how strong the recommendation is.
- [Transaction_Attributes](https://docs.amperity.com/reference/data_tables.html#transaction-attributes) | Summarized behaviors from transaction history rolled into useful fields. Look at purchase behavior over different time periods, lifetime value, flags for one-and-done purchasers, etc.
- [Transaction_Attributes_Extended](https://docs.amperity.com/reference/data_tables.html#transaction-attributes-extended) | Similar to the Transaction_Attributes table but with far more granular and expanded fields.
- [Email_Engagement_Summary](https://docs.amperity.com/reference/data_tables.html#email-engagement-summary) | Contains a summary of email event statistics, such as counts for opens and clicks, the first open, and the most recent click, unique by email address.
- [Email_Engagement_Attributes](https://docs.amperity.com/reference/data_tables.html#email-engagement-attributes) | Contains many of the same fields as the Email Engagement Summary, except for the addition of the Amperity ID field. Whereas the Email Engagement Summary table is unique by email and brand (if available), the Email Engagement Attributes table is unique by the Amperity ID and email for each brand combination.

**Silver Tables**

These are standardized and cleaned up data assets that enable analyst, data science and compliance teams to easily build their own models and processes.

- [Unified_Customer](https://docs.amperity.com/datagrid/table_unified_customers.html) | Standardized stitched profiled data without any merge applied. This is the Bronze data standardized into a clean, unified model.
- [Unified_Merged](https://docs.amperity.com/datagrid/table_merged_customers.html) | This is Unified_Customer with merge logic applied to get to the most robust single record per Amperity ID.
- [Unified_Itemized_Transactions](https://docs.amperity.com/reference/data_tables.html#unified-itemized-transactions) | This is a standardized view of all transaction data from any source at the item level.
- [Unified_Transactions](https://docs.amperity.com/reference/data_tables.html#unified-transactions) | This is a standardized view of all transaction data from any source rolled up to the transaction level.
- [Unified_Household](https://docs.amperity.com/datagrid/householding.html) | Groups profiles by likely household based on configurable rules and allows easy building of a household view of behaviors.
- [Unified_Scores](https://docs.amperity.com/datagrid/table_unified_scores.html) | Contains the graph edges of Amperity clusters with links between Bronze tables and confidence weights showing the confidence of the match between two pairwise connections.
- Unified_Paid_Media| Standardized data optimized for use with paid media connectors to optimize match rates.
- [Unified_Loyalty](https://docs.amperity.com/reference/data_tables.html#unified-loyalty) | Standardized view of all people participating in your loyalty program.
- [Unified_Loyalty_Events](https://docs.amperity.com/reference/data_tables.html#unified-loyalty-events) | Standardized view of loyalty events like tier changes, points accrual, etc.
- [Unified_Compliance](https://docs.amperity.com/reference/data_tables.html#unified-compliance) | Standardized view of compliance requests for things like CCPA and GDPR. [Click here to learn more about how Amperity can solve for compliance workflows.](https://docs.amperity.com/datagrid/privacy_rights.html)
- [Fiscal_Calendar](https://docs.amperity.com/reference/data_tables.html#fiscal-calendar) | Different businesses operate on different calendars and this table is used by our analytics models to tailor results to your fiscal calendar.

**Bronze Tables**

These are sample data for a small collection of raw data to help better understand the "before". They are generated by Amperity and entirely fake, but consider this an example of the "before" state of data.

- E_Commerce_Customers | Raw ecommerce customer profiles.
- Loyalty_Members | Raw loyalty profiles.
- POS_Customers | Raw point of sale customer profiles.