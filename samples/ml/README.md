# ML related samples

## Productionize your ML workflow using Snowflake DAG APIs

This is the code location corresponding to this [blog](https://medium.com/snowflake/productionize-your-ml-workflow-using-snowflake-task-dag-apis-8470aa33172c).

This sample uses multiple products
* User Task infrastructure to schedule daily training runs
* Snowflake Python API for DAG/Task
* Snowpark ML Modeling API to train a model
* Snowpark ML Model Registry to manage the models

### An example: A daily training on transaction data for fraud detection

It is very common for companies to have their customer’s data (transactions, enrollment, etc.) constantly coming in and being stored in an ongoing transactions table. For this demo, we are going to use SnowparkML to predict whether financial transactions are fraudulent or not. This will not only show how you can use Snowpark ML for the entirety of a typical ML workflow, including development and deployment but also how it can be seamlessly deployed to Snowflake for automation using DAG API.

To do that, we have both customer-level and account-level data, as well as
labeled (fraud/no fraud) individual transaction data. Let’s assume there are
tables for them `FINANCIAL_ACCOUNT` and `FINANCIAL_TRANSACTIONS`, where the latter is constantly getting updated with the latest data as they happen. We want to run a daily training job on the financial transactions data from the prior seven days of data to predict fraud. This new model will be tested against the current production model. If it passes the evaluation with test data, the daily production pipeline will automatically push the new model into production by marking it live.

### Set up

First of all, let’s talk about some housekeeping and setup for our pipeline.
For better debugging and isolation, we should run our production pipeline under
a separate database, say, `DAG_RUNS`. Under that, we would keep all production
settings such as task definitions under schema, say `SCHEDULED_RUNS`. Every
daily run of the pipeline will have its own schema like `RUN_<runid>`, created exclusively for one run of the DAG and deleted at the end. In case of failure, it will be easy to debug and peek into that particular schema.

