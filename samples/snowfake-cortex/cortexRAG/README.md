# About cortexRAG
cortexRAG gives users an easy way to build an end to end RAG application in Snowflake with just a few lines of python code.



#### End to End Snowflake RAG
```python
from snowflake.snowpark import Session 
from snowrag import SnowRAG 
from snowvecdb import SnowVecDB

# Initialize Snowpark
connection_parameters = {} 
snowpark = Session.builder.configs(connection_parameters).create()

# Load Knowledge Base + Store Embeddings in a new Snowflake Table
SVDB = SnowVecDB(snowflake_session=snowpark) 
SVDB("TEST_EMBEDDINGS",data_source_directory="/Users/my_pdf_directory/")

# Create RAG program powered by Snowflake
rag = SnowRAG(embeddings_table="TEST_EMBEDDINGS",lm_model="mixtral-8x7b",snowflake_session=snowpark)
rag("What was Snowflake's income in fiscal year 2022?")
```
