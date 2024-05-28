# About cortexRAG
cortexRAG gives users an easy way to build an end to end RAG application in Snowflake with just a few lines of python code. NOTE - this is still experimental.

The `SnowVecDB` helper makes it easy to process pdf files from a local directory or from a Snowflake stage and create a Snowflake table with the relevant embeddings for those files. The chunking, staging, embedding generation, table creation + loading is done under the hood.

`SnowRAG` is a very simple abstraction layer that allows users to setup a very basic RAG pipeline using a Snowflake Cortex hosted LLM and the relevant Snowflake embeddings table. By default it uses a Chain of Thought template under the hood, but users can override the prompt template with the `prompt_template` argument.


#### End to End Snowflake RAG
```python
from snowflake.snowpark import Session 
from snowrag import SnowRAG 
from snowvecdb import SnowVecDB

# Initialize Snowpark
connection_parameters = {} # See snowflake docs for requisite params
snowpark = Session.builder.configs(connection_parameters).create()

# Load Knowledge Base From Local Directory + Store Embeddings in a new Snowflake Table
SVDB = SnowVecDB(snowflake_session=snowpark) 
SVDB("TEST_EMBEDDINGS",data_source_directory="/Users/my_pdf_directory/")

# Load Knowledge Base From Snowflake Stage + Store Embeddings in a new Snowflake Table
SVDB("TEST_EMBEDDINGS",stage="snowflake_stage_name")

# Create RAG program powered by Snowflake
rag = SnowRAG(embeddings_table="TEST_EMBEDDINGS",lm_model="mixtral-8x7b",snowflake_session=snowpark)
rag("What was Snowflake's income in fiscal year 2022?")
```
