# Samples for running LLM on Snowpark Optimized Warehouses
  * Local LLM (outside Snowpark for testing and serialization)
    * Local_LLM.ipynb
  * LLM within Snowpark using External Access (see other ZIP design for larger LLMs)
    * externalAccess_1GB_UDF.ipynb
  * LLM within Snowpark using ZIP file and Stage (Recommended Design)
    * 3GB_UDF.ipynb
    * 11GB_UDF.ipynb

## Install the following packages via:

### Assumes you put a table of prompts
  * FLAN_PROMPT, e.g. with CSV file exmple:
```
  prompt
  translate English to French: What time is it??
  translate English to German: What is your name?
```

### pip  install torch transformers sentencepiece seaborn pandas jupyter snowflake-connector-python snowflake-snowpark-python protobuf

  * torch
  * transformers
  * sentencepiece
  * seaborn
  * pandas
  * jupyter
  * snowflake-connector-python
  * snowflake-snowpark-python
  * protobuf

### conda install pytorch::pytorch
  * pytorch:pytorch

