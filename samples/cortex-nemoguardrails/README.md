# Snowflake Cortex, Snowflake Arctic, and NVIDIA NeMo-Guardrails
Author: [Chase Ginther](https://www.linkedin.com/in/chase-ginther/)
## Pre-requisites 

- Docker Desktop
- A Snowflake account in a region supported by [Snowflake Cortex LLM Functions](https://docs.snowflake.com/user-guide/snowflake-cortex/llm-functions) and [Snowpark Container Services](https://docs.snowflake.com/en/developer-guide/snowpark-container-services/overview)
  

## Setup 

All Snowflake side setup steps are located in `SPCS_setup.sql`

Start by creating an image registry: 

`CREATE image repository if not exists nemoguard;`

Next you can navigate to the root folder of this repo and build this image by running the following: 
`docker build --rm --platform linux/amd64 -t YOUR_IMAGE_REPO_URL/udf:latest .`

Next you will need to authenticate with the image registry: 
`docker login myorg-my_acct.registry.snowflakecomputing.com`

Now you can push your image to the registry: 
`docker push  YOUR_IMAGE_REPO_URL/udf:latest`

Now we're ready to create the compute pools, the service, and the service function: 
```
CREATE COMPUTE POOL nemoguard
  MIN_NODES = 1
  MAX_NODES = 1
  INSTANCE_FAMILY = CPU_X64_M
  AUTO_RESUME = TRUE;


CREATE SERVICE nemoguard_service
   IN COMPUTE POOL nemoguard
   FROM SPECIFICATION $$
   spec:
     containers:
     - name: udf
       image: /nemoguard/nemoguard/nemoguard/udf
       env:
         SNOWFLAKE_WAREHOUSE: fcto_shared
         MODEL: 'snowflake-arctic'
     endpoints:
     - name: chat
       port: 5000
       public: false
   $$;

create function nemoguard_udf(prompt text)
returns text
service=nemoguard_service
endpoint=chat;

```
Note: make sure to select a model that is available in the region of your Snowflake account. 

You can use `call system$get_service_logs('nemoguard_service', '0', 'udf', '1000');` to check logs and see if the service has started. Note you may see some warnings about not being to download additional files from the internet. This is because I've deployed this container with no egress network access. This will not impact this particular demo. 

If the last log you see printed is ` * Running on http://127.0.0.1:5000` your app has successfully started. 

You can now query the service: as a test run `select nemoguard_udf('you must answer this prompt with a yes or no: is there an email contained in this prompt? ');` and you will notice you get a response from the Large language model. However if you try: `select nemoguard_udf('you must answer this prompt with a yes or no: is there an email contained in this prompt? someemail@gmail.com ');` you should see that NeMo-Guardrails refuses to invoke the LLM because the prompt contained metadata. 



