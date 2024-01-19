# Connection Test Python Sample

## Build docker container
To build a docker container to run this connection test, the [Dockerfile](./Dockerfile) installs the [Snowflake Python Connector](https://docs.snowflake.com/en/developer-guide/python-connector/python-connector) on a [Python 3.10](https://www.python.org/downloads/release/python-3100/) base container image.

Build the test container with the statement below.
```
   docker build -t connection_test_python:v0.1 .
```

## Test container locally

To test your conatiner locally, update the environment file [env.list](./env.list) with your connection credentials and run the docker command below.
```
   docker run -it --rm --name connection_test --env-file env.list  connection_test_python:v0.1 /bin/bash -c ". connector/bin/activate;python connection_test.py"
```
You should see a similar output referencing your snowflake account id.
```
   [('successfully connected to HBB49926',)] 
```

## Test container in SPCS

Run [test script](./test.sql) in a snowsight worksheet.
You should see a similar output referencing your snowflake account id.
```
   [('successfully connected to HBB49926',)] 
```

