# Connection Test Go Sample

## Build docker container
To build a docker container to run this connection test, the [Dockerfile](./Dockerfile) installs the [Snowflake Go Driver ](https://docs.snowflake.com/en/developer-guide/golang/go-driver)  on a [Go 1.21](https://hub.docker.com/_/golang)  base container image.

Build the test container with the statement below.
```
   docker build -t connection_test_go:v0.1 .
```

## Test container locally

To test your conatiner locally, update the environment file [env.list](./env.list) with your connection credentials and run the docker command below.
```
   docker run -it --rm --name connection_test --env-file env.list  connection_test_go:v0.1 /bin/bash -c "./connection_test_go"
```
You should see a similar output referencing your snowflake account id.
```
   SELECT 'successfully connected to '||current_account(); returns successfully connected to HBB49926
```

## Test container in SPCS

Run [test script](./test.sql) in a snowsight worksheet.
You should see a similar output referencing your snowflake account id.
```
   SELECT 'successfully connected to '||current_account(); returns successfully connected to HBB49926
```

