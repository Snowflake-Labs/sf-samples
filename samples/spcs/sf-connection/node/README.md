# Example NodeJS App for Snowpark Container Services

# Setup
1. In a SQL Worksheet, execute `SHOW IMAGE REPOSITORIES` and look
   for the entry for your image repository.
   Note the value for `repository_url`.
2. In the main directory of this repo, execute 
   `bash ./configure.sh`. Enter the URL of the repository that you
   noted in step 1 for the repository. Enter the name of a warehouse
   to use.
3. Log into the Docker repository, build the Docker image, and push
   the image to the repository by running `make all`
   1. You can also run the steps individually. Log into the Docker 
      repository by running `make login` and entering your credentials.
   2. Make the Docker image by running `make build`.
   3. Push the image to the repository by running `make push_docker`
4. Upload the `bb_node.yaml` to a stage. You can use SnowSQL, Snowsight, 
   or any other method to `PUT` the file to the Stage.
5. Create the service by executing
   ```
   CREATE SERVICE bb_node
     IN COMPUTE POOL poolname
     FROM @stagename
     SPEC = 'bb_node.yaml';
   ```
   where you replace `poolname` with the COMPUTE POOL to use, and replace
   `stagename` with the name of the stage where you uploaded the `bb_node.yaml`
   file.
6. Use the `SYSTEM$GET_SERVICE_LOGS('bb_node')` command to confirm
   that the run succeeded and returned `SUCCESS`.


## Local Testing
This conatiner can be tested running locally. To do that, build the
image for your local machine with `make build_local`.

In order to run the container, we need to set some 
environment variables in our terminal session before running the 
container. The variables to set are:
* `SNOWFLAKE_ACCOUNT` - the account locator for the Snowflake account
* `SNOWFLAKE_USER` - the Snowflake username to use
* `SNOWFLAKE_PASSWORD` - the password for the Snowflake user
* `SNOWFLAKE_WAREHOUSE` - the warehouse to use
* `SNOWFLAKE_DATABASE` - the database to set as the current database (does not really matter that much what this is set to)
* `SNOWFLAKE_SCHEMA` - the schema in the database to set as the current schema (does not really matter that much what this is set to)

Once those have been set, run the container with `make run`. 