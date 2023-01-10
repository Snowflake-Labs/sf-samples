# A Simple Ad Tag Powered by Snowflake

Technologies used: [Snowflake](https://snowflake.com/), [Python](https://www.python.org/), [Flask](https://palletsprojects.com/p/flask/), [AWS API Gateway](https://aws.amazon.com/api-gateway/), [AWS Lambda](https://aws.amazon.com/lambda/), [Serverless Framework](https://www.serverless.com/)

Requirements: 
* Snowflake.com and Serverless.com account
* node.js, python 3, virtualenv installed

This project demonstrates how to build and deploy a simple Ad Tag powered by Snowflake. It uses a simple Python Flask API service running on AWS Lambda using Serverless Framework.

## Configuration

Copy the serverless-template.yml to serverless.yml and modify the parameters according to your Snowflake configuration. Put your private key
to your Snowflake user in AWS SSM is us-west-2 region under the parameter <ACCOUNT>.DATA_APPS_DEMO.

Install serverless and other required node packages and configure serverless (sls) for the project.

```bash
npm install
sls login
```

Create a virtualenv locally and install python packages.

```bash
virtualenv venv --python=python3
source ./venv/bin/activate
pip install -r requirements.txt
```

## Local Development

For local development you will want to use the venv previously created. This will run a local application server and connect to your Snowflake account for data access.

Start the local serverless server.

```bash
sls wsgi serve
```

### Invocation

After successful startup, you can call the created tag via HTTP:

```bash
curl http://localhost:5000/?adid=1234&placementid=abcd&creativeid=6789
```

Which should result in an empty result and a 204 no content status.

## Deployment

Build and deploy the application to AWS. For your first time, you will have to run sls without deploy to configure the project.

```bash
sls deploy
```

### Invocation

After successful deployment, you can call the created application via HTTP:

```bash
curl https://xxxxxxx.execute-api.us-west-2.amazonaws.com/dev/
```

Which should result in an empty result and a 204 no content status.

## Scaling

By default, AWS Lambda limits the total concurrent executions across all functions within a given region to 1000. The default limit is a safety limit that protects you from costs due to potential runaway or recursive functions during initial development and testing. To increase this limit above the default, follow the steps in [To request a limit increase for concurrent executions](http://docs.aws.amazon.com/lambda/latest/dg/concurrent-executions.html#increase-concurrent-executions-limit).