Snowflake Snowpipe Streaming + DTs Demo
=======================================

This demo does the following:

- Records audio from the default microphone
- Does speech to text transcription
- Pipes those words to Snowflake using Snowpipe Streaming
- Has a DT that aggregates words and their counts
- Has a DT that uses the aforementioned DT with an ads table to determine what ads to show to the user

Snowflake Setup
---------------

Set up a role that has create database/warehouse, ex:

```
use role accountadmin;
create or replace role tjones_summit_rl;
grant create database on account to role tjones_summit_rl;
grant create warehouse on account to role tjones_summit_rl;
grant role tjones_summit_rl to user tjones;
```

Set up a private/public keypair for the user by following [these instructions](https://docs.snowflake.com/en/user-guide/key-pair-auth).

Create a `profile.json` with the following attributes:

- `user`: Login Name or Name of User
- `url`: The account URL, ex `https://account.snowflakecomputing.com`
- `account`: Name of the account
- `private_key`: The private key contents, without headers
- `port`: Port to use, use `443` for secure connections
- `host`: Account URL, ex `account.snowflakecomputing.com`
- `role`: The role to use, ex `tjones_summit_rl`

Create the following directories locally:
```
mkdir audio;
mkdir audio_raw;
```

Build the Client SDK driver:
```
mvn clean install;
```

Set the following environment variables:

- `SNOW_ACCOUNT`: The name of the account, ex `account`
- `SNOW_ROLE`: The name of the role to use, ex `tjones_summit_rl`
- `SNOW_USER`: Login Name or Name of User
- `SNOW_PW`: Password of the user.

Set up the Python virtual environment:
```
python -m venv venv;
source venv/bin/activate;
pip install -r requirements.txt
```

Run the demo, either via Python or the `run_demo.sh` script:
```
python summit_demo.py -nopython

OR

./run_demo.sh
```
