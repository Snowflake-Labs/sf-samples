# Example connection for dotnet



## Building

```sh
docker build -t sf-image -f Dockerfile .
```

## Configuring

Create a .env file in the running directory with these configured

```
SNOWFLAKE_ACCOUNT=<REDACTED>
SNOWFLAKE_DATABASE=<REDACTED>
SNOWFLAKE_SCHEMA=<REDACTED>
SNOWFLAKE_HOST=<REDACTED>
SNOWFLAKE_USER=<REDACTED>
SNOWFLAKE_PASSWORD=<REDACTED>
```

## Running

```sh
docker run --env-file .env -it --rm sf-image
```