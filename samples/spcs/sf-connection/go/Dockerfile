FROM golang:1.21

RUN apt-get update \
    && apt-get upgrade -y

RUN useradd --user-group --create-home --no-log-init --shell /bin/bash snowflake

USER snowflake
WORKDIR /home/snowflake

COPY --chown=snowflake:snowflake ./connection_example.go ./connection_example.go

RUN go mod init connection-test \
    && go mod tidy \
    && go build -o connection_test_go connection_example.go
