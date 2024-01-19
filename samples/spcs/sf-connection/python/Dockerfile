FROM python:3.10

RUN apt-get update \
    && apt-get upgrade -y

RUN useradd --user-group --create-home --no-log-init --shell /bin/bash snowflake

USER snowflake
WORKDIR /home/snowflake
RUN python3 -m venv connector && \
     . connector/bin/activate && \
     python -m pip install --upgrade pip && \
     pip install snowflake-connector-python

COPY ./connection_test.py ./connection_test.py
