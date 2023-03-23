FROM python:3.9-slim

ENV DBT_DIR /dbt
ENV DBT_PROFILES_DIR /dbt
WORKDIR $DBT_DIR

# Update and install system packages
RUN apt-get update && apt-get install -y \
    git \
  && rm -rf /var/lib/apt/lists/* \
  && pip install -U pip

COPY requirements.txt .
RUN pip install -r requirements.txt

VOLUME /dbt
ENTRYPOINT ["dbt"]
