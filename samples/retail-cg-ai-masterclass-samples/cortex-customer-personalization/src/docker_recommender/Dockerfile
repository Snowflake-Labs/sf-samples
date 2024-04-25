ARG BASE_IMAGE=continuumio/miniconda3:4.12.0   
FROM $BASE_IMAGE
RUN apt-get update && \
     apt-get -y --no-install-recommends install \
     libgomp1
RUN conda install python=3.8 && \
    conda install snowflake-snowpark-python && \
    conda install -c conda-forge optuna scikit-learn pandas
RUN pip install xgboost
RUN pip install psutil
COPY training.py ./
ENTRYPOINT ["python3", "training.py"]
