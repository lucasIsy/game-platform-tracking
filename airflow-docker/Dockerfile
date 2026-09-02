FROM apache/airflow:latest-python3.13

USER airflow

RUN pip install --no-cache-dir \
    databricks-sdk \
    requests \
    polars[rtcompat]
USER airflow