FROM apache/airflow:2.10.5

USER root
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    build-essential \
    git \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Install dbt-snowflake and other required packages
RUN pip install --no-cache-dir \
    dbt-snowflake==1.8.0 \
    dbt-core==1.8.0 \
    apache-airflow-providers-snowflake==5.3.0 \
    apache-airflow-providers-dbt-cloud==3.7.0 \
    airflow-dbt-python==3.0.0

# We'll use the dbt-core Python package directly instead of an Airflow provider
# This will allow us to run dbt commands via BashOperator
RUN pip install --no-cache-dir dbt-core