FROM apache/airflow:3.2.1

# Install dbt components directly into the default airflow user space
RUN pip install --no-cache-dir \
    dbt-core==1.11.11 \
    dbt-bigquery==1.11.1 \
    dbt-adapters==1.24.2 \
    dbt-common==1.38.0
