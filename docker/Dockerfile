FROM apache/airflow:3.2.0-python3.13

USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
 && rm -rf /var/lib/apt/lists/*

USER airflow

COPY docker/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Em prod, DAGs/plugins/dbt sao baked na imagem (k3s nao git-syncs).
# Em dev (docker-compose), os volumes em ./dags|./plugins|./dbt sobrescrevem
# essas copias — ou seja, hot reload continua funcionando.
COPY dags/    ${AIRFLOW_HOME}/dags/
COPY plugins/ ${AIRFLOW_HOME}/plugins/
COPY dbt/     /opt/dbt/
