FROM apache/airflow:3.2.0-python3.13

USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
 && rm -rf /var/lib/apt/lists/*

USER airflow

COPY docker/requirements.txt /tmp/requirements.txt
# Instalacao em 2 passos por conta de conflito irreconciliavel entre
# ecossistemas Airflow e dbt:
#   - constraints do Airflow 3.2.0 pinam protobuf==6.x (deps internas)
#   - dbt-core 1.9.1 exige protobuf<6
# Passo 1: FAB provider COM constraints — pega FAB 3.5.0 + connexion +
# flask-* nas versoes que o Airflow testou (sem isso FAB 2.x antigo
# nao puxa connexion -> ImportError em runtime).
# Passo 2: resto do requirements.txt SEM constraints — libera dbt+cosmos
# com protobuf<6. FAB nao usa protobuf, entao downgrade nao quebra.
RUN pip install --no-cache-dir apache-airflow-providers-fab==3.5.0 \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-3.2.0/constraints-3.13.txt"
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Em prod, DAGs/plugins/dbt sao baked na imagem (k3s nao git-syncs).
# Em dev (docker-compose), os volumes em ./dags|./plugins|./dbt sobrescrevem
# essas copias — ou seja, hot reload continua funcionando.
COPY dags/    ${AIRFLOW_HOME}/dags/
COPY plugins/ ${AIRFLOW_HOME}/plugins/
COPY dbt/     /opt/dbt/
