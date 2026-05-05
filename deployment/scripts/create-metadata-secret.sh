#!/bin/bash
set -euo pipefail

# Cria/atualiza o secret `airflow-metadata-secret` (connection string do
# Postgres metadata) e o secret `datajoin-airflow-env` (envs sensiveis
# referenciadas no base.yaml via secretKeyRef).
#
# Le tudo de deployment/.env.prod (variaveis listadas em .env.prod.example).

NAMESPACE="airflow"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ENV_FILE="${SCRIPT_DIR}/../.env.prod"

if [ ! -f "$ENV_FILE" ]; then
  echo "ERRO: $ENV_FILE nao encontrado." >&2
  echo "      Copie deployment/.env.prod.example para deployment/.env.prod e preencha." >&2
  exit 1
fi

# shellcheck disable=SC1090
set -a; source "$ENV_FILE"; set +a

: "${POSTGRES_USER:?defina no .env.prod}"
: "${POSTGRES_PASSWORD:?defina no .env.prod}"
: "${POSTGRES_DB:?defina no .env.prod}"
: "${AIRFLOW_FERNET_KEY:?defina no .env.prod}"
: "${AIRFLOW_SECRET_KEY:?defina no .env.prod}"
: "${AIRFLOW_JWT_SECRET:?defina no .env.prod}"
: "${SERVICE_TOKEN:?defina no .env.prod}"

# Conexao aponta pro Service interno (criado por setup-postgres-svc.sh).
PG_HOST_IN_CLUSTER="${PG_HOST_IN_CLUSTER:-dj-postgres.airflow.svc.cluster.local}"
PG_PORT="${POSTGRES_PORT:-5432}"

CONN="postgresql://${POSTGRES_USER}:${POSTGRES_PASSWORD}@${PG_HOST_IN_CLUSTER}:${PG_PORT}/${POSTGRES_DB}"

kubectl create namespace "$NAMESPACE" --dry-run=client -o yaml | kubectl apply -f -

# Secret 1 — metadata DB connection (formato esperado pelo chart)
kubectl create secret generic airflow-metadata-secret \
  --namespace "$NAMESPACE" \
  --from-literal=connection="$CONN" \
  --dry-run=client -o yaml | kubectl apply -f -

# Secret 2 — envs sensiveis montadas em todos os pods via valueFrom no base.yaml
kubectl create secret generic datajoin-airflow-env \
  --namespace "$NAMESPACE" \
  --from-literal=AIRFLOW_FERNET_KEY="$AIRFLOW_FERNET_KEY" \
  --from-literal=AIRFLOW_SECRET_KEY="$AIRFLOW_SECRET_KEY" \
  --from-literal=AIRFLOW_JWT_SECRET="$AIRFLOW_JWT_SECRET" \
  --from-literal=SERVICE_TOKEN="$SERVICE_TOKEN" \
  --from-literal=PLATFORM_API_URL="${PLATFORM_API_URL:-http://localhost:8000}" \
  --from-literal=DW_DEFAULT_HOST="${PG_HOST_IN_CLUSTER}" \
  --from-literal=DW_DEFAULT_PORT="${PG_PORT}" \
  --from-literal=DW_DB_USER="${DW_DB_USER:-dw_admin}" \
  --from-literal=DW_DB_PASSWORD="${DW_DB_PASSWORD:-dw_admin}" \
  --from-literal=DW_DB_NAME="${DW_DB_NAME:-warehouse}" \
  --dry-run=client -o yaml | kubectl apply -f -

echo ">>> Secrets atualizados em namespace $NAMESPACE"
kubectl get secret -n "$NAMESPACE" | grep -E 'airflow-metadata-secret|datajoin-airflow-env' || true
