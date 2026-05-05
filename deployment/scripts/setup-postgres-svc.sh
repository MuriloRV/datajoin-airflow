#!/bin/bash
set -euo pipefail

# Cria/atualiza um Service+Endpoints `dj-postgres` no namespace airflow,
# apontando para o Postgres que roda em Docker NA MESMA VM (datajoin-app).
#
# Por que: pods do k3s nao tem DNS automatico pra processos do host. A
# solucao classica e' criar um Service SEM selector + Endpoints manuais
# com o IP do node — assim `dj-postgres.airflow.svc.cluster.local:5432`
# resolve dentro do cluster e roteia pra :5432 no host.
#
# Idempotente. Detecta o IP do node automaticamente; pode ser sobrescrito
# com POSTGRES_HOST_IP no .env.prod.

NAMESPACE="airflow"
SVC_NAME="dj-postgres"
PG_PORT="${POSTGRES_PORT:-5432}"

# IP do host: se o caller setou POSTGRES_HOST_IP, usa. Senao, descobre o
# InternalIP do node k3s (que coincide com o IP da VM nesse setup).
if [ -n "${POSTGRES_HOST_IP:-}" ]; then
  HOST_IP="$POSTGRES_HOST_IP"
else
  HOST_IP=$(kubectl get nodes -o jsonpath='{.items[0].status.addresses[?(@.type=="InternalIP")].address}')
fi

if [ -z "$HOST_IP" ]; then
  echo "ERRO: nao consegui descobrir o IP do host. Set POSTGRES_HOST_IP no .env.prod." >&2
  exit 1
fi

echo ">>> Apontando Service $SVC_NAME -> $HOST_IP:$PG_PORT"

kubectl create namespace "$NAMESPACE" --dry-run=client -o yaml | kubectl apply -f -

cat <<YAML | kubectl apply -f -
apiVersion: v1
kind: Service
metadata:
  name: $SVC_NAME
  namespace: $NAMESPACE
spec:
  type: ClusterIP
  ports:
    - port: $PG_PORT
      targetPort: $PG_PORT
      protocol: TCP
---
apiVersion: v1
kind: Endpoints
metadata:
  name: $SVC_NAME
  namespace: $NAMESPACE
subsets:
  - addresses:
      - ip: $HOST_IP
    ports:
      - port: $PG_PORT
        protocol: TCP
YAML

echo ">>> Service+Endpoints aplicados."
kubectl get svc,endpoints -n "$NAMESPACE" | grep "$SVC_NAME" || true
