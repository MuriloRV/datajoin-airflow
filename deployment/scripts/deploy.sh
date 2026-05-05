#!/bin/bash
set -euo pipefail

# Deploy/upgrade do Airflow via Helm.
# Roda na VM, a partir da raiz do projeto. Idempotente.
#
# Uso:
#   ./deployment/scripts/deploy.sh                  # tag = latest
#   ./deployment/scripts/deploy.sh <git-sha>        # ou outra tag
#
# Pre-requisitos (rodar 1x):
#   sudo bash deployment/scripts/setup-k3s.sh

TAG="${1:-latest}"
NAMESPACE="airflow"
RELEASE_NAME="airflow"
CHART_REPO="apache-airflow"
CHART_NAME="apache-airflow/airflow"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

cd "$PROJECT_ROOT"

echo "==> Deploy datajoin-airflow (tag=$TAG)"

# 1. Repo helm
if ! helm repo list 2>/dev/null | grep -q "$CHART_REPO"; then
  echo "==> Adicionando repo Helm $CHART_REPO"
  helm repo add "$CHART_REPO" https://airflow.apache.org
fi
helm repo update

# 2. Namespace
kubectl create namespace "$NAMESPACE" --dry-run=client -o yaml | kubectl apply -f -

# 3. Setup auxiliares (idempotentes)
echo "==> Setup logs volume (hostPath)"
bash "$SCRIPT_DIR/setup-logs-volume.sh"

echo "==> Setup Service -> postgres (Docker no host)"
bash "$SCRIPT_DIR/setup-postgres-svc.sh"

echo "==> Aplicando secrets a partir de deployment/.env.prod"
bash "$SCRIPT_DIR/create-metadata-secret.sh"

# 4. Build + import da imagem no k3s
echo "==> Build + import da imagem"
bash "$SCRIPT_DIR/build-and-load.sh" "$TAG"

# 5. Helm upgrade --install
echo "==> helm upgrade --install"
helm upgrade --install "$RELEASE_NAME" "$CHART_NAME" \
  --namespace "$NAMESPACE" \
  --values "$PROJECT_ROOT/deployment/values/base.yaml" \
  --values "$PROJECT_ROOT/deployment/values/prod.yaml" \
  --set "images.airflow.tag=$TAG" \
  --timeout 10m

# 6. db migrate manual — o Helm hook e' flaky em primeira instalacao quando
# o postgres externo nao existia ainda; rodar como pod throwaway garante.
echo "==> Rodando airflow db migrate"
DB_CONN=$(kubectl get secret airflow-metadata-secret -n "$NAMESPACE" -o jsonpath='{.data.connection}' | base64 -d)
kubectl delete pod airflow-db-migrate -n "$NAMESPACE" --ignore-not-found
kubectl run airflow-db-migrate \
  --restart=Never \
  --image="datajoin-airflow:${TAG}" \
  --image-pull-policy=Never \
  -n "$NAMESPACE" \
  --env="AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=${DB_CONN}" \
  -- airflow db migrate

echo "==> Aguardando migrate terminar..."
kubectl wait --for=condition=Ready pod/airflow-db-migrate -n "$NAMESPACE" --timeout=120s 2>/dev/null || true
sleep 10
kubectl logs airflow-db-migrate -n "$NAMESPACE" 2>&1 | tail -5 || true
kubectl delete pod airflow-db-migrate -n "$NAMESPACE" --ignore-not-found

# 7. Senha previsivel pro SimpleAuthManager (admin/admin) — escrita em runtime
# via initContainer/exec nao funciona limpo no chart; gravamos no PVC quando
# o api-server sobe pela 1a vez via post-upgrade hook simulado:
echo "==> Garantindo password file do SimpleAuthManager"
kubectl rollout status deployment -n "$NAMESPACE" -l component=api-server --timeout=60s 2>/dev/null || true
API_POD=$(kubectl get pod -n "$NAMESPACE" -l component=api-server -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || echo "")
if [ -n "$API_POD" ]; then
  kubectl exec -n "$NAMESPACE" "$API_POD" -- \
    sh -c 'echo "{\"admin\": \"admin\"}" > /opt/airflow/simple_auth_manager_passwords.json.generated' \
    2>/dev/null || echo "    (skip — pod ainda nao pronto, aplicara no proximo restart)"
fi

# 8. Restart pra apanhar imagem nova (Helm so muda a tag se mudou; aqui forcamos)
echo "==> Rolling restart"
kubectl rollout restart deployment -n "$NAMESPACE" 2>/dev/null || true
kubectl rollout restart statefulset -n "$NAMESPACE" 2>/dev/null || true

echo "==> Aguardando api-server"
sleep 15
kubectl wait --for=condition=ready pod -l component=api-server -n "$NAMESPACE" --timeout=300s

echo ""
echo "==> Deploy completo"
echo "    UI:    http://$(curl -s ifconfig.me 2>/dev/null || hostname -I | awk '{print $1}'):30080"
echo "    Login: admin / admin"
