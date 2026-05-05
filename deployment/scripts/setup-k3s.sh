#!/bin/bash
set -euo pipefail

# Setup k3s + Helm na VM Hostinger (Ubuntu).
# Roda como root ou com sudo. Idempotente.
#
# Uso (1a vez na VM):
#   sudo bash deployment/scripts/setup-k3s.sh

echo "============================================"
echo "  datajoin-airflow — setup k3s + Helm"
echo "============================================"

# 1. Pacotes basicos
echo ">>> Instalando dependencias do sistema"
apt-get update -y
apt-get install -y curl wget jq ca-certificates

# 2. Docker (necessario pro build da imagem do Airflow). O datajoin-app
# tambem usa, entao provavelmente ja esta instalado.
if ! command -v docker >/dev/null 2>&1; then
  echo ">>> Instalando Docker"
  curl -fsSL https://get.docker.com | sh
fi
# Permissao pro user que vai rodar deploy (ajustar se nao for ubuntu)
DEPLOY_USER="${DEPLOY_USER:-ubuntu}"
if id "$DEPLOY_USER" >/dev/null 2>&1; then
  usermod -aG docker "$DEPLOY_USER" || true
fi

# 3. k3s — single node, sem traefik (usamos NodePort), sem servicelb
if ! command -v k3s >/dev/null 2>&1; then
  echo ">>> Instalando k3s"
  curl -sfL https://get.k3s.io | sh -s - \
    --write-kubeconfig-mode 644 \
    --disable traefik \
    --disable servicelb
fi

echo ">>> Aguardando k3s ficar Ready..."
until kubectl get nodes 2>/dev/null | grep -q " Ready"; do
  echo "    waiting..."
  sleep 5
done

# 4. kubeconfig pro deploy user (pra rodar kubectl/helm sem sudo)
if id "$DEPLOY_USER" >/dev/null 2>&1; then
  mkdir -p "/home/${DEPLOY_USER}/.kube"
  cp /etc/rancher/k3s/k3s.yaml "/home/${DEPLOY_USER}/.kube/config"
  chown -R "${DEPLOY_USER}:${DEPLOY_USER}" "/home/${DEPLOY_USER}/.kube"
fi

# 5. Helm
if ! command -v helm >/dev/null 2>&1; then
  echo ">>> Instalando Helm"
  curl https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3 | bash
fi

# 6. Diretorio de logs no host (acessivel fora do container)
LOGS_DIR="${AIRFLOW_LOGS_DIR:-/var/log/datajoin-airflow}"
mkdir -p "$LOGS_DIR"
# UID 50000 = user `airflow` dentro da imagem oficial
chown -R 50000:0 "$LOGS_DIR"
chmod -R 775 "$LOGS_DIR"
echo ">>> Logs do Airflow vao para: $LOGS_DIR"

echo ""
echo "============================================"
echo "  Setup completo"
echo "============================================"
echo "k3s:  $(k3s --version | head -1)"
echo "helm: $(helm version --short)"
kubectl get nodes
echo ""
echo "Proximos passos:"
echo "  1. Editar deployment/.env.prod com seus secrets"
echo "  2. bash deployment/scripts/deploy.sh"
echo "  3. UI:  http://<IP_DA_VM>:30080  (admin/admin)"
