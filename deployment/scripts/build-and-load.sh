#!/bin/bash
set -euo pipefail

# Builda a imagem custom do Airflow (com DAGs/plugins/dbt baked) e
# importa no containerd do k3s. Sem registry remoto.
#
# Uso (na VM, a partir da raiz do projeto):
#   ./deployment/scripts/build-and-load.sh [tag]

TAG="${1:-latest}"
IMAGE_NAME="datajoin-airflow:${TAG}"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

cd "$PROJECT_ROOT"

echo ">>> Build: $IMAGE_NAME (context=$PROJECT_ROOT)"
docker build -t "$IMAGE_NAME" -f docker/Dockerfile .

echo ">>> Exportando imagem"
TARFILE="/tmp/datajoin-airflow-${TAG}.tar"
docker save "$IMAGE_NAME" -o "$TARFILE"

echo ">>> Importando no containerd do k3s"
sudo k3s ctr images import "$TARFILE"
rm -f "$TARFILE"

echo ">>> Imagens disponiveis no k3s:"
sudo k3s ctr images list | grep datajoin-airflow || true
echo ">>> $IMAGE_NAME pronta."
