#!/bin/bash
set -euo pipefail

# Cria PV (hostPath) + PVC para os logs do Airflow.
# O hostPath aponta pra um diretorio na VM (default /var/log/datajoin-airflow),
# que voce pode tail/cat de fora do container — esse e' o requisito.
#
# Single-node k3s: ReadWriteOnce + hostPath funciona pros 4 pods (api,
# scheduler, triggerer, dag-processor) porque todos rodam no mesmo node.

NAMESPACE="airflow"
PV_NAME="airflow-logs-pv"
PVC_NAME="airflow-logs-pvc"
LOGS_DIR="${AIRFLOW_LOGS_DIR:-/var/log/datajoin-airflow}"
SIZE="${AIRFLOW_LOGS_SIZE:-20Gi}"

echo ">>> Garantindo $LOGS_DIR no host"
sudo mkdir -p "$LOGS_DIR"
sudo chown -R 50000:0 "$LOGS_DIR"  # UID do user airflow na imagem oficial
sudo chmod -R 775 "$LOGS_DIR"

kubectl create namespace "$NAMESPACE" --dry-run=client -o yaml | kubectl apply -f -

cat <<YAML | kubectl apply -f -
apiVersion: v1
kind: PersistentVolume
metadata:
  name: $PV_NAME
  labels:
    app: datajoin-airflow
    type: logs
spec:
  capacity:
    storage: $SIZE
  accessModes:
    - ReadWriteOnce
  persistentVolumeReclaimPolicy: Retain
  storageClassName: ""        # bind manual, nao usa storage class
  hostPath:
    path: $LOGS_DIR
    type: DirectoryOrCreate
---
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: $PVC_NAME
  namespace: $NAMESPACE
spec:
  accessModes:
    - ReadWriteOnce
  storageClassName: ""        # bate com o PV acima (selector via volumeName)
  volumeName: $PV_NAME
  resources:
    requests:
      storage: $SIZE
YAML

echo ">>> Logs:"
kubectl get pv "$PV_NAME"
kubectl get pvc "$PVC_NAME" -n "$NAMESPACE"
echo ">>> Caminho no host: $LOGS_DIR"
