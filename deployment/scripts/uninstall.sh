#!/bin/bash
set -euo pipefail

# Remove o release do Airflow do k3s. NAO mexe nos logs do host
# (/var/log/datajoin-airflow), no Postgres do Docker, nem no proprio k3s.
#
# Uso:
#   ./deployment/scripts/uninstall.sh             # remove release
#   ./deployment/scripts/uninstall.sh --purge     # remove release + namespace + PV/PVC

NAMESPACE="airflow"
PURGE="${1:-}"

echo ">>> Helm uninstall airflow -n $NAMESPACE"
helm uninstall airflow -n "$NAMESPACE" 2>/dev/null || echo "(release nao encontrado)"

if [ "$PURGE" = "--purge" ]; then
  echo ">>> --purge: removendo PVC, PV e namespace"
  kubectl delete pvc airflow-logs-pvc -n "$NAMESPACE" --ignore-not-found
  kubectl delete pv airflow-logs-pv --ignore-not-found
  kubectl delete namespace "$NAMESPACE" --ignore-not-found
  echo ">>> Logs no host (/var/log/datajoin-airflow) NAO foram apagados."
fi

echo ">>> Pronto."
