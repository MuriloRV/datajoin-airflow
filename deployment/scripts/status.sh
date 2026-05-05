#!/bin/bash
set -euo pipefail

NAMESPACE="airflow"

echo "=== Helm release ==="
helm list -n "$NAMESPACE" 2>/dev/null || echo "(nenhum release)"

echo ""
echo "=== Pods ==="
kubectl get pods -n "$NAMESPACE" -o wide

echo ""
echo "=== Services ==="
kubectl get svc,endpoints -n "$NAMESPACE"

echo ""
echo "=== PV / PVC ==="
kubectl get pv | grep airflow || true
kubectl get pvc -n "$NAMESPACE"

echo ""
echo "=== Eventos recentes ==="
kubectl get events -n "$NAMESPACE" --sort-by='.lastTimestamp' | tail -15
