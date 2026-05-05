"""Cliente HTTP para a API Core da plataforma."""
from __future__ import annotations

import os
from decimal import Decimal
from typing import Any

import httpx


class PlatformClient:
    """Cliente mínimo. URL e token vêm das envs injetadas no container do Airflow."""

    def __init__(self, base_url: str | None = None, token: str | None = None) -> None:
        self.base_url = (base_url or os.environ["PLATFORM_API_URL"]).rstrip("/")
        self.token = token or os.environ["PLATFORM_API_TOKEN"]
        self._client = httpx.Client(
            base_url=self.base_url,
            headers={"Authorization": f"Bearer {self.token}"},
            timeout=10.0,
        )

    def upsert_job_run(self, tenant_id: str, payload: dict[str, Any]) -> dict:
        r = self._client.post(f"/tenants/{tenant_id}/job-runs", json=payload)
        r.raise_for_status()
        return r.json()

    # ─── SyncRun (mig 0030) ─────────────────────────────────
    # Rollup de DAG run. Callbacks de DAG-level (on_dag_run_start/success/failure)
    # publicam aqui; backend recomputa rollup a partir das job_runs filhas.

    def start_sync_run(self, tenant_id: str, payload: dict[str, Any]) -> dict:
        """Callback on_dag_run_start. Idempotente — retry da DAG nao quebra."""
        r = self._client.post(
            f"/tenants/{tenant_id}/sync-runs/start", json=payload
        )
        r.raise_for_status()
        return r.json()

    def finalize_sync_run(self, tenant_id: str, payload: dict[str, Any]) -> dict:
        """Callback on_dag_run_success/failure. Recomputa rollup."""
        r = self._client.post(
            f"/tenants/{tenant_id}/sync-runs/finalize", json=payload
        )
        r.raise_for_status()
        return r.json()

    def emit_usage(self, tenant_id: str, payload: dict[str, Any]) -> dict:
        r = self._client.post(f"/tenants/{tenant_id}/usage/events", json=payload)
        r.raise_for_status()
        return r.json()

    # ─── ETL watermarks (mig 0028) ─────────────────────────────────
    # Centralizados na metadata DB — connector le/grava via API.
    # Razao detalhada na migration e em platform_telemetry/watermarks.py.

    def get_watermark(
        self, tenant_slug: str, source: str, entity: str
    ) -> dict[str, Any] | None:
        """Le o watermark de (tenant_slug, source, entity).

        Retorna o dict armazenado em `watermark` (shape opaco — connector
        decide), ou None se nao houver registro (primeira run).

        404 do API e' tratado como "nao existe ainda" — nao propagamos
        excecao, e' caso de uso esperado.
        """
        r = self._client.get(
            f"/admin/etl-watermarks/{tenant_slug}/{source}/{entity}"
        )
        if r.status_code == 404:
            return None
        r.raise_for_status()
        return r.json()["watermark"]

    def set_watermark(
        self,
        tenant_slug: str,
        source: str,
        entity: str,
        watermark: dict[str, Any],
        *,
        rows_loaded: int | None = None,
        run_id: str | None = None,
    ) -> dict:
        """Upsert do watermark. Idempotente — re-rodar com mesmo trio
        sobrescreve."""
        r = self._client.put(
            f"/admin/etl-watermarks/{tenant_slug}/{source}/{entity}",
            json={
                "watermark": watermark,
                "rows_loaded": rows_loaded,
                "run_id": run_id,
            },
        )
        r.raise_for_status()
        return r.json()

    def close(self) -> None:
        self._client.close()

    @staticmethod
    def _json_safe(value: Any) -> Any:
        if isinstance(value, Decimal):
            return float(value)
        return value
