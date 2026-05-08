"""Cliente HTTP para a API Core da plataforma.

Endpoints consumidos pelo Airflow (mig 0036):
- POST /tenants/{id}/pipeline-runs/start       (callback on_dag_run_start)
- POST /tenants/{id}/pipeline-runs/finalize    (callback on_dag_run_success/failure)
- POST /tenants/{id}/pipeline-tasks/upsert     (callback por task)
- GET/PUT /admin/etl-watermarks/{slug}/{source}/{entity}

Auth: Authorization: Bearer <PLATFORM_API_TOKEN>. O token estatico mapeia
no backend pra SERVICE_USER (role platform_operator) — aceito pelos
endpoints com require_staff.
"""
from __future__ import annotations

import os
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

    # ─── PipelineTask (mig 0036) ─────────────────────────────────
    # 1 callback por task do Airflow. Idempotente por
    # (tenant_id, source, external_run_id, task_id). Liga ao
    # PipelineRun pai via (dag_id, dag_run_id) — retro-link automatico
    # quando o start callback chega depois.

    def upsert_pipeline_task(self, tenant_id: str, payload: dict[str, Any]) -> dict:
        r = self._client.post(
            f"/tenants/{tenant_id}/pipeline-tasks/upsert", json=payload
        )
        r.raise_for_status()
        return r.json()

    # ─── PipelineRun (mig 0036) ─────────────────────────────────
    # Rollup de DAG run. Callbacks de DAG-level publicam aqui;
    # backend recomputa rollup a partir das pipeline_tasks filhas.

    def start_pipeline_run(self, tenant_id: str, payload: dict[str, Any]) -> dict:
        """Callback on_dag_run_start. Idempotente — retry da DAG nao quebra."""
        r = self._client.post(
            f"/tenants/{tenant_id}/pipeline-runs/start", json=payload
        )
        r.raise_for_status()
        return r.json()

    def finalize_pipeline_run(self, tenant_id: str, payload: dict[str, Any]) -> dict:
        """Callback on_dag_run_success/failure. Recomputa rollup."""
        r = self._client.post(
            f"/tenants/{tenant_id}/pipeline-runs/finalize", json=payload
        )
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
